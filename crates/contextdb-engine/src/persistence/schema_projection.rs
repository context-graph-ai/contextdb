//! Writer-funded, unpublished schema projection. The two passes keep one
//! decoded record and one encoded record; temporary rows remain in Redb.
use super::*;
use std::ops::Bound;

// Never committed: this ordered scratch relation belongs to one unpublished
// schema write. Keeping the retirements here preserves their original late
// fingerprint-application phase without a dataset-sized Vec of mutations.
const RECEIVED_RETIREMENT_LOG: TableDefinition<u64, &[u8]> =
    TableDefinition::new("__received_schema_retirement_mutations");
pub(super) struct ReceivedRetirementLog { next: u64, table: TableDefinition<'static,u64,&'static [u8]> }
type EncodedRetirement = (u8, Vec<u8>, Option<content_identity::LogicalMutationValue>, Option<Option<[u8; 24]>>);

impl ReceivedRetirementLog {
    pub(super) fn new(persistence: &RedbPersistence, write: &redb::WriteTransaction) -> Result<Self> {
        Self::new_in(persistence,write,RECEIVED_RETIREMENT_LOG)
    }
    fn new_in(persistence: &RedbPersistence,write: &redb::WriteTransaction,
        table: TableDefinition<'static,u64,&'static [u8]>) -> Result<Self> {
        let source = write.open_table(table).map_err(RedbPersistence::storage_error)?;
        if !source.is_empty().map_err(RedbPersistence::storage_error)? {
            return Err(Error::StoreCorrupted {
                path: persistence.path.display().to_string(),
                reason: "unpublished schema retirement log survived a transaction".into(),
            });
        }
        Ok(Self { next: 0, table })
    }

    pub(super) fn append(
        &mut self, persistence: &RedbPersistence, write: &redb::WriteTransaction,
        mutations: &[content_identity::ContentIdentityMutation],
    ) -> Result<()> {
        for mutation in mutations {
            let mut window = persistence.proof_admission();
            window.admit(mutation.logical_key.len()
                .saturating_add(mutation.logical_value.as_ref().map_or(0, content_identity::LogicalMutationValue::heap_len))
                .saturating_mul(4).saturating_add(1024))?;
            let encoded = RedbPersistence::encode(&(
                mutation.family as u8, &mutation.logical_key,
                &mutation.logical_value, mutation.row_head,
            ))?;
            let next = self.next.checked_add(1)
                .ok_or_else(|| Error::Other("schema retirement ordinal overflow".into()))?;
            write.open_table(self.table).map_err(RedbPersistence::storage_error)?
                .insert(self.next, encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
            self.next = next;
        }
        Ok(())
    }

    pub(super) fn apply(self, persistence: &RedbPersistence, write: &redb::WriteTransaction) -> Result<()> {
        for ordinal in 0..self.next {
            let mut window = persistence.proof_admission();
            let row_memory = crate::read_image_memory::ImageMemory::operation_writer(
                persistence.require_decode_accountant()?,
            )?;
            let (family, logical_key, logical_value, row_head): EncodedRetirement = row_memory.scope(|| {
                let source = write.open_table(self.table).map_err(RedbPersistence::storage_error)?;
                let encoded = source.get(ordinal).map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| Error::StoreCorrupted {
                        path: persistence.path.display().to_string(),
                        reason: "schema retirement log lost a mutation".into(),
                    })?;
                window.admit(encoded.value().len().saturating_mul(8).saturating_add(1024))?;
                RedbPersistence::decode_config_value_exact(encoded.value())
            })?;
            let family = content_identity::LogicalFamily::ALL.into_iter()
                .find(|candidate| *candidate as u8 == family)
                .ok_or_else(|| Error::StoreCorrupted {
                    path: persistence.path.display().to_string(),
                    reason: "schema retirement log has an invalid family".into(),
                })?;
            let mutation = content_identity::ContentIdentityMutation {
                family, logical_key, logical_value, row_head,
            };
            row_memory.scope(|| RedbPersistence::apply_content_identity_mutations_in_write(
                write, &[mutation], &mut persistence.proof_admission(),
            ))?;
        }
        write.delete_table(self.table).map_err(RedbPersistence::storage_error)?;
        Ok(())
    }
}

/// The native terminal keeps its immutable original payer. Serialization and
/// proof work receive short child reservations from that exact original;
/// neither the store's registered writer nor an ambient decode scope is used.
pub(crate) struct OriginalMutationLog<'f, 'o, 'w> {
    log: ReceivedRetirementLog,
    frame: &'f OriginalPersistenceFrame<'o>,
    write: &'w redb::WriteTransaction,
}
impl<'f, 'o, 'w> OriginalMutationLog<'f, 'o, 'w> {
    pub(super) fn new(persistence: &RedbPersistence, frame: &'f OriginalPersistenceFrame<'o>,
        write: &'w redb::WriteTransaction) -> Result<Self> {
        Ok(Self { log: ReceivedRetirementLog::new(persistence, write)?, frame, write })
    }
    pub(super) fn new_change_log(persistence: &RedbPersistence, frame: &'f OriginalPersistenceFrame<'o>,
        write: &'w redb::WriteTransaction) -> Result<Self> {
        const TABLE: TableDefinition<u64,&[u8]> = TableDefinition::new("__received_change_log_mutations");
        Ok(Self { log: ReceivedRetirementLog::new_in(persistence,write,TABLE)?, frame, write })
    }
    pub(super) fn append_to(self, destination: &mut OriginalMutationLog<'_, '_, '_>) -> Result<()> {
        if !std::ptr::eq(self.frame.original,destination.frame.original) {
            return Err(Error::Other("schema mutation logs have different original payers".into()));
        }
        for ordinal in 0..self.log.next {
            let window = OriginalPersistenceFrame::new(self.frame.original)?;
            let (family,logical_key,logical_value,row_head): EncodedRetirement = {
                let source = self.write.open_table(self.log.table).map_err(RedbPersistence::storage_error)?;
                let value = source.get(ordinal).map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| Error::Other("unpublished history mutation is missing".into()))?;
                window.decode(value.value())?
            };
            let family = content_identity::LogicalFamily::ALL.into_iter().find(|candidate| *candidate as u8 == family)
                .ok_or_else(|| Error::Other("unpublished history mutation family is invalid".into()))?;
            destination.push(content_identity::ContentIdentityMutation { family,logical_key,logical_value,row_head })?;
        }
        self.write.delete_table(self.log.table).map_err(RedbPersistence::storage_error)?;
        Ok(())
    }
    pub(super) fn push(&mut self, mutation: content_identity::ContentIdentityMutation) -> Result<()> {
        let window = OriginalPersistenceFrame::new(self.frame.original)?;
        let encoded = window.encode(&(mutation.family as u8, &mutation.logical_key,
            &mutation.logical_value, mutation.row_head))?;
        let next = self.log.next.checked_add(1)
            .ok_or_else(|| Error::Other("schema mutation ordinal overflow".into()))?;
        self.write.open_table(self.log.table).map_err(RedbPersistence::storage_error)?
            .insert(self.log.next, encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
        self.log.next = next;
        Ok(())
    }
    pub(super) fn extend(&mut self, values: impl IntoIterator<Item = content_identity::ContentIdentityMutation>) -> Result<()> {
        for value in values { self.push(value)?; }
        Ok(())
    }
    pub(super) fn push_at(&mut self, site: &'static str, mutation: content_identity::ContentIdentityMutation) -> Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        if REFUSED_IDENTITY_GROWTH_SITE.with(|armed| armed.get() == Some(site)) {
            return Err(Error::MemoryBudgetExceeded {
                subsystem: "persistence".into(), operation: "prepare_commit".into(),
                requested_bytes: std::mem::size_of::<content_identity::ContentIdentityMutation>(),
                available_bytes: 0, budget_limit_bytes: 0,
                hint: "Raise MEMORY_LIMIT to admit commit preparation.".into(),
            });
        }
        self.push(mutation)
    }
    pub(super) fn apply(self, persistence: &RedbPersistence,
        groups: Option<&custody_groups::GroupPlan>,
    ) -> Result<()> {
        for ordinal in 0..self.log.next {
            let window = OriginalPersistenceFrame::new(self.frame.original)?;
            let (family, logical_key, logical_value, row_head): EncodedRetirement = {
                let source = self.write.open_table(self.log.table).map_err(RedbPersistence::storage_error)?;
                let value = source.get(ordinal).map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| Error::Other("unpublished schema mutation is missing".into()))?;
                window.decode(value.value())?
            };
            let family = content_identity::LogicalFamily::ALL.into_iter()
                .find(|candidate| *candidate as u8 == family)
                .ok_or_else(|| Error::Other("unpublished schema mutation family is invalid".into()))?;
            let mutation = content_identity::ContentIdentityMutation { family, logical_key, logical_value, row_head };
            RedbPersistence::apply_owned_content_identity_with_manifests_and_groups_in_write(
                self.write, &[mutation], &[],
                &mut content_identity::original_writer::ProofAdmission::new(
                    self.frame.original, persistence.proof_scratch_owner(),
                )?, groups,
            )?;
        }
        self.write.delete_table(self.log.table).map_err(RedbPersistence::storage_error)?;
        // Every custody mutation borrows the same complete prepaid plan.
        // Finish the root bindings even when the logical log was empty.
        RedbPersistence::apply_owned_content_identity_with_manifests_and_groups_in_write(
            self.write, &[], &[],
            &mut content_identity::original_writer::ProofAdmission::new(
                self.frame.original, persistence.proof_scratch_owner(),
            )?, groups,
        )?;
        Ok(())
    }
}

/// Sequential private records live in immediately unlinked writer scratch.
/// Only a fixed header and one encoded/decoded body are resident per visit.
pub(crate) struct ReceivedSchemaRecordStream {
    backend: Box<dyn redb::ScratchBackend>,
    end: u64,
    count: u64,
    _owner: contextdb_vector::VectorCopyAdmission,
}
impl ReceivedSchemaRecordStream {
    fn io(error: std::io::Error) -> Error {
        Error::ScratchWorkingStorageUnavailable {
            operation: "received_vector_projection".into(), io_kind: error.kind(),
        }
    }
    fn open(store: &RedbPersistence, parent: &contextdb_vector::VectorCopyAdmission) -> Result<Self> {
        let mut owner = parent.scratch();
        owner.reserve(redb::scratch_backend_allocation_size()
            .saturating_add(std::mem::size_of::<Self>()))?;
        let writer = store.writer_file.read().map_err(|_| Error::Other("writer ownership lock poisoned".into()))?;
        let writer = writer.as_ref().ok_or_else(|| Self::io(std::io::ErrorKind::Unsupported.into()))?;
        let backend = writer.scratch_backend().map_err(Self::io)?;
        Ok(Self { backend, end: 0, count: 0, _owner: owner })
    }
    pub(crate) fn push<T: serde::Serialize>(&mut self, value: &T) -> Result<()> {
        let mut window = self._owner.scratch();
        let mut measured = OwnedEventEncodedLength(0);
        bincode::serde::encode_into_std_write(value, &mut measured, bincode::config::standard())
            .map_err(|error| Error::Other((format!("bincode encode error: {error}")).into()))?;
        window.reserve(measured.0)?;
        let mut encoded = vec![0u8;measured.0];
        let written = bincode::serde::encode_into_slice(value, encoded.as_mut_slice(), bincode::config::standard())
            .map_err(|error| Error::Other((format!("bincode encode error: {error}")).into()))?;
        if written != measured.0 || encoded.capacity() != measured.0 {
            return Err(Error::Other("received record differs from its measured size".into()));
        }
        let length = u64::try_from(encoded.len()).map_err(|_| Error::Other("received record length overflow".into()))?;
        let end = self.end.checked_add(40).and_then(|end| end.checked_add(length))
            .ok_or_else(|| Error::Other("received record stream overflow".into()))?;
        let count = self.count.checked_add(1).ok_or_else(|| Error::Other("received record count overflow".into()))?;
        self.backend.set_len(end).map_err(Self::io)?;
        self.backend.write(self.end, &length.to_be_bytes()).map_err(Self::io)?;
        self.backend.write(self.end + 8, blake3::hash(&encoded).as_bytes()).map_err(Self::io)?;
        self.backend.write(self.end + 40, &encoded).map_err(Self::io)?;
        self.end = end;
        self.count = count;
        Ok(())
    }
    fn header(&self, offset: u64) -> Result<(usize,u64,[u8;32])> {
        let mut header = [0u8;40];
        if offset.checked_add(40).is_none_or(|end| end > self.end) {
            return Err(Self::io(std::io::ErrorKind::UnexpectedEof.into()));
        }
        self.backend.read(offset,&mut header).map_err(Self::io)?;
        let length = u64::from_be_bytes(header[..8].try_into().unwrap());
        let end = offset.checked_add(40).and_then(|end| end.checked_add(length))
            .filter(|end| *end <= self.end)
            .ok_or_else(|| Self::io(std::io::ErrorKind::UnexpectedEof.into()))?;
        let length = usize::try_from(length).map_err(|_| Error::Other("received record exceeds address space".into()))?;
        Ok((length,end,header[8..].try_into().unwrap()))
    }
    fn body(&self, offset: u64, length: usize, hash: &[u8;32]) -> Result<Vec<u8>> {
        // Caller admits length before entering this allocation door.
        let mut bytes = vec![0u8;length];
        self.backend.read(offset + 40,&mut bytes).map_err(Self::io)?;
        if blake3::hash(&bytes).as_bytes() != hash {
            return Err(Self::io(std::io::ErrorKind::InvalidData.into()));
        }
        Ok(bytes)
    }
    pub(super) fn visit_original<T: serde::de::DeserializeOwned>(&self, original: &OriginalPersistenceFrame<'_>,
        mut visit: impl FnMut(&T) -> Result<()>) -> Result<()> {
        self.visit_original_range(original, (0,self.end,self.count), visit)
    }
    fn visit_original_range<T: serde::de::DeserializeOwned>(&self, original: &OriginalPersistenceFrame<'_>,
        range: (u64,u64,u64), mut visit: impl FnMut(&T) -> Result<()>) -> Result<()> {
        let (mut offset,limit,count) = range;
        for _ in 0..count {
            let window = OriginalPersistenceFrame::new(original.original)?;
            let (length,end,hash) = self.header(offset)?;
            if end > limit { return Err(Self::io(std::io::ErrorKind::InvalidData.into())); }
            window.reserve(length)?;
            let encoded = self.body(offset,length,&hash)?;
            let value: T = window.decode(&encoded)?;
            visit(&value)?;
            offset = end;
        }
        if offset != limit { return Err(Self::io(std::io::ErrorKind::InvalidData.into())); }
        Ok(())
    }
    pub(super) fn visit<T: serde::de::DeserializeOwned>(&self, memory: &Arc<crate::read_image_memory::ImageMemory>,
        mut visit: impl FnMut(&T) -> Result<()>) -> Result<()> {
        self.visit_range(memory, (0,self.end,self.count), visit)
    }
    fn visit_range<T: serde::de::DeserializeOwned>(&self, memory: &Arc<crate::read_image_memory::ImageMemory>,
        range: (u64,u64,u64), mut visit: impl FnMut(&T) -> Result<()>) -> Result<()> {
        let (mut offset,limit,count) = range;
        for _ in 0..count {
            let mut window = self._owner.scratch();
            let (length,end,hash) = self.header(offset)?;
            if end > limit { return Err(Self::io(std::io::ErrorKind::InvalidData.into())); }
            window.reserve(length.saturating_mul(8).saturating_add(std::mem::size_of::<T>()))?;
            let encoded = self.body(offset,length,&hash)?;
            memory.scope(|| {
                let value: T = RedbPersistence::decode_config_value_exact(&encoded)?;
                visit(&value)
            })?;
            offset = end;
        }
        if offset != limit { return Err(Self::io(std::io::ErrorKind::InvalidData.into())); }
        Ok(())
    }
}
pub(crate) struct ReceivedVectorRecords {
    pub(crate) bodies: ReceivedSchemaRecordStream,
    pub(crate) memberships: ReceivedSchemaRecordStream,
}
impl RedbPersistence {
    pub(crate) fn prepare_received_vector_records(&self, owner: &contextdb_vector::VectorCopyAdmission) -> Result<ReceivedVectorRecords> {
        Ok(ReceivedVectorRecords {
            bodies: ReceivedSchemaRecordStream::open(self,owner)?,
            memberships: ReceivedSchemaRecordStream::open(self,owner)?,
        })
    }
}

/// One range per declared table, never one heap entry per row. Each range
/// preserves the prepared (row, transaction, LSN) order for index folding.
pub(crate) struct ReceivedRowRecords {
    stream: ReceivedSchemaRecordStream,
    tables: HashMap<String,(u64,u64,u64)>,
    _owner: contextdb_vector::VectorCopyAdmission,
}
impl ReceivedRowRecords {
    pub(crate) fn push_table(&mut self, table: &str, rows: &[VersionedRow]) -> Result<()> {
        self.push_table_with(table, |push| {
            for row in rows { push(row)?; }
            Ok(())
        })
    }
    /// The producer lends a single admitted body at a time. No body or
    /// callback is retained after this call; only the existing table range is.
    pub(crate) fn push_table_with(&mut self, table: &str,
        produce: impl FnOnce(&mut dyn FnMut(&VersionedRow) -> Result<()>) -> Result<()>) -> Result<()> {
        if self.tables.contains_key(table) { return Err(Error::Other("received row table repeated".into())); }
        self._owner.reserve(table.len().saturating_add(std::mem::size_of::<(String,(u64,u64,u64))>().saturating_mul(4)))?;
        self.tables.reserve(1);
        let start = self.stream.end;
        let count = self.stream.count;
        produce(&mut |row| {
            // Value::Json serializes through a temporary string even during
            // sizing. Keep the existing concrete record-work admission.
            let mut window = self._owner.scratch();
            let (json_scratch, _) = crate::memory_accounting::preparation_json_work_bytes(row.values.values())?;
            window.reserve(row.estimated_bytes().saturating_mul(8).max(json_scratch))?;
            self.stream.push(row)
        })?;
        self.tables.insert(table.to_owned(),(start,self.stream.end,self.stream.count - count));
        Ok(())
    }
    pub(super) fn visit_table_original(&self, original: &OriginalPersistenceFrame<'_>, table: &str,
        visit: impl FnMut(&VersionedRow) -> Result<()>) -> Result<()> {
        match self.tables.get(table) {
            Some(range) => self.stream.visit_original_range(original,*range,visit), None => Ok(()),
        }
    }
    pub(super) fn visit_original(&self, original: &OriginalPersistenceFrame<'_>,
        mut visit: impl FnMut(&str,&VersionedRow) -> Result<()>) -> Result<()> {
        for (table,range) in &self.tables {
            self.stream.visit_original_range(original,*range,|row| visit(table,row))?;
        }
        Ok(())
    }
    pub(super) fn visit(&self, memory: &Arc<crate::read_image_memory::ImageMemory>,
        mut visit: impl FnMut(&str,&VersionedRow) -> Result<()>) -> Result<()> {
        for (table,range) in &self.tables { self.stream.visit_range(memory,*range,|row| visit(table,row))?; }
        Ok(())
    }
}
impl RedbPersistence {
    pub(crate) fn prepare_received_row_records(&self, owner: &contextdb_vector::VectorCopyAdmission) -> Result<ReceivedRowRecords> {
        let mut directory = owner.scratch();
        directory.reserve(std::mem::size_of::<ReceivedRowRecords>())?;
        Ok(ReceivedRowRecords { stream: ReceivedSchemaRecordStream::open(self,owner)?, tables: HashMap::new(), _owner: directory })
    }
}

pub(crate) struct ReceivedSyncSourceRecords { stream: ReceivedSchemaRecordStream }
impl ReceivedSyncSourceRecords {
    pub(crate) fn push(&mut self, table: &str, row: RowId, lsn: Lsn, kind: SyncSourceKind) -> Result<()> {
        self.stream.push(&(table,row,lsn,sync_source_kind_to_u8(kind)))
    }
    pub(super) fn visit_original(&self, frame: &OriginalPersistenceFrame<'_>,
        mut visit: impl FnMut(&str,RowId,Lsn,u8) -> Result<()>) -> Result<()> {
        self.stream.visit_original(frame,|record: &(String,RowId,Lsn,u8)| {
            if record.3 > 2 { return Err(Error::Other("received source kind is invalid".into())); }
            visit(&record.0,record.1,record.2,record.3)
        })
    }
    pub(super) fn visit(&self, memory: &Arc<crate::read_image_memory::ImageMemory>,
        mut visit: impl FnMut(&str,RowId,Lsn,u8) -> Result<()>) -> Result<()> {
        self.stream.visit(memory,|record: &(String,RowId,Lsn,u8)| {
            if record.3 > 2 { return Err(Error::Other("received source kind is invalid".into())); }
            visit(&record.0,record.1,record.2,record.3)
        })
    }
}
impl RedbPersistence {
    pub(crate) fn prepare_received_sync_source_records(&self, owner: &contextdb_vector::VectorCopyAdmission) -> Result<ReceivedSyncSourceRecords> {
        Ok(ReceivedSyncSourceRecords { stream: ReceivedSchemaRecordStream::open(self,owner)? })
    }
}

// One private latest-version directory at a time. The payload is only an
// ordinal into the already funded stage; identities and ranks remain on disk.
const RECEIVED_LATEST_RECORDS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("__received_schema_latest_records");
pub(super) struct ReceivedLatestRecords;
impl ReceivedLatestRecords {
    pub(super) fn new(write: &redb::WriteTransaction) -> Result<Self> {
        let table = write.open_table(RECEIVED_LATEST_RECORDS).map_err(RedbPersistence::storage_error)?;
        if !table.is_empty().map_err(RedbPersistence::storage_error)? {
            return Err(Error::Other("unpublished latest-version directory survived a transaction".into()));
        }
        Ok(Self)
    }
    pub(super) fn consider(&self, persistence: &RedbPersistence,
        write: &redb::WriteTransaction, key: &[u8], lsn: Lsn, tx: TxId, index: usize,
    ) -> Result<()> {
        let mut window = persistence.proof_admission();
        window.admit(key.len().saturating_mul(4).saturating_add(512))?;
        let mut table = write.open_table(RECEIVED_LATEST_RECORDS).map_err(RedbPersistence::storage_error)?;
        let previous = table.get(key).map_err(RedbPersistence::storage_error)?
            .map(|value| Self::decode(value.value())).transpose()?;
        if previous.is_some_and(|(seen_lsn, seen_tx, _)| (seen_lsn, seen_tx) >= (lsn.0, tx.0)) {
            return Ok(());
        }
        let index = u64::try_from(index).map_err(|_| Error::Other("schema record ordinal overflow".into()))?;
        let mut value = [0u8; 24];
        value[..8].copy_from_slice(&lsn.0.to_be_bytes());
        value[8..16].copy_from_slice(&tx.0.to_be_bytes());
        value[16..].copy_from_slice(&index.to_be_bytes());
        table.insert(key, value.as_slice()).map_err(RedbPersistence::storage_error)?;
        Ok(())
    }
    pub(super) fn consider_original(&self, original: &OriginalPersistenceFrame<'_>,
        write: &redb::WriteTransaction, key: &[u8], lsn: Lsn, tx: TxId, index: usize,
    ) -> Result<()> {
        let window = OriginalPersistenceFrame::new(original.original)?;
        window.reserve(key.len().saturating_mul(4).saturating_add(512))?;
        let mut table = write.open_table(RECEIVED_LATEST_RECORDS).map_err(RedbPersistence::storage_error)?;
        let previous = table.get(key).map_err(RedbPersistence::storage_error)?
            .map(|value| Self::decode(value.value())).transpose()?;
        if previous.is_some_and(|(seen_lsn, seen_tx, _)| (seen_lsn, seen_tx) >= (lsn.0, tx.0)) {
            return Ok(());
        }
        let index = u64::try_from(index).map_err(|_| Error::Other("schema record ordinal overflow".into()))?;
        let mut value = [0u8; 24];
        value[..8].copy_from_slice(&lsn.0.to_be_bytes());
        value[8..16].copy_from_slice(&tx.0.to_be_bytes());
        value[16..].copy_from_slice(&index.to_be_bytes());
        table.insert(key, value.as_slice()).map_err(RedbPersistence::storage_error)?;
        Ok(())
    }
    fn decode(value: &[u8]) -> Result<(u64, u64, u64)> {
        if value.len() != 24 {
            return Err(Error::Other("unpublished latest-version rank has invalid width".into()));
        }
        Ok((u64::from_be_bytes(value[..8].try_into().unwrap()),
            u64::from_be_bytes(value[8..16].try_into().unwrap()),
            u64::from_be_bytes(value[16..].try_into().unwrap())))
    }
    pub(super) fn visit(self, persistence: &RedbPersistence, write: &redb::WriteTransaction,
        mut visitor: impl FnMut(usize) -> Result<()>,
    ) -> Result<()> {
        let mut coordinate_credit = persistence.proof_admission();
        let mut coordinate_bytes = 0usize;
        let mut after: Option<Vec<u8>> = None;
        loop {
            let mut window = persistence.proof_admission();
            let selected = {
                let table = write.open_table(RECEIVED_LATEST_RECORDS).map_err(RedbPersistence::storage_error)?;
                let lower = after.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
                let mut cursor = table.range::<&[u8]>((lower, Bound::Unbounded)).map_err(RedbPersistence::storage_error)?;
                cursor.next().transpose().map_err(RedbPersistence::storage_error)?.map(|(key, value)| {
                    let required = key.value().len().saturating_mul(2).saturating_add(128);
                    if required > coordinate_bytes {
                        coordinate_credit.admit(required - coordinate_bytes)?;
                        coordinate_bytes = required;
                    }
                    window.admit(key.value().len().saturating_add(256))?;
                    let (_, _, index) = Self::decode(value.value())?;
                    let index = usize::try_from(index).map_err(|_| Error::Other("schema record ordinal exceeds address space".into()))?;
                    Ok::<_, Error>((key.value().to_vec(), index))
                }).transpose()?
            };
            let Some((key, index)) = selected else { break; };
            // End all native handles before the callback opens proof tables.
            visitor(index)?;
            after = Some(key);
        }
        write.delete_table(RECEIVED_LATEST_RECORDS).map_err(RedbPersistence::storage_error)?;
        Ok(())
    }    pub(super) fn visit_original(self, original: &OriginalPersistenceFrame<'_>, write: &redb::WriteTransaction,
        mut visitor: impl FnMut(usize) -> Result<()>,
    ) -> Result<()> {
        let coordinate_credit = OriginalPersistenceFrame::new(original.original)?;
        let mut coordinate_bytes = 0usize;
        let mut after: Option<Vec<u8>> = None;
        loop {
            let window = OriginalPersistenceFrame::new(original.original)?;
            let selected = {
                let table = write.open_table(RECEIVED_LATEST_RECORDS).map_err(RedbPersistence::storage_error)?;
                let lower = after.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
                let mut cursor = table.range::<&[u8]>((lower, Bound::Unbounded)).map_err(RedbPersistence::storage_error)?;
                cursor.next().transpose().map_err(RedbPersistence::storage_error)?.map(|(key, value)| {
                    let required = key.value().len().saturating_mul(2).saturating_add(128);
                    if required > coordinate_bytes {
                        coordinate_credit.reserve(required - coordinate_bytes)?;
                        coordinate_bytes = required;
                    }
                    window.reserve(key.value().len().saturating_add(256))?;
                    let (_, _, index) = Self::decode(value.value())?;
                    let index = usize::try_from(index).map_err(|_| Error::Other("schema record ordinal exceeds address space".into()))?;
                    Ok::<_, Error>((key.value().to_vec(), index))
                }).transpose()?
            };
            let Some((key, index)) = selected else { break; };
            // End all native handles before the callback opens proof tables.
            visitor(index)?;
            after = Some(key);
        }
        write.delete_table(RECEIVED_LATEST_RECORDS).map_err(RedbPersistence::storage_error)?;
        Ok(())
    }
}

/// The native identity fold retains the prepared mutation itself on disk.
/// It never leaves a row/vector array offset for a later consumer to resolve.
/// Equal ranks keep the first input, including a deleted latest record.
pub(super) struct ReceivedLatestMutations;
impl ReceivedLatestMutations {
    pub(super) fn new(write: &redb::WriteTransaction) -> Result<Self> {
        ReceivedLatestRecords::new(write)?;
        Ok(Self)
    }
    pub(super) fn consider(&self, original: &OriginalPersistenceFrame<'_>,
        write: &redb::WriteTransaction, key: &[u8], lsn: Lsn, tx: TxId,
        mutation: Option<&content_identity::ContentIdentityMutation>,
    ) -> Result<()> {
        let window = OriginalPersistenceFrame::new(original.original)?;
        window.reserve(key.len().saturating_mul(4).saturating_add(256))?;
        let mut table = write.open_table(RECEIVED_LATEST_RECORDS).map_err(RedbPersistence::storage_error)?;
        {
            let previous = table.get(key).map_err(RedbPersistence::storage_error)?;
            if let Some(previous) = previous {
                let bytes = previous.value();
                if bytes.len() < 16 {
                    return Err(Error::Other("unpublished latest mutation has invalid rank".into()));
                }
                let rank = (u64::from_be_bytes(bytes[..8].try_into().unwrap()),
                    u64::from_be_bytes(bytes[8..16].try_into().unwrap()));
                if rank >= (lsn.0, tx.0) { return Ok(()); }
            }
        }
        let payload = mutation.map(|m| (m.family as u8, &m.logical_key, &m.logical_value, m.row_head));
        let encoded = window.encode(&payload)?;
        let length = encoded.len().checked_add(16)
            .ok_or_else(|| Error::Other("unpublished latest mutation size overflow".into()))?;
        window.reserve(length)?;
        let mut value = Vec::with_capacity(length);
        value.extend_from_slice(&lsn.0.to_be_bytes());
        value.extend_from_slice(&tx.0.to_be_bytes());
        value.extend_from_slice(&encoded);
        table.insert(key, value.as_slice()).map_err(RedbPersistence::storage_error)?;
        Ok(())
    }
    pub(super) fn visit(self, original: &OriginalPersistenceFrame<'_>, write: &redb::WriteTransaction,
        mut visitor: impl FnMut(content_identity::ContentIdentityMutation) -> Result<()>,
    ) -> Result<()> {
        let coordinate_credit = OriginalPersistenceFrame::new(original.original)?;
        let mut coordinate_bytes = 0usize;
        let mut after: Option<Vec<u8>> = None;
        loop {
            let window = OriginalPersistenceFrame::new(original.original)?;
            let selected = {
                let table = write.open_table(RECEIVED_LATEST_RECORDS).map_err(RedbPersistence::storage_error)?;
                let lower = after.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
                let mut cursor = table.range::<&[u8]>((lower, Bound::Unbounded)).map_err(RedbPersistence::storage_error)?;
                cursor.next().transpose().map_err(RedbPersistence::storage_error)?.map(|(key, value)| {
                    let required = key.value().len().saturating_mul(2).saturating_add(128);
                    if required > coordinate_bytes {
                        coordinate_credit.reserve(required - coordinate_bytes)?;
                        coordinate_bytes = required;
                    }
                    let bytes = value.value();
                    if bytes.len() < 16 {
                        return Err(Error::Other("unpublished latest mutation has invalid rank".into()));
                    }
                    let payload: Option<EncodedRetirement> = window.decode(&bytes[16..])?;
                    Ok::<_, Error>((key.value().to_vec(), payload))
                }).transpose()?
            };
            let Some((key, payload)) = selected else { break; };
            // Native handles end before proof tables are opened. The complete
            // decoded payload stays under this actual-original record window.
            if let Some((family, logical_key, logical_value, row_head)) = payload {
                let family = content_identity::LogicalFamily::ALL.into_iter()
                    .find(|candidate| *candidate as u8 == family)
                    .ok_or_else(|| Error::Other("unpublished latest mutation family is invalid".into()))?;
                visitor(content_identity::ContentIdentityMutation { family, logical_key, logical_value, row_head })?;
            }
            after = Some(key);
        }
        write.delete_table(RECEIVED_LATEST_RECORDS).map_err(RedbPersistence::storage_error)?;
        Ok(())
    }
}

impl RedbPersistence {
    pub(super) fn erase_received_graph_adjacencies_in_write(
        &self, write: &redb::WriteTransaction, selected_types: &HashSet<String>,
    ) -> Result<()> {
        for (definition, probe, context) in [
            (GRAPH_FWD_TABLE, "graph_fwd", "received_schema_forward_selection"),
            (GRAPH_REV_TABLE, "graph_rev", "received_schema_reverse_selection"),
        ] {
            let mut coordinate_credit = self.proof_admission();
            let mut coordinate_bytes = 0usize;
            let mut after: Option<Vec<u8>> = None;
            loop {
                let mut window = self.proof_admission();
                let row_memory = crate::read_image_memory::ImageMemory::operation_writer(
                    self.require_decode_accountant()?,
                )?;
                let selected = row_memory.scope(|| {
                    let source = write.open_table(definition).map_err(Self::storage_error)?;
                    let lower = after.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
                    let mut cursor = source.range::<&[u8]>((lower, Bound::Unbounded)).map_err(Self::storage_error)?;
                    cursor.next().transpose().map_err(Self::storage_error)?.map(|(key, value)| {
                        #[cfg(any(test, feature = "test-seams"))]
                        crate::read_work_probe::graph_storage_key(probe, "iteration_step", key.value());
                        #[cfg(any(test, feature = "test-seams"))]
                        crate::read_work_probe::note_graph("graph_payload", "payload_decode",
                            key.value(), None, Some(context));
                        let required = key.value().len().saturating_mul(2).saturating_add(128);
                        if required > coordinate_bytes {
                            coordinate_credit.admit(required - coordinate_bytes)?;
                            coordinate_bytes = required;
                        }
                        window.admit(key.value().len().saturating_mul(2)
                            .saturating_add(value.value().len().saturating_mul(8)).saturating_add(1024))?;
                        let edge = Self::decode::<AdjEntry>(value.value())?;
                        Ok::<_, Error>((key.value().to_vec(), selected_types.contains(&edge.edge_type)))
                    }).transpose()
                })?;
                let Some((key, remove)) = selected else { break; };
                if remove {
                    write.open_table(definition).map_err(Self::storage_error)?
                        .remove(observed_graph_key!(probe, "remove", key.as_slice(), 5))
                        .map_err(Self::storage_error)?;
                }
                after = Some(key);
            }
        }
        Ok(())
    }

    pub(super) fn erase_received_graph_adjacencies_in_write_original(
        &self, original: &OriginalPersistenceFrame<'_>, write: &redb::WriteTransaction, selected_types: &HashSet<String>,
        working: Option<(&received_working::ReceivedWorkingWrite, Option<&received_working_graph::WorkingGraphCapture>)>,
    ) -> Result<()> {
        for (definition, probe, context) in [
            (GRAPH_FWD_TABLE, "graph_fwd", "received_schema_forward_selection"),
            (GRAPH_REV_TABLE, "graph_rev", "received_schema_reverse_selection"),
        ] {
            let coordinate_credit = OriginalPersistenceFrame::new(original.original)?;
            let mut coordinate_bytes = 0usize;
            let mut after: Option<Vec<u8>> = None;
            loop {
                let window = OriginalPersistenceFrame::new(original.original)?;
                let selected = (|| {
                    let source = write.open_table(definition).map_err(Self::storage_error)?;
                    let lower = after.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
                    let mut cursor = source.range::<&[u8]>((lower, Bound::Unbounded)).map_err(Self::storage_error)?;
                    cursor.next().transpose().map_err(Self::storage_error)?.map(|(key, value)| {
                        #[cfg(any(test, feature = "test-seams"))]
                        crate::read_work_probe::graph_storage_key(probe, "iteration_step", key.value());
                        #[cfg(any(test, feature = "test-seams"))]
                        crate::read_work_probe::note_graph("graph_payload", "payload_decode",
                            key.value(), None, Some(context));
                        let required = key.value().len().saturating_mul(2).saturating_add(128);
                        if required > coordinate_bytes {
                            coordinate_credit.reserve(required - coordinate_bytes)?;
                            coordinate_bytes = required;
                        }
                        window.reserve(key.value().len().saturating_mul(2)
                            .saturating_add(value.value().len().saturating_mul(8)).saturating_add(1024))?;
                        let edge = window.decode::<AdjEntry>(value.value())?;
                        Ok::<_, Error>((key.value().to_vec(), if let Some((working, Some(selection))) = working {
                            working.selected_graph_type_original(original.original, selection, &edge.edge_type)?
                        } else { selected_types.contains(&edge.edge_type) }))
                    }).transpose()
                })()?;
                let Some((key, remove)) = selected else { break; };
                if remove {
                    write.open_table(definition).map_err(Self::storage_error)?
                        .remove(observed_graph_key!(probe, "remove", key.as_slice(), 5))
                        .map_err(Self::storage_error)?;
                }
                after = Some(key);
            }
        }
        Ok(())
    }

    pub(super) fn erase_received_index_counts_original(
        &self, original: &OriginalPersistenceFrame<'_>, write: &redb::WriteTransaction,
        table: &str, mut retire: impl FnMut(content_identity::ContentIdentityMutation) -> Result<()>,
    ) -> Result<()> {
        let cursor_credit = OriginalPersistenceFrame::new(original.original)?;
        let mut coordinate_bytes = 0usize;
        let mut after: Option<Vec<u8>> = None;
        loop {
            let window = OriginalPersistenceFrame::new(original.original)?;
            let selected = {
                let source = write.open_table(RELATIONAL_INDEX_COUNT_TABLE).map_err(Self::storage_error)?;
                let lower = after.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
                let mut cursor = source.range::<&[u8]>((lower, Bound::Unbounded)).map_err(Self::storage_error)?;
                cursor.next().transpose().map_err(Self::storage_error)?.map(|(key, _)| {
                    let required = key.value().len().saturating_mul(2).saturating_add(128);
                    if required > coordinate_bytes {
                        cursor_credit.reserve(required - coordinate_bytes)?;
                        coordinate_bytes = required;
                    }
                    window.reserve(key.value().len().saturating_mul(8).saturating_add(1024))?;
                    let decoded: EncodedRelationalIndexCountKey = window.decode(key.value())?;
                    Ok::<_, Error>((key.value().to_vec(), decoded.table == table))
                }).transpose()?
            };
            let Some((key, remove)) = selected else { break; };
            if remove {
                write.open_table(RELATIONAL_INDEX_COUNT_TABLE).map_err(Self::storage_error)?
                    .remove(key.as_slice()).map_err(Self::storage_error)?;
                retire(Self::remove_relational_index_count_mutation(&key))?;
            }
            after = Some(key);
        }
        Ok(())
    }

    pub(super) fn erase_received_retention_expiries_original(
        &self, original: &OriginalPersistenceFrame<'_>, write: &redb::WriteTransaction,
        table: &str, mutations: &mut OriginalMutationLog<'_, '_, '_>,
    ) -> Result<()> {
        let cursor_credit = OriginalPersistenceFrame::new(original.original)?;
        cursor_credit.reserve(table.len().saturating_add(128).saturating_mul(4))?;
        let prefix = format!("{}{}:{table}:", crate::composite_store::RETENTION_EXPIRY_PREFIX, table.len());
        let mut coordinate_bytes = 0usize;
        let mut after: Option<String> = None;
        loop {
            let window = OriginalPersistenceFrame::new(original.original)?;
            let selected = {
                let source = write.open_table(CONFIG_TABLE).map_err(Self::storage_error)?;
                let lower = after.as_deref().map_or(Bound::Included(prefix.as_str()), Bound::Excluded);
                let mut cursor = source.range::<&str>((lower, Bound::Unbounded)).map_err(Self::storage_error)?;
                cursor.next().transpose().map_err(Self::storage_error)?.filter(|(key, _)| key.value().starts_with(&prefix))
                    .map(|(key, _)| {
                        let required = key.value().len().saturating_mul(2).saturating_add(128);
                        if required > coordinate_bytes {
                            cursor_credit.reserve(required - coordinate_bytes)?;
                            coordinate_bytes = required;
                        }
                        window.copy_text(key.value())
                    }).transpose()?
            };
            let Some(key) = selected else { break; };
            write.open_table(CONFIG_TABLE).map_err(Self::storage_error)?
                .remove(key.as_str()).map_err(Self::storage_error)?;
            mutations.extend(content_identity::original_producers::config_mutations(&window, &key, None)?)?;
            after = Some(key);
        }
        Ok(())
    }

    pub(super) fn retire_index_posting_identities_in_write(
        &self, write: &redb::WriteTransaction, table: &str, declaration: &IndexDecl,
        retired: &mut ReceivedRetirementLog,
    ) -> Result<()> {
        let mut cursor_credit = self.proof_admission();
        cursor_credit.admit(table.len().saturating_add(declaration.name.len()).saturating_mul(4)
            .saturating_add(256))?;
        let name = Self::relational_index_table_name(table, &declaration.name);
        let definition = TableDefinition::<DurableRelationalIndexKey, &[u8]>::new(&name);
        let mut after: Option<DurableRelationalIndexKey> = None;
        let mut cursor_bytes = 0usize;
        loop {
            let mut window = self.proof_admission();
            let row_memory = crate::read_image_memory::ImageMemory::operation_writer(
                self.require_decode_accountant()?,
            )?;
            let selected = row_memory.scope(|| {
                let source = match write.open_table(definition) {
                    Ok(source) => source,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                    Err(error) => return Err(Self::storage_error(error)),
                };
                let lower = after.as_ref().map_or(Bound::Unbounded, Bound::Excluded);
                let mut cursor = source.range::<&DurableRelationalIndexKey>((lower, Bound::Unbounded)).map_err(Self::storage_error)?;
                let Some((key, value)) = cursor.next().transpose().map_err(Self::storage_error)? else {
                    return Ok(None);
                };
                // This native key representation still owns from_bytes output.
                // Quote its backing before asking for that first owned value;
                // subsequent decoding is priced from the actual encoded width.
                window.admit(key.retained_page_bytes())?;
                let key_bytes = key.value().0.len();
                window.admit(key_bytes.saturating_mul(16)
                    .saturating_add(value.value().len().saturating_mul(8)).saturating_add(1024))?;
                let (coordinate, position, deleted) = decode_relational_index_entry(
                    &self.path, Some((key, value)),
                )?.expect("an existing posting decodes");
                let mutation = if deleted.is_none() {
                    let logical = DurableRelationalIndexKey::new(position.key, position.row_id, TxId(0))?;
                    Some(content_identity::remove_relational_index_posting_mutation(
                        table, &declaration.name, &logical.0,
                    ))
                } else { None };
                Ok(Some((coordinate, mutation)))
            })?;
            let Some((coordinate, mutation)) = selected else { break; };
            let required = coordinate.len().saturating_mul(2).saturating_add(128);
            if required > cursor_bytes {
                cursor_credit.admit(required - cursor_bytes)?;
                cursor_bytes = required;
            }
            if let Some(mutation) = mutation {
                retired.append(self, write, &[mutation])?;
            }
            after = Some(DurableRelationalIndexKey(coordinate));
        }
        Ok(())
    }

    pub(super) fn erase_table_index_counts_in_write(
        &self, write: &redb::WriteTransaction, table: &str,
        retired: &mut ReceivedRetirementLog,
    ) -> Result<()> {
        let mut cursor_credit = self.proof_admission();
        let mut cursor_bytes = 0usize;
        let mut after: Option<Vec<u8>> = None;
        loop {
            let mut window = self.proof_admission();
            let row_memory = crate::read_image_memory::ImageMemory::operation_writer(
                self.require_decode_accountant()?,
            )?;
            let selected = row_memory.scope(|| {
                let source = write.open_table(RELATIONAL_INDEX_COUNT_TABLE).map_err(Self::storage_error)?;
                let lower = after.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
                let mut cursor = source.range::<&[u8]>((lower, Bound::Unbounded)).map_err(Self::storage_error)?;
                cursor.next().transpose().map_err(Self::storage_error)?.map(|(key, _)| {
                    window.admit(key.value().len().saturating_mul(16).saturating_add(1024))?;
                    let decoded: EncodedRelationalIndexCountKey = Self::decode(key.value())?;
                    Ok::<_, Error>((key.value().to_vec(), decoded.table == table))
                }).transpose()
            })?;
            let Some((key, remove)) = selected else { break; };
            let required = key.len().saturating_mul(2).saturating_add(128);
            if required > cursor_bytes {
                cursor_credit.admit(required - cursor_bytes)?;
                cursor_bytes = required;
            }
            if remove {
                {
                    let mut source = write.open_table(RELATIONAL_INDEX_COUNT_TABLE).map_err(Self::storage_error)?;
                    source.remove(key.as_slice()).map_err(Self::storage_error)?;
                }
                retired.append(self, write, &[Self::remove_relational_index_count_mutation(&key)])?;
            }
            after = Some(key);
        }
        Ok(())
    }

    /// A schema rewrite owns only its addressed declaration. Its source bytes
    /// and decoded copy stay funded by the original writer through the caller's
    /// complete mutation, rather than borrowing a later decoder's call owner.
    pub(super) fn schema_projection_metadata_point(
        &self, name: &str,
    ) -> Result<Option<(TableMeta, content_identity::OwnedProofAdmission)>> {
        let mut credit = self.proof_admission();
        credit.admit(name.len().saturating_mul(2).saturating_add(256))?;
        let meta = self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let metadata = read.open_table(META_TABLE).map_err(Self::storage_error)?;
            metadata.get(Self::meta_key(name).as_str()).map_err(Self::storage_error)?
                .map(|encoded| {
                    credit.admit(encoded.value().len().saturating_mul(64).saturating_add(1024))?;
                    let (meta, legacy) = Self::decode_table_meta_versioned(encoded.value())?;
                    if legacy {
                        self.used_legacy_table_meta_layout.store(true, Ordering::Relaxed);
                    }
                    Ok(meta)
                }).transpose()
        })?;
        Ok(meta.map(|meta| (meta, credit)))
    }

    // The committed source, rather than the already-mutated memory catalog,
    // decides whether a direct received ALTER can use the bounded cursor.
    fn scalar_schema_projection_source(
        &self, name: &str, meta: &TableMeta,
    ) -> Result<Option<(TableMeta, content_identity::OwnedProofAdmission)>> {
        let Some(source) = self.schema_projection_metadata_point(name)? else {
            return Ok(None);
        };
        let previous = &source.0;
        // A dimension/quantization replacement retires the old vector
        // lifetime, as the ordinary received ALTER adjudicator already does.
        // Repartitioning and vector-to-scalar conversion still require their
        // existing replacement paths; this cursor does not invent a conversion.
        if previous.columns.iter().filter(|column|matches!(column.column_type,ColumnType::Vector(_)))
            .any(|old|meta.columns.iter().find(|new|new.name == old.name)
                .is_some_and(|new|!matches!(new.column_type,ColumnType::Vector(_))
                    || old.partition_key_columns != new.partition_key_columns)) { return Ok(None); }
        Ok(Some(source))
    }

    pub(crate) fn supports_streamed_schema_projection(
        &self, name: &str, meta: &TableMeta,
    ) -> Result<bool> {
        let _decode = self.writer_decode_scope()?;
        Ok(self.scalar_schema_projection_source(name, meta)?.is_some())
    }

    pub(crate) fn stream_scalar_schema_projection(
        &self,
        name: &str,
        meta: &TableMeta,
        lsn: Lsn,
        ddl: &[DdlChange],
        config_values: &[(&str, Vec<u8>)],
    ) -> Result<bool> {
        let _decode = self.writer_decode_scope()?;
        let Some(source) = self.scalar_schema_projection_source(name, meta)? else {
            return Ok(false);
        };
        // Keep the payload/credit tuple intact: its declaration drops before
        // its original writer credit on success, refusal and unwind.
        let previous = &source.0;
        let mut scratch = self.proof_admission();
        scratch.admit(
            meta.estimated_bytes()
                .saturating_add(previous.estimated_bytes())
                .saturating_mul(2),
        )?;
        let renames = ddl
            .iter()
            .filter_map(|change| {
                if let DdlChange::AlterTable {
                    name: table,
                    constraints,
                    ..
                } = change
                {
                    (table == name)
                        .then(|| crate::database::sync_column_renames_from_constraints(constraints))
                } else {
                    None
                }
            })
            .flatten()
            .collect::<Vec<_>>();
        let vector_renames = ddl.iter().filter_map(|change| {
            if let DdlChange::AlterTable { name: table,constraints,.. } = change {
                (table == name).then(|| crate::database::sync_vector_rename_from_constraints(constraints)).flatten()
            } else { None }
        }).map(|(from,to)|(VectorIndexRef::new(name,from),VectorIndexRef::new(name,to))).collect::<Vec<_>>();
        let mut dropped_vectors = previous.columns.iter().filter(|old|matches!(old.column_type,ColumnType::Vector(_))
            && !meta.columns.iter().any(|new|new.name == old.name)
            && !vector_renames.iter().any(|(from,_)|from.column == old.name))
            .map(|column|column.name.clone()).collect::<HashSet<_>>();
        let replaced = |old: &&contextdb_core::ColumnDef| {
            matches!(old.column_type, ColumnType::Vector(_))
                && meta.columns.iter().find(|new| new.name == old.name)
                    .is_some_and(|new| old.column_type != new.column_type
                        || old.quantization != new.quantization)
        };
        scratch.admit(previous.columns.iter().filter(replaced).fold(0usize, |bytes, old| {
            bytes.saturating_add(old.name.len().saturating_mul(2))
                .saturating_add(256)
        }))?;
        let replaced_vectors = previous.columns.iter().filter(replaced)
            .map(|column| column.name.as_str()).collect::<Vec<_>>();
        dropped_vectors.extend(replaced_vectors.iter().map(|name| (*name).to_owned()));
        let vector_quantization = Self::vector_quantization_map(&HashMap::from([(name.to_owned(),meta.clone())]));
        let schema =
            self.prepare_writer_schema_change(std::iter::empty(), std::iter::once((name, meta)))?;
        self.with_db(|db| {
            let write = Self::begin_history_write(db)?;
            if crate::database::has_graph_edge_table_shape(previous)
                && !crate::database::has_graph_edge_table_shape(meta) {
                self.graph_writer_window(|| self.retire_row_graph_table_in_write(&write, name, None))?;
            }
            let physical = Self::rel_table_name(name);
            let definition = TableDefinition::<&[u8], &[u8]>::new(&physical);
            let temporary = TableDefinition::<&[u8], &[u8]>::new("__schema_projection_rows");
            self.retain_authored_rows_in_write(&write, name)?;
            Self::invalidate_retention_clock_in_write(&write, name)?;
            {
                let output = write.open_table(temporary).map_err(Self::storage_error)?;
                if !output.is_empty().map_err(Self::storage_error)? {
                    return Err(Error::StoreCorrupted {
                        path: name.into(),
                        reason: "unpublished schema rows survived a transaction".into(),
                    });
                }
            }
            self.visit_relational_rows_in_write(&write, name, |source_row| {
                let mut window = self.proof_admission();
                window.admit(source_row.estimated_bytes().saturating_mul(8)
                    .saturating_add(meta.estimated_bytes()).saturating_add(1024))?;
                let key = Self::rel_row_key(source_row);
                let mut row = source_row.clone();
                for (from, to) in renames.iter().map(|(from,to)|(from,to))
                    .chain(vector_renames.iter().map(|(from,to)|(&from.column,&to.column))) {
                    if let Some(value) = row.values.remove(from) {
                        row.values.insert(to.clone(), value);
                    }
                }
                row.values.retain(|column, _| {
                    meta.columns.iter().any(|declared| declared.name == *column)
                });
                for column in &replaced_vectors {
                    // The old body stays in authored custody; its projection
                    // cannot masquerade as a value of the new storage shape.
                    row.values.insert((*column).to_owned(), Value::Null);
                }
                let encoded = Self::encode_versioned_row(&row, Some(meta))?;
                let mut output = write.open_table(temporary).map_err(Self::storage_error)?;
                output.insert(key.as_slice(), encoded.as_slice()).map_err(Self::storage_error)?;
                Ok(())
            })?;
            if !dropped_vectors.is_empty() {
                self.remove_vector_partition_records_matching_all_in_write(&write,name,Some(&dropped_vectors),
                    &|index:&VectorIndexRef|index.table == name && dropped_vectors.contains(&index.column))?;
            }
            self.rewrite_vector_entries_for_schema_in_write(&write,name,&vector_renames,&dropped_vectors,&vector_quantization)?;
            self.retire_relational_manifests_in_write(&write, name)?;
            content_size::reset_rows(&write, name)?;
            write
                .delete_table(definition)
                .map_err(Self::storage_error)?;
            {
                let mut metadata = write.open_table(META_TABLE).map_err(Self::storage_error)?;
                let encoded = self.schema_update_in_table(&metadata, name, meta, ddl)?;
                metadata
                    .insert(Self::meta_key(name).as_str(), encoded.bytes.as_slice())
                    .map_err(Self::storage_error)?;
            }
            for (from,to) in &vector_renames {
                self.rename_vector_partition_generation_index_in_write(&write,from,to)?;
            }
            vector_membership_pages::ensure_declared_routes(&write,name,meta,self.proof_accountant())?;
            let mut commitments = historical_index_commitment::CommitmentBatch::for_writer()?;
            {
                let source = write.open_table(temporary).map_err(Self::storage_error)?;
                let mut output = content_size::open_rows(&write, definition, Some(meta))
                    .map_err(Self::storage_error)?;
                for declaration in output.indexes() {
                    commitments.reset(
                        &write,
                        name,
                        declaration,
                        historical_index_commitment::Side::Versions,
                    )?;
                }
                for entry in source.iter().map_err(Self::storage_error)? {
                    let (key, value) = entry.map_err(Self::storage_error)?;
                    let mut window = self.proof_admission();
                    window.admit(value.value().len().saturating_mul(8).saturating_add(1024))?;
                    let row = Self::decode_versioned_row(value.value(), None)?;
                    self.replace_relational_manifest_in_write(
                        &write,
                        name,
                        key.value(),
                        None,
                        Some(value.value()),
                    )?;
                    let keys = content_size::VersionLife::keys_of(output.indexes(), &row)?;
                    output.insert_version(
                        &mut commitments,
                        key.value(),
                        value.value(),
                        &content_size::VersionLife {
                            keys: &keys,
                            created: row.created_tx,
                            deleted: row.deleted_tx,
                        },
                    )?;
                }
            }
            Self::retire_undeclared_relational_indexes_in_write(
                &write, &self.path, name, &previous, meta, &mut self.proof_admission(),
            )?;
            let mut indexes = ProjectedIndexes::prepare(self, &write, name, meta, &mut commitments)?;
            {
                let mut after: Option<[u8; 24]> = None;
                let mut head: Option<(
                    RowId,
                    Lsn,
                    content_identity::ContentIdentityMutation,
                    content_identity::OwnedProofAdmission,
                )> = None;
                loop {
                    let mut window = self.proof_admission();
                    // Every fingerprint update may finalize closed table pages.
                    // End the source table and access guards before that update.
                    let selected = {
                        let source = write.open_table(temporary).map_err(Self::storage_error)?;
                        let lower = after
                            .as_ref()
                            .map_or(Bound::Unbounded, |key| Bound::Excluded(key.as_slice()));
                        let mut cursor = source
                            .range::<&[u8]>((lower, Bound::Unbounded))
                            .map_err(Self::storage_error)?;
                        let entry = cursor.next().transpose().map_err(Self::storage_error)?;
                        entry
                            .map(|(key, value)| {
                                window.admit(
                                    value.value().len().saturating_mul(8).saturating_add(1024),
                                )?;
                                let key: [u8; 24] =
                                    key.value().try_into().map_err(|_| Error::StoreCorrupted {
                                        path: name.into(),
                                        reason: "invalid schema projection key".into(),
                                    })?;
                                Self::decode_versioned_row(value.value(), None)
                                    .map(|row| (key, row))
                            })
                            .transpose()?
                    };
                    let Some((key, row)) = selected else {
                        break;
                    };
                    after = Some(key);
                    indexes.push(self, &write, name, meta, &row, &mut commitments)?;
                    if head.as_ref().is_some_and(|(id, _, _, _)| *id != row.row_id) {
                        let (_, _, mutation, _head_credit) =
                            head.take().expect("previous row head");
                        Self::apply_content_identity_mutations_in_write(
                            &write,
                            &[mutation],
                            &mut self.proof_admission(),
                        )?;
                    }
                    if head
                        .as_ref()
                        .is_none_or(|(_, latest, _, _)| row.lsn >= *latest)
                    {
                        head = Some((
                            row.row_id,
                            row.lsn,
                            content_identity::row_mutation(name, &row, Some(meta))?,
                            window,
                        ));
                    }
                }
                if let Some((_, _, mutation, _head_credit)) = head {
                    Self::apply_content_identity_mutations_in_write(
                        &write,
                        &[mutation],
                        &mut self.proof_admission(),
                    )?;
                }
            }
            write.delete_table(temporary).map_err(Self::storage_error)?;
            Self::retire_undeclared_relational_indexes_in_write(
                &write,
                &self.path,
                name,
                &previous,
                meta,
                &mut self.proof_admission(),
            )?;
            #[cfg(any(test, feature = "test-seams"))]
            table_meta_rewrite_checkpoint_for_test(1);
            indexes.finish(self, &write, name, meta)?;
            let (mutations, _credit) = commitments.finish(&write)?.into_parts();
            Self::apply_content_identity_mutations_in_write(
                &write,
                &mutations,
                &mut self.proof_admission(),
            )?;
            #[cfg(any(test, feature = "test-seams"))]
            table_meta_rewrite_checkpoint_for_test(2);
            Self::apply_content_identity_mutations_in_write(
                &write,
                &content_identity::schema_mutations(name, meta)?,
                &mut self.proof_admission(),
            )?;
            {
                for (key, encoded) in config_values {
                    Self::raise_config_identity_floors_in_write(&write, key, encoded)?;
                    Self::update_policy_hold_release_index(&write, key, true)?;
                    write
                        .open_table(CONFIG_TABLE)
                        .map_err(Self::storage_error)?
                        .insert(*key, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    Self::apply_content_identity_mutations_in_write(
                        &write,
                        &content_identity::config_mutations(key, Some(encoded)),
                        &mut self.proof_admission(),
                    )?;
                }
            }
            {
                for (ordinal, change) in ddl.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(lsn, ordinal, ddl.len());
                    self.append_ddl_summary_in_write(&write, &key, change)?;
                    let encoded = Self::encode(change)?;
                    write
                        .open_table(DDL_LOG_TABLE)
                        .map_err(Self::storage_error)?
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    Self::apply_content_identity_mutations_in_write(
                        &write,
                        &[content_identity::encoded_history_mutation(&key, change)?],
                        &mut self.proof_admission(),
                    )?;
                }
            }
            Self::raise_table_read_lsn_in_write(&write, name, lsn)?;
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(Self::storage_error)?;
            self.complete_writer_schema_change(schema);
            #[cfg(any(test, feature = "test-seams"))]
            table_meta_rewrite_checkpoint_for_test(3);
            Ok(())
        })?;
        Ok(true)
    }
}

// A rebuild follows the relation's (row, transaction, LSN) order. Each index
// retains only the last interval of one row; maintained counts are folded in
// the unpublished store, with duplicate prefixes removed for that row alone.
struct ProjectedIndexHead {
    key: IndexKey,
    row: RowId,
    created: TxId,
    end: Option<TxId>,
    _credit: content_identity::OwnedProofAdmission,
}
struct ProjectedIndexes {
    declarations: Vec<IndexDecl>,
    heads: Vec<Option<ProjectedIndexHead>>,
    row: Option<RowId>,
    _credit: content_identity::OwnedProofAdmission,
}
impl ProjectedIndexes {
    fn prepare(
        persistence: &RedbPersistence, write: &redb::WriteTransaction,
        table: &str, meta: &TableMeta,
        commitments: &mut historical_index_commitment::CommitmentBatch,
    ) -> Result<Self> {
        let mut credit = persistence.proof_admission();
        credit.admit(meta.estimated_bytes().saturating_mul(8).saturating_add(1024))?;
        let declarations = RedbPersistence::relational_index_declarations(meta);
        // Remove current posting publications one at a time. No historical
        // key list survives this cursor step or becomes rebuild working state.
        for declaration in &declarations {
            let current_name = RedbPersistence::current_relational_index_table_name(table, &declaration.name);
            let current_definition = TableDefinition::<DurableRelationalIndexKey, &[u8]>::new(&current_name);
            loop {
                let mut window = persistence.proof_admission();
                let key = {
                    let mut current = write.open_table(current_definition).map_err(RedbPersistence::storage_error)?;
                    let first = current.iter().map_err(RedbPersistence::storage_error)?
                        .next().transpose().map_err(RedbPersistence::storage_error)?;
                    let key = first.map(|(key, _)| key.value());
                    if let Some(key) = key.as_ref() {
                        window.admit(key.0.len().saturating_mul(4).saturating_add(256))?;
                        current.remove(key).map_err(RedbPersistence::storage_error)?;
                    }
                    key
                };
                let Some(key) = key else { break; };
                RedbPersistence::apply_content_identity_mutations_in_write(write, &[
                    content_identity::remove_relational_index_posting_mutation(table, &declaration.name, &key.0),
                ], &mut persistence.proof_admission())?;
            }
            let historical_name = RedbPersistence::relational_index_table_name(table, &declaration.name);
            write.delete_table(TableDefinition::<DurableRelationalIndexKey, &[u8]>::new(&historical_name))
                .map_err(RedbPersistence::storage_error)?;
            write.delete_table(current_definition).map_err(RedbPersistence::storage_error)?;
            commitments.reset(write, table, declaration, historical_index_commitment::Side::Postings)?;
            // An empty declared index is still an authenticated physical
            // family. Its first insert must not have to invent a missing root.
            drop(historical_index_commitment::HistoricalPostings::open(write, table, declaration)
                .map_err(RedbPersistence::storage_error)?);
            drop(write.open_table(current_definition).map_err(RedbPersistence::storage_error)?);
        }
        // Count keys are self-describing. Keep only one cursor coordinate even
        // when this table shares the count relation with other declarations.
        let mut after: Option<(Vec<u8>, content_identity::OwnedProofAdmission)> = None;
        loop {
            let mut window = persistence.proof_admission();
            let selected = {
                let mut counts = write.open_table(RELATIONAL_INDEX_COUNT_TABLE).map_err(RedbPersistence::storage_error)?;
                let lower = after.as_ref().map_or(Bound::Unbounded, |(key, _)| Bound::Excluded(key.as_slice()));
                let first = counts.range::<&[u8]>((lower, Bound::Unbounded)).map_err(RedbPersistence::storage_error)?
                    .next().transpose().map_err(RedbPersistence::storage_error)?;
                let selected = first.map(|(key, _)| {
                    window.admit(key.value().len().saturating_mul(8).saturating_add(256))?;
                    let decoded: EncodedRelationalIndexCountKey = RedbPersistence::decode(key.value())?;
                    Ok::<_, Error>((key.value().to_vec(), decoded.table == table))
                }).transpose()?;
                if let Some((key, true)) = selected.as_ref() {
                    counts.remove(key.as_slice()).map_err(RedbPersistence::storage_error)?;
                }
                selected
            };
            let Some((key, remove)) = selected else { break; };
            if remove {
                RedbPersistence::apply_content_identity_mutations_in_write(write,
                    &[RedbPersistence::remove_relational_index_count_mutation(&key)],
                    &mut persistence.proof_admission())?;
            }
            after = Some((key, window));
        }
        let heads = (0..declarations.len()).map(|_| None).collect();
        Ok(Self { declarations, heads, row: None, _credit: credit })
    }
    fn push(
        &mut self, persistence: &RedbPersistence, write: &redb::WriteTransaction,
        table: &str, meta: &TableMeta, row: &VersionedRow,
        commitments: &mut historical_index_commitment::CommitmentBatch,
    ) -> Result<()> {
        if self.row.is_some_and(|previous| previous != row.row_id) {
            self.finish_row(persistence, write, table, meta)?;
        }
        self.row = Some(row.row_id);
        for (declaration, slot) in self.declarations.iter().zip(&mut self.heads) {
            let mut credit = persistence.proof_admission();
            credit.admit(row.estimated_bytes().saturating_mul(8).saturating_add(1024))?;
            let key = contextdb_relational::index_key_for_row(&declaration.columns, &row.values);
            let created = slot.as_ref().filter(|previous|
                previous.row == row.row_id && previous.key == key && previous.end == Some(row.created_tx))
                .map_or(row.created_tx, |previous| previous.created);
            let durable = DurableRelationalIndexKey::new(key.clone(), row.row_id, created)?;
            historical_index_commitment::HistoricalPostings::open(write, table, declaration)
                .map_err(RedbPersistence::storage_error)?
                .put(commitments, write, durable, row.deleted_tx)?;
            *slot = Some(ProjectedIndexHead { key, row: row.row_id, created, end: row.deleted_tx, _credit: credit });
        }
        Ok(())
    }
    fn finish_row(
        &mut self, persistence: &RedbPersistence, write: &redb::WriteTransaction,
        table: &str, meta: &TableMeta,
    ) -> Result<()> {
        let mut window = persistence.proof_admission();
        // Only one row's declared count prefixes coexist; every insertion below
        // grows this ownership before it allocates a coordinate.
        let mut count_keys = BTreeSet::new();
        for (declaration, head) in self.declarations.iter().zip(&mut self.heads) {
            let Some(head) = head.take() else { continue; };
            if head.end.is_some() { continue; }
            let logical = DurableRelationalIndexKey::new(head.key.clone(), head.row, TxId(0))?;
            let current_name = RedbPersistence::current_relational_index_table_name(table, &declaration.name);
            {
                write.open_table(TableDefinition::<DurableRelationalIndexKey, &[u8]>::new(&current_name))
                    .map_err(RedbPersistence::storage_error)?
                    .insert(&logical, RedbPersistence::encode(&head.created)?.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            #[cfg(any(test, feature = "test-seams"))]
            observe_index_posting(table, declaration);
            RedbPersistence::apply_content_identity_mutations_in_write(write, &[
                content_identity::relational_index_posting_mutation(table, &declaration.name, &logical.0),
            ], &mut persistence.proof_admission())?;
            for prefix_len in 1..=head.key.len() {
                if !crate::maintained_counts::maintained_count_member(meta, &declaration.columns[..prefix_len]) { continue; }
                window.admit(meta.estimated_bytes().saturating_add(head.key.iter().map(|part| match part {
                    DirectedValue::Asc(TotalOrdAsc(value)) | DirectedValue::Desc(TotalOrdDesc(value)) => value.estimated_bytes(),
                }).sum::<usize>()).saturating_mul(4).saturating_add(256))?;
                let key = RedbPersistence::relational_index_count_key(table,
                    &declaration.columns[..prefix_len], &head.key[..prefix_len])?;
                #[cfg(any(test, feature = "test-seams"))]
                observe_index_prefix_count(table, meta, declaration, prefix_len, &key);
                count_keys.insert(key);
            }
        }
        for key in count_keys {
            let next = {
                let mut counts = write.open_table(RELATIONAL_INDEX_COUNT_TABLE).map_err(RedbPersistence::storage_error)?;
                let prior = counts.get(key.as_slice()).map_err(RedbPersistence::storage_error)?
                    .map_or(0, |value| value.value());
                let next = prior.checked_add(1).ok_or_else(|| Error::Other("relational count overflow".into()))?;
                counts.insert(key.as_slice(), next).map_err(RedbPersistence::storage_error)?;
                next
            };
            RedbPersistence::apply_content_identity_mutations_in_write(write,
                &[RedbPersistence::relational_index_count_mutation(&key, next)], &mut persistence.proof_admission())?;
        }
        self.row = None;
        Ok(())
    }
    fn finish(mut self, persistence: &RedbPersistence, write: &redb::WriteTransaction, table: &str, meta: &TableMeta) -> Result<()> {
        self.finish_row(persistence, write, table, meta)?;
        write.open_table(RELATIONAL_INDEX_READY_TABLE).map_err(RedbPersistence::storage_error)?
            .insert(table, RELATIONAL_INDEX_READY_VERSION).map_err(RedbPersistence::storage_error)?;
        RedbPersistence::apply_content_identity_mutations_in_write(write,
            &[content_identity::status_mutation(RELATIONAL_INDEX_COUNT_READY_IDENTITY_NAMESPACE,
                table.as_bytes(), RELATIONAL_INDEX_READY_VERSION)], &mut persistence.proof_admission())?;
        Ok(())
    }
}

// A received rebuild walks the authenticated physical row order. Only the
// current row's contiguous run for each declared index survives a step.
struct OriginalProjectedIndexHead {
    key: IndexKey,
    row: RowId,
    created: TxId,
    end: Option<TxId>,
    _credit: crate::memory_accounting::OwnedMemoryReservation,
}
pub(super) struct OriginalProjectedIndexes<'o> {
    declarations: Vec<IndexDecl>,
    heads: Vec<Option<OriginalProjectedIndexHead>>,
    row: Option<RowId>,
    original: &'o crate::memory_accounting::OwnedMemoryReservation,
    _credit: crate::memory_accounting::OwnedMemoryReservation,
}
fn original_projected_index_key(
    frame: &OriginalPersistenceFrame<'_>, key: IndexKey, row: RowId, created: TxId,
) -> Result<DurableRelationalIndexKey> {
    frame.array::<(bool, Value)>(key.len())?;
    Ok(DurableRelationalIndexKey(frame.encode(
        &EncodedRelationalIndexPosition::from(RelationalIndexPosition {
            key, row_id: row, created_tx: created,
        }),
    )?))
}
fn apply_original_projected_mutation(
    persistence: &RedbPersistence, original: &crate::memory_accounting::OwnedMemoryReservation,
    write: &redb::WriteTransaction, mutation: content_identity::ContentIdentityMutation,
) -> Result<()> {
    RedbPersistence::apply_owned_content_identity_with_manifests_in_write(
        write, &[mutation], &[],
        &mut content_identity::original_writer::ProofAdmission::new(original, persistence.proof_scratch_owner())?,
    ).map(|_| ())
}
impl<'o> OriginalProjectedIndexes<'o> {
    pub(super) fn prepare(
        persistence: &RedbPersistence, frame: &OriginalPersistenceFrame<'o>,
        write: &redb::WriteTransaction, table: &str, meta: &TableMeta,
        commitments: &mut historical_index_commitment::CommitmentBatch,
    ) -> Result<Self> {
        let credit = OriginalPersistenceFrame::new(frame.original)?;
        credit.reserve(meta.estimated_bytes().saturating_mul(8).saturating_add(1024))?;
        let declarations = RedbPersistence::relational_index_declarations(meta);
        credit.array::<Option<OriginalProjectedIndexHead>>(declarations.len())?;
        for declaration in &declarations {
            let historical_name = RedbPersistence::relational_index_table_name(table, &declaration.name);
            let historical_definition = TableDefinition::<DurableRelationalIndexKey, &[u8]>::new(&historical_name);
            // Preserve the original rebuild's exact/canonical key and declared
            // position checks. Retire logical publications from authoritative
            // historical live postings, even if a current companion is absent.
            let current_name = RedbPersistence::current_relational_index_table_name(table, &declaration.name);
            let current_definition = TableDefinition::<DurableRelationalIndexKey, &[u8]>::new(&current_name);
            let coordinate_credit = OriginalPersistenceFrame::new(frame.original)?;
            let mut coordinate_bytes = 0usize;
            let mut after: Option<DurableRelationalIndexKey> = None;
            loop {
                let window = OriginalPersistenceFrame::new(frame.original)?;
                let selected = {
                    let source = match write.open_table(historical_definition) {
                        Ok(source) => source,
                        Err(redb::TableError::TableDoesNotExist(_)) => break,
                        Err(error) => return Err(RedbPersistence::storage_error(error)),
                    };
                    let lower = after.as_ref().map_or(Bound::Unbounded, Bound::Excluded);
                    let mut cursor = source.range::<&DurableRelationalIndexKey>((lower, Bound::Unbounded))
                        .map_err(RedbPersistence::storage_error)?;
                    decode_relational_index_entry_original(&window, &persistence.path,
                        cursor.next().transpose().map_err(RedbPersistence::storage_error)?)?
                };
                let Some((coordinate, position, deleted)) = selected else { break; };
                RedbPersistence::validated_index_position(declaration, meta, &position)
                    .map_err(|reason| Error::StoreCorrupted { path: persistence.path.display().to_string(), reason })?;
                let required = coordinate.len().saturating_mul(2).saturating_add(128);
                if required > coordinate_bytes {
                    coordinate_credit.reserve(required - coordinate_bytes)?;
                    coordinate_bytes = required;
                }
                if deleted.is_none() {
                    let logical = original_projected_index_key(&window, position.key, position.row_id, TxId(0))?;
                    apply_original_projected_mutation(persistence, frame.original, write,
                        content_identity::original_producers::remove_relational_index_posting_mutation(
                            &window, table, &declaration.name, &logical.0,
                        )?,
                    )?;
                }
                after = Some(DurableRelationalIndexKey(coordinate));
            }
            match write.delete_table(historical_definition) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {},
                Err(error) => return Err(RedbPersistence::storage_error(error)),
            }
            write.delete_table(current_definition).map_err(RedbPersistence::storage_error)?;
            commitments.reset(write, table, declaration, historical_index_commitment::Side::Postings)?;
            commitments.reset(write, table, declaration, historical_index_commitment::Side::Versions)?;
            drop(historical_index_commitment::HistoricalPostings::open(write, table, declaration)
                .map_err(RedbPersistence::storage_error)?);
            drop(write.open_table(current_definition).map_err(RedbPersistence::storage_error)?);
        }
        persistence.erase_received_index_counts_original(frame, write, table, |mutation|
            apply_original_projected_mutation(persistence, frame.original, write, mutation))?;
        let heads = (0..declarations.len()).map(|_| None).collect();
        Ok(Self { declarations, heads, row: None, original: frame.original, _credit: credit.into_reservation() })
    }
    pub(super) fn push(
        &mut self, persistence: &RedbPersistence, write: &redb::WriteTransaction,
        table: &str, meta: &TableMeta, row: &VersionedRow,
        commitments: &mut historical_index_commitment::CommitmentBatch,
    ) -> Result<()> {
        if self.row.is_some_and(|previous| previous != row.row_id) {
            self.finish_row(persistence, write, table, meta)?;
        }
        self.row = Some(row.row_id);
        for (declaration, slot) in self.declarations.iter().zip(&mut self.heads) {
            let window = OriginalPersistenceFrame::new(self.original)?;
            window.reserve(row.estimated_bytes().saturating_mul(8).saturating_add(1024))?;
            let key = contextdb_relational::index_key_for_row(&declaration.columns, &row.values);
            // Row replacement folds Versions at the physical insert. This
            // producer owns only the independently derived posting coverage.
            let created = slot.as_ref().filter(|previous|
                previous.row == row.row_id && previous.key == key && previous.end == Some(row.created_tx))
                .map_or(row.created_tx, |previous| previous.created);
            let durable = original_projected_index_key(&window, key.clone(), row.row_id, created)?;
            historical_index_commitment::HistoricalPostings::open(write, table, declaration)
                .map_err(RedbPersistence::storage_error)?.put(commitments, write, durable, row.deleted_tx)?;
            *slot = Some(OriginalProjectedIndexHead {
                key, row: row.row_id, created, end: row.deleted_tx, _credit: window.into_reservation(),
            });
        }
        Ok(())
    }
    fn finish_row(
        &mut self, persistence: &RedbPersistence, write: &redb::WriteTransaction,
        table: &str, meta: &TableMeta,
    ) -> Result<()> {
        let window = OriginalPersistenceFrame::new(self.original)?;
        let mut count_keys = BTreeSet::new();
        for (declaration, head) in self.declarations.iter().zip(&mut self.heads) {
            let Some(head) = head.take() else { continue; };
            if head.end.is_some() { continue; }
            window.reserve(table.len().saturating_add(declaration.name.len()).saturating_mul(4)
                .saturating_add(head.key.iter().map(|part| match part {
                    DirectedValue::Asc(TotalOrdAsc(value)) | DirectedValue::Desc(TotalOrdDesc(value)) => value.estimated_bytes(),
                }).sum::<usize>().saturating_mul(4)).saturating_add(512))?;
            let logical = original_projected_index_key(&window, head.key.clone(), head.row, TxId(0))?;
            let current_name = RedbPersistence::current_relational_index_table_name(table, &declaration.name);
            {
                let encoded = window.encode(&head.created)?;
                write.open_table(TableDefinition::<DurableRelationalIndexKey, &[u8]>::new(&current_name))
                    .map_err(RedbPersistence::storage_error)?.insert(&logical, encoded.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            #[cfg(any(test, feature = "test-seams"))]
            observe_index_posting(table, declaration);
            apply_original_projected_mutation(persistence, self.original, write,
                content_identity::original_producers::relational_index_posting_mutation(
                    &window, table, &declaration.name, &logical.0,
                )?,
            )?;
            for prefix_len in 1..=head.key.len() {
                if !crate::maintained_counts::maintained_count_member(meta, &declaration.columns[..prefix_len]) { continue; }
                window.reserve(meta.estimated_bytes().saturating_add(head.key.iter().map(|part| match part {
                    DirectedValue::Asc(TotalOrdAsc(value)) | DirectedValue::Desc(TotalOrdDesc(value)) => value.estimated_bytes(),
                }).sum::<usize>()).saturating_mul(4).saturating_add(256))?;
                let key = RedbPersistence::relational_index_count_key(table,
                    &declaration.columns[..prefix_len], &head.key[..prefix_len])?;
                #[cfg(any(test, feature = "test-seams"))]
                observe_index_prefix_count(table, meta, declaration, prefix_len, &key);
                count_keys.insert(key);
            }
        }
        for key in count_keys {
            let next = {
                let mut counts = write.open_table(RELATIONAL_INDEX_COUNT_TABLE).map_err(RedbPersistence::storage_error)?;
                let prior = counts.get(key.as_slice()).map_err(RedbPersistence::storage_error)?.map_or(0, |value| value.value());
                let next = prior.checked_add(1).ok_or_else(|| Error::Other("relational count overflow".into()))?;
                counts.insert(key.as_slice(), next).map_err(RedbPersistence::storage_error)?;
                next
            };
            window.reserve(key.len().saturating_mul(4).saturating_add(256))?;
            apply_original_projected_mutation(persistence, self.original, write,
                RedbPersistence::relational_index_count_mutation(&key, next))?;
        }
        self.row = None;
        Ok(())
    }
    pub(super) fn finish(mut self, persistence: &RedbPersistence, write: &redb::WriteTransaction,
        table: &str, meta: &TableMeta,
    ) -> Result<()> {
        self.finish_row(persistence, write, table, meta)?;
        write.open_table(RELATIONAL_INDEX_READY_TABLE).map_err(RedbPersistence::storage_error)?
            .insert(table, RELATIONAL_INDEX_READY_VERSION).map_err(RedbPersistence::storage_error)?;
        let window = OriginalPersistenceFrame::new(self.original)?;
        apply_original_projected_mutation(persistence, self.original, write,
            content_identity::original_producers::status_mutation(&window,
                RELATIONAL_INDEX_COUNT_READY_IDENTITY_NAMESPACE, table.as_bytes(), RELATIONAL_INDEX_READY_VERSION)?)?;
        Ok(())
    }
}
impl RedbPersistence {
    pub(super) fn rebuild_received_indexes_paged_original(
        &self, frame: &OriginalPersistenceFrame<'_>, write: &redb::WriteTransaction,
        table: &str, meta: &TableMeta, supplied_rows: Option<&[VersionedRow]>,
        commitments: &mut historical_index_commitment::CommitmentBatch,
    ) -> Result<()> {
        let mut indexes = OriginalProjectedIndexes::prepare(self, frame, write, table, meta, commitments)?;
        // A standalone rebuild does not insert rows, so derive their coverage
        // here exactly once, separately from the posting producer.
        let mut rebuild_row = |row: &VersionedRow| -> Result<()> {
            let window = OriginalPersistenceFrame::new(frame.original)?;
            window.reserve(row.estimated_bytes().saturating_mul(8).saturating_add(1024))?;
            for declaration in &indexes.declarations {
                let key = contextdb_relational::index_key_for_row(&declaration.columns, &row.values);
                commitments.fold_interval(write, table, declaration, historical_index_commitment::Side::Versions,
                    &historical_index_commitment::key_row(&key, row.row_id)?, row.created_tx, row.deleted_tx, true)?;
            }
            indexes.push(self, write, table, meta, row, commitments)
        };
        if let Some(rows) = supplied_rows {
            if rows.windows(2).any(|pair|
                (pair[0].row_id, pair[0].created_tx, pair[0].lsn) > (pair[1].row_id, pair[1].created_tx, pair[1].lsn)) {
                return Err(Error::Other("received index input is not in prepared durable order".into()));
            }
            for row in rows { rebuild_row(row)?; }
        } else {
            self.visit_relational_rows_in_write_original(frame, write, table, |row, _window| {
                #[cfg(any(test, feature = "test-seams"))]
                RELATIONAL_INDEX_REBUILD_ROWS_DECODED.with(|count| count.set(count.get().saturating_add(1)));
                rebuild_row(row)
            })?;
        }
        indexes.finish(self, write, table, meta)
    }
}

// The native received kernel stages its derived placement image inside the
// same unpublished Redb transaction. These relations never cross commit.
const RECEIVED_VECTOR_ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_vector_row_refs");
const RECEIVED_VECTOR_BODIES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_vector_body_refs");
const RECEIVED_VECTOR_BODY_USED: TableDefinition<&[u8], u8> = TableDefinition::new("__received_vector_body_used");
const RECEIVED_VECTOR_MEMBERS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_vector_members");
const RECEIVED_VECTOR_OCCUPIED: TableDefinition<&[u8], u64> = TableDefinition::new("__received_vector_occupied");
const RECEIVED_VECTOR_COUNTS: TableDefinition<&[u8], u64> = TableDefinition::new("__received_vector_partition_counts");

impl RedbPersistence {
    pub(super) fn received_vector_body_in_write_original(
        &self, frame: &OriginalPersistenceFrame<'_>, write: &redb::WriteTransaction,
        member: &VectorPartitionMembershipRecord,
    ) -> Result<Option<VectorEntry>> {
        frame.reserve(member.index.table.len().saturating_add(member.index.column.len()).saturating_add(34))?;
        let key = Self::vector_identity_key(&member.index,member.row_id,member.vector_created_tx,member.vector_lsn);
        let bodies = write.open_table(RECEIVED_VECTOR_BODIES).map_err(Self::storage_error)?;
        let value = bodies.get(key.as_slice()).map_err(Self::storage_error)?;
        value.map(|value| frame.decode(value.value())).transpose()
    }
}

impl RedbPersistence {
    pub(super) fn prepare_complete_vector_partition_image_paged_original(
        &self, original: &OriginalPersistenceFrame<'_>, write: &redb::WriteTransaction,
        stage: &ReceivedSchemaPersistenceProjection,
    ) -> Result<CompleteVectorPartitionImage> {
        let _controls = OriginalPersistenceFrame::new(original.original)?;
        _controls.reserve(8192)?;
        macro_rules! empty {
            ($definition:expr) => {
                if !write.open_table($definition).map_err(Self::storage_error)?
                    .is_empty().map_err(Self::storage_error)? {
                    return Err(self.vector_partition_corruption("unpublished vector image survived a transaction"));
                }
            };
        }
        empty!(RECEIVED_VECTOR_ROWS); empty!(RECEIVED_VECTOR_BODIES); empty!(RECEIVED_VECTOR_BODY_USED);
        empty!(RECEIVED_VECTOR_MEMBERS); empty!(RECEIVED_VECTOR_OCCUPIED); empty!(RECEIVED_VECTOR_COUNTS);
        // The exact owning row is a paged record, not an offset into a
        // population-sized staging vector. Its original-funded decode window
        // below remains coupled to the derived membership through validation.
        stage.visit_rows_original(original, |table,row| {
            let window = OriginalPersistenceFrame::new(original.original)?;
            let key = window.encode(&(table,row.row_id,row.created_tx,row.lsn))?;
            let mut refs = write.open_table(RECEIVED_VECTOR_ROWS).map_err(Self::storage_error)?;
            if refs.get(key.as_slice()).map_err(Self::storage_error)?.is_none() {
                let encoded = Self::encode_versioned_row_original(&window,row,None)?;
                refs.insert(key.as_slice(),encoded.as_slice()).map_err(Self::storage_error)?;
            }
            Ok(())
        })?;
        stage.visit_vector_bodies_original(original, |body| {
            let window = OriginalPersistenceFrame::new(original.original)?;
            window.reserve(body.index.table.len().saturating_add(body.index.column.len()).saturating_add(512))?;
            let key = Self::vector_key(body);
            let mut refs = write.open_table(RECEIVED_VECTOR_BODIES).map_err(Self::storage_error)?;
            let prior: Option<VectorEntry> = refs.get(key.as_slice()).map_err(Self::storage_error)?
                .map(|prior| window.decode(prior.value())).transpose()?;
            if let Some(old) = prior {
                if old.deleted_tx != body.deleted_tx || old.vector.len() != body.vector.len()
                    || old.vector.iter().zip(body.vector.iter()).any(|(a,b)| a.to_bits() != b.to_bits()) {
                    return Err(self.vector_partition_corruption("received placements disagree on their canonical body"));
                }
            } else {
                let encoded = window.encode(body)?;
                refs.insert(key.as_slice(),encoded.as_slice()).map_err(Self::storage_error)?;
            }
            Ok(())
        })?;
        stage.visit_vector_memberships_original(original, |member| {
            let window = OriginalPersistenceFrame::new(original.original)?;
            window.record(member)?;
            let (meta, declaration) = stage.table_meta.get(&member.index.table).and_then(|meta|
                meta.columns.iter().find(|column| column.name == member.index.column).map(|column| (meta,column)))
                .filter(|(_,column)| matches!(column.column_type,ColumnType::Vector(_)))
                .ok_or_else(|| Error::UnknownVectorIndex { index: member.index.clone() })?;
            let row_key = window.encode(&(member.index.table.as_str(), member.row_id, member.row_created_tx, member.row_lsn))?;
            let row = {
                let rows = write.open_table(RECEIVED_VECTOR_ROWS).map_err(Self::storage_error)?;
                let encoded = rows.get(row_key.as_slice()).map_err(Self::storage_error)?
                    .ok_or_else(|| self.vector_partition_corruption("received placement has no exact accepted row owner"))?;
                Self::decode_versioned_row_original(&window,encoded.value(),None)?
            };
            window.reserve(member.index.table.len().saturating_add(member.index.column.len()).saturating_add(512))?;
            let body_key = Self::vector_identity_key(&member.index,member.row_id,member.vector_created_tx,member.vector_lsn);
            let body: VectorEntry = {
                let refs = write.open_table(RECEIVED_VECTOR_BODIES).map_err(Self::storage_error)?;
                let encoded = refs.get(body_key.as_slice()).map_err(Self::storage_error)?
                    .ok_or_else(|| self.vector_partition_corruption("received placement has no exact canonical body"))?;
                window.decode(encoded.value())?
            };
            write.open_table(RECEIVED_VECTOR_BODY_USED).map_err(Self::storage_error)?
                .insert(body_key.as_slice(),1).map_err(Self::storage_error)?;
            if member.visible_from < TxId(row.created_tx.0.max(body.created_tx.0))
                || member.deleted_tx.is_some_and(|deleted| deleted <= member.visible_from)
                || [row.deleted_tx,body.deleted_tx].into_iter().flatten().any(|deleted|
                    member.deleted_tx.is_none_or(|end| end > deleted)) {
                return Err(self.vector_partition_corruption("received placement exceeds its exact row or body lifecycle"));
            }
            window.reserve(crate::memory_accounting::preparation_row_copy_bytes(&row)?
                .saturating_add(meta.estimated_bytes().saturating_mul(8)))?;
            let partition_key = vector_partition_key_for_row(&member.index,meta,declaration,&row)?;
            if partition_key != member.partition_key {
                return Err(self.vector_partition_corruption("received placement partition disagrees with its exact row owner"));
            }
            let key = Self::vector_partition_membership_key(member);
            let encoded = window.encode(member)?;
            {
                let mut members = write.open_table(RECEIVED_VECTOR_MEMBERS).map_err(Self::storage_error)?;
                let old = members.get(key.as_slice()).map_err(Self::storage_error)?
                    .map(|value| window.decode::<VectorPartitionMembershipRecord>(value.value())).transpose()?;
                if old.as_ref().is_some_and(|old| old != member) {
                    return Err(self.vector_partition_corruption("complete received image contains conflicting vector memberships"));
                }
                if old.is_none() { members.insert(key.as_slice(),encoded.as_slice()).map_err(Self::storage_error)?; }
            }
            if member.deleted_tx.is_none() {
                let route = Self::vector_partition_prefix(&member.index,&partition_key);
                let mut occupied = write.open_table(RECEIVED_VECTOR_OCCUPIED).map_err(Self::storage_error)?;
                let fresh = occupied.get(route.as_slice()).map_err(Self::storage_error)?.is_none();
                if fresh {
                    occupied.insert(route.as_slice(),0).map_err(Self::storage_error)?;
                    let index_key = window.encode(&member.index)?;
                    let mut counts = write.open_table(RECEIVED_VECTOR_COUNTS).map_err(Self::storage_error)?;
                    let count = counts.get(index_key.as_slice()).map_err(Self::storage_error)?
                        .map_or(0,|value| value.value()).checked_add(1)
                        .ok_or_else(|| Error::Other("received partition count overflow".into()))?;
                    if let Some(max_partitions) = declaration.effective_max_partitions()
                        && count > u64::from(max_partitions) {
                        return Err(Error::VectorPartitionLimitExceeded { index: member.index.clone(),max_partitions });
                    }
                    counts.insert(index_key.as_slice(),count).map_err(Self::storage_error)?;
                }
            }
            Ok(())
        })?;
        {
            let bodies = write.open_table(RECEIVED_VECTOR_BODIES).map_err(Self::storage_error)?;
            let used = write.open_table(RECEIVED_VECTOR_BODY_USED).map_err(Self::storage_error)?;
            for record in bodies.iter().map_err(Self::storage_error)? {
                let (key,_) = record.map_err(Self::storage_error)?;
                if used.get(key.value()).map_err(Self::storage_error)?.is_none() {
                    return Err(self.vector_partition_corruption("received canonical body has no exact accepted placement"));
                }
            }
        }
        write.delete_table(RECEIVED_VECTOR_ROWS).map_err(Self::storage_error)?;
        // Canonical bodies remain transaction-local until fixed-input
        // reconciliation finishes; it point-loads them without stage offsets.
        write.delete_table(RECEIVED_VECTOR_BODY_USED).map_err(Self::storage_error)?;
        write.delete_table(RECEIVED_VECTOR_OCCUPIED).map_err(Self::storage_error)?;
        write.delete_table(RECEIVED_VECTOR_COUNTS).map_err(Self::storage_error)?;
        Ok(CompleteVectorPartitionImage { paged_memberships: true, ..Default::default() })
    }

    pub(super) fn visit_complete_vector_image_memberships(
        &self, write: &redb::WriteTransaction, image: &CompleteVectorPartitionImage,
        original: Option<&OriginalPersistenceFrame<'_>>,
        route: Option<&VectorPartitionRef>,
        mut visit: impl FnMut(&VectorPartitionMembershipRecord) -> Result<bool>,
    ) -> Result<()> {
        if !image.paged_memberships {
            for member in &image.memberships {
                if route.is_some_and(|route| member.index != route.index || member.partition_key != route.partition_key) { continue; }
                if !visit(member)? { break; }
            }
            return Ok(());
        }
        let original = original.ok_or_else(|| Error::Other("paged vector image lost its original payer".into()))?;
        let route_window = OriginalPersistenceFrame::new(original.original)?;
        let prefix = route.map(|route| -> Result<Vec<u8>> {
            route_window.reserve(route.index.table.len().saturating_add(route.index.column.len())
                .saturating_add(route.partition_key.estimated_bytes().saturating_mul(8)).saturating_add(1024))?;
            Ok(Self::vector_partition_prefix(&route.index,&route.partition_key))
        }).transpose()?;
        let members = write.open_table(RECEIVED_VECTOR_MEMBERS).map_err(Self::storage_error)?;
        let lower = prefix.as_ref().map_or(Bound::Unbounded, |prefix| Bound::Included(prefix.as_slice()));
        for item in members.range::<&[u8]>((lower,Bound::Unbounded)).map_err(Self::storage_error)? {
            let window = OriginalPersistenceFrame::new(original.original)?;
            let (key,value) = item.map_err(Self::storage_error)?;
            if prefix.as_ref().is_some_and(|prefix| !key.value().starts_with(prefix)) { break; }
            let member: VectorPartitionMembershipRecord = window.decode(value.value())?;
            window.record(&member)?;
            if Self::vector_partition_membership_key(&member).as_slice() != key.value() {
                return Err(self.vector_partition_corruption("unpublished vector membership key disagrees with its payload"));
            }
            if !visit(&member)? { break; }
        }
        Ok(())
    }

    pub(super) fn replace_paged_vector_partition_image_in_write_original(
        &self, original: &OriginalPersistenceFrame<'_>, write: &redb::WriteTransaction,
        image: &CompleteVectorPartitionImage, tables: &HashSet<String>, lsn: Lsn,
    ) -> Result<()> {
        let accountant = crate::memory_accounting::persistence_original_projection(original.original)?.as_handle();
        self.prepare_vector_partition_image_replacement_in_write(write,tables,accountant)?;
        let mut ordinal = 0_u64;
        self.visit_complete_vector_image_memberships(write,image,Some(original),None,|member| {
            let window = OriginalPersistenceFrame::new(original.original)?;
            window.record(member)?;
            self.insert_vector_partition_membership(write,
                &mut write.open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE).map_err(Self::storage_error)?,member)?;
            let upsert = VectorPartitionJournalRecord { index: member.index.clone(),partition_key: member.partition_key.clone(),
                lsn,ordinal,change: VectorPartitionJournalChange::Upsert(member.clone()) };
            window.record(&upsert)?;
            self.append_vector_partition_journal(&mut write.open_table(VECTOR_PARTITION_JOURNAL_TABLE)
                .map_err(Self::storage_error)?,&upsert)?;
            ordinal = ordinal.checked_add(1).ok_or_else(|| Error::Other("complete vector partition journal ordinal overflow".into()))?;
            if let Some(deleted_tx) = member.deleted_tx {
                let tombstone = VectorPartitionTombstoneRecord { membership: member.clone(),deleted_tx,lsn,
                    cause: VectorPartitionTombstoneCause::Delete };
                window.record(&tombstone)?;
                let journal = VectorPartitionJournalRecord { index: member.index.clone(),partition_key: member.partition_key.clone(),
                    lsn,ordinal,change: VectorPartitionJournalChange::Tombstone(tombstone.clone()) };
                window.record(&journal)?;
                self.append_vector_partition_journal(&mut write.open_table(VECTOR_PARTITION_JOURNAL_TABLE)
                    .map_err(Self::storage_error)?,&journal)?;
                ordinal = ordinal.checked_add(1).ok_or_else(|| Error::Other("complete vector partition journal ordinal overflow".into()))?;
                let key = Self::vector_partition_tombstone_key(&tombstone);
                self.insert_vector_partition_record(&mut write.open_table(VECTOR_PARTITION_TOMBSTONE_TABLE)
                    .map_err(Self::storage_error)?,&key,VectorPartitionRecordKind::Tombstone,&tombstone)?;
            }
            Ok(true)
        })?;
        write.delete_table(RECEIVED_VECTOR_BODIES).map_err(Self::storage_error)?;
        write.delete_table(RECEIVED_VECTOR_MEMBERS).map_err(Self::storage_error)?;
        Self::ensure_open_vector_directory_in_write(write)
    }
}

const RECEIVED_DROPPED_HISTORY_LSNS: TableDefinition<u64,u8> = TableDefinition::new("__received_dropped_history_lsns");
const RECEIVED_HISTORY_SURVIVORS: TableDefinition<u64,&[u8]> = TableDefinition::new("__received_history_survivors");
impl RedbPersistence {
    pub(super) fn retire_received_change_log_paged_original(
        &self, original: &OriginalPersistenceFrame<'_>, write: &redb::WriteTransaction,
        dropped: &HashSet<String>, mutations: &mut OriginalMutationLog<'_, '_, '_>,
    ) -> Result<()> {
        if dropped.is_empty() { return Ok(()); }
        let controls = OriginalPersistenceFrame::new(original.original)?;
        controls.reserve(4096)?;
        if !write.open_table(RECEIVED_DROPPED_HISTORY_LSNS).map_err(Self::storage_error)?
            .is_empty().map_err(Self::storage_error)?
            || !write.open_table(RECEIVED_HISTORY_SURVIVORS).map_err(Self::storage_error)?
                .is_empty().map_err(Self::storage_error)? {
            return Err(Error::StoreCorrupted { path: self.path.display().to_string(),
                reason: "unpublished received history scratch survived a transaction".into() });
        }
        {
            let log = write.open_table(CHANGE_LOG_TABLE).map_err(Self::storage_error)?;
            let mut affected = write.open_table(RECEIVED_DROPPED_HISTORY_LSNS).map_err(Self::storage_error)?;
            #[cfg(any(test,feature="test-seams"))]
            crate::read_work_probe::note_graph("history","population_walk",&[],None,None);
            for item in log.iter().map_err(Self::storage_error)? {
                let window = OriginalPersistenceFrame::new(original.original)?;
                let (key,value) = item.map_err(Self::storage_error)?;
                #[cfg(any(test,feature="test-seams"))]
                crate::read_work_probe::graph_storage_key("history","iteration_step",key.value());
                let record = Self::decode_stored_change_log_original(&window,value.value())?;
                if Self::change_log_entry_references_any_table(&record.entry,dropped) {
                    // The primary history group is the durable acceptance
                    // envelope. Its payload may carry a private source position.
                    let coordinate = Self::numeric_change_key(key.value())?;
                    let envelope = u64::from_be_bytes(coordinate[..8].try_into().expect("fixed history coordinate"));
                    affected.insert(envelope,0).map_err(Self::storage_error)?;
                }
            }
        }
        let affected = write.open_table(RECEIVED_DROPPED_HISTORY_LSNS).map_err(Self::storage_error)?;
        for position in affected.iter().map_err(Self::storage_error)? {
            let lsn = Lsn(position.map_err(Self::storage_error)?.0.value());
            let prefix_window = OriginalPersistenceFrame::new(original.original)?;
            prefix_window.reserve(256)?;
            let prefix = format!("{:020}:",lsn.0);
            let mut cursor = None::<(String,OriginalPersistenceFrame<'_>)>;
            let mut survivors = 0_u64;
            loop {
                let key_window = OriginalPersistenceFrame::new(original.original)?;
                let window = OriginalPersistenceFrame::new(original.original)?;
                let _graph_window = self.original_graph_window(&window)?;
                let next = {
                    let log = write.open_table(CHANGE_LOG_TABLE).map_err(Self::storage_error)?;
                    let lower = cursor.as_ref().map_or(Bound::Included(prefix.as_str()),|(key,_)| Bound::Excluded(key.as_str()));
                    let mut rows = log.range::<&str>((lower,Bound::Unbounded)).map_err(Self::storage_error)?;
                    rows.next().transpose().map_err(Self::storage_error)?
                        .filter(|(key,_)| key.value().starts_with(&prefix))
                        .map(|(key,value)| -> Result<_> {
                            #[cfg(any(test,feature="test-seams"))]
                            crate::read_work_probe::graph_storage_key("history","iteration_step",key.value());
                            let key = key_window.copy_text(key.value())?;
                            let record = Self::decode_stored_change_log_original(&window,value.value())?;
                            let bytes = window.copy_bytes(value.value())?;
                            Ok((key,record,bytes))
                        }).transpose()?
                };
                let Some((key,record,bytes)) = next else { break; };
                let carried = !Self::change_log_entry_references_any_table(&record.entry,dropped);
                write.open_table(CHANGE_LOG_TABLE).map_err(Self::storage_error)?
                    .remove(observed_graph_key!("history","remove",key.as_str(),8)).map_err(Self::storage_error)?;
                Self::detach_change_log_selectors_in_write(write,key.as_str(),carried)?;
                mutations.push(content_identity::original_producers::remove_encoded_history_mutation(&window,&key,&record.entry)?)?;
                if carried {
                    write.open_table(RECEIVED_HISTORY_SURVIVORS).map_err(Self::storage_error)?
                        .insert(survivors,bytes.as_slice()).map_err(Self::storage_error)?;
                    survivors = survivors.checked_add(1).ok_or_else(|| Error::Other("received history survivor ordinal overflow".into()))?;
                }
                drop(bytes); drop(record);
                drop(_graph_window);
                cursor = Some((key,key_window));
            }
            drop(cursor);
            for ordinal in 0..survivors {
                let window = OriginalPersistenceFrame::new(original.original)?;
                let _graph_window = self.original_graph_window(&window)?;
                let bytes = {
                    let mut scratch = write.open_table(RECEIVED_HISTORY_SURVIVORS).map_err(Self::storage_error)?;
                    let value = scratch.remove(ordinal).map_err(Self::storage_error)?
                        .ok_or_else(|| Error::Other("unpublished history survivor is missing".into()))?;
                    window.copy_bytes(value.value())?
                };
                // Revalidate exact private bytes, including the graph witness,
                // before carrying the occurrence to its packed coordinate.
                let _record = Self::decode_stored_change_log_original(&window,&bytes)?;
                window.reserve(Self::change_log_entry_key_len())?;
                let mut key = String::with_capacity(Self::change_log_entry_key_len());
                let index = usize::try_from(ordinal).map_err(|_| Error::Other("received history coordinate overflow".into()))?;
                Self::write_change_log_entry_key(lsn,index,&mut key);
                write.open_table(CHANGE_LOG_TABLE).map_err(Self::storage_error)?
                    .insert(observed_graph_key!("history","insert",key.as_str(),8),bytes.as_slice()).map_err(Self::storage_error)?;
                self.graph_writer_window(|| Self::write_carried_change_log_selectors_in_write(write,&key,&bytes))?;
                mutations.push_at("received_change_log",
                    content_identity::original_producers::private_history_mutation(&window,&key,&bytes)?)?;
            }
        }
        drop(affected);
        write.delete_table(RECEIVED_DROPPED_HISTORY_LSNS).map_err(Self::storage_error)?;
        write.delete_table(RECEIVED_HISTORY_SURVIVORS).map_err(Self::storage_error)?;
        Ok(())
    }
}

impl ReceivedSchemaPersistenceProjection {
    pub(super) fn visit_edges_original(
        &self,
        frame: &OriginalPersistenceFrame<'_>,
        write: &received_working::ReceivedWorkingWrite,
        mut visit: impl FnMut(&AdjEntry, Option<(&[u8], &[u8])>) -> Result<()>,
    ) -> Result<()> {
        if write.graph_prepared()? {
            if let Some(selection) = &self.graph_selection {
                write.visit_selected_graph_original(frame.original, selection,
                    |edge, key, encoded| visit(edge, Some((key, encoded))))
            } else {
                write.visit_graph_original(frame.original, Some(&self.affected_graph_edge_types),
                    |edge, key, encoded| visit(edge, Some((key, encoded))))
            }
        } else {
            for edge in &self.edges { visit(edge, None)?; }
            Ok(())
        }
    }
}
