//! Exact discard selections in the terminal's unpublished write. No selected
//! history directory or decoded history population escapes a record window.
use super::*;
use std::ops::Bound;

const ROW_GRAPH: TableDefinition<u64, &[u8]> = TableDefinition::new("__discard_row_graph");
pub(crate) const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("__discard_rows");
pub(crate) const EDGES: TableDefinition<u64, &[u8]> = TableDefinition::new("__discard_edges");
const CONFIGS: TableDefinition<u64, &[u8]> = TableDefinition::new("__discard_configs");
const EDGE_KEYS: TableDefinition<&[u8], u8> = TableDefinition::new("__discard_edge_keys");
const OCCURRENCES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__discard_occurrences");
const CHANGES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__discard_changes");
const GROUPS: TableDefinition<u64, u8> = TableDefinition::new("__discard_groups");
const COMMITS: TableDefinition<u64, u64> = TableDefinition::new("__discard_commits");
const SURVIVORS: TableDefinition<u64, &[u8]> = TableDefinition::new("__discard_survivors");
pub(crate) type Row = (String, RowId, TxId, Lsn);

// Ordinary owned commits keep their original raw transaction path and do
// not allocate an unused received-source owner for this erasure facility.
pub(super) enum TerminalWrite {
    Fresh(redb::WriteTransaction),
    Prepared(received_working::ReceivedWorkingWrite),
}
impl std::ops::Deref for TerminalWrite {
    type Target = redb::WriteTransaction;
    fn deref(&self) -> &Self::Target { match self { Self::Fresh(write) => write, Self::Prepared(write) => write } }
}
impl TerminalWrite {
    pub(super) fn commit(self) -> std::result::Result<(),redb::CommitError> {
        match self { Self::Fresh(write) => write.commit(), Self::Prepared(write) => write.commit() }
    }
}

pub(crate) struct OriginalDiscardHistory {
    // Abort the actual write before retiring its exact Original control.
    write: received_working::ReceivedWorkingWrite,
    sealed: Option<[Option<Option<[u8; 32]>>; 14]>,
    root: writer_source::WriterSnapshotHandle,
}
impl OriginalDiscardHistory {
    pub(crate) fn begin(store: &RedbPersistence, original: &crate::memory_accounting::OwnedMemoryReservation,
        root: &writer_source::WriterSnapshotHandle) -> Result<Self> {
        root.require_working_store(store)?;
        // Both the unpublished write and its initial-root read use this same
        // authentic Original; byte-only handle access creates no codec owner
        // and cannot fall back to an attached store or unrelated reader payer.
        let storage_scope = native_storage_admission::StorageAdmissionScope::writer_original(original)?;
        let write = storage_scope.run_original_unqualified(|| store.with_db_handle(|db| {
            let write = received_working::ReceivedWorkingWrite::begin_in(store, db, original)?;
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            if read.storage_snapshot_identity() != root.storage_identity() {
                return Err(Error::Other("discard selection differs from its terminal's initial root".into()));
            }
            Ok(write)
        }))?;
        for table in [ROWS, EDGES, CONFIGS, ROW_GRAPH] {
            if !write.open_table(table).map_err(RedbPersistence::storage_error)?.is_empty().map_err(RedbPersistence::storage_error)? {
                return Err(invalid());
            }
        }
        if !write.open_table(EDGE_KEYS).map_err(RedbPersistence::storage_error)?.is_empty().map_err(RedbPersistence::storage_error)?
            || !write.open_table(OCCURRENCES).map_err(RedbPersistence::storage_error)?.is_empty().map_err(RedbPersistence::storage_error)?
            || !write.open_table(CHANGES).map_err(RedbPersistence::storage_error)?.is_empty().map_err(RedbPersistence::storage_error)?
            || !write.open_table(GROUPS).map_err(RedbPersistence::storage_error)?.is_empty().map_err(RedbPersistence::storage_error)? {
            return Err(invalid());
        }
        {
            let mut commits = write.open_table(COMMITS).map_err(RedbPersistence::storage_error)?;
            if !commits.is_empty().map_err(RedbPersistence::storage_error)? { return Err(invalid()); }
            let owner = writer_source::SourceWindowOwner::Read(
                crate::memory_accounting::persistence_original_projection(original)?.read_budget()?);
            let mut cursor = root.commit_cursor(owner, Lsn(0), root.lsn, false)?;
            while let Some((lsn, tx)) = cursor.next()? {
                if commits.get(tx.0).map_err(RedbPersistence::storage_error)?.is_none() {
                    commits.insert(tx.0, lsn.0).map_err(RedbPersistence::storage_error)?;
                }
            }
        }
        Ok(Self { write, sealed: None, root: root.clone() })
    }
    pub(crate) fn retain_original_owner(&mut self, events: &crate::database::event_bus::OwnedEventBusHandle) -> Result<()> {
        self.write.retain_original_owner(events)
    }
    pub(crate) fn commit_lsn(&self, tx: TxId) -> Result<Option<Lsn>> {
        self.write.open_table(COMMITS).map_err(RedbPersistence::storage_error)?
            .get(tx.0).map_err(RedbPersistence::storage_error).map(|value| value.map(|v| Lsn(v.value())))
    }
    pub(crate) fn write(&self) -> &redb::WriteTransaction { &self.write }
    pub(crate) fn blob_write(&self, original:&crate::memory_accounting::OwnedMemoryReservation) -> Result<(&redb::WriteTransaction,u64)> {
        self.mutable(original)?;
        Ok((&self.write,self.write.control_identity))
    }
    fn mutable(&self, original: &crate::memory_accounting::OwnedMemoryReservation) -> Result<()> {
        self.write.require_original(original)?;
        if self.sealed.is_some() { return Err(invalid()); }
        Ok(())
    }
    pub(crate) fn push<T: serde::Serialize>(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        table: TableDefinition<'static, u64, &'static [u8]>, value: &T) -> Result<()> {
        self.mutable(original)?;
        let window = OriginalPersistenceFrame::new(original)?;
        let encoded = window.encode(value)?;
        let mut table = self.write.open_table(table).map_err(RedbPersistence::storage_error)?;
        let next = table.len().map_err(RedbPersistence::storage_error)?;
        next.checked_add(1).ok_or_else(invalid)?;
        if table.insert(next, encoded.as_slice()).map_err(RedbPersistence::storage_error)?.is_some() { return Err(invalid()); }
        Ok(())
    }
    pub(crate) fn push_row_graph(&self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        edge: &AdjEntry, owner: &contextdb_tx::RowEdgeOwner,
    ) -> Result<()> {
        let window = OriginalPersistenceFrame::new(original)?;
        window.record(edge)?;
        window.record(owner)?;
        let owned = contextdb_tx::OwnedAdjEntry { owner: owner.clone(), entry: edge.clone() };
        self.push(original, ROW_GRAPH, &owned)
    }
    pub(crate) fn push_config(&self, original: &crate::memory_accounting::OwnedMemoryReservation, key: &str) -> Result<()> {
        self.push(original, CONFIGS, &key)
    }
    pub(crate) fn contains_config(&self, original: &crate::memory_accounting::OwnedMemoryReservation, key: &str) -> Result<bool> {
        self.write.require_original(original)?;
        let mut found = false;
        visit::<String>(&self.write,original,CONFIGS,|selected| { found |= selected == key; Ok(()) })?;
        Ok(found)
    }
    pub(crate) fn push_edge(&self, store: &RedbPersistence,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        edge: &AdjEntry) -> Result<()> {
        self.mutable(original)?;
        self.write.require_store(store)?;
        self.root.require_working_store(store)?;
        let window = OriginalPersistenceFrame::new(original)?;
        let _image = store.original_graph_window(&window)?;
        window.record(edge)?;
        window.reserve(edge.edge_type.len().saturating_add(49))?;
        let physical = RedbPersistence::graph_fwd_key(edge);
        // A decoded adjacency is a request, never authority. Re-read the exact
        // predecessor in this same write and authenticate it at the pinned root.
        let forward = self.write.open_table(GRAPH_FWD_TABLE).map_err(RedbPersistence::storage_error)?;
        let body = forward.get(physical.as_slice()).map_err(RedbPersistence::storage_error)?.ok_or_else(invalid)?;
        let actual: AdjEntry = window.decode(body.value())?;
        if actual != *edge { return Err(invalid()); }
        let owner = writer_source::SourceWindowOwner::Read(
            crate::memory_accounting::persistence_original_projection(original)?.read_budget()?);
        self.root.verify_graph_edge_at_root(&physical, body.value(), edge, false, &owner)?;
        let receipt_key = graph_history_record::lifecycle_key(&physical)?;
        let receipts = self.write.open_table(graph_history_record::GRAPH_PREDECESSOR_LIFECYCLE)
            .map_err(RedbPersistence::storage_error)?;
        let deletion_count = if let Some(receipt) = receipts.get(receipt_key.as_slice()).map_err(RedbPersistence::storage_error)? {
            let bytes = receipt.value();
            if bytes.len() != 18 || bytes[0] != 1 || bytes[17] != 0
                || edge.deleted_tx != Some(TxId(u64::from_be_bytes(bytes[1..9].try_into().unwrap()))) {
                return Err(invalid());
            }
            let count = u64::from_be_bytes(bytes[9..17].try_into().unwrap());
            if count == 0 { return Err(invalid()); }
            count
        } else { 0 };
        if self.write.open_table(EDGE_KEYS).map_err(RedbPersistence::storage_error)?
            .insert(physical.as_slice(), 0).map_err(RedbPersistence::storage_error)?.is_some() {
            return Err(Error::SyncError("authoritative purge edge-row witness has no exact forward adjacency".into()));
        }
        self.push(original, EDGES, edge)?;
        // Creation records have no private deletion witness: preserve their
        // existing one-per-adjacency matching. Deletions use a disjoint key
        // space and the complete authenticated private predecessor instead.
        let creation = window.encode(&(0u8, edge.source, edge.target, &edge.edge_type, edge.lsn))?;
        let deletion = window.encode(&(2u8, physical.as_slice()))?;
        let mut occurrences = self.write.open_table(OCCURRENCES).map_err(RedbPersistence::storage_error)?;
        let count = occurrences.get(creation.as_slice()).map_err(RedbPersistence::storage_error)?
            .map(|value| window.decode::<u64>(value.value())).transpose()?.unwrap_or(0);
        let count = window.encode(&count.checked_add(1).ok_or_else(invalid)?)?;
        occurrences.insert(creation.as_slice(), count.as_slice()).map_err(RedbPersistence::storage_error)?;
        if deletion_count != 0 {
            let count = window.encode(&deletion_count)?;
            if occurrences.insert(deletion.as_slice(), count.as_slice()).map_err(RedbPersistence::storage_error)?.is_some() {
                return Err(invalid());
            }
        }
        Ok(())
    }
    pub(crate) fn select_changes(&self, store: &RedbPersistence,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        mut selected: impl FnMut(&ChangeLogEntry) -> bool) -> Result<()> {
        self.mutable(original)?;
        self.write.require_store(store)?;
        self.root.require_working_store(store)?;
        let owner = writer_source::SourceWindowOwner::Read(
            crate::memory_accounting::persistence_original_projection(original)?.read_budget()?);
        // This cursor is constructed here, from begin's exact root and Original.
        // No caller can supply a decoded witness, occurrence id or coordinate.
        let mut cursor = self.root.change_log_cursor(owner, Lsn(0), self.root.lsn)?;
        while let Some(source) = cursor.next_stored_change(|bytes, record| {
            crate::read_image_memory::reserve(bytes.len())?;
            Ok((record, bytes.to_vec()))
        })? {
            let window = OriginalPersistenceFrame::new(original)?;
            let _image = store.original_graph_window(&window)?;
            let (record, stored_bytes) = &source.value;
            let mut graph_selected = false;
            let key = match &record.entry {
                ChangeLogEntry::EdgeDelete {..} => record.witness.as_ref()
                    .map(|w| window.encode(&(2u8, w.predecessor.as_slice()))).transpose()?,
                ChangeLogEntry::EdgeInsert {source,target,edge_type,lsn} =>
                    Some(window.encode(&(0u8, source, target, edge_type, lsn))?),
                _ => None,
            };
            if let Some(key) = key {
                let mut occurrences = self.write.open_table(OCCURRENCES).map_err(RedbPersistence::storage_error)?;
                let count = occurrences.get(key.as_slice()).map_err(RedbPersistence::storage_error)?
                    .map(|v| window.decode::<u64>(v.value())).transpose()?;
                if let Some(witness) = &record.witness {
                    let selected_predecessor = self.write.open_table(EDGE_KEYS).map_err(RedbPersistence::storage_error)?
                        .get(witness.predecessor.as_slice()).map_err(RedbPersistence::storage_error)?.is_some();
                    if selected_predecessor && count.is_none() { return Err(invalid()); }
                }
                if let Some(count) = count {
                    // For deletions the source authenticated full stored bytes,
                    // this distinct occurrence id's location and contribution,
                    // and its exact private predecessor and lifecycle receipt.
                    let remaining = count.checked_sub(1).ok_or_else(invalid)?;
                    if remaining == 0 {
                        occurrences.remove(key.as_slice()).map_err(RedbPersistence::storage_error)?;
                    } else {
                        let count = window.encode(&remaining)?;
                        occurrences.insert(key.as_slice(), count.as_slice()).map_err(RedbPersistence::storage_error)?;
                    }
                    graph_selected = true;
                }
            }
            if graph_selected || selected(&record.entry) {
                self.push_change(original, source.position(), stored_bytes)?;
            }
        }
        Ok(())
    }
    fn push_change(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        position: &[u8], stored_bytes: &[u8]) -> Result<()> {
        self.mutable(original)?;
        let position: &[u8;16] = position.try_into().map_err(|_| invalid())?;
        if self.write.open_table(CHANGES).map_err(RedbPersistence::storage_error)?
            .insert(position.as_slice(), stored_bytes).map_err(RedbPersistence::storage_error)?.is_some() { return Err(invalid()); }
        self.write.open_table(GROUPS).map_err(RedbPersistence::storage_error)?
            .insert(u64::from_be_bytes(position[..8].try_into().unwrap()), 0).map_err(RedbPersistence::storage_error)?;
        Ok(())
    }
    pub(crate) fn seal(&mut self, original: &crate::memory_accounting::OwnedMemoryReservation) -> Result<()> {
        self.mutable(original)?;
        if !self.write.open_table(OCCURRENCES).map_err(RedbPersistence::storage_error)?.is_empty().map_err(RedbPersistence::storage_error)? {
            return Err(Error::SyncError("authoritative purge edge change-log ownership is incomplete".into()));
        }
        self.sealed = Some(roots(&self.write)?);
        Ok(())
    }
    pub(crate) fn into_write(self, store: &RedbPersistence,
        original: &crate::memory_accounting::OwnedMemoryReservation) -> Result<received_working::ReceivedWorkingWrite> {
        self.write.require_store(store)?;
        self.write.require_original(original)?;
        if self.sealed != Some(roots(&self.write)?) { return Err(invalid()); }
        Ok(self.write)
    }
}
fn roots(write: &redb::WriteTransaction) -> Result<[Option<Option<[u8;32]>>;14]> {
    let mut roots = [None;14];
    for (root, name) in roots.iter_mut().zip([ROWS.name(), EDGES.name(), CONFIGS.name(), EDGE_KEYS.name(),
        OCCURRENCES.name(), CHANGES.name(), GROUPS.name(), COMMITS.name(), ROW_GRAPH.name(),
        crate::blob_repository::native_purge::WITNESSES.name(),crate::blob_repository::native_purge::NEXT.name(),
        crate::blob_repository::native_purge::OBJECTS.name(),crate::blob_repository::native_purge::CONTROL.name(),
        crate::blob_repository::native_purge::BYTES.name()]) {
        *root = write.normal_table_root(name).map_err(RedbPersistence::storage_error)?;
        if root.is_none() { return Err(invalid()); }
    }
    Ok(roots)
}
fn invalid() -> Error { Error::Other("discard history selection lost its exact unpublished write".into()) }

pub(crate) fn visit<T: serde::de::DeserializeOwned>(write: &redb::WriteTransaction,
    original: &crate::memory_accounting::OwnedMemoryReservation,
    table: TableDefinition<'static,u64,&'static [u8]>, mut consume: impl FnMut(&T) -> Result<()>) -> Result<()> {
    let count = write.open_table(table).map_err(RedbPersistence::storage_error)?.len().map_err(RedbPersistence::storage_error)?;
    for ordinal in 0..count {
        with_record(write, original, table, ordinal, |value| consume(value))?;
    }
    Ok(())
}
pub(crate) fn with_record<T: serde::de::DeserializeOwned, R>(write: &redb::WriteTransaction,
    original: &crate::memory_accounting::OwnedMemoryReservation,
    table: TableDefinition<'static,u64,&'static [u8]>, ordinal: u64, consume: impl FnOnce(&T) -> Result<R>) -> Result<R> {
    let window = OriginalPersistenceFrame::new(original)?;
    let value: T = {
        let source = write.open_table(table).map_err(RedbPersistence::storage_error)?;
        let encoded = source.get(ordinal).map_err(RedbPersistence::storage_error)?.ok_or_else(invalid)?;
        window.decode(encoded.value())?
    };
    consume(&value)
}
pub(crate) fn row_count(write: &redb::WriteTransaction) -> Result<u64> {
    write.open_table(ROWS).map_err(RedbPersistence::storage_error)?.len().map_err(RedbPersistence::storage_error)
}

// A published erasure fence survives both the transaction and its stack
// Original. It owns one admitted descendant and an immediately unlinked
// immutable backend. An on-disk offset index permits exact binary search;
// record authentication binds ordinal, offset, length and the complete key.
pub(super) const FENCE_KEYS: TableDefinition<&[u8],u8> = TableDefinition::new("__discard_fence_keys");
pub(super) struct FrozenGraphErasure {
    backend: Box<dyn redb::ScratchBackend>,
    count: u64,
    end: u64,
    _memory: crate::memory_accounting::OwnedMemoryReservation,
}
impl FrozenGraphErasure {
    pub(super) fn build(store: &RedbPersistence, original: &crate::memory_accounting::OwnedMemoryReservation,
        write: &redb::WriteTransaction) -> Result<Self> {
        let memory = crate::memory_accounting::reserve_preparation_from_original(original,
            redb::scratch_backend_allocation_size().saturating_add(std::mem::size_of::<Self>())
                .saturating_add(4 * std::mem::size_of::<usize>()),
            "discard", "graph_erasure_fence", "Raise MEMORY_LIMIT to retain the graph erasure fence.")?;
        let writer = store.writer_file.read().map_err(|_| Self::corrupt())?;
        let backend = writer.as_ref().ok_or_else(Self::corrupt)?.scratch_backend().map_err(Self::io)?;
        let keys = write.open_table(FENCE_KEYS).map_err(RedbPersistence::storage_error)?;
        let count = keys.len().map_err(RedbPersistence::storage_error)?;
        let mut result = Self {backend, count, end:count.checked_mul(8).ok_or_else(Self::corrupt)?, _memory:memory};
        result.backend.set_len(result.end).map_err(Self::io)?;
        let mut ordinal = 0u64;
        for item in keys.iter().map_err(RedbPersistence::storage_error)? {
            let (key,_) = item.map_err(RedbPersistence::storage_error)?;
            let key = key.value();
            let len = u64::try_from(key.len()).map_err(|_| Self::corrupt())?;
            let at = result.end;
            let end = at.checked_add(40).and_then(|end|end.checked_add(len)).ok_or_else(Self::corrupt)?;
            let mut hash = Self::hasher(ordinal,at,len);
            hash.update(key);
            result.backend.set_len(end).map_err(Self::io)?;
            result.backend.write(ordinal.checked_mul(8).ok_or_else(Self::corrupt)?,&at.to_be_bytes()).map_err(Self::io)?;
            result.backend.write(at,&len.to_be_bytes()).map_err(Self::io)?;
            result.backend.write(at+8,hash.finalize().as_bytes()).map_err(Self::io)?;
            result.backend.write(at+40,key).map_err(Self::io)?;
            result.end = end;
            ordinal = ordinal.checked_add(1).ok_or_else(Self::corrupt)?;
        }
        if ordinal != count { return Err(Self::corrupt()); }
        Ok(result)
    }
    fn hasher(ordinal:u64, at:u64, len:u64) -> blake3::Hasher {
        let mut hash = blake3::Hasher::new();
        hash.update(b"contextdb-discard-graph-fence");
        hash.update(&ordinal.to_be_bytes()); hash.update(&at.to_be_bytes()); hash.update(&len.to_be_bytes());
        hash
    }
    fn corrupt() -> Error {
        Error::StoreCorrupted {path:"discard graph erasure fence".into(),
            reason:"immutable graph erasure index or key authentication failed".into()}
    }
    fn io(error: std::io::Error) -> Error {
        Error::ScratchWorkingStorageUnavailable { operation:"discard_graph_erasure".into(), io_kind:error.kind() }
    }
    fn header(&self, ordinal:u64) -> Result<(u64,u64,[u8;32])> {
        if ordinal >= self.count { return Err(Self::corrupt()); }
        let mut offset = [0u8;8];
        self.backend.read(ordinal.checked_mul(8).ok_or_else(Self::corrupt)?,&mut offset).map_err(Self::io)?;
        let at = u64::from_be_bytes(offset);
        let mut header = [0u8;40];
        if at < self.count.checked_mul(8).ok_or_else(Self::corrupt)?
            || at.checked_add(40).is_none_or(|end|end > self.end) { return Err(Self::corrupt()); }
        self.backend.read(at,&mut header).map_err(Self::io)?;
        let len = u64::from_be_bytes(header[..8].try_into().unwrap());
        if at.checked_add(40).and_then(|end|end.checked_add(len)).is_none_or(|end|end > self.end) { return Err(Self::corrupt()); }
        Ok((at,len,header[8..].try_into().unwrap()))
    }
    pub(super) fn copy_keys(&self, original:&crate::memory_accounting::OwnedMemoryReservation,
        write:&redb::WriteTransaction) -> Result<()> {
        for ordinal in 0..self.count {
            let window = OriginalPersistenceFrame::new(original)?;
            let (at,len,expected) = self.header(ordinal)?;
            let len_usize = usize::try_from(len).map_err(|_|Self::corrupt())?;
            window.reserve(len_usize)?;
            let mut key = vec![0u8;len_usize];
            self.backend.read(at+40,&mut key).map_err(Self::io)?;
            let mut hash = Self::hasher(ordinal,at,len); hash.update(&key);
            if hash.finalize().as_bytes() != &expected { return Err(Self::corrupt()); }
            write.open_table(FENCE_KEYS).map_err(RedbPersistence::storage_error)?
                .insert(key.as_slice(),0).map_err(RedbPersistence::storage_error)?;
        }
        Ok(())
    }
    pub(super) fn retained_memory_bytes(&self) -> usize { self._memory.bytes() }
    pub(super) fn contains(&self, key:&[u8], mut checkpoint: Option<&mut dyn FnMut() -> Result<()>>) -> Result<bool> {
        let (mut lower,mut upper) = (0u64,self.count);
        let mut buffer = [0u8;4096];
        while lower < upper {
            if let Some(checkpoint) = checkpoint.as_mut() { checkpoint()?; }
            let ordinal = lower + (upper-lower)/2;
            let (at,len,expected) = self.header(ordinal)?;
            let mut hash = Self::hasher(ordinal,at,len);
            let mut order = std::cmp::Ordering::Equal;
            let mut offset = 0u64;
            while offset < len {
                if let Some(checkpoint) = checkpoint.as_mut() { checkpoint()?; }
                let size = usize::try_from((len-offset).min(buffer.len() as u64)).map_err(|_|Self::corrupt())?;
                self.backend.read(at+40+offset,&mut buffer[..size]).map_err(Self::io)?;
                hash.update(&buffer[..size]);
                if order == std::cmp::Ordering::Equal {
                    let start = usize::try_from(offset).map_err(|_|Self::corrupt())?;
                    let remaining = key.get(start..).unwrap_or(&[]);
                    let shared = size.min(remaining.len());
                    order = buffer[..shared].cmp(&remaining[..shared]);
                    if order == std::cmp::Ordering::Equal && shared < size { order = std::cmp::Ordering::Greater; }
                }
                offset += size as u64;
            }
            if let Some(checkpoint) = checkpoint.as_mut() { checkpoint()?; }
            if hash.finalize().as_bytes() != &expected { return Err(Self::corrupt()); }
            if order == std::cmp::Ordering::Equal { order = len.cmp(&(key.len() as u64)); }
            match order {
                std::cmp::Ordering::Equal => return Ok(true),
                std::cmp::Ordering::Less => lower = ordinal.checked_add(1).ok_or_else(Self::corrupt)?,
                std::cmp::Ordering::Greater => upper = ordinal,
            }
        }
        Ok(false)
    }
}

const TABLES: TableDefinition<&str,u8> = TableDefinition::new("__discard_tables");
impl RedbPersistence {
    pub(super) fn erase_original_discard_rows(&self, original: &OriginalPersistenceFrame<'_>,
        write: &redb::WriteTransaction, mutations: &mut schema_projection::OriginalMutationLog<'_, '_, '_>) -> Result<()> {
        let controls = OriginalPersistenceFrame::new(original.original)?;
        controls.reserve(4096)?;
        if !write.open_table(TABLES).map_err(Self::storage_error)?.is_empty().map_err(Self::storage_error)? { return Err(invalid()); }
        // Preserve the existing first-table destructive fault boundary. The
        // probe names the last occurrence of the first table even when input
        // selections interleave tables; only one table name and scalar remain.
        #[cfg(any(test,feature="test-seams"))]
        let first_table_end = {
            let mut first = None::<String>;
            let mut last = None::<u64>;
            let mut ordinal = 0u64;
            visit::<Row>(write,original.original,ROWS,|(table,_,_,_)| {
                if first.is_none() { first = Some(controls.copy_text(table)?); }
                if first.as_deref() == Some(table.as_str()) { last = Some(ordinal); }
                ordinal = ordinal.checked_add(1).ok_or_else(invalid)?;
                Ok(())
            })?;
            last
        };
        #[cfg(any(test,feature="test-seams"))]
        let mut row_ordinal = 0u64;
        // Expiry removals retain their established before-row phase.
        visit::<Row>(write,original.original,ROWS,|(table,row,created,_)| {
            let window = OriginalPersistenceFrame::new(original.original)?;
            window.reserve(table.len().saturating_add(256))?;
            let expiry = crate::composite_store::retention_expiry_key(table,*row,*created);
            write.open_table(CONFIG_TABLE).map_err(Self::storage_error)?.remove(expiry.as_str()).map_err(Self::storage_error)?;
            mutations.extend(content_identity::original_producers::config_mutations(&window,&expiry,None)?)
        })?;
        let mut commitments = historical_index_commitment::CommitmentBatch::for_original(original.original)?;
        visit::<Row>(write, original.original, ROWS, |(table,row,created,lsn)| {
            let window = OriginalPersistenceFrame::new(original.original)?;
            let _image = self.original_graph_window(&window)?;
            window.reserve(table.len().saturating_mul(8).saturating_add(512))?;
            let key = Self::rel_row_key_from_parts(*row,*created,*lsn);
            let archived = {
                let archive_key = authored_history::body_key(table,&key);
                if write.normal_table_root(authored_history::BODIES.name()).map_err(Self::storage_error)?.is_some() {
                    write.open_table(authored_history::BODIES).map_err(Self::storage_error)?
                        .remove(archive_key.as_str()).map_err(Self::storage_error)?.is_some()
                } else { false }
            };
            let meta = {
                let source = write.open_table(META_TABLE).map_err(Self::storage_error)?;
                source.get(Self::meta_key(table).as_str()).map_err(Self::storage_error)?
                    .map(|value| Self::decode_table_meta_versioned_original(&window,value.value()).map(|(meta,_)|meta)).transpose()?
            };
            #[cfg(any(test,feature="test-seams"))]
            let has_live_table = meta.is_some();
            if let Some(meta) = meta {
                window.reserve(meta.estimated_bytes().saturating_mul(8).saturating_add(4096))?;
                let name = Self::rel_table_name(table);
                let mut relation = content_size::open_rows(write,TableDefinition::new(name.as_str()),Some(&meta)).map_err(Self::storage_error)?;
                let present = {
                    let predecessor = relation.get(key.as_slice()).map_err(Self::storage_error)?;
                    self.replace_relational_manifest_in_write_original(original.original,write,table,key.as_slice(),
                        predecessor.as_ref().map(|value|value.value()),None)?;
                    predecessor.is_some()
                };
                relation.remove_body(&mut commitments,key.as_slice())?;
                if !present && !archived { return Err(Error::SyncError("authoritative purge selected relational version disappeared before durable commit".into())); }
                write.open_table(TABLES).map_err(Self::storage_error)?.insert(table.as_str(),0).map_err(Self::storage_error)?;
            } else if !archived {
                return Err(Error::SyncError("authoritative purge lost its retired row occurrence".into()));
            }
            mutations.push(content_identity::original_producers::remove_row_mutation(&window,table,*row)?)?;
            #[cfg(any(test,feature="test-seams"))]
            {
                if has_live_table && Some(row_ordinal) == first_table_end { erasure_first_table_staged_for_test()?; }
                row_ordinal = row_ordinal.checked_add(1).ok_or_else(invalid)?;
            }
            Ok(())
        })?;
        // Rebuild once per selected table. The helper streams physical rows,
        // one current row/run, and on-disk counts rather than all versions.
        {
            let tables = write.open_table(TABLES).map_err(Self::storage_error)?;
            for item in tables.iter().map_err(Self::storage_error)? {
                let window = OriginalPersistenceFrame::new(original.original)?;
                let (table,_) = item.map_err(Self::storage_error)?;
                let table = window.copy_text(table.value())?;
                let meta = {
                    let source = write.open_table(META_TABLE).map_err(Self::storage_error)?;
                    let value = source.get(Self::meta_key(&table).as_str()).map_err(Self::storage_error)?.ok_or_else(invalid)?;
                    Self::decode_table_meta_versioned_original(&window,value.value())?.0
                };
                self.rebuild_discard_relational_indexes(&window,write,&table,&meta,&mut commitments,mutations)?;
            }
        }
        let (publications, credit) = commitments.finish(write)?.into_parts();
        mutations.extend(publications)?;
        drop(credit);
        Ok(())
    }
    pub(super) fn erase_original_discard_graph(&self, original: &OriginalPersistenceFrame<'_>,
        write: &redb::WriteTransaction, mutations: &mut schema_projection::OriginalMutationLog<'_, '_, '_>) -> Result<()> {
        visit::<AdjEntry>(write,original.original,EDGES,|entry| {
            let window = OriginalPersistenceFrame::new(original.original)?;
            let _image = self.original_graph_window(&window)?;
            window.reserve(entry.edge_type.len().saturating_mul(4).saturating_add(256))?;
            let forward_key = Self::graph_fwd_key(entry);
            let reverse_key = Self::graph_rev_key(entry);
            let mut forward = write.open_table(GRAPH_FWD_TABLE).map_err(Self::storage_error)?;
            let mut reverse = write.open_table(GRAPH_REV_TABLE).map_err(Self::storage_error)?;
            {
                let body = forward.get(observed_graph_key!("graph_fwd","get",forward_key.as_slice())).map_err(Self::storage_error)?
                    .ok_or_else(||Error::StoreCorrupted {path:self.path.display().to_string(),reason:"purged graph forward projection missing".into()})?;
                let other = reverse.get(observed_graph_key!("graph_rev","get",reverse_key.as_slice())).map_err(Self::storage_error)?
                    .ok_or_else(||Error::StoreCorrupted {path:self.path.display().to_string(),reason:"purged graph reverse projection missing".into()})?;
                if body.value() != other.value() { return Err(Error::StoreCorrupted {path:self.path.display().to_string(),reason:"purged graph projections disagree".into()}); }
                let actual: AdjEntry = window.decode(body.value())?;
                if actual != *entry { return Err(Error::SyncError("authoritative purge selected graph predecessor changed before durable commit".into())); }
                Self::reclaim_graph_creation_in_write(write,&forward_key,body.value())?;
            }
            if forward.remove(observed_graph_key!("graph_fwd","remove",forward_key.as_slice(),5)).map_err(Self::storage_error)?.is_none()
                || reverse.remove(observed_graph_key!("graph_rev","remove",reverse_key.as_slice(),5)).map_err(Self::storage_error)?.is_none() {
                return Err(Error::SyncError("authoritative purge selected graph occurrence disappeared before durable commit".into()));
            }
            mutations.push(content_identity::original_producers::remove_edge_mutation(&window,entry.source,&entry.edge_type,entry.target)?)?;
            Ok(())
        })?;
        visit::<contextdb_tx::OwnedAdjEntry>(write, original.original, ROW_GRAPH, |owned| {
            self.graph_writer_window(|| Self::erase_row_graph_in_write(write, std::slice::from_ref(owned)))
        })?;
        Ok(())
    }
    pub(super) fn erase_original_discard_configs(&self, original: &OriginalPersistenceFrame<'_>,
        write: &redb::WriteTransaction, mutations: &mut schema_projection::OriginalMutationLog<'_, '_, '_>) -> Result<()> {
        visit::<String>(write,original.original,CONFIGS,|key| {
            let window = OriginalPersistenceFrame::new(original.original)?;
            write.open_table(CONFIG_TABLE).map_err(Self::storage_error)?.remove(key.as_str()).map_err(Self::storage_error)?;
            mutations.extend(content_identity::original_producers::config_mutations(&window,key,None)?)
        })?;
        Ok(())
    }
    pub(super) fn finish_original_discard_history(&self, write: &redb::WriteTransaction) -> Result<()> {
        // No selected record or disk selector survives the one terminal commit.
        for table in [ROWS,EDGES,CONFIGS,ROW_GRAPH] { write.delete_table(table).map_err(Self::storage_error)?; }
        write.delete_table(EDGE_KEYS).map_err(Self::storage_error)?;
        write.delete_table(OCCURRENCES).map_err(Self::storage_error)?;
        write.delete_table(CHANGES).map_err(Self::storage_error)?;
        write.delete_table(GROUPS).map_err(Self::storage_error)?;
        write.delete_table(COMMITS).map_err(Self::storage_error)?;
        write.delete_table(TABLES).map_err(Self::storage_error)?;
        Ok(())
    }

    pub(super) fn retire_discard_change_log(&self, original: &OriginalPersistenceFrame<'_>, write: &redb::WriteTransaction,
        mutations: &mut schema_projection::OriginalMutationLog<'_, '_, '_>) -> Result<()> {
        if !write.open_table(SURVIVORS).map_err(Self::storage_error)?.is_empty().map_err(Self::storage_error)? { return Err(invalid()); }
        let groups = write.open_table(GROUPS).map_err(Self::storage_error)?;
        for item in groups.iter().map_err(Self::storage_error)? {
            let group = Lsn(item.map_err(Self::storage_error)?.0.value());
            let prefix_window = OriginalPersistenceFrame::new(original.original)?;
            prefix_window.reserve(256)?;
            let prefix = format!("{:020}:",group.0);
            let mut after = None::<(String,OriginalPersistenceFrame<'_>)>;
            let mut survivors = 0u64;
            loop {
                let key_window = OriginalPersistenceFrame::new(original.original)?;
                let window = OriginalPersistenceFrame::new(original.original)?;
                let _image = self.original_graph_window(&window)?;
                let next = {
                    let source = write.open_table(CHANGE_LOG_TABLE).map_err(Self::storage_error)?;
                    let lower = after.as_ref().map_or(Bound::Included(prefix.as_str()),|(key,_)|Bound::Excluded(key.as_str()));
                    source.range::<&str>((lower,Bound::Unbounded)).map_err(Self::storage_error)?.next()
                        .transpose().map_err(Self::storage_error)?.filter(|(key,_)|key.value().starts_with(&prefix))
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
                let coordinate = Self::numeric_change_key(&key)?;
                let selected = {
                    let mut selected = write.open_table(CHANGES).map_err(Self::storage_error)?;
                    let wanted = selected.remove(coordinate.as_slice()).map_err(Self::storage_error)?;
                    if let Some(wanted) = wanted {
                        // Compare the complete authenticated stored occurrence,
                        // including private tag, predecessor and occurrence id.
                        if wanted.value() != bytes.as_slice() { return Err(Error::SyncError("authoritative purge change-log witness changed at its physical coordinate".into())); }
                        true
                    } else { false }
                };
                write.open_table(CHANGE_LOG_TABLE).map_err(Self::storage_error)?.remove(observed_graph_key!("history","remove",key.as_str(),8)).map_err(Self::storage_error)?;
                Self::detach_change_log_selectors_in_write(write,&key,!selected)?;
                mutations.push(content_identity::original_producers::remove_encoded_history_mutation(&window,&key,&record.entry)?)?;
                if !selected {
                    // Text keys have only a minimum six-digit width. Stage by
                    // the original numeric ordinal before assigning dense ones.
                    if write.open_table(SURVIVORS).map_err(Self::storage_error)?
                        .insert(u64::from_be_bytes(coordinate[8..].try_into().unwrap()),bytes.as_slice())
                        .map_err(Self::storage_error)?.is_some() { return Err(invalid()); }
                    survivors = survivors.checked_add(1).ok_or_else(invalid)?;
                }
                drop(bytes); drop(record); drop(_image);
                after = Some((key,key_window));
            }
            drop(after);
            for ordinal in 0..survivors {
                let window = OriginalPersistenceFrame::new(original.original)?;
                let _image = self.original_graph_window(&window)?;
                let bytes = {
                    let mut source = write.open_table(SURVIVORS).map_err(Self::storage_error)?;
                    let first = source.iter().map_err(Self::storage_error)?.next()
                        .transpose().map_err(Self::storage_error)?.map(|(key,_)| key.value()).ok_or_else(invalid)?;
                    let value = source.remove(first).map_err(Self::storage_error)?.ok_or_else(invalid)?;
                    window.copy_bytes(value.value())?
                };
                let _record = Self::decode_stored_change_log_original(&window,&bytes)?;
                let mut key = window.copy_text("")?;
                window.reserve(Self::change_log_entry_key_len())?;
                key.reserve_exact(Self::change_log_entry_key_len());
                Self::write_change_log_entry_key(group,usize::try_from(ordinal).map_err(|_| invalid())?,&mut key);
                write.open_table(CHANGE_LOG_TABLE).map_err(Self::storage_error)?.insert(observed_graph_key!("history","insert",key.as_str(),8),bytes.as_slice()).map_err(Self::storage_error)?;
                self.graph_writer_window(|| Self::write_carried_change_log_selectors_in_write(write,&key,&bytes))?;
                mutations.push(content_identity::original_producers::private_history_mutation(&window,&key,&bytes)?)?;
            }
        }
        if !write.open_table(CHANGES).map_err(Self::storage_error)?.is_empty().map_err(Self::storage_error)? {
            return Err(Error::SyncError("authoritative purge change-log witness is missing from its LSN group".into()));
        }
        drop(groups);
        write.delete_table(SURVIVORS).map_err(Self::storage_error)?;
        Ok(())
    }
}

const COUNT_MEMBERS: TableDefinition<&[u8],u8> = TableDefinition::new("__discard_count_members");
type IndexHead = (RowId, IndexKey, TxId, Option<TxId>);
impl RedbPersistence {
    fn rebuild_discard_relational_indexes(&self, original: &OriginalPersistenceFrame<'_>,
        write: &redb::WriteTransaction, table: &str, meta: &TableMeta,
        commitments: &mut historical_index_commitment::CommitmentBatch,
        mutations: &mut schema_projection::OriginalMutationLog<'_, '_, '_>) -> Result<()> {
        if !write.open_table(COUNT_MEMBERS).map_err(Self::storage_error)?.is_empty().map_err(Self::storage_error)? { return Err(invalid()); }
        // Retire counts with one continuation key, not a retained key list.
        let mut after = None::<(Vec<u8>,OriginalPersistenceFrame<'_>)>;
        loop {
            let key_window = OriginalPersistenceFrame::new(original.original)?;
            let window = OriginalPersistenceFrame::new(original.original)?;
            let next = {
                let source = write.open_table(RELATIONAL_INDEX_COUNT_TABLE).map_err(Self::storage_error)?;
                let lower = after.as_ref().map_or(Bound::Unbounded,|(key,_)|Bound::Excluded(key.as_slice()));
                source.range::<&[u8]>((lower,Bound::Unbounded)).map_err(Self::storage_error)?.next()
                    .transpose().map_err(Self::storage_error)?.map(|(key,_)| -> Result<_> {
                        let decoded: EncodedRelationalIndexCountKey = window.decode(key.value())?;
                        Ok((key_window.copy_bytes(key.value())?, decoded.table == table))
                    }).transpose()?
            };
            let Some((key, selected)) = next else { break; };
            if selected {
                write.open_table(RELATIONAL_INDEX_COUNT_TABLE).map_err(Self::storage_error)?.remove(key.as_slice()).map_err(Self::storage_error)?;
                window.reserve(key.len().saturating_add(256))?;
                mutations.push(Self::remove_relational_index_count_mutation(&key))?;
            }
            after = Some((key,key_window));
        }
        drop(after);
        let declarations_window = OriginalPersistenceFrame::new(original.original)?;
        declarations_window.reserve(meta.estimated_bytes().saturating_mul(8).saturating_add(4096))?;
        let declarations = Self::relational_index_declarations(meta);
        for declaration in declarations {
            let declaration_window = OriginalPersistenceFrame::new(original.original)?;
            declaration_window.record(&declaration)?;
            declaration_window.reserve(table.len().saturating_add(declaration.name.len()).saturating_add(256))?;
            let name = Self::relational_index_table_name(table,&declaration.name);
            let definition = TableDefinition::<DurableRelationalIndexKey,&[u8]>::new(&name);
            {
                let source = write.open_table(definition).map_err(Self::storage_error)?;
                for item in source.iter().map_err(Self::storage_error)? {
                    let window = OriginalPersistenceFrame::new(original.original)?;
                    let _image = self.original_graph_window(&window)?;
                    let decoded = decode_relational_index_entry(&self.path,Some(item.map_err(Self::storage_error)?))?.ok_or_else(invalid)?;
                    let (_,position,deleted_tx) = decoded;
                    Self::validated_index_position(&declaration,meta,&position).map_err(|reason|Error::StoreCorrupted {
                        path:self.path.display().to_string(),reason,
                    })?;
                    if deleted_tx.is_none() {
                        window.reserve(position.key.iter().map(|part| match part {
                            DirectedValue::Asc(TotalOrdAsc(value)) | DirectedValue::Desc(TotalOrdDesc(value)) => value.estimated_bytes(),
                        }).sum::<usize>().saturating_mul(4).saturating_add(256))?;
                        let key = DurableRelationalIndexKey::new(position.key,position.row_id,TxId(0))?;
                        mutations.push(content_identity::original_producers::remove_relational_index_posting_mutation(
                            &window,table,&declaration.name,&key.0)?)?;
                    }
                }
            }
            write.delete_table(definition).map_err(Self::storage_error)?;
            let current_name = Self::current_relational_index_table_name(table,&declaration.name);
            let current_definition = TableDefinition::<DurableRelationalIndexKey,&[u8]>::new(&current_name);
            write.delete_table(current_definition).map_err(Self::storage_error)?;
            commitments.reset(write,table,&declaration,historical_index_commitment::Side::Postings)?;
            commitments.reset(write,table,&declaration,historical_index_commitment::Side::Versions)?;
            let mut postings = historical_index_commitment::HistoricalPostings::open(write,table,&declaration).map_err(Self::storage_error)?;
            // Redb's physical order is (row, created_tx, lsn). Only the latest
            // run of that one row is needed; flushing on row change preserves
            // the previous rebuild's current-key and interval semantics.
            let mut head = None::<(IndexHead,OriginalPersistenceFrame<'_>)>;
            let relation_name = Self::rel_table_name(table);
            let relation = write.open_table(TableDefinition::<&[u8],&[u8]>::new(&relation_name)).map_err(Self::storage_error)?;
            for item in relation.iter().map_err(Self::storage_error)? {
                let window = OriginalPersistenceFrame::new(original.original)?;
                let _image = self.original_graph_window(&window)?;
                let (physical,value) = item.map_err(Self::storage_error)?;
                let row = Self::decode_versioned_row_original(&window,value.value(),Some(meta))?;
                if Self::rel_row_key(&row).as_slice() != physical.value() { return Err(invalid()); }
                #[cfg(any(test, feature="test-seams"))]
                RELATIONAL_INDEX_REBUILD_ROWS_DECODED.with(|count| count.set(count.get().saturating_add(1)));
                window.reserve(row.estimated_bytes().saturating_mul(4).saturating_add(1024))?;
                let key = contextdb_relational::index_key_for_row(&declaration.columns,&row.values);
                commitments.fold_interval(write,table,&declaration,historical_index_commitment::Side::Versions,
                    &historical_index_commitment::key_row(&key,row.row_id)?,row.created_tx,row.deleted_tx,true)?;
                if head.as_ref().is_some_and(|((id,_,_,_),_)|*id != row.row_id) {
                    let (prior,paid) = head.take().expect("selected prior row");
                    self.finish_discard_index_head(&paid,write,table,meta,&declaration,&current_name,&prior,mutations)?;
                }
                let created = match &head {
                    Some(((id, prior, created, end),_)) if *id == row.row_id && *prior == key && *end == Some(row.created_tx) => *created,
                    _ => row.created_tx,
                };
                let durable = DurableRelationalIndexKey::new(key.clone(),row.row_id,created)?;
                postings.put(commitments,write,durable,row.deleted_tx)?;
                let head_window = OriginalPersistenceFrame::new(original.original)?;
                head_window.reserve(row.estimated_bytes().saturating_mul(4).saturating_add(1024))?;
                head = Some(((row.row_id,key,created,row.deleted_tx),head_window));
            }
            drop(relation); drop(postings);
            if let Some((prior,paid)) = head {
                self.finish_discard_index_head(&paid,write,table,meta,&declaration,&current_name,&prior,mutations)?;
            }
        }
        write.delete_table(COUNT_MEMBERS).map_err(Self::storage_error)?;
        write.open_table(RELATIONAL_INDEX_READY_TABLE).map_err(Self::storage_error)?
            .insert(table,RELATIONAL_INDEX_READY_VERSION).map_err(Self::storage_error)?;
        let window = OriginalPersistenceFrame::new(original.original)?;
        mutations.push(content_identity::original_producers::status_mutation(&window,
            RELATIONAL_INDEX_COUNT_READY_IDENTITY_NAMESPACE,table.as_bytes(),RELATIONAL_INDEX_READY_VERSION)?)?;
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    fn finish_discard_index_head(&self, original: &OriginalPersistenceFrame<'_>, write: &redb::WriteTransaction,
        table: &str, meta: &TableMeta, declaration: &IndexDecl, current_name: &str, head: &IndexHead,
        mutations: &mut schema_projection::OriginalMutationLog<'_, '_, '_>) -> Result<()> {
        let (row,key,created,end) = head;
        if end.is_some() { return Ok(()); }
        let window = OriginalPersistenceFrame::new(original.original)?;
        window.reserve(key.len().saturating_mul(std::mem::size_of::<contextdb_core::DirectedValue>()))?;
        for item in key { let value = match item { contextdb_core::DirectedValue::Asc(v) => &v.0, contextdb_core::DirectedValue::Desc(v) => &v.0 }; window.reserve(crate::memory_accounting::preparation_value_copy_bytes(value)?)?; }
        window.reserve(table.len().saturating_add(declaration.name.len()).saturating_add(1024))?;
        let logical = DurableRelationalIndexKey::new(key.clone(),*row,TxId(0))?;
        let encoded = window.encode(created)?;
        write.open_table(TableDefinition::<DurableRelationalIndexKey,&[u8]>::new(current_name)).map_err(Self::storage_error)?
            .insert(&logical,encoded.as_slice()).map_err(Self::storage_error)?;
        mutations.push(content_identity::original_producers::relational_index_posting_mutation(&window,table,&declaration.name,&logical.0)?)?;
        for prefix_len in 1..=key.len() {
            if !crate::maintained_counts::maintained_count_member(meta,&declaration.columns[..prefix_len]) { continue; }
            let window = OriginalPersistenceFrame::new(original.original)?;
            window.reserve(key[..prefix_len].iter().map(|item| match item { contextdb_core::DirectedValue::Asc(v) => v.0.estimated_bytes(), contextdb_core::DirectedValue::Desc(v) => v.0.estimated_bytes() }).sum())?;
            window.record(&declaration.columns[..prefix_len])?;
            window.reserve(table.len().saturating_add(256))?;
            let count_key = Self::relational_index_count_key(table,&declaration.columns[..prefix_len],&key[..prefix_len])?;
            let member = window.encode(&(&count_key,row))?;
            let fresh = write.open_table(COUNT_MEMBERS).map_err(Self::storage_error)?
                .insert(member.as_slice(),0).map_err(Self::storage_error)?.is_none();
            if !fresh { continue; }
            let mut counts = write.open_table(RELATIONAL_INDEX_COUNT_TABLE).map_err(Self::storage_error)?;
            let count = counts.get(count_key.as_slice()).map_err(Self::storage_error)?.map_or(0,|v|v.value())
                .checked_add(1).ok_or_else(invalid)?;
            counts.insert(count_key.as_slice(),count).map_err(Self::storage_error)?;
            mutations.push(Self::relational_index_count_mutation(&count_key,count))?;
            #[cfg(any(test,feature="test-seams"))]
            observe_index_prefix_count(table,meta,declaration,prefix_len,&count_key);
        }
        Ok(())
    }
}
