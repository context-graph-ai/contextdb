//! Row-derived adjacency is an authenticated projection of an exact row version.
//! Standalone graph history and its transport lane remain independent.
use super::*;
use contextdb_tx::OwnedAdjEntry;

pub(super) const FORWARD: TableDefinition<&[u8], &[u8]> = TableDefinition::new("row_graph_fwd");
pub(super) const REVERSE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("row_graph_rev");

fn corruption(reason: &str) -> Error {
    Error::StoreCorrupted {
        path: "row-derived adjacency".into(),
        reason: reason.into(),
    }
}

pub(super) fn validate(owned: &OwnedAdjEntry) -> Result<()> {
    if owned.owner.table.is_empty()
        || owned.owner.created_tx != owned.entry.created_tx
        || owned.owner.lsn != owned.entry.lsn
    {
        return Err(corruption("row owner and adjacency version disagree"));
    }
    Ok(())
}

pub(super) fn encode_row_graph(owned: &OwnedAdjEntry) -> Result<Vec<u8>> {
    struct Size(usize);
    impl std::io::Write for Size {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("row graph codec size overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut size = Size(0);
    bincode::serde::encode_into_std_write(owned, &mut size, bincode::config::standard())
        .map_err(|e| Error::Other((e.to_string()).into()))?;
    crate::read_image_memory::reserve(size.0)?;
    let mut bytes = Vec::with_capacity(size.0);
    bincode::serde::encode_into_std_write(owned, &mut bytes, bincode::config::standard())
        .map_err(|e| Error::Other((e.to_string()).into()))?;
    if bytes.len() != size.0 || bytes.capacity() != size.0 {
        return Err(corruption(
            "row graph encoded allocation differs from its admission",
        ));
    }
    Ok(bytes)
}

impl RedbPersistence {
    /// The table and generation are part of ownership, even when two rows
    /// share endpoints, a transaction and an LSN.
    pub(super) fn row_graph_key(owned: &OwnedAdjEntry, reverse: bool) -> Result<Vec<u8>> {
        validate(owned)?;
        let owner = &owned.owner;
        let entry = &owned.entry;
        let table_bytes = u32::try_from(owner.table.len())
            .map_err(|_| corruption("row owner table name exceeds its encoded range"))?;
        let bytes = 32usize
            .checked_add(entry.edge_type.len())
            .and_then(|n| n.checked_add(1 + 4 + 4 * 8))
            .and_then(|n| n.checked_add(owner.table.len()))
            .ok_or_else(|| corruption("row adjacency key size overflow"))?;
        crate::read_image_memory::reserve(bytes)?;
        let mut key = Vec::with_capacity(bytes);
        let (first, second) = if reverse {
            (entry.target, entry.source)
        } else {
            (entry.source, entry.target)
        };
        key.extend_from_slice(first.as_bytes());
        key.extend_from_slice(second.as_bytes());
        key.extend_from_slice(entry.edge_type.as_bytes());
        key.push(0);
        key.extend_from_slice(&table_bytes.to_be_bytes());
        key.extend_from_slice(owner.table.as_bytes());
        key.extend_from_slice(&owner.table_generation.to_be_bytes());
        key.extend_from_slice(&owner.row_id.0.to_be_bytes());
        key.extend_from_slice(&owner.created_tx.0.to_be_bytes());
        key.extend_from_slice(&owner.lsn.0.to_be_bytes());
        Ok(key)
    }

    pub(super) fn initialize_row_graph_in_write(write: &redb::WriteTransaction) -> Result<()> {
        write.open_table(FORWARD).map_err(Self::storage_error)?;
        write.open_table(REVERSE).map_err(Self::storage_error)?;
        Ok(())
    }

    pub(super) fn apply_row_graph_in_write(
        write: &redb::WriteTransaction,
        ws: &contextdb_tx::WriteSet,
    ) -> Result<()> {
        Self::apply_received_row_graph_parts_in_write(
            write,
            &ws.row_adj_inserts,
            &ws.row_adj_deletes,
            &[],
        )
    }

    /// Deliberate fixture damage bypasses producer witnesses; the next graph
    /// operation must refuse rather than select a surviving sibling.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn remove_row_graph_witness_for_test(
        path: &Path,
        table: &str,
        row: contextdb_core::RowId,
        reverse: bool,
    ) -> Result<()> {
        let database = redb_builder().open(path).map_err(Self::storage_error)?;
        // Authenticate the deliberately damaged physical image so refusal
        // exercises exact owner/reciprocal validation, not a stale root proof.
        let memory = crate::read_image_memory::ImageMemory::new(None);
        let _scope = memory.enter();
        let write = Self::begin_history_write(&database)?;
        {
            let mut physical = write
                .open_table(if reverse { REVERSE } else { FORWARD })
                .map_err(Self::storage_error)?;
            let mut selected = None;
            for item in physical.iter().map_err(Self::storage_error)? {
                let (key, value) = item.map_err(Self::storage_error)?;
                let (owned, _): (OwnedAdjEntry, usize) =
                    bincode::serde::decode_from_slice(value.value(), bincode::config::standard())
                        .map_err(|e| Error::Other((e.to_string()).into()))?;
                if owned.owner.table == table && owned.owner.row_id == row {
                    if selected.is_some() {
                        return Err(Error::Other(
                            "fixture selector names multiple row versions".into()));
                    }
                    selected = Some(key.value().to_vec());
                }
            }
            let key = selected.ok_or_else(|| Error::NotFound("fixture row graph owner".into()))?;
            physical
                .remove(key.as_slice())
                .map_err(Self::storage_error)?;
        }
        write.commit().map_err(Self::storage_error)
    }

    pub(crate) fn append_row_graph_edge(&self, owned: &OwnedAdjEntry) -> Result<()> {
        self.prepare_proof_owner()?;
        self.graph_writer_window(|| {
            self.with_db(|db| {
                let write = Self::begin_history_write(db)?;
                {
                    validate(owned)?;
                    let fwd = Self::row_graph_key(owned, false)?;
                    let rev = Self::row_graph_key(owned, true)?;
                    let encoded = encode_row_graph(owned)?;
                    let mut forward = write.open_table(FORWARD).map_err(Self::storage_error)?;
                    let mut reverse = write.open_table(REVERSE).map_err(Self::storage_error)?;
                    if forward
                        .get(fwd.as_slice())
                        .map_err(Self::storage_error)?
                        .is_some()
                        || reverse
                            .get(rev.as_slice())
                            .map_err(Self::storage_error)?
                            .is_some()
                    {
                        return Err(corruption("export repeats a row-derived owner"));
                    }
                    forward
                        .insert(fwd.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    reverse
                        .insert(rev.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
                write.commit().map_err(Self::storage_error)
            })
        })
    }

    pub(super) fn erase_row_graph_in_write(
        write: &redb::WriteTransaction,
        selected: &[OwnedAdjEntry],
    ) -> Result<()> {
        if selected.is_empty() {
            return Ok(());
        }
        let mut forward = write.open_table(FORWARD).map_err(Self::storage_error)?;
        let mut reverse = write.open_table(REVERSE).map_err(Self::storage_error)?;
        for owned in selected {
            let fwd = Self::row_graph_key(owned, false)?;
            let rev = Self::row_graph_key(owned, true)?;
            let expected = encode_row_graph(owned)?;
            let a = forward
                .get(fwd.as_slice())
                .map_err(Self::storage_error)?
                .ok_or_else(|| corruption("erasure lost the selected forward witness"))?;
            let b = reverse
                .get(rev.as_slice())
                .map_err(Self::storage_error)?
                .ok_or_else(|| corruption("erasure lost the selected reverse witness"))?;
            if a.value() != expected || b.value() != expected {
                return Err(corruption("erasure witness changed after selection"));
            }
            drop(a);
            drop(b);
            forward
                .remove(fwd.as_slice())
                .map_err(Self::storage_error)?;
            reverse
                .remove(rev.as_slice())
                .map_err(Self::storage_error)?;
        }
        Ok(())
    }
}

impl RedbPersistence {
    /// Retire one table's exact row owners in the caller's schema transaction.
    /// Each native window and its decode are funded until physically freed.
    pub(super) fn retire_row_graph_table_in_write(
        &self, write: &redb::WriteTransaction, table: &str,
        original: Option<&crate::memory_accounting::OwnedMemoryReservation>,
    ) -> Result<()> {
        let mut after: Option<(Vec<u8>, Arc<crate::read_image_memory::ImageMemory>, Option<crate::memory_accounting::OwnedMemoryReservation>)> = None;
        loop {
            let frame = original.map(OriginalPersistenceFrame::new).transpose()?;
            let memory = match frame.as_ref() {
                Some(frame) => crate::memory_accounting::image_memory_for_original_record(frame)?,
                None => crate::read_image_memory::ImageMemory::operation_writer(self.require_decode_accountant()?)?,
            };
            let next = memory.scope(|| -> Result<_> {
                let source = write.open_table(FORWARD).map_err(Self::storage_error)?;
                let lower = after.as_ref().map_or(std::ops::Bound::Unbounded,
                    |(key, _, _)| std::ops::Bound::Excluded(key.as_slice()));
                let mut cursor = source.range::<&[u8]>((lower, std::ops::Bound::Unbounded))
                    .map_err(Self::storage_error)?;
                let Some((key, value)) = cursor.next().transpose().map_err(Self::storage_error)? else { return Ok(None); };
                crate::read_image_memory::reserve(key.value().len())?;
                let position = key.value().to_vec();
                let owned: OwnedAdjEntry = Self::decode_exact(value.value())?;
                validate(&owned)?;
                if Self::row_graph_key(&owned, false)? != position {
                    return Err(corruption("schema retirement found a mismatched owned key"));
                }
                Ok(Some((position, owned)))
            })?;
            let Some((key, owned)) = next else { break; };
            if owned.owner.table == table {
                memory.scope(|| Self::erase_row_graph_in_write(write, std::slice::from_ref(&owned)))?;
            }
            drop(owned);
            after = Some((key, memory, frame.map(OriginalPersistenceFrame::into_reservation)));
        }
        Ok(())
    }

    pub(super) fn retire_received_shape_losses_in_write(
        &self, write: &redb::WriteTransaction, stage: &ReceivedSchemaPersistenceProjection,
        original: Option<&crate::memory_accounting::OwnedMemoryReservation>,
    ) -> Result<()> {
        for table in &stage.affected_tables {
            if stage.table_meta.get(table).is_some_and(crate::database::has_graph_edge_table_shape) { continue; }
            let frame = original.map(OriginalPersistenceFrame::new).transpose()?;
            let memory = match frame.as_ref() {
                Some(frame) => crate::memory_accounting::image_memory_for_original_record(frame)?,
                None => crate::read_image_memory::ImageMemory::operation_writer(self.require_decode_accountant()?)?,
            };
            let loses_shape = memory.scope(|| -> Result<bool> {
                let metadata = write.open_table(META_TABLE).map_err(Self::storage_error)?;
                crate::read_image_memory::reserve(table.len().saturating_add(64))?;
                let key = Self::meta_key(table);
                let Some(encoded) = metadata.get(key.as_str()).map_err(Self::storage_error)? else { return Ok(false); };
                let (before, _) = Self::decode_table_meta_versioned(encoded.value())?;
                Ok(crate::database::has_graph_edge_table_shape(&before))
            })?;
            if loses_shape { self.retire_row_graph_table_in_write(write, table, original)?; }
        }
        Ok(())
    }
}
