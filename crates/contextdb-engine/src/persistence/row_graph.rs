//! Row-derived adjacency is an authenticated projection of an exact row version.
//! Standalone graph history and its transport lane remain independent.
use super::*;
use contextdb_tx::OwnedAdjEntry;

pub(crate) const RETIRED_PREFIX: &str = "row_graph_retired_v1.";

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct Retirement {
    pub(crate) version: u8,
    pub(crate) owned: OwnedAdjEntry,
}

// The exact row-version prefix precedes the generation, so deleting that
// version can remove its controls without scanning another row's history.
#[expect(
    clippy::result_large_err,
    reason = "Preserve typed inline storage errors."
)]
pub(crate) fn retirement_version_prefix(
    table: &str,
    row: RowId,
    created: TxId,
    lsn: Lsn,
) -> Result<String> {
    use std::fmt::Write;
    let bytes = RETIRED_PREFIX
        .len()
        .saturating_add(table.len().saturating_mul(2))
        .saturating_add(64);
    crate::read_image_memory::reserve(bytes)?;
    let mut key = String::with_capacity(bytes);
    key.push_str(RETIRED_PREFIX);
    for byte in table.as_bytes() {
        write!(&mut key, "{byte:02x}").map_err(|_| corruption("retirement key encoding failed"))?;
    }
    write!(&mut key, ".{:020}.{:020}.{:020}.", row.0, created.0, lsn.0)
        .map_err(|_| corruption("retirement coordinates encoding failed"))?;
    Ok(key)
}

#[expect(
    clippy::result_large_err,
    reason = "Preserve typed inline storage errors."
)]
pub(crate) fn retirement_key(owner: &contextdb_tx::RowEdgeOwner) -> Result<String> {
    use std::fmt::Write;
    let mut key =
        retirement_version_prefix(&owner.table, owner.row_id, owner.created_tx, owner.lsn)?;
    crate::read_image_memory::reserve(20)?;
    key.reserve_exact(20);
    write!(&mut key, "{:020}", owner.table_generation)
        .map_err(|_| corruption("retirement generation encoding failed"))?;
    Ok(key)
}

#[expect(
    clippy::result_large_err,
    reason = "Preserve typed inline storage errors."
)]
pub(crate) fn validate_retirement(record: &Retirement, expected: &OwnedAdjEntry) -> Result<()> {
    validate(&record.owned)?;
    let a = &record.owned.entry;
    let b = &expected.entry;
    if record.version != 1
        || record.owned.entry.deleted_tx.is_none()
        || record.owned.owner != expected.owner
        || a.source != b.source
        || a.target != b.target
        || a.edge_type != b.edge_type
        || a.created_tx != b.created_tx
        || a.lsn != b.lsn
        || !a.properties.is_empty()
    {
        return Err(corruption(
            "retirement does not authorize this exact owning edge row",
        ));
    }
    Ok(())
}

#[expect(
    clippy::result_large_err,
    reason = "Preserve typed inline storage errors."
)]
pub(crate) fn decode_retirement(bytes: &[u8]) -> Result<Retirement> {
    // The leading u8 is the version tag in this private bincode record.
    // Reject it before any owned body field can request an allocation.
    if bytes.first() != Some(&1) {
        return Err(corruption(
            "missing or unsupported row graph retirement version",
        ));
    }
    let record: Retirement =
        RedbPersistence::decode_config_value_exact(bytes).map_err(|error| match error {
            Error::Other(_) => corruption("malformed exact row graph retirement"),
            other => other,
        })?;
    if record.version != 1 {
        return Err(corruption("unknown row graph retirement version"));
    }
    validate(&record.owned)?;
    Ok(record)
}

pub(super) const FORWARD: TableDefinition<&[u8], &[u8]> = TableDefinition::new("row_graph_fwd");
pub(super) const REVERSE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("row_graph_rev");

fn corruption(reason: impl Into<contextdb_core::error::DiagnosticText>) -> Error {
    Error::StoreCorrupted {
        path: "row-derived adjacency".into(),
        reason: reason.into(),
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn validate(owned: &OwnedAdjEntry) -> Result<()> {
    if owned.owner.table.is_empty()
        || owned.owner.created_tx != owned.entry.created_tx
        || owned.owner.lsn != owned.entry.lsn
    {
        return Err(corruption("row owner and adjacency version disagree"));
    }
    Ok(())
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
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
    #[cfg(feature = "test-seams")]
    contextdb_core::family_loads::loaded(contextdb_core::family_loads::Family::GraphEdge, size.0);
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
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
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

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn initialize_row_graph_in_write(write: &redb::WriteTransaction) -> Result<()> {
        write.open_table(FORWARD).map_err(Self::storage_error)?;
        write.open_table(REVERSE).map_err(Self::storage_error)?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
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
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(crate) fn remove_row_graph_witness_for_test(
        path: &Path,
        table: &str,
        row: contextdb_core::RowId,
        reverse: bool,
    ) -> Result<()> {
        let database = redb_builder().open(path).map_err(Self::storage_error)?;
        // Authenticate the deliberately damaged physical image so refusal
        // exercises exact owner/reciprocal validation, not a stale root proof.
        let memory = crate::read_image_memory::ImageMemory::new(None)?;
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
                            "fixture selector names multiple row versions".into(),
                        ));
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

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn append_row_graph_edge(&self, owned: &OwnedAdjEntry) -> Result<()> {
        self.prepare_proof_owner()?;
        self.graph_writer_window(|| {
            self.with_db(|db| {
                let write = Self::begin_history_write(db)?;
                {
                    validate(owned)?;
                    if Self::row_retirement_in_write(&write, owned)? {
                        return Ok(());
                    }

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

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
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
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retire_row_graph_table_in_write(
        &self,
        write: &redb::WriteTransaction,
        table: &str,
        original: Option<&crate::memory_accounting::OwnedMemoryReservation>,
    ) -> Result<()> {
        let mut after: Option<(
            Vec<u8>,
            crate::read_image_memory::ImageMemoryHandle,
            Option<crate::memory_accounting::OwnedMemoryReservation>,
        )> = None;
        loop {
            let frame = original.map(OriginalPersistenceFrame::new).transpose()?;
            let memory = match frame.as_ref() {
                Some(frame) => crate::memory_accounting::image_memory_for_original_record(frame)?,
                None => crate::read_image_memory::ImageMemory::operation_writer(
                    self.require_decode_accountant()?,
                )?,
            };
            let next = memory.scope(|| -> Result<_> {
                let source = write.open_table(FORWARD).map_err(Self::storage_error)?;
                let lower = after
                    .as_ref()
                    .map_or(std::ops::Bound::Unbounded, |(key, _, _)| {
                        std::ops::Bound::Excluded(key.as_slice())
                    });
                let mut cursor = source
                    .range::<&[u8]>((lower, std::ops::Bound::Unbounded))
                    .map_err(Self::storage_error)?;
                let Some((key, value)) = cursor.next().transpose().map_err(Self::storage_error)?
                else {
                    return Ok(None);
                };
                crate::read_image_memory::reserve(key.value().len())?;
                let position = key.value().to_vec();
                let owned: OwnedAdjEntry = Self::decode_exact(value.value())?;
                validate(&owned)?;
                if Self::row_graph_key(&owned, false)? != position {
                    return Err(corruption("schema retirement found a mismatched owned key"));
                }
                Ok(Some((position, owned)))
            })?;
            let Some((key, owned)) = next else {
                break;
            };
            if owned.owner.table == table {
                memory.scope(|| {
                    Self::erase_row_graph_in_write(write, std::slice::from_ref(&owned))
                })?;
            }
            drop(owned);
            after = Some((
                key,
                memory,
                frame.map(OriginalPersistenceFrame::into_reservation),
            ));
        }
        self.erase_table_row_retirements_in_write(write, table, original)?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Preserve typed inline storage errors."
    )]
    pub(super) fn erase_table_row_retirements_in_write(
        &self,
        write: &redb::WriteTransaction,
        table: &str,
        original: Option<&crate::memory_accounting::OwnedMemoryReservation>,
    ) -> Result<()> {
        let frame = original.map(OriginalPersistenceFrame::new).transpose()?;
        let memory = match frame.as_ref() {
            Some(frame) => crate::memory_accounting::image_memory_for_original_record(frame)?,
            None => crate::read_image_memory::ImageMemory::operation_writer(
                self.require_decode_accountant()?,
            )?,
        };
        memory.scope(|| -> Result<()> {
            let mut prefix = retirement_version_prefix(table, RowId(0), TxId(0), Lsn(0))?;
            prefix.truncate(
                RETIRED_PREFIX
                    .len()
                    .saturating_add(table.len().saturating_mul(2))
                    .saturating_add(1),
            );
            let mut identities = Vec::new();
            Self::erase_retirement_prefix_in_write(write, &prefix, &mut identities)?;
            Self::apply_content_identity_mutations_in_write(
                write,
                &identities,
                &mut self.proof_admission(),
            )
            .map(|_| ())
        })?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retire_received_shape_losses_in_write(
        &self,
        write: &redb::WriteTransaction,
        stage: &ReceivedSchemaPersistenceProjection,
        original: Option<&crate::memory_accounting::OwnedMemoryReservation>,
    ) -> Result<()> {
        Self::erase_row_graph_in_write(write, &stage.dropped_row_graph)?;
        for table in &stage.affected_tables {
            if stage
                .retired_row_graph_tables
                .iter()
                .any(|retired| retired == table)
            {
                self.retire_row_graph_table_in_write(write, table, original)?;
                continue;
            }
            if stage
                .table_meta
                .get(table)
                .is_some_and(crate::database::has_graph_edge_table_shape)
            {
                continue;
            }
            let frame = original.map(OriginalPersistenceFrame::new).transpose()?;
            let memory = match frame.as_ref() {
                Some(frame) => crate::memory_accounting::image_memory_for_original_record(frame)?,
                None => crate::read_image_memory::ImageMemory::operation_writer(
                    self.require_decode_accountant()?,
                )?,
            };
            let loses_shape = memory.scope(|| -> Result<bool> {
                let metadata = write.open_table(META_TABLE).map_err(Self::storage_error)?;
                crate::read_image_memory::reserve(table.len().saturating_add(64))?;
                let key = Self::meta_key(table);
                let Some(encoded) = metadata.get(key.as_str()).map_err(Self::storage_error)? else {
                    return Ok(false);
                };
                let (before, _) = Self::decode_table_meta_versioned(encoded.value())?;
                Ok(crate::database::has_graph_edge_table_shape(&before))
            })?;
            if loses_shape {
                self.retire_row_graph_table_in_write(write, table, original)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
impl RedbPersistence {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_row_owned_edges_for_test(&self, reverse: bool) -> Result<Vec<AdjEntry>> {
        let _decode_scope = self.reader_decode_scope()?;
        self.with_db(|database| {
            let read = database.begin_read().map_err(Self::storage_error)?;
            let table = match read.open_table(if reverse { REVERSE } else { FORWARD }) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (_, value) = entry.map_err(Self::storage_error)?;
                let owned: OwnedAdjEntry = Self::decode_exact(value.value())?;
                validate(&owned)?;
                entries.push(owned.entry);
            }
            Ok(entries)
        })
    }
}

impl RedbPersistence {
    #[expect(
        clippy::result_large_err,
        reason = "Preserve typed inline storage errors."
    )]
    pub(super) fn retire_owned_edges_in_write(
        write: &redb::WriteTransaction,
        selected: &[OwnedAdjEntry],
        removed_rows: &[(String, RowId, TxId, Lsn)],
        identities: &mut Vec<content_identity::ContentIdentityMutation>,
    ) -> Result<()> {
        // Authenticate the pre-removal state while both directions still exist.
        for owned in selected {
            if Self::row_retirement_in_write(write, owned)? {
                // A published retirement must identify this exact tombstone.
                let config = write
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                let key = retirement_key(&owned.owner)?;
                let value = config
                    .get(key.as_str())
                    .map_err(Self::storage_error)?
                    .ok_or_else(|| corruption("retirement disappeared during reclamation"))?;
                let record = decode_retirement(value.value())?;
                if record.owned != *owned {
                    return Err(corruption(
                        "reclamation does not match the retired tombstone",
                    ));
                }
            }
        }
        Self::erase_row_graph_in_write(write, selected)?;
        for owned in selected {
            if removed_rows.iter().any(|(table, row, created, lsn)| {
                table == &owned.owner.table
                    && *row == owned.owner.row_id
                    && *created == owned.owner.created_tx
                    && *lsn == owned.owner.lsn
            }) {
                continue;
            }
            if Self::row_retirement_in_write(write, owned)? {
                continue;
            }
            if owned.entry.deleted_tx.is_none() {
                return Err(corruption(
                    "physical retention cannot retire an unstamped row binding",
                ));
            }
            let key = retirement_key(&owned.owner)?;
            crate::read_image_memory::reserve(owned.estimated_bytes())?;
            let value = Self::encode_config_value(&Retirement {
                version: 1,
                owned: owned.clone(),
            })?;
            {
                let mut canonical = write
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                if canonical
                    .get(key.as_str())
                    .map_err(Self::storage_error)?
                    .is_some()
                {
                    return Err(corruption(
                        "live row graph binding conflicts with retirement",
                    ));
                }
                canonical
                    .insert(key.as_str(), value.as_slice())
                    .map_err(Self::storage_error)?;
            }
            identities.extend(content_identity::config_mutations(&key, Some(&value)));
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Preserve typed inline storage errors."
    )]
    pub(super) fn erase_row_retirements_in_write(
        write: &redb::WriteTransaction,
        table: &str,
        row: RowId,
        created: TxId,
        lsn: Lsn,
        identities: &mut Vec<content_identity::ContentIdentityMutation>,
    ) -> Result<()> {
        let prefix = retirement_version_prefix(table, row, created, lsn)?;
        Self::erase_retirement_prefix_in_write(write, &prefix, identities)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Preserve typed inline storage errors."
    )]
    pub(super) fn erase_retirement_prefix_in_write(
        write: &redb::WriteTransaction,
        prefix: &str,
        identities: &mut Vec<content_identity::ContentIdentityMutation>,
    ) -> Result<()> {
        loop {
            let key = {
                let config = write
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                let next = config
                    .range(prefix..)
                    .map_err(Self::storage_error)?
                    .next()
                    .transpose()
                    .map_err(Self::storage_error)?;
                match next {
                    Some((key, _)) if key.value().starts_with(prefix) => {
                        crate::read_image_memory::reserve(key.value().len())?;
                        Some(key.value().to_owned())
                    }
                    _ => None,
                }
            };
            let Some(key) = key else { break };
            write
                .open_table(CONFIG_TABLE)
                .map_err(Self::storage_error)?
                .remove(key.as_str())
                .map_err(Self::storage_error)?;
            identities.extend(content_identity::config_mutations(&key, None));
        }
        Ok(())
    }
}

impl RedbPersistence {
    #[expect(
        clippy::result_large_err,
        reason = "Preserve typed inline storage errors."
    )]
    pub(super) fn row_retirement_in_read(
        read: &redb::ReadTransaction,
        owned: &OwnedAdjEntry,
    ) -> Result<bool> {
        let key = retirement_key(&owned.owner)?;
        Self::verify_history_root(read, READ_LINEAGE_CONFIG_TABLE.name())?;
        let table = read
            .open_table(READ_LINEAGE_CONFIG_TABLE)
            .map_err(Self::storage_error)?;
        let Some(value) = table.get(key.as_str()).map_err(Self::storage_error)? else {
            return Ok(false);
        };
        let record = decode_retirement(value.value())?;
        validate_retirement(&record, owned)?;
        let expected = encode_row_graph(&record.owned)?;
        let mut present = 0;
        for (definition, reverse) in [(FORWARD, false), (REVERSE, true)] {
            Self::verify_history_root(read, definition.name())?;
            let physical = read.open_table(definition).map_err(Self::storage_error)?;
            let key = Self::row_graph_key(&record.owned, reverse)?;
            if let Some(bytes) = physical.get(key.as_slice()).map_err(Self::storage_error)? {
                if bytes.value() != expected.as_slice() {
                    return Err(corruption(
                        "retirement conflicts with its exact physical tombstone",
                    ));
                }
                present += 1;
            }
        }
        if present == 1 {
            return Err(corruption("retirement has only one reciprocal direction"));
        }
        Ok(true)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Preserve typed inline storage errors."
    )]
    pub(super) fn row_retirement_in_write(
        write: &redb::WriteTransaction,
        owned: &OwnedAdjEntry,
    ) -> Result<bool> {
        let key = retirement_key(&owned.owner)?;
        let table = write
            .open_table(CONFIG_TABLE)
            .map_err(Self::storage_error)?;
        let Some(value) = table.get(key.as_str()).map_err(Self::storage_error)? else {
            return Ok(false);
        };
        let record = decode_retirement(value.value())?;
        validate_retirement(&record, owned)?;
        Ok(true)
    }
}
