//! Root-pinned writer reads. Decoded records belong to a caller window, never
//! to the published source; the only retained payload cache is redb's cache.
use super::*;
use crate::memory_accounting::{MemoryAccountant, OwnedMemoryReservation};
use crate::read_image_memory::ImageMemory;
use std::ops::Bound;

fn decode_window_exact<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let (value, consumed) = crate::read_image_memory::decode(bytes)?;
    if consumed != bytes.len() {
        return Err(Error::Other(
            "writer cursor record has trailing bytes".into(),
        ));
    }
    Ok(value)
}

const HINT: &str = "Raise MEMORY_LIMIT to admit this writer cursor window.";

pub(crate) fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != u8::MAX {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

/// An admitted durable descriptor prepared before the transaction manager's
/// publication swap. Only capture_published_read captures statement roots.
pub(crate) struct WriterSnapshot {
    read: Arc<redb::ReadTransaction>,
    pub(crate) snapshot: SnapshotId,
    pub(crate) lsn: Lsn,
    accountant: Arc<MemoryAccountant>,
    _control: OwnedMemoryReservation,
    erasure: parking_lot::RwLock<WriterErasure>,
    schema: Arc<WriterSchema>,
    registry: Arc<parking_lot::Mutex<Vec<std::sync::Weak<WriterSnapshot>>>>,
}

/// An immutable schema owner, admitted before its durable publication.
/// Data commits share it; schema writes build a replacement from their exact
/// prepared mutation, without consulting a newer catalog during capture.
pub(crate) struct WriterSchema {
    tables: HashMap<String, Arc<WriterTableMeta>>,
    _memory: OwnedMemoryReservation,
}

struct WriterTableMeta {
    meta: TableMeta,
    _memory: OwnedMemoryReservation,
}

impl WriterSnapshot {
    pub(crate) fn row_body_bytes(&self, table: &str, row: &VersionedRow) -> Result<usize> {
        #[cfg(test)]
        stored_relational_row_bytes_fault_for_test()?;
        let _scratch = self.adapter_control(table.len().saturating_add(128))?;
        self.check_table_scope(table)?;
        let name = RedbPersistence::rel_table_name(table);
        let physical = self
            .read
            .open_table(TableDefinition::<&[u8], &[u8]>::new(&name))
            .map_err(RedbPersistence::storage_error)?;
        let key = RedbPersistence::rel_row_key(row);
        physical
            .get(key.as_slice())
            .map_err(RedbPersistence::storage_error)?
            .map(|value| value.value().len())
            .ok_or_else(|| Error::StoreCorrupted {
                path: table.to_owned(),
                reason: "selected saved row is missing".into(),
            })
    }

    pub(crate) fn captured_table_meta(&self, table: &str) -> Option<TableMeta> {
        self.borrow_captured_table_meta(table).cloned()
    }

    pub(crate) fn borrow_captured_table_meta(&self, table: &str) -> Option<&TableMeta> {
        self.schema.tables.get(table).map(|owned| &owned.meta)
    }

    pub(crate) fn captured_table_names(&self) -> Vec<String> {
        let mut names = self.schema.tables.keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }

    pub(crate) fn captured_table_metadata(&self) -> impl Iterator<Item = (&str, &TableMeta)> {
        self.schema
            .tables
            .iter()
            .map(|(name, owned)| (name.as_str(), &owned.meta))
    }
}

impl Drop for WriterSnapshot {
    fn drop(&mut self) {
        let mut roots = self.registry.lock();
        roots.retain(|root| !std::ptr::eq(root.as_ptr(), self));
        roots.shrink_to_fit();
    }
}

#[derive(Default)]
struct WriterErasure {
    tables: BTreeSet<String>,
    graph_keys: BTreeSet<Vec<u8>>,
    _memory: Option<OwnedMemoryReservation>,
}

pub(crate) struct PreparedWriterErasure(Vec<(Arc<WriterSnapshot>, WriterErasure)>);
impl PreparedWriterErasure {
    pub(crate) fn publish(self) {
        for (root, erasure) in self.0 {
            *root.erasure.write() = erasure;
        }
    }
}

impl RedbPersistence {
    /// Startup alone resolves metadata from disk, outside publication locks.
    fn prepared_writer_schema(&self) -> Result<Arc<WriterSchema>> {
        if let Some(schema) = self.writer_schema.lock().as_ref().cloned() {
            return Ok(schema);
        }
        let accountant = self
            .proof_accountant()
            .ok_or_else(|| Error::Other("writer schema requires its accountant".into()))?;
        let mut memory = OwnedMemoryReservation::try_new_for(
            accountant.clone(),
            std::mem::size_of::<WriterSchema>(),
            "writer_source",
            "captured_schema",
            HINT,
        )?;
        let tables = self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let table = match read.open_table(META_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut tables = HashMap::new();
            for entry in table
                .range("table:".."table;")
                .map_err(Self::storage_error)?
            {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let name = key
                    .value()
                    .strip_prefix("table:")
                    .ok_or_else(|| Error::Other("invalid schema key".into()))?;
                let scratch = OwnedMemoryReservation::try_new_for(
                    accountant.clone(),
                    value.value().len().saturating_mul(16).saturating_add(4096),
                    "writer_source",
                    "schema_decode",
                    HINT,
                )?;
                let (meta, legacy) = Self::decode_table_meta_versioned(value.value())?;
                if legacy {
                    return Err(Error::Other("unsupported table metadata layout".into()));
                }
                memory.try_grow_for(
                    name.len().saturating_add(128),
                    "writer_source",
                    "captured_schema",
                    HINT,
                )?;
                let body_memory = OwnedMemoryReservation::try_new_for(
                    accountant.clone(),
                    meta.metadata_bytes().saturating_add(64),
                    "writer_source",
                    "captured_schema",
                    HINT,
                )?;
                tables.insert(
                    name.to_owned(),
                    Arc::new(WriterTableMeta {
                        meta,
                        _memory: body_memory,
                    }),
                );
                drop(scratch);
            }
            Ok(tables)
        })?;
        let schema = Arc::new(WriterSchema {
            tables,
            _memory: memory,
        });
        let prior = self.writer_schema.lock().replace(schema.clone());
        drop(prior);
        Ok(schema)
    }

    /// The caller supplies exactly the metadata its write transaction installs.
    /// This preparation can refuse; it occurs before opening that transaction.
    pub(crate) fn prepare_writer_schema_change<'a>(
        &self,
        removed: impl Iterator<Item = &'a str>,
        installed: impl Iterator<Item = (&'a str, &'a TableMeta)>,
    ) -> Result<Option<Arc<WriterSchema>>> {
        if self.proof_accountant().is_none() {
            return Ok(None);
        }
        let base = self.prepared_writer_schema()?;
        let removed = removed.collect::<Vec<_>>();
        let installed = installed.collect::<Vec<_>>();
        let unchanged = |name: &str| {
            !removed.contains(&name) && !installed.iter().any(|(table, _)| *table == name)
        };
        let bytes = base
            .tables
            .keys()
            .filter(|name| unchanged(name))
            .map(|name| name.len().saturating_add(128))
            .chain(
                installed
                    .iter()
                    .map(|(name, _)| name.len().saturating_add(128)),
            )
            .fold(std::mem::size_of::<WriterSchema>(), usize::saturating_add);
        let accountant = self.proof_accountant().unwrap();
        let memory = OwnedMemoryReservation::try_new_for(
            accountant.clone(),
            bytes,
            "writer_source",
            "captured_schema",
            HINT,
        )?;
        let mut tables = HashMap::new();
        for (name, owned) in &base.tables {
            if unchanged(name) {
                tables.insert(name.clone(), owned.clone());
            }
        }
        for (name, meta) in installed {
            let body_memory = OwnedMemoryReservation::try_new_for(
                accountant.clone(),
                meta.metadata_bytes().saturating_add(64),
                "writer_source",
                "captured_schema",
                HINT,
            )?;
            tables.insert(
                name.to_owned(),
                Arc::new(WriterTableMeta {
                    meta: meta.clone(),
                    _memory: body_memory,
                }),
            );
        }
        Ok(Some(Arc::new(WriterSchema {
            tables,
            _memory: memory,
        })))
    }

    pub(crate) fn complete_writer_schema_change(&self, schema: Option<Arc<WriterSchema>>) {
        if let Some(schema) = schema {
            let prior = self.writer_schema.lock().replace(schema);
            drop(prior);
        }
    }
    /// Prepare capability restrictions before durability. Old descriptors retain
    /// their native image, but cannot decode erased families after publication.
    pub(crate) fn prepare_writer_erasure(
        &self,
        tables: &BTreeSet<String>,
        edges: &[AdjEntry],
    ) -> Result<PreparedWriterErasure> {
        let roots = {
            let mut roots = self.writer_snapshots.lock();
            roots.retain(|root| root.strong_count() != 0);
            roots
                .iter()
                .filter_map(std::sync::Weak::upgrade)
                .collect::<Vec<_>>()
        };
        let mut replacements = Vec::with_capacity(roots.len());
        for root in roots {
            let current = root.erasure.read();
            let bytes = current
                .tables
                .iter()
                .chain(tables.iter())
                .map(|table| table.len().saturating_add(96))
                .sum::<usize>()
                .saturating_add(
                    current
                        .graph_keys
                        .iter()
                        .map(|key| key.len().saturating_add(96))
                        .sum::<usize>(),
                )
                .saturating_add(
                    edges
                        .iter()
                        .map(|edge| edge.edge_type.len().saturating_add(160))
                        .sum::<usize>()
                        .saturating_mul(2),
                )
                .saturating_add(std::mem::size_of::<WriterErasure>());
            let memory = root.adapter_control(bytes)?;
            let mut next = WriterErasure {
                tables: current.tables.clone(),
                graph_keys: current.graph_keys.clone(),
                _memory: Some(memory),
            };
            next.tables.extend(tables.iter().cloned());
            for edge in edges {
                next.graph_keys.insert(RedbPersistence::graph_fwd_key(edge));
                next.graph_keys.insert(RedbPersistence::graph_rev_key(edge));
            }
            drop(current);
            replacements.push((root, next));
        }
        Ok(PreparedWriterErasure(replacements))
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn writer_snapshot_charge_for_test(&self) -> usize {
        let roots = self
            .writer_snapshots
            .lock()
            .iter()
            .filter_map(std::sync::Weak::upgrade)
            .collect::<Vec<_>>();
        let mut schemas = std::collections::HashSet::new();
        let mut metadata = std::collections::HashSet::new();
        roots
            .iter()
            .map(|root| {
                let mut bytes = root._control.bytes().saturating_add(
                    root.erasure
                        .read()
                        ._memory
                        .as_ref()
                        .map_or(0, OwnedMemoryReservation::bytes),
                );
                if schemas.insert(Arc::as_ptr(&root.schema) as usize) {
                    bytes = bytes.saturating_add(root.schema._memory.bytes());
                    for owned in root.schema.tables.values() {
                        if metadata.insert(Arc::as_ptr(owned) as usize) {
                            bytes = bytes.saturating_add(owned._memory.bytes());
                        }
                    }
                }
                bytes
            })
            .sum()
    }

    /// Prepare the initial admitted root before constructing the manager.
    /// Pass it to new_with_directory; persistence keeps no publication slot.
    pub(crate) fn initialize_writer_snapshot(
        &self,
        snapshot: SnapshotId,
        lsn: Lsn,
    ) -> Result<Arc<WriterSnapshot>> {
        let control = self.reserve_writer_snapshot()?;
        self.prepare_writer_snapshot(control, snapshot, lsn)
    }

    /// Reserve this before durable commit. Failed reservation leaves the
    /// existing root untouched and must abort commit preparation.
    pub(crate) fn reserve_writer_snapshot(&self) -> Result<OwnedMemoryReservation> {
        let _schema = self.prepared_writer_schema()?;
        let accountant = self.proof_accountant().ok_or_else(|| {
            Error::Other("writer source requires the attached memory accountant".into())
        })?;
        OwnedMemoryReservation::try_new_for(
            accountant,
            std::mem::size_of::<WriterSnapshot>() + 128,
            "writer_source",
            "published_root",
            HINT,
        )
    }

    /// Called after durability, while serialized with commits. The caller
    /// must poison the writer if this fails; the durable commit cannot roll
    /// back. This deliberately does not publish the new root.
    pub(crate) fn prepare_writer_snapshot(
        &self,
        control: OwnedMemoryReservation,
        snapshot: SnapshotId,
        lsn: Lsn,
    ) -> Result<Arc<WriterSnapshot>> {
        let accountant = self.proof_accountant().ok_or_else(|| {
            Error::Other("writer source requires the attached memory accountant".into())
        })?;
        let schema = self.writer_schema.lock().as_ref().cloned().ok_or_else(|| {
            Error::Other("publication schema was not prepared before durability".into())
        })?;
        self.with_db(|db| {
            let read = Arc::new(db.begin_read().map_err(Self::storage_error)?);
            let root = Arc::new(WriterSnapshot {
                read,
                snapshot,
                lsn,
                accountant,
                _control: control,
                erasure: parking_lot::RwLock::new(WriterErasure::default()),
                schema,
                registry: self.writer_snapshots.clone(),
            });
            let mut roots = self.writer_snapshots.lock();
            roots.retain(|root| root.strong_count() != 0);
            roots.push(Arc::downgrade(&root));
            Ok(root)
        })
    }
}

// Same on-disk type and comparator, with borrowed guards. A cursor admits
// its owned resume key before copying rather than letting `value()` allocate.
#[derive(Debug)]
struct BorrowedRelationalIndexKey;
impl RedbValue for BorrowedRelationalIndexKey {
    type SelfType<'a> = &'a [u8];
    type AsBytes<'a> = &'a [u8];
    fn fixed_width() -> Option<usize> {
        None
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        data
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> {
        value
    }
    fn type_name() -> redb::TypeName {
        DurableRelationalIndexKey::type_name()
    }
}
impl RedbKey for BorrowedRelationalIndexKey {
    fn compare(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
        DurableRelationalIndexKey::compare(left, right)
    }
}

#[derive(Clone, Copy)]
pub(crate) enum WriterCursorFamily {
    Bytes,
    Index,
    Text,
}

/// Resume using the complete physical key, in the original captured root.
/// A one-record window may outlive an advance only while its caller holds
/// its admission owner. There is no decoded state on this cursor.
pub(crate) struct WriterReadCursor {
    root: Arc<WriterSnapshot>,
    table: String,
    logical_table: Option<String>,
    family: WriterCursorFamily,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    after: Option<Vec<u8>>,
    reverse: bool,
    finished: bool,
    examined: u64,
    continuation: OwnedMemoryReservation,
}

pub(crate) struct WriterWindow<T> {
    pub(crate) value: T,
    _memory: Arc<ImageMemory>,
    pub(super) position: Vec<u8>,
}

impl<T> WriterWindow<T> {
    pub(crate) fn position(&self) -> &[u8] {
        &self.position
    }

    pub(super) fn map<U>(self, f: impl FnOnce(T) -> U) -> WriterWindow<U> {
        WriterWindow {
            value: f(self.value),
            _memory: self._memory,
            position: self.position,
        }
    }
    pub(super) fn boxed(self) -> Result<Box<Self>> {
        self._memory.reserve(std::mem::size_of::<Self>())?;
        Ok(Box::new(self))
    }
}
impl WriterSnapshot {
    pub(super) fn adapter_control(&self, bytes: usize) -> Result<OwnedMemoryReservation> {
        OwnedMemoryReservation::try_new_for(
            self.accountant.clone(),
            bytes,
            "writer_source",
            "store_adapter",
            HINT,
        )
    }
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn control_bytes(&self) -> usize {
        self._control
            .bytes()
            .saturating_add(self.schema._memory.bytes())
            .saturating_add(
                self.schema
                    .tables
                    .values()
                    .map(|owned| owned._memory.bytes())
                    .sum::<usize>(),
            )
            .saturating_add(
                self.erasure
                    .read()
                    ._memory
                    .as_ref()
                    .map_or(0, OwnedMemoryReservation::bytes),
            )
    }
    pub(super) fn owner_read(&self) -> Arc<redb::ReadTransaction> {
        Arc::clone(&self.read)
    }

    pub(crate) fn check_table_scope(&self, table: &str) -> Result<()> {
        self.erasure
            .read()
            .check_scope(Some(table), "ACL entitlement")
    }

    pub(crate) fn storage_identity(&self) -> redb::StorageSnapshotIdentity {
        self.read.storage_snapshot_identity()
    }
    #[cfg(feature = "test-seams")]
    pub(crate) fn note_read_source(&self, family: crate::writer_query_test_seams::PhysicalFamily) {
        crate::writer_query_test_seams::note_physical_source(family, &self.read);
    }

    pub(crate) fn cursor(
        self: &Arc<Self>,
        table: &str,
        family: WriterCursorFamily,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        reverse: bool,
    ) -> Result<WriterReadCursor> {
        let bound_len = |b: &Bound<&[u8]>| match b {
            Bound::Included(v) | Bound::Excluded(v) => v.len(),
            Bound::Unbounded => 0,
        };
        let continuation = OwnedMemoryReservation::try_new_for(
            Arc::clone(&self.accountant),
            table
                .len()
                .saturating_add(bound_len(&lower))
                .saturating_add(bound_len(&upper))
                .saturating_add(std::mem::size_of::<WriterReadCursor>()),
            "writer_source",
            "cursor_keys",
            HINT,
        )?;
        Ok(WriterReadCursor {
            root: Arc::clone(self),
            table: table.to_owned(),
            logical_table: table.strip_prefix("rel_").map(str::to_owned),
            family,
            lower: lower.map(<[u8]>::to_vec),
            upper: upper.map(<[u8]>::to_vec),
            after: None,
            reverse,
            finished: false,
            examined: 0,
            continuation,
        })
    }

    pub(crate) fn row_cursor(
        self: &Arc<Self>,
        table: &str,
        row_id: RowId,
    ) -> Result<WriterReadCursor> {
        let prefix = row_id.0.to_be_bytes();
        let end = prefix_end(&prefix);
        self.cursor(
            &RedbPersistence::rel_table_name(table),
            WriterCursorFamily::Bytes,
            Bound::Included(&prefix),
            end.as_deref().map_or(Bound::Unbounded, Bound::Excluded),
            true,
        )
    }

    pub(crate) fn exact_row(
        self: &Arc<Self>,
        table: &str,
        key: &[u8; 24],
    ) -> Result<WriterReadCursor> {
        self.cursor(
            &RedbPersistence::rel_table_name(table),
            WriterCursorFamily::Bytes,
            Bound::Included(key),
            Bound::Included(key),
            false,
        )
    }

    pub(crate) fn graph_cursor(
        self: &Arc<Self>,
        node: NodeId,
        reverse: bool,
    ) -> Result<WriterReadCursor> {
        let prefix = node.as_bytes();
        let end = prefix_end(prefix);
        self.cursor(
            if reverse { "graph_rev" } else { "graph_fwd" },
            WriterCursorFamily::Bytes,
            Bound::Included(prefix),
            end.as_deref().map_or(Bound::Unbounded, Bound::Excluded),
            false,
        )
    }

    pub(crate) fn membership_cursor(
        self: &Arc<Self>,
        index: &VectorIndexRef,
        partition: &VectorPartitionKey,
    ) -> Result<WriterReadCursor> {
        let _scratch = OwnedMemoryReservation::try_new_for(
            Arc::clone(&self.accountant),
            partition
                .estimated_bytes()
                .saturating_mul(8)
                .saturating_add(
                    index
                        .table
                        .len()
                        .saturating_add(index.column.len())
                        .saturating_mul(4),
                )
                .saturating_add(256),
            "writer_source",
            "membership_seek_keys",
            HINT,
        )?;
        let prefix = RedbPersistence::vector_partition_prefix(index, partition);
        let end = prefix_end(&prefix);
        self.cursor(
            "vector_partition_membership",
            WriterCursorFamily::Bytes,
            Bound::Included(&prefix),
            end.as_deref().map_or(Bound::Unbounded, Bound::Excluded),
            false,
        )?
        .with_table_scope(&index.table)
    }

    /// Ordered index endpoints use the canonical index codec/comparator,
    /// including complete row/version suffixes supplied by the query planner.
    pub(crate) fn index_cursor(
        self: &Arc<Self>,
        table: &str,
        index: &str,
        current: bool,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        reverse: bool,
    ) -> Result<WriterReadCursor> {
        let name = if current {
            RedbPersistence::current_relational_index_table_name(table, index)
        } else {
            RedbPersistence::relational_index_table_name(table, index)
        };
        self.cursor(&name, WriterCursorFamily::Index, lower, upper, reverse)?
            .with_table_scope(table)
    }
}

impl WriterErasure {
    fn refusal() -> Error {
        Error::ReadFailure(
            contextdb_core::read_contract::ReadFailure::new(
                contextdb_core::read_contract::ReadFailureKind::AuthoritativeErasure,
                contextdb_core::read_contract::ReadFailureDetail::Reason {
                    reason: "authoritative erasure invalidated this captured storage capability"
                        .into(),
                },
            )
            .expect("authoritative erasure carries reason detail"),
        )
    }
    fn check_scope(&self, logical: Option<&str>, physical: &str) -> Result<()> {
        let affected = match logical {
            Some(table) => self.tables.contains(table),
            None => !self.tables.is_empty() && !matches!(physical, "graph_fwd" | "graph_rev"),
        };
        if affected {
            Err(Self::refusal())
        } else {
            Ok(())
        }
    }
    fn check_key(&self, physical: &str, key: &[u8]) -> Result<()> {
        if matches!(physical, "graph_fwd" | "graph_rev") && self.graph_keys.contains(key) {
            Err(Self::refusal())
        } else {
            Ok(())
        }
    }
}

impl WriterReadCursor {
    fn with_table_scope(mut self, table: &str) -> Result<Self> {
        self.continuation
            .try_grow_for(table.len(), "writer_source", "cursor_keys", HINT)?;
        self.logical_table = Some(table.to_owned());
        Ok(self)
    }
    pub(crate) fn examined_entries(&self) -> u64 {
        self.examined
    }
    pub(crate) fn storage_identity(&self) -> redb::StorageSnapshotIdentity {
        self.root.storage_identity()
    }

    fn bounds(&self) -> (Bound<&[u8]>, Bound<&[u8]>) {
        let mut lower = self.lower.as_ref().map(|v| v.as_slice());
        let mut upper = self.upper.as_ref().map(|v| v.as_slice());
        if let Some(after) = self.after.as_deref() {
            if self.reverse {
                upper = Bound::Excluded(after);
            } else {
                lower = Bound::Excluded(after);
            }
        }
        (lower, upper)
    }

    /// The closure decodes one record under allocation-aware serde admission.
    /// Its returned object retains that admission until the caller drops it.
    pub(crate) fn next<T>(
        &mut self,
        decode: impl FnOnce(&[u8], &[u8]) -> Result<T>,
    ) -> Result<Option<WriterWindow<T>>> {
        self.next_before(&mut || Ok(()), decode)
    }

    pub(crate) fn next_before<T>(
        &mut self,
        before: &mut dyn FnMut() -> Result<()>,
        decode: impl FnOnce(&[u8], &[u8]) -> Result<T>,
    ) -> Result<Option<WriterWindow<T>>> {
        if self.finished {
            return Ok(None);
        }
        let memory = ImageMemory::charging_for(
            Arc::clone(&self.root.accountant),
            "writer_source",
            "cursor_window",
            HINT,
        );
        let result = self.read_one(&memory, before, decode)?;
        let Some((key, value)) = result else {
            self.finished = true;
            if let Some(prior) = self.after.take() {
                self.continuation.try_shrink(prior.capacity())?;
            }
            return Ok(None);
        };
        self.examined = self.examined.saturating_add(1);
        // Admit both the new and old resume key while replacing, then release
        // the old capacity. Window owns the decode; cursor owns only this key.
        self.continuation
            .try_grow_for(key.capacity(), "writer_source", "cursor_keys", HINT)?;
        if let Some(prior) = self.after.replace(key) {
            self.continuation.try_shrink(prior.capacity())?;
        }
        // The decode owner retains the copied physical position; the cursor
        // separately owns and admits its resume key.
        let position = self.after.as_ref().expect("resume key installed").clone();
        Ok(Some(WriterWindow {
            value,
            position,
            _memory: memory,
        }))
    }

    fn read_one<T>(
        &self,
        memory: &Arc<ImageMemory>,
        before: &mut dyn FnMut() -> Result<()>,
        decode: impl FnOnce(&[u8], &[u8]) -> Result<T>,
    ) -> Result<Option<(Vec<u8>, T)>> {
        self.root
            .erasure
            .read()
            .check_scope(self.logical_table.as_deref(), &self.table)?;
        let (lower, upper) = self.bounds();
        let finish = |key: &[u8], value: &[u8]| -> Result<(Vec<u8>, T)> {
            before()?;
            let erasure = self.root.erasure.read();
            erasure.check_scope(self.logical_table.as_deref(), &self.table)?;
            erasure.check_key(&self.table, key)?;
            memory.reserve(key.len())?;
            let decoded = memory.scope(|| decode(key, value))?;
            Ok((key.to_vec(), decoded))
        };
        match self.family {
            WriterCursorFamily::Bytes => {
                let table = match self
                    .root
                    .read
                    .open_table(TableDefinition::<&[u8], &[u8]>::new(&self.table))
                {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                    Err(e) => return Err(RedbPersistence::storage_error(e)),
                };
                if self.table.starts_with("rel_")
                    && self.after.is_none()
                    && matches!(self.lower, Bound::Unbounded)
                    && matches!(self.upper, Bound::Unbounded)
                    && let Some(logical) = self.logical_table.as_deref()
                {
                    memory.reserve(logical.len().saturating_add(32))?;
                    RedbPersistence::verify_writer_relational_count_in_read(
                        &self.root.read,
                        logical,
                        table.len().map_err(RedbPersistence::storage_error)?,
                    )?;
                }
                let mut range = table
                    .range::<&[u8]>((lower, upper))
                    .map_err(RedbPersistence::storage_error)?;
                let next = if self.reverse {
                    range.next_back()
                } else {
                    range.next()
                };
                next.transpose()
                    .map_err(RedbPersistence::storage_error)?
                    .map(|(key, value)| finish(key.value(), value.value()))
                    .transpose()
            }
            WriterCursorFamily::Index => {
                let table = match self
                    .root
                    .read
                    .open_table(TableDefinition::<BorrowedRelationalIndexKey, &[u8]>::new(
                        &self.table,
                    )) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => {
                        return Err(Error::StoreCorrupted {
                            path: self.table.clone(),
                            reason: "missing durable posting family".into(),
                        });
                    }
                    Err(e) => return Err(RedbPersistence::storage_error(e)),
                };
                let mut range = table
                    .range::<&[u8]>((lower, upper))
                    .map_err(RedbPersistence::storage_error)?;
                let next = if self.reverse {
                    range.next_back()
                } else {
                    range.next()
                };
                next.transpose()
                    .map_err(RedbPersistence::storage_error)?
                    .map(|(key, value)| finish(key.value(), value.value()))
                    .transpose()
            }
            WriterCursorFamily::Text => {
                let utf8 = |b| {
                    std::str::from_utf8(b).map_err(|e| Error::Other(format!("cursor key: {e}")))
                };
                let lower = match lower {
                    Bound::Included(v) => Bound::Included(utf8(v)?),
                    Bound::Excluded(v) => Bound::Excluded(utf8(v)?),
                    Bound::Unbounded => Bound::Unbounded,
                };
                let upper = match upper {
                    Bound::Included(v) => Bound::Included(utf8(v)?),
                    Bound::Excluded(v) => Bound::Excluded(utf8(v)?),
                    Bound::Unbounded => Bound::Unbounded,
                };
                let table = match self
                    .root
                    .read
                    .open_table(TableDefinition::<&str, &[u8]>::new(&self.table))
                {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                    Err(e) => return Err(RedbPersistence::storage_error(e)),
                };
                let mut range = table
                    .range::<&str>((lower, upper))
                    .map_err(RedbPersistence::storage_error)?;
                let next = if self.reverse {
                    range.next_back()
                } else {
                    range.next()
                };
                next.transpose()
                    .map_err(RedbPersistence::storage_error)?
                    .map(|(key, value)| finish(key.value().as_bytes(), value.value()))
                    .transpose()
            }
        }
    }

    pub(crate) fn next_row(&mut self) -> Result<Option<WriterWindow<VersionedRow>>> {
        self.next_row_before(&mut || Ok(()))
    }

    pub(crate) fn next_row_before(
        &mut self,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Option<WriterWindow<VersionedRow>>> {
        #[cfg(feature = "test-seams")]
        self.root
            .note_read_source(crate::writer_query_test_seams::PhysicalFamily::Row);
        self.next_before(before, |key, value| {
            let row = RedbPersistence::decode_versioned_row(value, None)?;
            if RedbPersistence::rel_row_key(&row) != key {
                return Err(Error::StoreCorrupted {
                    path: "relational row".into(),
                    reason: "row cursor encountered a mismatched version key".into(),
                });
            }
            Ok(row)
        })
    }
}

pub(crate) struct WriterIndexPosting {
    pub(crate) tuple: IndexKey,
    pub(crate) row_id: RowId,
    pub(crate) created_tx: TxId,
    pub(crate) deleted_tx: Option<TxId>,
}

pub(crate) struct WriterCommitCursor {
    root: Arc<WriterSnapshot>,
    lower: u64,
    upper: u64,
    after: Option<u64>,
    reverse: bool,
    _control: OwnedMemoryReservation,
}

impl WriterSnapshot {
    pub(crate) fn change_log_cursor(
        self: &Arc<Self>,
        first: Lsn,
        last: Lsn,
    ) -> Result<WriterReadCursor> {
        let mut lower = [0; 16];
        lower[..8].copy_from_slice(&first.0.to_be_bytes());
        let mut upper = [255; 16];
        upper[..8].copy_from_slice(&last.0.to_be_bytes());
        self.cursor(
            "writer_change_log",
            WriterCursorFamily::Bytes,
            Bound::Included(&lower),
            Bound::Included(&upper),
            false,
        )
    }

    pub(crate) fn config_cursor(self: &Arc<Self>, prefix: &str) -> Result<WriterReadCursor> {
        let end = prefix_end(prefix.as_bytes());
        self.cursor(
            "config",
            WriterCursorFamily::Text,
            Bound::Included(prefix.as_bytes()),
            end.as_deref().map_or(Bound::Unbounded, Bound::Excluded),
            false,
        )
    }

    pub(crate) fn commit_cursor(
        self: &Arc<Self>,
        first: Lsn,
        last: Lsn,
        reverse: bool,
    ) -> Result<WriterCommitCursor> {
        let control = OwnedMemoryReservation::try_new_for(
            Arc::clone(&self.accountant),
            std::mem::size_of::<WriterCommitCursor>(),
            "writer_source",
            "commit_cursor",
            HINT,
        )?;
        Ok(WriterCommitCursor {
            root: Arc::clone(self),
            lower: first.0,
            upper: last.0,
            after: None,
            reverse,
            _control: control,
        })
    }

    pub(crate) fn commit_bounds(&self) -> Result<(Option<(Lsn, TxId)>, Option<(Lsn, TxId)>, u64)> {
        let table = self
            .read
            .open_table(COMMIT_INDEX_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        let first = table
            .first()
            .map_err(RedbPersistence::storage_error)?
            .map(|(lsn, tx)| (Lsn(lsn.value()), TxId(tx.value())));
        let last = table
            .last()
            .map_err(RedbPersistence::storage_error)?
            .map(|(lsn, tx)| (Lsn(lsn.value()), TxId(tx.value())));
        Ok((
            first,
            last,
            table.len().map_err(RedbPersistence::storage_error)?,
        ))
    }

    pub(crate) fn sync_source(&self, table: &str, row_id: RowId) -> Result<Option<(Lsn, u8)>> {
        let erasure = self.erasure.read();
        erasure.check_scope(Some(table), "sync provenance")?;
        let _scratch = OwnedMemoryReservation::try_new_for(
            Arc::clone(&self.accountant),
            table.len().saturating_add(8),
            "writer_source",
            "sync_point",
            HINT,
        )?;
        let key = RedbPersistence::sync_row_source_lsn_key(table, row_id);
        let lsns = match self.read.open_table(SYNC_ROW_SOURCE_LSN_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(RedbPersistence::storage_error(e)),
        };
        let Some(lsn) = lsns
            .get(key.as_slice())
            .map_err(RedbPersistence::storage_error)?
        else {
            return Ok(None);
        };
        let kinds = self
            .read
            .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        let kind = kinds
            .get(key.as_slice())
            .map_err(RedbPersistence::storage_error)?
            .ok_or_else(|| Error::Other("sync source LSN has no source-kind sidecar".into()))?;
        Ok(Some((Lsn(lsn.value()), kind.value())))
    }
}

impl WriterReadCursor {
    pub(crate) fn next_index(
        &mut self,
        current: bool,
    ) -> Result<Option<WriterWindow<WriterIndexPosting>>> {
        self.next_index_before(current, &mut || Ok(()))
    }
    pub(crate) fn next_index_before(
        &mut self,
        current: bool,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Option<WriterWindow<WriterIndexPosting>>> {
        #[cfg(feature = "test-seams")]
        self.root
            .note_read_source(crate::writer_query_test_seams::PhysicalFamily::Posting);
        self.next_before(before, |key, value| {
            let encoded = decode_window_exact::<EncodedRelationalIndexPosition>(key)?;
            crate::read_image_memory::reserve(key.len().saturating_mul(2))?;
            if RedbPersistence::encode(&encoded)? != key {
                return Err(Error::StoreCorrupted {
                    path: "relational index".into(),
                    reason: "noncanonical posting key".into(),
                });
            }
            let position: RelationalIndexPosition = encoded.into();
            if current && position.created_tx != TxId(0) {
                return Err(Error::StoreCorrupted {
                    path: "current relational index".into(),
                    reason: "current posting has a version suffix".into(),
                });
            }
            let (created_tx, deleted_tx) = if current {
                (decode_window_exact(value)?, None)
            } else {
                (position.created_tx, decode_window_exact(value)?)
            };
            Ok(WriterIndexPosting {
                tuple: position.key,
                row_id: position.row_id,
                created_tx,
                deleted_tx,
            })
        })
    }

    pub(crate) fn next_edge(&mut self) -> Result<Option<WriterWindow<AdjEntry>>> {
        let reverse = self.table == "graph_rev";
        self.next(|key, value| {
            let edge: AdjEntry = decode_window_exact(value)?;
            crate::read_image_memory::reserve(key.len())?;
            let expected = if reverse {
                RedbPersistence::graph_rev_key(&edge)
            } else {
                RedbPersistence::graph_fwd_key(&edge)
            };
            if expected != key {
                return Err(Error::StoreCorrupted {
                    path: "graph adjacency".into(),
                    reason: "edge key does not match its payload".into(),
                });
            }
            Ok(edge)
        })
    }

    pub(crate) fn next_membership(
        &mut self,
    ) -> Result<Option<WriterWindow<VectorPartitionMembershipRecord>>> {
        self.next(|key, value| {
            let record = decode_window_exact::<VectorPartitionMembershipRecord>(
                vector_partition_record_payload(VectorPartitionRecordKind::Membership, value)
                    .map_err(|reason| Error::StoreCorrupted {
                        path: "vector_partition_membership".into(),
                        reason,
                    })?,
            )?;
            if RedbPersistence::vector_partition_membership_key(&record) != key {
                return Err(Error::Other(
                    "membership cursor encountered a mismatched identity".into(),
                ));
            }
            Ok(record)
        })
    }

    pub(crate) fn next_change(&mut self) -> Result<Option<WriterWindow<ChangeLogEntry>>> {
        self.next(|_, value| decode_window_exact(value))
    }
}

impl WriterCommitCursor {
    pub(crate) fn next(&mut self) -> Result<Option<(Lsn, TxId)>> {
        let table = self
            .root
            .read
            .open_table(COMMIT_INDEX_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        let lower = if !self.reverse {
            self.after
                .map_or(Bound::Included(self.lower), Bound::Excluded)
        } else {
            Bound::Included(self.lower)
        };
        let upper = if self.reverse {
            self.after
                .map_or(Bound::Included(self.upper), Bound::Excluded)
        } else {
            Bound::Included(self.upper)
        };
        let mut range = table
            .range((lower, upper))
            .map_err(RedbPersistence::storage_error)?;
        let next = if self.reverse {
            range.next_back()
        } else {
            range.next()
        };
        let value = next
            .transpose()
            .map_err(RedbPersistence::storage_error)?
            .map(|(lsn, tx)| (Lsn(lsn.value()), TxId(tx.value())));
        if let Some((lsn, _)) = value {
            self.after = Some(lsn.0);
        }
        Ok(value)
    }
}

impl WriterSnapshot {
    pub(crate) fn table_change_cursor(
        self: &Arc<Self>,
        table: &str,
        first: Lsn,
        last: Lsn,
    ) -> Result<WriterReadCursor> {
        let _scratch = OwnedMemoryReservation::try_new_for(
            Arc::clone(&self.accountant),
            table.len().saturating_mul(2).saturating_add(34),
            "writer_source",
            "history_seek_keys",
            HINT,
        )?;
        let mut lower = Vec::with_capacity(table.len() + 17);
        lower.extend_from_slice(table.as_bytes());
        lower.push(0);
        lower.extend_from_slice(&first.0.to_be_bytes());
        lower.extend_from_slice(&0_u64.to_be_bytes());
        let mut upper = Vec::with_capacity(table.len() + 17);
        upper.extend_from_slice(table.as_bytes());
        upper.push(0);
        upper.extend_from_slice(&last.0.to_be_bytes());
        upper.extend_from_slice(&u64::MAX.to_be_bytes());
        self.cursor(
            "change_log_by_table",
            WriterCursorFamily::Bytes,
            Bound::Included(&lower),
            Bound::Included(&upper),
            false,
        )?
        .with_table_scope(table)
    }

    pub(crate) fn change_log_occurrences(&self, lsn: Lsn) -> Result<u64> {
        let counts = match self
            .read
            .open_table(TableDefinition::<u64, u64>::new("change_log_lsn_count"))
        {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
            Err(e) => return Err(RedbPersistence::storage_error(e)),
        };
        Ok(counts
            .get(lsn.0)
            .map_err(RedbPersistence::storage_error)?
            .map_or(0, |value| value.value()))
    }

    pub(crate) fn config_point<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<WriterWindow<T>>> {
        let erasure = self.erasure.read();
        erasure.check_scope(None, "config")?;
        let table = match self.read.open_table(CONFIG_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(RedbPersistence::storage_error(e)),
        };
        let Some(value) = table.get(key).map_err(RedbPersistence::storage_error)? else {
            return Ok(None);
        };
        let memory = ImageMemory::charging_for(
            Arc::clone(&self.accountant),
            "writer_source",
            "sync_point",
            HINT,
        );
        let value = memory.scope(|| decode_window_exact(value.value()))?;
        Ok(Some(WriterWindow {
            value,
            position: Vec::new(),
            _memory: memory,
        }))
    }
}

impl WriterReadCursor {
    pub(crate) fn next_table_change(&mut self) -> Result<Option<WriterWindow<ChangeLogEntry>>> {
        let root = Arc::clone(&self.root);
        self.next(|_, position| {
            if position.len() != 16 {
                return Err(Error::Other(
                    "table log selector has an invalid position width".into(),
                ));
            }
            let log = root
                .read
                .open_table(writer_history::WRITER_CHANGE_LOG_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let record = log
                .get(position)
                .map_err(RedbPersistence::storage_error)?
                .ok_or_else(|| {
                    Error::Other("table log selector names a missing occurrence".into())
                })?;
            decode_window_exact(record.value())
        })
    }
}

impl RedbPersistence {
    pub(crate) fn reserve_redb_allocator_state(
        &self,
    ) -> Result<crate::memory_accounting::RedbAllocatorStateCharge> {
        let accountant = self.proof_accountant().ok_or_else(|| {
            Error::Other("allocator ownership requires the attached memory accountant".into())
        })?;
        Ok(crate::memory_accounting::RedbAllocatorStateCharge::new(
            accountant,
        ))
    }
}

impl WriterSnapshot {
    pub(crate) fn exact_index_cursor(
        self: &Arc<Self>,
        table: &str,
        index: &str,
        current: bool,
        tuple: &IndexKey,
    ) -> Result<WriterReadCursor> {
        let tuple_bytes = tuple
            .iter()
            .map(|part| match part {
                DirectedValue::Asc(value) => value.0.estimated_bytes(),
                DirectedValue::Desc(value) => value.0.estimated_bytes(),
            })
            .fold(0usize, usize::saturating_add);
        let _scratch = OwnedMemoryReservation::try_new_for(
            Arc::clone(&self.accountant),
            tuple_bytes.saturating_mul(8).saturating_add(256),
            "writer_source",
            "index_seek_keys",
            HINT,
        )?;
        let lower = DurableRelationalIndexKey::new(tuple.clone(), RowId(0), TxId(0))?;
        let upper = DurableRelationalIndexKey::new(tuple.clone(), RowId(u64::MAX), TxId(u64::MAX))?;
        self.index_cursor(
            table,
            index,
            current,
            Bound::Included(&lower.0),
            Bound::Included(&upper.0),
            false,
        )
    }
}

impl WriterSnapshot {
    /// Build an admitted physical endpoint using the persistence codec. Query
    /// adapters need not duplicate its encoding or its row/version suffixes.
    pub(crate) fn index_position_key(
        &self,
        tuple: &IndexKey,
        row_id: RowId,
        created_tx: TxId,
    ) -> Result<WriterWindow<Vec<u8>>> {
        let tuple_bytes = tuple
            .iter()
            .map(|part| match part {
                DirectedValue::Asc(value) => value.0.estimated_bytes(),
                DirectedValue::Desc(value) => value.0.estimated_bytes(),
            })
            .fold(0usize, usize::saturating_add);
        let memory = ImageMemory::charging_for(
            Arc::clone(&self.accountant),
            "writer_source",
            "index_seek_keys",
            HINT,
        );
        memory.reserve(tuple_bytes.saturating_mul(8).saturating_add(256))?;
        let key = DurableRelationalIndexKey::new(tuple.clone(), row_id, created_tx)?;
        Ok(WriterWindow {
            value: key.0,
            position: Vec::new(),
            _memory: memory,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(path: &Path) -> (RedbPersistence, Arc<MemoryAccountant>) {
        let store = RedbPersistence::create(path).unwrap();
        let accountant = Arc::new(MemoryAccountant::no_limit());
        store.attach_memory_accountant(Arc::clone(&accountant));
        (store, accountant)
    }

    // Exercise the manager's real publication door with the durable source.
    struct RootApplicator {
        store: Arc<RedbPersistence>,
        prepared: parking_lot::Mutex<Option<Arc<WriterSnapshot>>>,
    }
    impl contextdb_tx::WriteSetApplicator for RootApplicator {
        fn apply(&self, ws: &WriteSet) -> Result<()> {
            let reservation = self.store.reserve_writer_snapshot()?;
            self.store.flush_data(ws)?;
            let root = self
                .store
                .prepare_writer_snapshot(
                    reservation,
                    SnapshotId(ws.relational_inserts[0].1.created_tx.0),
                    ws.commit_lsn.unwrap(),
                )
                .expect("post-durability root preparation must terminate on failure");
            *self.prepared.lock() = Some(root);
            Ok(())
        }
        fn new_row_id(&self) -> RowId {
            RowId(0)
        }
        fn publication_handle(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
            self.prepared
                .lock()
                .as_ref()
                .map(|root| Arc::clone(root) as Arc<dyn std::any::Any + Send + Sync>)
        }
    }

    #[test]
    fn published_writer_root_stays_old_until_explicit_swap_and_windows_release() {
        let directory = tempfile::tempdir().unwrap();
        let (store, accountant) = store(&directory.path().join("root.db"));
        store
            .flush_table_meta("items", &TableMeta::default())
            .unwrap();
        let initial = store
            .initialize_writer_snapshot(SnapshotId(0), Lsn(0))
            .unwrap();
        let store = Arc::new(store);
        let source = contextdb_tx::TransactionManager::new_with_directory(
            RootApplicator {
                store: Arc::clone(&store),
                prepared: parking_lot::Mutex::new(None),
            },
            TxId(1),
            Lsn(1),
            TxId(0),
            store.clone(),
            Some(initial),
        );
        let capture = || {
            source
                .capture_published_read()
                .handle
                .unwrap()
                .downcast::<WriterSnapshot>()
                .unwrap()
        };
        let old = capture();
        let old_identity = old.storage_identity();
        let row = VersionedRow {
            row_id: RowId(7),
            values: HashMap::from([("body".into(), Value::Text("durable body".repeat(1000)))]),
            created_tx: TxId(1),
            deleted_tx: None,
            lsn: Lsn(1),
            created_at: None,
        };
        let tx = source.begin();
        source
            .with_write_set(tx, |ws| {
                ws.relational_inserts.push(("items".into(), row.clone()))
            })
            .unwrap();
        source
            .commit_with_lsn_prepared_and_applied(
                tx,
                |_| Ok(()),
                |_, _| {
                    // Durability and preparation completed; logical publication has not.
                    assert_eq!(capture().storage_identity(), old_identity);
                    assert!(
                        old.row_cursor("items", RowId(7))
                            .unwrap()
                            .next_row()
                            .unwrap()
                            .is_none()
                    );
                    // A live redb write cannot delay capture at the published root.
                    store
                        .with_db(|db| {
                            let pending =
                                db.begin_write().map_err(RedbPersistence::storage_error)?;
                            let captured = capture();
                            assert_eq!(captured.storage_identity(), old_identity);
                            assert!(
                                captured
                                    .row_cursor("items", RowId(7))?
                                    .next_row()?
                                    .is_none()
                            );
                            drop(pending);
                            Ok(())
                        })
                        .unwrap();
                },
            )
            .unwrap();
        let current = capture();
        assert_ne!(current.storage_identity(), old_identity);
        assert_eq!((current.snapshot, current.lsn), (SnapshotId(1), Lsn(1)));
        let baseline = accountant.usage().used;
        let mut cursor = current.row_cursor("items", RowId(7)).unwrap();
        let cursor_only = accountant.usage().used;
        let window = cursor.next_row().unwrap().unwrap();
        assert_eq!(window.value, row);
        assert!(accountant.usage().used > cursor_only);
        drop(window);
        assert!(cursor.next_row().unwrap().is_none());
        assert_eq!(accountant.usage().used, cursor_only);
        drop(cursor);
        assert_eq!(accountant.usage().used, baseline);
        assert!(
            old.row_cursor("items", RowId(7))
                .unwrap()
                .next_row()
                .unwrap()
                .is_none()
        );
        // The manager uses the durable directory, not a mirrored tree.
        use contextdb_tx::CommitDirectory;
        assert_eq!(store.census().unwrap(), (1, Lsn(1)));
        assert!(store.contains(Lsn(1)).unwrap());
        assert_eq!(
            store.floor(Bound::Included(Lsn(1))).unwrap(),
            Some((Lsn(1), TxId(1)))
        );
        assert_eq!(store.floor(Bound::Excluded(Lsn(1))).unwrap(), None);
        let mut pinned = source
            .commit_directory_cursor(Bound::Unbounded, Bound::Unbounded)
            .unwrap();
        assert_eq!(store.remove(&[Lsn(1), Lsn(1), Lsn(99)]).unwrap(), 1);
        assert_eq!(store.census().unwrap(), (0, Lsn(0)));
        assert!(!store.contains(Lsn(1)).unwrap());
        assert_eq!(pinned.next_entry().unwrap(), Some((Lsn(1), TxId(1))));
        assert_eq!(pinned.next_entry().unwrap(), None);
        let bounds = store.writer_open_bounds(["items"]).unwrap();
        assert_eq!(
            (bounds.max_tx, bounds.max_lsn, bounds.commit_count),
            (TxId(1), Lsn(1), 0)
        );
        let retained = BTreeMap::from([(Lsn(2), TxId(2)), (Lsn(3), TxId(3))]);
        assert_eq!(store.replace(retained.clone()).unwrap(), 0);
        assert_eq!(store.load_commit_index().unwrap(), retained);
        assert_eq!(
            store.replace(BTreeMap::from([(Lsn(3), TxId(4))])).unwrap(),
            1
        );
        assert_eq!(
            store.load_commit_index().unwrap(),
            BTreeMap::from([(Lsn(3), TxId(4))])
        );
        let mut exclusive = store
            .cursor(Bound::Excluded(Lsn(2)), Bound::Included(Lsn(3)))
            .unwrap();
        assert_eq!(exclusive.next_entry().unwrap(), Some((Lsn(3), TxId(4))));
        assert_eq!(exclusive.next_entry().unwrap(), None);
        for (lower, upper) in [
            (Bound::Excluded(Lsn(u64::MAX)), Bound::Unbounded),
            (Bound::Unbounded, Bound::Excluded(Lsn(0))),
            (Bound::Included(Lsn(3)), Bound::Excluded(Lsn(3))),
            (Bound::Included(Lsn(9)), Bound::Included(Lsn(1))),
        ] {
            assert_eq!(
                store.cursor(lower, upper).unwrap().next_entry().unwrap(),
                None
            );
        }
        assert_eq!(store.replace(BTreeMap::new()).unwrap(), 1);
        let bounds = store.writer_open_bounds(["items"]).unwrap();
        assert_eq!(
            (bounds.max_tx, bounds.max_lsn, bounds.commit_count),
            (TxId(4), Lsn(3), 0)
        );
    }

    #[test]
    fn current_writer_postings_keep_one_member_after_repeated_key_changes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("postings.db");
        {
            let db = crate::Database::open(&path).unwrap();
            db.set_maintenance_policy(crate::MaintenancePolicy::CallerDriven);
            for sql in [
                "CREATE TABLE items (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
                "CREATE INDEX by_body ON items (body)",
                "INSERT INTO items VALUES (1, 'zero')",
            ] {
                db.execute(sql, &HashMap::new()).unwrap();
            }
            for i in 0..24 {
                db.execute(
                    &format!("UPDATE items SET body = 'value{i}' WHERE id = 1"),
                    &HashMap::new(),
                )
                .unwrap();
            }
        }
        let store = RedbPersistence::open(&path).unwrap();
        store.attach_memory_accountant(Arc::new(MemoryAccountant::no_limit()));
        let bounds = store.writer_open_bounds(["items"]).unwrap();
        let root = store
            .initialize_writer_snapshot(SnapshotId(bounds.max_tx.0), bounds.max_lsn)
            .unwrap();
        let mut current = root
            .index_cursor(
                "items",
                "by_body",
                true,
                Bound::Unbounded,
                Bound::Unbounded,
                false,
            )
            .unwrap();
        let posting = current.next_index(true).unwrap().unwrap();
        assert_eq!(
            posting.value.tuple,
            vec![DirectedValue::Asc(TotalOrdAsc(Value::Text(
                "value23".into()
            )))]
        );
        assert!(current.next_index(true).unwrap().is_none());
        let mut historical = root
            .index_cursor(
                "items",
                "by_body",
                false,
                Bound::Unbounded,
                Bound::Unbounded,
                false,
            )
            .unwrap();
        let mut count = 0;
        while historical.next_index(false).unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 25);
        let mut pk = root
            .index_cursor(
                "items",
                "__pk_id",
                true,
                Bound::Unbounded,
                Bound::Unbounded,
                false,
            )
            .unwrap();
        assert!(pk.next_index(true).unwrap().is_some());
        assert!(pk.next_index(true).unwrap().is_none());
    }

    #[test]
    fn graph_delete_updates_both_directions_and_preserves_unrelated_edges() {
        let directory = tempfile::tempdir().unwrap();
        let (store, _) = store(&directory.path().join("graph.db"));
        let node = |i| uuid::Uuid::from_u128(i);
        let edge = |source, target, created| AdjEntry {
            source: node(source),
            target: node(target),
            edge_type: "link".into(),
            properties: HashMap::new(),
            created_tx: TxId(created),
            deleted_tx: None,
            lsn: Lsn(created),
        };
        let mut edges = (2..102).map(|id| edge(id, 200, 1)).collect::<Vec<_>>();
        edges.extend([edge(1, 200, 1), edge(1, 200, 2), edge(1, 201, 1)]);
        store.append_graph_edges_batch(&edges).unwrap();
        let before = store
            .initialize_writer_snapshot(SnapshotId(2), Lsn(2))
            .unwrap();
        let mut ws = WriteSet::new();
        ws.commit_lsn = Some(Lsn(3));
        ws.adj_deletes
            .push((node(1), "link".into(), node(200), TxId(3)));
        store.flush_data(&ws).unwrap();
        let after = store
            .prepare_writer_snapshot(
                store.reserve_writer_snapshot().unwrap(),
                SnapshotId(3),
                Lsn(3),
            )
            .unwrap();
        for (root, stamp) in [(&before, None), (&after, Some(TxId(3)))] {
            let mut fwd = root.graph_cursor(node(1), false).unwrap();
            let mut selected = 0;
            while let Some(window) = fwd.next_edge().unwrap() {
                if window.value.target == node(200) {
                    selected += 1;
                    assert_eq!(window.value.deleted_tx, stamp);
                } else {
                    assert_eq!(window.value.deleted_tx, None);
                }
            }
            assert_eq!(selected, 2);
            let mut rev = root.graph_cursor(node(200), true).unwrap();
            let mut count = 0;
            while let Some(window) = rev.next_edge().unwrap() {
                count += 1;
                assert_eq!(
                    window.value.deleted_tx,
                    if window.value.source == node(1) {
                        stamp
                    } else {
                        None
                    }
                );
            }
            assert_eq!(count, 102);
        }
    }

    #[test]
    fn ordered_writer_history_preserves_same_lsn_occurrences_through_rewrite() {
        let directory = tempfile::tempdir().unwrap();
        let (store, _) = store(&directory.path().join("history.db"));
        let entries = vec![
            ChangeLogEntry::RowInsert {
                table: "a".into(),
                row_id: RowId(1),
                lsn: Lsn(9),
            },
            ChangeLogEntry::RowDelete {
                table: "a".into(),
                row_id: RowId(1),
                natural_key: crate::sync_types::NaturalKey::single("id".into(), Value::Int64(1)),
                lsn: Lsn(9),
            },
        ];
        store.append_change_log(Lsn(9), &entries).unwrap();
        let old = store
            .initialize_writer_snapshot(SnapshotId(0), Lsn(9))
            .unwrap();
        assert_eq!(old.change_log_occurrences(Lsn(9)).unwrap(), 2);
        let mut selected = old.table_change_cursor("a", Lsn(9), Lsn(9)).unwrap();
        for entry in &entries {
            assert_eq!(&selected.next_table_change().unwrap().unwrap().value, entry);
        }
        assert!(selected.next_table_change().unwrap().is_none());
        let mut cursor = old.change_log_cursor(Lsn(9), Lsn(9)).unwrap();
        for entry in &entries {
            assert_eq!(&cursor.next_change().unwrap().unwrap().value, entry);
        }
        assert!(cursor.next_change().unwrap().is_none());
        store.rewrite_change_log(&entries[..1]).unwrap();
        let new = store
            .prepare_writer_snapshot(
                store.reserve_writer_snapshot().unwrap(),
                SnapshotId(0),
                Lsn(9),
            )
            .unwrap();
        assert_eq!(new.change_log_occurrences(Lsn(9)).unwrap(), 1);
        assert_eq!(old.change_log_occurrences(Lsn(9)).unwrap(), 2);
        let mut cursor = new.change_log_cursor(Lsn(9), Lsn(9)).unwrap();
        assert_eq!(cursor.next_change().unwrap().unwrap().value, entries[0]);
        assert!(cursor.next_change().unwrap().is_none());
        assert!(
            old.change_log_cursor(Lsn(9), Lsn(9))
                .unwrap()
                .next_change()
                .unwrap()
                .is_some()
        );
    }
}
