//! Root-pinned writer reads. Decoded records belong to a caller window, never
//! to the published source; the only retained payload cache is redb's cache.
use super::*;
use crate::memory_accounting::{ContainedMemoryReservation, OwnedMemoryReservation};
use crate::read_image_memory::ImageMemory;
use std::ops::Bound;

fn decode_window_exact<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
    let owner = crate::read_image_memory::active_owner()?;
    let before = owner.held();
    let (value, consumed) = crate::read_image_memory::decode(bytes)?;
    if consumed != bytes.len() {
        drop(value);
        owner.release(owner.held() - before);
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
    pub(super) read: parking_lot::RwLock<Option<Arc<redb::ReadTransaction>>>,
    identity: redb::StorageSnapshotIdentity,
    pub(crate) snapshot: SnapshotId,
    pub(crate) lsn: Lsn,
    accountant: crate::memory_accounting::AccountantHandle,
    _control: ContainedMemoryReservation,
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
    /// Whether an authoritative erasure invalidated part of this captured view.
    pub(crate) fn erased(&self) -> bool {
        let erasure = self.erasure.read();
        !erasure.tables.is_empty() || !erasure.graph_keys.is_empty()
    }
    pub(super) fn internal_source_owner(&self) -> SourceWindowOwner {
        SourceWindowOwner::Write(self.accountant.clone(), crate::memory_accounting::WriterUse::retain_if_writer(&self.accountant))
    }

    pub(crate) fn row_body_bytes(
        &self,
        owner: &SourceWindowOwner,
        table: &str,
        row: &VersionedRow,
    ) -> Result<usize> {
        let read = self.owner_read()?;
        #[cfg(test)]
        stored_relational_row_bytes_fault_for_test()?;
        let _scratch = self.adapter_control(owner, table.len().saturating_add(128))?;
        self.check_table_scope(table)?;
        let name = RedbPersistence::rel_table_name(table);
        let physical = read
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
        {
            let mut roots = self.registry.lock();
            roots.retain(|root| !std::ptr::eq(root.as_ptr(), self));
            roots.shrink_to_fit();
        }
        drop(self.read.get_mut().take());
        // Rust then drops erasure, registry and the original reservation;
        // the existing schema field is the final counted source owner.
    }
}

#[derive(Default)]
struct WriterErasure {
    tables: BTreeSet<String>,
    graph_keys: BTreeSet<Vec<u8>>,
    _memory: Option<ContainedMemoryReservation>,
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
    /// The captured schema is charged, not admitted: a limit is persisted
    /// with the store, so refusing the open on it would brick a store whose
    /// operator once declared a limit below this floor, with no way back in
    /// to raise it. The limit sees the bytes, and the ordinary typed refusal
    /// fires on the next admission (the same rule as the writer page cache).
    fn prepared_writer_schema(&self) -> Result<Arc<WriterSchema>> {
        if let Some(schema) = self.writer_schema.lock().as_ref().cloned() {
            return Ok(schema);
        }
        let accountant = self
            .proof_accountant()
            .ok_or_else(|| Error::Other("writer schema requires its accountant".into()))?;
        let charge = |bytes: usize| {
            accountant.charge_retained(bytes);
            OwnedMemoryReservation::from_admitted(accountant.clone(), bytes)
        };
        let memory = charge(0).with_enclosing_arc_control::<WriterSchema>(true)?;
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
                let scratch = charge(value.value().len().saturating_mul(16).saturating_add(4096));
                let (meta, legacy) = Self::decode_table_meta_versioned(value.value())?;
                if legacy {
                    return Err(Error::Other("unsupported table metadata layout".into()));
                }
                // The table's directory entry and its body are one owner.
                let body_memory = charge(
                    meta.metadata_bytes()
                        .saturating_add(64)
                        .saturating_add(name.len().saturating_add(128)),
                ).with_enclosing_arc_control::<WriterTableMeta>(true)?;
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
        self.prepare_proof_owner()?;
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
            .fold(0usize, usize::saturating_add);
        let accountant = self.proof_accountant().unwrap();
        let memory = OwnedMemoryReservation::try_new_for(
            accountant.clone(),
            bytes,
            "writer_source",
            "captured_schema",
            HINT,
        )?.with_enclosing_arc_control::<WriterSchema>(false)?;
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
            )?.with_enclosing_arc_control::<WriterTableMeta>(false)?;
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
        writer: crate::memory_accounting::AccountantHandle,
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
            let memory = OwnedMemoryReservation::try_new_for(
                writer.clone(),
                bytes,
                "writer_source",
                "store_adapter",
                HINT,
            )?;
            let mut next = WriterErasure {
                tables: current.tables.clone(),
                graph_keys: current.graph_keys.clone(),
                _memory: Some(ContainedMemoryReservation::from_owned(memory)),
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
                        .map_or(0, ContainedMemoryReservation::bytes),
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
        self.prepare_proof_owner()?;
        // Open charges its first root like the captured schema: a persisted
        // limit below this floor must not make the store unopenable.
        let _schema = self.prepared_writer_schema()?;
        let accountant = self.proof_accountant().ok_or_else(|| {
            Error::Other("writer source requires the attached memory accountant".into())
        })?;
        let bytes = std::mem::size_of::<WriterSnapshot>() + 56;
        accountant.charge_retained(bytes);
        let control = OwnedMemoryReservation::from_admitted(accountant, bytes);
        self.prepare_writer_snapshot(control, snapshot, lsn)
    }

    /// Reserve this before durable commit. Failed reservation leaves the
    /// existing root untouched and must abort commit preparation.
    pub(crate) fn reserve_writer_snapshot(&self) -> Result<OwnedMemoryReservation> {
        self.prepare_proof_owner()?;
        let _schema = self.prepared_writer_schema()?;
        let accountant = self.proof_accountant().ok_or_else(|| {
            Error::Other("writer source requires the attached memory accountant".into())
        })?;
        OwnedMemoryReservation::try_new_for(
            accountant,
            std::mem::size_of::<WriterSnapshot>() + 56,
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
        self.prepare_proof_owner()?;
        let accountant = self.proof_accountant().ok_or_else(|| {
            Error::Other("writer source requires the attached memory accountant".into())
        })?;
        let schema = self.writer_schema.lock().as_ref().cloned().ok_or_else(|| {
            Error::Other("publication schema was not prepared before durability".into())
        })?;
        self.with_db(|db| {
            db.install_local_snapshot_provider(self.writer_root_provider());
            let read = Arc::new(db.begin_verified_read().map_err(Self::storage_error)?);
            let identity = read.storage_snapshot_identity();
            let root = Arc::new(WriterSnapshot {
                read: parking_lot::RwLock::new(Some(read)),
                identity,
                snapshot,
                lsn,
                accountant,
                _control: ContainedMemoryReservation::from_owned(control),
                erasure: parking_lot::RwLock::new(WriterErasure::default()),
                schema,
                registry: Arc::clone(&self.writer_snapshots),
            });
            let mut roots = self.writer_snapshots.lock();
            roots.retain(|root| root.strong_count() != 0);
            roots.push(Arc::downgrade(&root));
            drop(roots);
            // Admit the lease while its original native view protects the pages.
            // Keep that view until recycle, so accepted cursors retain native pins.
            drop(
                db.begin_external_read(identity)
                    .map_err(Self::storage_error)?,
            );
            Ok(root)
        })
    }
}

// Same on-disk type and comparator, with borrowed guards. A cursor admits
// its owned resume key before copying rather than letting `value()` allocate.
#[derive(Debug)]
pub(super) struct BorrowedRelationalIndexKey;
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
/// Every operation receives its funding capability with its captured root.
/// Read ownership cannot be recovered from the writer accountant.
#[derive(Clone)]
pub(crate) enum SourceWindowOwner {
    Read(Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>),
    Write(crate::memory_accounting::AccountantHandle, Option<crate::memory_accounting::WriterUse>),
}

pub(crate) enum SourceWindowCredit {
    Read(contextdb_core::read_memory::ReadCredit),
    Write(OwnedMemoryReservation),
}

impl SourceWindowOwner {
    pub(crate) fn writer(writer: crate::memory_accounting::AccountantHandle) -> Self {
        let token = crate::memory_accounting::WriterUse::retain_if_writer(&writer);
        Self::Write(writer, token)
    }
    pub(super) fn reserve_box_credit(
        &self,
        bytes: usize,
    ) -> Result<contextdb_core::read_memory::ReadCredit> {
        let budget: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget> = match self {
            Self::Read(read) => Arc::clone(read),
            Self::Write(writer, _) => writer.read_budget()?,
        };
        contextdb_core::read_memory::ReadCredit::try_new(
            budget,
            bytes,
            "writer_source",
            "store_adapter",
            HINT,
        )
    }

    pub(crate) fn reserve(
        &self,
        bytes: usize,
        operation: &'static str,
    ) -> Result<SourceWindowCredit> {
        let mut credit = match self {
            Self::Read(read) => SourceWindowCredit::Read(
                contextdb_core::read_memory::ReadCredit::new(Arc::clone(read)),
            ),
            Self::Write(writer, _) => {
                SourceWindowCredit::Write(OwnedMemoryReservation::new(writer.clone()))
            }
        };
        match &mut credit {
            SourceWindowCredit::Read(read) => {
                read.try_grow_for(bytes, "writer_source", operation, HINT)?
            }
            SourceWindowCredit::Write(writer) => {
                writer.try_grow_for(bytes, "writer_source", operation, HINT)?
            }
        }
        Ok(credit)
    }

    pub(super) fn endpoint(
        &self,
        parts: &[&[u8]],
        operation: &'static str,
    ) -> Result<SourceResume> {
        let len = parts.iter().try_fold(0usize, |len, part| {
            len.checked_add(part.len())
                .ok_or_else(|| Error::Other("source endpoint size overflow".into()))
        })?;
        let layout = std::alloc::Layout::array::<u8>(len)
            .map_err(|_| Error::Other("source endpoint layout overflow".into()))?;
        let credit = self.reserve(layout.size(), operation)?;
        let mut key = Box::<[u8]>::new_uninit_slice(len);
        for (slot, byte) in key
            .iter_mut()
            .zip(parts.iter().flat_map(|part| part.iter()))
        {
            slot.write(*byte);
        }
        // The checked length is exactly the concatenated iterator length;
        // every element was initialized by the loop above.
        let key = unsafe { key.assume_init() };
        Ok(SourceResume {
            key,
            _credit: credit,
        })
    }

    pub(super) fn prefix_end(
        &self,
        prefix: &[u8],
        operation: &'static str,
    ) -> Result<Option<SourceResume>> {
        let Some(last) = prefix.iter().rposition(|byte| *byte != u8::MAX) else {
            return Ok(None);
        };
        let mut endpoint = self.endpoint(&[&prefix[..=last]], operation)?;
        endpoint.key[last] += 1;
        Ok(Some(endpoint))
    }

    pub(super) fn image(&self, root: &WriterSnapshot, operation: &'static str) -> Arc<ImageMemory> {
        match self {
            Self::Read(read) => ImageMemory::charging_reader_for(
                root.accountant.clone(),
                Arc::clone(read),
                "writer_source",
                operation,
                HINT,
            ),
            Self::Write(writer, _) => {
                ImageMemory::charging_for(writer.clone(), "writer_source", operation, HINT)
            }
        }
    }
}

// Each separately owned resume key carries its own credit. This composes a
// move-only ReadCredit without a detach, readmission, or reader/writer transfer.
pub(super) struct SourceResume {
    pub(super) key: Box<[u8]>,
    _credit: SourceWindowCredit,
}

pub(crate) struct WriterReadCursor {
    root: Arc<WriterSnapshot>,
    table: Box<str>,
    logical_table: Option<Box<str>>,
    family: WriterCursorFamily,
    lower: Bound<Box<[u8]>>,
    upper: Bound<Box<[u8]>>,
    after: Option<SourceResume>,
    reverse: bool,
    finished: bool,
    examined: u64,
    historical_proved_group: Option<RowId>,
    owner: SourceWindowOwner,
    _continuation: SourceWindowCredit,
    _logical_credit: SourceWindowCredit,
}

pub(crate) struct WriterWindow<T> {
    pub(crate) value: T,
    pub(super) position: Box<[u8]>,
    _root: Arc<WriterSnapshot>,
    // The image retains its own original writer role through charge refund.
    _memory: Arc<ImageMemory>,
}

impl<T> WriterWindow<T> {
    /// Move the value while retaining its original source credit and root.
    pub(crate) fn into_parts(self) -> (T, WriterWindow<()>) {
        (
            self.value,
            WriterWindow {
                value: (),
                position: self.position,
                _root: self._root,
                _memory: self._memory,
            },
        )
    }

    pub(crate) fn position(&self) -> &[u8] {
        &self.position
    }

    pub(super) fn map<U>(self, f: impl FnOnce(T) -> U) -> WriterWindow<U> {
        WriterWindow {
            value: f(self.value),
            position: self.position,
            _root: self._root,
            _memory: self._memory,
        }
    }
    pub(super) fn boxed<U: ?Sized>(
        self,
        coerce: impl FnOnce(Box<Self>) -> Box<U>,
    ) -> Result<contextdb_core::read_memory::OwnedReadBox<U>> {
        let budget = self._memory.source_box_budget()?;
        let credit = contextdb_core::read_memory::ReadCredit::try_new(
            budget,
            std::alloc::Layout::new::<Self>().size(),
            "writer_source",
            "store_adapter",
            HINT,
        )?;
        let payload = coerce(Box::new(self));
        Ok(contextdb_core::read_memory::OwnedReadBox::new(
            payload, credit,
        ))
    }
}
impl WriterSnapshot {
    pub(super) fn adapter_control(
        &self,
        owner: &SourceWindowOwner,
        bytes: usize,
    ) -> Result<SourceWindowCredit> {
        owner.reserve(bytes, "store_adapter")
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
                    .map_or(0, ContainedMemoryReservation::bytes),
            )
    }
    pub(crate) fn content_size_totals(
        &self,
        memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before_table: &mut dyn FnMut() -> Result<()>,
    ) -> Result<super::content_size::ContentSizeTotals> {
        let read = self.owner_read()?;
        super::content_size::read_totals(read.as_ref(), "writer source", memory, before_table)
    }

    /// Visit every maintained count this root stores for one column list
    /// under one bound leading prefix, in stored key order, with the full
    /// group key and its count. `false` means the table's counts are not ready
    /// at this root and nothing was visited. Each record is decoded under its
    /// own admission, released after `visit` returns, and checked against the
    /// table, list and prefix it was asked for.
    pub(crate) fn maintained_count_groups<E: From<Error>>(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        table: &str,
        columns: &[(String, contextdb_core::SortDirection)],
        prefix: &[DirectedValue],
        before_touch: &mut dyn FnMut() -> std::result::Result<(), E>,
        visit: &mut dyn FnMut(IndexKey, u64) -> std::result::Result<(), E>,
    ) -> std::result::Result<bool, E> {
        self.erasure
            .read()
            .check_scope(Some(table), RELATIONAL_INDEX_COUNT_TABLE.name())?;
        let read = self.owner_read()?;
        let meta = self
            .borrow_captured_table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.into()))?;
        let selected = meta
            .indexes
            .iter()
            .enumerate()
            .filter(|(position, index)| {
                !meta.indexes[..*position]
                    .iter()
                    .any(|earlier| earlier.name == index.name)
            })
            .map(|(_, index)| index)
            .filter(|index| {
                index.columns.len() >= columns.len() && index.columns[..columns.len()] == *columns
            })
            .min_by(|left, right| left.name.cmp(&right.name));
        let Some(index) = selected else {
            return Ok(false);
        };
        let witness_memory = ImageMemory::charging_reader_for(
            self.accountant.clone(),
            read_memory.clone(),
            "writer_source",
            "count_witness_names",
            HINT,
        );
        witness_memory.reserve(
            table
                .len()
                .saturating_add(index.name.len())
                .saturating_mul(2)
                .saturating_add(256),
        )?;
        RedbPersistence::verify_history_root(
            read.as_ref(),
            &RedbPersistence::rel_table_name(table),
        )?;
        RedbPersistence::verify_history_root(
            read.as_ref(),
            &RedbPersistence::relational_index_table_name(table, &index.name),
        )?;
        let ready = match read.open_table(RELATIONAL_INDEX_READY_TABLE) {
            Ok(ready) => ready,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
            Err(error) => return Err(RedbPersistence::storage_error(error).into()),
        };
        if ready
            .get(table)
            .map_err(RedbPersistence::storage_error)?
            .is_none_or(|version| version.value() != RELATIONAL_INDEX_READY_VERSION)
        {
            return Ok(false);
        }
        let counts =
            read.open_table(RELATIONAL_INDEX_COUNT_TABLE)
                .map_err(|error| match error {
                    redb::TableError::TableDoesNotExist(_) => Error::StoreCorrupted {
                        path: "writer source".into(),
                        reason: format!("maintained counts of ready table '{table}' are absent"),
                    },
                    other => RedbPersistence::storage_error(other),
                })?;
        let lower = RedbPersistence::relational_index_count_range_prefix(table, columns, prefix)?;
        let upper = prefix_end(&lower);
        let range = match upper.as_deref() {
            Some(upper) => counts.range::<&[u8]>(lower.as_slice()..upper),
            None => counts.range::<&[u8]>(lower.as_slice()..),
        }
        .map_err(RedbPersistence::storage_error)?;
        let corrupt = |reason: &str| Error::StoreCorrupted {
            path: "writer source".into(),
            reason: format!("maintained count of '{table}': {reason}"),
        };
        for entry in range {
            before_touch()?;
            self.erasure
                .read()
                .check_scope(Some(table), RELATIONAL_INDEX_COUNT_TABLE.name())?;
            let (key, count) = entry.map_err(RedbPersistence::storage_error)?;
            let memory = ImageMemory::charging_reader_for(
                self.accountant.clone(),
                Arc::clone(&read_memory),
                "writer_source",
                "maintained_count_groups",
                HINT,
            );
            let decoded: EncodedRelationalIndexCountKey =
                memory.scope(|| decode_window_exact(key.value()))?;
            if decoded.table != table
                || decoded.columns.as_slice() != columns
                || decoded.prefix.len() != columns.len()
            {
                return Err(corrupt("a record in its range names another column list").into());
            }
            let group: IndexKey = decoded
                .prefix
                .into_iter()
                .map(|(descending, value)| {
                    if descending {
                        DirectedValue::Desc(TotalOrdDesc(value))
                    } else {
                        DirectedValue::Asc(TotalOrdAsc(value))
                    }
                })
                .collect();
            if group[..prefix.len()] != *prefix {
                return Err(corrupt("a record in its range has another leading prefix").into());
            }
            let count = count.value();
            if count == 0 {
                return Err(corrupt("a stored count is zero").into());
            }
            visit(group, count)?;
            drop(memory);
        }
        Ok(true)
    }

    pub(super) fn owner_read(&self) -> Result<Arc<redb::ReadTransaction>> {
        let read = self.read
            .read()
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::StoreHandleRecycleFailed {
                path: "writer source".into(),
                reason: "the retained source has no open storage view".into(),
            })?;
        if read.storage_snapshot_identity() != self.identity {
            return Err(Error::StoreCorrupted {
                path: "writer source".into(),
                reason: "the retained storage transaction differs from its captured root".into(),
            });
        }
        Ok(read)
    }

    pub(crate) fn check_table_scope(&self, table: &str) -> Result<()> {
        self.erasure
            .read()
            .check_scope(Some(table), "ACL entitlement")?;
        RedbPersistence::verify_history_root(
            self.owner_read()?.as_ref(),
            &RedbPersistence::rel_table_name(table),
        )
    }

    pub(crate) fn storage_identity(&self) -> redb::StorageSnapshotIdentity {
        self.identity
    }
    #[cfg(feature = "test-seams")]
    pub(crate) fn note_read_source(&self, family: crate::writer_query_test_seams::PhysicalFamily) {
        if let Ok(read) = self.owner_read() {
            crate::writer_query_test_seams::note_physical_source(family, &read);
        }
    }

    pub(crate) fn cursor(
        self: &Arc<Self>,
        owner: SourceWindowOwner,
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
        let historical = if table.starts_with("__rel_index_") {
            table.strip_suffix("_current").unwrap_or(table)
        } else {
            table
        };
        RedbPersistence::verify_history_root(self.owner_read()?.as_ref(), historical)?;
        let logical = table.strip_prefix("rel_");
        let continuation = owner.reserve(
            table
                .len()
                .saturating_add(bound_len(&lower))
                .saturating_add(bound_len(&upper)),
            "cursor_keys",
        )?;
        let logical_credit = owner.reserve(logical.map_or(0, str::len), "cursor_keys")?;
        Ok(WriterReadCursor {
            root: Arc::clone(self),
            table: Box::<str>::from(table),
            logical_table: logical.map(Box::<str>::from),
            family,
            lower: lower.map(Box::<[u8]>::from),
            upper: upper.map(Box::<[u8]>::from),
            after: None,
            reverse,
            finished: false,
            examined: 0,
            historical_proved_group: None,
            owner,
            _continuation: continuation,
            _logical_credit: logical_credit,
        })
    }

    pub(crate) fn row_cursor(
        self: &Arc<Self>,
        owner: SourceWindowOwner,
        table: &str,
        row_id: RowId,
    ) -> Result<WriterReadCursor> {
        let physical = owner.endpoint(&[b"rel_", table.as_bytes()], "cursor_keys")?;
        let name = std::str::from_utf8(&physical.key).expect("UTF-8 table name with ASCII prefix");
        let prefix = row_id.0.to_be_bytes();
        let end = owner.prefix_end(&prefix, "cursor_keys")?;
        self.cursor(
            owner,
            name,
            WriterCursorFamily::Bytes,
            Bound::Included(&prefix),
            end.as_ref()
                .map_or(Bound::Unbounded, |end| Bound::Excluded(end.key.as_ref())),
            true,
        )
    }

    pub(crate) fn exact_row(
        self: &Arc<Self>,
        owner: SourceWindowOwner,
        table: &str,
        key: &[u8; 24],
    ) -> Result<WriterReadCursor> {
        let physical = owner.endpoint(&[b"rel_", table.as_bytes()], "cursor_keys")?;
        let name = std::str::from_utf8(&physical.key).expect("UTF-8 table name with ASCII prefix");
        self.cursor(
            owner,
            name,
            WriterCursorFamily::Bytes,
            Bound::Included(key),
            Bound::Included(key),
            false,
        )
    }

    pub(crate) fn graph_cursor(
        self: &Arc<Self>,
        owner: SourceWindowOwner,
        node: NodeId,
        reverse: bool,
    ) -> Result<WriterReadCursor> {
        let prefix = node.as_bytes();
        let end = owner.prefix_end(prefix, "cursor_keys")?;
        self.cursor(
            owner,
            if reverse {
                GRAPH_REV_TABLE.name()
            } else {
                GRAPH_FWD_TABLE.name()
            },
            WriterCursorFamily::Bytes,
            Bound::Included(prefix),
            end.as_ref()
                .map_or(Bound::Unbounded, |end| Bound::Excluded(end.key.as_ref())),
            false,
        )
    }

    /// Read the canonical body named by a placement in this exact root.
    /// Placement lifetimes stay on the membership; the body keeps its own stamps.
    pub(crate) fn export_vector_body(
        self: &Arc<Self>,
        owner: SourceWindowOwner,
        membership: &VectorPartitionMembershipRecord,
    ) -> Result<WriterWindow<VectorEntry>> {
        let key_bytes = membership
            .index
            .table
            .len()
            .checked_add(membership.index.column.len())
            .and_then(|bytes| bytes.checked_add(34))
            .ok_or_else(|| Error::Other("vector export key size overflow".into()))?;
        let _key_credit = owner.reserve(key_bytes, "export_vector_identity")?;
        let key = RedbPersistence::vector_identity_key(
            &membership.index,
            membership.row_id,
            membership.vector_created_tx,
            membership.vector_lsn,
        );
        let mut cursor = self
            .cursor(
                owner,
                VECTORS_TABLE.name(),
                WriterCursorFamily::Bytes,
                Bound::Included(&key),
                Bound::Included(&key),
                false,
            )?
            .with_table_scope(&membership.index.table)?;
        cursor
            .next(|physical_key, bytes| {
                if physical_key != key.as_slice() {
                    return Err(Error::StoreCorrupted {
                        path: "vectors".into(),
                        reason: "export body key differs from source membership".into(),
                    });
                }
                let persisted: PersistedVectorEntry = decode_window_exact(bytes)?;
                if persisted.index != membership.index
                    || persisted.row_id != membership.row_id
                    || persisted.created_tx != membership.vector_created_tx
                    || persisted.lsn != membership.vector_lsn
                {
                    return Err(Error::StoreCorrupted {
                        path: "vectors".into(),
                        reason: "export body identity differs from source membership".into(),
                    });
                }
                crate::read_image_memory::reserve(persisted.vector.decoded_bytes())?;
                Ok(VectorEntry {
                    index: persisted.index,
                    row_id: persisted.row_id,
                    vector: persisted.vector.to_f32(),
                    created_tx: persisted.created_tx,
                    deleted_tx: persisted.deleted_tx,
                    lsn: persisted.lsn,
                })
            })?
            .ok_or_else(|| Error::StoreCorrupted {
                path: "vectors".into(),
                reason: "source membership export lost its canonical body".into(),
            })
    }

    pub(crate) fn membership_cursor(
        self: &Arc<Self>,
        owner: SourceWindowOwner,
        index: &VectorIndexRef,
        partition: &VectorPartitionKey,
    ) -> Result<WriterReadCursor> {
        // Legacy codec estimate: exact codec allocation sizing remains unresolved.
        let _scratch = owner.reserve(
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
            "membership_seek_keys",
        )?;
        let prefix = RedbPersistence::vector_partition_prefix(index, partition);
        let end = owner.prefix_end(&prefix, "cursor_keys")?;
        self.cursor(
            owner,
            VECTOR_PARTITION_MEMBERSHIP_TABLE.name(),
            WriterCursorFamily::Bytes,
            Bound::Included(&prefix),
            end.as_ref()
                .map_or(Bound::Unbounded, |end| Bound::Excluded(end.key.as_ref())),
            false,
        )?
        .with_table_scope(&index.table)
    }

    /// Ordered index endpoints use the canonical index codec/comparator,
    /// including complete row/version suffixes supplied by the query planner.
    pub(crate) fn index_cursor(
        self: &Arc<Self>,
        owner: SourceWindowOwner,
        table: &str,
        index: &str,
        current: bool,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        reverse: bool,
    ) -> Result<WriterReadCursor> {
        // Same durable index-name codec: hash table, NUL, index, then
        // format the fixed ASCII name. Both owned byte arrays are preadmitted.
        let input = owner.endpoint(
            &[table.as_bytes(), &[0], index.as_bytes()],
            "index_seek_keys",
        )?;
        let hash = blake3::hash(&input.key).to_hex();
        let physical = owner.endpoint(
            &[
                b"__rel_index_",
                hash.as_bytes(),
                if current { b"_current" } else { b"" },
            ],
            "index_seek_keys",
        )?;
        let name = std::str::from_utf8(&physical.key).expect("ASCII durable index table name");
        let _witness_names =
            owner.reserve(table.len().saturating_add(64), "index_witness_names")?;
        // Both witnesses belong to this exact retained transaction, including
        // ranges that have no current posting and never load a row body.
        let read = self.owner_read()?;
        RedbPersistence::verify_history_root(
            read.as_ref(),
            &RedbPersistence::rel_table_name(table),
        )?;
        RedbPersistence::verify_history_root(
            read.as_ref(),
            name.strip_suffix("_current").unwrap_or(name),
        )?;
        self.cursor(
            owner,
            name,
            WriterCursorFamily::Index,
            lower,
            upper,
            reverse,
        )?
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
            None => {
                !self.tables.is_empty()
                    && physical != GRAPH_FWD_TABLE.name()
                    && physical != GRAPH_REV_TABLE.name()
            }
        };
        if affected {
            Err(Self::refusal())
        } else {
            Ok(())
        }
    }
    fn check_key(&self, physical: &str, key: &[u8]) -> Result<()> {
        if (physical == GRAPH_FWD_TABLE.name() || physical == GRAPH_REV_TABLE.name())
            && self.graph_keys.contains(key)
        {
            Err(Self::refusal())
        } else {
            Ok(())
        }
    }
}

impl WriterReadCursor {
    fn with_table_scope(mut self, table: &str) -> Result<Self> {
        let credit = self.owner.reserve(table.len(), "cursor_keys")?;
        let logical = Box::<str>::from(table);
        let prior = self.logical_table.replace(logical);
        drop(prior);
        self._logical_credit = credit;
        Ok(self)
    }
    pub(crate) fn examined_entries(&self) -> u64 {
        self.examined
    }
    pub(crate) fn storage_identity(&self) -> redb::StorageSnapshotIdentity {
        self.root.storage_identity()
    }

    fn bounds(&self) -> (Bound<&[u8]>, Bound<&[u8]>) {
        let mut lower = self.lower.as_ref().map(|v| v.as_ref());
        let mut upper = self.upper.as_ref().map(|v| v.as_ref());
        if let Some(after) = self.after.as_ref().map(|resume| resume.key.as_ref()) {
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
        self.next_before_handoff(before, decode, Ok)
    }

    fn next_before_handoff<T, H>(
        &mut self,
        before: &mut dyn FnMut() -> Result<()>,
        decode: impl FnOnce(&[u8], &[u8]) -> Result<T>,
        handoff: impl FnOnce(WriterWindow<T>) -> Result<H>,
    ) -> Result<Option<H>> {
        if self.finished {
            return Ok(None);
        }
        let memory = self.owner.image(&self.root, "cursor_window");
        let mut found = false;
        let result = self.read_one(
            &memory,
            &mut || {
                found = true;
                before()
            },
            decode,
        );
        if found {
            self.examined = self.examined.saturating_add(1);
        }
        let result = result?;
        let Some((key, value)) = result else {
            self.finished = true;
            // Payload and its unique credit are destroyed together.
            drop(self.after.take());
            return Ok(None);
        };
        // read_one admitted the new resume before allocating it, while the
        // old resume remains alive. Window position is a separate allocation.
        memory.reserve(key.key.len())?;
        let position = Box::<[u8]>::from(key.key.as_ref());
        let delivered = handoff(WriterWindow {
            value,
            position,
            _root: Arc::clone(&self.root),
            _memory: memory,
        })?;
        drop(self.after.replace(key));
        Ok(Some(delivered))
    }

    fn read_one<T>(
        &self,
        memory: &Arc<ImageMemory>,
        before: &mut dyn FnMut() -> Result<()>,
        decode: impl FnOnce(&[u8], &[u8]) -> Result<T>,
    ) -> Result<Option<(SourceResume, T)>> {
        let read = self.root.owner_read()?;
        self.root
            .erasure
            .read()
            .check_scope(self.logical_table.as_deref(), &self.table)?;
        let (lower, upper) = self.bounds();
        #[cfg(any(test, feature = "test-seams"))]
        if self.table.starts_with("graph_") || matches!(self.table.as_ref(), "change_log" | "writer_change_log") {
            crate::read_work_probe::note_graph(&self.table, "range", &[], None, None);
        }
        let finish = |key: &[u8], value: &[u8]| -> Result<(SourceResume, T)> {
            before()?;
            let erasure = self.root.erasure.read();
            erasure.check_scope(self.logical_table.as_deref(), &self.table)?;
            erasure.check_key(&self.table, key)?;
            let credit = self.owner.reserve(key.len(), "cursor_keys")?;
            let resume = SourceResume {
                key: Box::<[u8]>::from(key),
                _credit: credit,
            };
            let decoded = memory.scope(|| decode(key, value))?;
            Ok((resume, decoded))
        };
        match self.family {
            WriterCursorFamily::Bytes => {
                let graph = self.table.as_ref() == GRAPH_FWD_TABLE.name()
                    || self.table.as_ref() == GRAPH_REV_TABLE.name();
                let storage_error = |error: redb::StorageError| {
                    if self.table.starts_with("rel_") {
                        return RedbPersistence::relational_storage_error(
                            self.logical_table.as_deref().unwrap_or("relational row"), error);
                    }
                    let corrupted = graph && matches!(&error, redb::StorageError::Corrupted(_));
                    match RedbPersistence::storage_error(error) {
                        Error::Other(reason) if corrupted => Error::StoreCorrupted {
                            path: "graph adjacency".into(),
                            reason,
                        },
                        other => other,
                    }
                };
                let table = match read.open_table(TableDefinition::<&[u8], &[u8]>::new(&self.table))
                {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => {
                        if graph {
                            return Err(Error::StoreCorrupted {
                                path: "graph adjacency".into(),
                                reason: "graph projection table missing".into(),
                            });
                        }
                        return Ok(None);
                    }
                    Err(redb::TableError::Storage(error)) => return Err(storage_error(error)),
                    Err(error) => {
                        return Err(match RedbPersistence::storage_error(error) {
                            Error::Other(reason) if graph => Error::StoreCorrupted {
                                path: "graph adjacency".into(),
                                reason,
                            },
                            other => other,
                        });
                    }
                };
                if self.table.starts_with("rel_")
                    && self.after.is_none()
                    && let Some(logical) = self.logical_table.as_deref()
                {
                    memory.reserve(logical.len().saturating_add(32))?;
                    RedbPersistence::verify_writer_relational_count_in_read(
                        &read,
                        logical,
                        table.len().map_err(|error| {
                            RedbPersistence::storage_source_error(
                                self.logical_table.as_deref().unwrap_or(&self.table),
                                error,
                            )
                        })?,
                    )?;
                }
                if let (Bound::Included(low), Bound::Included(high)) = (&lower, &upper)
                    && low == high
                {
                    #[cfg(feature = "test-seams")]
                    if self.table.starts_with("rel_") {
                        crate::read_work_probe::note_row_version_point_read();
                    }
                    #[cfg(any(test, feature = "test-seams"))]
                    if self.table.starts_with("graph_") || matches!(self.table.as_ref(), "change_log" | "writer_change_log") {
                        crate::read_work_probe::graph_storage_key(&self.table, "get", *low);
                    }
                    return table
                        .get(*low)
                        .map_err(|error| {
                            RedbPersistence::storage_source_error(
                                self.logical_table.as_deref().unwrap_or(&self.table),
                                error,
                            )
                        })?
                        .map(|value| finish(low, value.value()))
                        .transpose();
                }
                let mut range = table.range::<&[u8]>((lower, upper)).map_err(|error| {
                    RedbPersistence::storage_source_error(
                        self.logical_table.as_deref().unwrap_or(&self.table),
                        error,
                    )
                })?;
                let next = if self.reverse {
                    range.next_back()
                } else {
                    range.next()
                };
                next.transpose()
                    .map_err(|error| RedbPersistence::storage_source_error(self.logical_table.as_deref().unwrap_or(&self.table), error))?
                    .map(|(key, value)| {
                        #[cfg(any(test, feature = "test-seams"))]
                        if self.table.starts_with("graph_") || matches!(self.table.as_ref(), "change_log" | "writer_change_log") {
                            crate::read_work_probe::graph_storage_key(&self.table, "iteration_step", key.value());
                        }
                        finish(key.value(), value.value())
                    })
                    .transpose()
            }
            WriterCursorFamily::Index => {
                let table = match read
                    .open_table(TableDefinition::<BorrowedRelationalIndexKey, &[u8]>::new(
                        &self.table,
                    )) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => {
                        return Err(Error::StoreCorrupted {
                            path: self.table.as_ref().to_owned(),
                            reason: "missing durable posting family".into(),
                        });
                    }
                    Err(e) => return Err(RedbPersistence::storage_error(e)),
                };
                let mut range = table.range::<&[u8]>((lower, upper)).map_err(|error| {
                    RedbPersistence::storage_source_error(
                        self.logical_table.as_deref().unwrap_or(&self.table),
                        error,
                    )
                })?;
                let next = if self.reverse {
                    range.next_back()
                } else {
                    range.next()
                };
                next.transpose()
                    .map_err(|error| {
                        RedbPersistence::storage_source_error(
                            self.logical_table.as_deref().unwrap_or(&self.table),
                            error,
                        )
                    })?
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
                let table = match read.open_table(TableDefinition::<&str, &[u8]>::new(&self.table))
                {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                    Err(e) => return Err(RedbPersistence::storage_error(e)),
                };
                let mut range = table.range::<&str>((lower, upper)).map_err(|error| {
                    RedbPersistence::storage_source_error(
                        self.logical_table.as_deref().unwrap_or(&self.table),
                        error,
                    )
                })?;
                let next = if self.reverse {
                    range.next_back()
                } else {
                    range.next()
                };
                next.transpose()
                    .map_err(|error| RedbPersistence::storage_source_error(self.logical_table.as_deref().unwrap_or(&self.table), error))?
                    .map(|(key, value)| {
                        #[cfg(any(test, feature = "test-seams"))]
                        if self.table.starts_with("graph_") || matches!(self.table.as_ref(), "change_log" | "writer_change_log") {
                            crate::read_work_probe::graph_storage_key(&self.table, "iteration_step", key.value().as_bytes());
                        }
                        finish(key.value().as_bytes(), value.value())
                    })
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
        self.next_row_before_handoff(before, Ok)
    }
    pub(super) fn next_row_before_handoff<H>(
        &mut self,
        before: &mut dyn FnMut() -> Result<()>,
        handoff: impl FnOnce(WriterWindow<VersionedRow>) -> Result<H>,
    ) -> Result<Option<H>> {
        #[cfg(feature = "test-seams")]
        self.root
            .note_read_source(crate::writer_query_test_seams::PhysicalFamily::Row);
        #[cfg(feature = "test-seams")]
        let _target_reads = self.logical_table.as_deref().map(|table| {
            crate::writer_query_test_seams::physical_operation(
                crate::writer_query_test_seams::PhysicalFamily::Row,
                table,
            )
        });
        self.next_before_handoff(
            before,
            |key, value| {
                #[cfg(feature = "test-seams")]
                {
                    crate::read_work_probe::note_decoded_row();
                    crate::read_work_probe::note_stored_row_bytes(value.len());
                }
                let row = RedbPersistence::decode_versioned_row(value, None)
                    .map_err(|error| RedbPersistence::relational_decode_error("relational row", error))?;
                if RedbPersistence::rel_row_key(&row) != key {
                    return Err(Error::StoreCorrupted {
                        path: "relational row".into(),
                        reason: "row cursor encountered a mismatched version key".into(),
                    });
                }
                Ok(row)
            },
            handoff,
        )
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
    _owner: SourceWindowOwner,
    _control: SourceWindowCredit,
}

impl WriterSnapshot {
    pub(crate) fn change_log_cursor(
        self: &Arc<Self>,
        owner: SourceWindowOwner,
        first: Lsn,
        last: Lsn,
    ) -> Result<WriterReadCursor> {
        let mut lower = [0; 16];
        lower[..8].copy_from_slice(&first.0.to_be_bytes());
        let mut upper = [255; 16];
        upper[..8].copy_from_slice(&last.0.to_be_bytes());
        self.cursor(
            owner,
            writer_history::WRITER_CHANGE_LOG_TABLE.name(),
            WriterCursorFamily::Bytes,
            Bound::Included(&lower),
            Bound::Included(&upper),
            false,
        )
    }

    pub(crate) fn config_cursor(
        self: &Arc<Self>,
        owner: SourceWindowOwner,
        prefix: &str,
    ) -> Result<WriterReadCursor> {
        let end = owner.prefix_end(prefix.as_bytes(), "cursor_keys")?;
        self.cursor(
            owner,
            CONFIG_TABLE.name(),
            WriterCursorFamily::Text,
            Bound::Included(prefix.as_bytes()),
            end.as_ref()
                .map_or(Bound::Unbounded, |end| Bound::Excluded(end.key.as_ref())),
            false,
        )
    }

    pub(crate) fn commit_cursor(
        self: &Arc<Self>,
        owner: SourceWindowOwner,
        first: Lsn,
        last: Lsn,
        reverse: bool,
    ) -> Result<WriterCommitCursor> {
        let control = owner.reserve(std::mem::size_of::<WriterCommitCursor>(), "commit_cursor")?;
        Ok(WriterCommitCursor {
            root: Arc::clone(self),
            lower: first.0,
            upper: last.0,
            after: None,
            reverse,
            _owner: owner,
            _control: control,
        })
    }

    pub(crate) fn commit_bounds(&self) -> Result<(Option<(Lsn, TxId)>, Option<(Lsn, TxId)>, u64)> {
        let read = self.owner_read()?;
        let table = read
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

    pub(crate) fn sync_source(
        &self,
        owner: SourceWindowOwner,
        table: &str,
        row_id: RowId,
    ) -> Result<Option<(Lsn, u8)>> {
        let read = self.owner_read()?;
        let erasure = self.erasure.read();
        erasure.check_scope(Some(table), "sync provenance")?;
        let key = owner.endpoint(&[table.as_bytes(), &row_id.0.to_be_bytes()], "sync_point")?;
        let lsns = match read.open_table(SYNC_ROW_SOURCE_LSN_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(RedbPersistence::storage_error(e)),
        };
        let Some(lsn) = lsns
            .get(key.key.as_ref())
            .map_err(RedbPersistence::storage_error)?
        else {
            return Ok(None);
        };
        let kinds = read
            .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        let kind = kinds
            .get(key.key.as_ref())
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
        self.next_index_before_handoff(current, before, Ok)
    }
    pub(super) fn next_index_before_handoff<H>(
        &mut self,
        current: bool,
        before: &mut dyn FnMut() -> Result<()>,
        handoff: impl FnOnce(WriterWindow<WriterIndexPosting>) -> Result<H>,
    ) -> Result<Option<H>> {
        #[cfg(feature = "test-seams")]
        self.root
            .note_read_source(crate::writer_query_test_seams::PhysicalFamily::Posting);
        #[cfg(feature = "test-seams")]
        let _target_reads = self.logical_table.as_deref().map(|table| {
            crate::writer_query_test_seams::source_posting_operation(table, |target| {
                let expected = DurableRelationalIndexKey::new(target.clone(), RowId(0), TxId(0));
                expected.is_ok_and(|expected| {
                    matches!(&self.lower,
                    Bound::Included(lower) if lower.as_ref() == expected.0.as_slice())
                })
            })
        });
        self.next_before_handoff(
            before,
            |key, value| {
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
            },
            handoff,
        )
    }

    pub(crate) fn next_edge(&mut self) -> Result<Option<WriterWindow<AdjEntry>>> {
        self.next_edge_handoff(Ok)
    }
    pub(super) fn next_edge_handoff<H>(
        &mut self,
        handoff: impl FnOnce(WriterWindow<AdjEntry>) -> Result<H>,
    ) -> Result<Option<H>> {
        let reverse = self.table.as_ref() == GRAPH_REV_TABLE.name();
        let root = Arc::clone(&self.root);
        let owner = self.owner.clone();
        self.next_before_handoff(
            &mut || Ok(()),
            |key, value| {
                #[cfg(any(test, feature = "test-seams"))]
                {
                    crate::read_work_probe::note_graph_root_bound(root.identity);
                    crate::read_work_probe::note_graph(
                        if reverse { "graph_rev" } else { "graph_fwd" },
                        "supplied_primary",
                        key,
                        None,
                        None,
                    );
                }
                #[cfg(any(test, feature = "test-seams"))]
                crate::read_work_probe::note_graph("graph_payload", "payload_decode", key, None, None);
                let edge: AdjEntry = decode_window_exact(value).map_err(|error| match error {
                    Error::Other(reason) => Error::StoreCorrupted {
                        path: "graph adjacency".into(),
                        reason,
                    },
                    other => other,
                })?;
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
                root.verify_graph_edge_at_root(key, value, &edge, reverse, &owner)?;
                Ok(edge)
            },
            handoff,
        )
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
        let root = Arc::clone(&self.root);
        let owner = self.owner.clone();
        self.next(|position, value| {
            #[cfg(feature = "test-seams")]
            crate::database::resident_history_test_seams::note(|work| {
                work.log_keys_examined += 1;
            });
            if position.len() != 16 {
                return Err(Error::StoreCorrupted {
                    path: "change_log".into(),
                    reason: "history cursor physical position width is invalid".into(),
                });
            }
            crate::read_image_memory::reserve(41)?;
            let key = format!(
                "{:020}:{:06}",
                u64::from_be_bytes(position[..8].try_into().unwrap()),
                u64::from_be_bytes(position[8..].try_into().unwrap())
            );
            root.verify_stored_graph_history_at_root(&key, value, &owner)?;
            RedbPersistence::decode_stored_change_log(value).map(|record| record.entry)
        })
    }
}

impl WriterCommitCursor {
    pub(crate) fn next(&mut self) -> Result<Option<(Lsn, TxId)>> {
        let read = self.root.owner_read()?;
        let table = read
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
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::note_graph("commit_index", "range", &[], None, None);
        let mut range = table
            .range((lower, upper))
            .map_err(RedbPersistence::storage_error)?.inspect(|_entry| { #[cfg(any(test, feature = "test-seams"))] if let Ok((key, _)) = _entry { crate::read_work_probe::note_graph("commit_index", "iteration_step", &key.value().to_be_bytes(), Some(contextdb_core::Lsn(key.value())), Some("writer_commit_cursor")); } });
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
        owner: SourceWindowOwner,
        table: &str,
        first: Lsn,
        last: Lsn,
    ) -> Result<WriterReadCursor> {
        let lower = owner.endpoint(
            &[
                table.as_bytes(),
                &[0],
                &first.0.to_be_bytes(),
                &0_u64.to_be_bytes(),
            ],
            "history_seek_keys",
        )?;
        let upper = owner.endpoint(
            &[
                table.as_bytes(),
                &[0],
                &last.0.to_be_bytes(),
                &u64::MAX.to_be_bytes(),
            ],
            "history_seek_keys",
        )?;
        self.cursor(
            owner,
            writer_history::CHANGE_LOG_BY_TABLE.name(),
            WriterCursorFamily::Bytes,
            Bound::Included(&lower.key),
            Bound::Included(&upper.key),
            false,
        )?
        .with_table_scope(table)
    }

    pub(crate) fn change_log_occurrences(&self, lsn: Lsn) -> Result<u64> {
        let read = self.owner_read()?;
        let counts = match read.open_table(writer_history::CHANGE_LOG_LSN_COUNT) {
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
        self: &Arc<Self>,
        owner: SourceWindowOwner,
        key: &str,
    ) -> Result<Option<WriterWindow<T>>> {
        let read = self.owner_read()?;
        let erasure = self.erasure.read();
        erasure.check_scope(None, CONFIG_TABLE.name())?;
        let table = match read.open_table(CONFIG_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(RedbPersistence::storage_error(e)),
        };
        let Some(value) = table.get(key).map_err(RedbPersistence::storage_error)? else {
            return Ok(None);
        };
        let memory = owner.image(self, "sync_point");
        let value = memory.scope(|| decode_window_exact(value.value()))?;
        Ok(Some(WriterWindow {
            value,
            position: Box::default(),
            _root: Arc::clone(self),
            _memory: memory,
        }))
    }
}

impl WriterReadCursor {
    pub(crate) fn next_table_change(&mut self) -> Result<Option<WriterWindow<ChangeLogEntry>>> {
        let root = Arc::clone(&self.root);
        let owner = self.owner.clone();
        self.next(|_, position| {
            if position.len() != 16 {
                return Err(Error::Other(
                    "table log selector has an invalid position width".into(),
                ));
            }
            let read = root.owner_read()?;
            let log = read
                .open_table(writer_history::WRITER_CHANGE_LOG_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let record = log
                .get(observed_graph_key!("history", "get", position))
                .map_err(RedbPersistence::storage_error)?
                .ok_or_else(|| {
                    Error::Other("table log selector names a missing occurrence".into())
                })?;
            crate::read_image_memory::reserve(41)?;
            let key = format!(
                "{:020}:{:06}",
                u64::from_be_bytes(position[..8].try_into().unwrap()),
                u64::from_be_bytes(position[8..].try_into().unwrap())
            );
            root.verify_stored_graph_history_at_root(&key, record.value(), &owner)?;
            RedbPersistence::decode_stored_change_log(record.value()).map(|record| record.entry)
        })
    }
}

impl RedbPersistence {
    pub(crate) fn reserve_redb_allocator_state(
        &self,
    ) -> Result<crate::memory_accounting::RedbAllocatorStateCharge> {
        self.prepare_proof_owner()?;
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
        owner: SourceWindowOwner,
        table: &str,
        index: &str,
        current: bool,
        tuple: &IndexKey,
    ) -> Result<WriterReadCursor> {
        let memory = owner.image(self, "index_seek_keys");
        // Borrow value payloads while sizing and encoding; no tuple clone is
        // made before its owner admits the complete endpoint capacities.
        let lower = admitted_relational_index_endpoint(tuple, RowId(0), TxId(0), &memory)?;
        let upper = admitted_relational_index_endpoint(tuple, RowId(u64::MAX), TxId(u64::MAX), &memory)?;
        self.index_cursor(
            owner,
            table,
            index,
            current,
            Bound::Included(&lower),
            Bound::Included(&upper),
            false,
        )
    }
}

impl WriterSnapshot {
    /// Build an admitted physical endpoint using the persistence codec. Query
    /// adapters need not duplicate its encoding or its row/version suffixes.
    pub(crate) fn index_position_key(
        self: &Arc<Self>, owner: SourceWindowOwner, tuple: &IndexKey,
        row_id: RowId, created_tx: TxId,
    ) -> Result<WriterWindow<Vec<u8>>> {
        self.index_position_key_with_checkpoint(owner, tuple, row_id, created_tx, &mut || Ok(()))
    }

    pub(crate) fn row_index_position_key_with_checkpoint(
        self: &Arc<Self>, owner: SourceWindowOwner, declaration: &IndexDecl,
        row: &VersionedRow, created_tx: TxId, checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<WriterWindow<Vec<u8>>> {
        let memory = owner.image(self, "index_seek_keys");
        let key = admitted_relational_row_index_endpoint_with_checkpoint(declaration, row, created_tx, &memory, checkpoint)?;
        Ok(WriterWindow { value: key, position: Box::default(), _root: Arc::clone(self), _memory: memory })
    }

    pub(crate) fn index_position_key_with_checkpoint(
        self: &Arc<Self>,
        owner: SourceWindowOwner,
        tuple: &[DirectedValue],
        row_id: RowId,
        created_tx: TxId,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<WriterWindow<Vec<u8>>> {
        let memory = owner.image(self, "index_seek_keys");
        let key = admitted_relational_index_endpoint_with_checkpoint(tuple, row_id, created_tx, &memory, checkpoint)?;
        Ok(WriterWindow {
            value: key,
            position: Box::default(),
            _root: Arc::clone(self),
            _memory: memory,
        })
    }
}

impl RedbPersistence {
    pub(super) fn writer_root_provider(&self) -> redb::ExternalSnapshotProvider {
        let registry = Arc::downgrade(&self.writer_snapshots);
        Arc::new(move || {
            let Some(registry) = registry.upgrade() else {
                return Ok(Vec::new());
            };
            let roots = {
                let mut roots = registry.lock();
                roots.retain(|root| root.strong_count() != 0);
                roots.clone()
            };
            Ok(roots
                .into_iter()
                .filter_map(|root| root.upgrade().map(|root| root.storage_identity()))
                .collect())
        })
    }
    pub(super) fn live_writer_roots(&self) -> Vec<Arc<WriterSnapshot>> {
        let roots = self.writer_snapshots.lock().clone();
        roots
            .into_iter()
            .filter_map(|root| root.upgrade())
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)] // Keep test-only source controls beside their tests.
mod tests {
    use super::*;

    fn store(path: &Path) -> (RedbPersistence, crate::memory_accounting::AccountantHandle) {
        let store = RedbPersistence::create(path).unwrap();
        let accountant = Arc::new(MemoryAccountant::no_limit());
        store.attach_memory_accountant(accountant.clone());
        (store, accountant)
    }

    // Exercise the manager's real publication door with the durable source.
    struct RootApplicator {
        store: Arc<RedbPersistence>,
        prepared: parking_lot::Mutex<Option<Arc<WriterSnapshot>>>,
    }
    impl contextdb_tx::WriteSetApplicator for RootApplicator {
        fn apply(
            &self,
            ws: &WriteSet,
            _executing_read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        ) -> Result<()> {
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
        let reader = Arc::new(MemoryAccountant::no_limit());
        let directory = tempfile::tempdir().unwrap();
        let (store, _accountant) = store(&directory.path().join("root.db"));
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
                reader.clone(),
                |_| Ok(()),
                |_, _| {
                    // Durability and preparation completed; logical publication has not.
                    {
                        let observed_identity = capture().storage_identity();
                        assert!(observed_identity.transaction_id >= old_identity.transaction_id);
                        assert_eq!(observed_identity.user_root, old_identity.user_root);
                    }
                    assert!(
                        old.row_cursor(SourceWindowOwner::Read(reader.clone()), "items", RowId(7))
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
                            {
                                let observed_identity = captured.storage_identity();
                                assert!(observed_identity.transaction_id >= old_identity.transaction_id);
                                assert_eq!(observed_identity.user_root, old_identity.user_root);
                            }
                            assert!(
                                captured
                                    .row_cursor(
                                        SourceWindowOwner::Read(reader.clone()),
                                        "items",
                                        RowId(7)
                                    )?
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
        let baseline = reader.usage().used;
        let mut cursor = current
            .row_cursor(SourceWindowOwner::Read(reader.clone()), "items", RowId(7))
            .unwrap();
        let cursor_only = reader.usage().used;
        let window = cursor.next_row().unwrap().unwrap();
        assert_eq!(window.value, row);
        assert!(reader.usage().used > cursor_only);
        drop(window);
        assert!(cursor.next_row().unwrap().is_none());
        assert_eq!(reader.usage().used, cursor_only);
        drop(cursor);
        assert_eq!(reader.usage().used, baseline);
        assert!(
            old.row_cursor(SourceWindowOwner::Read(reader.clone()), "items", RowId(7))
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
        let reader = Arc::new(MemoryAccountant::no_limit());
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
        let fixture_open_accountant_2311 = Arc::new(MemoryAccountant::no_limit());
        let _fixture_decode_scope_2311 = crate::read_image_memory::ImageMemory::enter_writer_operation(std::sync::Arc::clone(&fixture_open_accountant_2311)).unwrap();
        let store = RedbPersistence::open(&path).unwrap();
        store.attach_memory_accountant(std::sync::Arc::clone(&fixture_open_accountant_2311));
        let bounds = store.writer_open_bounds(["items"]).unwrap();
        let root = store
            .initialize_writer_snapshot(SnapshotId(bounds.max_tx.0), bounds.max_lsn)
            .unwrap();
        let mut current = root
            .index_cursor(
                SourceWindowOwner::Read(reader.clone()),
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
                SourceWindowOwner::Read(reader.clone()),
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
                SourceWindowOwner::Read(reader.clone()),
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
        let reader = Arc::new(MemoryAccountant::no_limit());
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
            let mut fwd = root
                .graph_cursor(SourceWindowOwner::Read(reader.clone()), node(1), false)
                .unwrap();
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
            let mut rev = root
                .graph_cursor(SourceWindowOwner::Read(reader.clone()), node(200), true)
                .unwrap();
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
        let reader = Arc::new(MemoryAccountant::no_limit());
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
        let mut selected = old
            .table_change_cursor(SourceWindowOwner::Read(reader.clone()), "a", Lsn(9), Lsn(9))
            .unwrap();
        for entry in &entries {
            assert_eq!(&selected.next_table_change().unwrap().unwrap().value, entry);
        }
        assert!(selected.next_table_change().unwrap().is_none());
        let mut cursor = old
            .change_log_cursor(SourceWindowOwner::Read(reader.clone()), Lsn(9), Lsn(9))
            .unwrap();
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
        let mut cursor = new
            .change_log_cursor(SourceWindowOwner::Read(reader.clone()), Lsn(9), Lsn(9))
            .unwrap();
        assert_eq!(cursor.next_change().unwrap().unwrap().value, entries[0]);
        assert!(cursor.next_change().unwrap().is_none());
        assert!(
            old.change_log_cursor(SourceWindowOwner::Read(reader.clone()), Lsn(9), Lsn(9))
                .unwrap()
                .next_change()
                .unwrap()
                .is_some()
        );
    }
}


impl WriterSnapshot {
    /// Reconcile an independently rooted retained-version range before any
    /// selected value or successful absence escapes its immutable capture.
    pub(crate) fn visit_historical_rows(
        self: &Arc<Self>,
        table: &str,
        row_id: Option<RowId>,
        snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before: &mut dyn FnMut() -> Result<()>,
        visit: &mut dyn FnMut(WriterWindow<VersionedRow>) -> Result<()>,
    ) -> Result<()> {
        use super::content_identity::version_manifest as manifest;
        self.check_table_scope(table)?;
        let owner = SourceWindowOwner::Read(read_memory.clone());
        let image = owner.image(self, "historical_row_range");
        let _controls = owner.reserve(
            std::mem::size_of::<content_identity::ContentIdentityReadVerifier>()
                .checked_add(std::mem::size_of::<Option<WriterWindow<VersionedRow>>>())
                .and_then(|n| n.checked_add(std::mem::size_of::<ReadHistoryProofOwner>()))
                .and_then(|n| n.checked_add(2 * table.len() + 144))
                .ok_or_else(|| Error::Other("historical row controls overflow".into()))?,
            "historical_row_controls",
        )?;
        graph_read::with_pages(read_memory.clone(), || image.scope(|| {
            before()?;
            let read = self.owner_read()?;
            let bounds = RedbPersistence::read_authenticated_bounds_in_read(&read)?;
            let gate = content_identity::history_open_control::acquire(&read, bounds.commit_count)?;
            before()?;
            gate.check_directory(&read)?;
            let verifier = content_identity::ContentIdentityReadVerifier::open(
                &read, Path::new(table), None,
            )?;
            let meta = self.borrow_captured_table_meta(table)
                .ok_or_else(|| Error::TableNotFound(table.into()))?;
            let prefix_credit = owner.reserve(table.len().checked_add(72)
                .ok_or_else(|| Error::Other("historical row bound overflow".into()))?, "historical_row_bound")?;
            let mut prefix = manifest::manifest_prefix(table)?;
            if let Some(row) = row_id { prefix.extend_from_slice(&row.0.to_be_bytes()); }
            let physical_name = owner.endpoint(&[b"rel_", table.as_bytes()], "historical_row_table")?;
            let name = std::str::from_utf8(&physical_name.key).expect("UTF-8 table name");
            let relation = read.open_table(TableDefinition::<&[u8], &[u8]>::new(name))
                .map_err(|error| match error {
                    redb::TableError::TableDoesNotExist(_) => verifier.corrupt("required relational table is absent"),
                    other => RedbPersistence::relational_table_error(table, other),
                })?;
            let (low, high) = row_id.map(RedbPersistence::rel_row_key_range)
                .unwrap_or(([0; 8], [255; 8]));
            let mut physical = relation.range::<&[u8]>((
                if row_id.is_some() { Bound::Included(low.as_slice()) } else { Bound::Unbounded },
                if row_id.is_some() { Bound::Excluded(high.as_slice()) } else { Bound::Unbounded },
            )).map_err(|error| RedbPersistence::relational_storage_error(table, error))?;
            let proof_owner = ReadHistoryProofOwner::new(read_memory.clone())?;
            let proof = content_identity::ProofMemory::growable(0, proof_owner.clone());
            let touch = std::cell::RefCell::new(&mut *before);
            let typed_body_error = |error| match error {
                Error::ReadCancelled | Error::ReadFailure(_)
                | Error::MemoryBudgetExceeded { .. } | Error::StoreCorrupted { .. } => error,
                other => verifier.corrupt(other),
            };
            let mut previous = None::<[u8; 24]>;
            let mut group = None::<RowId>;
            let mut selected = None::<WriterWindow<VersionedRow>>;
            let complete = verifier.visit_history_from::<Error>(
                content_identity::HistoryKind::RelationalVersion,
                Bound::Included(prefix.clone()), false, &proof,
                &mut || (touch.borrow_mut())(),
                &mut |coordinate, marker| {
                    let witness = manifest::parse_manifest(coordinate).map_err(&typed_body_error)?;
                    if !coordinate.starts_with(&prefix) { return Ok(false); }
                    if marker != manifest::manifest_marker_hash()
                        || previous.is_some_and(|prior| prior >= witness.identity) {
                        return Err(verifier.corrupt("duplicate, reordered or invalid version member"));
                    }
                    let id = RowId(u64::from_be_bytes(witness.identity[..8].try_into().expect("row identity")));
                    if group.is_some_and(|prior| prior != id)
                        && let Some(row) = selected.take().filter(|row| row.value.visible_at(snapshot)) {
                        (touch.borrow_mut())()?;
                        visit(row)?;
                    }
                    group = Some(id);
                    previous = Some(witness.identity);
                    (touch.borrow_mut())()?;
                    let Some((key, body)) = physical.next().transpose().map_err(|error| RedbPersistence::relational_storage_error(table, error))? else {
                        return Err(verifier.corrupt("rooted relational version is physically missing"));
                    };
                    if key.value() != witness.identity {
                        return Err(verifier.corrupt("physical relational gap or extra member"));
                    }
                    let mut admission = content_identity::ProofAdmission::for_reader(read_memory.clone());
                    let actual = manifest::manifest_for_body_with_checkpoint(table, key.value(), body.value(),
                        &mut admission, &mut || (touch.borrow_mut())());
                    if let Some(error) = admission.take_reader_refusal() { return Err(error); }
                    if actual.map_err(&typed_body_error)?.as_slice() != coordinate {
                        return Err(verifier.corrupt("persisted row body or closure does not authenticate"));
                    }
                    let created = TxId(u64::from_be_bytes(witness.identity[8..16].try_into().expect("creation identity")));
                    let lsn = Lsn(u64::from_be_bytes(witness.identity[16..].try_into().expect("log identity")));
                    validate_read_version_bounds(&read, table, lsn, created, witness.deleted_tx,
                        TxId(self.snapshot.0), self.lsn)
                        .map_err(|error| match error {
                            LoadReadImageError::Memory(error) => typed_body_error(error),
                            other => verifier.corrupt(other),
                        })?;
                    if created.0 <= snapshot.0 {
                        let memory = owner.image(self, "historical_selected_row");
                        let row = memory.scope(|| RedbPersistence::decode_relational_source_row(table, meta, body.value()))
                            .map_err(&typed_body_error)?;
                        memory.reserve(witness.identity.len())?;
                        selected = Some(WriterWindow {
                            value: row, position: Box::from(witness.identity.as_slice()),
                            _root: self.clone(), _memory: memory,
                        });
                    }
                    Ok(true)
                },
            )?;
            if let Some(error) = proof_owner.refusal.lock().take() { return Err(error); }
            if !complete { return Err(verifier.corrupt("historical version proof did not complete")); }
            (touch.borrow_mut())()?;
            if physical.next().transpose().map_err(|error| RedbPersistence::relational_storage_error(table, error))?.is_some() {
                return Err(verifier.corrupt("unwitnessed physical relational suffix"));
            }
            if let Some(row) = selected.filter(|row| row.value.visible_at(snapshot)) {
                (touch.borrow_mut())()?;
                visit(row)?;
            }
            drop(prefix_credit);
            Ok(())
        }))
    }
}


impl WriterReadCursor {
    /// The manifest chooses the next physical identity independently of the
    /// surviving relation. Group proof precedes decoded value or empty range.
    pub(super) fn next_historical_row_before_handoff<H>(
        &mut self,
        snapshot: SnapshotId,
        before: &mut dyn FnMut() -> Result<()>,
        handoff: impl FnOnce(WriterWindow<VersionedRow>) -> Result<H>,
    ) -> Result<Option<H>> {
        use super::content_identity::version_manifest as manifest;
        let SourceWindowOwner::Read(memory) = &self.owner else {
            return Err(Error::Other("historical row cursor requires its reader child".into()));
        };
        let memory = memory.clone();
        let table = self.logical_table.as_deref().ok_or_else(|| Error::Other("historical row cursor has no table".into()))?;
        let controls = self.owner.reserve(
            std::mem::size_of::<content_identity::ContentIdentityReadVerifier>()
                .checked_add(2 * table.len() + 144)
                .ok_or_else(|| Error::Other("historical cursor controls overflow".into()))?,
            "historical_cursor_controls",
        )?;
        let image = self.owner.image(&self.root, "historical_cursor_proof");
        let expected = graph_read::with_pages(memory.clone(), || image.scope(|| {
            before()?;
            let read = self.root.owner_read()?;
            let bounds = RedbPersistence::read_authenticated_bounds_in_read(&read)?;
            let gate = content_identity::history_open_control::acquire(&read, bounds.commit_count)?;
            before()?;
            gate.check_directory(&read)?;
            let verifier = content_identity::ContentIdentityReadVerifier::open(&read,
                Path::new(table), None)?;
            let prefix = manifest::manifest_prefix(table)?;
            let (lower, upper) = self.bounds();
            let edge = if self.reverse { upper } else { lower };
            let mut start = Vec::with_capacity(prefix.len() + 65);
            start.extend_from_slice(&prefix);
            let inclusive = match edge {
                Bound::Included(key) => {
                    start.extend_from_slice(key);
                    if self.reverse { start.extend_from_slice(&[255; 41]); }
                    true
                }
                Bound::Excluded(key) => {
                    start.extend_from_slice(key);
                    if !self.reverse { start.extend_from_slice(&[255; 41]); }
                    false
                }
                Bound::Unbounded => {
                    if self.reverse { start.extend_from_slice(&[255; 65]); }
                    true
                }
            };
            let owner = ReadHistoryProofOwner::new(memory.clone())?;
            let proof = content_identity::ProofMemory::growable(0, owner.clone());
            let mut expected = None;
            let complete = verifier.visit_history_from::<Error>(content_identity::HistoryKind::RelationalVersion,
                if inclusive { Bound::Included(start) } else { Bound::Excluded(start) }, self.reverse,
                &proof, before, &mut |coordinate, marker| {
                    let witness = manifest::parse_manifest(coordinate)?;
                    if marker != manifest::manifest_marker_hash() {
                        return Err(verifier.corrupt("historical cursor has an invalid manifest marker"));
                    }
                    if coordinate.starts_with(&prefix) {
                        let inside_lower = match lower {
                            Bound::Included(key) => witness.identity.as_slice() >= key,
                            Bound::Excluded(key) => witness.identity.as_slice() > key,
                            Bound::Unbounded => true,
                        };
                        let inside_upper = match upper {
                            Bound::Included(key) => witness.identity.as_slice() <= key,
                            Bound::Excluded(key) => witness.identity.as_slice() < key,
                            Bound::Unbounded => true,
                        };
                        if inside_lower && inside_upper { expected = Some(witness.identity); }
                    }
                    Ok(false)
                })?;
            if let Some(error) = owner.refusal.lock().take() { return Err(error); }
            if !complete { return Err(verifier.corrupt("historical cursor proof did not complete")); }
            Ok(expected)
        }))?;
        if let Some(identity) = expected {
            let id = RowId(u64::from_be_bytes(identity[..8].try_into().expect("row identity")));
            if self.historical_proved_group != Some(id) {
                self.root.visit_historical_rows(table, Some(id), snapshot, memory,
                    before, &mut |_| Ok(()))?;
                self.historical_proved_group = Some(id);
            }
        }
        before()?;
        let row = self.next_row_before(before)?;
        let actual = row.as_ref().map(|row| RedbPersistence::rel_row_key(&row.value));
        if actual.as_deref() != expected.as_ref().map(|key| key.as_slice()) {
            return Err(Error::StoreCorrupted { path: "historical relational cursor".into(),
                reason: "physical version gap or extra member differs from independent manifest".into() });
        }
        drop(controls);
        row.map(handoff).transpose()
    }
}

#[cfg(all(test, feature = "test-seams"))]
pub(crate) fn r1_schema_control_rows_for_test() {
    use crate::memory_accounting::{WriterBudget, writer_adapter_type_layout};
    use crate::persistence::content_identity::proof_allocation_oracle_tests::{last_request_for_test, measured, };
    let owner = Arc::new(MemoryAccountant::no_limit());
    let root = WriterBudget::from_accountant(owner.clone()).unwrap();
    let baseline = owner.usage().used;
    let controls = owner.writer_fixed_ownership().2;
    let meta: TableMeta = serde_json::from_str(r#"{"columns":[],"immutable":false}"#).unwrap();
    let memory = OwnedMemoryReservation::from_admitted(owner.clone(), 0)
        .with_enclosing_arc_control::<WriterTableMeta>(false)
        .unwrap();
    last_request_for_test(true);
    let (table, _, _) = measured(|| {
        Arc::new(WriterTableMeta {
            meta,
            _memory: memory ,
        })
    });
    let request = last_request_for_test(true);
    let table_layout = writer_adapter_type_layout::<WriterTableMeta>();
    assert_eq!(request, (table_layout.size(), table_layout.align(), 1));
    assert_eq!(owner.writer_fixed_ownership().2, controls + table_layout.size());
    let mut tables = HashMap::new();
    tables.insert("r1".to_owned(), table.clone());
    let memory = OwnedMemoryReservation::from_admitted(owner.clone(), 0)
        .with_enclosing_arc_control::<WriterSchema>(false).unwrap();
    last_request_for_test(true);
    let (schema, _, _) = measured(|| Arc::new(WriterSchema { tables, _memory: memory }));
    let request = last_request_for_test(true);
    let schema_layout = writer_adapter_type_layout::<WriterSchema>();
    assert_eq!(request, (schema_layout.size(), schema_layout.align(), 1));
    assert_eq!(owner.usage().used, baseline + table_layout.size() + schema_layout.size());
    let cloned = schema.clone();
    drop(schema);
    assert_eq!(owner.usage().used, baseline + table_layout.size() + schema_layout.size());
    drop(cloned);
    assert_eq!(owner.usage().used, baseline + table_layout.size());
    drop(root);
    assert_ne!(owner.writer_registration_observation_for_test().2, 0, "escaping table metadata holds the real source use");
    drop(table);
    assert_eq!(owner.usage().used, 0);
    println!("CDB127_R1_ROW schema_controls table_size={} table_align={} schema_size={} schema_align={} one_request_each=true clone_single_debit=true last_child_holds_epoch=true original_funder_final_zero=true", table_layout.size(), table_layout.align(), schema_layout.size(), schema_layout.align());
    println!("CDB127_R1_LAYOUT WriterSnapshot size={} align={} read={} control={} schema={} registry={}",
        std::mem::size_of::<WriterSnapshot>(), std::mem::align_of::<WriterSnapshot>(),
        std::mem::offset_of!(WriterSnapshot, read), std::mem::offset_of!(WriterSnapshot, _control),
        std::mem::offset_of!(WriterSnapshot, schema), std::mem::offset_of!(WriterSnapshot, registry));
    println!("CDB127_R1_LAYOUT WriterErasure size={} align={} memory={}",
        std::mem::size_of::<WriterErasure>(), std::mem::align_of::<WriterErasure>(), std::mem::offset_of!(WriterErasure, _memory));
}

#[cfg(test)]
use crate::memory_accounting::MemoryAccountant;
