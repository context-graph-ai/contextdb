//! Durable vector membership sources for a writable store.
//!
//! Every operation captures the writer's current published root once and
//! reads only that root, so one count, probe or cursor never mixes two
//! frontiers. Row commits publish their memberships and the maintained
//! summaries in the same transaction as the row, so a later operation sees
//! them without the vector store holding any decoded directory. Each record
//! read is authenticated (checksum, key and route) before it is used.
use super::*;
use crate::memory_accounting::{MemoryAccountant, StoreAccountantHandle};
use contextdb_vector::store::{
    DormantRawVectorLoader, DormantVectorLoadRequest, RawVectorInspectionSnapshot,
    RawVectorMembershipCursor, RawVectorMembershipSource, RawVectorMembershipSummary,
    RawVectorReverseMembershipCursor, RawVectorReverseMembershipSource,
};
use std::ops::Bound;

const HINT: &str = "Raise MEMORY_LIMIT to read this vector membership window.";

#[cfg(feature = "test-seams")]
thread_local! {
    static PENDING_SUMMARY_FAILURE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

#[cfg(feature = "test-seams")]
pub(crate) fn with_pending_summary_failure_for_test<R>(
    persistence: Option<usize>,
    action: impl FnOnce() -> R,
) -> R {
    struct Restore(Option<usize>);
    impl Drop for Restore {
        fn drop(&mut self) {
            PENDING_SUMMARY_FAILURE.with(|slot| slot.set(self.0));
        }
    }
    let _restore = Restore(PENDING_SUMMARY_FAILURE.with(|slot| slot.replace(persistence)));
    action()
}

pub(super) fn damage(reason: &str) -> Error {
    Error::StoreCorrupted {
        path: "vector_partition_memberships".into(),
        reason: reason.into(),
    }
}

#[derive(Clone)]
pub(crate) enum MembershipOwner {
    Writable(Arc<RedbPersistence>),
    Retained(Arc<RetainedReadSource>),
}
impl MembershipOwner {
    pub(crate) fn historical_operation(&self) -> bool {
        operation_for(self).is_some_and(|context| context.historical)
    }
    fn capture(&self) -> Result<MembershipRoot> {
        if let Some(context) = operation_for(self) {
            return Ok(context.root.clone());
        }
        match self {
            Self::Writable(persistence) => MembershipRoot::capture(persistence),
            Self::Retained(source) => {
                let read = source.begin_read()?;
                if read.storage_snapshot_identity() != source.storage_identity() {
                    return Err(damage("membership read escaped its retained physical root"));
                }
                vector_membership_pages::validate_in_read(&read)?;
                Ok(MembershipRoot {
                    read: Arc::new(read),
                    writer: None,
                    retained: Some(source.clone()),
                })
            }
        }
    }
    fn latest(&self) -> Result<MembershipRoot> {
        if let Some(context) = operation_for(self) {
            return Ok(context.root.clone());
        }
        match self {
            Self::Writable(persistence) => Ok(MembershipRoot {
                read: Arc::new(
                    persistence
                        .with_db(|db| db.begin_read().map_err(RedbPersistence::storage_error))?,
                ),
                writer: None,
                retained: None,
            }),
            Self::Retained(_) => self.capture(),
        }
    }
    #[cfg(feature = "test-seams")]
    fn persistence_identity(&self) -> Option<usize> {
        match self {
            Self::Writable(persistence) => Some(Arc::as_ptr(persistence) as usize),
            Self::Retained(_) => None,
        }
    }
}

/// One operation's root: the writer's published root once the writer is
/// installed, otherwise (only while the store is still opening) a plain read
/// of the committed file.
#[derive(Clone)]
struct MembershipRoot {
    read: Arc<redb::ReadTransaction>,
    writer: Option<Arc<WriterSnapshot>>,
    // Hold the registered committed source throughout each cursor operation.
    retained: Option<Arc<RetainedReadSource>>,
}

impl MembershipRoot {
    fn capture(persistence: &RedbPersistence) -> Result<Self> {
        if let Some(writer) = persistence.scoped_vector_root_capture.get()
            .map(|capture| capture()).transpose()?.flatten() {
            let read = writer.owner_read()?;
            vector_membership_pages::validate_in_read(&read)?;
            return Ok(Self { read, writer: Some(writer), retained: None });
        }
        if let Ok(capture) = persistence.paged_root_capture() {
            let (_read, handle) =
                capture().map_err(vector_membership_pages::typed_metadata_error)?;
            let writer = handle
                .downcast::<WriterSnapshot>()
                .map_err(|_| damage("writer root capture returned another owner"))?;
            let identity = writer.storage_identity();
            let read = persistence.with_db(|db| {
                db.begin_external_read(identity)
                    .map_err(vector_membership_pages::metadata_error)
            })?;
            if read.storage_snapshot_identity() != identity {
                return Err(damage("membership read escaped its captured physical root"));
            }
            vector_membership_pages::validate_in_read(&read)?;
            return Ok(Self {
                read: Arc::new(read),
                writer: Some(writer),
                retained: None,
            });
        }
        let read = persistence.with_db(|db| {
            db.begin_read()
                .map_err(vector_membership_pages::metadata_error)
        })?;
        Ok(Self {
            read: Arc::new(read),
            writer: None,
            retained: None,
        })
    }

    fn check_scope(&self, table: &str) -> Result<()> {
        if let Some(source) = &self.retained
            && self.read.storage_snapshot_identity() != source.storage_identity()
        {
            return Err(damage("membership cursor escaped its retained root"));
        }
        match &self.writer {
            Some(writer) => writer.check_table_scope(table),
            None => Ok(()),
        }
    }

    /// The newest snapshot this root holds, when it is the writer's root.
    fn snapshot(&self) -> Option<SnapshotId> {
        self.writer.as_ref().map(|writer| writer.snapshot)
    }
}

// This slot belongs to the executing operation, never to the store. It
// carries the already selected physical root and the existing Read(child).
thread_local! {
    static OPERATION: std::cell::RefCell<Option<Arc<VectorReadOperation>>> = const { std::cell::RefCell::new(None) };
    static EXPLICIT_INTENT: std::cell::RefCell<Option<HistoricalReadIntent>> = const { std::cell::RefCell::new(None) };
}
// Explicit history is selected at the public request door, independently
// of an internal exact-LSN body locator or the store's later publication.
struct HistoricalReadIntent {
    owner: MembershipOwner,
    snapshot: SnapshotId,
    memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
}
pub(crate) struct HistoricalReadIntentScope {
    prior: Option<HistoricalReadIntent>,
    _same_thread: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl Drop for HistoricalReadIntentScope {
    fn drop(&mut self) {
        EXPLICIT_INTENT.with(|slot| *slot.borrow_mut() = self.prior.take());
    }
}
pub(crate) fn enter_historical_read_intent(
    owner: MembershipOwner,
    snapshot: SnapshotId,
    memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> HistoricalReadIntentScope {
    let prior = EXPLICIT_INTENT.with(|slot| slot.borrow_mut().replace(
        HistoricalReadIntent { owner, snapshot, memory }));
    HistoricalReadIntentScope { prior, _same_thread: std::marker::PhantomData }
}
fn explicit_historical_intent(
    owner: &MembershipOwner,
    snapshot: SnapshotId,
    memory: &Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> bool {
    EXPLICIT_INTENT.with(|slot| slot.borrow().as_ref().is_some_and(|intent|
        same_owner(&intent.owner, owner) && intent.snapshot == snapshot
            && Arc::ptr_eq(&intent.memory, memory)))
}

pub(crate) struct VectorReadOperation {
    owner: MembershipOwner,
    root: MembershipRoot,
    snapshot: SnapshotId,
    historical: bool,
    checkpoint: Option<crate::executor::bounded::VectorReadCheckpoint>,
    memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    _credit: contextdb_core::read_memory::ReadCredit,
}
pub(crate) struct VectorReadScope {
    prior: Option<Arc<VectorReadOperation>>,
    _same_thread: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl Drop for VectorReadScope {
    fn drop(&mut self) {
        OPERATION.with(|slot| *slot.borrow_mut() = self.prior.take());
    }
}
fn same_owner(a: &MembershipOwner, b: &MembershipOwner) -> bool {
    match (a, b) {
        (MembershipOwner::Writable(a), MembershipOwner::Writable(b)) => Arc::ptr_eq(a, b),
        (MembershipOwner::Retained(a), MembershipOwner::Retained(b)) => Arc::ptr_eq(a, b),
        _ => false,
    }
}
fn operation_for(owner: &MembershipOwner) -> Option<Arc<VectorReadOperation>> {
    OPERATION.with(|slot| slot.borrow().as_ref().filter(|op| same_owner(&op.owner, owner)).cloned())
}

pub(crate) fn enter_read_operation(
    owner: MembershipOwner,
    snapshot: SnapshotId,
    memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    copies: &mut contextdb_vector::VectorCopyAdmission,
    route: &mut dyn FnMut(&VectorPartitionRef) -> Result<()>,
    checkpoint: Option<crate::executor::bounded::ReadOpenContext>,
) -> Result<VectorReadScope> {
    if let Some(checkpoint) = &checkpoint {
        if !Arc::ptr_eq(&checkpoint.read_memory(), &memory) {
            return Err(damage("vector preflight checkpoint belongs to another reader child"));
        }
        checkpoint.checkpoint()?;
    }
    let explicit_history = explicit_historical_intent(&owner, snapshot, &memory);
    if let Some(checkpoint) = &checkpoint
        && let Some(context) = checkpoint.vector_read_capture()
    {
        if !same_owner(&context.owner, &owner) || context.snapshot != snapshot
            || !Arc::ptr_eq(&context.memory, &memory)
            || context.checkpoint.as_ref().is_none_or(|captured| !captured.belongs_to(checkpoint))
            || (explicit_history && !context.historical)
        {
            return Err(damage("resumed vector read changed its captured owner, snapshot, intent or reader child"));
        }
        if let Some(writer) = &context.root.writer {
            if context.root.read.storage_snapshot_identity() != writer.storage_identity() {
                return Err(damage("resumed vector read escaped its captured writer root"));
            }
            if let MembershipOwner::Writable(source) = &owner
                && let Some(scoped) = source.scoped_vector_root_capture.get()
                    .map(|capture| capture()).transpose()?.flatten()
                && (scoped.storage_identity() != writer.storage_identity()
                    || scoped.snapshot != writer.snapshot)
            {
                return Err(damage("resumed vector read changed its captured writer schema root"));
            }
        }
        if let Some(source) = &context.root.retained
            && (context.root.read.storage_snapshot_identity() != source.storage_identity()
                || source.registration.snapshot.storage_snapshot != source.storage_identity())
        {
            return Err(damage("resumed vector read escaped its retained reader hold"));
        }
        context.checkpoint.as_ref().expect("checked request checkpoint").checkpoint()?;
        let prior = OPERATION.with(|slot| slot.borrow_mut().replace(context));
        return Ok(VectorReadScope { prior, _same_thread: std::marker::PhantomData });
    }

    if let Some(context) = operation_for(&owner) {
        if context.snapshot != snapshot { return Err(damage("nested vector read changed its requested snapshot")); }
        if Arc::ptr_eq(&context.memory, &memory)
            && (!explicit_history || context.historical)
            && (checkpoint.as_ref().is_none_or(|request|
                context.checkpoint.as_ref().is_some_and(|captured| captured.belongs_to(request)))) {
            if let Some(captured) = &context.checkpoint { captured.checkpoint()?; }
            if let Some(checkpoint) = &checkpoint { checkpoint.retain_vector_read_capture(context.clone())?; }
            return Ok(VectorReadScope { prior: Some(context), _same_thread: std::marker::PhantomData });
        }
    }
    if let Some(checkpoint) = &checkpoint { checkpoint.checkpoint()?; }
    let credit = contextdb_core::read_memory::ReadCredit::try_new(memory.clone(),
        std::mem::size_of::<VectorReadOperation>() + 2 * std::mem::size_of::<usize>()
            + checkpoint.as_ref().map_or(0, |_| std::mem::size_of::<
                std::sync::Mutex<Option<Arc<VectorReadOperation>>>>()),
        "reader", "vector_capture", HINT)?;
    let root = graph_read::with_pages(memory.clone(), || owner.capture())?;
    let frontier = root.writer.as_ref().map_or_else(
        || match &owner { MembershipOwner::Retained(source) => source.logical_frontier,
                         MembershipOwner::Writable(_) => snapshot },
        |writer| writer.snapshot,
    );
    if snapshot > frontier { return Err(damage("vector snapshot exceeds its captured frontier")); }

    let context = Arc::new(VectorReadOperation { owner, root, snapshot,
        historical: snapshot != frontier || explicit_history,
        checkpoint: checkpoint.as_ref().map(|context| context.vector_checkpoint()), memory, _credit: credit });
    let prior = OPERATION.with(|slot| slot.borrow_mut().replace(context.clone()));
    let scope = VectorReadScope { prior, _same_thread: std::marker::PhantomData };
    if context.historical {
        graph_read::with_pages(context.memory.clone(), ||
            validate_universe(&context.root.read, copies, &mut || operation_checkpoint(), route))?;
    }
    operation_checkpoint()?;
    if let Some(checkpoint) = &checkpoint { checkpoint.retain_vector_read_capture(context)?; }
    Ok(scope)
}

/// The operation refers to the bounded kernel's actual executing context. Plain
/// library reads have no cancellation/deadline context to replace.
pub(crate) fn operation_checkpoint() -> Result<()> {
    let context = OPERATION.with(|slot| slot.borrow().clone());
    if let Some(checkpoint) = context.as_ref().and_then(|context| context.checkpoint.as_ref()) {
        checkpoint.checkpoint()?;
    }
    Ok(())
}

pub(crate) fn read_memory_for_operation() -> Option<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>> {
    OPERATION.with(|slot| slot.borrow().as_ref().map(|context| context.memory.clone()))
}

/// Reconcile the independent committed directory with every physical member,
/// both companion indexes and every summary. One decoded window is live; no
/// population directory, body set or route map is constructed.
pub(super) fn validate_universe(
    read: &redb::ReadTransaction,
    copies: &mut contextdb_vector::VectorCopyAdmission,
    before: &mut dyn FnMut() -> Result<()>,
    route: &mut dyn FnMut(&VectorPartitionRef) -> Result<()>,
) -> Result<()> {
    before()?;
    vector_membership_pages::validate_in_read(read)?;
    let mut controls = copies.scratch();
    controls.reserve(std::mem::size_of::<content_identity::SetCommitment>()
        + std::mem::size_of::<blake3::Hasher>() + 3 * std::mem::size_of::<vector_membership_pages::MembershipSummary>()
        + 8 * std::mem::size_of::<redb::ReadOnlyTable<&[u8], &[u8]>>()
        + 3 * std::mem::size_of::<redb::Range<'static, &'static [u8], &'static [u8]>>() + 40)?;
    // Table presence is independent of the fingerprint's absent-table value.
    let members = open_optional_read_table(read, VECTOR_PARTITION_MEMBERSHIP_TABLE)?;
    let source = open_optional_read_table(read, vector_membership_pages::SOURCE)?;
    let reverse = open_optional_read_table(read, vector_membership_pages::REVERSE)?;
    let summaries = open_optional_read_table(read, vector_membership_pages::SUMMARY)?;
    let directory = read.open_table(VECTOR_OPEN_DIRECTORY_TABLE)
        .map_err(|_| damage("captured vector directory table is absent"))?;
    let vectors = open_optional_read_table(read, VECTORS_TABLE)?;
    before()?;
    let aggregate = directory.get(VECTOR_OPEN_DIRECTORY_ROOT_KEY)
        .map_err(vector_membership_pages::metadata_error)?
        .ok_or_else(|| damage("captured vector directory aggregate is absent"))?;
    if aggregate.value().len() != 40 { return Err(damage("captured vector directory aggregate has invalid size")); }
    let metadata = read.open_table(CONTENT_IDENTITY_METADATA_TABLE)
        .map_err(vector_membership_pages::metadata_error)?;
    let commitment = metadata.get(VECTOR_OPEN_DIRECTORY_COMMITMENT_KEY)
        .map_err(vector_membership_pages::metadata_error)?
        .ok_or_else(|| damage("captured vector directory commitment is absent"))?;
    if commitment.value() != blake3::hash(aggregate.value()).as_bytes() {
        return Err(damage("captured vector directory aggregate is not committed"));
    }
    let mut projection = content_identity::SetCommitment::empty(content_identity::CompactIndexSet::VectorDirectory);
    let mut directory_cursor = directory.iter().map_err(vector_membership_pages::metadata_error)?;
    let first = directory_cursor.next().transpose().map_err(vector_membership_pages::metadata_error)?
        .ok_or_else(|| damage("captured vector directory has no aggregate"))?;
    if first.0.value() != VECTOR_OPEN_DIRECTORY_ROOT_KEY { return Err(damage("vector directory aggregate is not its lower boundary")); }
    drop(first);
    let mut count = 0u64;
    let mut digest = [0u8; 32];
    let mut member_cursor = members.as_ref().map(|table| table.iter().map_err(vector_membership_pages::metadata_error)).transpose()?;
    loop {
        before()?;
        let Some(item) = member_cursor.as_mut().and_then(Iterator::next) else { break; };
        let (key, value) = item.map_err(vector_membership_pages::metadata_error)?;
        let mut window = copies.scratch();
        let bytes = key.value().len().checked_add(value.value().len())
            .and_then(|n| n.checked_mul(8)).and_then(|n| n.checked_add(std::mem::size_of::<VectorPartitionMembershipRecord>()))
            .ok_or_else(|| damage("vector membership decode capacity overflow"))?;
        window.reserve(bytes)?;
        let payload = vector_partition_record_payload(VectorPartitionRecordKind::Membership, value.value())
            .map_err(|reason| damage(&reason))?;
        prevalidate_vector_partition_metadata_routes(VectorPartitionRecordKind::Membership, payload)
            .map_err(|reason| damage(&reason))?;
        window.reserve(vector_partition_route_native_capacity(payload).map_err(|reason| damage(&reason))?
            .checked_mul(6).ok_or_else(|| damage("vector route clone capacity overflow"))?)?;
        let member: VectorPartitionMembershipRecord = decode_vector_partition_record_payload_checked(
            VectorPartitionRecordKind::Membership, value.value()).map_err(|failure| failure.into_error(|reason| damage(&reason)))?;
        let partition = VectorPartitionRef::new(member.index.clone(), member.partition_key.clone());
        decode_forward(&partition, key.value(), value.value())?;
        let context = OPERATION.with(|slot| slot.borrow().clone())
            .ok_or_else(|| damage("historical vector universe has no operation capture"))?;
        authenticate_owning_row(&context.owner, read, &member.index, &body_identity(&member), None, before)?;
        route(&partition)?;
        before()?;
        let source_key = vector_membership_pages::source_key(&member);
        let reverse_key = vector_membership_pages::reverse_key(&member);
        let source = source.as_ref().ok_or_else(|| damage("nonempty universe has no source table"))?;
        let reverse = reverse.as_ref().ok_or_else(|| damage("nonempty universe has no reverse table"))?;
        let vectors = vectors.as_ref().ok_or_else(|| damage("nonempty universe has no body table"))?;
        let summaries = summaries.as_ref().ok_or_else(|| damage("nonempty universe has no summary table"))?;
        if source.get_bounded(source_key.as_slice()).map_err(vector_membership_pages::metadata_error)?
            .is_none_or(|copy| copy.value() != value.value())
            || reverse.get_bounded(reverse_key.as_slice()).map_err(vector_membership_pages::metadata_error)?
                .is_none_or(|copy| copy.value() != value.value()) {
            return Err(damage("membership source or reverse companion differs from its captured member"));
        }
        let body_key = RedbPersistence::vector_identity_key(&member.index, member.row_id,
            member.vector_created_tx, member.vector_lsn);
        if vectors.get_bounded(body_key.as_slice()).map_err(vector_membership_pages::metadata_error)?.is_none() {
            return Err(damage("captured membership references a missing body"));
        }
        before()?;
        let mut hash = blake3::Hasher::new(); hash.update(key.value()); hash.update(value.value());
        let hash = hash.finalize();
        projection.insert(key.value(), hash.as_bytes())?;
        let (expected_key, expected_hash) = directory_cursor.next().transpose()
            .map_err(vector_membership_pages::metadata_error)?
            .ok_or_else(|| damage("captured vector directory omits a membership"))?;
        if expected_key.value() != key.value() || expected_hash.value() != hash.as_bytes() {
            return Err(damage("captured directory membership order or body differs"));
        }
        count = count.checked_add(1).ok_or_else(|| damage("vector membership count overflow"))?;
        for (dst, src) in digest.iter_mut().zip(hash.as_bytes()) { *dst ^= src; }
        let prefix = RedbPersistence::vector_partition_prefix(&partition.index, &partition.partition_key);
        if summaries.get_bounded(prefix.as_slice()).map_err(vector_membership_pages::metadata_error)?.is_none() {
            return Err(damage("captured route has no summary"));
        }
    }
    before()?;
    if directory_cursor.next().transpose().map_err(vector_membership_pages::metadata_error)?.is_some()
        || source.as_ref().map(|table| table.len().map_err(vector_membership_pages::metadata_error)).transpose()?.unwrap_or(0) != count
        || reverse.as_ref().map(|table| table.len().map_err(vector_membership_pages::metadata_error)).transpose()?.unwrap_or(0) != count
        || aggregate.value()[..8] != count.to_be_bytes() || aggregate.value()[8..] != digest {
        return Err(damage("captured directory or companion universe has missing or extra members"));
    }
    let body_count = vector_membership_pages::referenced_body_count_in_read(read)?;
    if vectors.as_ref().map(|table| table.len().map_err(vector_membership_pages::metadata_error)).transpose()?.unwrap_or(0) != body_count
        || projection.root_with_vector_body_count(body_count) != content_identity::vector_directory_projection_in_read(read)
            .map_err(|error| {
                if crate::read_image_memory::is_memory_refusal(&error) || matches!(error, Error::ReadCancelled) {
                    return error;
                }
                damage(&format!("captured vector root projection is invalid: {error}"))
            })?
        || !vector_scan_body_root_matches(read)? {
        return Err(damage("captured vector body universe differs from its committed directory"));
    }
    // Summaries are scanned reciprocally, including the independent global
    // summary and lawful empty declared routes. Each source prefix is bounded.
    let mut summary_cursor = summaries.as_ref().map(|table| table.iter().map_err(vector_membership_pages::metadata_error)).transpose()?;
    let mut global_seen = false;
    loop {
        before()?;
        let Some(item) = summary_cursor.as_mut().and_then(Iterator::next) else { break; };
        let (key, value) = item.map_err(vector_membership_pages::metadata_error)?;
        let mut window = copies.scratch();
        window.reserve(key.value().len().checked_mul(8).and_then(|n| n.checked_add(value.value().len()))
            .and_then(|n| n.checked_add(2 * std::mem::size_of::<vector_membership_pages::MembershipSummary>()))
            .ok_or_else(|| damage("vector summary window overflow"))?)?;
        if key.value().is_empty() { global_seen = true; }
        if !key.value().is_empty() {
            let partition = vector_membership_pages::partition_from_prefix_admitted(key.value(), &mut window)?;
            route(&partition)?;
        }
        vector_membership_pages::verify_summary_projection(read, key.value(), value.value(), copies, before)?;
    }
    if count != 0 && !global_seen { return Err(damage("nonempty universe has no independent global summary")); }
    before()?;
    Ok(())
}

pub(crate) fn check_declared_routes(
    root: Option<&Arc<WriterSnapshot>>,
    retained_tables: Option<&HashMap<String, TableMeta>>,
    store: &contextdb_vector::store::VectorStore,
    copies: &mut contextdb_vector::VectorCopyAdmission,
) -> Result<()> {
    operation_checkpoint()?;
    let context = OPERATION.with(|slot| slot.borrow().clone())
        .ok_or_else(|| damage("declared vector routes have no operation capture"))?;
    if !context.historical { return Ok(()); }
    if root.is_some() == retained_tables.is_some() {
        return Err(damage("declared vector routes have no unique captured schema"));
    }
    let read = context.root.read.clone();
    let captured = root.into_iter().flat_map(|root| root.captured_table_metadata());
    let retained = retained_tables.into_iter().flat_map(|tables|
        tables.iter().map(|(table, meta)| (table.as_str(), meta)));
    for (table, meta) in captured.chain(retained) {
        context.root.check_scope(table)?;
        for column in &meta.columns {
            operation_checkpoint()?;
            if !matches!(column.column_type, ColumnType::Vector(_)) { continue; }
            let mut window = copies.scratch();
            window.reserve(table.len().checked_add(column.name.len())
                .and_then(|n| n.checked_mul(8)).and_then(|n| n.checked_add(std::mem::size_of::<VectorPartitionRef>()))
                .ok_or_else(|| damage("declared vector route capacity overflow"))?)?;
            let index = VectorIndexRef::new(table, &column.name);
            if !store.contains_index(&index) { return Err(damage("captured vector declaration is not installed")); }
            if column.partition_key_columns.as_ref().is_none_or(|keys| keys.is_empty()) {
                let partition = VectorPartitionRef::new(index, VectorPartitionKey::unpartitioned());
                let prefix = RedbPersistence::vector_partition_prefix(&partition.index, &partition.partition_key);
                let summaries = read.open_table(vector_membership_pages::SUMMARY)
                    .map_err(|_| damage("declared vector route has no summary table"))?;
                if summaries.get_bounded(prefix.as_slice()).map_err(vector_membership_pages::metadata_error)?.is_none()
                    || store.try_partition_state(&partition.index, &partition.partition_key).is_none() {
                    return Err(damage("captured declared empty vector route is absent"));
                }
            }
        }
    }
    Ok(())
}

pub(super) fn operation_read_for_persistence(source: &RedbPersistence) -> Option<Arc<redb::ReadTransaction>> {
    OPERATION.with(|slot| slot.borrow().as_ref().and_then(|context| match &context.owner {
        MembershipOwner::Writable(owner) if std::ptr::eq(owner.as_ref(), source) => Some(context.root.read.clone()),
        _ => None,
    }))
}
pub(super) fn operation_read_for_retained(source: &RetainedReadSource) -> Option<Arc<redb::ReadTransaction>> {
    OPERATION.with(|slot| slot.borrow().as_ref().and_then(|context| match &context.owner {
        MembershipOwner::Retained(owner) if std::ptr::eq(owner.as_ref(), source) => Some(context.root.read.clone()),
        _ => None,
    }))
}

pub(crate) fn retained_partition_row_at_lsn(
    source: &RetainedReadSource, table: &str, meta: &TableMeta, row: RowId, lsn: Lsn,
    memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Option<VersionedRow>> {
    operation_checkpoint()?;
    let read = match operation_read_for_retained(source) {
        Some(read) => read,
        None => Arc::new(source.begin_read()?),
    };
    if read.storage_snapshot_identity() != source.storage_identity() {
        return Err(damage("partition row lookup escaped its retained capture"));
    }
    let mut last = None;
    source.reconcile_relational_group::<Error>(&read, table, meta, row, source.logical_frontier,
        Some(lsn), memory, &mut || operation_checkpoint(), None,
        &mut |admission: crate::read_image_memory::RowReadAdmission<'_>| admission.retain_into_image(crate::read_image_memory::active_owner()?.as_ref()), &mut last)
}

pub(super) fn authenticate_retained_historical_body(
    source: &RetainedReadSource,
    read: &redb::ReadTransaction,
    entry: Option<&VectorEntry>,
    member: &VectorPartitionMembershipRecord,
    dimension: usize,
    quantization: VectorQuantization,
) -> Result<bool> {
    let context = OPERATION.with(|slot| slot.borrow().as_ref().filter(|context| context.historical).and_then(|context| match &context.owner {
        MembershipOwner::Retained(owner) if std::ptr::eq(owner.as_ref(), source) => Some(context.clone()),
        _ => None,
    }));
    let Some(context) = context else { return Ok(false); };
    operation_checkpoint()?;
    authenticate_body_schema(&context.owner, read, &member.index, dimension, quantization)?;
    let Some(entry) = entry else {
        authenticate_owning_row(&context.owner, read, &member.index, &body_identity(member), None,
            &mut || operation_checkpoint())?;
        return Ok(true);
    };
    let bytes = std::mem::size_of::<content_identity::ContentIdentityReadVerifier>()
        .checked_add(std::mem::size_of::<content_identity::ContentIdentityMutation>())
        .and_then(|n| n.checked_add(entry.vector.len().checked_mul(4)?))
        .and_then(|n| n.checked_add(entry.index.table.len()))
        .and_then(|n| n.checked_add(entry.index.column.len()))
        .and_then(|n| n.checked_add(320))
        .ok_or_else(|| damage("historical vector bulk proof capacity overflow"))?;
    let _credit = contextdb_core::read_memory::ReadCredit::try_new(context.memory.clone(), bytes,
        "reader", "historical_vector_bulk_proof", HINT)?;
    graph_read::with_pages(context.memory.clone(), || {
        let verifier = content_identity::ContentIdentityReadVerifier::open(read, Path::new("historical vector bulk"), None)?;
        verifier.verify_with_read_memory_before(&content_identity::decoded_vector_version_mutation(entry, quantization),
            context.memory.clone(), &mut || operation_checkpoint())?;
        authenticate_owning_row(&context.owner, read, &entry.index, &body_identity(member), Some(entry.deleted_tx),
            &mut || operation_checkpoint())
    })?;
    Ok(true)
}

pub(super) fn historical_memory_for_persistence(
    source: &RedbPersistence,
    read: &redb::ReadTransaction,
) -> Result<Option<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>>> {
    let owner = OPERATION.with(|slot| slot.borrow().as_ref().and_then(|context| match &context.owner {
        MembershipOwner::Writable(owner) if std::ptr::eq(owner.as_ref(), source) => Some(context.owner.clone()),
        _ => None,
    }));
    match owner { Some(owner) => historical_operation_memory(&owner, read), None => Ok(None) }
}
pub(super) fn body_identity_for_record(record: &VectorPartitionMembershipRecord) -> RawVectorDirectoryEntry {
    body_identity(record)
}
pub(super) fn authenticate_row_for_persistence(
    source: &RedbPersistence,
    read: &redb::ReadTransaction,
    index: &VectorIndexRef,
    identity: &RawVectorDirectoryEntry,
    dimension: usize,
    quantization: VectorQuantization,
    body_deleted: Option<Option<TxId>>,
) -> Result<()> {
    let owner = OPERATION.with(|slot| slot.borrow().as_ref().and_then(|context| match &context.owner {
        MembershipOwner::Writable(owner) if std::ptr::eq(owner.as_ref(), source) => Some(context.owner.clone()),
        _ => None,
    }));
    if let Some(owner) = owner {
        authenticate_body_schema(&owner, read, index, dimension, quantization)?;
        authenticate_owning_row(&owner, read, index, identity, body_deleted, &mut || operation_checkpoint())?;
    }
    Ok(())
}

pub(super) fn operation_read(owner: &MembershipOwner) -> Option<Arc<redb::ReadTransaction>> {
    operation_for(owner).map(|context| context.root.read.clone())
}

pub(super) fn historical_operation_memory(
    owner: &MembershipOwner,
    read: &redb::ReadTransaction,
) -> Result<Option<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>>> {
    let Some(context) = operation_for(owner) else { return Ok(None); };
    if read.storage_snapshot_identity() != context.root.read.storage_snapshot_identity() {
        return Err(damage("vector body read escaped its immutable operation capture"));
    }
    Ok(context.historical.then(|| context.memory.clone()))
}

pub(super) fn authenticate_body_schema(
    owner: &MembershipOwner,
    read: &redb::ReadTransaction,
    index: &VectorIndexRef,
    dimension: usize,
    quantization: VectorQuantization,
) -> Result<()> {
    let Some(context) = operation_for(owner).filter(|context| context.historical) else { return Ok(()); };
    if read.storage_snapshot_identity() != context.root.read.storage_snapshot_identity() {
        return Err(damage("vector schema proof escaped its immutable capture"));
    }
    operation_checkpoint()?;
    let check = |meta: &TableMeta| {
        let column = meta.columns.iter().find(|column| column.name == index.column)
            .ok_or_else(|| damage("vector body has no captured column declaration"))?;
        if !matches!(column.column_type, ColumnType::Vector(n) if n == dimension)
            || column.quantization != quantization {
            return Err(damage("vector body loader schema differs from its capture"));
        }
        Ok(())
    };
    if let Some(root) = &context.root.writer {
        check(root.borrow_captured_table_meta(&index.table)
            .ok_or_else(|| damage("vector body has no captured table declaration"))?)
    } else {
        let MembershipOwner::Retained(source) = owner else { return Err(damage("vector schema has no retained owner")); };
        let image = crate::read_image_memory::ImageMemory::charging_reader_for(
            source._source_memory.memory.accountant.clone(), context.memory.clone(),
            "reader", "vector_schema_binding", HINT);
        let _controls = contextdb_core::read_memory::ReadCredit::try_new(context.memory.clone(),
            std::mem::size_of::<crate::read_image_memory::ImageMemory>() + 2 * std::mem::size_of::<usize>(),
            "reader", "vector_schema_controls", HINT)?;
        image.scope(|| check(&RedbPersistence::load_table_meta_in_read_txn(read, &index.table)?
            .ok_or_else(|| damage("vector body has no captured table declaration"))?))
    }
}

/// Bind the placement to its independently authenticated owning row group.
/// CURRENT intentionally performs no manifest seek/hash/proof work here.
pub(super) fn authenticate_owning_row(
    owner: &MembershipOwner,
    read: &redb::ReadTransaction,
    index: &VectorIndexRef,
    identity: &RawVectorDirectoryEntry,
    body_deleted: Option<Option<TxId>>,
    before: &mut dyn FnMut() -> Result<()>,
) -> Result<()> {
    let Some(context) = operation_for(owner).filter(|context| context.historical) else { return Ok(()); };
    operation_checkpoint()?;
    if context.root.read.storage_snapshot_identity() != read.storage_snapshot_identity() {
        return Err(damage("vector owning row proof escaped its immutable capture"));
    }
    context.root.check_scope(&index.table)?;
    before()?;
    let stamp = identity.membership;
    if stamp.visible_from != stamp.owning_row_tx.max(identity.created_tx)
        || stamp.deleted_tx != identity.deleted_tx {
        return Err(damage("vector placement has an invalid owning-row interval"));
    }
    let mut row_deleted = None;
    let mut found = false;
    match &context.root.writer {
        Some(root) => root.visit_historical_rows(&index.table, Some(identity.row_id),
            SnapshotId(stamp.visible_from.0), context.memory.clone(), before,
            &mut |window| {
                if window.value.created_tx != stamp.owning_row_tx || window.value.lsn != stamp.row_lsn {
                    return Err(damage("vector membership names another owning row version"));
                }
                if found { return Err(damage("vector placement has duplicate owning row versions")); }
                row_deleted = window.value.deleted_tx; found = true;
                Ok(())
            })?,
        None => {
            let MembershipOwner::Retained(source) = owner else {
                return Err(damage("historical vector proof has no captured owner"));
            };
            let image = crate::read_image_memory::ImageMemory::charging_reader_for(
                source._source_memory.memory.accountant.clone(), context.memory.clone(),
                "reader", "vector_row_binding", HINT);
            let _controls = contextdb_core::read_memory::ReadCredit::try_new(context.memory.clone(),
                std::mem::size_of::<crate::read_image_memory::ImageMemory>() + 2 * std::mem::size_of::<usize>(),
                "reader", "vector_row_binding_controls", HINT)?;
            image.scope(|| -> Result<()> {
                let meta = RedbPersistence::load_table_meta_in_read_txn(read, &index.table)?
                    .ok_or_else(|| damage("vector owning row table has no captured schema"))?;
                let mut credit = contextdb_core::read_memory::ReadCredit::new(context.memory.clone());
                let mut last = None;
                let row = source.reconcile_relational_group::<Error>(read, &index.table, &meta, identity.row_id,
                    SnapshotId(stamp.visible_from.0), None, context.memory.clone(), before, None,
                    &mut |admission: crate::read_image_memory::RowReadAdmission<'_>| admission.retain_into(&mut credit, "reader", "vector_owning_row", HINT), &mut last)?;
                if let Some(row) = row {
                    if row.created_tx != stamp.owning_row_tx || row.lsn != stamp.row_lsn {
                        return Err(damage("vector membership names another owning row version"));
                    }
                    row_deleted = row.deleted_tx; found = true;
                }
                Ok(())
            })?;
        }
    }
    if !found { return Err(damage("vector membership has no authenticated owning row")); }
    if let Some(body_deleted) = body_deleted {
        let effective = match (row_deleted, body_deleted) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        if stamp.deleted_tx != effective { return Err(damage("vector placement closure differs from owning row and canonical body")); }
    } else if row_deleted.is_some_and(|closed| stamp.deleted_tx.is_none_or(|end| end > closed)) {
        return Err(damage("vector placement outlives its owning row version"));
    }
    before()?;
    Ok(())
}

pub(crate) fn select_live_fallback(
    owner: MembershipOwner,
    row: RowId,
    snapshot: SnapshotId,
    copies: &mut contextdb_vector::VectorCopyAdmission,
    allowed: &mut dyn FnMut(&VectorPartitionMembershipRecord) -> Result<bool>,
) -> Result<Option<SelectedVectorMembership>> {
    operation_checkpoint()?;
    let root = owner.capture()?;
    let members = open_optional_read_table(&root.read, VECTOR_PARTITION_MEMBERSHIP_TABLE)?;
    let mut cursor = members.as_ref().map(|table| table.iter().map_err(vector_membership_pages::metadata_error)).transpose()?;
    let mut selected = None::<SelectedVectorMembership>;
    loop {
        operation_checkpoint()?;
        let Some(item) = cursor.as_mut().and_then(Iterator::next) else { break; };
        let (key, value) = item.map_err(vector_membership_pages::metadata_error)?;
        let mut window = copies.scratch();
        window.reserve(key.value().len().checked_add(value.value().len())
            .and_then(|n| n.checked_mul(8)).and_then(|n| n.checked_add(std::mem::size_of::<SelectedVectorMembership>()))
            .ok_or_else(|| damage("live vector membership window overflow"))?)?;
        let payload = vector_partition_record_payload(VectorPartitionRecordKind::Membership, value.value())
            .map_err(|reason| damage(&reason))?;
        prevalidate_vector_partition_metadata_routes(VectorPartitionRecordKind::Membership, payload)
            .map_err(|reason| damage(&reason))?;
        window.reserve(vector_partition_route_native_capacity(payload).map_err(|reason| damage(&reason))?
            .checked_mul(6).ok_or_else(|| damage("vector route clone capacity overflow"))?)?;
        let record: VectorPartitionMembershipRecord = decode_vector_partition_record_payload_checked(
            VectorPartitionRecordKind::Membership, value.value()).map_err(|failure| failure.into_error(|reason| damage(&reason)))?;
        let partition = VectorPartitionRef::new(record.index.clone(), record.partition_key.clone());
        decode_forward(&partition, key.value(), value.value())?;
        authenticate_owning_row(&owner, &root.read, &record.index, &body_identity(&record), None,
            &mut || operation_checkpoint())?;
        if record.row_id != row || !visible(&record, snapshot) || !allowed(&record)? { continue; }
        // The old max_by_key selected the last occurrence for equal stamps.
        if selected.as_ref().is_none_or(|prior| (prior.record.vector_lsn, prior.record.vector_created_tx)
            <= (record.vector_lsn, record.vector_created_tx)) {
            selected = Some(SelectedVectorMembership { root: root.clone(), owner: owner.clone(),
                record, _copy_owner: window });
        }
    }
    Ok(selected)
}

pub(crate) struct SelectedVectorMembership {
    root: MembershipRoot,
    owner: MembershipOwner,
    record: VectorPartitionMembershipRecord,
    _copy_owner: contextdb_vector::VectorCopyAdmission,
}

impl contextdb_vector::store::RawVectorMembershipSelection for SelectedVectorMembership {
    fn identity(&self) -> RawVectorDirectoryEntry {
        body_identity(&self.record)
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl SelectedVectorMembership {
    pub(crate) fn record(&self) -> &VectorPartitionMembershipRecord { &self.record }
    pub(crate) fn read_body(
        &self,
        owner: &MembershipOwner,
        partition: &VectorPartitionRef,
        dimension: usize,
        quantization: VectorQuantization,
    ) -> Result<VectorEntry> {
        self.read_body_before(owner, partition, dimension, quantization, &mut || operation_checkpoint())
    }
    pub(crate) fn read_body_before(
        &self,
        owner: &MembershipOwner,
        partition: &VectorPartitionRef,
        dimension: usize,
        quantization: VectorQuantization,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<VectorEntry> {
        let same_source = match (&self.owner, owner) {
            (MembershipOwner::Writable(a), MembershipOwner::Writable(b)) => Arc::ptr_eq(a, b),
            (MembershipOwner::Retained(a), MembershipOwner::Retained(b)) => Arc::ptr_eq(a, b),
            _ => false,
        };
        if !same_source
            || self.record.index != partition.index
            || self.record.partition_key != partition.partition_key
        {
            return Err(damage(
                "selected membership belongs to another source or route",
            ));
        }
        operation_checkpoint()?;
        self.root.check_scope(&partition.index.table)?;
        let identity = body_identity(&self.record);
        let memory = historical_operation_memory(owner, &self.root.read)?;
        let mut load = || match owner {
            MembershipOwner::Writable(source) => {
                let pin = self
                    .root
                    .writer
                    .as_ref()
                    .map(|writer| writer.clone() as Arc<dyn std::any::Any + Send + Sync>);
                source
                    .open_raw_vector_body_scan_at(&partition.index, self.root.read.clone(), pin)?
                    .read_partition_body_before(
                        &partition.index,
                        &partition.partition_key,
                        &identity,
                        dimension,
                        quantization,
                        true,
                        before,
                    )
            }
            MembershipOwner::Retained(source) => source
                .open_raw_vector_body_scan_at(self.root.read.clone())?
                .read_partition_body_before(
                    &partition.index,
                    &partition.partition_key,
                    &identity,
                    dimension,
                    quantization,
                    true,
                    before,
                ),
        };
        match memory {
            Some(memory) => graph_read::with_pages(memory, load),
            None => load(),
        }
    }
}

pub(super) fn read_summary(
    read: &redb::ReadTransaction,
    partition: &VectorPartitionRef,
) -> Result<RawVectorMembershipSummary> {
    operation_checkpoint()?;
    let context = OPERATION.with(|slot| slot.borrow().clone());
    let memory = match &context {
        Some(context) => historical_operation_memory(&context.owner, read)?,
        None => None,
    };
    let summary = match memory {
        Some(memory) => graph_read::with_pages(memory, || vector_membership_pages::read_partition_summary(read, partition))?,
        None => vector_membership_pages::read_partition_summary(read, partition)?,
    };
    summary_counts(&summary)
}

fn summary_counts(
    summary: &vector_membership_pages::MembershipSummary,
) -> Result<RawVectorMembershipSummary> {
    Ok(RawVectorMembershipSummary {
        entries: usize::try_from(summary.entries)
            .map_err(|_| damage("membership count overflow"))?,
        live_entries: usize::try_from(summary.live_entries)
            .map_err(|_| damage("live membership count overflow"))?,
        max_tx: summary.max_tx,
    })
}

/// The body identity a raw-vector loader resolves.
fn body_identity(record: &VectorPartitionMembershipRecord) -> RawVectorDirectoryEntry {
    RawVectorDirectoryEntry {
        row_id: record.row_id,
        created_tx: record.vector_created_tx,
        deleted_tx: record.deleted_tx,
        lsn: record.vector_lsn,
        membership: contextdb_vector::store::VectorMembershipProvenance::new(
            record.row_created_tx,
            record.row_lsn,
            record.visible_from,
            record.deleted_tx,
        ),
    }
}

/// The visibility interval a cursor reports: the owning row version's
/// interval, which is when this placement is visible.
fn visibility_interval(record: &VectorPartitionMembershipRecord) -> RawVectorDirectoryEntry {
    RawVectorDirectoryEntry {
        row_id: record.row_id,
        created_tx: record.visible_from,
        deleted_tx: record.deleted_tx,
        lsn: record.vector_lsn,
        membership: contextdb_vector::store::VectorMembershipProvenance::new(
            record.row_created_tx,
            record.row_lsn,
            record.visible_from,
            record.deleted_tx,
        ),
    }
}

fn visible(record: &VectorPartitionMembershipRecord, snapshot: SnapshotId) -> bool {
    record.visible_from.0 <= snapshot.0 && record.deleted_tx.is_none_or(|tx| tx.0 > snapshot.0)
}

fn decode_forward(
    partition: &VectorPartitionRef,
    key: &[u8],
    value: &[u8],
) -> Result<VectorPartitionMembershipRecord> {
    #[cfg(feature = "test-seams")]
    crate::database::resident_history_test_seams::note(|work| work.membership_keys_examined += 1);
    let record: VectorPartitionMembershipRecord =
        decode_vector_partition_record_payload_checked(VectorPartitionRecordKind::Membership, value)
            .map_err(|failure| failure.into_error(|reason| damage(&reason)))?;
    if RedbPersistence::vector_partition_membership_key(&record).as_slice() != key
        || record.index != partition.index
        || record.partition_key != partition.partition_key
        || record.visible_from != record.row_created_tx.max(record.vector_created_tx)
        || record
            .deleted_tx
            .is_some_and(|tx| tx <= record.visible_from)
    {
        return Err(damage(
            "membership key, route or lifecycle differs from its authenticated value",
        ));
    }
    Ok(record)
}

pub(crate) type PublishedMembershipRoot =
    Arc<dyn Fn() -> Result<Option<Arc<WriterSnapshot>>> + Send + Sync>;

/// One partition's membership directory, read from the forward membership
/// table at each operation's captured root.
pub(crate) struct DurableMembershipSource {
    published_root: Option<PublishedMembershipRoot>,
    owner: MembershipOwner,
    partition: VectorPartitionRef,
    summary: RawVectorMembershipSummary,
    _copy_owner: Option<Arc<contextdb_vector::VectorCopyAdmission>>,
    // Retire the selected writer epoch only after all payload owners finish.
    accountant: StoreAccountantHandle,
}

impl DurableMembershipSource {
    pub(crate) fn from_owner_admitted(
        source_owner: MembershipOwner,
        published_root: Option<PublishedMembershipRoot>,
        partition: &VectorPartitionRef,
        summary: RawVectorMembershipSummary,
        accountant: Arc<MemoryAccountant>,
        workspace: &mut contextdb_vector::VectorCopyAdmission,
        mut retained: contextdb_vector::VectorCopyAdmission,
    ) -> Result<Arc<Self>> {
        workspace.reserve(
            std::mem::size_of::<Self>()
                .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
                .saturating_add(4 * std::mem::size_of::<usize>())
                .saturating_add(
                    partition
                        .index
                        .table
                        .len()
                        .saturating_add(partition.index.column.len())
                        .saturating_add(partition.partition_key.estimated_bytes()),
                ),
        )?;
        retained.reserve(
            std::mem::size_of::<Self>()
                .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
                .saturating_add(4 * std::mem::size_of::<usize>())
                .saturating_add(
                    partition
                        .index
                        .table
                        .len()
                        .saturating_add(partition.index.column.len())
                        .saturating_add(partition.partition_key.estimated_bytes()),
                ),
        )?;
        Ok(Arc::new(Self {
            owner: source_owner,
            published_root,
            partition: partition.clone(),
            summary,
            _copy_owner: Some(Arc::new(retained)),
            accountant: StoreAccountantHandle::new(accountant),
        }))
    }

    // Compare physical user-tree headers, independent of logical TxId and
    // of the Arc wrapper used to materialize a retained root.
    fn captured_catalog_required(&self, root: &MembershipRoot) -> Result<bool> {
        let Some(captured) = &root.writer else {
            return Ok(false);
        };
        if let MembershipOwner::Writable(persistence) = &self.owner
            && let Some(operation) = persistence.scoped_vector_root_capture.get()
                .map(|capture| capture()).transpose()?.flatten()
        {
            if operation.storage_identity() != captured.storage_identity() {
                return Err(damage("membership catalog escaped its operation capture"));
            }
            return Ok(true);
        }
        let published = (self
            .published_root
            .as_ref()
            .ok_or_else(|| damage("writer publication has no root capture"))?)(
        )?
        .ok_or_else(|| damage("writer publication has no durable root"))?;
        Ok(captured.storage_identity().user_root != published.storage_identity().user_root)
    }

    fn inspection_in_root(
        &self,
        root: &MembershipRoot,
        generations: Option<(
            VectorPartitionGenerationDescriptor,
            Option<VectorPartitionGenerationDescriptor>,
        )>,
        serving_covered: Option<TxId>,
    ) -> Result<RawVectorInspectionSnapshot> {
        let mut summary = self.read_pending_metadata(root)?;
        let retired_empty = !self.partition.partition_key.is_unpartitioned()
            && summary == vector_membership_pages::MembershipSummary::default();
        let (base, change) = generations.map_or((None, None), |(base, change)| {
            let identity = |generation: VectorPartitionGenerationDescriptor| {
                contextdb_vector::store::VectorGraphGeneration {
                    generation_id: generation.generation_id,
                    covered_tx: generation.covered_tx,
                    covered_lsn: generation.covered_lsn,
                    durable_bytes: generation.durable_bytes,
                    policy_revision: generation.policy_revision,
                    hnsw_m: generation.hnsw_m,
                    hnsw_ef_construction: generation.hnsw_ef_construction,
                }
            };
            (Some(identity(base)), change.map(identity))
        });
        let covered = change
            .or(base)
            .map_or(summary.pending.covered_tx, |generation| {
                generation.covered_tx
            });
        // Serving metadata and counters are committed together. A candidate
        // matching some other frontier cannot authenticate this catalog.
        if retired_empty {
            // A pinned retired catalog outlives its removed summary. The read
            // helper validated authoritative membership absence on this root.
            summary.pending.covered_tx = covered;
        } else if summary.pending.covered_tx != covered {
            return Err(damage(
                "pending summary disagrees with captured catalog frontier",
            ));
        }
        let captured_catalog = self.captured_catalog_required(root)?;
        let frontier = if captured_catalog || generations.is_none() {
            covered
        } else {
            serving_covered.unwrap_or(covered)
        };
        let mut frontier = frontier;
        if let (Some(base), Some(change)) = (base, change)
            && change.generation_id & super::vector_paged::MUTABLE_STORAGE != 0
        {
            let mut native_metadata =
                contextdb_vector::VectorCopyAdmission::new(self.accountant.vector_budget()?);
            native_metadata.reserve(16 * 1024 + self.window_bytes())?;
            let record = RedbPersistence::load_vector_partition_change_generation_in_read(
                &root.read,
                &self.partition.index,
                &self.partition.partition_key,
                base.generation_id,
                change.generation_id,
            )?;
            let meta = super::vector_paged::PagedGenerationMeta::decode(&record.graph.bytes)?;
            if meta.page_set.pages == super::vector_paged::MUTABLE_PAGE_SET {
                // Native tail publication advances serving coverage while
                // retaining the base's default consolidation counters.
                frontier = base.covered_tx;
            }
        }
        let (pending_inserts, tombstones) = Self::pending_counts_for(&summary, Some(frontier))?;
        Ok(RawVectorInspectionSnapshot {
            captured_catalog,
            summary: summary_counts(&summary)?,
            base,
            change,
            pending_inserts,
            tombstones,
            #[cfg(feature = "test-seams")]
            base_pending: {
                let frontier = base.map_or(TxId(0), |generation| generation.covered_tx);
                [
                    &summary.pending,
                    summary.base_pending.as_ref().unwrap_or(&summary.pending),
                ]
                .into_iter()
                .find(|counts| counts.covered_tx == frontier)
                .map(|counts| (counts.pending_inserts as usize, counts.tombstones as usize))
            },
        })
    }

    fn catalog_frontier(&self, root: &MembershipRoot) -> Result<Option<TxId>> {
        let generations = self.catalog_generations(root)?;
        Ok(generations.map(|(base, change)| change.unwrap_or(base).covered_tx))
    }

    fn catalog_generations(
        &self,
        root: &MembershipRoot,
    ) -> Result<
        Option<(
            VectorPartitionGenerationDescriptor,
            Option<VectorPartitionGenerationDescriptor>,
        )>,
    > {
        match RedbPersistence::vector_partition_generation_descriptors_in_read(
            &root.read,
            &self.partition.index,
            &self.partition.partition_key,
        )
        .map_err(vector_membership_pages::typed_metadata_error)
        {
            Ok(generations) => Ok(generations),
            // Catalog damage belongs to its route. The caller has already
            // authenticated the membership summary at this captured root.
            Err(Error::StoreCorrupted { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn window_bytes(&self) -> usize {
        self.partition
            .index
            .table
            .len()
            .saturating_add(self.partition.index.column.len())
            .saturating_add(self.partition.partition_key.estimated_bytes())
            .saturating_mul(32)
            .saturating_add(4096)
    }

    fn read_pending_metadata(
        &self,
        root: &MembershipRoot,
    ) -> Result<vector_membership_pages::MembershipSummary> {
        root.check_scope(&self.partition.index.table)?;
        #[cfg(feature = "test-seams")]
        crate::database::resident_history_test_seams::note(|work| work.pending_summary_reads += 1);
        #[cfg(feature = "test-seams")]
        if self.owner.persistence_identity().is_some_and(|identity| {
            PENDING_SUMMARY_FAILURE.with(|slot| slot.get()) == Some(identity)
        }) {
            return Err(damage("injected pending summary metadata read failure"));
        }
        let summary = vector_membership_pages::read_partition_summary(&root.read, &self.partition)?;
        Ok(summary)
    }

    fn pending_metadata_window(
        &self,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<contextdb_vector::VectorCopyAdmission> {
        let mut scratch = owner.scratch();
        scratch.reserve_for(
            self.partition
                .index
                .table
                .len()
                .saturating_add(self.partition.index.column.len())
                .saturating_add(self.partition.partition_key.estimated_bytes())
                .saturating_add(24)
                .saturating_mul(2)
                .saturating_add(512),
            "vector",
            "pending_metadata",
            HINT,
        )?;
        Ok(scratch)
    }

    fn pending_counts_for(
        summary: &vector_membership_pages::MembershipSummary,
        covered: Option<TxId>,
    ) -> Result<(usize, usize)> {
        // The requested frontier must be represented at this exact root.
        let counts = match covered {
            None => &summary.pending,
            Some(frontier) => std::iter::once(&summary.pending)
                .chain(summary.base_pending.iter())
                .chain(summary.candidate_pending.iter())
                .find(|counts| counts.covered_tx == frontier)
                .ok_or_else(|| damage("pending summary disagrees with serving frontier"))?,
        };
        Ok((
            usize::try_from(counts.pending_inserts)
                .map_err(|_| damage("pending insert count overflow"))?,
            usize::try_from(counts.tombstones)
                .map_err(|_| damage("pending tombstone count overflow"))?,
        ))
    }

    fn read_pending_counts(
        &self,
        covered: Option<TxId>,
        capture: impl FnOnce() -> Result<MembershipRoot>,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Option<(usize, usize)>> {
        let _window = self.pending_metadata_window(owner)?;
        let root = capture()?;
        let summary = self.read_pending_metadata(&root)?;
        let covered = if self.captured_catalog_required(&root)? {
            let frontier = self
                .catalog_frontier(&root)?
                .unwrap_or(summary.pending.covered_tx);
            if summary.pending.covered_tx != frontier {
                return Err(damage(
                    "pending summary disagrees with captured catalog frontier",
                ));
            }
            Some(frontier)
        } else {
            covered
        };
        let mut covered = covered;
        if let Some((base, Some(change))) = self.catalog_generations(&root)?
            && change.generation_id & super::vector_paged::MUTABLE_STORAGE != 0
            && covered == Some(change.covered_tx)
        {
            let mut metadata =
                contextdb_vector::VectorCopyAdmission::new(self.accountant.vector_budget()?);
            metadata.reserve(16 * 1024 + self.window_bytes())?;
            let record = RedbPersistence::load_vector_partition_change_generation_in_read(
                &root.read,
                &self.partition.index,
                &self.partition.partition_key,
                base.generation_id,
                change.generation_id,
            )?;
            let meta = super::vector_paged::PagedGenerationMeta::decode(&record.graph.bytes)?;
            if meta.page_set.pages == super::vector_paged::MUTABLE_PAGE_SET {
                covered = Some(base.covered_tx);
            }
        }
        Self::pending_counts_for(&summary, covered).map(Some)
    }

    fn cursor(&self, row: Option<RowId>) -> Result<MembershipCursor> {
        operation_checkpoint()?;
        let root = self.owner.capture()?;
        root.check_scope(&self.partition.index.table)?;
        let prefix = match row {
            Some(row) => RedbPersistence::vector_partition_membership_row_prefix(
                &self.partition.index,
                &self.partition.partition_key,
                row,
            ),
            None => RedbPersistence::vector_partition_prefix(
                &self.partition.index,
                &self.partition.partition_key,
            ),
        };
        let expected = match row {
            Some(_) => None,
            None => Some(read_summary(&root.read, &self.partition)?.entries),
        };
        Ok(MembershipCursor {
            owner: self.owner.clone(),
            selected_identity: None,
            root,
            partition: self.partition.clone(),
            prefix,
            range: None,
            last_success: None,
            done: false,
            max_bytes: self.window_bytes(),
            seen: 0,
            expected,
        })
    }

    /// Visit this partition's journal changes after `after_lsn`, in LSN
    /// order, at one captured root; `visit` returns false to stop.
    fn journal_suffix(
        &self,
        after_lsn: Lsn,
        visit: &mut dyn FnMut(VectorPartitionJournalChange) -> bool,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<()> {
        let mut scratch = owner.scratch();
        scratch.reserve_for(self.window_bytes(), "vector", "membership_window", HINT)?;
        operation_checkpoint()?;
        let root = self.owner.capture()?;
        root.check_scope(&self.partition.index.table)?;
        let table = match root.read.open_table(VECTOR_PARTITION_JOURNAL_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
            Err(error) => return Err(vector_membership_pages::metadata_error(error)),
        };
        let prefix = RedbPersistence::vector_partition_prefix(
            &self.partition.index,
            &self.partition.partition_key,
        );
        let mut lower = prefix.clone();
        lower.extend_from_slice(&after_lsn.0.saturating_add(1).to_be_bytes());
        let end = writer_source::prefix_end(&prefix);
        let upper = end.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
        for item in table
            .range::<&[u8]>((Bound::Included(lower.as_slice()), upper))
            .map_err(vector_membership_pages::metadata_error)?
        {
            let (key, value) = item.map_err(vector_membership_pages::metadata_error)?;
            let required = key
                .value()
                .len()
                .saturating_mul(2)
                .saturating_add(value.value().len().saturating_mul(2))
                .saturating_add(self.window_bytes());
            if required > scratch.retained_bytes() {
                scratch.reserve(required - scratch.retained_bytes())?;
            }
            let record: VectorPartitionJournalRecord = decode_vector_partition_record_payload_checked(
                VectorPartitionRecordKind::Journal,
                value.value(),
            )
            .map_err(|failure| failure.into_error(|reason| damage(&reason)))?;
            if record.index != self.partition.index
                || record.partition_key != self.partition.partition_key
            {
                return Err(damage("journal route disagrees with membership source"));
            }
            if RedbPersistence::vector_partition_journal_key(&record).as_slice() != key.value()
                || record.lsn <= after_lsn
            {
                return Err(damage("journal key or LSN disagrees with its suffix range"));
            }
            if !visit(record.change) {
                break;
            }
        }
        Ok(())
    }

    /// Walk one row prefix (or the whole partition) under a single admitted
    /// window, stopping at the first record `select` accepts.
    fn find_selected(
        &self,
        row: RowId,
        before: &mut dyn FnMut() -> bool,
        select: impl Fn(&VectorPartitionMembershipRecord) -> bool,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Option<Box<dyn contextdb_vector::store::RawVectorMembershipSelection>>> {
        let mut scratch = owner.scratch();
        scratch.reserve_for(self.window_bytes(), "vector", "membership_window", HINT)?;
        let mut cursor = self.cursor(Some(row))?;
        loop {
            let mut interrupted = None;
            let next = cursor.next_record(&mut |request| match request {
                DormantVectorLoadRequest::Reserve(bytes) => match scratch.reserve(bytes) {
                    Ok(()) => true,
                    Err(error) => {
                        interrupted = Some(error);
                        false
                    }
                },
                _ => before(),
            });
            if let Some(error) = interrupted {
                return Err(error);
            }
            let Some(record) = next? else {
                return Ok(None);
            };
            if select(&record) {
                authenticate_owning_row(&self.owner, &cursor.root.read, &self.partition.index,
                    &body_identity(&record), None, &mut || if before() { Ok(()) } else { Err(Error::ReadCancelled) })?;
                let mut retained = owner.scratch();
                retained.reserve(
                    std::mem::size_of::<SelectedVectorMembership>()
                        .saturating_add(
                            std::mem::size_of::<WritableRawVectorBodyScan>()
                                .max(std::mem::size_of::<RetainedRawVectorBodyScan>()),
                        )
                        .saturating_add(std::mem::size_of::<redb::ReadTransaction>())
                        .saturating_add(4 * std::mem::size_of::<usize>())
                        .saturating_add(record.index.table.len())
                        .saturating_add(record.index.column.len())
                        .saturating_add(record.partition_key.estimated_bytes()),
                )?;
                return Ok(Some(Box::new(SelectedVectorMembership {
                    root: cursor.root,
                    owner: self.owner.clone(),
                    record,
                    _copy_owner: retained,
                })));
            }
        }
    }

    fn find<T>(
        &self,
        row: Option<RowId>,
        before: &mut dyn FnMut() -> bool,
        select: impl FnMut(&VectorPartitionMembershipRecord) -> Option<T>,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Option<T>> {
        self.find_with_copy_admission(
            row,
            before,
            select,
            owner,
            &mut |token, bytes, component, operation, hint| {
                token.reserve_for(bytes, component, operation, hint)
            },
        )
    }

    fn find_with_copy_admission<T>(
        &self,
        row: Option<RowId>,
        before: &mut dyn FnMut() -> bool,
        mut select: impl FnMut(&VectorPartitionMembershipRecord) -> Option<T>,
        owner: &mut contextdb_vector::VectorCopyAdmission,
        admit: &mut dyn FnMut(
            &mut contextdb_vector::VectorCopyAdmission,
            usize,
            &str,
            &str,
            &str,
        ) -> Result<()>,
    ) -> Result<Option<T>> {
        let mut scratch = owner.scratch();
        admit(
            &mut scratch,
            self.window_bytes(),
            "vector",
            "membership_window",
            HINT,
        )?;
        let mut cursor = self.cursor(row)?;
        loop {
            let mut interrupted = None;
            let next = cursor.next_record(&mut |request| match request {
                DormantVectorLoadRequest::Reserve(bytes) => match admit(
                    &mut scratch,
                    bytes,
                    "vector_copy",
                    "materialize",
                    "Reduce the selected copy or raise the executing statement's memory limit.",
                ) {
                    Ok(()) => true,
                    Err(error) => {
                        interrupted = Some(error);
                        false
                    }
                },
                _ => before(),
            });
            if let Some(error) = interrupted {
                return Err(error);
            }
            let Some(record) = next? else {
                break;
            };
            if let Some(value) = select(&record) {
                authenticate_owning_row(&self.owner, &cursor.root.read, &self.partition.index,
                    &body_identity(&record), None, &mut || if before() { Ok(()) } else { Err(Error::ReadCancelled) })?;
                return Ok(Some(value));
            }
        }
        Ok(None)
    }
}

pub(crate) struct MembershipCursor {
    owner: MembershipOwner,
    selected_identity: Option<RawVectorDirectoryEntry>,
    root: MembershipRoot,
    partition: VectorPartitionRef,
    prefix: Vec<u8>,
    range: Option<redb::Range<'static, &'static [u8], &'static [u8]>>,
    last_success: Option<Vec<u8>>,
    done: bool,
    max_bytes: usize,
    seen: usize,
    expected: Option<usize>,
}

impl MembershipCursor {
    pub(crate) fn open_body_scan(
        &self,
        owner: &MembershipOwner,
        partition: &VectorPartitionRef,
    ) -> Result<Box<dyn contextdb_vector::store::RawVectorBodyScan>> {
        self.validate_selected(owner, &self.root.read, partition)?;
        match owner {
            MembershipOwner::Writable(source) => {
                let pin = self
                    .root
                    .writer
                    .as_ref()
                    .map(|writer| writer.clone() as Arc<dyn std::any::Any + Send + Sync>);
                Ok(Box::new(source.open_raw_vector_body_scan_at(
                    &partition.index,
                    self.root.read.clone(),
                    pin,
                )?))
            }
            MembershipOwner::Retained(source) => Ok(Box::new(
                source.open_raw_vector_body_scan_at(self.root.read.clone())?,
            )),
        }
    }

    pub(crate) fn validate_selected(
        &self,
        owner: &MembershipOwner,
        read: &redb::ReadTransaction,
        partition: &VectorPartitionRef,
    ) -> Result<RawVectorDirectoryEntry> {
        let same_source = match (&self.owner, owner) {
            (MembershipOwner::Writable(a), MembershipOwner::Writable(b)) => Arc::ptr_eq(a, b),
            (MembershipOwner::Retained(a), MembershipOwner::Retained(b)) => Arc::ptr_eq(a, b),
            _ => false,
        };
        if !same_source
            || self.partition != *partition
            || self.root.read.storage_snapshot_identity() != read.storage_snapshot_identity()
        {
            return Err(damage(
                "streamed membership belongs to another source, route or physical root",
            ));
        }
        self.root.check_scope(&partition.index.table)?;
        self.selected_identity
            .clone()
            .ok_or_else(|| damage("membership cursor has no selected record"))
    }

    fn next_record(
        &mut self,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<Option<VectorPartitionMembershipRecord>> {
        operation_checkpoint()?;
        self.selected_identity = None;
        let memory = historical_operation_memory(&self.owner, &self.root.read)?;
        let result = match memory {
            Some(memory) => graph_read::with_pages(memory, || self.next_record_attempt(request)),
            None => self.next_record_attempt(request),
        };
        if let Ok(Some(record)) = &result {
            self.selected_identity = Some(body_identity(record));
        }
        if result.is_err() {
            // The failed iterator step may already have moved its leaf cursor.
            // Reopen only after the last authenticated success at this same root.
            self.range = None;
        }
        result
    }

    fn next_record_attempt(
        &mut self,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<Option<VectorPartitionMembershipRecord>> {
        operation_checkpoint()?;
        if self.done {
            return Ok(None);
        }
        if !request(DormantVectorLoadRequest::Checkpoint) {
            return Err(Error::ReadCancelled);
        }
        self.root.check_scope(&self.partition.index.table)?;
        // Membership pages use the writer's declared native cache share.
        // Repeated exhaustive reads reuse admitted pages without a decoded mirror.
        if self.range.is_none() {
            let table = match self.root.read.open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => {
                    if self.expected.is_some_and(|expected| expected != 0) {
                        return Err(damage(
                            "membership table is missing beside its maintained summary",
                        ));
                    }
                    self.done = true;
                    return Ok(None);
                }
                Err(error) => return Err(vector_membership_pages::metadata_error(error)),
            };
            let end = writer_source::prefix_end(&self.prefix);
            let upper = end.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
            // The range is held for the stream's lifetime: one seek, then
            // each record advances within its leaf.
            self.range = Some(
                table
                    .range::<&[u8]>((
                        self.last_success
                            .as_deref()
                            .map_or(Bound::Included(self.prefix.as_slice()), Bound::Excluded),
                        upper,
                    ))
                    .map_err(vector_membership_pages::metadata_error)?,
            );
        }
        let item = self
            .range
            .as_mut()
            .expect("membership range is open")
            .next()
            .transpose()
            .map_err(vector_membership_pages::metadata_error)?;
        let Some((key, value)) = item else {
            if self.expected.is_some_and(|expected| expected != self.seen) {
                return Err(damage(
                    "membership stream cardinality differs from its maintained summary",
                ));
            }
            self.done = true;
            return Ok(None);
        };
        let required = key
            .value()
            .len()
            .checked_add(value.value().len())
            .and_then(|bytes| bytes.checked_mul(3))
            .ok_or_else(|| damage("membership metadata window overflow"))?;
        if required > self.max_bytes {
            // Admission precedes decode and the retained retry-key copy.
            // Refusal leaves this same record available for a funded retry.
            if !request(DormantVectorLoadRequest::Reserve(required - self.max_bytes)) {
                return Err(Error::ReadCancelled);
            }
            self.max_bytes = required;
        }
        let payload = vector_partition_record_payload(VectorPartitionRecordKind::Membership, value.value())
            .map_err(|reason| damage(&reason))?;
        prevalidate_vector_partition_metadata_routes(VectorPartitionRecordKind::Membership, payload)
            .map_err(|reason| damage(&reason))?;
        let native = vector_partition_route_native_capacity(payload).map_err(|reason| damage(&reason))?;
        let required = required.checked_add(native.checked_mul(3)
            .ok_or_else(|| damage("membership route native capacity overflow"))?)
            .ok_or_else(|| damage("membership native window overflow"))?;
        if required > self.max_bytes {
            if !request(DormantVectorLoadRequest::Reserve(required - self.max_bytes)) { return Err(Error::ReadCancelled); }
            self.max_bytes = required;
        }
        let record = decode_forward(&self.partition, key.value(), value.value())?;
        authenticate_owning_row(&self.owner, &self.root.read, &self.partition.index,
            &body_identity(&record), None, &mut || if request(DormantVectorLoadRequest::Checkpoint) {
                Ok(())
            } else { Err(Error::ReadCancelled) })?;
        // The admitted metadata window includes this retained retry key.
        self.last_success = Some(key.value().to_vec());
        self.seen += 1;
        Ok(Some(record))
    }
}

impl RawVectorMembershipCursor for MembershipCursor {
    fn selected_receipt(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }

    fn next_with_body_identity(
        &mut self,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<Option<(RawVectorDirectoryEntry, RawVectorDirectoryEntry)>> {
        self.next_record(request).map(|record| {
            record
                .as_ref()
                .map(|record| (visibility_interval(record), body_identity(record)))
        })
    }

    fn next(
        &mut self,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<Option<RawVectorDirectoryEntry>> {
        self.next_record(request)
            .map(|record| record.as_ref().map(visibility_interval))
    }
}

impl RawVectorMembershipSource for DurableMembershipSource {
    fn visit_retained_copy_credits(
        &self,
        caller: &dyn contextdb_vector::MemoryBudget,
        visit: &mut dyn FnMut(usize, usize, bool),
    ) {
        if let Some(credit) = &self._copy_owner {
            contextdb_vector::VectorCopyAdmission::visit_retained_copy_credit(
                credit, caller, visit,
            );
        }
    }

    fn capture_serving_selection(
        &self,
        snapshot: SnapshotId,
        frozen_base: Option<&contextdb_vector::store::VectorGraphGeneration>,
        frozen_change: Option<&contextdb_vector::store::VectorGraphGeneration>,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<contextdb_vector::store::CapturedVectorServingSelection> {
        let _window = self.pending_metadata_window(owner)?;
        operation_checkpoint()?;
        let root = if let Some(context) = operation_for(&self.owner) {
            context.root.clone()
        } else { match &self.owner {
            MembershipOwner::Writable(persistence) => {
                let writer = persistence
                    .scoped_vector_root_capture
                    .get()
                    .ok_or_else(|| {
                        damage("writable vector serving has no scoped root supplier")
                    })?()?
                .ok_or_else(|| damage("writable vector serving has no operation-scoped root"))?;
                writer.check_table_scope(&self.partition.index.table)?;
                let read = writer.owner_read()?;
                if read.storage_snapshot_identity().user_root != writer.storage_identity().user_root
                {
                    return Err(damage("serving read escaped its operation physical root"));
                }
                vector_membership_pages::validate_in_read(&read)?;
                MembershipRoot {
                    read,
                    writer: Some(writer),
                    retained: None,
                }
            }
            MembershipOwner::Retained(_) => self.owner.capture()?,
        } };
        root.check_scope(&self.partition.index.table)?;
        // Unlike passive inspection, serving cannot turn catalog corruption
        // into catalog absence or substitute a raw-only/fresh-root route.
        let generations = RedbPersistence::vector_partition_generation_descriptors_in_read(
            &root.read,
            &self.partition.index,
            &self.partition.partition_key,
        )
        .map_err(vector_membership_pages::typed_metadata_error)?;
        let mut inspection = self.inspection_in_root(&root, generations, None)?;
        let frozen_lsn = frozen_change
            .or(frozen_base)
            .map_or(Lsn(0), |layer| layer.covered_lsn);
        let catalog_floor =
            generations.map_or(Lsn(0), |(base, change)| change.unwrap_or(base).covered_lsn);
        if generations.is_none() || (!inspection.captured_catalog && frozen_lsn < catalog_floor) {
            // The catalog can commit and its journal prefix be truncated before
            // the registry installs. Serve that same root's coherent catalog,
            // never replay its missing prefix against the frozen predecessor.
            inspection.captured_catalog = true;
        }
        if inspection
            .change
            .or(inspection.base)
            .is_some_and(|generation| generation.covered_tx > TxId::from_snapshot(snapshot))
        {
            return Err(damage(
                "captured serving frontier is newer than the requested snapshot",
            ));
        }
        if generations.is_none() {
            let summary = self.read_pending_metadata(&root)?;
            if summary.pending.covered_tx != TxId(0) {
                return Err(damage(
                    "missing captured catalog has a nonempty serving frontier",
                ));
            }
        }
        match &self.owner {
            MembershipOwner::Writable(persistence) => persistence
                .captured_vector_serving_selection_in_read(
                    root.read,
                    root.writer,
                    root.retained,
                    &self.partition,
                    generations,
                    inspection,
                    snapshot,
                    owner,
                ),
            MembershipOwner::Retained(source) => source.captured_vector_serving_selection_in_read(
                root.read,
                root.writer,
                root.retained,
                &self.partition,
                generations,
                inspection,
                snapshot,
                owner,
            ),
        }
    }

    fn for_renamed_index(
        &self,
        index: &VectorIndexRef,
        owner: &mut contextdb_vector::VectorCopyAdmission,
        mut retained: contextdb_vector::VectorCopyAdmission,
    ) -> Result<Arc<dyn RawVectorMembershipSource>> {
        let mut charge = owner.scratch();
        charge.reserve(
            std::mem::size_of::<Self>()
                .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
                .saturating_add(4 * std::mem::size_of::<usize>())
                .saturating_add(index.table.len())
                .saturating_add(index.column.len())
                .saturating_add(self.partition.partition_key.estimated_bytes()),
        )?;
        retained.reserve(
            std::mem::size_of::<Self>()
                .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
                .saturating_add(4 * std::mem::size_of::<usize>())
                .saturating_add(index.table.len())
                .saturating_add(index.column.len())
                .saturating_add(self.partition.partition_key.estimated_bytes()),
        )?;
        Ok(Arc::new(Self {
            owner: self.owner.clone(),
            published_root: self.published_root.clone(),
            partition: VectorPartitionRef::new(index.clone(), self.partition.partition_key.clone()),
            summary: self.summary,
            _copy_owner: Some(Arc::new(retained)),
            accountant: self.accountant.clone(),
        }))
    }

    fn published_summary(&self) -> RawVectorMembershipSummary {
        self.summary
    }

    fn current_summary(
        &self,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<RawVectorMembershipSummary> {
        let _window = self.pending_metadata_window(owner)?;
        // Called after a removal the store did not publish itself (cleanup,
        // retention, purge), which commits before the writer republishes its
        // root: read the latest committed summary.
        let root = self.owner.latest()?;
        read_summary(&root.read, &self.partition)
    }

    fn occupancy_visible_at_any(
        &self,
        snapshots: &[SnapshotId],
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Option<bool>> {
        let mut scratch = owner.scratch();
        let route = RedbPersistence::vector_partition_prefix(
            &self.partition.index,
            &self.partition.partition_key,
        );
        scratch.reserve_for(
            route.len().saturating_mul(4).saturating_add(256),
            "vector",
            "membership_occupancy_lookup",
            HINT,
        )?;
        operation_checkpoint()?;
        let root = self.owner.capture()?;
        root.check_scope(&self.partition.index.table)?;
        vector_membership_pages::occupancy_visible_at_any_in_read(&root.read, &route, snapshots)
    }

    fn visible_count(
        &self,
        snapshot: SnapshotId,
        before: &mut dyn FnMut() -> bool,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<usize> {
        let mut scratch = owner.scratch();
        scratch.reserve_for(self.window_bytes(), "vector", "membership_window", HINT)?;
        operation_checkpoint()?;
        let root = self.owner.capture()?;
        root.check_scope(&self.partition.index.table)?;
        if root.snapshot().is_some_and(|newest| snapshot.0 >= newest.0) {
            // Every membership in this root is committed at or before the
            // statement's snapshot, so the visible ones are exactly the
            // maintained live count.
            return Ok(read_summary(&root.read, &self.partition)?.live_entries);
        }
        drop(root);
        drop(scratch);
        let mut count = 0;
        self.find(
            None,
            before,
            |record| {
                count += usize::from(visible(record, snapshot));
                None::<()>
            },
            owner,
        )?;
        Ok(count)
    }

    fn supports_root_selection(&self) -> bool {
        true
    }

    fn select_visible_by_row(
        &self,
        row: RowId,
        snapshot: SnapshotId,
        before: &mut dyn FnMut() -> bool,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Option<Box<dyn contextdb_vector::store::RawVectorMembershipSelection>>> {
        self.find_selected(row, before, |record| visible(record, snapshot), owner)
    }

    fn select_identity_by_row_lsn(
        &self,
        row: RowId,
        lsn: Lsn,
        before: &mut dyn FnMut() -> bool,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Option<Box<dyn contextdb_vector::store::RawVectorMembershipSelection>>> {
        self.find_selected(row, before, |record| record.vector_lsn == lsn, owner)
    }

    fn visible_by_row(
        &self,
        row: RowId,
        snapshot: SnapshotId,
        before: &mut dyn FnMut() -> bool,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Option<RawVectorDirectoryEntry>> {
        self.visible_by_row_with_copy_admission(
            row,
            snapshot,
            before,
            owner,
            &mut |token, bytes, component, operation, hint| {
                token.reserve_for(bytes, component, operation, hint)
            },
        )
    }

    fn visible_by_row_with_copy_admission(
        &self,
        row: RowId,
        snapshot: SnapshotId,
        before: &mut dyn FnMut() -> bool,
        owner: &mut contextdb_vector::VectorCopyAdmission,
        admit: &mut dyn FnMut(
            &mut contextdb_vector::VectorCopyAdmission,
            usize,
            &str,
            &str,
            &str,
        ) -> Result<()>,
    ) -> Result<Option<RawVectorDirectoryEntry>> {
        self.find_with_copy_admission(
            Some(row),
            before,
            |record| visible(record, snapshot).then(|| body_identity(record)),
            owner,
            admit,
        )
    }

    fn contains_row_lsn(
        &self,
        row: RowId,
        lsn: Lsn,
        before: &mut dyn FnMut() -> bool,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<bool> {
        Ok(self.identity_by_row_lsn(row, lsn, before, owner)?.is_some())
    }

    fn identity_by_row_lsn(
        &self,
        row: RowId,
        lsn: Lsn,
        before: &mut dyn FnMut() -> bool,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Option<RawVectorDirectoryEntry>> {
        self.find(
            Some(row),
            before,
            |record| (record.vector_lsn == lsn).then(|| body_identity(record)),
            owner,
        )
    }

    fn visit_suffix(
        &self,
        after_lsn: Lsn,
        visit: &mut dyn FnMut(contextdb_vector::store::RawVectorSuffixChange) -> Result<()>,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<bool> {
        use contextdb_vector::store::RawVectorSuffixChange;
        let mut failed = None;
        self.journal_suffix(
            after_lsn,
            &mut |change| {
                let result = match change {
                    VectorPartitionJournalChange::Upsert(membership) => {
                        let opened = visit(RawVectorSuffixChange::Opened {
                            identity: RawVectorDirectoryEntry {
                                deleted_tx: None,
                                ..body_identity(&membership)
                            },
                            visible_from: membership.visible_from,
                        });
                        match (opened, membership.deleted_tx) {
                            (Ok(()), Some(deleted_tx)) => visit(RawVectorSuffixChange::Closed {
                                row_id: membership.row_id,
                                created_tx: membership.vector_created_tx,
                                lsn: membership.vector_lsn,
                                deleted_tx,
                            }),
                            (result, _) => result,
                        }
                    }
                    VectorPartitionJournalChange::Tombstone(tombstone) => {
                        visit(RawVectorSuffixChange::Closed {
                            row_id: tombstone.membership.row_id,
                            created_tx: tombstone.membership.vector_created_tx,
                            lsn: tombstone.membership.vector_lsn,
                            deleted_tx: tombstone.deleted_tx,
                        })
                    }
                };
                if let Err(error) = result {
                    failed = Some(error);
                    return false;
                }
                true
            },
            owner,
        )?;
        failed.map_or(Ok(true), Err)
    }

    fn has_durable_pending_counts(&self) -> bool {
        true
    }

    fn pending_counts(
        &self,
        covered: Option<TxId>,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Option<(usize, usize)>> {
        self.read_pending_counts(covered, || self.owner.capture(), owner)
    }

    fn inspection_snapshot(
        &self,
        serving_covered: Option<TxId>,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Option<RawVectorInspectionSnapshot>> {
        let _window = self.pending_metadata_window(owner)?;
        operation_checkpoint()?;
        let root = self.owner.capture()?;
        let generations = self.catalog_generations(&root)?;
        self.inspection_in_root(&root, generations, serving_covered)
            .map(Some)
    }

    fn visit_identities(
        &self,
        before: &mut dyn FnMut() -> bool,
        visit: &mut dyn FnMut(RawVectorDirectoryEntry) -> Result<()>,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<()> {
        let mut failed = None;
        self.find(
            None,
            before,
            |record| {
                if let Err(error) = visit(body_identity(record)) {
                    failed = Some(error);
                    return Some(());
                }
                None
            },
            owner,
        )?;
        failed.map_or(Ok(()), Err)
    }

    fn visit_row_versions(
        &self,
        row: RowId,
        before: &mut dyn FnMut() -> bool,
        visit: &mut dyn FnMut(RawVectorDirectoryEntry) -> Result<()>,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<()> {
        let mut failed = None;
        self.find(
            Some(row),
            before,
            |record| {
                if let Err(error) = visit(body_identity(record)) {
                    failed = Some(error);
                    return Some(());
                }
                None
            },
            owner,
        )?;
        failed.map_or(Ok(()), Err)
    }

    fn open(
        &self,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<Box<dyn RawVectorMembershipCursor>> {
        if !request(DormantVectorLoadRequest::Reserve(self.window_bytes())) {
            return Err(Error::ReadCancelled);
        }
        Ok(Box::new(self.cursor(None)?))
    }
}

/// A column's row-to-partition directory, read from the reverse companion
/// at each operation's captured root.
pub(crate) struct DurableReverseMembershipSource {
    owner: MembershipOwner,
    index: VectorIndexRef,
    _copy_owner: Option<Arc<contextdb_vector::VectorCopyAdmission>>,
    // Retire the selected writer epoch only after all payload owners finish.
    accountant: StoreAccountantHandle,
}

impl DurableReverseMembershipSource {
    pub(crate) fn from_owner_admitted(
        source_owner: MembershipOwner,
        index: &VectorIndexRef,

        accountant: Arc<MemoryAccountant>,
        workspace: &mut contextdb_vector::VectorCopyAdmission,
        mut retained: contextdb_vector::VectorCopyAdmission,
    ) -> Result<Arc<Self>> {
        workspace.reserve(
            std::mem::size_of::<Self>()
                .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
                .saturating_add(4 * std::mem::size_of::<usize>())
                .saturating_add(index.table.len().saturating_add(index.column.len())),
        )?;
        retained.reserve(
            std::mem::size_of::<Self>()
                .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
                .saturating_add(4 * std::mem::size_of::<usize>())
                .saturating_add(index.table.len().saturating_add(index.column.len())),
        )?;
        Ok(Arc::new(Self {
            owner: source_owner,
            index: index.clone(),
            _copy_owner: Some(Arc::new(retained)),
            accountant: StoreAccountantHandle::new(accountant),
        }))
    }

    fn route_window_bytes(&self) -> usize {
        (self.index.table.len() + self.index.column.len()).saturating_mul(32) + 4096
    }

    fn cursor_admitted(
        &self,
        row: RowId,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<ReverseCursor> {
        let bytes = self.route_window_bytes();
        let mut scratch = owner.scratch();
        scratch.reserve(bytes)?;
        self.cursor_reserved(row, bytes, scratch)
    }

    fn cursor_reserved(
        &self,
        row: RowId,
        admitted_bytes: usize,
        scratch: contextdb_vector::VectorCopyAdmission,
    ) -> Result<ReverseCursor> {
        operation_checkpoint()?;
        let root = self.owner.capture()?;
        root.check_scope(&self.index.table)?;
        Ok(ReverseCursor {
            owner: self.owner.clone(),
            root,
            index: self.index.clone(),
            row,
            prefix: vector_membership_pages::reverse_row_prefix(&self.index, row),
            before_key: None,
            done: false,
            admitted_bytes,
            scratch,
        })
    }
}

struct ReverseCursor {
    owner: MembershipOwner,
    root: MembershipRoot,
    index: VectorIndexRef,
    row: RowId,
    prefix: Vec<u8>,
    before_key: Option<Vec<u8>>,
    done: bool,
    admitted_bytes: usize,
    // Declared last so all cursor buffers and the captured root drop first.
    scratch: contextdb_vector::VectorCopyAdmission,
}

impl ReverseCursor {
    fn next_record(
        &mut self,
        before: &mut dyn FnMut() -> bool,
    ) -> Result<Option<VectorPartitionMembershipRecord>> {
        operation_checkpoint()?;
        let memory = historical_operation_memory(&self.owner, &self.root.read)?;
        match memory {
            Some(memory) => graph_read::with_pages(memory, || self.next_record_in_capture(before)),
            None => self.next_record_in_capture(before),
        }
    }

    fn next_record_in_capture(
        &mut self,
        before: &mut dyn FnMut() -> bool,
    ) -> Result<Option<VectorPartitionMembershipRecord>> {
        operation_checkpoint()?;
        if self.done {
            return Ok(None);
        }
        if !before() {
            return Err(Error::ReadCancelled);
        }
        self.root.check_scope(&self.index.table)?;
        let table = match self.root.read.open_table(vector_membership_pages::REVERSE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                self.done = true;
                return Ok(None);
            }
            Err(error) => return Err(vector_membership_pages::metadata_error(error)),
        };
        let lower = Bound::Included(self.prefix.as_slice());
        let end = writer_source::prefix_end(&self.prefix);
        let upper = self
            .before_key
            .as_deref()
            .or(end.as_deref())
            .map_or(Bound::Unbounded, Bound::Excluded);
        let item = table
            .range::<&[u8]>((lower, upper))
            .map_err(vector_membership_pages::metadata_error)?
            .next_back()
            .transpose()
            .map_err(vector_membership_pages::metadata_error)?;
        let Some((key, value)) = item else {
            self.done = true;
            return Ok(None);
        };
        let needed = (key.value().len() + value.value().len())
            .saturating_mul(3)
            .saturating_add(std::mem::size_of::<VectorPartitionMembershipRecord>());
        if needed > self.admitted_bytes {
            let growth = needed - self.admitted_bytes;
            self.scratch.reserve(growth)?;
            self.admitted_bytes = needed;
        }
        #[cfg(feature = "test-seams")]
        crate::database::resident_history_test_seams::note(|work| {
            work.membership_keys_examined += 1
        });
        let payload = vector_partition_record_payload(VectorPartitionRecordKind::Membership, value.value())
            .map_err(|reason| damage(&reason))?;
        prevalidate_vector_partition_metadata_routes(VectorPartitionRecordKind::Membership, payload)
            .map_err(|reason| damage(&reason))?;
        let native = vector_partition_route_native_capacity(payload).map_err(|reason| damage(&reason))?;
        let needed = needed.checked_add(native.checked_mul(3)
            .ok_or_else(|| damage("reverse membership native route overflow"))?)
            .ok_or_else(|| damage("reverse membership native window overflow"))?;
        if needed > self.admitted_bytes {
            self.scratch.reserve(needed - self.admitted_bytes)?;
            self.admitted_bytes = needed;
        }
        let record: VectorPartitionMembershipRecord = decode_vector_partition_record_payload_checked(
            VectorPartitionRecordKind::Membership,
            value.value(),
        )
        .map_err(|failure| failure.into_error(|reason| damage(&reason)))?;
        if vector_membership_pages::reverse_key(&record).as_slice() != key.value()
            || record.index != self.index
            || record.row_id != self.row
        {
            return Err(damage(
                "reverse membership differs from its authenticated row identity",
            ));
        }
        authenticate_owning_row(&self.owner, &self.root.read, &self.index,
            &body_identity(&record), None, &mut || if before() { Ok(()) } else { Err(Error::ReadCancelled) })?;
        self.before_key = Some(key.value().to_vec());
        Ok(Some(record))
    }
}

impl RawVectorReverseMembershipCursor for ReverseCursor {
    fn next(
        &mut self,
        before: &mut dyn FnMut() -> bool,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Option<VectorPartitionKey>> {
        let Some(record) = self.next_record(before)? else {
            return Ok(None);
        };
        // A caller may retain several yielded keys while this cursor advances.
        // Keep their owned storage funded separately from reusable decode scratch.
        owner.reserve(record.partition_key.estimated_bytes())?;
        Ok(Some(record.partition_key))
    }
}

impl RawVectorReverseMembershipSource for DurableReverseMembershipSource {
    fn visit_retained_copy_credits(
        &self,
        caller: &dyn contextdb_vector::MemoryBudget,
        visit: &mut dyn FnMut(usize, usize, bool),
    ) {
        if let Some(credit) = &self._copy_owner {
            contextdb_vector::VectorCopyAdmission::visit_retained_copy_credit(
                credit, caller, visit,
            );
        }
    }

    fn for_renamed_index(
        &self,
        index: &VectorIndexRef,
        owner: &mut contextdb_vector::VectorCopyAdmission,
        mut retained: contextdb_vector::VectorCopyAdmission,
    ) -> Result<Arc<dyn RawVectorReverseMembershipSource>> {
        let mut charge = owner.scratch();
        charge.reserve(
            std::mem::size_of::<Self>()
                .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
                .saturating_add(4 * std::mem::size_of::<usize>())
                .saturating_add(index.table.len())
                .saturating_add(index.column.len()),
        )?;
        retained.reserve(
            std::mem::size_of::<Self>()
                .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
                .saturating_add(4 * std::mem::size_of::<usize>())
                .saturating_add(index.table.len())
                .saturating_add(index.column.len()),
        )?;
        Ok(Arc::new(Self {
            owner: self.owner.clone(),
            index: index.clone(),
            _copy_owner: Some(Arc::new(retained)),
            accountant: self.accountant.clone(),
        }))
    }

    fn visible_partition_admitted(
        &self,
        row: RowId,
        snapshot: SnapshotId,
        before: &mut dyn FnMut() -> bool,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Option<VectorPartitionKey>> {
        let mut cursor = self.cursor_admitted(row, owner)?;
        while let Some(record) = cursor.next_record(before)? {
            if visible(&record, snapshot) {
                // The key escapes the cursor's reusable decode window.
                // Admit its caller-held storage before moving it out.
                owner.reserve(record.partition_key.estimated_bytes())?;
                return Ok(Some(record.partition_key));
            }
        }
        Ok(None)
    }

    fn open_row(
        &self,
        row: RowId,
        before: &mut dyn FnMut() -> bool,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Box<dyn RawVectorReverseMembershipCursor>> {
        if !before() {
            return Err(Error::ReadCancelled);
        }
        Ok(Box::new(self.cursor_admitted(row, owner)?))
    }
}

/// Creates the durable source pair for a partition first written after open.
pub(crate) struct DurablePartitionSourceFactory {
    pub(crate) published_root: PublishedMembershipRoot,
    pub(crate) persistence: Arc<RedbPersistence>,
    pub(crate) loader: Arc<
        dyn Fn(
                &VectorPartitionRef,
                contextdb_vector::VectorCopyAdmission,
                contextdb_vector::VectorCopyAdmission,
            ) -> Result<Arc<dyn DormantRawVectorLoader>>
            + Send
            + Sync,
    >,
    // The layout captured by loader shares this retained construction owner.
    _copy_owner: Arc<contextdb_vector::VectorCopyAdmission>,

    // This last field preserves the original accountant/epoch on derived owners.
    pub(crate) accountant: StoreAccountantHandle,
}

impl contextdb_vector::store::DurableRawPartitionFactory for DurablePartitionSourceFactory {
    fn visit_retained_copy_credits(
        &self,
        caller: &dyn contextdb_vector::MemoryBudget,
        visit: &mut dyn FnMut(usize, usize, bool),
    ) {
        contextdb_vector::VectorCopyAdmission::visit_retained_copy_credit(
            &self._copy_owner,
            caller,
            visit,
        );
    }

    fn create(
        &self,
        partition: &VectorPartitionRef,
        mut workspace: contextdb_vector::VectorCopyAdmission,
        mut owner: contextdb_vector::VectorCopyAdmission,
    ) -> Result<(
        Arc<dyn RawVectorMembershipSource>,
        Arc<dyn DormantRawVectorLoader>,
    )> {
        let loader_owner = owner.scratch();
        let loader_workspace = workspace.scratch();
        workspace.reserve(
            std::mem::size_of::<DurableMembershipSource>()
                .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
                .saturating_add(4 * std::mem::size_of::<usize>())
                .saturating_add(partition.index.table.len())
                .saturating_add(partition.index.column.len())
                .saturating_add(partition.partition_key.estimated_bytes()),
        )?;
        owner.reserve(
            std::mem::size_of::<DurableMembershipSource>()
                .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
                .saturating_add(4 * std::mem::size_of::<usize>())
                .saturating_add(partition.index.table.len())
                .saturating_add(partition.index.column.len())
                .saturating_add(partition.partition_key.estimated_bytes()),
        )?;
        let loader = (self.loader)(partition, loader_workspace, loader_owner)?;
        let source = Arc::new(DurableMembershipSource {
            owner: MembershipOwner::Writable(self.persistence.clone()),
            published_root: Some(self.published_root.clone()),
            partition: partition.clone(),
            summary: RawVectorMembershipSummary::default(),
            _copy_owner: Some(Arc::new(owner)),
            accountant: self.accountant.clone(),
        });
        Ok((source, loader))
    }
}

/// Builds an admitted raw-body loader for one partition of a declared layout.
pub(crate) type DurableLoaderBuilder = Arc<
    dyn Fn(
            &VectorPartitionRef,
            &contextdb_vector::VectorIndexLayout,
            contextdb_vector::VectorCopyAdmission,
            contextdb_vector::VectorCopyAdmission,
        ) -> Result<Arc<dyn DormantRawVectorLoader>>
        + Send
        + Sync,
>;

// Keep the complete captured layout alive before releasing its shared
// construction charge, including when the loader closure is cloned alone.
struct AdmittedLayoutLoader {
    loader: DurableLoaderBuilder,
    layout: contextdb_vector::VectorIndexLayout,
    _owner: Arc<contextdb_vector::VectorCopyAdmission>,
}
impl AdmittedLayoutLoader {
    fn load(
        &self,
        partition: &VectorPartitionRef,
        workspace: contextdb_vector::VectorCopyAdmission,
        owner: contextdb_vector::VectorCopyAdmission,
    ) -> Result<Arc<dyn DormantRawVectorLoader>> {
        (self.loader)(partition, &self.layout, workspace, owner)
    }
}

/// Creates the durable sources of a vector column declared after open.
pub(crate) struct DurableColumnSourceFactory {
    pub(crate) published_root: PublishedMembershipRoot,
    pub(crate) persistence: Arc<RedbPersistence>,
    pub(crate) loader: DurableLoaderBuilder,
    pub(crate) _copy_owner: Arc<contextdb_vector::VectorCopyAdmission>,

    // This last field preserves the original accountant/epoch on derived owners.
    pub(crate) accountant: StoreAccountantHandle,
}

impl DurableColumnSourceFactory {
    pub(crate) fn column_sources(
        &self,
        index: &VectorIndexRef,
        layout: &contextdb_vector::VectorIndexLayout,
        mut workspace: contextdb_vector::VectorCopyAdmission,
        mut owner: contextdb_vector::VectorCopyAdmission,
    ) -> Result<(
        Arc<dyn RawVectorReverseMembershipSource>,
        Arc<dyn contextdb_vector::store::DurableRawPartitionFactory>,
    )> {
        // The reverse source can be replaced independently of the factory
        // and captured layout. Admit their distinct credits before any clone,
        // so publishing a rename returns the old reverse source's funding.
        let reverse_bytes = std::mem::size_of::<DurableReverseMembershipSource>()
            .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
            .saturating_add(4 * std::mem::size_of::<usize>())
            .saturating_add(index.table.len())
            .saturating_add(index.column.len());
        let factory_bytes = std::mem::size_of::<DurablePartitionSourceFactory>()
            .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
            .saturating_add(6 * std::mem::size_of::<usize>())
            .saturating_add(std::mem::size_of::<DurableLoaderBuilder>())
            .saturating_add(layout.estimated_bytes().saturating_mul(2));
        workspace.reserve(reverse_bytes.saturating_add(factory_bytes))?;
        let mut reverse_owner = owner.scratch();
        reverse_owner.reserve(reverse_bytes)?;
        owner.reserve(factory_bytes)?;
        let loader = self.loader.clone();
        let layout = layout.clone();
        let copy_owner = Arc::new(owner);
        #[cfg(all(test, feature = "test-seams"))]
        crate::database::d1_initialized_control_proofs::observe_partition_credit_for_test();
        let reverse = Arc::new(DurableReverseMembershipSource {
            owner: MembershipOwner::Writable(self.persistence.clone()),
            index: index.clone(),
            _copy_owner: Some(Arc::new(reverse_owner)),
            accountant: self.accountant.clone(),
        });
        #[cfg(all(test, feature = "test-seams"))]
        crate::database::d1_initialized_control_proofs::observe_reverse_for_test(&reverse);
        let admitted_loader = AdmittedLayoutLoader {
            loader,
            layout,
            _owner: copy_owner.clone(),
        };
        let partitions = Arc::new(DurablePartitionSourceFactory {
            published_root: self.published_root.clone(),
            persistence: self.persistence.clone(),
            loader: Arc::new(move |partition, workspace, owner| {
                admitted_loader.load(partition, workspace, owner)
            }),
            _copy_owner: copy_owner,
            accountant: self.accountant.clone(),
        });
        Ok((reverse, partitions))
    }
}

impl contextdb_vector::store::DurableRawColumnFactory for DurableColumnSourceFactory {
    fn visit_retained_copy_credits(
        &self,
        caller: &dyn contextdb_vector::MemoryBudget,
        visit: &mut dyn FnMut(usize, usize, bool),
    ) {
        contextdb_vector::VectorCopyAdmission::visit_retained_copy_credit(
            &self._copy_owner,
            caller,
            visit,
        );
    }

    fn create(
        &self,
        index: &VectorIndexRef,
        layout: &contextdb_vector::VectorIndexLayout,
        workspace: contextdb_vector::VectorCopyAdmission,
        owner: contextdb_vector::VectorCopyAdmission,
    ) -> Result<(
        Arc<dyn RawVectorReverseMembershipSource>,
        Arc<dyn contextdb_vector::store::DurableRawPartitionFactory>,
    )> {
        self.column_sources(index, layout, workspace, owner)
    }
}

pub(super) fn membership_partitions_in_read(
    read: &redb::ReadTransaction,
    owner: &mut contextdb_vector::VectorCopyAdmission,
) -> Result<Vec<(VectorPartitionRef, RawVectorMembershipSummary)>> {
    vector_membership_pages::validate_in_read(read)?;
    let summaries = match read.open_table(vector_membership_pages::SUMMARY) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    let mut partitions = Vec::new();
    for item in summaries.iter().map_err(RedbPersistence::storage_error)? {
        let (key, value) = item.map_err(vector_membership_pages::metadata_error)?;
        if key.value().is_empty() {
            continue;
        }
        let mut scratch = owner.scratch();
        scratch.reserve(value.value().len().saturating_mul(2).saturating_add(
            std::mem::size_of::<vector_membership_pages::MembershipSummary>(),
        ))?;
        owner.reserve(std::mem::size_of::<(
            VectorPartitionRef,
            RawVectorMembershipSummary,
        )>())?;
        let summary = vector_membership_pages::decode_summary(key.value(), value.value())?;
        partitions.reserve_exact(1);
        partitions.push((
            vector_membership_pages::partition_from_prefix_admitted(key.value(), owner)?,
            summary_counts(&summary)?,
        ));
    }
    Ok(partitions)
}

impl RedbPersistence {
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn durable_vector_storage_inventory_for_test(
        &self,
        partition: &VectorPartitionRef,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<crate::database::DurableVectorStorageInventoryForTest> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let count = |definition| -> Result<usize> {
                let rows = match read.open_table(definition) {
                    Ok(table) => table.len().map_err(Self::storage_error)?,
                    Err(redb::TableError::TableDoesNotExist(_)) => 0,
                    Err(error) => return Err(Self::storage_error(error)),
                };
                usize::try_from(rows)
                    .map_err(|_| Error::Other("vector inventory count overflow".into()))
            };
            let mut partition_summaries = 0usize;
            let mut empty_partition_summaries = 0usize;
            match read.open_table(vector_membership_pages::SUMMARY) {
                Ok(summaries) => {
                    for item in summaries.iter().map_err(Self::storage_error)? {
                        let (key, value) = item.map_err(Self::storage_error)?;
                        if key.value().is_empty() {
                            continue;
                        }
                        let mut scratch = owner.scratch();
                        scratch.reserve(value.value().len().saturating_mul(2).saturating_add(
                            std::mem::size_of::<vector_membership_pages::MembershipSummary>(),
                        ))?;
                        let route = vector_membership_pages::partition_from_prefix_admitted(
                            key.value(),
                            &mut scratch,
                        )?;
                        let summary =
                            vector_membership_pages::decode_summary(key.value(), value.value())?;
                        if !route.partition_key.is_unpartitioned() {
                            partition_summaries += 1;
                            empty_partition_summaries += usize::from(summary.entries == 0);
                        }
                        drop((route, summary));
                    }
                }
                Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(error) => return Err(Self::storage_error(error)),
            }
            owner.reserve(
                partition
                    .index
                    .table
                    .len()
                    .saturating_add(partition.index.column.len())
                    .saturating_add(partition.partition_key.estimated_bytes())
                    .saturating_mul(8)
                    .saturating_add(128),
            )?;
            let key = Self::vector_partition_catalog_key_from_parts(
                &partition.index,
                &partition.partition_key,
            );
            let catalog_frontiers = match read.open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
            {
                Ok(catalogs) => match catalogs
                    .get_bounded(key.as_slice())
                    .map_err(Self::storage_error)?
                {
                    Some(value) => {
                        let catalog: VectorPartitionGenerationCatalogRecord = self
                            .decode_expected_vector_partition_metadata(
                                VectorPartitionRecordKind::GenerationCatalog,
                                value.value(),
                                &partition.index,
                                &partition.partition_key,
                            )?;
                        if Self::vector_partition_catalog_key(&catalog) != key {
                            return Err(self.vector_partition_corruption(
                                "generation catalog key does not match its checksummed payload",
                            ));
                        }
                        Some((
                            catalog.base.covered_tx,
                            catalog.change.map(|change| change.covered_tx),
                        ))
                    }
                    None => None,
                },
                Err(redb::TableError::TableDoesNotExist(_)) => None,
                Err(error) => return Err(Self::storage_error(error)),
            };
            let paged_cleanup_entries = match read.open_table(vector_paged::CLEANUP) {
                Ok(table) => usize::try_from(table.len().map_err(Self::storage_error)?)
                    .map_err(|_| Error::Other("physical table count does not fit usize".into()))?,
                Err(redb::TableError::TableDoesNotExist(_)) => 0,
                Err(error) => return Err(Self::storage_error(error)),
            };
            Ok(crate::database::DurableVectorStorageInventoryForTest {
                partition_summaries,
                empty_partition_summaries,
                catalog_frontiers,
                catalogs: count(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)?,
                base_generations: count(VECTOR_PARTITION_BASE_GENERATION_TABLE)?,
                change_generations: count(VECTOR_PARTITION_CHANGE_GENERATION_TABLE)?,
                graph_pages: count(VECTOR_PARTITION_GRAPH_PAGE_TABLE)?,
                paged_pages: count(vector_paged::PAGES)?,
                paged_restart_records: count(vector_paged::BUILDS)?,
                paged_cleanup_entries,
            })
        })
    }

    pub(crate) fn durable_membership_partitions(
        &self,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Vec<(VectorPartitionRef, RawVectorMembershipSummary)>> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            membership_partitions_in_read(&read, owner)
        })
    }
}

#[cfg(all(test, feature = "test-seams"))]
impl DurableMembershipSource {
    pub(crate) fn d1_initialized_observation_for_test(&self) -> (bool, usize) {
        (self.accountant.is_writer(), self.partition.index.table.as_ptr() as usize)
    }
}
#[cfg(all(test, feature = "test-seams"))]
impl DurableReverseMembershipSource {
    pub(crate) fn d1_initialized_observation_for_test(&self) -> (bool, usize) {
        (self.accountant.is_writer(), self.index.table.as_ptr() as usize)
    }
}
