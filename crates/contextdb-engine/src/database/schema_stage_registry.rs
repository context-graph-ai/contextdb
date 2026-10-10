//! The three schema-stage registries own their physical backing independently
//! of logical entries. Reservation tokens keep occupied rollback/publication
//! cells and the original accountant alive across work outside the mutex.
use super::*;
use crate::memory_accounting::{ControlCredit, PendingControlRefund, StoreAccountantHandle};
use std::alloc::Layout;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;

// Rust 1.94.1 std uses checksum-pinned hashbrown 0.16.1. This selection
// follows its control/group/mod.rs (std enables nightly, including LSX).
const GROUP_WIDTH: usize = if cfg!(all(
    target_feature = "sse2", any(target_arch = "x86", target_arch = "x86_64"), not(miri)
)) || cfg!(all(target_arch = "aarch64", target_feature = "neon", target_endian = "little", not(miri)))
    || cfg!(all(target_arch = "loongarch64", target_feature = "lsx", not(miri))) {
    16
} else if cfg!(any(target_pointer_width = "64", target_arch = "aarch64", target_arch = "x86_64", target_arch = "wasm32")) {
    8
} else { 4 };

// CDB127 private static-map extension. These are the exact pre2 family
// admission labels/hints for backing growth. New private constructor doors
// inherit their family labels; PRE2 had no constructor admission.
pub(super) struct Attribution { subsystem: &'static str, operation: &'static str, hint: &'static str }
const PENDING_ATTRIBUTION: Attribution = Attribution {
    subsystem: "ddl", operation: "stage_transactional_schema",
    hint: "Reduce the schema transaction size or raise MEMORY_LIMIT before retrying.",
};
const LOCAL_ATTRIBUTION: Attribution = Attribution {
    subsystem: "ddl", operation: "commit_transactional_schema",
    hint: "Reduce the schema transaction size or raise MEMORY_LIMIT before retrying.",
};
const RECEIVED_ATTRIBUTION: Attribution = Attribution {
    subsystem: "sync", operation: "prepare_received_schema_replacement",
    hint: "Reduce the replicated schema/data working set or raise MEMORY_LIMIT before receiving schema.",
};
pub(super) trait RegistryFamily {
    fn attribution() -> &'static Attribution;
    fn open_residue_operation() -> &'static str { "pending_registry_open_residue" }
}
impl RegistryFamily for Pre2PendingLocalSchemaStage { fn attribution() -> &'static Attribution { &PENDING_ATTRIBUTION } }
impl RegistryFamily for Pre2ReceivedSchemaStage {
    fn attribution() -> &'static Attribution { &RECEIVED_ATTRIBUTION }
    fn open_residue_operation() -> &'static str { "received_registry_open_residue" }
}

fn capacity_error() -> Error {
    Error::Other("schema stage registry allocation capacity overflow".to_string())
}
fn bucket_count<T>(capacity: usize) -> Result<usize> {
    if capacity < 15 {
        let minimum = match (GROUP_WIDTH, std::mem::size_of::<T>()) {
            (16, 0..=1) => 14,
            (16, 2..=3) | (8, 0..=1) => 7,
            _ => 3,
        };
        let capacity = capacity.max(minimum);
        Ok(if capacity < 4 { 4 } else if capacity < 8 { 8 } else { 16 })
    } else {
        (capacity.checked_mul(8).ok_or_else(capacity_error)? / 7)
            .checked_next_power_of_two().ok_or_else(capacity_error)
    }
}
fn bucket_capacity(buckets: usize) -> usize {
    if buckets == 0 { 0 } else if buckets <= 8 { buckets - 1 } else { (buckets / 8) * 7 }
}
fn bucket_layout<T>(buckets: usize) -> Result<Layout> {
    if buckets == 0 { return Ok(Layout::new::<()>()); }
    let alignment = std::mem::align_of::<T>().max(GROUP_WIDTH);
    let offset = std::mem::size_of::<T>().checked_mul(buckets)
        .and_then(|bytes| bytes.checked_add(alignment - 1)).ok_or_else(capacity_error)?
        & !(alignment - 1);
    let bytes = offset.checked_add(buckets).and_then(|bytes| bytes.checked_add(GROUP_WIDTH))
        .ok_or_else(capacity_error)?;
    if bytes > isize::MAX as usize - (alignment - 1) { return Err(capacity_error()); }
    Layout::from_size_align(bytes, alignment).map_err(|_| capacity_error())
}

/// No raw map access or Deref is exposed: every new physical key enters the
/// one checked reserve protocol, and every free enters advancing cleanup.
pub(crate) struct Backing<K: Eq + Hash + Copy, V, P> {
    map: ManuallyDrop<HashMap<K, V>>,
    buckets: usize,
    backing: ManuallyDrop<Option<ControlCredit>>,
    accountant: ManuallyDrop<StoreAccountantHandle>,
    control: ManuallyDrop<Option<ControlCredit>>,
    baseline: PhantomData<P>,
    attribution: &'static Attribution,
}
impl<K: Eq + Hash + Copy, V, P> Backing<K, V, P> {
    fn new(accountant: StoreAccountantHandle, control: ControlCredit, attribution: &'static Attribution) -> Self {
        Self {
            map: ManuallyDrop::new(HashMap::new()), buckets: 0,
            backing: ManuallyDrop::new(None),
            accountant: ManuallyDrop::new(accountant),
            control: ManuallyDrop::new(Some(control)), baseline: PhantomData, attribution,
        }
    }
    fn reserve_key(&mut self, key: &K) -> Result<()> {
        if self.map.contains_key(key) { return Ok(()); }
        let free = self.map.capacity().checked_sub(self.map.len()).ok_or_else(capacity_error)?;
        if free >= 1 {
            self.map.try_reserve(1).map_err(|error| Error::Other(format!("schema stage registry allocation failed: {error}")))?;
            return Ok(());
        }
        let next_len = self.map.len().checked_add(1).ok_or_else(capacity_error)?;
        let full = bucket_capacity(self.buckets);
        if next_len <= full / 2 {
            // Native try_reserve rehashes tombstones in place on this branch.
            self.map.try_reserve(1).map_err(|error| Error::Other(format!("schema stage registry allocation failed: {error}")))?;
            return Ok(());
        }
        let required = next_len.max(full.checked_add(1).ok_or_else(capacity_error)?);
        let new_buckets = bucket_count::<(K, V)>(required)?;
        let final_layout = bucket_layout::<(K, V)>(new_buckets)?;
        let original_layout = bucket_layout::<(K, P)>(new_buckets)?;
        let correction = final_layout.size().checked_sub(original_layout.size()).ok_or_else(capacity_error)?;
        // No old liability is released while the separate new allocation is
        // live. Hashbrown frees old backing before successful reserve returns.
        let new_credit = ControlCredit::admit_from_handle(&self.accountant, correction,
            self.attribution.subsystem, self.attribution.operation, self.attribution.hint)?;
        self.map.try_reserve(1).map_err(|error| Error::Other(format!("schema stage registry allocation failed: {error}")))?;
        self.buckets = new_buckets;
        let old = self.backing.replace(new_credit);
        drop(old);
        Ok(())
    }
    /// An empty registry holds nothing: free its physical backing first, then
    /// refund the backing credit, so the accountant returns to the registry's
    /// fixed control once every stage has left (CDB-127 ledger promise).
    fn release_if_empty(&mut self) {
        if !self.map.is_empty() || self.buckets == 0 && self.backing.is_none() {
            return;
        }
        drop(std::mem::replace(&mut *self.map, HashMap::new()));
        self.buckets = 0;
        drop(self.backing.take());
    }
}
struct BackingCleanup<K: Eq + Hash + Copy, V, P> {
    source: *mut Backing<K, V, P>,
    next: u8,
    refund: Option<PendingControlRefund>,
}
impl<K: Eq + Hash + Copy, V, P> BackingCleanup<K, V, P> {
    fn finish(&mut self) {
        // Remove first: a payload panic leaves the next entry for this guard's
        // destructor. The empty map's destructor cannot leak its allocation.
        unsafe {
            if self.next == 0 {
                while let Some(key) = (*self.source).map.keys().next().copied() {
                    let removed = (*self.source).map.remove(&key);
                    drop(removed);
                }
                self.next = 1;
            }
            while self.next < 5 {
                let step = self.next; self.next += 1;
                match step {
                    1 => ManuallyDrop::drop(&mut (*self.source).map),
                    2 => ManuallyDrop::drop(&mut (*self.source).backing),
                    3 => ManuallyDrop::drop(&mut (*self.source).accountant),
                    4 => drop(self.refund.take()),
                    _ => unreachable!(),
                }
            }
        }
    }
}
impl<K: Eq + Hash + Copy, V, P> Drop for BackingCleanup<K, V, P> {
    fn drop(&mut self) { self.finish(); }
}
impl<K: Eq + Hash + Copy, V, P> Drop for Backing<K, V, P> {
    fn drop(&mut self) {
        let refund = self.control.take().map(ControlCredit::prepare);
        let mut cleanup = BackingCleanup { source: self, next: 0, refund };
        cleanup.finish();
    }
}

pub(crate) struct LocalRegistry(Backing<Lsn, LocalSchemaStage, Pre2LocalSchemaStage>);
impl LocalRegistry {
    pub(super) fn new_arc(accountant: &Arc<MemoryAccountant>) -> Result<Arc<Mutex<Self>>> {
        let final_layout = crate::memory_accounting::writer_adapter_type_layout::<Mutex<Self>>();
        let old_layout = crate::memory_accounting::writer_adapter_type_layout::<Mutex<HashMap<Lsn, Pre2LocalSchemaStage>>>();
        let bytes = final_layout.size().checked_sub(old_layout.size()).ok_or_else(capacity_error)?;
        let accountant = StoreAccountantHandle::new(accountant.clone());
        let attribution = &LOCAL_ATTRIBUTION;
        let control = if accountant.is_writer() {
            ControlCredit::admit_open_residue(&accountant, bytes, "local_registry_open_residue")?
        } else {
            ControlCredit::admit_from_handle(&accountant, bytes,
                attribution.subsystem, attribution.operation, attribution.hint)?
        };
        Ok(Arc::new(Mutex::new(Self(Backing::new(accountant, control, attribution)))))
    }
    pub(crate) fn insert(&mut self, key: Lsn, value: LocalSchemaStage) -> Result<Option<LocalSchemaStage>> {
        self.0.reserve_key(&key)?;
        Ok(self.0.map.insert(key, value))
    }
    pub(crate) fn get(&self, key: &Lsn) -> Option<&LocalSchemaStage> { self.0.map.get(key) }
    pub(crate) fn contains_key(&self, key: &Lsn) -> bool { self.0.map.contains_key(key) }
    pub(crate) fn remove(&mut self, key: &Lsn) -> Option<LocalSchemaStage> {
        let removed = self.0.map.remove(key);
        self.0.release_if_empty();
        removed
    }
    pub(crate) fn len(&self) -> usize { self.0.map.len() }
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool { self.0.map.is_empty() }
    pub(crate) fn clear(&mut self) {
        // Removal advances before payload destruction; the backing and its
        // liability are released once the map is empty.
        while let Some(key) = self.0.map.keys().next().copied() { drop(self.0.map.remove(&key)); }
        self.0.release_if_empty();
    }
}

pub(crate) struct Cell<V> { stage: Option<V>, tokens: usize }
pub(crate) struct OccupiedRegistry<K: Eq + Hash + Copy, V, P>(Backing<K, Cell<V>, P>);
impl<K: Eq + Hash + Copy, V, P> OccupiedRegistry<K, V, P> {
    pub(super) fn new_arc(accountant: &Arc<MemoryAccountant>) -> Result<Arc<Mutex<Self>>> where P: RegistryFamily {
        let final_layout = crate::memory_accounting::writer_adapter_type_layout::<Mutex<Self>>();
        let old_layout = crate::memory_accounting::writer_adapter_type_layout::<Mutex<HashMap<K, P>>>();
        let bytes = final_layout.size().checked_sub(old_layout.size()).ok_or_else(capacity_error)?;
        let accountant = StoreAccountantHandle::new(accountant.clone());
        let attribution = P::attribution();
        let control = if accountant.is_writer() {
            // Each fixed occupied-registry constructor is 104 B of named open
            // residue. Map backing growth retains its existing writer charge.
            ControlCredit::admit_open_residue(&accountant, bytes, P::open_residue_operation())?
        } else {
            ControlCredit::admit_from_handle(&accountant, bytes,
                attribution.subsystem, attribution.operation, attribution.hint)?
        };
        Ok(Arc::new(Mutex::new(Self(Backing::new(accountant, control, attribution)))))
    }
    pub(crate) fn get(&self, key: &K) -> Option<&V> { self.0.map.get(key).and_then(|cell| cell.stage.as_ref()) }
    pub(crate) fn contains_key(&self, key: &K) -> bool { self.get(key).is_some() }
    pub(crate) fn len(&self) -> usize { self.0.map.values().filter(|cell| cell.stage.is_some()).count() }
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool { self.len() == 0 }
    pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
        let cell = self.0.map.get_mut(key)?;
        let stage = cell.stage.take();
        if cell.tokens == 0 { self.0.map.remove(key); }
        self.0.release_if_empty();
        stage
    }
    pub(crate) fn clear(&mut self) {
        // Preserve all occupied cells and never allocate a collection of keys.
        loop {
            let key = self.0.map.iter().find_map(|(key, cell)| cell.stage.is_some().then_some(*key));
            let Some(key) = key else { break; };
            drop(self.remove(&key));
        }
        self.0.map.retain(|_, cell| cell.tokens != 0);
        self.0.release_if_empty();
    }
    fn acquire(&mut self, key: K, detach: bool) -> Result<Option<V>> {
        if let Some(cell) = self.0.map.get_mut(&key) {
            let count = cell.tokens.checked_add(1).ok_or_else(capacity_error)?;
            cell.tokens = count;
            return Ok(if detach { cell.stage.take() } else { None });
        }
        self.0.reserve_key(&key)?;
        self.0.map.insert(key, Cell { stage: None, tokens: 1 });
        Ok(None)
    }
    fn finish(&mut self, key: K, stage: Option<V>) -> Option<V> {
        let cell = self.0.map.get_mut(&key).expect("live token retains its physical cell");
        let displaced = if let Some(stage) = stage { cell.stage.replace(stage) } else { None };
        cell.tokens = cell.tokens.checked_sub(1).expect("live token owns one occupancy");
        if cell.tokens == 0 && cell.stage.is_none() { self.0.map.remove(&key); }
        self.0.release_if_empty();
        displaced
    }
}

/// Stack-only occupancy ownership. Its Arc prevents final registry teardown
/// while any outside-lock operation still owns a restoration/publication slot.
pub(crate) struct Token<K: Eq + Hash + Copy, V, P> {
    registry: Arc<Mutex<OccupiedRegistry<K, V, P>>>, key: K, armed: bool,
}
impl<K: Eq + Hash + Copy, V, P> Token<K, V, P> {
    pub(super) fn detach_existing(registry: &Arc<Mutex<OccupiedRegistry<K, V, P>>>, key: K) -> Result<Option<(Self, V)>> {
        let mut locked = registry.lock();
        if !locked.contains_key(&key) { return Ok(None); }
        let stage = locked.acquire(key, true)?.expect("visible stage was detached under the same lock");
        Ok(Some((Self { registry: registry.clone(), key, armed: true }, stage)))
    }
    pub(super) fn reserve(registry: &Arc<Mutex<OccupiedRegistry<K, V, P>>>, key: K) -> Result<Self> {
        registry.lock().acquire(key, false)?;
        Ok(Self { registry: registry.clone(), key, armed: true })
    }
    pub(super) fn publish(mut self, stage: V) {
        let displaced = self.registry.lock().finish(self.key, Some(stage));
        self.armed = false;
        drop(displaced);
    }
}
impl<K: Eq + Hash + Copy, V, P> Drop for Token<K, V, P> {
    fn drop(&mut self) {
        if self.armed {
            let displaced = self.registry.lock().finish(self.key, None);
            self.armed = false;
            drop(displaced);
        }
    }
}

pub(super) type PendingRegistry = OccupiedRegistry<TxId, PendingLocalSchemaStage, Pre2PendingLocalSchemaStage>;
pub(super) type PendingToken = Token<TxId, PendingLocalSchemaStage, Pre2PendingLocalSchemaStage>;
pub(crate) type ReceivedRegistry = OccupiedRegistry<Lsn, ReceivedSchemaStage, Pre2ReceivedSchemaStage>;
pub(super) type ReceivedToken = Token<Lsn, ReceivedSchemaStage, Pre2ReceivedSchemaStage>;

pub(super) struct LocalRegistration { registry: Arc<Mutex<LocalRegistry>>, lsn: Lsn, armed: bool }
impl LocalRegistration {
    pub(super) fn new(registry: &Arc<Mutex<LocalRegistry>>, lsn: Lsn) -> Self {
        Self { registry: registry.clone(), lsn, armed: true }
    }
    pub(super) fn ownership_transferred(&mut self) { self.armed = false; }
}
impl Drop for LocalRegistration {
    fn drop(&mut self) {
        if self.armed { let removed = self.registry.lock().remove(&self.lsn); drop(removed); }
    }
}

// Lawful field-for-field pre2 value representations. These are never
// constructed or allocated: only their compiler layouts enter same-bucket
// subtraction. Database and public child layouts retain the pre2 contract;
// independent pre2/final layout receipts must verify that prerequisite.
#[allow(dead_code)]
struct Pre2TemporaryMemoryReservation {
    accountant: Arc<MemoryAccountant>,
    bytes: usize,
}

#[allow(dead_code)]
pub(crate) struct Pre2PreparedMemorySwap {
    accountant: Arc<MemoryAccountant>,
    reserved_positive_delta: usize,
    release_after_swap: usize,
    /// What each row version STAGED in a still-open transaction is admitted
    /// for once this swap's shape is the live one.
    ///
    /// The re-pricing has to land exactly when the shape does. Written before
    /// the publication, a change that is then refused or fails to persist
    /// leaves every staged row recorded at a figure nothing charged, and the
    /// release hands back more than admission took.
    staged_admissions: Option<StagedAdmissionRepricing>,
}

#[allow(dead_code)]
pub(super) struct Pre2PendingLocalSchemaStage {
    working: Pre2Database,
    base_tables: HashMap<String, TableMeta>,
    base_event_bus: event_bus::EventBusDefinitions,
    base_triggers: BTreeMap<String, TriggerDeclaration>,
    base_table_generations: HashMap<String, u64>,
    temporary_memory: Pre2TemporaryMemoryReservation,
}

#[allow(dead_code)]
pub(crate) struct Pre2LocalSchemaStage {
    relational: Vec<TableProjection>,
    affected_tables: HashSet<String>,
    dag_edge_types: PreparedDagEdgeTypes,
    rank_formulas: HashMap<(String, String), Arc<RankFormula>>,
    vector: Option<contextdb_vector::PreparedVectorTablePublication>,
    event_bus: Option<event_bus::PreparedEventBusPublication>,
    _queue_mutation_lease: Option<event_bus::QueueMutationLease>,
    trigger: trigger::PreparedTriggerPublication,
    table_generations: Vec<(String, u64)>,
    ddl_generations: Vec<(DdlGenerationKey, DdlGenerationValue)>,
    ddl_log: Vec<(Lsn, DdlChange)>,
    memory_swap: Pre2PreparedMemorySwap,
    _temporary_memory: Pre2TemporaryMemoryReservation,
    pub(crate) durable_projection: LocalSchemaPersistenceProjection,
    _vector_copy_owner: contextdb_vector::VectorCopyAdmission,
}

#[allow(dead_code)]
pub(crate) struct Pre2ReceivedSchemaStage {
    custody_memory: Option<custody_store::CustodyDelta>,
    source_order: Vec<ReceivedSchemaOrderEntry>,
    relational: Vec<TableProjection>,
    affected_tables: HashSet<String>,
    graph: Option<PreparedGraphPublication>,
    affected_graph_edge_types: HashSet<String>,
    dag_edge_types: Option<PreparedDagEdgeTypes>,
    rank_formulas: Option<HashMap<(String, String), Arc<RankFormula>>>,
    vector: Option<contextdb_vector::PreparedVectorTablePublication>,
    event_bus: Option<event_bus::PreparedEventBusPublication>,
    /// Acquired while the received-schema unit is still being prepared and
    /// retained through its matching durable config transaction plus queue
    /// publication, so a purge cannot split that queue-definition change.
    queue_mutation_lease: Option<event_bus::QueueMutationLease>,
    trigger: Option<trigger::PreparedTriggerPublication>,
    pub(crate) sink_events: Vec<event_bus::PreparedSinkEvent>,
    next_sink_queue_id: u64,
    pub(crate) trigger_audits: Vec<(u64, TriggerAuditEntry)>,
    next_trigger_audit_index: u64,
    mirrors: ReceivedSchemaMemoryMirrors,
    memory_swap: Pre2PreparedMemorySwap,
    /// The detached projection owns row allocation while it is being
    /// adjudicated.  It becomes visible to the live allocator only after the
    /// matching Redb transaction commits and the memory image is published.
    next_row_id: RowId,
    persistence: ReceivedSchemaPersistencePayload,
    /// The detached adjudicator's exact outcome. It stays private until the
    /// staged receiver commit has crossed durability; test-only receipt
    /// observation copies it immediately before this stage is consumed.
    apply_result: ApplyResult,
    /// Absent for an unattested unit: a manifest is the replay identity of a
    /// SIGNED migration, and a unit with no provenance has nothing to be
    /// idempotent against.
    received_schema_manifest: Option<(String, DurableReceivedSchemaManifest)>,
    in_memory_lineage_delta: InMemoryLineageDelta,
    pub(crate) write_set: WriteSet,
    pub(crate) durable_projection: ReceivedSchemaPersistenceProjection,
    pub(crate) structurally_dropped_tables: HashSet<String>,
    pub(crate) change_log_entries: Vec<ChangeLogEntry>,
    operational_memberships: Option<contextdb_relational::store::PreparedMembershipBatch>,
    metadata_copy_charges: Vec<StagedCopyCharge>,
    _vector_copy_owner: contextdb_vector::VectorCopyAdmission,
}

#[cfg(all(test, feature = "test-seams"))]
pub(super) fn mapped_rows_for_test() {
    use crate::memory_accounting::WriterBudget;
    use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
    assert_eq!(std::mem::size_of::<Database>(), std::mem::size_of::<Pre2Database>(), "the Database field representation remains pre2 sized");
    assert_eq!(std::mem::align_of::<Database>(), std::mem::align_of::<Pre2Database>());
    let owner = Arc::new(MemoryAccountant::no_limit());
    let writer = WriterBudget::from_accountant(owner.clone()).unwrap();
    let baseline = owner.usage().used;
    let counter = owner.writer_registration_observation_for_test().4;
    macro_rules! constructor {
        ($registry:ty, $original:ty, $name:literal) => {{
            oracle::last_request_for_test(true);
            let (registry, _, _) = oracle::measured(|| <$registry>::new_arc(&owner).unwrap());
            let request = oracle::last_request_for_test(true);
            let final_layout = crate::memory_accounting::writer_adapter_type_layout::<Mutex<$registry>>();
            let original = crate::memory_accounting::writer_adapter_type_layout::<Mutex<$original>>();
            let correction = final_layout.size() - original.size();
            assert_eq!(request, (final_layout.size(), final_layout.align(), 1));
            assert_eq!(owner.usage().used, baseline + correction);
            let retained = registry.clone();
            drop(registry);
            assert_eq!(owner.usage().used, baseline + correction);
            drop(retained);
            assert_eq!(owner.usage().used, baseline);
            owner.set_budget(Some(baseline)).unwrap();
            assert!(<$registry>::new_arc(&owner).is_err());
            assert_eq!(owner.usage().used, baseline);
            owner.set_budget(None).unwrap();
            println!("CDB127_D2_ROW constructor={} actual_request={} align={} pre2_arc={} correction={} clone_single_debit=true refusal=true", $name, request.0, request.1, original.size(), correction);
        }};
    }
    constructor!(PendingRegistry, HashMap<TxId, Pre2PendingLocalSchemaStage>, "pending");
    constructor!(ReceivedRegistry, HashMap<Lsn, Pre2ReceivedSchemaStage>, "received");
    constructor!(LocalRegistry, HashMap<Lsn, Pre2LocalSchemaStage>, "local");
    constructor_custody_rows_for_test();
    family_refusal_literals_for_test();
    local_growth_for_test();
    occupied_populated_rows_for_test();
    local_reader_growth_provenance_for_test();
    tombstone_rehash_rows_for_test();
    received_interleavings_for_test();
    received_prepublication_refusal_and_cleanup_for_test();
    received_retired_construction_stage_witness_for_test();
    original_provenance_and_attribution_for_test();
    received_growth_for_test();

    // Real pending cells are valid reserved-but-unpublished production state;
    // no synthetic value type substitutes for their allocation layout.
    let registry = PendingRegistry::new_arc(&owner).unwrap();
    let control_baseline = owner.usage().used;
    let mut old_pointer = 0;
    let mut old_delta = 0;
    let mut tokens = Vec::with_capacity(64);
    for key in 0..64 {
        let old_buckets = registry.lock().0.buckets;
        if old_pointer != 0 { oracle::watch_registry_free_for_test(old_pointer, counter); }
        oracle::last_request_for_test(true);
        let (token, _, _) = oracle::measured(|| PendingToken::reserve(&registry, TxId(key)).unwrap());
        let request = oracle::last_request_for_test(true);
        let new_buckets = registry.lock().0.buckets;
        if new_buckets != old_buckets {
            let final_layout = bucket_layout::<(TxId, Cell<PendingLocalSchemaStage>)>(new_buckets).unwrap();
            let original_layout = bucket_layout::<(TxId, Pre2PendingLocalSchemaStage)>(new_buckets).unwrap();
            let new_delta = final_layout.size() - original_layout.size();
            assert_eq!(request, (final_layout.size(), final_layout.align(), 1));
            if old_pointer != 0 {
                assert_eq!(oracle::registry_free_observation_for_test(true),
                    (control_baseline + old_delta + new_delta, control_baseline + old_delta + new_delta, true));
            }
            old_pointer = oracle::last_request_pointer_for_test();
            old_delta = new_delta;
            assert_eq!(owner.usage().used, control_baseline + new_delta);
            println!("CDB127_D2_ROW pending_growth buckets={} actual_request={} pre2_same_buckets={} delta={} old_free_before_refund=true", new_buckets, request.0, original_layout.size(), new_delta);
        } else { assert_eq!(request.2, 0); oracle::registry_free_observation_for_test(true); }
        tokens.push(token);
    }
    assert_eq!(registry.lock().len(), 0, "physical placeholders stay logically hidden");
    oracle::watch_registry_free_for_test(old_pointer, counter);
    // CDB-127 ledger promise (fable-18 ruling, 2026-10-09): an emptied
    // registry frees its backing, then refunds it, when it empties.
    drop(tokens);
    assert_eq!(oracle::registry_free_observation_for_test(true),
        (control_baseline + old_delta, control_baseline + old_delta, true));
    assert_eq!(owner.usage().used, control_baseline, "an empty registry holds only its fixed control");
    let retained = registry.clone();
    drop(registry);
    assert_eq!(owner.usage().used, control_baseline);
    drop(retained);
    assert_eq!(owner.usage().used, baseline);
    drop(writer);
    assert_eq!(owner.usage().used, 0);
    assert_eq!(owner.underflow_count_for_test(), 0);
    println!("CDB127_D2_ROW pending_empty_released backing_free_before_refund_when_empty=true last_alias=true final_original_funder_zero=true");
}

#[cfg(all(test, feature = "test-seams"))]
fn received_growth_for_test() {
    use crate::memory_accounting::WriterBudget;
    use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
    let owner = Arc::new(MemoryAccountant::no_limit());
    let writer = WriterBudget::from_accountant(owner.clone()).unwrap();
    let baseline = owner.usage().used;
    let counter = owner.writer_registration_observation_for_test().4;
    // Real received cells are valid reserved-but-unpublished production state;
    // no synthetic value type substitutes for their allocation layout.
    let registry = ReceivedRegistry::new_arc(&owner).unwrap();
    let control_baseline = owner.usage().used;
    let mut old_pointer = 0;
    let mut old_delta = 0;
    let mut tokens = Vec::with_capacity(64);
    for key in 0..64 {
        let old_buckets = registry.lock().0.buckets;
        if old_pointer != 0 { oracle::watch_registry_free_for_test(old_pointer, counter); }
        oracle::last_request_for_test(true);
        let (token, _, _) = oracle::measured(|| ReceivedToken::reserve(&registry, Lsn(key)).unwrap());
        let request = oracle::last_request_for_test(true);
        let new_buckets = registry.lock().0.buckets;
        if new_buckets != old_buckets {
            let final_layout = bucket_layout::<(Lsn, Cell<ReceivedSchemaStage>)>(new_buckets).unwrap();
            let original_layout = bucket_layout::<(Lsn, Pre2ReceivedSchemaStage)>(new_buckets).unwrap();
            let new_delta = final_layout.size() - original_layout.size();
            assert_eq!(request, (final_layout.size(), final_layout.align(), 1));
            if old_pointer != 0 {
                assert_eq!(oracle::registry_free_observation_for_test(true),
                    (control_baseline + old_delta + new_delta, control_baseline + old_delta + new_delta, true));
            }
            old_pointer = oracle::last_request_pointer_for_test();
            old_delta = new_delta;
            assert_eq!(owner.usage().used, control_baseline + new_delta);
            println!("CDB127_D2_ROW received_growth buckets={} actual_request={} pre2_same_buckets={} delta={} old_free_before_refund=true", new_buckets, request.0, original_layout.size(), new_delta);
        } else { assert_eq!(request.2, 0); oracle::registry_free_observation_for_test(true); }
        tokens.push(token);
    }
    assert_eq!(registry.lock().len(), 0, "physical placeholders stay logically hidden");
    oracle::watch_registry_free_for_test(old_pointer, counter);
    // CDB-127 ledger promise (fable-18 ruling, 2026-10-09): an emptied
    // registry frees its backing, then refunds it, when it empties.
    drop(tokens);
    assert_eq!(oracle::registry_free_observation_for_test(true),
        (control_baseline + old_delta, control_baseline + old_delta, true));
    assert_eq!(owner.usage().used, control_baseline, "an empty registry holds only its fixed control");
    let retained = registry.clone();
    drop(registry);
    assert_eq!(owner.usage().used, control_baseline);
    drop(retained);
    assert_eq!(owner.usage().used, baseline);
    drop(writer);
    assert_eq!(owner.usage().used, 0);
    assert_eq!(owner.underflow_count_for_test(), 0);
    println!("CDB127_D2_ROW received_empty_released backing_free_before_refund_when_empty=true last_alias=true final_original_funder_zero=true");
}

#[cfg(all(test, feature = "test-seams"))]
fn original_provenance_and_attribution_for_test() {
    use crate::memory_accounting::WriterBudget;
    macro_rules! provenance {
        ($registry:ty, $attribution:expr, $name:literal, $growth:literal, $grow:expr) => {{
            let owner = Arc::new(MemoryAccountant::no_limit());
            let registry = <$registry>::new_arc(&owner).unwrap();
            let reader_floor = owner.usage().used;
            assert!(!registry.lock().0.accountant.is_writer());
            let writer = WriterBudget::from_accountant(owner.clone()).unwrap();
            let epoch = owner.writer_registration_observation_for_test().2;
            let receipt = owner.writer_fixed_ownership().2;
            let uses = owner.writer_registration_observation_for_test().3;
            ($grow)(&registry);
            assert!(!registry.lock().0.accountant.is_writer());
            assert_eq!(owner.writer_fixed_ownership().2, receipt, "reader controls never acquire a later writer's receipt");
            assert_eq!(owner.writer_registration_observation_for_test().3, uses, "reader controls never acquire later writer uses");
            drop(writer);
            assert_eq!(owner.writer_registration_observation_for_test().2, 0);
            assert!(owner.usage().used >= reader_floor);
            let reopened = WriterBudget::from_accountant(owner.clone()).unwrap();
            assert_ne!(owner.writer_registration_observation_for_test().2, epoch);
            let receipt = owner.writer_fixed_ownership().2;
            drop(registry);
            assert_eq!(owner.writer_fixed_ownership().2, receipt, "original reader refund cannot debit the reopened epoch");
            drop(reopened);
            assert_eq!(owner.usage().used, 0);
            assert_eq!(owner.underflow_count_for_test(), 0);
            owner.set_budget(Some(1)).unwrap();
            let error = match <$registry>::new_arc(&owner) { Ok(_) => panic!("constructor must refuse before allocation"), Err(error) => error };
            match error {
                Error::MemoryBudgetExceeded { subsystem, operation, hint, .. } => {
                    assert_eq!(subsystem, $attribution.subsystem);
                    assert_eq!(operation, $attribution.operation);
                    assert_eq!(hint, $attribution.hint);
                }
                other => panic!("original typed refusal required: {other}"),
            }
            assert_eq!(owner.usage().used, 0);
            println!("CDB127_D2_ROW original_provenance family={} constructor_reader=true growth_exercised={} later_writer_epoch_not_acquired=true original_counter_refund=true exact_pre2_labels=true", $name, $growth);
        }};
    }
    provenance!(PendingRegistry, PENDING_ATTRIBUTION, "pending", true, |registry: &Arc<Mutex<PendingRegistry>>| {
        drop(PendingToken::reserve(registry, TxId(7)).unwrap());
    });
    provenance!(ReceivedRegistry, RECEIVED_ATTRIBUTION, "received", true, |registry: &Arc<Mutex<ReceivedRegistry>>| {
        drop(ReceivedToken::reserve(registry, Lsn(7)).unwrap());
    });
    provenance!(LocalRegistry, LOCAL_ATTRIBUTION, "local", false, |_: &Arc<Mutex<LocalRegistry>>| {});
}

#[cfg(all(test, feature = "test-seams"))]
pub(super) fn pending_interleavings_for_test() {
    use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
    let owner = Arc::new(MemoryAccountant::no_limit());
    let database = Database::open_memory_with_plugin_and_accountant(Arc::new(CorePlugin), owner.clone()).unwrap();
    let registry = database.pending_local_schema_stages.clone();
    let key = TxId(91);
    let first = database.new_pending_local_schema_stage().unwrap();
    PendingToken::reserve(&registry, key).unwrap().publish(first);
    let (a, first) = PendingToken::detach_existing(&registry, key).unwrap().unwrap();
    assert!(!registry.lock().contains_key(&key));
    let b = PendingToken::reserve(&registry, key).unwrap();
    assert_eq!(registry.lock().0.map.get(&key).unwrap().tokens, 2);
    let mut other_keys = Vec::with_capacity(128);
    for key in 200..328 { other_keys.push(PendingToken::reserve(&registry, TxId(key)).unwrap()); }
    let second = database.new_pending_local_schema_stage().unwrap();
    b.publish(second);
    assert!(registry.lock().contains_key(&key));
    drop(registry.lock().remove(&key));
    registry.lock().clear();
    assert!(registry.lock().is_empty());
    assert_eq!(registry.lock().0.map.get(&key).unwrap().tokens, 1);
    oracle::last_request_for_test(true);
    let (_, _, _) = oracle::measured(|| a.publish(first));
    assert_eq!(oracle::last_request_for_test(true).2, 0, "rollback restoration never allocates after other-key growth, same-key replacement, removal and clear");
    assert!(registry.lock().contains_key(&key));
    drop(other_keys);
    drop(registry.lock().remove(&key));
    let empty_capacious = owner.usage().used;
    let retained = registry.clone();
    drop(registry);
    drop(database);
    assert!(owner.usage().used > 0, "registry aliases retain funded physical backing after Database drop");
    assert!(owner.usage().used < empty_capacious);
    assert_ne!(owner.writer_registration_observation_for_test().2, 0);
    drop(retained);
    assert_eq!(owner.usage().used, 0);
    assert_eq!(owner.writer_registration_observation_for_test().2, 0);
    assert_eq!(owner.underflow_count_for_test(), 0);
    println!("CDB127_D2_ROW pending_interleavings other_key_growth=true same_key_overlap=true remove_clear_preserves_occupancy=true actual_restore_request_count=0 surviving_database_alias=true final_funder_zero=true");
}

// These rows are reached only from the frozen aggregate's existing mapper.
// Payload preparation is outside the allocator window: each measured insert
// therefore identifies its sole backing allocation independently of layout.
#[cfg(all(test, feature = "test-seams"))]
fn local_stage_for_test(database: &Database) -> LocalSchemaStage {
    let tx = database.begin().unwrap();
    database.execute_in_tx(tx,
        "CREATE TABLE cdb127_local_proof (id INTEGER PRIMARY KEY)",
        &HashMap::new()).unwrap();
    let lsn = Lsn(1);
    let registration = database.prepare_and_register_local_schema_stage(
        tx, tx, lsn, database.declared_reader_memory().unwrap()).unwrap().unwrap();
    let stage = database.take_local_schema_stage(lsn).unwrap();
    drop(registration);
    database.rollback(tx).unwrap();
    assert!(database.table_meta("cdb127_local_proof").is_none(),
        "the real local stage has not crossed publication");
    stage
}

#[cfg(all(test, feature = "test-seams"))]
fn local_growth_for_test() {
    use crate::memory_accounting::WriterBudget;
    use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
    // A distinct production Database owns each real stage's payload charges;
    // registry corrections remain attributable to the tested original payer.
    let payload_owner = Arc::new(MemoryAccountant::no_limit());
    let database = Database::open_memory_with_plugin_and_accountant(
        Arc::new(CorePlugin), payload_owner.clone()).unwrap();
    let owner = Arc::new(MemoryAccountant::no_limit());
    let writer = WriterBudget::from_accountant(owner.clone()).unwrap();
    let baseline = owner.usage().used;
    let counter = owner.writer_registration_observation_for_test().4;
    let registry = LocalRegistry::new_arc(&owner).unwrap();
    let control_baseline = owner.usage().used;
    let mut old_pointer = 0;
    let mut old_delta = 0;
    for key in 0..64 {
        let stage = local_stage_for_test(&database);
        let old_buckets = registry.lock().0.buckets;
        if old_pointer != 0 { oracle::watch_registry_free_for_test(old_pointer, counter); }
        oracle::last_request_for_test(true);
        let (displaced, _, _) = oracle::measured(|| registry.lock().insert(Lsn(key), stage).unwrap());
        assert!(displaced.is_none());
        let request = oracle::last_request_for_test(true);
        let new_buckets = registry.lock().0.buckets;
        if new_buckets != old_buckets {
            let final_layout = bucket_layout::<(Lsn, LocalSchemaStage)>(new_buckets).unwrap();
            let original_layout = bucket_layout::<(Lsn, Pre2LocalSchemaStage)>(new_buckets).unwrap();
            let new_delta = final_layout.size() - original_layout.size();
            assert_eq!(request, (final_layout.size(), final_layout.align(), 1));
            if old_pointer != 0 {
                assert_eq!(oracle::registry_free_observation_for_test(true),
                    (control_baseline + old_delta + new_delta,
                     control_baseline + old_delta + new_delta, true));
            }
            old_pointer = oracle::last_request_pointer_for_test();
            old_delta = new_delta;
            assert_eq!(owner.usage().used, control_baseline + new_delta);
            println!("CDB127_D2_ROW local_populated_growth buckets={} actual_request={} pre2_same_buckets={} delta={} old_free_before_refund=true real_local_prepare_door=true distinct_payload_payer=true",
                new_buckets, request.0, original_layout.size(), new_delta);
        } else {
            assert_eq!(request.2, 0);
            oracle::registry_free_observation_for_test(true);
        }
    }
    assert_eq!(registry.lock().len(), 64);
    // Existing-entry replacement neither reserves nor changes bucket credit.
    let stage = local_stage_for_test(&database);
    oracle::last_request_for_test(true);
    let (displaced, _, _) = oracle::measured(|| registry.lock().insert(Lsn(0), stage).unwrap());
    assert_eq!(oracle::last_request_for_test(true).2, 0);
    assert!(displaced.is_some());
    drop(displaced);
    let usage = owner.usage().used;
    drop(registry.lock().remove(&Lsn(1)));
    assert_eq!(owner.usage().used, usage, "removal keeps physical backing funded while entries remain");
    oracle::watch_registry_free_for_test(old_pointer, counter);
    // CDB-127 ledger promise (fable-18 ruling, 2026-10-09): an emptied
    // registry frees its backing, then refunds it, when it empties.
    registry.lock().clear();
    assert_eq!(registry.lock().len(), 0);
    assert_eq!(oracle::registry_free_observation_for_test(true),
        (control_baseline + old_delta, control_baseline + old_delta, true));
    assert_eq!(owner.usage().used, control_baseline, "an empty registry holds only its fixed control");
    let retained = registry.clone();
    drop(registry);
    assert_eq!(owner.usage().used, control_baseline);
    drop(retained);
    assert_eq!(owner.usage().used, baseline);
    drop(writer);
    assert_eq!(owner.usage().used, 0);
    assert_eq!(owner.underflow_count_for_test(), 0);
    drop(database);
    assert_eq!(payload_owner.usage().used, 0);
    assert_eq!(payload_owner.underflow_count_for_test(), 0);
    println!("CDB127_D2_ROW local_empty_released remove_keeps_backing_until_empty=true clear_frees_then_refunds=true existing_entry_request_count=0 last_alias=true post_system_free_before_refund=true original_registry_and_payload_funders_zero=true");
}

// Select valid numeric keys through this actual map's standard hasher. The
// pinned hashbrown h1 masks the low bits with B-1; this creates a collision
// history under ANY RandomState seed rather than hoping random removals leave
// tombstones. No bucket/control/header bytes or allocator metadata are edited.
#[cfg(all(test, feature = "test-seams"))]
fn clustered_numeric_keys_for_test<K: Eq + Hash + Copy, V, P>(
    backing: &Backing<K, V, P>, make_key: impl Fn(u64) -> K,
) -> [u64; 56] {
    use std::hash::BuildHasher;
    let mut keys = [0; 56];
    let mut found = 0;
    let mut candidate = 0_u64;
    while found < keys.len() {
        if backing.map.hasher().hash_one(make_key(candidate)) as usize & 63 == 0 {
            keys[found] = candidate;
            found += 1;
        }
        candidate = candidate.checked_add(1).expect("numeric key space exhausted");
    }
    keys
}

#[cfg(all(test, feature = "test-seams"))]
fn occupied_tombstone_rehash_for_test<K, V, P>(
    registry: Arc<Mutex<OccupiedRegistry<K, V, P>>>, owner: &Arc<MemoryAccountant>,
    make_key: impl Fn(u64) -> K, family: &'static str,
) where K: Eq + Hash + Copy, P: RegistryFamily {
    use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
    let keys = clustered_numeric_keys_for_test(&registry.lock().0, &make_key);
    let control_baseline = owner.usage().used;
    let counter = owner.writer_registration_observation_for_test().4;
    let mut tokens: [Option<Token<K, V, P>>; 56] = std::array::from_fn(|_| None);
    let mut pointer = 0;
    for (index, key) in keys.iter().enumerate() {
        oracle::last_request_for_test(true);
        let (token, _, _) = oracle::measured(|| Token::reserve(&registry, make_key(*key)).unwrap());
        let request = oracle::last_request_for_test(true);
        if request.2 != 0 {
            assert_eq!(request.2, 1);
            pointer = oracle::last_request_pointer_for_test();
        }
        tokens[index] = Some(token);
    }
    let delta;
    {
        let locked = registry.lock();
        assert_eq!(locked.0.buckets, 64, "bucket identity comes from admitted physical metadata");
        assert_eq!(locked.0.map.len(), 56);
        assert_eq!(locked.0.map.capacity(), 56);
        delta = bucket_layout::<(K, Cell<V>)>(64).unwrap().size()
            - bucket_layout::<(K, P)>(64).unwrap().size();
    }
    // Resize may reorder equal-probe keys. Read actual iteration order after
    // growth: pinned RawIter visits increasing physical control positions.
    // The cluster's first 29 occupied positions lie in its full prefix.
    let remove_keys: [K; 29] = std::array::from_fn(|index|
        registry.lock().0.map.keys().nth(index).copied().unwrap());
    for key in remove_keys {
        let index = keys.iter().position(|raw| make_key(*raw) == key).unwrap();
        drop(tokens[index].take());
        let locked = registry.lock();
        assert_eq!(locked.0.map.capacity() - locked.0.map.len(), 0,
            "this real numeric-key removal creates a tombstone, not growth headroom");
    }
    {
        let locked = registry.lock();
        assert_eq!(locked.0.map.len(), 27);
        assert_eq!(locked.0.map.capacity() - locked.0.map.len(), 0);
        assert!(locked.0.map.len() + 1 <= bucket_capacity(locked.0.buckets) / 2,
            "the next real reserve must enter the pinned tombstone rehash branch");
    }
    assert_eq!(owner.usage().used, control_baseline + delta);
    // Even at zero headroom, in-place rehash needs no new backing liability.
    owner.set_budget(Some(owner.usage().used)).unwrap();
    oracle::watch_registry_free_for_test(pointer, counter);
    oracle::last_request_for_test(true);
    let (token, _, _) = oracle::measured(|| Token::reserve(&registry, remove_keys[0]).unwrap());
    assert_eq!(oracle::last_request_for_test(true).2, 0);
    assert_eq!(oracle::registry_free_observation_for_test(true), (0, 0, false));
    assert_eq!(registry.lock().0.buckets, 64);
    {
        let locked = registry.lock();
        assert!(locked.0.map.capacity() - locked.0.map.len() > 0,
            "native rehash reclaimed the tombstones");
    }
    assert_eq!(owner.usage().used, control_baseline + delta);
    owner.set_budget(None).unwrap();
    oracle::watch_registry_free_for_test(pointer, counter);
    // CDB-127 ledger promise (fable-18 ruling, 2026-10-09): an emptied
    // registry frees its backing, then refunds it, when it empties.
    drop(token);
    drop(tokens);
    assert_eq!(registry.lock().0.map.len(), 0);
    assert_eq!(oracle::registry_free_observation_for_test(true),
        (control_baseline + delta, control_baseline + delta, true));
    assert_eq!(owner.usage().used, control_baseline, "an empty registry holds only its fixed control");
    drop(registry);
    println!("CDB127_D2_ROW tombstone_rehash family={} numeric_hasher_cluster=true tracked_buckets=64 physical_len=27 growth_left=0 next_len=28 full_capacity=56 actual_request_count=0 reclaimed_tombstones=true retained_credit_while_populated=true free_before_refund_when_empty=true",
        family);
}

#[cfg(all(test, feature = "test-seams"))]
fn tombstone_rehash_rows_for_test() {
    use crate::memory_accounting::WriterBudget;
    use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
    let owner = Arc::new(MemoryAccountant::no_limit());
    let writer = WriterBudget::from_accountant(owner.clone()).unwrap();
    let baseline = owner.usage().used;
    occupied_tombstone_rehash_for_test(PendingRegistry::new_arc(&owner).unwrap(), &owner, TxId, "pending");
    assert_eq!(owner.usage().used, baseline);
    occupied_tombstone_rehash_for_test(ReceivedRegistry::new_arc(&owner).unwrap(), &owner, Lsn, "received");
    assert_eq!(owner.usage().used, baseline);

    let payload_owner = Arc::new(MemoryAccountant::no_limit());
    let database = Database::open_memory_with_plugin_and_accountant(
        Arc::new(CorePlugin), payload_owner.clone()).unwrap();
    let registry = LocalRegistry::new_arc(&owner).unwrap();
    let keys = clustered_numeric_keys_for_test(&registry.lock().0, Lsn);
    let control_baseline = owner.usage().used;
    let counter = owner.writer_registration_observation_for_test().4;
    let mut pointer = 0;
    for key in keys {
        let stage = local_stage_for_test(&database);
        oracle::last_request_for_test(true);
        let (displaced, _, _) = oracle::measured(|| registry.lock().insert(Lsn(key), stage).unwrap());
        assert!(displaced.is_none());
        if oracle::last_request_for_test(true).2 != 0 { pointer = oracle::last_request_pointer_for_test(); }
    }
    assert_eq!(registry.lock().0.buckets, 64);
    assert_eq!(registry.lock().0.map.len(), 56);
    assert_eq!(registry.lock().0.map.capacity(), 56);
    let delta = bucket_layout::<(Lsn, LocalSchemaStage)>(64).unwrap().size()
        - bucket_layout::<(Lsn, Pre2LocalSchemaStage)>(64).unwrap().size();
    let remove_keys: [Lsn; 29] = std::array::from_fn(|index|
        registry.lock().0.map.keys().nth(index).copied().unwrap());
    for key in remove_keys {
        drop(registry.lock().remove(&key));
        let locked = registry.lock();
        assert_eq!(locked.0.map.capacity() - locked.0.map.len(), 0,
            "real local-stage removals create tombstones");
    }
    assert_eq!(registry.lock().0.map.len(), 27);
    {
        let locked = registry.lock();
        assert!(locked.0.map.len() + 1 <= bucket_capacity(locked.0.buckets) / 2);
    }
    let stage = local_stage_for_test(&database);
    owner.set_budget(Some(owner.usage().used)).unwrap();
    oracle::watch_registry_free_for_test(pointer, counter);
    oracle::last_request_for_test(true);
    let (displaced, _, _) = oracle::measured(|| registry.lock().insert(remove_keys[0], stage).unwrap());
    assert!(displaced.is_none());
    assert_eq!(oracle::last_request_for_test(true).2, 0);
    assert_eq!(oracle::registry_free_observation_for_test(true), (0, 0, false));
    assert_eq!(registry.lock().0.buckets, 64);
    {
        let locked = registry.lock();
        assert!(locked.0.map.capacity() - locked.0.map.len() > 0);
    }
    assert_eq!(owner.usage().used, control_baseline + delta);
    owner.set_budget(None).unwrap();
    oracle::watch_registry_free_for_test(pointer, counter);
    // CDB-127 ledger promise (fable-18 ruling, 2026-10-09): an emptied
    // registry frees its backing, then refunds it, when it empties.
    registry.lock().clear();
    assert_eq!(oracle::registry_free_observation_for_test(true),
        (control_baseline + delta, control_baseline + delta, true));
    assert_eq!(owner.usage().used, control_baseline, "an empty registry holds only its fixed control");
    drop(registry);
    assert_eq!(owner.usage().used, baseline);
    drop(writer);
    assert_eq!(owner.usage().used, 0);
    assert_eq!(owner.underflow_count_for_test(), 0);
    drop(database);
    assert_eq!(payload_owner.usage().used, 0);
    assert_eq!(payload_owner.underflow_count_for_test(), 0);
    println!("CDB127_D2_ROW tombstone_rehash family=local real_local_prepare_door=true numeric_hasher_cluster=true tracked_buckets=64 physical_len=27 growth_left=0 next_len=28 full_capacity=56 actual_request_count=0 reclaimed_tombstones=true retained_credit_while_populated=true free_before_refund_when_empty=true");
}

#[cfg(all(test, feature = "test-seams"))]
fn constructor_custody_rows_for_test() {
    use crate::memory_accounting::WriterBudget;
    use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
    macro_rules! custody {
        ($registry:ty, $name:literal) => {{
            let owner = Arc::new(MemoryAccountant::no_limit());
            let writer = WriterBudget::from_accountant(owner.clone()).unwrap();
            let root_uses = owner.writer_registration_observation_for_test().3;
            let epoch = owner.writer_registration_observation_for_test().2;
            let counter = owner.writer_registration_observation_for_test().4;
            let baseline = owner.usage().used;
            oracle::last_request_for_test(true);
            let (registry, _, _) = oracle::measured(|| <$registry>::new_arc(&owner).unwrap());
            let layout = crate::memory_accounting::writer_adapter_type_layout::<Mutex<$registry>>();
            assert_eq!(oracle::last_request_for_test(true), (layout.size(), layout.align(), 1));
            assert_eq!(owner.usage().used, baseline + 104,
                "each initialized x86_64 registry constructor admits its pinned positive 104-byte Arc correction");
            let pointer = oracle::last_request_pointer_for_test();
            let retained = registry.clone();
            assert_eq!(owner.writer_registration_observation_for_test().3, root_uses + 1);
            drop(registry);
            drop(writer);
            assert_eq!(owner.writer_registration_observation_for_test().2, epoch,
                "the original registry handle, independently of refund arithmetic, pins the writer epoch");
            assert_eq!(owner.writer_registration_observation_for_test().3, 1);
            oracle::watch_registry_free_for_test(pointer, counter);
            drop(retained);
            // Only this enclosing ordinary Arc correction has the authorized
            // immediate-before-std-free refund exception. Backing watches in
            // growth/empty rows demand retained credit through System.dealloc.
            assert_eq!(oracle::registry_free_observation_for_test(true), (0, 0, true));
            assert_eq!(owner.writer_registration_observation_for_test().2, 0);
            assert_eq!(owner.writer_registration_observation_for_test().3, 0);
            assert_eq!(owner.usage().used, 0);
            assert_eq!(owner.underflow_count_for_test(), 0);
            println!("CDB127_D2_ROW constructor_custody family={} actual_arc_identity=true clone_single_use=true independent_original_epoch_retained=true actual_arc_system_free=true final_epoch_and_credit_zero=true", $name);
        }};
    }
    custody!(PendingRegistry, "pending");
    custody!(ReceivedRegistry, "received");
    custody!(LocalRegistry, "local");
}

#[cfg(all(test, feature = "test-seams"))]
fn local_reader_growth_provenance_for_test() {
    use crate::memory_accounting::WriterBudget;
    let database = Database::open_memory();
    let owner = Arc::new(MemoryAccountant::no_limit());
    let registry = LocalRegistry::new_arc(&owner).unwrap();
    assert!(!registry.lock().0.accountant.is_writer());
    let writer = WriterBudget::from_accountant(owner.clone()).unwrap();
    let epoch = owner.writer_registration_observation_for_test().2;
    let receipt = owner.writer_fixed_ownership().2;
    let uses = owner.writer_registration_observation_for_test().3;
    for key in 0..8 { registry.lock().insert(Lsn(key), local_stage_for_test(&database)).unwrap(); }
    assert!(!registry.lock().0.accountant.is_writer());
    assert_eq!(owner.writer_fixed_ownership().2, receipt);
    assert_eq!(owner.writer_registration_observation_for_test().3, uses);
    registry.lock().clear();
    drop(writer);
    assert_eq!(owner.writer_registration_observation_for_test().2, 0);
    assert!(owner.usage().used > 0);
    let reopened = WriterBudget::from_accountant(owner.clone()).unwrap();
    assert_ne!(owner.writer_registration_observation_for_test().2, epoch);
    let receipt = owner.writer_fixed_ownership().2;
    drop(registry);
    assert_eq!(owner.writer_fixed_ownership().2, receipt);
    drop(reopened);
    assert_eq!(owner.usage().used, 0);
    assert_eq!(owner.underflow_count_for_test(), 0);
    println!("CDB127_D2_ROW local_reader_populated_growth original_untagged_constructor=true real_local_prepare_door=true later_writer_uses_not_acquired=true reopened_epoch_not_debited=true original_funder_zero=true");
}

#[cfg(all(test, feature = "test-seams"))]
fn received_stage_for_test(database: &Database) -> ReceivedSchemaStage {
    let statement = contextdb_parser::parse(
        "CREATE TABLE cdb127_received_proof (id INTEGER PRIMARY KEY)").unwrap();
    let change = database.ddl_change_for_statement(&statement, None).unwrap();
    let changes = ChangeSet { ddl: vec![change], ddl_lsn: vec![Lsn(41)], ..ChangeSet::default() };
    let received = crate::protocol::ReceivedDdlContext {
        tenant_id: TenantId::from("cdb127-proof"),
        source_node_id: "cdb127-proof-source".to_string(),
        source_incarnation: Incarnation(1),
        entries: vec![crate::protocol::ReceivedDdlEntry {
            source_ddl_lsn: Lsn(41), ordinal: 0,
            table: Some("cdb127_received_proof".to_string()), table_generation: Some(1),
            digest: vec![1], author: None,
        }],
    };
    let read_memory = database.declared_reader_memory().unwrap();
    let mut copy_owner = crate::vector_read_memory::admission(read_memory.clone());
    database.prepare_received_schema_stage(Lsn(9), &changes, &received, &[],
        &mut copy_owner, read_memory).unwrap()
}

#[cfg(all(test, feature = "test-seams"))]
fn received_retired_construction_stage_witness_for_test() {
    use crate::memory_accounting::{ContainedMemoryReservation, OwnedMemoryReservation};
    use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;

    let owner = Arc::new(MemoryAccountant::no_limit());
    let database = Database::open_memory_with_plugin_and_accountant(Arc::new(CorePlugin), owner.clone()).unwrap();
    let before_stage = owner.usage().used;
    let stage = received_stage_for_test(&database);
    let stage_charge = owner.usage().used;
    assert!(stage_charge > before_stage, "the real received Stage retains its original writer-backed projection charge");
    let epoch = owner.writer_registration_observation_for_test().2;
    assert_ne!(epoch, 0, "the real Stage owns an original writer epoch");

    let mut retired = stage.durable_projection.retired_vector_construction.lock();
    let mut route_charge = 0usize;
    for entry in 0..6usize {
        let bytes = 19 + entry;
        let reservation = OwnedMemoryReservation::try_new_for(
            owner.clone(), bytes, "sync", "cdb127_stage_witness",
            "Raise MEMORY_LIMIT to retain this received-stage witness route.",
        ).unwrap();
        route_charge += bytes;
        retired.push((
            contextdb_vector::VectorPartitionRef::new(
                contextdb_core::VectorIndexRef::new("cdb127_retired_witness", format!("embedding_{entry}")),
                contextdb_core::VectorPartitionKey::unpartitioned(),
            ),
            Some(ContainedMemoryReservation::from_owned(reservation)),
        ));
    }
    assert_eq!(std::mem::size_of_val(retired.as_slice()) / retired.len(), 88,
        "the actual retired-construction tuple remains the pre2 88-byte representation");
    let backing = retired.as_ptr() as usize;
    let backing_capacity = retired.capacity();
    assert_ne!(backing, 0);
    assert!(backing_capacity >= 6, "multiple actual route credits force real Vec backing");
    assert_eq!(owner.usage().used, stage_charge + route_charge);
    drop(retired);

    oracle::watch_candidate_payload_for_test(backing, &owner);
    let drained: Vec<_> = stage.durable_projection.retired_vector_construction.lock().drain(..).collect();
    assert_eq!(stage.durable_projection.retired_vector_construction.lock().capacity(), backing_capacity,
        "draining retains the projection's actual Vec backing until the real Stage is dropped");
    drop(drained);
    assert_eq!(owner.usage().used, stage_charge,
        "draining refunds route receipts but retains the Stage projection's backing custody");
    assert_eq!(owner.writer_registration_observation_for_test().2, epoch,
        "the existing final _vector_copy_owner keeps the original writer epoch live after drain");

    drop(stage);
    let (census, physical_free, registration_free) = oracle::candidate_payload_free_observation_for_test();
    assert_eq!(physical_free, (true, false),
        "the actual drained Vec backing is physically freed while the database retains its writer registration");
    assert_eq!(registration_free.2, false,
        "the Stage projection frees before the database's original writer registration");
    assert_eq!(census.0, epoch);
    assert!(census.3 > before_stage,
        "the existing final owner charge remains held at the actual Vec backing System.dealloc");
    assert_eq!(owner.usage().used, before_stage,
        "dropping the real Stage physically frees its retained Vec backing and refunds its projection charge");
    drop(database);
    assert_eq!(owner.usage().used, 0);
    assert_eq!(owner.writer_registration_observation_for_test().2, 0);
    assert_eq!(owner.underflow_count_for_test(), 0);
    println!("CDB127_D2_ROW received_retired_construction_stage_witness real_received_stage=true real_projection=true tuple88=true entries=6 drained_projection_retained=true actual_vec_system_free=true backing_charge_held=true physical_free_and_refund=true final_original_funder_zero=true");
}

#[cfg(all(test, feature = "test-seams"))]
fn received_interleavings_for_test() {
    use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
    let owner = Arc::new(MemoryAccountant::no_limit());
    let database = Database::open_memory_with_plugin_and_accountant(Arc::new(CorePlugin), owner.clone()).unwrap();
    let registry = ReceivedRegistry::new_arc(&owner).unwrap();
    let key = Lsn(101);
    ReceivedToken::reserve(&registry, key).unwrap().publish(received_stage_for_test(&database));
    let (a, first) = ReceivedToken::detach_existing(&registry, key).unwrap().unwrap();
    assert!(!registry.lock().contains_key(&key));
    let b = ReceivedToken::reserve(&registry, key).unwrap();
    assert_eq!(registry.lock().0.map.get(&key).unwrap().tokens, 2);
    let mut others: [Option<ReceivedToken>; 128] = std::array::from_fn(|_| None);
    for (index, token) in others.iter_mut().enumerate() {
        *token = Some(ReceivedToken::reserve(&registry, Lsn(200 + index as u64)).unwrap());
    }
    b.publish(received_stage_for_test(&database));
    assert!(registry.lock().contains_key(&key));
    drop(registry.lock().remove(&key));
    registry.lock().clear();
    assert!(registry.lock().is_empty());
    assert_eq!(registry.lock().0.map.get(&key).unwrap().tokens, 1);
    oracle::last_request_for_test(true);
    let ((), _, _) = oracle::measured(|| a.publish(first));
    assert_eq!(oracle::last_request_for_test(true).2, 0,
        "received publication uses the same occupied physical cell after other-key growth, same-key replacement, remove and clear");
    assert!(registry.lock().contains_key(&key));
    drop(others);
    drop(registry.lock().remove(&key));
    let retained = registry.clone();
    drop(registry);
    drop(database);
    assert_ne!(owner.writer_registration_observation_for_test().2, 0);
    drop(retained);
    assert_eq!(owner.usage().used, 0);
    assert_eq!(owner.writer_registration_observation_for_test().2, 0);
    assert_eq!(owner.underflow_count_for_test(), 0);
    println!("CDB127_D2_ROW received_interleavings real_received_prepare_door=true other_key_growth=true same_key_overlap=true remove_clear_preserves_occupancy=true actual_restore_request_count=0 surviving_database_alias=true final_original_funder_zero=true");
}

#[cfg(all(test, feature = "test-seams"))]
fn received_prepublication_refusal_and_cleanup_for_test() {
    let owner = Arc::new(MemoryAccountant::no_limit());
    let database = Database::open_memory_with_plugin_and_accountant(Arc::new(CorePlugin), owner.clone()).unwrap();
    let before = owner.usage().used;
    let stage = received_stage_for_test(&database);
    assert_eq!(database.received_schema_stages.lock().0.buckets, 0);
    assert_eq!(database.received_schema_stages.lock().0.map.capacity(), 0);
    owner.set_budget(Some(owner.usage().used)).unwrap();
    let error = match database.register_received_schema_stage(Lsn(9), stage) {
        Ok(_) => panic!("the initial backing correction must refuse before preparation effects"),
        Err(error) => error,
    };
    match error {
        Error::MemoryBudgetExceeded { subsystem, operation, hint, .. } => {
            assert_eq!(subsystem, RECEIVED_ATTRIBUTION.subsystem);
            assert_eq!(operation, RECEIVED_ATTRIBUTION.operation);
            assert_eq!(hint, RECEIVED_ATTRIBUTION.hint);
        }
        other => panic!("original received-family typed refusal required: {other}"),
    }
    assert_eq!(database.received_schema_stages.lock().0.buckets, 0);
    assert_eq!(database.received_schema_stages.lock().0.map.capacity(), 0,
        "refusal must not leave physically allocated unadmitted backing");
    assert!(database.received_schema_stages.lock().is_empty());
    assert!(database.received_arriving_table_meta.lock().is_empty());
    assert!(database.table_meta("cdb127_received_proof").is_none());
    assert_eq!(owner.usage().used, before, "the refused stage and occupancy leave no operation-only charge");
    owner.set_budget(None).unwrap();
    let registration = database.register_received_schema_stage(Lsn(9), received_stage_for_test(&database)).unwrap();
    assert!(database.received_schema_stages.lock().contains_key(&Lsn(9)));
    assert!(database.received_arriving_table_meta.lock().contains_key(&Lsn(9)));
    assert_ne!(database.received_schema_stages.lock().0.buckets, 0);
    drop(registration);
    assert!(database.received_schema_stages.lock().is_empty());
    assert!(database.received_arriving_table_meta.lock().is_empty());
    // CDB-127 ledger promise (fable-18 ruling, 2026-10-09): the emptied
    // registry frees and refunds its backing, so cleanup returns exactly to
    // what the database held before the registration.
    assert_eq!(database.received_schema_stages.lock().0.buckets, 0);
    assert_eq!(owner.usage().used, before,
        "normal registration cleanup returns the emptied registry's backing");
    drop(database);
    assert_eq!(owner.usage().used, 0);
    assert_eq!(owner.writer_registration_observation_for_test().2, 0);
    assert_eq!(owner.underflow_count_for_test(), 0);
    println!("CDB127_D2_ROW received_prepublication_refusal actual_received_register_door=true initial_admission_before_effects=true original_family_refusal=true no_stage_or_arriving_publication=true normal_registration_cleanup=true empty_backing_released=true final_original_funder_zero=true");
}

#[cfg(all(test, feature = "test-seams"))]
fn occupied_populated_growth_for_test<K, V, P>(
    registry: Arc<Mutex<OccupiedRegistry<K, V, P>>>, owner: &Arc<MemoryAccountant>,
    make_key: impl Fn(u64) -> K, make_stage: impl Fn() -> V, family: &'static str,
) where K: Eq + Hash + Copy, P: RegistryFamily {
    use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
    let control_baseline = owner.usage().used;
    let counter = owner.writer_registration_observation_for_test().4;
    let mut old_pointer = 0;
    let mut old_delta = 0;
    for key in 0..32 {
        let stage = make_stage();
        let old_buckets = registry.lock().0.buckets;
        if old_pointer != 0 { oracle::watch_registry_free_for_test(old_pointer, counter); }
        oracle::last_request_for_test(true);
        let (token, _, _) = oracle::measured(|| Token::reserve(&registry, make_key(key)).unwrap());
        let request = oracle::last_request_for_test(true);
        let new_buckets = registry.lock().0.buckets;
        if new_buckets != old_buckets {
            let final_layout = bucket_layout::<(K, Cell<V>)>(new_buckets).unwrap();
            let original_layout = bucket_layout::<(K, P)>(new_buckets).unwrap();
            let new_delta = final_layout.size() - original_layout.size();
            assert_eq!(request, (final_layout.size(), final_layout.align(), 1));
            if old_pointer != 0 {
                assert_eq!(oracle::registry_free_observation_for_test(true),
                    (control_baseline + old_delta + new_delta,
                     control_baseline + old_delta + new_delta, true));
            }
            old_pointer = oracle::last_request_pointer_for_test();
            old_delta = new_delta;
            assert_eq!(owner.usage().used, control_baseline + new_delta);
            println!("CDB127_D2_ROW occupied_populated_growth family={} buckets={} actual_request={} pre2_same_buckets={} delta={} old_free_before_refund=true real_stage_payloads=true distinct_payload_payer=true",
                family, new_buckets, request.0, original_layout.size(), new_delta);
        } else {
            assert_eq!(request.2, 0);
            oracle::registry_free_observation_for_test(true);
        }
        token.publish(stage);
        assert_eq!(registry.lock().len(), key as usize + 1);
    }
    oracle::watch_registry_free_for_test(old_pointer, counter);
    // CDB-127 ledger promise (fable-18 ruling, 2026-10-09): an emptied
    // registry frees its backing, then refunds it, when it empties.
    registry.lock().clear();
    assert!(registry.lock().is_empty());
    assert_eq!(oracle::registry_free_observation_for_test(true),
        (control_baseline + old_delta, control_baseline + old_delta, true));
    assert_eq!(owner.usage().used, control_baseline, "an empty registry holds only its fixed control");
    let retained = registry.clone();
    drop(registry);
    assert_eq!(owner.usage().used, control_baseline);
    drop(retained);
    println!("CDB127_D2_ROW occupied_populated_empty family={} real_payload_clear=true empty_registry_frees_then_refunds=true last_alias=true post_system_free_before_refund=true", family);
}

#[cfg(all(test, feature = "test-seams"))]
fn occupied_populated_rows_for_test() {
    use crate::memory_accounting::WriterBudget;
    let payload_owner = Arc::new(MemoryAccountant::no_limit());
    let database = Database::open_memory_with_plugin_and_accountant(
        Arc::new(CorePlugin), payload_owner.clone()).unwrap();
    let owner = Arc::new(MemoryAccountant::no_limit());
    let writer = WriterBudget::from_accountant(owner.clone()).unwrap();
    let baseline = owner.usage().used;
    occupied_populated_growth_for_test(PendingRegistry::new_arc(&owner).unwrap(), &owner,
        TxId, || database.new_pending_local_schema_stage().unwrap(), "pending");
    assert_eq!(owner.usage().used, baseline);
    occupied_populated_growth_for_test(ReceivedRegistry::new_arc(&owner).unwrap(), &owner,
        Lsn, || received_stage_for_test(&database), "received");
    assert_eq!(owner.usage().used, baseline);
    drop(writer);
    assert_eq!(owner.usage().used, 0);
    assert_eq!(owner.underflow_count_for_test(), 0);
    drop(database);
    assert_eq!(payload_owner.usage().used, 0);
    assert_eq!(payload_owner.underflow_count_for_test(), 0);
}

#[cfg(all(test, feature = "test-seams"))]
fn assert_pre2_family_refusal_for_test(error: Error, family: &'static str) {
    // Literal PRE2 vocabulary is independent of the production attribution
    // constants, so mutating a production label does not mutate the oracle.
    let (expected_subsystem, expected_operation, expected_hint) = match family {
        "pending" => ("ddl", "stage_transactional_schema",
            "Reduce the schema transaction size or raise MEMORY_LIMIT before retrying."),
        "local" => ("ddl", "commit_transactional_schema",
            "Reduce the schema transaction size or raise MEMORY_LIMIT before retrying."),
        "received" => ("sync", "prepare_received_schema_replacement",
            "Reduce the replicated schema/data working set or raise MEMORY_LIMIT before receiving schema."),
        _ => unreachable!(),
    };
    match error {
        Error::MemoryBudgetExceeded { subsystem, operation, hint, .. } => {
            assert_eq!(subsystem, expected_subsystem);
            assert_eq!(operation, expected_operation);
            assert_eq!(hint, expected_hint);
        }
        other => panic!("original typed family refusal required: {other}"),
    }
}

#[cfg(all(test, feature = "test-seams"))]
fn family_refusal_literals_for_test() {
    use crate::memory_accounting::WriterBudget;
    let database = Database::open_memory();
    macro_rules! refusal {
        ($registry:ty, $name:literal, $grow:expr) => {{
            let owner = Arc::new(MemoryAccountant::no_limit());
            let empty_baseline = owner.usage().used;
            let occupied_constructor = <$registry>::new_arc(&owner).unwrap();
            let occupied_baseline = owner.usage().used;
            let correction = occupied_baseline - empty_baseline;
            owner.set_budget(Some(occupied_baseline + correction - 1)).unwrap();
            let error = match <$registry>::new_arc(&owner) {
                Ok(_) => panic!("positive constructor correction must refuse"), Err(error) => error,
            };
            drop(occupied_constructor);
            assert_pre2_family_refusal_for_test(error, $name);
            assert_eq!(owner.usage().used, 0);
            owner.set_budget(None).unwrap();
            let registry = <$registry>::new_arc(&owner).unwrap();
            assert!(!registry.lock().0.accountant.is_writer());
            let writer = WriterBudget::from_accountant(owner.clone()).unwrap();
            let receipt = owner.writer_fixed_ownership().2;
            let uses = owner.writer_registration_observation_for_test().3;
            let baseline = owner.usage().used;
            owner.set_budget(Some(baseline)).unwrap();
            let error = ($grow)(&registry).expect_err("initial scoped backing correction must refuse");
            assert_pre2_family_refusal_for_test(error, $name);
            assert_eq!(registry.lock().0.buckets, 0);
            assert_eq!(registry.lock().0.map.capacity(), 0);
            assert_eq!(owner.usage().used, baseline);
            assert!(!registry.lock().0.accountant.is_writer());
            assert_eq!(owner.writer_fixed_ownership().2, receipt);
            assert_eq!(owner.writer_registration_observation_for_test().3, uses);
            owner.set_budget(None).unwrap();
            drop(registry);
            drop(writer);
            assert_eq!(owner.usage().used, 0);
            assert_eq!(owner.underflow_count_for_test(), 0);
            println!("CDB127_D2_ROW family_refusal family={} expected_pre2_literals_independent=true constructor_new_family_door=true initial_backing_refusal=true original_reader_after_later_writer=true no_unadmitted_backing=true final_original_funder_zero=true", $name);
        }};
    }
    refusal!(PendingRegistry, "pending", |registry: &Arc<Mutex<PendingRegistry>>| {
        match PendingToken::reserve(registry, TxId(1)) { Ok(token) => { drop(token); Ok(()) }, Err(error) => Err(error) }
    });
    refusal!(ReceivedRegistry, "received", |registry: &Arc<Mutex<ReceivedRegistry>>| {
        match ReceivedToken::reserve(registry, Lsn(1)) { Ok(token) => { drop(token); Ok(()) }, Err(error) => Err(error) }
    });
    refusal!(LocalRegistry, "local", |registry: &Arc<Mutex<LocalRegistry>>| {
        registry.lock().insert(Lsn(1), local_stage_for_test(&database)).map(|stage| drop(stage))
    });
}

// Pre2 Database field declaration, never instantiated. Arc/Mutex registry
// fields have the same pointer layout under the selected private wrappers.
#[allow(dead_code)]
struct Pre2Database {
    /// This handle's identity, never an address. Every handle gets its own,
    /// including one derived by narrowing another's access, exactly as each
    /// had its own address before.
    id: DatabaseId,
    tx_mgr: Arc<TransactionManager<DynStore>>,
    relational_store: Arc<RelationalStore>,
    graph_store: Arc<GraphStore>,
    vector_store: Arc<VectorStore>,
    vector_schema_gates: Arc<VectorSchemaGates>,
    change_log: Arc<RwLock<Vec<ChangeLogEntry>>>,
    #[cfg(feature = "test-seams")]
    forced_change_log_gap_through: Arc<AtomicU64>,
    /// Per-table shadow of `change_log`'s own `RowInsert`/`RowDelete`
    /// entries, and a global LSN reference count over every entry kind --
    /// see `ChangeLogTableIndex`/`ChangeLogLsnRefcounts` on
    /// `composite_store` for why these exist and how they stay in lockstep.
    change_log_table_index: Arc<RwLock<ChangeLogTableIndex>>,
    change_log_lsn_refcounts: Arc<RwLock<ChangeLogLsnRefcounts>>,
    ddl_log: Arc<RwLock<Vec<(Lsn, DdlChange)>>>,
    /// In-memory provenance for the CURRENT delete marker of a natural key.
    /// A row delete has no live relational row on which to retain the ordinary
    /// sync-source sidecar, but a pulled tombstone must not be offered back to
    /// the hub as fresh local work. This deliberately lasts only for the open
    /// database lifetime; rebuilding old tombstone provenance after restart is
    /// a separate retention/recovery design.
    sync_tombstone_arrivals: Arc<RwLock<HashMap<(String, String), SyncTombstoneArrival>>>,
    /// Exact receiver-local graph occurrences that arrived through sync.
    /// Unlike a current-state flag, retaining the local LSN lets a later
    /// local mutation of the same edge propagate without resurrecting the
    /// older pulled occurrence.
    sync_graph_arrivals: Arc<RwLock<HashMap<GraphIdentity, DurableGraphArrival>>>,
    /// Private, exact-version markers for rows the hub terminally refused.
    /// They are reconciliation state, never a queryable rejection inbox.
    terminal_refusal_markers:
        Arc<RwLock<HashMap<TerminalRefusalMarkerKey, TerminalRefusalMarkerRecord>>>,
    terminal_refusal_scans:
        Arc<RwLock<HashMap<TerminalRefusalContextKey, TerminalRefusalScanState>>>,
    /// Authenticated peer identity for the currently sync-authored row. This
    /// mirrors the durable config sidecar for memory databases and is updated
    /// only after the row transaction commits.
    accepted_sync_row_authors: Arc<RwLock<AcceptedSyncRowAuthors>>,
    /// Private commit-LSN keyed received-schema stages.  Scoped database
    /// handles share this exact map because the transaction manager assigns
    /// the LSN and invokes callbacks through the shared database state.
    received_schema_stages: Arc<Mutex<HashMap<Lsn, ReceivedSchemaStage>>>,
    local_erasure_stages: discard::StageRegistry,
    /// The shape of every currently registered received-schema stage, staged
    /// under its own commit LSN so `CompositeStore::apply_exact` can resolve
    /// a staged unit's own vector writes against the schema it is installing
    /// rather than the receiver's current one. Cloned out of the same
    /// `CompositeStore` instance that owns and consumes it.
    received_arriving_table_meta: crate::composite_store::ReceivedArrivingTableMetaRegistry,
    /// The explicit SQL-transaction half is keyed by TxId until the commit
    /// manager assigns its one LSN; PersistentCompositeStore consumes the
    /// resulting LSN-keyed stage in that same Redb write transaction.
    pending_local_schema_stages: Arc<Mutex<HashMap<TxId, PendingLocalSchemaStage>>>,
    local_schema_stages: LocalSchemaStageRegistry,
    /// Private detached-adjudication probe.  It is armed only by received
    /// schema preparation and snapshots the exact WriteSet after normal sync
    /// commit preparation has stamped lineage/delete positions and receipt
    /// writes, but before the detached store applies it.
    capture_detached_sync_write_set: Arc<AtomicBool>,
    detached_sync_write_set: Arc<Mutex<Option<WriteSet>>>,
    received_staging_access: Option<Arc<ReceivedStagingAccess>>,
    received_vector_image_pending: Arc<AtomicBool>,
    /// Ephemeral authenticated hub identity reserved immediately before an
    /// acknowledgement is durably promoted.  It is consumed by the same
    /// config write that changes Pending to Accepted; a crash before that
    /// promotion leaves the ordinary delete pending and resumable.
    persistence: Option<Arc<RedbPersistence>>,
    #[cfg(any(test, feature = "test-seams"))]
    commit_source_reference_charge: usize,
    /// Present only on a handle built from a committed image: the startup
    /// rows a live store would have been asked for at open.
    committed_image_startup: Option<Arc<CommittedImageStartupState>>,
    /// Present only on a handle built from a committed image that left its
    /// history in the store: where to read it the first time it is asked for.
    image_history: Option<Arc<ImageHistory>>,
    /// Descriptor-backed relational pages for a committed image whose rows
    /// were deliberately left out of memory at open.
    retained_relational_source: Option<Arc<crate::persistence::RetainedReadSource>>,
    // CDB-137 sibling field, present in the composed baseline without CDB-127.
    retained_graph_source: Option<Arc<crate::persistence::RetainedReadSource>>,
    /// Engine-owned media repository. File-backed databases share their
    /// existing Redb; memory databases own a temporary file-backed Redb so
    /// large media never becomes process-resident merely because rows do.
    blob_repository: Arc<BlobRepository>,
    open_registry_path: Mutex<Option<PathBuf>>,
    operation_gate: Arc<RwLock<()>>,
    read_publication_gate: Arc<ReadPublicationGate>,
    /// The statement-level reader/writer barrier for table metadata, rows and
    /// index publication. Shared by scoped handles of this database.
    schema_publication_gate: Arc<RwLock<()>>,
    apply_phase_pause: Arc<ApplyPhasePause>,
    /// Test-only pause point in a sync apply's per-row loop, immediately
    /// before the changeset's terminal commit -- after every row (including
    /// a stampless row's sentinel-stamped provenance sidecar write) has been
    /// staged into the transaction's write set, but before the commit that
    /// assigns this changeset's own LSN. Distinct from `apply_phase_pause`
    /// (which fires INSIDE an already-committing transaction, holding the
    /// commit lock): this fires before commit-lock acquisition, so a second,
    /// independent apply can run a full commit while this one is paused
    /// here -- exactly the interleaving a same-instant-sampled, different-
    /// commit-order race needs to reproduce deterministically. Armed
    /// globally by `Database::pause_before_sync_apply_commit_for_test`, but
    /// only actually STOPS a thread that separately marked itself via
    /// `Database::mark_this_thread_for_sync_apply_pre_commit_pause_for_test`
    /// (see `SYNC_APPLY_PRE_COMMIT_PAUSE_ARMED_HERE`) -- an unmarked thread
    /// passes this checkpoint unconditionally even while armed, so a test
    /// can pause exactly one concurrent apply while a second, independent
    /// one commits normally on another thread. The production path reads
    /// state that is never armed outside a test.
    sync_apply_pre_commit_pause: Arc<ApplyPhasePause>,
    /// Instance-owned fence after the complete received-schema write set is
    /// finalized and immediately before the durable store apply.
    #[cfg(any(test, feature = "test-seams"))]
    received_schema_pre_commit_pause: Arc<ApplyPhasePause>,
    /// Instance-owned fence after a received schema unit is durable and
    /// before its prepared memory image is published. It is test-only to arm.
    #[cfg(any(test, feature = "test-seams"))]
    received_schema_pre_publish_pause: Arc<ApplyPhasePause>,
    #[cfg(any(test, feature = "test-seams"))]
    received_schema_apply_observer: Arc<Mutex<Option<SharedReceivedSchemaApplyObservationForTest>>>,
    #[cfg(any(test, feature = "test-seams"))]
    local_schema_pre_commit_pause: Arc<ApplyPhasePause>,
    #[cfg(any(test, feature = "test-seams"))]
    local_schema_pre_publish_pause: Arc<ApplyPhasePause>,
    /// Test-only fence BETWEEN two of a local commit's own publication
    /// components -- the prepared schema image already in place, the custody
    /// and lineage deltas not yet. Never armed in production.
    #[cfg(any(test, feature = "test-seams"))]
    local_schema_between_publication_pause: Arc<ApplyPhasePause>,
    /// Test-only export fence after a consistent snapshot is captured and
    /// before its artifact can be published. It is never armed in production.
    export_after_capture_pause: Arc<ApplyPhasePause>,
    relational: MemRelationalExecutor<DynStore>,
    graph: MemGraphExecutor<DynStore>,
    vector: MemVectorExecutor<DynStore>,
    /// The store file's length as the reader found it, for a handle built
    /// from a committed image, which has no persistence to ask.
    image_store_file_bytes: Option<u64>,
    session_tx: Arc<Mutex<Option<TxId>>>,
    /// The transaction slot of the handle a READER handle was opened from.
    ///
    /// A reader handle deliberately has no transaction control of its own, but
    /// the reads it serves belong to whoever opened it: a writing session that
    /// asks its own handle for a bounded read is entitled to see what that
    /// session has done so far. So the reader watches the opener's slot rather
    /// than owning one, which also means a COMMIT or ROLLBACK on the opener is
    /// visible to the reader the moment it happens.
    origin_session_tx: Option<Arc<Mutex<Option<TxId>>>>,
    instance_id: uuid::Uuid,
    owner_thread: thread::ThreadId,
    plugin: Arc<dyn DatabasePlugin>,
    owner_read_config: Arc<OwnerReadConfig>,
    declared_read_memory: Arc<crate::vector_read_memory::DeclaredReadMemory>,
    /// The live owner-inspection service this open started, when it started
    /// one. It holds a shared handle onto the same stores rather than this
    /// value, so releasing the database releases the service with it.
    owner_read_service: Option<Arc<crate::owner_read::OwnerReadService>>,
    /// Why inspection is unavailable when serving was asked for and could not
    /// start. The database itself opened; the caller is told the operating
    /// system's own reason rather than discovering an empty result later.
    owner_read_startup_failure: Option<String>,
    #[cfg(feature = "test-seams")]
    route_observer: Option<Arc<dyn crate::read_session::ReadSessionTestObserver>>,
    #[cfg(feature = "test-seams")]
    kernel_observer: Option<Arc<dyn crate::read_session::ReadKernelTestObserver>>,
    /// Production-dead: counts how many times
    /// `retire_snapshot_free_vector_partitions` actually began its guarded
    /// removal pass, as opposed to skipping it because a cheap pre-check
    /// found nothing retirable. Shared across every scoped handle of the
    /// same database so a test can observe it regardless of which handle
    /// drove maintenance.
    #[cfg(feature = "test-seams")]
    snapshot_free_retirement_pass_count: Arc<AtomicUsize>,
    access: AccessConstraints,
    accountant: Arc<MemoryAccountant>,
    conflict_policies: RwLock<ConflictPolicies>,
    subscriptions: Arc<Mutex<SubscriptionState>>,
    pruning_runtime: Mutex<PruningRuntime>,
    pruning_guard: Arc<Mutex<()>>,
    /// The ONE hub this database's retained tables are delivered to, loaded
    /// from database metadata at open so a restart cannot let a second hub
    /// claim first place.
    retention_sync_peer: Arc<Mutex<Option<String>>>,
    /// Memory databases have no restart contract, but the same public
    /// destination-change operation still needs its one-shot ancestry
    /// selection for the life of the shared database.
    in_memory_destination_reuploads: Arc<Mutex<HashMap<String, DestinationReuploadEpoch>>>,
    lineage_state_lock: Arc<Mutex<InMemoryLineageState>>,
    /// Serializes whole-migration retry classification through the matching
    /// durable stage commit. The lock is shared by derived handles, so an
    /// absent-vector observation cannot race another local stage writer.
    received_schema_manifest_lock: Arc<Mutex<()>>,
    sync_push_apply_locks: Arc<SyncPushApplyLocks>,
    #[cfg(feature = "sync-orchestration")]
    sync_client_push_locks: Arc<SyncClientPushLocks>,
    in_memory_received_schema_manifests: Arc<Mutex<InMemoryReceivedSchemaManifestState>>,
    /// The sole schema-instance generation owner. File-backed databases keep
    /// the same value in `sync_lineage.v1.table_generation.*`; memory
    /// databases retain it for this handle's life.
    in_memory_table_generations: Arc<Mutex<HashMap<String, u64>>>,
    in_memory_ddl_generations: Arc<Mutex<InMemoryDdlGenerations>>,
    /// Receiver-local arrival evidence for exact DDL occurrences. File-backed
    /// opens hydrate this from its dedicated config prefix; memory databases
    /// publish it only after the staged commit has become durable.
    received_ddl_arrivals: Arc<RwLock<HashMap<(Lsn, u32), DurableReceivedDdlArrival>>>,
    /// Locally authored DDL occurrences an authenticated received vector has
    /// since restated. Hydrated and published exactly like the arrival
    /// evidence above, and consumed only by outbound echo classification.
    superseded_ddl_occurrences: Arc<RwLock<HashMap<(Lsn, u32), DurableSupersededDdlOccurrence>>>,
    cron: Arc<CronState>,
    event_bus: Arc<EventBusState>,
    trigger: Arc<TriggerState>,
    /// How long THIS handle waits for a same-database trigger callback to go
    /// idle before the typed cross-thread error is due, when something has
    /// stated a deadline other than [`DEFAULT_TRIGGER_DEADLOCK_TIMEOUT`].
    ///
    /// The deadline is behavior, so it is never taken from the environment:
    /// no exported name moves it, and a journey that needs a shorter one
    /// states it through `set_trigger_deadlock_timeout_for_test` on the very
    /// handle under test. `None` -- the only value a shipped open produces --
    /// means the shipped default.
    trigger_deadlock_timeout_override: Mutex<Option<Duration>>,
    sync_relay_mode: Arc<AtomicBool>,
    in_memory_sync_progress: Arc<Mutex<InMemorySyncProgressState>>,
    in_memory_applied_push_watermarks: Arc<Mutex<HashMap<String, Lsn>>>,
    /// This database life's per-tenant sync incarnation, minted on first use and
    /// held for the life of the handle. Backed by a persisted config value for
    /// file-backed databases (so a plain reopen loads the same one) and by this
    /// cache alone for in-memory databases (each open is a fresh life).
    sync_incarnations: Arc<Mutex<HashMap<String, Incarnation>>>,
    // No public authority setter.
    custody_runtime: Arc<Mutex<Option<crate::custody::policy::Runtime>>>,
    custody_cache: Arc<Mutex<Option<crate::custody::store::Store>>>,
    custody_metadata: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    pending_event_bus_ddl: Mutex<HashMap<TxId, Vec<DdlChange>>>,
    pending_commit_metadata: Mutex<HashMap<TxId, PendingCommitMetadata>>,
    limit_update_lock: Arc<Mutex<()>>,
    #[cfg(any(test, feature = "test-seams"))]
    maintenance_poll_interval_update_pause: Arc<ApplyPhasePause>,
    #[cfg(any(test, feature = "test-seams"))]
    maintenance_poll_interval_update_probe: Arc<MaintenancePollIntervalUpdateProbe>,
    /// The store's disk budget, SHARED by every handle open on it.
    ///
    /// It is a property of the store, not of whoever is holding it: it is
    /// persisted with the store and it governs the same file whoever asks. A
    /// per-handle copy meant a limit set after a handle existed was invisible
    /// to that handle -- so a writer's own owner-read service answered "no
    /// disk limit" about a store that had one.
    disk_limit: Arc<AtomicU64>,
    disk_limit_startup_ceiling: Arc<AtomicU64>,
    /// Engine open-config for the engine's OWN durable trigger-audit history:
    /// an in-process, non-persisted retention window in seconds, defaulting
    /// to [`TRIGGER_AUDIT_RETENTION`]. Deliberately NOT backed by the redb
    /// config store -- it is caller policy for this handle's lifetime only,
    /// the same way a disk limit or memory ceiling is caller policy, never a
    /// stored declaration, DDL surface, or transported value. Shared (like
    /// [`Self::auto_compact_min_interval`]), not per-handle, and re-read per
    /// tick by the maintenance loop's [`MaintenanceContext`] -- a declared
    /// override reaches an ALREADY-RUNNING engine-owned loop, not only a
    /// loop spawned after the setter call.
    trigger_audit_retention_secs: Arc<AtomicU64>,
    /// Every read snapshot currently entitled to resolve a version — see
    /// [`SnapshotFloorRegistry`]. Shared (not per-handle) so a pin taken
    /// through one derived handle is honored by cleanup running through
    /// another handle onto the same underlying database.
    snapshot_registry: Arc<SnapshotFloorRegistry>,
    /// Commit-LSN bridge from the snapshot guard owned here to the erased
    /// `WriteSetApplicator` inside the transaction manager. The store consumes
    /// each sample exactly once before durable vector preparation; absence is
    /// never interpreted as permission to retire historical index state.
    vector_partition_snapshot_stages: VectorPartitionSnapshotStageRegistry,
    /// Node ids whose live row a retention pass already reclaimed while a
    /// registered reader could still see adjacency entries touching them.
    /// Those entries stay physically in place, and nothing else in the store
    /// records that the id ever carried a row, so the next pass reads this
    /// set to revisit them. Shared (not per-handle), like the registry
    /// itself, so a cycle driven through any handle sees the same carry-over.
    retention_deferred_edge_nodes: Arc<Mutex<RetentionDeferredEdges>>,
    /// Engine open-config: `true` when this database's maintenance is
    /// `MaintenancePolicy::CallerDriven`. Shared, not per-handle, so the
    /// policy is a property of the database, not of one handle onto it.
    maintenance_caller_driven: Arc<AtomicBool>,
    /// Persisted database poll setting in milliseconds. Shared across handles
    /// and sampled whenever the engine-owned worker is (re)started.
    maintenance_poll_interval_ms: Arc<AtomicU64>,
    /// The serving page cache a maintained vector partition may keep resident,
    /// as the consumer declared it. Zero means undeclared, so the engine's
    /// default share of the declared memory limit applies.
    vector_serving_cache_bytes: Arc<AtomicU64>,
    /// When the last maintenance cycle actually ran (engine-owned tick or a
    /// caller's own `run_maintenance_cycle` call), for the caller-driven
    /// backlog warning's "has a tick interval elapsed" check.
    last_maintenance_cycle_at: Arc<Mutex<Option<std::time::Instant>>>,
    /// Throttle for the caller-driven backlog warning, so a busy caller that
    /// never drives a cycle gets ONE warning per elapsed interval, not one
    /// per commit.
    caller_driven_backlog_warned_at: Arc<Mutex<Option<std::time::Instant>>>,
    /// When an automatic (threshold-gated) or explicit (`compact_now`) redb
    /// compaction last actually ran on this database — see
    /// [`AUTO_COMPACT_MIN_INTERVAL`]. Shared, not per-handle: a compaction
    /// run through one derived handle must throttle the automatic path on
    /// every other handle onto the same database too.
    last_auto_compact_at: Arc<Mutex<Option<std::time::Instant>>>,
    /// Engine open-config, test-only override of [`AUTO_COMPACT_MIN_INTERVAL`]
    /// — see `__set_auto_compact_min_interval_for_test`.
    auto_compact_min_interval: Arc<Mutex<Duration>>,
    sync_watermark: Arc<AtomicLsn>,
    closed: AtomicBool,
    close_progress: Arc<Mutex<CloseProgress>>,
    close_changed: Arc<Condvar>,
    /// The first cleanup or handoff failure survives an owner-read drain
    /// timeout so the retry that releases resources still reports its cause.
    close_error: Mutex<Option<Error>>,
    #[cfg(feature = "test-seams")]
    close_follower_observer: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    resource_closed: Arc<AtomicBool>,
    /// Set when a commit reached durable storage and its in-memory
    /// publication was then interrupted by an unwind. Shared with every
    /// derived handle: from then on the database refuses service with
    /// `Error::PublicationInterrupted` until it is reopened. See
    /// `PublicationInterruptionWatch`.
    publication_interrupted: Arc<AtomicBool>,
    rows_examined: AtomicU64,
    /// Actual complete candidate rows materialized by this database's vector
    /// candidate subtree. Shared with derived handles, isolated from every
    /// independently opened database, and present only for test observation.
    #[cfg(any(test, feature = "test-seams"))]
    candidate_rows_materialized: Arc<AtomicU64>,
    last_vector_search_used_hnsw: Arc<AtomicBool>,
    last_vector_search_trace: Arc<RwLock<Option<contextdb_vector::mem::OwnedVectorSearchTrace>>>,
    statement_cache: RwLock<HashMap<String, Arc<CachedStatement>>>,
    /// The rank-policy formulas this STORE has registered, held once and read
    /// by every handle onto it. A derived handle -- a scoped view, a bounded
    /// read session -- is another way of looking at the same store, not
    /// another store, so it must see the formulas DDL registered whether that
    /// DDL ran before the handle was taken or after it. A per-handle copy
    /// answered "no such formula" to a search the other door answers.
    #[allow(clippy::type_complexity)]
    rank_formula_cache: Arc<RwLock<HashMap<(String, String), Arc<RankFormula>>>>,
    acl_grant_cache: RwLock<gate::AdmittedAclGrantCache>,
    rank_policy_eval_count: AtomicU64,
    rank_policy_formula_parse_count: AtomicU64,
    fk_indexed_tuple_probes: AtomicU64,
    fk_full_scan_fallbacks: AtomicU64,
    commit_rows_validated: AtomicU64,
    commit_indexed_probes: AtomicU64,
    commit_staged_vs_staged_comparisons: AtomicU64,
    commit_scan_rows_touched: AtomicU64,
    commit_index_maintenance_visits: AtomicU64,
    #[cfg(feature = "test-seams")]
    commit_stage_wall_nanos: [AtomicU64; 7],
    corrupt_joined_values: RwLock<HashSet<(String, RowId, String)>>,
    resource_owner: bool,
    /// What this handle is for, and therefore whether the store waits for it.
    handle_role: HandleRole,
    /// Shared by every handle on this store: how many holders are still live,
    /// and what the owner left for the last of them to release.
    finalization: Arc<StoreFinalization>,
    // Last to drop: every derived execution target owns the image admission
    // until its stores and suspended reads have released their allocations.
    read_image_memory: Option<Arc<crate::read_image_memory::ImageMemory>>,
}

#[cfg(all(test, feature = "test-seams"))]
pub(super) fn pending_drop_unwind_for_test() {
    use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
    let owner = Arc::new(MemoryAccountant::no_limit());
    let database = Database::open_memory_with_plugin_and_accountant(Arc::new(CorePlugin), owner.clone()).unwrap();
    let registry = PendingRegistry::new_arc(&owner).unwrap();
    let first = database.new_pending_local_schema_stage().unwrap();
    oracle::last_request_for_test(true);
    let (token, _, _) = oracle::measured(|| PendingToken::reserve(&registry, TxId(1)).unwrap());
    assert_eq!(oracle::last_request_for_test(true).2, 1);
    let backing_pointer = oracle::last_request_pointer_for_test();
    token.publish(first);
    for key in 2..=3 {
        PendingToken::reserve(&registry, TxId(key)).unwrap()
            .publish(database.new_pending_local_schema_stage().unwrap());
    }
    let buckets = registry.lock().0.buckets;
    let map_delta = bucket_layout::<(TxId, Cell<PendingLocalSchemaStage>)>(buckets).unwrap().size()
        - bucket_layout::<(TxId, Pre2PendingLocalSchemaStage)>(buckets).unwrap().size();
    let control_delta = crate::memory_accounting::writer_adapter_type_layout::<Mutex<PendingRegistry>>().size()
        - crate::memory_accounting::writer_adapter_type_layout::<Mutex<HashMap<TxId, Pre2PendingLocalSchemaStage>>>().size();
    let counter = owner.writer_registration_observation_for_test().4;
    drop(database);
    oracle::watch_registry_free_for_test(backing_pointer, counter);
    R1_DATABASE_DROP_PANIC_FOR_TEST.with(|slot| slot.set(true));
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(registry)));
    assert!(outcome.is_err(), "the real pending working-Database destructor reaches the one-unwind seam");
    let (before_free, after_free, physically_freed) = oracle::registry_free_observation_for_test(true);
    assert!(physically_freed, "single-unwind cleanup reaches actual map backing System.dealloc return");
    assert_eq!(before_free, after_free);
    assert!(before_free >= map_delta + control_delta, "all scoped registry bytes stay funded through physical backing free");
    assert_eq!(owner.usage().used, 0, "advancing registry cleanup frees backing and refunds only after the remaining real payloads");
    assert_eq!(owner.writer_registration_observation_for_test().2, 0);
    assert_eq!(owner.underflow_count_for_test(), 0);
    println!("CDB127_D2_ROW pending_single_unwind actual_working_database_payload=true remaining_entries_cleaned=true actual_backing_free=true map_credit_retained_through_free=true final_original_funder_zero=true");
}

#[cfg(all(test, feature = "test-seams"))]
mod named_open_residue_tests {
    use super::*;
    use crate::memory_accounting::{WriterBudget, process_aggregate_snapshot, set_process_ceiling_for_test};
    use crate::read_image_memory::ImageMemory;

    #[test]
    fn fixed_writer_open_controls_use_the_process_ceiling_and_refund_once() {
        struct CeilingReset;
        impl Drop for CeilingReset {
            fn drop(&mut self) { set_process_ceiling_for_test(None); }
        }
        let owner = Arc::new(MemoryAccountant::no_limit());
        let writer = WriterBudget::from_accountant(owner.clone()).unwrap();
        let local = owner.usage().used;
        let aggregate = process_aggregate_snapshot().0;
        owner.set_budget(Some(1)).unwrap();
        let assembly_writer = WriterBudget::from_accountant(owner.clone()).unwrap();
        let assembly_budget = assembly_writer.vector_accountant();
        assert_eq!(MemoryAccountant::writer_vector_adapter_type_layout_for_test().size(), 64);
        assert_eq!(process_aggregate_snapshot().0, aggregate + 64);
        assert_eq!(owner.usage().used, local);
        assert!(matches!(assembly_budget.try_allocate_for(local + 2, "writer", "payload", "test"), Err(Error::MemoryBudgetExceeded { .. })));
        assert!(matches!(owner.read_budget(), Err(Error::MemoryBudgetExceeded { .. })),
            "ordinary working-memory adapter controls retain writer admission");
        drop(assembly_budget);
        drop(assembly_writer);
        assert_eq!(process_aggregate_snapshot().0, aggregate);
        let registry = ReceivedRegistry::new_arc(&owner).unwrap();
        assert_eq!(process_aggregate_snapshot().0, aggregate + 104);
        assert_eq!(owner.usage().used, local);
        let pending = PendingRegistry::new_arc(&owner).unwrap();
        let local_registry = LocalRegistry::new_arc(&owner).unwrap();
        assert_eq!(process_aggregate_snapshot().0, aggregate + 3 * 104);
        assert_eq!(owner.usage().used, local);
        let image = ImageMemory::operation_writer(owner.clone()).unwrap();
        assert_eq!(std::mem::size_of::<ImageMemory>() + 2 * std::mem::size_of::<usize>(), 232);
        assert_eq!(process_aggregate_snapshot().0, aggregate + 3 * 104 + 232);
        assert_eq!(owner.usage().used, local);
        assert!(matches!(image.reserve(local + 2), Err(Error::MemoryBudgetExceeded { .. })),
            "growing writer payload still obeys the declared limit");
        let alias = registry.clone();
        drop(registry);
        assert_eq!(process_aggregate_snapshot().0, aggregate + 3 * 104 + 232);
        drop(image);
        assert_eq!(process_aggregate_snapshot().0, aggregate + 3 * 104);
        drop(pending);
        drop(local_registry);
        assert_eq!(process_aggregate_snapshot().0, aggregate + 104);
        set_process_ceiling_for_test(Some(aggregate + 104 + 231));
        let _reset = CeilingReset;
        assert!(matches!(ImageMemory::operation_writer(owner.clone()),
            Err(Error::MemoryBudgetExceeded { subsystem, operation, requested_bytes: 232, .. })
            if subsystem == "process_native_ceiling" && operation == "writer_decode_open_residue"));
        assert_eq!(process_aggregate_snapshot().0, aggregate + 104);
        drop(alias);
        assert_eq!(process_aggregate_snapshot().0, aggregate);
        set_process_ceiling_for_test(Some(aggregate + 103));
        assert!(matches!(ReceivedRegistry::new_arc(&owner),
            Err(Error::MemoryBudgetExceeded { subsystem, operation, requested_bytes: 104, .. })
            if subsystem == "process_native_ceiling" && operation == "received_registry_open_residue"));
        assert_eq!(process_aggregate_snapshot().0, aggregate);
        assert_eq!(owner.usage().used, local);
        set_process_ceiling_for_test(Some(aggregate + 63));
        assert!(matches!(WriterBudget::from_accountant(owner.clone()),
            Err(Error::MemoryBudgetExceeded { subsystem, operation, requested_bytes: 64, .. })
            if subsystem == "process_native_ceiling" && operation == "assembly_budget_adapter_open_residue"));
        assert_eq!(process_aggregate_snapshot().0, aggregate);
        assert_eq!(owner.usage().used, local);
        drop(writer);
        assert_eq!(owner.underflow_count_for_test(), 0);
        println!("ASSEMBLY_RESIDUE staged_posting_provider={} store_finalization_control={} dyn_store_memory_or_image={} dyn_store_persistent={}",
            crate::memory_accounting::writer_adapter_type_layout::<crate::database::writer_read::WriterStagedPostingAdmission>().size(),
            crate::memory_accounting::writer_adapter_type_layout::<super::super::StoreFinalization>().size(),
            std::mem::size_of::<super::super::AdmittedDynStore<crate::composite_store::CompositeStore>>(),
            std::mem::size_of::<super::super::AdmittedDynStore<crate::persistent_store::PersistentCompositeStore>>());
        println!("OPEN_RESIDUE assembly_budget_adapter=64 writer_decode=232 received_registry=104 pending_registry=104 local_registry=104 writer_limit=1 process_refusal=true single_refund=true");
    }
}
