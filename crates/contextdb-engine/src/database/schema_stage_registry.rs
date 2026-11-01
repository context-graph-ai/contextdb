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
    target_feature = "sse2",
    any(target_arch = "x86", target_arch = "x86_64"),
    not(miri)
)) || cfg!(all(
    target_arch = "aarch64",
    target_feature = "neon",
    target_endian = "little",
    not(miri)
)) || cfg!(all(
    target_arch = "loongarch64",
    target_feature = "lsx",
    not(miri)
)) {
    16
} else if cfg!(any(
    target_pointer_width = "64",
    target_arch = "aarch64",
    target_arch = "x86_64",
    target_arch = "wasm32"
)) {
    8
} else {
    4
};

// CDB127 private static-map extension. These are the exact pre2 family
// admission labels/hints for backing growth. New private constructor doors
// inherit their family labels; PRE2 had no constructor admission.
pub(super) struct Attribution {
    subsystem: &'static str,
    operation: &'static str,
    hint: &'static str,
}
const PENDING_ATTRIBUTION: Attribution = Attribution {
    subsystem: "ddl",
    operation: "stage_transactional_schema",
    hint: "Reduce the schema transaction size or raise MEMORY_LIMIT before retrying.",
};
const LOCAL_ATTRIBUTION: Attribution = Attribution {
    subsystem: "ddl",
    operation: "commit_transactional_schema",
    hint: "Reduce the schema transaction size or raise MEMORY_LIMIT before retrying.",
};
const RECEIVED_ATTRIBUTION: Attribution = Attribution {
    subsystem: "sync",
    operation: "prepare_received_schema_replacement",
    hint: "Reduce the replicated schema/data working set or raise MEMORY_LIMIT before receiving schema.",
};
pub(super) trait RegistryFamily {
    fn attribution() -> &'static Attribution;
    fn open_residue_operation() -> &'static str {
        "pending_registry_open_residue"
    }
}
impl RegistryFamily for Pre2PendingLocalSchemaStage {
    fn attribution() -> &'static Attribution {
        &PENDING_ATTRIBUTION
    }
}
impl RegistryFamily for Pre2ReceivedSchemaStage {
    fn attribution() -> &'static Attribution {
        &RECEIVED_ATTRIBUTION
    }
    fn open_residue_operation() -> &'static str {
        "received_registry_open_residue"
    }
}

fn capacity_error() -> Error {
    Error::Other(("schema stage registry allocation capacity overflow".to_string()).into())
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn bucket_count<T>(capacity: usize) -> Result<usize> {
    if capacity < 15 {
        let minimum = match (GROUP_WIDTH, std::mem::size_of::<T>()) {
            (16, 0..=1) => 14,
            (16, 2..=3) | (8, 0..=1) => 7,
            _ => 3,
        };
        let capacity = capacity.max(minimum);
        Ok(if capacity < 4 {
            4
        } else if capacity < 8 {
            8
        } else {
            16
        })
    } else {
        (capacity.checked_mul(8).ok_or_else(capacity_error)? / 7)
            .checked_next_power_of_two()
            .ok_or_else(capacity_error)
    }
}
fn bucket_capacity(buckets: usize) -> usize {
    if buckets == 0 {
        0
    } else if buckets <= 8 {
        buckets - 1
    } else {
        (buckets / 8) * 7
    }
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn bucket_layout<T>(buckets: usize) -> Result<Layout> {
    if buckets == 0 {
        return Ok(Layout::new::<()>());
    }
    let alignment = std::mem::align_of::<T>().max(GROUP_WIDTH);
    let offset = std::mem::size_of::<T>()
        .checked_mul(buckets)
        .and_then(|bytes| bytes.checked_add(alignment - 1))
        .ok_or_else(capacity_error)?
        & !(alignment - 1);
    let bytes = offset
        .checked_add(buckets)
        .and_then(|bytes| bytes.checked_add(GROUP_WIDTH))
        .ok_or_else(capacity_error)?;
    if bytes > isize::MAX as usize - (alignment - 1) {
        return Err(capacity_error());
    }
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
    fn new(
        accountant: StoreAccountantHandle,
        control: ControlCredit,
        attribution: &'static Attribution,
    ) -> Self {
        Self {
            map: ManuallyDrop::new(HashMap::new()),
            buckets: 0,
            backing: ManuallyDrop::new(None),
            accountant: ManuallyDrop::new(accountant),
            control: ManuallyDrop::new(Some(control)),
            baseline: PhantomData,
            attribution,
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn reserve_key(&mut self, key: &K) -> Result<()> {
        if self.map.contains_key(key) {
            return Ok(());
        }
        let free = self
            .map
            .capacity()
            .checked_sub(self.map.len())
            .ok_or_else(capacity_error)?;
        if free >= 1 {
            self.map.try_reserve(1).map_err(|error| {
                Error::Other((format!("schema stage registry allocation failed: {error}")).into())
            })?;
            return Ok(());
        }
        let next_len = self.map.len().checked_add(1).ok_or_else(capacity_error)?;
        let full = bucket_capacity(self.buckets);
        if next_len <= full / 2 {
            // Native try_reserve rehashes tombstones in place on this branch.
            self.map.try_reserve(1).map_err(|error| {
                Error::Other((format!("schema stage registry allocation failed: {error}")).into())
            })?;
            return Ok(());
        }
        let required = next_len.max(full.checked_add(1).ok_or_else(capacity_error)?);
        let new_buckets = bucket_count::<(K, V)>(required)?;
        let final_layout = bucket_layout::<(K, V)>(new_buckets)?;
        let original_layout = bucket_layout::<(K, P)>(new_buckets)?;
        let correction = final_layout
            .size()
            .checked_sub(original_layout.size())
            .ok_or_else(capacity_error)?;
        // No old liability is released while the separate new allocation is
        // live. Hashbrown frees old backing before successful reserve returns.
        let new_credit = ControlCredit::admit_from_handle(
            &self.accountant,
            correction,
            self.attribution.subsystem,
            self.attribution.operation,
            self.attribution.hint,
        )?;
        self.map.try_reserve(1).map_err(|error| {
            Error::Other((format!("schema stage registry allocation failed: {error}")).into())
        })?;
        self.buckets = new_buckets;
        let old = self.backing.replace(new_credit);
        drop(old);
        Ok(())
    }
    /// An empty registry holds nothing: free its physical backing first, then
    /// refund the backing credit, so the accountant returns to the registry's
    /// fixed control once every stage has left.
    fn release_if_empty(&mut self) {
        if !self.map.is_empty() || self.buckets == 0 && self.backing.is_none() {
            return;
        }
        drop(std::mem::take(&mut *self.map));
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
                let step = self.next;
                self.next += 1;
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
    fn drop(&mut self) {
        self.finish();
    }
}
impl<K: Eq + Hash + Copy, V, P> Drop for Backing<K, V, P> {
    fn drop(&mut self) {
        let refund = self.control.take().map(ControlCredit::prepare);
        let mut cleanup = BackingCleanup {
            source: self,
            next: 0,
            refund,
        };
        cleanup.finish();
    }
}

pub(crate) struct LocalRegistry(Backing<Lsn, LocalSchemaStage, Pre2LocalSchemaStage>);
impl LocalRegistry {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn new_arc(
        accountant: &crate::memory_accounting::AccountantHandle,
    ) -> Result<Arc<Mutex<Self>>> {
        let final_layout = crate::memory_accounting::writer_adapter_type_layout::<Mutex<Self>>();
        let old_layout = crate::memory_accounting::writer_adapter_type_layout::<
            Mutex<HashMap<Lsn, Pre2LocalSchemaStage>>,
        >();
        let bytes = final_layout
            .size()
            .checked_sub(old_layout.size())
            .ok_or_else(capacity_error)?;
        let accountant = StoreAccountantHandle::new(accountant.clone());
        let attribution = &LOCAL_ATTRIBUTION;
        let control = if accountant.is_writer() {
            ControlCredit::admit_open_residue(&accountant, bytes, "local_registry_open_residue")?
        } else {
            ControlCredit::admit_from_handle(
                &accountant,
                bytes,
                attribution.subsystem,
                attribution.operation,
                attribution.hint,
            )?
        };
        Ok(Arc::new(Mutex::new(Self(Backing::new(
            accountant,
            control,
            attribution,
        )))))
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn insert(
        &mut self,
        key: Lsn,
        value: LocalSchemaStage,
    ) -> Result<Option<LocalSchemaStage>> {
        self.0.reserve_key(&key)?;
        Ok(self.0.map.insert(key, value))
    }
    pub(crate) fn get(&self, key: &Lsn) -> Option<&LocalSchemaStage> {
        self.0.map.get(key)
    }
    pub(crate) fn contains_key(&self, key: &Lsn) -> bool {
        self.0.map.contains_key(key)
    }
    pub(crate) fn remove(&mut self, key: &Lsn) -> Option<LocalSchemaStage> {
        let removed = self.0.map.remove(key);
        self.0.release_if_empty();
        removed
    }
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature"
        )
    )]
    pub(crate) fn len(&self) -> usize {
        self.0.map.len()
    }
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.0.map.is_empty()
    }
    pub(crate) fn clear(&mut self) {
        // Removal advances before payload destruction; the backing and its
        // liability are released once the map is empty.
        while let Some(key) = self.0.map.keys().next().copied() {
            drop(self.0.map.remove(&key));
        }
        self.0.release_if_empty();
    }
}

pub(crate) struct Cell<V> {
    stage: Option<V>,
    tokens: usize,
}
pub(crate) struct OccupiedRegistry<K: Eq + Hash + Copy, V, P>(Backing<K, Cell<V>, P>);
impl<K: Eq + Hash + Copy, V, P> OccupiedRegistry<K, V, P> {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn new_arc(
        accountant: &crate::memory_accounting::AccountantHandle,
    ) -> Result<Arc<Mutex<Self>>>
    where
        P: RegistryFamily,
    {
        let final_layout = crate::memory_accounting::writer_adapter_type_layout::<Mutex<Self>>();
        let old_layout =
            crate::memory_accounting::writer_adapter_type_layout::<Mutex<HashMap<K, P>>>();
        let bytes = final_layout
            .size()
            .checked_sub(old_layout.size())
            .ok_or_else(capacity_error)?;
        let accountant = StoreAccountantHandle::new(accountant.clone());
        let attribution = P::attribution();
        let control = if accountant.is_writer() {
            // Each fixed occupied-registry constructor is 104 B of named open
            // residue. Map backing growth retains its existing writer charge.
            ControlCredit::admit_open_residue(&accountant, bytes, P::open_residue_operation())?
        } else {
            ControlCredit::admit_from_handle(
                &accountant,
                bytes,
                attribution.subsystem,
                attribution.operation,
                attribution.hint,
            )?
        };
        Ok(Arc::new(Mutex::new(Self(Backing::new(
            accountant,
            control,
            attribution,
        )))))
    }
    pub(crate) fn get(&self, key: &K) -> Option<&V> {
        self.0.map.get(key).and_then(|cell| cell.stage.as_ref())
    }
    pub(crate) fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.0.map.get_mut(key).and_then(|cell| cell.stage.as_mut())
    }
    pub(crate) fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn stages(&self) -> impl Iterator<Item = &V> {
        self.0.map.values().filter_map(|cell| cell.stage.as_ref())
    }
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature"
        )
    )]
    pub(crate) fn len(&self) -> usize {
        self.0
            .map
            .values()
            .filter(|cell| cell.stage.is_some())
            .count()
    }
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
        let cell = self.0.map.get_mut(key)?;
        let stage = cell.stage.take();
        if cell.tokens == 0 {
            self.0.map.remove(key);
        }
        self.0.release_if_empty();
        stage
    }
    pub(crate) fn clear(&mut self) {
        // Preserve all occupied cells and never allocate a collection of keys.
        loop {
            let key = self
                .0
                .map
                .iter()
                .find_map(|(key, cell)| cell.stage.is_some().then_some(*key));
            let Some(key) = key else {
                break;
            };
            drop(self.remove(&key));
        }
        self.0.map.retain(|_, cell| cell.tokens != 0);
        self.0.release_if_empty();
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn acquire(&mut self, key: K, detach: bool) -> Result<Option<V>> {
        if let Some(cell) = self.0.map.get_mut(&key) {
            let count = cell.tokens.checked_add(1).ok_or_else(capacity_error)?;
            cell.tokens = count;
            return Ok(if detach { cell.stage.take() } else { None });
        }
        self.0.reserve_key(&key)?;
        self.0.map.insert(
            key,
            Cell {
                stage: None,
                tokens: 1,
            },
        );
        Ok(None)
    }
    fn finish(&mut self, key: K, stage: Option<V>) -> Option<V> {
        let cell = self
            .0
            .map
            .get_mut(&key)
            .expect("live token retains its physical cell");
        let displaced = if let Some(stage) = stage {
            cell.stage.replace(stage)
        } else {
            None
        };
        cell.tokens = cell
            .tokens
            .checked_sub(1)
            .expect("live token owns one occupancy");
        if cell.tokens == 0 && cell.stage.is_none() {
            self.0.map.remove(&key);
        }
        self.0.release_if_empty();
        displaced
    }
}

/// Stack-only occupancy ownership. Its Arc prevents final registry teardown
/// while any outside-lock operation still owns a restoration/publication slot.
pub(crate) struct Token<K: Eq + Hash + Copy, V, P> {
    registry: Arc<Mutex<OccupiedRegistry<K, V, P>>>,
    key: K,
    armed: bool,
}
impl<K: Eq + Hash + Copy, V, P> Token<K, V, P> {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn detach_existing(
        registry: &Arc<Mutex<OccupiedRegistry<K, V, P>>>,
        key: K,
    ) -> Result<Option<(Self, V)>> {
        let mut locked = registry.lock();
        if !locked.contains_key(&key) {
            return Ok(None);
        }
        let stage = locked
            .acquire(key, true)?
            .expect("visible stage was detached under the same lock");
        Ok(Some((
            Self {
                registry: registry.clone(),
                key,
                armed: true,
            },
            stage,
        )))
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reserve(
        registry: &Arc<Mutex<OccupiedRegistry<K, V, P>>>,
        key: K,
    ) -> Result<Self> {
        registry.lock().acquire(key, false)?;
        Ok(Self {
            registry: registry.clone(),
            key,
            armed: true,
        })
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

pub(super) type PendingRegistry =
    OccupiedRegistry<TxId, PendingLocalSchemaStage, Pre2PendingLocalSchemaStage>;
pub(super) type PendingToken = Token<TxId, PendingLocalSchemaStage, Pre2PendingLocalSchemaStage>;
pub(crate) type ReceivedRegistry =
    OccupiedRegistry<Lsn, ReceivedSchemaStage, Pre2ReceivedSchemaStage>;
pub(super) type ReceivedToken = Token<Lsn, ReceivedSchemaStage, Pre2ReceivedSchemaStage>;

pub(super) struct LocalRegistration {
    registry: Arc<Mutex<LocalRegistry>>,
    lsn: Lsn,
    armed: bool,
}
impl LocalRegistration {
    pub(super) fn new(registry: &Arc<Mutex<LocalRegistry>>, lsn: Lsn) -> Self {
        Self {
            registry: registry.clone(),
            lsn,
            armed: true,
        }
    }
    pub(super) fn ownership_transferred(&mut self) {
        self.armed = false;
    }
}
impl Drop for LocalRegistration {
    fn drop(&mut self) {
        if self.armed {
            let removed = self.registry.lock().remove(&self.lsn);
            drop(removed);
        }
    }
}

// Lawful field-for-field pre2 value representations. These are never
// constructed or allocated: only their compiler layouts enter same-bucket
// subtraction. Database and public child layouts retain the pre2 contract;
// independent pre2/final layout receipts must verify that prerequisite.
#[allow(
    dead_code,
    reason = "LAYOUT-ONLY: never constructed; its layout sizes production memory charges"
)]
struct Pre2TemporaryMemoryReservation {
    accountant: crate::memory_accounting::AccountantHandle,
    bytes: usize,
}

#[allow(
    dead_code,
    reason = "LAYOUT-ONLY: never constructed; its layout sizes production memory charges"
)]
pub(crate) struct Pre2PreparedMemorySwap {
    accountant: crate::memory_accounting::AccountantHandle,
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

#[allow(
    dead_code,
    reason = "LAYOUT-ONLY: never constructed; its layout sizes production memory charges"
)]
pub(super) struct Pre2PendingLocalSchemaStage {
    working: Pre2Database,
    base_tables: HashMap<String, TableMeta>,
    base_event_bus: event_bus::EventBusDefinitions,
    base_triggers: BTreeMap<String, TriggerDeclaration>,
    base_table_generations: HashMap<String, u64>,
    temporary_memory: Pre2TemporaryMemoryReservation,
}

#[allow(
    dead_code,
    reason = "LAYOUT-ONLY: never constructed; its layout sizes production memory charges"
)]
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

#[allow(
    dead_code,
    reason = "LAYOUT-ONLY: never constructed; its layout sizes production memory charges"
)]
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

// Pre2 Database field declaration, never instantiated. Arc/Mutex registry
// fields have the same pointer layout under the selected private wrappers.
// It pins only the registry migration; every other field takes its current
// type, so `tx_mgr` follows the transaction host Database now holds.
#[allow(
    dead_code,
    reason = "LAYOUT-ONLY: never constructed; its layout sizes production memory charges"
)]
struct Pre2Database {
    /// This handle's identity, never an address. Every handle gets its own,
    /// including one derived by narrowing another's access, exactly as each
    /// had its own address before.
    id: DatabaseId,
    tx_mgr: super::DatabaseTransactionHost,
    relational_store: Arc<RelationalStore>,
    graph_store: Arc<GraphStore>,
    vector_store: Arc<VectorStore>,
    vector_schema_gates: Arc<VectorSchemaGates>,
    change_log: Arc<RwLock<Vec<ChangeLogEntry>>>,
    #[cfg(any(test, feature = "test-seams"))]
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
    retained_relational_source: Option<crate::persistence::RetainedReadSource>,
    // Descriptor-backed graph pages retained for the committed image.
    retained_graph_source: Option<crate::persistence::RetainedReadSource>,
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
    owner_thread: crate::owned_worker::CurrentThreadId,
    plugin: Arc<dyn DatabasePlugin>,
    owner_read_config: Arc<OwnerReadConfig>,
    declared_read_memory: crate::vector_read_memory::DeclaredReadRoot,
    /// The live owner-inspection service this open started, when it started
    /// one. It holds a shared handle onto the same stores rather than this
    /// value, so releasing the database releases the service with it.
    owner_read_service: Option<Arc<crate::owner_read::OwnerReadService>>,
    /// Why inspection is unavailable when serving was asked for and could not
    /// start. The database itself opened; the caller is told the operating
    /// system's own reason rather than discovering an empty result later.
    owner_read_startup_failure: Option<String>,
    #[cfg(any(test, feature = "test-seams"))]
    route_observer: Option<Arc<dyn crate::read_session::ReadSessionTestObserver>>,
    #[cfg(any(test, feature = "test-seams"))]
    kernel_observer: Option<Arc<dyn crate::read_session::ReadKernelTestObserver>>,
    /// Production-dead: counts how many times
    /// `retire_snapshot_free_vector_partitions` actually began its guarded
    /// removal pass, as opposed to skipping it because a cheap pre-check
    /// found nothing retirable. Shared across every scoped handle of the
    /// same database so a test can observe it regardless of which handle
    /// drove maintenance.
    #[cfg(any(test, feature = "test-seams"))]
    snapshot_free_retirement_pass_count: Arc<AtomicUsize>,
    access: AccessConstraints,
    accountant: crate::memory_accounting::AccountantHandle,
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
    /// The declared storage compaction policy (`SET STORAGE_COMPACTION`) and
    /// the test-only interval override. Shared, like the stamp above.
    storage_compaction_policy: Arc<Mutex<crate::database::StorageCompactionState>>,
    sync_watermark: Arc<AtomicLsn>,
    closed: AtomicBool,
    close_progress: Arc<Mutex<CloseProgress>>,
    close_changed: Arc<Condvar>,
    /// The first cleanup or handoff failure survives an owner-read drain
    /// timeout so the retry that releases resources still reports its cause.
    close_error: Mutex<Option<Error>>,
    #[cfg(any(test, feature = "test-seams"))]
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
    #[cfg(any(test, feature = "test-seams"))]
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
    read_image_memory: Option<crate::read_image_memory::ImageMemoryHandle>,
}

#[cfg(all(test, feature = "test-seams"))]
mod named_open_residue_tests {
    use super::*;
    use crate::memory_accounting::{
        WriterBudget, process_aggregate_snapshot, set_process_ceiling_for_test,
    };
    use crate::read_image_memory::ImageMemory;

    #[test]
    fn fixed_writer_open_controls_use_the_process_ceiling_and_refund_once() {
        struct CeilingReset;
        impl Drop for CeilingReset {
            fn drop(&mut self) {
                set_process_ceiling_for_test(None);
            }
        }
        let owner = Arc::new(MemoryAccountant::no_limit());
        let writer = WriterBudget::from_accountant(owner.clone()).unwrap();
        let local = owner.usage().used;
        let aggregate = process_aggregate_snapshot().0;
        owner.set_budget(Some(1)).unwrap();
        let assembly_writer = WriterBudget::from_accountant(owner.clone()).unwrap();
        let assembly_budget = assembly_writer.vector_accountant();
        assert_eq!(
            MemoryAccountant::writer_vector_adapter_type_layout_for_test().size(),
            64
        );
        assert_eq!(process_aggregate_snapshot().0, aggregate + 64);
        assert_eq!(owner.usage().used, local);
        assert!(matches!(
            assembly_budget.try_allocate_for(local + 2, "writer", "payload", "test"),
            Err(Error::MemoryBudgetExceeded { .. })
        ));
        assert!(
            matches!(
                crate::memory_accounting::AccountantHandle::from_host(owner.clone()).read_budget(),
                Err(Error::MemoryBudgetExceeded { .. })
            ),
            "ordinary working-memory adapter controls retain writer admission"
        );
        drop(assembly_budget);
        drop(assembly_writer);
        assert_eq!(process_aggregate_snapshot().0, aggregate);
        let registry = ReceivedRegistry::new_arc(
            &crate::memory_accounting::AccountantHandle::from_host(owner.clone()),
        )
        .unwrap();
        assert_eq!(process_aggregate_snapshot().0, aggregate + 104);
        assert_eq!(owner.usage().used, local);
        let pending = PendingRegistry::new_arc(
            &crate::memory_accounting::AccountantHandle::from_host(owner.clone()),
        )
        .unwrap();
        let local_registry = LocalRegistry::new_arc(
            &crate::memory_accounting::AccountantHandle::from_host(owner.clone()),
        )
        .unwrap();
        assert_eq!(process_aggregate_snapshot().0, aggregate + 3 * 104);
        assert_eq!(owner.usage().used, local);
        let image = ImageMemory::operation_writer(owner.clone().into()).unwrap();
        // The decode owner carries its per-call slot and prepaid output
        // credit (32 bytes) beside its funder and labels.
        assert_eq!(
            std::mem::size_of::<ImageMemory>() + 2 * std::mem::size_of::<usize>(),
            264
        );
        assert_eq!(process_aggregate_snapshot().0, aggregate + 3 * 104 + 264);
        assert_eq!(owner.usage().used, local);
        assert!(
            matches!(
                image.reserve(local + 2),
                Err(Error::MemoryBudgetExceeded { .. })
            ),
            "growing writer payload still obeys the declared limit"
        );
        let alias = registry.clone();
        drop(registry);
        assert_eq!(process_aggregate_snapshot().0, aggregate + 3 * 104 + 264);
        drop(image);
        assert_eq!(process_aggregate_snapshot().0, aggregate + 3 * 104);
        drop(pending);
        drop(local_registry);
        assert_eq!(process_aggregate_snapshot().0, aggregate + 104);
        set_process_ceiling_for_test(Some(aggregate + 104 + 263));
        let _reset = CeilingReset;
        assert!(
            matches!(ImageMemory::operation_writer(owner.clone().into()),
            Err(Error::MemoryBudgetExceeded { subsystem, operation, requested_bytes: 264, .. })
            if subsystem == "process_native_ceiling" && operation == "writer_decode_open_residue")
        );
        assert_eq!(process_aggregate_snapshot().0, aggregate + 104);
        drop(alias);
        assert_eq!(process_aggregate_snapshot().0, aggregate);
        set_process_ceiling_for_test(Some(aggregate + 103));
        assert!(
            matches!(ReceivedRegistry::new_arc(&crate::memory_accounting::AccountantHandle::from_host(owner.clone())),
            Err(Error::MemoryBudgetExceeded { subsystem, operation, requested_bytes: 104, .. })
            if subsystem == "process_native_ceiling" && operation == "received_registry_open_residue")
        );
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
        println!(
            "OPEN_RESIDUE assembly_budget_adapter=64 writer_decode=264 received_registry=104 pending_registry=104 local_registry=104 writer_limit=1 process_refusal=true single_refund=true"
        );
    }
}
