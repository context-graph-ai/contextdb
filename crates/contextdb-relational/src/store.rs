use crate::membership::MembershipImage;
use contextdb_core::read_memory::{ReadCredit, ReadMemoryBudget};
use contextdb_core::{
    ColumnType, DirectedValue, IndexKey, IndexKind, Lsn, RowId, SortDirection, TableMeta,
    TableName, TotalOrdAsc, TotalOrdDesc, TxId, Value, VersionedRow,
};
use parking_lot::{Mutex, RwLock};
use std::collections::{BTreeMap, HashMap, HashSet, hash_map::Entry};
use std::hash::BuildHasherDefault;
use std::hash::{Hash, Hasher};
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

// writer_reopen_and_doubling_keep_a_fixed_resident_envelope observes the private expiry directory.
#[cfg(feature = "test-seams")]
#[path = "store/resident_directory_test_seams.rs"]
mod resident_directory_test_seams;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    pub row_id: RowId,
    pub created_tx: TxId,
    pub deleted_tx: Option<TxId>,
}

/// The charge model in `contextdb-core` sizes one posting list from this, and
/// core cannot see this type. A change here that the charge does not follow
/// would under-charge every index in the store, so it fails the build instead.
const _: () = assert!(
    std::mem::size_of::<IndexEntry>() * 4 + 114 <= contextdb_core::INDEX_POSTING_SLOT_BYTES
);

impl IndexEntry {
    pub fn visible_at(&self, snapshot: contextdb_core::SnapshotId) -> bool {
        self.created_tx.0 <= snapshot.0 && self.deleted_tx.is_none_or(|tx| tx.0 > snapshot.0)
    }
}

/// A parameter-bound relational predicate which may be answered solely from
/// ordinary index postings.  This is deliberately not an authorization
/// predicate: callers must intersect its result with their own scope and
/// principal decision.
#[derive(Debug, Clone, PartialEq)]
pub enum UnauthorisedCandidatePredicate {
    Equality {
        column: String,
        value: Value,
    },
    FiniteIn {
        column: String,
        values: Vec<Value>,
    },
    Range {
        column: String,
        lower: Option<(Value, bool)>,
        upper: Option<(Value, bool)>,
    },
    And(Vec<Self>),
    Or(Vec<Self>),
}

/// Deterministic work charged while deriving relational candidates.  The
/// bounded reader can charge memory/work and cancel at every event.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UnauthorisedCandidateWork {
    pub index_probes: u64,
    pub index_entries: u64,
    pub set_operations: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnauthorisedCandidateWorkKind {
    IndexProbe,
    IndexEntry,
    SetOperation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnauthorisedCandidateControl {
    Continue,
    Cancel,
}

/// The only successful route is complete with respect to the supplied
/// predicate.  It says nothing about authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnauthorisedCandidateRoute {
    Complete {
        row_ids: Vec<RowId>,
        row_count: u64,
        work: UnauthorisedCandidateWork,
    },
    ProvablyEmpty {
        work: UnauthorisedCandidateWork,
    },
    /// No table rows were read. `recommended_columns` contains only predicate
    /// structure, so an explain surface can safely suggest CREATE INDEX.
    UnsupportedBeforeScan {
        recommended_columns: Vec<String>,
        work: UnauthorisedCandidateWork,
    },
    Cancelled {
        work: UnauthorisedCandidateWork,
    },
}

#[derive(Debug, Clone)]
struct ExactBucket {
    key: IndexKey,
    postings: Vec<IndexEntry>,
}

pub enum IndexReadRange<'a> {
    Current(crate::membership::MembershipRange<'a>),
    Historical(std::collections::btree_map::Range<'a, IndexKey, Vec<IndexEntry>>),
}
impl<'a> Iterator for IndexReadRange<'a> {
    type Item = (&'a IndexKey, &'a [IndexEntry]);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Current(entries) => entries
                .next()
                .map(|(key, entry)| (key, std::slice::from_ref(entry))),
            Self::Historical(entries) => entries
                .next()
                .map(|(key, entries)| (key, entries.as_slice())),
        }
    }
}

type ExactMap = HashMap<u64, Vec<ExactBucket>, BuildHasherDefault<IdentityHasher>>;
type RowPositionMaps = (
    HashMap<(TableName, RowId), usize>,
    HashMap<(TableName, RowId), Vec<usize>>,
);

const EXACT_FILTER_WORDS: usize = 8192;
/// The charge model in `contextdb-core` charges this filter with the table's
/// schema and cannot see its size from here.
const _: () = assert!(
    EXACT_FILTER_WORDS * std::mem::size_of::<u64>() == contextdb_core::EXACT_PROBE_FILTER_BYTES
);
const EXACT_FILTER_BITS: u64 = (EXACT_FILTER_WORDS as u64) * 64;
const EXACT_FILTER_MASK: u64 = EXACT_FILTER_BITS - 1;

#[derive(Default)]
struct IndexKeyHasher {
    state: u64,
}

impl IndexKeyHasher {
    fn mix(&mut self, value: u64) {
        let mixed = value
            .wrapping_add(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(self.state << 6)
            .wrapping_add(self.state >> 2);
        self.state ^= mixed;
    }
}

impl Hasher for IndexKeyHasher {
    fn finish(&self) -> u64 {
        self.state
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for chunk in &mut chunks {
            self.mix(u64::from_ne_bytes(
                chunk.try_into().expect("chunk size checked"),
            ));
        }
        let remainder = chunks.remainder();
        if !remainder.is_empty() {
            let mut tail = [0_u8; 8];
            tail[..remainder.len()].copy_from_slice(remainder);
            self.mix(u64::from_ne_bytes(tail));
        }
    }

    fn write_u8(&mut self, i: u8) {
        self.mix(i as u64);
    }

    fn write_u64(&mut self, i: u64) {
        self.mix(i);
    }

    fn write_i64(&mut self, i: i64) {
        self.mix(i as u64);
    }
}

#[derive(Default)]
struct IdentityHasher {
    state: u64,
}

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.state
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut padded = [0_u8; 8];
        let len = bytes.len().min(8);
        padded[..len].copy_from_slice(&bytes[..len]);
        self.state = u64::from_ne_bytes(padded);
    }

    fn write_u64(&mut self, i: u64) {
        self.state = i;
    }
}

fn hash_index_key(key: &IndexKey) -> Option<u64> {
    let mut hasher = IndexKeyHasher::default();
    for value in key {
        match value {
            DirectedValue::Asc(value) => {
                0_u8.hash(&mut hasher);
                hash_value(&value.0, &mut hasher)?;
            }
            DirectedValue::Desc(value) => {
                1_u8.hash(&mut hasher);
                hash_value(&value.0, &mut hasher)?;
            }
        }
    }
    Some(hasher.finish())
}

fn hash_value(value: &Value, hasher: &mut impl Hasher) -> Option<()> {
    match value {
        Value::Null => 0_u8.hash(hasher),
        Value::Bool(value) => {
            1_u8.hash(hasher);
            value.hash(hasher);
        }
        Value::Int64(value) => {
            2_u8.hash(hasher);
            value.hash(hasher);
        }
        Value::Float64(value) => {
            3_u8.hash(hasher);
            value.to_bits().hash(hasher);
        }
        Value::Text(value) => {
            4_u8.hash(hasher);
            value.hash(hasher);
        }
        Value::Uuid(value) => {
            5_u8.hash(hasher);
            value.hash(hasher);
        }
        Value::Timestamp(value) => {
            6_u8.hash(hasher);
            value.hash(hasher);
        }
        Value::TxId(value) => {
            7_u8.hash(hasher);
            value.0.hash(hasher);
        }
        Value::Json(_) | Value::Vector(_) => return None,
    }
    Some(())
}

/// Storage for one declared index. User-declared indexes keep the ordered
/// BTree; auto constraint indexes may be exact-only when callers only need
/// full-key probes.
#[derive(Debug)]
pub struct IndexStorage {
    pub columns: Vec<(String, SortDirection)>,
    /// Non-unique keys map to Vec<IndexEntry>; tie-break by row_id ascending
    /// is maintained at insert time (I18).
    pub tree: BTreeMap<IndexKey, Vec<IndexEntry>>,
    exact: ExactMap,
    /// Present only on an index that files EXACT postings. Only such an index
    /// ever marks or probes it, and an ordered index that allocated one anyway
    /// held 64 KiB it could never read -- six of the ten indexes on one
    /// cg-shaped `events` table, 393,216 B of it. Removing a retention is
    /// better than charging it. What remains is allocated with the index and
    /// charged with the index's declaration
    /// (`IndexDecl::estimated_bytes`), so the charge and the allocation
    /// happen at the same moment and a DROP gives back exactly what a CREATE
    /// took.
    exact_filter: Option<Box<[u64; EXACT_FILTER_WORDS]>>,
    exact_only: bool,
    /// Automatic constraint indexes with identical declared keys share one
    /// physical store. Keep that provenance separate from representation so
    /// an ordered user index with the same columns is never mistaken for the
    /// shared automatic store.
    automatic: bool,
    current: MembershipImage,
    maintain_current: bool,
    /// Whether `current` contains the complete live ordered membership.
    /// Loaded automatic composites start tree-only until an accountant is
    /// available, so opening them never creates an uncharged duplicate tree.
    current_complete: bool,
    source_backed: bool,
}

/// A complete, already-indexed replacement for one table.  Received-schema
/// application builds these before it takes any of the relational publication
/// locks, then publishes every affected table as one infallible memory step
/// after the matching durable transaction commits.
#[derive(Debug)]
pub struct TableProjection {
    pub table: String,
    pub meta: TableMeta,
    pub rows: Vec<VersionedRow>,
    pub indexes: HashMap<String, IndexStorage>,
    pub sync_sources: Vec<(RowId, Lsn, SyncSourceKind)>,
}

/// Complete relational replacement, including the derived row maps and sync
/// provenance sidecars.  It is constructed before durability; publication
/// only swaps these already-owned maps under the established lock order.
pub struct PreparedRelationalPublication {
    tables: HashMap<TableName, Vec<VersionedRow>>,
    table_meta: HashMap<TableName, TableMeta>,
    indexes: HashMap<TableName, HashMap<String, IndexStorage>>,
    row_positions: HashMap<(TableName, RowId), usize>,
    row_version_positions: HashMap<(TableName, RowId), Vec<usize>>,
    sync_source_lsns: HashMap<TableName, HashMap<RowId, Lsn>>,
    sync_source_kinds: HashMap<TableName, HashMap<RowId, SyncSourceKind>>,
}

impl PreparedRelationalPublication {
    pub fn admit_current_memberships(
        &mut self,
        budget: Arc<dyn ReadMemoryBudget>,
    ) -> contextdb_core::Result<()> {
        for indexes in self.indexes.values_mut() {
            for storage in indexes.values_mut() {
                storage.admit_current(budget.clone())?;
            }
        }
        Ok(())
    }
}

impl TableProjection {
    pub fn admit_current_memberships(
        &mut self,
        budget: Arc<dyn ReadMemoryBudget>,
    ) -> contextdb_core::Result<()> {
        for storage in self.indexes.values_mut() {
            storage.admit_current(budget.clone())?;
        }
        Ok(())
    }
}

impl Default for IndexStorage {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl IndexStorage {
    pub fn new(columns: Vec<(String, SortDirection)>) -> Self {
        Self::new_with_kind(columns, false, false)
    }

    pub fn new_exact_only(columns: Vec<(String, SortDirection)>) -> Self {
        Self::new_with_kind(columns, true, true)
    }

    /// An automatic constraint index over a single column files EXACT
    /// postings; `IndexDecl::is_exact_only` decides the same thing from the
    /// declaration, and the charge model relies on the two agreeing.
    fn new_automatic(columns: Vec<(String, SortDirection)>) -> Self {
        let exact_only = columns.len() == 1;
        Self::new_with_kind(columns, exact_only, true)
    }

    fn new_with_kind(
        columns: Vec<(String, SortDirection)>,
        exact_only: bool,
        automatic: bool,
    ) -> Self {
        Self {
            columns,
            tree: BTreeMap::new(),
            exact: ExactMap::default(),
            exact_filter: exact_only.then(|| Box::new([0; EXACT_FILTER_WORDS])),
            exact_only,
            automatic,
            current: MembershipImage::default(),
            maintain_current: true,
            current_complete: true,
            source_backed: false,
        }
    }

    /// Metadata-only descriptor; durable postings stay in the captured source.
    pub fn new_source_backed(
        columns: Vec<(String, SortDirection)>,
        exact_only: bool,
        automatic: bool,
    ) -> Self {
        Self {
            columns,
            tree: BTreeMap::new(),
            exact: ExactMap::default(),
            exact_filter: None,
            exact_only,
            automatic,
            current: MembershipImage::default(),
            maintain_current: false,
            current_complete: false,
            source_backed: true,
        }
    }

    pub fn uses_source(&self) -> bool {
        self.source_backed
    }

    fn discard_resident_postings(&mut self) {
        self.tree = BTreeMap::new();
        self.exact = ExactMap::default();
        self.exact_filter = None;
        self.current = MembershipImage::default();
        self.maintain_current = false;
        self.current_complete = false;
        self.source_backed = true;
    }

    /// Number of live (non-tombstoned) postings, useful for leak detection.
    pub fn total_entries(&self) -> u64 {
        if self.exact_only {
            self.exact
                .values()
                .flat_map(|buckets| buckets.iter())
                .map(|bucket| {
                    bucket
                        .postings
                        .iter()
                        .filter(|e| e.deleted_tx.is_none())
                        .count() as u64
                })
                .sum()
        } else {
            self.tree
                .values()
                .map(|v| v.iter().filter(|e| e.deleted_tx.is_none()).count() as u64)
                .sum()
        }
    }

    /// Total postings including tombstones (for DROP TABLE leak detection).
    pub fn total_entries_including_tombstones(&self) -> u64 {
        if self.exact_only {
            self.exact
                .values()
                .flat_map(|buckets| buckets.iter())
                .map(|bucket| bucket.postings.len() as u64)
                .sum()
        } else {
            self.tree.values().map(|v| v.len() as u64).sum()
        }
    }

    /// Insert a posting at the given key, placing it in row_id-ascending order.
    pub fn insert_posting(&mut self, key: IndexKey, entry: IndexEntry) {
        self.try_insert_posting(key, entry)
            .expect("unbudgeted index insertion");
    }

    fn try_insert_posting(
        &mut self,
        key: IndexKey,
        entry: IndexEntry,
    ) -> contextdb_core::Result<()> {
        if self.source_backed {
            return Ok(());
        }
        if !self.exact_only && self.maintain_current {
            self.current.insert(&key, entry.clone())?;
        }
        if self.exact_only {
            self.insert_exact_posting(key, entry);
            return Ok(());
        }
        let vec = self.tree.entry(key).or_default();
        let pos = vec
            .binary_search_by(|e| e.row_id.cmp(&entry.row_id))
            .unwrap_or_else(|i| i);
        vec.insert(pos, entry);
        Ok(())
    }

    /// Stamp `deleted_tx` on the posting matching `row_id` at `key`.
    pub fn tombstone_posting(&mut self, key: &IndexKey, row_id: RowId, deleted_tx: TxId) {
        if self.source_backed {
            return;
        }
        if !self.exact_only && self.maintain_current {
            self.current
                .remove(key, row_id, deleted_tx)
                .expect("detached membership construction has no budget");
        }
        if self.exact_only {
            self.tombstone_exact_posting(key, row_id, deleted_tx);
            return;
        }
        if let Some(vec) = self.tree.get_mut(key) {
            for entry in vec.iter_mut() {
                if entry.row_id == row_id && entry.deleted_tx.is_none() {
                    entry.deleted_tx = Some(deleted_tx);
                    return;
                }
            }
        }
    }

    fn insert_exact_posting(&mut self, key: IndexKey, entry: IndexEntry) {
        let Some(hash) = hash_index_key(&key) else {
            return;
        };
        self.mark_exact_filter(hash);
        let buckets = self.exact.entry(hash).or_default();
        let postings = if let Some(bucket) = buckets.iter_mut().find(|bucket| bucket.key == key) {
            &mut bucket.postings
        } else {
            buckets.push(ExactBucket {
                key,
                postings: Vec::new(),
            });
            &mut buckets.last_mut().expect("bucket just pushed").postings
        };
        let pos = postings
            .binary_search_by(|e| e.row_id.cmp(&entry.row_id))
            .unwrap_or_else(|i| i);
        postings.insert(pos, entry);
    }

    fn tombstone_exact_posting(&mut self, key: &IndexKey, row_id: RowId, deleted_tx: TxId) {
        if let Some(hash) = hash_index_key(key)
            && let Some(bucket) = self
                .exact
                .get_mut(&hash)
                .and_then(|buckets| buckets.iter_mut().find(|bucket| bucket.key == *key))
        {
            for entry in bucket.postings.iter_mut() {
                if entry.row_id == row_id && entry.deleted_tx.is_none() {
                    entry.deleted_tx = Some(deleted_tx);
                    break;
                }
            }
        }
    }

    pub fn exact_postings(&self, key: &IndexKey) -> Option<&Vec<IndexEntry>> {
        if self.exact_only {
            return hash_index_key(key).and_then(|hash| {
                if !self.exact_filter_may_contain(hash) {
                    return None;
                }
                self.exact
                    .get(&hash)
                    .and_then(|buckets| buckets.iter().find(|bucket| bucket.key == *key))
                    .map(|bucket| &bucket.postings)
            });
        }
        self.tree.get(key)
    }

    pub fn current_image(&self, snapshot: contextdb_core::SnapshotId) -> Option<&MembershipImage> {
        (!self.exact_only && self.current_complete && self.current.visible_at(snapshot))
            .then_some(&self.current)
    }

    pub fn admit_current(
        &mut self,
        budget: Arc<dyn ReadMemoryBudget>,
    ) -> contextdb_core::Result<()> {
        if self.source_backed {
            return Ok(());
        }

        if !self.exact_only {
            if self.current_complete {
                self.current.admit(budget)?;
            } else {
                self.current = self.reconstruct_current(budget)?;
                self.current_complete = true;
                self.maintain_current = true;
            }
        }
        Ok(())
    }

    fn current_for_preparation(
        &self,
        budget: Arc<dyn ReadMemoryBudget>,
    ) -> contextdb_core::Result<MembershipImage> {
        if self.current_complete {
            let mut current = self.current.clone();
            current.admit(budget)?;
            Ok(current)
        } else {
            self.reconstruct_current(budget)
        }
    }

    fn reconstruct_current(
        &self,
        budget: Arc<dyn ReadMemoryBudget>,
    ) -> contextdb_core::Result<MembershipImage> {
        let mut current = MembershipImage::with_budget(budget);
        for (key, entries) in &self.tree {
            for entry in entries {
                current.insert(key, entry.clone())?;
            }
        }
        Ok(current)
    }

    /// Both ordinary and bounded readers select the same physical image.
    pub fn read_range<'a>(
        &'a self,
        snapshot: contextdb_core::SnapshotId,
        lower: Bound<&'a [DirectedValue]>,
        upper: Bound<&'a [DirectedValue]>,
    ) -> IndexReadRange<'a> {
        match self.current_image(snapshot) {
            Some(image) => IndexReadRange::Current(image.range(lower, upper)),
            None => {
                IndexReadRange::Historical(self.tree.range::<[DirectedValue], _>((lower, upper)))
            }
        }
    }

    pub fn exact_only(&self) -> bool {
        self.exact_only
    }

    fn reserve_insert_postings(&mut self, additional: usize) {
        if self.exact_only {
            self.exact.reserve(additional);
        }
    }

    fn mark_exact_filter(&mut self, hash: u64) {
        let ((word_a, bit_a), (word_b, bit_b)) = exact_filter_slots(hash);
        let filter = self
            .exact_filter
            .get_or_insert_with(|| Box::new([0; EXACT_FILTER_WORDS]));
        filter[word_a] |= bit_a;
        filter[word_b] |= bit_b;
    }

    fn exact_filter_may_contain(&self, hash: u64) -> bool {
        // No filter means no exact posting has ever been stored here, so
        // nothing can be found; the probe is answered without touching the map.
        let Some(filter) = self.exact_filter.as_ref() else {
            return false;
        };
        let ((word_a, bit_a), (word_b, bit_b)) = exact_filter_slots(hash);
        (filter[word_a] & bit_a) != 0 && (filter[word_b] & bit_b) != 0
    }
}

fn exact_filter_slots(hash: u64) -> ((usize, u64), (usize, u64)) {
    let bit_a = hash & EXACT_FILTER_MASK;
    let mixed = hash
        .rotate_left(27)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0xBF58_476D_1CE4_E5B9);
    let bit_b = mixed & EXACT_FILTER_MASK;
    (filter_slot(bit_a), filter_slot(bit_b))
}

fn filter_slot(bit: u64) -> (usize, u64) {
    ((bit / 64) as usize, 1_u64 << (bit % 64))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncSourceKind {
    Pulled,
    AcceptedLocal,
    /// A hub restore invalidated this local row's former hub order. The row
    /// remains outbound work until it receives a fresh acknowledgement.
    AcceptedLocalPending,
}

type CapturedIndexRoots = BTreeMap<TxId, CapturedMembership>;
type CapturedTableRoots = HashMap<String, CapturedIndexRoots>;
type RetentionExpiryKey = (TableName, RowId, TxId);
type RetentionExpiryOwner = (Lsn, ReadCredit);
pub type PreparedRetentionExpiry = (RetentionExpiryKey, RetentionExpiryOwner);

#[derive(Default)]
struct MembershipCaptures {
    readers: usize,
    preparations: usize,
    roots: HashMap<TableName, CapturedTableRoots>,
}

struct CapturedMembership {
    image: MembershipImage,
    _credit: ReadCredit,
}

impl MembershipCaptures {
    fn release_unused(&mut self) {
        if self.readers == 0 && self.preparations == 0 {
            self.roots = HashMap::new();
        }
    }
}

/// Protect current roots before sampling a read snapshot. Commits retain their
/// predecessor roots until every concurrent source has captured its image.
pub struct MembershipCaptureGuard(Arc<Mutex<MembershipCaptures>>);
impl Drop for MembershipCaptureGuard {
    fn drop(&mut self) {
        let mut captures = self.0.lock();
        captures.readers -= 1;
        captures.release_unused();
    }
}

struct MembershipCapturePreparation(Arc<Mutex<MembershipCaptures>>);
impl Drop for MembershipCapturePreparation {
    fn drop(&mut self) {
        let mut captures = self.0.lock();
        captures.preparations -= 1;
        captures.release_unused();
    }
}

pub struct RelationalStore {
    membership_captures: Arc<Mutex<MembershipCaptures>>,
    pub tables: RwLock<HashMap<TableName, Vec<VersionedRow>>>,
    row_positions: RwLock<HashMap<(TableName, RowId), usize>>,
    row_version_positions: RwLock<HashMap<(TableName, RowId), Vec<usize>>>,
    /// Sync provenance sidecar: table → row_id → originating source LSN.
    /// Nested keying lets the hot apply path probe with a borrowed `&str`
    /// table name, avoiding a per-row `String` allocation.
    sync_source_lsns: RwLock<HashMap<TableName, HashMap<RowId, Lsn>>>,
    sync_source_kinds: RwLock<HashMap<TableName, HashMap<RowId, SyncSourceKind>>>,
    pub table_meta: RwLock<HashMap<TableName, TableMeta>>,
    /// table → index_name → IndexStorage. Physical storage is table-keyed so
    /// DML can maintain only the affected table's indexes without consulting
    /// metadata that DDL mutates separately.
    pub indexes: RwLock<HashMap<TableName, HashMap<String, IndexStorage>>>,
    /// Counts how many times `apply_changes`-style batch-level index-lock
    /// acquisitions have happened. The per-row commit path does not bump this
    /// counter; only coarse batch holders (`apply_changes` wrapping N rows) do.
    pub index_write_lock_count: AtomicU64,
    pub index_maintenance_visits: AtomicU64,
    pub open_index_maintenance_visits: AtomicU64,
    pub scan_rows_touched: AtomicU64,
    /// Index entries a source read through an ordered index walk. It counts
    /// what the index side of a read touched, which the row counter cannot
    /// show: a read served entirely through an index touches no scanned rows
    /// at all.
    pub index_entries_touched: AtomicU64,
    retention_expiries: RwLock<HashMap<RetentionExpiryKey, RetentionExpiryOwner>>,
    next_row_id: AtomicU64,
    disk_backed_committed_bodies: AtomicBool,
}

/// What an open store is holding, structure by structure, counted from the
/// structures themselves -- lengths times element sizes, plus the heap each
/// owned `String`/`Vec` allocated -- and never from the process's resident
/// set. An allocator keeps a freed heap mapped and rounds every request up to
/// a size class, so resident bytes over-state what is live and the slack is
/// not something a charge model can be asked to predict. This is the bill a
/// declared `MEMORY_LIMIT` has to cover.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RetainedStructureBytes {
    /// Every row version in `tables`, values and all.
    pub row_versions: usize,
    /// `row_positions`: one entry per live row, owning a copy of the table name.
    pub row_positions: usize,
    /// `row_version_positions`: one entry per row id, owning a copy of the
    /// table name and a heap `Vec` of that row's version positions.
    pub row_version_positions: usize,
    /// `sync_source_lsns` and `sync_source_kinds` together.
    pub sync_provenance: usize,
    /// Every ordered index's `tree`, keys and postings.
    pub index_trees: usize,
    /// Every exact-only index's `exact` map, keys and postings.
    pub index_exact: usize,
    /// The fixed exact-probe filter each index allocates, whether or not the
    /// index is one that ever reads it.
    pub index_exact_filters: usize,
    /// Every index's current membership image.
    pub index_membership: usize,
    /// Retained table/index map capacity and declared key columns; independent
    /// of row history, including map buckets retained after schema removal.
    pub index_descriptors: usize,
}

impl RetainedStructureBytes {
    pub fn total(&self) -> usize {
        self.row_versions
            .saturating_add(self.row_positions)
            .saturating_add(self.row_version_positions)
            .saturating_add(self.sync_provenance)
            .saturating_add(self.index_trees)
            .saturating_add(self.index_exact)
            .saturating_add(self.index_exact_filters)
            .saturating_add(self.index_membership)
            .saturating_add(self.index_descriptors)
    }
}

/// The allocation a `HashMap` of `len` entries holds: hashbrown rounds the
/// bucket count up to a power of two and keeps one control byte per bucket.
fn hash_table_bytes<K, V>(len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    let buckets = len.saturating_mul(8).div_ceil(7).next_power_of_two().max(8);
    buckets.saturating_mul(std::mem::size_of::<(K, V)>() + 1)
}

/// What a `BTreeMap` entry occupies: its key and value slots, plus the share
/// of the node header and child-pointer array the slot carries. A node holds
/// up to eleven pairs and splits at half full, so the header is spread over
/// six slots rather than eleven.
fn btree_entry_bytes<K, V>() -> usize {
    std::mem::size_of::<K>() + std::mem::size_of::<V>() + 16
}

fn index_key_heap_bytes(key: &IndexKey) -> usize {
    key.iter().fold(
        key.capacity()
            .saturating_mul(std::mem::size_of::<DirectedValue>()),
        |bytes, value| {
            let value = match value {
                DirectedValue::Asc(v) => &v.0,
                DirectedValue::Desc(v) => &v.0,
            };
            bytes.saturating_add(value.estimated_bytes())
        },
    )
}

fn postings_heap_bytes(postings: &[IndexEntry]) -> usize {
    postings
        .len()
        .max(4)
        .saturating_mul(std::mem::size_of::<IndexEntry>())
}

fn row_version_heap_bytes(row: &VersionedRow) -> usize {
    row.values.iter().fold(
        hash_table_bytes::<contextdb_core::ColName, Value>(row.values.len()),
        |bytes, (column, value)| {
            bytes
                .saturating_add(column.len())
                .saturating_add(value.estimated_bytes())
        },
    )
}

impl IndexStorage {
    /// What this index is holding, counted from its own structures.
    fn retained_structure_bytes(&self, into: &mut RetainedStructureBytes) {
        into.index_exact_filters = into.index_exact_filters.saturating_add(
            self.exact_filter
                .as_ref()
                .map_or(0, |filter| std::mem::size_of_val(filter.as_ref())),
        );
        for (key, postings) in &self.tree {
            into.index_trees = into
                .index_trees
                .saturating_add(btree_entry_bytes::<IndexKey, Vec<IndexEntry>>())
                .saturating_add(index_key_heap_bytes(key))
                .saturating_add(postings_heap_bytes(postings));
        }
        into.index_exact = into
            .index_exact
            .saturating_add(hash_table_bytes::<u64, Vec<ExactBucket>>(self.exact.len()));
        for buckets in self.exact.values() {
            into.index_exact = into.index_exact.saturating_add(
                buckets
                    .capacity()
                    .saturating_mul(std::mem::size_of::<ExactBucket>()),
            );
            for bucket in buckets {
                into.index_exact = into
                    .index_exact
                    .saturating_add(index_key_heap_bytes(&bucket.key))
                    .saturating_add(postings_heap_bytes(&bucket.postings));
            }
        }
        into.index_membership = into
            .index_membership
            .saturating_add(self.current.counted_bytes());
    }
}

/// Prepared current roots. The engine owns the commit gate between preparation
/// and publication, alongside its prepared vector and durable row changes.
pub struct PreparedMembershipBatch {
    _capture_preparation: MembershipCapturePreparation,
    /// New root plus the complete predecessor retained for readers that
    /// registered while preparation was in flight.
    roots: Vec<(TableName, String, MembershipImage, MembershipImage)>,
    _credit: ReadCredit,
}

impl Default for RelationalStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RelationalStore {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            row_positions: RwLock::new(HashMap::new()),
            row_version_positions: RwLock::new(HashMap::new()),
            sync_source_lsns: RwLock::new(HashMap::new()),
            sync_source_kinds: RwLock::new(HashMap::new()),
            table_meta: RwLock::new(HashMap::new()),
            indexes: RwLock::new(HashMap::new()),
            index_write_lock_count: AtomicU64::new(0),
            index_maintenance_visits: AtomicU64::new(0),
            open_index_maintenance_visits: AtomicU64::new(0),
            scan_rows_touched: AtomicU64::new(0),
            index_entries_touched: AtomicU64::new(0),
            retention_expiries: RwLock::new(HashMap::new()),
            membership_captures: Arc::new(Mutex::new(MembershipCaptures::default())),
            next_row_id: AtomicU64::new(1),
            disk_backed_committed_bodies: AtomicBool::new(false),
        }
    }

    /// Select the durable source backend before exposing the store. Committed
    /// rows, postings and sidecars remain in storage; callers supply an already
    /// captured source to reads. This method does not capture or publish roots.
    pub fn enable_disk_backed_committed_bodies(&self) {
        self.disk_backed_committed_bodies
            .store(true, Ordering::Release);
        *self.tables.write() = HashMap::new();
        *self.row_positions.write() = HashMap::new();
        *self.row_version_positions.write() = HashMap::new();
        *self.sync_source_lsns.write() = HashMap::new();
        *self.sync_source_kinds.write() = HashMap::new();
        *self.retention_expiries.write() = HashMap::new();
        for indexes in self.indexes.write().values_mut() {
            for storage in indexes.values_mut() {
                storage.discard_resident_postings();
            }
        }
    }

    pub fn uses_disk_backed_committed_bodies(&self) -> bool {
        self.disk_backed_committed_bodies.load(Ordering::Acquire)
    }

    fn committed_directory_row_for_meta(
        disk_backed: bool,
        _meta: Option<&TableMeta>,
        row: &VersionedRow,
    ) -> VersionedRow {
        VersionedRow {
            row_id: row.row_id,
            values: if disk_backed {
                HashMap::new()
            } else {
                row.values.clone()
            },
            created_tx: row.created_tx,
            deleted_tx: row.deleted_tx,
            lsn: row.lsn,
            created_at: row.created_at,
        }
    }

    /// Source reads return complete admitted bodies; no resident row image
    /// omits columns in either backend.
    pub fn committed_row_may_omit_columns(&self, _meta: &TableMeta, _row: &VersionedRow) -> bool {
        false
    }

    fn committed_directory_row(&self, table: &str, row: &VersionedRow) -> VersionedRow {
        let meta = self.table_meta.read().get(table).cloned();
        Self::committed_directory_row_for_meta(
            self.uses_disk_backed_committed_bodies(),
            meta.as_ref(),
            row,
        )
    }

    fn compact_projection_rows(
        disk_backed: bool,
        meta: &TableMeta,
        rows: Vec<VersionedRow>,
    ) -> Vec<VersionedRow> {
        if !disk_backed {
            return rows;
        }
        let _ = meta;
        Vec::new()
    }

    /// Bytes retained after a durable commit, used by the engine to return
    /// the admitted body allocation once Redb owns the canonical row.
    pub fn committed_directory_estimated_bytes(&self, table: &str, row: &VersionedRow) -> usize {
        if self.uses_disk_backed_committed_bodies() {
            0
        } else {
            self.committed_directory_row(table, row).estimated_bytes()
        }
    }

    /// Resident bytes for a row published with projected metadata. This is
    /// used before that metadata becomes visible, so it cannot consult the
    /// store's current table declaration.
    pub fn committed_projection_estimated_bytes(
        &self,
        meta: &TableMeta,
        row: &VersionedRow,
    ) -> usize {
        if self.uses_disk_backed_committed_bodies() {
            return 0;
        }

        Self::committed_directory_row_for_meta(
            self.uses_disk_backed_committed_bodies(),
            Some(meta),
            row,
        )
        .estimated_bytes()
    }

    pub fn committed_projection_values(
        &self,
        meta: &TableMeta,
        row: &VersionedRow,
    ) -> HashMap<String, Value> {
        Self::committed_directory_row_for_meta(
            self.uses_disk_backed_committed_bodies(),
            Some(meta),
            row,
        )
        .values
    }

    pub fn committed_directory_values(
        &self,
        table: &str,
        row: &VersionedRow,
    ) -> HashMap<String, Value> {
        self.committed_directory_row(table, row).values
    }

    pub fn begin_membership_capture(&self) -> MembershipCaptureGuard {
        self.membership_captures.lock().readers += 1;
        MembershipCaptureGuard(self.membership_captures.clone())
    }

    pub fn captured_membership(
        &self,
        table: &str,
        index: &str,
        snapshot: contextdb_core::SnapshotId,
    ) -> Option<MembershipImage> {
        self.membership_captures
            .lock()
            .roots
            .get(table)?
            .get(index)?
            .range(..=TxId(snapshot.0))
            .next_back()
            .map(|(_, root)| root.image.clone())
    }

    pub fn prepare_retention_expiries(
        &self,
        identities: Vec<(TableName, RowId, TxId, Lsn)>,
        budget: Arc<dyn ReadMemoryBudget>,
    ) -> contextdb_core::Result<Vec<PreparedRetentionExpiry>> {
        if self.uses_disk_backed_committed_bodies() {
            return Ok(Vec::new());
        }
        identities
            .into_iter()
            .map(|identity| {
                let credit =
                    membership_credit(budget.clone(), identity.0.len().saturating_add(192))?;
                Ok(((identity.0, identity.1, identity.2), (identity.3, credit)))
            })
            .collect()
    }

    pub fn publish_retention_expiries(&self, prepared: Vec<PreparedRetentionExpiry>) {
        if self.uses_disk_backed_committed_bodies() {
            return;
        }

        self.retention_expiries.write().extend(prepared);
    }

    pub fn is_retention_expired(&self, table: &str, row: RowId, created: TxId) -> bool {
        self.retention_expiries
            .read()
            .contains_key(&(table.to_owned(), row, created))
    }

    pub fn reclaimed_retention_expiry_lsns(
        &self,
        removed: &HashMap<TableName, HashSet<(RowId, TxId)>>,
    ) -> Vec<Lsn> {
        let expiries = self.retention_expiries.read();
        let mut reclaimed = HashSet::new();
        let mut surviving = HashSet::new();
        for ((table, row, created), (lsn, _)) in expiries.iter() {
            if removed
                .get(table)
                .is_some_and(|rows| rows.contains(&(*row, *created)))
            {
                reclaimed.insert(*lsn);
            } else {
                surviving.insert(*lsn);
            }
        }
        reclaimed.difference(&surviving).copied().collect()
    }

    pub fn prepare_memberships(
        &self,
        deletes: &[(TableName, RowId, TxId)],
        inserts: &[(TableName, VersionedRow)],
        budget: Arc<dyn ReadMemoryBudget>,
    ) -> contextdb_core::Result<PreparedMembershipBatch> {
        if self.uses_disk_backed_committed_bodies() {
            let credit = membership_credit(budget, 0)?;
            self.membership_captures.lock().preparations += 1;
            return Ok(PreparedMembershipBatch {
                roots: Vec::new(),
                _credit: credit,
                _capture_preparation: MembershipCapturePreparation(
                    self.membership_captures.clone(),
                ),
            });
        }

        let affected_count = deletes.len().saturating_add(inserts.len());
        let _affected_credit = membership_credit(
            budget.clone(),
            affected_count.saturating_mul(std::mem::size_of::<&str>()),
        )?;
        let mut affected = Vec::with_capacity(affected_count);
        affected.extend(deletes.iter().map(|(table, _, _)| table.as_str()));
        affected.extend(inserts.iter().map(|(table, _)| table.as_str()));
        affected.sort_unstable();
        affected.dedup();
        let indexes = self.indexes.read();
        let mut root_count = 0usize;
        let mut root_bytes = 0usize;
        for table in &affected {
            if let Some(table_indexes) = indexes.get(*table) {
                for (name, storage) in table_indexes {
                    if !storage.exact_only {
                        root_count += 1;
                        root_bytes = root_bytes
                            .saturating_add(std::mem::size_of::<(
                                TableName,
                                String,
                                MembershipImage,
                                MembershipImage,
                            )>())
                            .saturating_add(table.len())
                            .saturating_add(name.len());
                    }
                }
            }
        }
        let root_credit = membership_credit(budget.clone(), root_bytes)?;
        let mut roots = Vec::with_capacity(root_count);
        for table in affected {
            let Some(table_indexes) = indexes.get(table) else {
                continue;
            };
            for (name, storage) in table_indexes {
                if storage.exact_only {
                    continue;
                }
                let old = storage.current_for_preparation(budget.clone())?;
                let mut image = old.clone();
                for (deleted_table, row_id, tx) in deletes {
                    if deleted_table != table {
                        continue;
                    }
                    let _probe_credit = membership_credit(budget.clone(), table.len())?;
                    self.with_live_row(table, *row_id, |row| {
                        let _key_credit =
                            membership_key_credit(&storage.columns, &row.values, budget.clone())?;
                        let key = index_key_for_row(&storage.columns, &row.values);
                        image.remove(&key, *row_id, *tx)
                    })?;
                }
                for (inserted_table, row) in inserts {
                    if inserted_table != table {
                        continue;
                    }
                    let _key_credit =
                        membership_key_credit(&storage.columns, &row.values, budget.clone())?;
                    let key = index_key_for_row(&storage.columns, &row.values);
                    image.insert(
                        &key,
                        IndexEntry {
                            row_id: row.row_id,
                            created_tx: row.created_tx,
                            deleted_tx: row.deleted_tx,
                        },
                    )?;
                }
                roots.push((table.to_owned(), name.clone(), image, old));
            }
        }
        // Retain before durability even if no reader has started yet: one
        // may register while this prepared batch is awaiting publication.
        self.membership_captures.lock().preparations += 1;
        let capture_preparation = MembershipCapturePreparation(self.membership_captures.clone());
        {
            let mut captures = self.membership_captures.lock();
            for (table, name, _, old) in &roots {
                // New maps allocate spare buckets and a full B-tree node.
                // Reserve that capacity before retaining the first root too.
                let credit = membership_credit(
                    budget.clone(),
                    16 * std::mem::size_of::<(TxId, CapturedMembership)>()
                        + 8 * std::mem::size_of::<(String, CapturedTableRoots)>()
                        + 8 * std::mem::size_of::<(String, CapturedIndexRoots)>()
                        + table.len()
                        + name.len(),
                )?;
                captures
                    .roots
                    .entry(table.clone())
                    .or_default()
                    .entry(name.clone())
                    .or_default()
                    .insert(
                        old.since(),
                        CapturedMembership {
                            image: old.clone(),
                            _credit: credit,
                        },
                    );
            }
        }
        Ok(PreparedMembershipBatch {
            roots,
            _credit: root_credit,
            _capture_preparation: capture_preparation,
        })
    }

    /// No allocation or admission of current membership happens after durability.
    pub fn apply_prepared_rows(
        &self,
        deletes: &[(TableName, RowId, TxId)],
        inserts: &[(TableName, VersionedRow)],
        prepared: PreparedMembershipBatch,
    ) {
        {
            let mut indexes = self.indexes.write();
            for (table, name, _, _) in &prepared.roots {
                indexes
                    .get_mut(table)
                    .and_then(|indexes| indexes.get_mut(name))
                    .expect("prepared index remains under commit gate")
                    .maintain_current = false;
            }
        }
        self.apply_replacements_ref(deletes, inserts);
        self.publish_memberships(prepared);
    }

    pub fn publish_memberships(&self, prepared: PreparedMembershipBatch) {
        let mut indexes = self.indexes.write();
        for (table, name, image, _) in prepared.roots {
            let storage = indexes
                .get_mut(&table)
                .and_then(|indexes| indexes.get_mut(&name))
                .expect("prepared index remains under commit gate");
            storage.current = image;
            storage.maintain_current = true;
            storage.current_complete = true;
        }
    }

    /// Derive row ids from ordinary relational indexes after parameter binding.
    ///
    /// This method never reads `tables`, never evaluates ACL/scope/principal
    /// policy, and never upgrades an unsupported predicate into a scan.  The
    /// returned ids are sorted and de-duplicated so callers can intersect them
    /// with authorization and pass them directly to a bounded vector reader.
    pub fn unauthorised_index_candidates<F>(
        &self,
        table: &str,
        snapshot: contextdb_core::SnapshotId,
        predicate: &UnauthorisedCandidatePredicate,
        observe: F,
    ) -> UnauthorisedCandidateRoute
    where
        F: FnMut(UnauthorisedCandidateWorkKind, u64) -> UnauthorisedCandidateControl,
    {
        struct State<F> {
            work: UnauthorisedCandidateWork,
            observe: F,
            cancelled: bool,
        }
        impl<F> State<F>
        where
            F: FnMut(UnauthorisedCandidateWorkKind, u64) -> UnauthorisedCandidateControl,
        {
            fn charge(&mut self, kind: UnauthorisedCandidateWorkKind, amount: u64) -> bool {
                match kind {
                    UnauthorisedCandidateWorkKind::IndexProbe => {
                        self.work.index_probes = self.work.index_probes.saturating_add(amount)
                    }
                    UnauthorisedCandidateWorkKind::IndexEntry => {
                        self.work.index_entries = self.work.index_entries.saturating_add(amount)
                    }
                    UnauthorisedCandidateWorkKind::SetOperation => {
                        self.work.set_operations = self.work.set_operations.saturating_add(amount)
                    }
                }
                self.cancelled |= matches!(
                    (self.observe)(kind, amount),
                    UnauthorisedCandidateControl::Cancel
                );
                !self.cancelled
            }
        }

        fn safe_value(value: &Value) -> bool {
            !value.is_null() && !matches!(value, Value::Float64(number) if number.is_nan())
        }

        fn matches_index_column_type(value: &Value, column_type: &ColumnType) -> bool {
            matches!(
                (value, column_type),
                (Value::Int64(_), ColumnType::Integer)
                    | (Value::Float64(_), ColumnType::Real)
                    | (Value::Text(_), ColumnType::Text)
                    | (Value::Bool(_), ColumnType::Boolean)
                    | (Value::Uuid(_), ColumnType::Uuid)
                    | (Value::Timestamp(_), ColumnType::Timestamp)
                    | (Value::TxId(_), ColumnType::TxId)
            )
        }

        enum ResultSet {
            Complete(BTreeMap<RowId, ()>),
            Empty,
            Unsupported(Vec<String>),
            Cancelled,
        }

        fn combine<F>(
            parts: &[UnauthorisedCandidatePredicate],
            conjunction: bool,
            table: &str,
            snapshot: contextdb_core::SnapshotId,
            store: &RelationalStore,
            state: &mut State<F>,
        ) -> ResultSet
        where
            F: FnMut(UnauthorisedCandidateWorkKind, u64) -> UnauthorisedCandidateControl,
        {
            let mut accumulated: Option<BTreeMap<RowId, ()>> = None;
            for part in parts {
                let next = evaluate(part, table, snapshot, store, state);
                let next = match next {
                    ResultSet::Complete(set) => set,
                    ResultSet::Empty if conjunction => return ResultSet::Empty,
                    ResultSet::Empty => continue,
                    other @ ResultSet::Unsupported(_) | other @ ResultSet::Cancelled => {
                        return other;
                    }
                };
                let Some(current) = accumulated.take() else {
                    accumulated = Some(next);
                    continue;
                };
                if !state.charge(
                    UnauthorisedCandidateWorkKind::SetOperation,
                    current.len().saturating_add(next.len()) as u64,
                ) {
                    return ResultSet::Cancelled;
                }
                let combined = if conjunction {
                    current
                        .into_iter()
                        .filter(|(id, _)| next.contains_key(id))
                        .collect()
                } else {
                    current.into_iter().chain(next).collect()
                };
                accumulated = Some(combined);
            }
            match accumulated {
                Some(set) if set.is_empty() => ResultSet::Empty,
                Some(set) => ResultSet::Complete(set),
                None => ResultSet::Empty,
            }
        }

        fn evaluate<F>(
            predicate: &UnauthorisedCandidatePredicate,
            table: &str,
            snapshot: contextdb_core::SnapshotId,
            store: &RelationalStore,
            state: &mut State<F>,
        ) -> ResultSet
        where
            F: FnMut(UnauthorisedCandidateWorkKind, u64) -> UnauthorisedCandidateControl,
        {
            match predicate {
                UnauthorisedCandidatePredicate::And(parts) => {
                    return combine(parts, true, table, snapshot, store, state);
                }
                UnauthorisedCandidatePredicate::Or(parts) => {
                    return combine(parts, false, table, snapshot, store, state);
                }
                _ => {}
            }
            let (column, values, range) = match predicate {
                UnauthorisedCandidatePredicate::Equality { column, value } => {
                    if !safe_value(value) {
                        return ResultSet::Empty;
                    }
                    (column, vec![value.clone()], None)
                }
                UnauthorisedCandidatePredicate::FiniteIn { column, values } => (
                    column,
                    values
                        .iter()
                        .filter(|value| safe_value(value))
                        .cloned()
                        .collect(),
                    None,
                ),
                UnauthorisedCandidatePredicate::Range {
                    column,
                    lower,
                    upper,
                } => {
                    if lower.as_ref().is_some_and(|(value, _)| !safe_value(value))
                        || upper.as_ref().is_some_and(|(value, _)| !safe_value(value))
                    {
                        return ResultSet::Empty;
                    }
                    (column, Vec::new(), Some((lower, upper)))
                }
                _ => unreachable!("compound predicate handled above"),
            };
            if values.is_empty() && range.is_none() {
                return ResultSet::Empty;
            }

            let meta = match store.table_meta(table) {
                Some(meta) => meta,
                None => return ResultSet::Unsupported(vec![column.clone()]),
            };
            let Some(column_def) = meta
                .columns
                .iter()
                .find(|candidate| candidate.name == *column)
            else {
                return ResultSet::Unsupported(vec![column.clone()]);
            };
            // The binding layer must apply ordinary SQL coercion before this
            // index-only route. Refusing an uncoerced value as empty avoids a
            // cross-variant B-tree comparison and preserves typed equality.
            let type_matches = values
                .iter()
                .all(|value| matches_index_column_type(value, &column_def.column_type))
                && range.as_ref().is_none_or(|(lower, upper)| {
                    lower.as_ref().is_none_or(|(value, _)| {
                        matches_index_column_type(value, &column_def.column_type)
                    }) && upper.as_ref().is_none_or(|(value, _)| {
                        matches_index_column_type(value, &column_def.column_type)
                    })
                });
            if !type_matches {
                return ResultSet::Empty;
            }
            // Only ordinary declared indexes make this a vector candidate route.
            let declaration = meta.indexes.iter().find(|index| {
                index.kind == IndexKind::UserDeclared
                    && index.columns.len() == 1
                    && index.columns[0].0 == *column
            });
            let Some(declaration) = declaration else {
                return ResultSet::Unsupported(vec![column.clone()]);
            };
            let indexes = store.indexes.read();
            let Some(storage) = indexes
                .get(table)
                .and_then(|table_indexes| table_indexes.get(&declaration.name))
            else {
                return ResultSet::Unsupported(vec![column.clone()]);
            };
            if range.is_some() && storage.exact_only() {
                return ResultSet::Unsupported(vec![column.clone()]);
            }

            let mut output = BTreeMap::new();
            let direction = declaration.columns[0].1;
            let key = |value: Value| index_key_from_values(&declaration.columns, &[value]);
            let mut collect = |entries: &[IndexEntry], state: &mut State<F>| -> bool {
                for entry in entries {
                    if !state.charge(UnauthorisedCandidateWorkKind::IndexEntry, 1) {
                        return false;
                    }
                    if entry.visible_at(snapshot) {
                        output.insert(entry.row_id, ());
                    }
                }
                true
            };
            if let Some((lower, upper)) = range {
                if !state.charge(UnauthorisedCandidateWorkKind::IndexProbe, 1) {
                    return ResultSet::Cancelled;
                }
                let directed = |value: Value| match direction {
                    SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(value)),
                    SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(value)),
                };
                let lower = match lower {
                    Some((value, true)) => Bound::Included(vec![directed(value.clone())]),
                    Some((value, false)) => Bound::Excluded(vec![directed(value.clone())]),
                    None => Bound::Unbounded,
                };
                let upper = match upper {
                    Some((value, true)) => Bound::Included(vec![directed(value.clone())]),
                    Some((value, false)) => Bound::Excluded(vec![directed(value.clone())]),
                    None => Bound::Unbounded,
                };
                // Descending index keys reverse the SQL comparison direction.
                let (lower, upper) = match direction {
                    SortDirection::Asc => (lower, upper),
                    SortDirection::Desc => (upper, lower),
                };
                if let (
                    Bound::Included(left) | Bound::Excluded(left),
                    Bound::Included(right) | Bound::Excluded(right),
                ) = (&lower, &upper)
                    && left > right
                {
                    return ResultSet::Empty;
                }
                for (_, entries) in storage.read_range(
                    snapshot,
                    lower.as_ref().map(|v| v.as_slice()),
                    upper.as_ref().map(|v| v.as_slice()),
                ) {
                    if !collect(entries, state) {
                        return ResultSet::Cancelled;
                    }
                }
            } else {
                for value in values {
                    if !state.charge(UnauthorisedCandidateWorkKind::IndexProbe, 1) {
                        return ResultSet::Cancelled;
                    }
                    let key = key(value);
                    for (_, entries) in storage.read_range(
                        snapshot,
                        Bound::Included(key.as_slice()),
                        Bound::Included(key.as_slice()),
                    ) {
                        if !collect(entries, state) {
                            return ResultSet::Cancelled;
                        }
                    }
                }
            }
            if output.is_empty() {
                ResultSet::Empty
            } else {
                ResultSet::Complete(output)
            }
        }

        let mut state = State {
            work: UnauthorisedCandidateWork::default(),
            observe,
            cancelled: false,
        };
        match evaluate(predicate, table, snapshot, self, &mut state) {
            ResultSet::Complete(ids) => {
                let row_count = ids.len() as u64;
                UnauthorisedCandidateRoute::Complete {
                    row_ids: ids.into_keys().collect(),
                    row_count,
                    work: state.work,
                }
            }
            ResultSet::Empty => UnauthorisedCandidateRoute::ProvablyEmpty { work: state.work },
            ResultSet::Unsupported(columns) => UnauthorisedCandidateRoute::UnsupportedBeforeScan {
                recommended_columns: columns,
                work: state.work,
            },
            ResultSet::Cancelled => UnauthorisedCandidateRoute::Cancelled { work: state.work },
        }
    }

    pub fn new_row_id(&self) -> RowId {
        RowId(self.next_row_id.fetch_add(1, Ordering::SeqCst))
    }

    /// The next allocator value without reserving it.  Received-schema
    /// planning uses this to give a detached projected image its own cursor;
    /// a failed plan must never consume an ID from the live store.
    pub fn next_row_id(&self) -> RowId {
        RowId(self.next_row_id.load(Ordering::SeqCst))
    }

    /// Advance, but never rewind, the live allocator after a prepared image
    /// has become durable.  A concurrent ordinary transaction can allocate
    /// while schema preparation is in flight, so publication must use max
    /// rather than assigning the detached cursor unconditionally.
    pub fn fetch_max_next_row_id(&self, next_row_id: RowId) {
        self.next_row_id.fetch_max(next_row_id.0, Ordering::SeqCst);
    }

    pub fn sync_source_lsn(&self, table: &str, row_id: RowId) -> Option<Lsn> {
        self.sync_source_lsns
            .read()
            .get(table)
            .and_then(|by_row| by_row.get(&row_id))
            .copied()
    }

    pub fn sync_source_kind(&self, table: &str, row_id: RowId) -> Option<SyncSourceKind> {
        self.sync_source_kinds
            .read()
            .get(table)
            .and_then(|rows| rows.get(&row_id))
            .copied()
    }

    /// Snapshot sync provenance while preparing a complete received-schema
    /// replacement.  Publication never derives sidecars after durability.
    pub fn sync_source_sidecars_snapshot(
        &self,
    ) -> HashMap<(TableName, RowId), (Lsn, SyncSourceKind)> {
        let lsns = self.sync_source_lsns.read();
        let kinds = self.sync_source_kinds.read();
        lsns.iter()
            .flat_map(|(table, by_row)| {
                by_row.iter().filter_map(|(row_id, lsn)| {
                    kinds
                        .get(table)
                        .and_then(|by_row| by_row.get(row_id))
                        .copied()
                        .map(|kind| ((table.clone(), *row_id), (*lsn, kind)))
                })
            })
            .collect()
    }

    /// Copy provenance only for tables in a received-schema replacement.
    /// Selection happens while the source maps are borrowed, so an unrelated
    /// table's sidecars never enter the temporary image.
    pub fn sync_source_sidecars_for_tables(
        &self,
        tables: &HashSet<TableName>,
    ) -> HashMap<(TableName, RowId), (Lsn, SyncSourceKind)> {
        let lsns = self.sync_source_lsns.read();
        let kinds = self.sync_source_kinds.read();
        tables
            .iter()
            .filter_map(|table| {
                lsns.get(table)
                    .map(|by_row| (table, by_row, kinds.get(table)))
            })
            .flat_map(|(table, by_row, kinds)| {
                by_row.iter().filter_map(move |(row_id, lsn)| {
                    kinds
                        .and_then(|by_row| by_row.get(row_id))
                        .copied()
                        .map(|kind| ((table.clone(), *row_id), (*lsn, kind)))
                })
            })
            .collect()
    }

    pub fn set_sync_source_lsns(&self, entries: impl IntoIterator<Item = (TableName, RowId, Lsn)>) {
        if self.uses_disk_backed_committed_bodies() {
            return;
        }

        let mut source_lsns = self.sync_source_lsns.write();
        for (table, row_id, lsn) in entries {
            source_lsns.entry(table).or_default().insert(row_id, lsn);
        }
    }

    pub fn set_sync_source_kinds(
        &self,
        entries: impl IntoIterator<Item = (TableName, RowId, SyncSourceKind)>,
    ) {
        if self.uses_disk_backed_committed_bodies() {
            return;
        }

        let mut kinds = self.sync_source_kinds.write();
        for (table, row_id, kind) in entries {
            kinds.entry(table).or_default().insert(row_id, kind);
        }
    }

    pub fn clear_sync_source_lsns(&self, entries: impl IntoIterator<Item = (TableName, RowId)>) {
        let entries = entries.into_iter().collect::<Vec<_>>();
        let mut source_lsns = self.sync_source_lsns.write();
        for (table, row_id) in &entries {
            if let Some(by_row) = source_lsns.get_mut(table) {
                by_row.remove(row_id);
                if by_row.is_empty() {
                    source_lsns.remove(table);
                }
            }
        }
        let mut kinds = self.sync_source_kinds.write();
        for (table, row_id) in entries {
            if let Some(by_row) = kinds.get_mut(&table) {
                by_row.remove(&row_id);
                if by_row.is_empty() {
                    kinds.remove(&table);
                }
            }
        }
    }

    pub fn replace_sync_source_lsns(&self, entries: HashMap<(TableName, RowId), Lsn>) {
        if self.uses_disk_backed_committed_bodies() {
            return;
        }

        let mut nested: HashMap<TableName, HashMap<RowId, Lsn>> = HashMap::new();
        for ((table, row_id), lsn) in entries {
            nested.entry(table).or_default().insert(row_id, lsn);
        }
        *self.sync_source_lsns.write() = nested;
    }

    pub fn replace_sync_source_kinds(&self, entries: HashMap<(TableName, RowId), SyncSourceKind>) {
        if self.uses_disk_backed_committed_bodies() {
            return;
        }

        let mut nested = HashMap::new();
        for ((table, row_id), kind) in entries {
            nested
                .entry(table)
                .or_insert_with(HashMap::new)
                .insert(row_id, kind);
        }
        *self.sync_source_kinds.write() = nested;
    }

    /// Apply row inserts AND maintain every index (user-declared AND auto)
    /// on the affected tables. The index update runs under the same
    /// write-lock scope that the relational insert takes (implicitly held
    /// by the caller's commit_mutex).
    pub fn apply_inserts(&self, inserts: Vec<(TableName, VersionedRow)>) {
        self.apply_inserts_ref(&inserts);
    }

    pub fn apply_inserts_ref(&self, inserts: &[(TableName, VersionedRow)]) {
        self.apply_inserts_ref_skipping_indexes(inserts, &HashSet::new());
    }

    fn apply_inserts_ref_skipping_indexes(
        &self,
        inserts: &[(TableName, VersionedRow)],
        skip_index_rows: &HashSet<(TableName, RowId)>,
    ) {
        if self.uses_disk_backed_committed_bodies() {
            return;
        }

        // Stage the owned row copies BEFORE acquiring any write guard. The
        // deep VersionedRow copy is the same retained data we keep either way;
        // doing it outside the lock keeps the writer's lock-hold window tight
        // so concurrent readers don't stall on the per-row clone cost.
        let staged: Vec<(TableName, VersionedRow)> = inserts.to_vec();
        let mut row_counts = HashMap::<TableName, usize>::new();
        let mut index_counts = HashMap::<TableName, usize>::new();
        for (table_name, row) in &staged {
            *row_counts.entry(table_name.clone()).or_default() += 1;
            if !skip_index_rows.contains(&(table_name.clone(), row.row_id)) {
                *index_counts.entry(table_name.clone()).or_default() += 1;
            }
        }
        let mut indexes = self.indexes.write();
        let mut tables = self.tables.write();
        let mut row_positions = self.row_positions.write();
        let mut row_version_positions = self.row_version_positions.write();
        row_positions.reserve(staged.len());
        row_version_positions.reserve(staged.len());
        for (table_name, count) in &row_counts {
            tables
                .entry(table_name.clone())
                .or_default()
                .reserve(*count);
        }
        for (table_name, count) in &index_counts {
            if let Some(table_indexes) = indexes.get_mut(table_name) {
                for index in table_indexes.values_mut() {
                    index.reserve_insert_postings(*count);
                }
            }
        }
        let mut visits = 0u64;
        for (table_name, row) in staged {
            let entry = IndexEntry {
                row_id: row.row_id,
                created_tx: row.created_tx,
                deleted_tx: row.deleted_tx,
            };
            let skip_indexes = skip_index_rows.contains(&(table_name.clone(), row.row_id));
            if !skip_indexes && let Some(table_indexes) = indexes.get_mut(&table_name) {
                for idx in table_indexes.values_mut() {
                    visits = visits.saturating_add(1);
                    let key = index_key_for_row(&idx.columns, &row.values);
                    idx.insert_posting(key, entry.clone());
                }
            }
            let row_id = row.row_id;
            let rows = tables.entry(table_name.clone()).or_default();
            let row_position = rows.len();
            row_positions.insert((table_name.clone(), row_id), row_position);
            row_version_positions
                .entry((table_name.clone(), row_id))
                .or_default()
                .push(row_position);
            rows.push(self.committed_directory_row(&table_name, &row));
        }
        if visits > 0 {
            self.bump_index_maintenance_visits(visits);
        }
    }

    pub fn apply_deletes(&self, deletes: Vec<(TableName, RowId, TxId)>) {
        self.apply_deletes_ref(&deletes);
    }

    pub fn apply_deletes_ref(&self, deletes: &[(TableName, RowId, TxId)]) {
        self.apply_deletes_ref_skipping_indexes(deletes, &HashSet::new());
    }

    fn apply_deletes_ref_skipping_indexes(
        &self,
        deletes: &[(TableName, RowId, TxId)],
        skip_index_rows: &HashSet<(TableName, RowId)>,
    ) {
        if self.uses_disk_backed_committed_bodies() {
            return;
        }

        let mut indexes = self.indexes.write();
        let mut tables = self.tables.write();
        let row_positions = self.row_positions.read();
        let mut visits = 0u64;
        for (table_name, row_id, deleted_tx) in deletes {
            let row_position = row_positions
                .get(&(table_name.clone(), *row_id))
                .copied()
                .and_then(|position| {
                    tables
                        .get(table_name)
                        .and_then(|rows| rows.get(position))
                        .filter(|row| row.row_id == *row_id && row.deleted_tx.is_none())
                        .map(|_| position)
                });
            let Some(row_position) = row_position else {
                continue;
            };

            let skip_indexes = skip_index_rows.contains(&(table_name.clone(), *row_id));
            if !skip_indexes
                && let Some(values) = tables
                    .get(table_name)
                    .and_then(|rows| rows.get(row_position))
                    .map(|row| &row.values)
                && let Some(table_indexes) = indexes.get_mut(table_name)
            {
                for idx in table_indexes.values_mut() {
                    visits = visits.saturating_add(1);
                    let key = index_key_for_row(&idx.columns, values);
                    idx.tombstone_posting(&key, *row_id, *deleted_tx);
                }
            }
            if let Some(row) = tables
                .get_mut(table_name)
                .and_then(|rows| rows.get_mut(row_position))
                && row.row_id == *row_id
                && row.deleted_tx.is_none()
            {
                row.deleted_tx = Some(*deleted_tx);
            }
        }
        if visits > 0 {
            self.bump_index_maintenance_visits(visits);
        }
    }

    pub fn apply_replacements_ref(
        &self,
        deletes: &[(TableName, RowId, TxId)],
        inserts: &[(TableName, VersionedRow)],
    ) {
        if self.uses_disk_backed_committed_bodies() {
            return;
        }

        let skip_index_rows = self.same_index_key_replacements(deletes, inserts);
        self.apply_deletes_ref_skipping_indexes(deletes, &skip_index_rows);
        self.apply_inserts_ref_skipping_indexes(inserts, &skip_index_rows);
    }

    pub fn create_table(&self, name: &str, meta: TableMeta) {
        if !self.uses_disk_backed_committed_bodies() {
            self.tables.write().entry(name.to_string()).or_default();
        }
        self.table_meta.write().insert(name.to_string(), meta);
    }

    pub fn insert_loaded_row(&self, name: &str, row: VersionedRow) -> contextdb_core::Result<()> {
        if self.uses_disk_backed_committed_bodies() {
            return Ok(());
        }

        // Row-load during Database::open: populate every index (user-declared
        // + auto) so the rebuild stays in lockstep. Rows arrive in row_id
        // ascending order, satisfying I18.
        let index_names = self.index_names_for_table(name);
        {
            let mut indexes = self.indexes.write();
            let entry = IndexEntry {
                row_id: row.row_id,
                created_tx: row.created_tx,
                deleted_tx: row.deleted_tx,
            };
            let mut visits = 0u64;
            if let Some(table_indexes) = indexes.get_mut(name) {
                for index_name in &index_names {
                    let Some(idx) = table_indexes.get_mut(index_name) else {
                        continue;
                    };
                    visits = visits.saturating_add(1);
                    let key = index_key_for_row(&idx.columns, &row.values);
                    idx.try_insert_posting(key, entry.clone())?;
                }
            }
            if visits > 0 {
                self.bump_open_index_maintenance_visits(visits);
            }
        }
        let mut tables = self.tables.write();
        let rows = tables.entry(name.to_string()).or_default();
        let row_position = rows.len();
        self.row_positions
            .write()
            .insert((name.to_string(), row.row_id), row_position);
        self.row_version_positions
            .write()
            .entry((name.to_string(), row.row_id))
            .or_default()
            .push(row_position);
        rows.push(self.committed_directory_row(name, &row));
        Ok(())
    }

    fn index_names_for_table(&self, table: &str) -> Vec<String> {
        self.indexes
            .read()
            .get(table)
            .map(|indexes| indexes.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default()
    }

    fn same_index_key_replacements(
        &self,
        deletes: &[(TableName, RowId, TxId)],
        inserts: &[(TableName, VersionedRow)],
    ) -> HashSet<(TableName, RowId)> {
        let mut inserts_by_row = HashMap::new();
        let mut duplicate_inserts = HashSet::new();
        for (table, row) in inserts {
            let key = (table.clone(), row.row_id);
            if inserts_by_row.insert(key.clone(), row).is_some() {
                duplicate_inserts.insert(key);
            }
        }
        if inserts_by_row.is_empty() {
            return HashSet::new();
        }

        let mut delete_counts = HashMap::<(TableName, RowId), usize>::new();
        for (table, row_id, _) in deletes {
            *delete_counts.entry((table.clone(), *row_id)).or_default() += 1;
        }

        let indexes = self.indexes.read();
        let tables = self.tables.read();
        let row_positions = self.row_positions.read();
        let mut skip = HashSet::new();
        for (table, row_id, _) in deletes {
            let key = (table.clone(), *row_id);
            if delete_counts.get(&key).copied() != Some(1) || duplicate_inserts.contains(&key) {
                continue;
            }
            let Some(new_row) = inserts_by_row.get(&key) else {
                continue;
            };
            let Some(table_indexes) = indexes.get(table) else {
                continue;
            };
            let old_position = row_positions.get(&key).copied().and_then(|position| {
                tables
                    .get(table)
                    .and_then(|rows| rows.get(position))
                    .filter(|row| row.row_id == *row_id && row.deleted_tx.is_none())
                    .map(|_| position)
            });
            let Some(old_row) = old_position
                .and_then(|position| tables.get(table).and_then(|rows| rows.get(position)))
            else {
                continue;
            };
            if table_indexes.values().all(|index| {
                index_key_for_row(&index.columns, &old_row.values)
                    == index_key_for_row(&index.columns, &new_row.values)
            }) {
                skip.insert(key);
            }
        }
        skip
    }

    pub fn row_by_id(
        &self,
        table: &str,
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
    ) -> Option<VersionedRow> {
        let tables = self.tables.read();
        let rows = tables.get(table)?;
        let version_positions = self.row_version_positions.read();
        let positions = version_positions.get(&(table.to_string(), row_id))?;
        let candidate_count = positions.partition_point(|position| {
            rows.get(*position)
                .is_some_and(|row| row.row_id == row_id && row.created_tx.0 <= snapshot.0)
        });
        let position = *positions.get(candidate_count.checked_sub(1)?)?;
        rows.get(position)
            .filter(|row| row.row_id == row_id && row.visible_at(snapshot))
            .cloned()
    }

    /// Resolve one change-log row without walking the table. Prefer the exact
    /// local LSN and retain the historical latest-version fallback used when
    /// compacted logs outlive their exact row version.
    pub fn row_by_id_at_lsn_or_latest(
        &self,
        table: &str,
        row_id: RowId,
        lsn: Lsn,
    ) -> Option<VersionedRow> {
        let tables = self.tables.read();
        let rows = tables.get(table)?;
        let version_positions = self.row_version_positions.read();
        let positions = version_positions.get(&(table.to_string(), row_id))?;
        positions
            .iter()
            .rev()
            .filter_map(|position| rows.get(*position))
            .find(|row| row.row_id == row_id && row.lsn == lsn)
            .or_else(|| {
                positions
                    .iter()
                    .rev()
                    .filter_map(|position| rows.get(*position))
                    .find(|row| row.row_id == row_id)
            })
            .cloned()
    }

    pub(crate) fn row_by_id_before_clone<E>(
        &self,
        table: &str,
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
        before_clone: impl FnOnce(&VersionedRow) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<VersionedRow>, E> {
        let tables = self.tables.read();
        let Some(rows) = tables.get(table) else {
            return Ok(None);
        };
        let version_positions = self.row_version_positions.read();
        let Some(positions) = version_positions.get(&(table.to_string(), row_id)) else {
            return Ok(None);
        };
        let candidate_count = positions.partition_point(|position| {
            rows.get(*position)
                .is_some_and(|row| row.row_id == row_id && row.created_tx.0 <= snapshot.0)
        });
        let Some(position) = candidate_count
            .checked_sub(1)
            .and_then(|candidate| positions.get(candidate))
        else {
            return Ok(None);
        };
        let Some(row) = rows
            .get(*position)
            .filter(|row| row.row_id == row_id && row.visible_at(snapshot))
        else {
            return Ok(None);
        };
        before_clone(row)?;
        Ok(Some(row.clone()))
    }

    /// Where the version of `row_id` visible at `snapshot` sits in the rows
    /// the caller already holds. The row's own ordered version list names the
    /// position, so resolving one identity costs an ordered probe rather than
    /// a pass over the table.
    pub fn visible_version_position(
        &self,
        table: &str,
        rows: &[VersionedRow],
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
    ) -> Option<usize> {
        let version_positions = self.row_version_positions.read();
        let positions = version_positions.get(&(table.to_string(), row_id))?;
        let candidate_count = positions.partition_point(|position| {
            rows.get(*position)
                .is_some_and(|row| row.row_id == row_id && row.created_tx.0 <= snapshot.0)
        });
        let position = *positions.get(candidate_count.checked_sub(1)?)?;
        rows.get(position)
            .filter(|row| row.row_id == row_id && row.visible_at(snapshot))
            .map(|_| position)
    }

    pub fn live_row_by_id(&self, table: &str, row_id: RowId) -> Option<VersionedRow> {
        let tables = self.tables.read();
        let positions = self.row_positions.read();
        let position = *positions.get(&(table.to_string(), row_id))?;
        let rows = tables.get(table)?;
        rows.get(position)
            .filter(|row| row.row_id == row_id && row.deleted_tx.is_none())
            .cloned()
    }

    fn with_live_row(
        &self,
        table: &str,
        row_id: RowId,
        visit: impl FnOnce(&VersionedRow) -> contextdb_core::Result<()>,
    ) -> contextdb_core::Result<()> {
        let tables = self.tables.read();
        let positions = self.row_positions.read();
        if let Some(row) = positions
            .get(&(table.to_owned(), row_id))
            .and_then(|position| tables.get(table)?.get(*position))
            .filter(|row| row.row_id == row_id && row.deleted_tx.is_none())
        {
            visit(row)?;
        }
        Ok(())
    }

    pub fn live_rows_by_id(
        &self,
        keys: &[(TableName, RowId)],
    ) -> HashMap<TableName, HashMap<RowId, VersionedRow>> {
        let tables = self.tables.read();
        let positions = self.row_positions.read();
        let mut by_table = HashMap::<TableName, HashMap<RowId, VersionedRow>>::new();
        for (table, row_id) in keys {
            if by_table
                .get(table)
                .is_some_and(|by_row| by_row.contains_key(row_id))
            {
                continue;
            }
            let Some(position) = positions.get(&(table.clone(), *row_id)).copied() else {
                continue;
            };
            let Some(row) = tables
                .get(table)
                .and_then(|rows| rows.get(position))
                .filter(|row| row.row_id == *row_id && row.deleted_tx.is_none())
                .cloned()
            else {
                continue;
            };
            by_table
                .entry(table.clone())
                .or_default()
                .insert(*row_id, row);
        }
        by_table
    }

    fn rebuild_position_maps_for_table(
        table: &str,
        rows: &[VersionedRow],
        positions: &mut HashMap<(TableName, RowId), usize>,
        version_positions: &mut HashMap<(TableName, RowId), Vec<usize>>,
    ) {
        positions.retain(|(position_table, _), _| position_table != table);
        version_positions.retain(|(position_table, _), _| position_table != table);

        let table_name = table.to_string();
        let mut row_keys = Vec::new();
        for (position, row) in rows.iter().enumerate() {
            let key = (table_name.clone(), row.row_id);
            match version_positions.entry(key) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().push(position);
                }
                Entry::Vacant(entry) => {
                    row_keys.push(entry.key().clone());
                    entry.insert(vec![position]);
                }
            }
        }

        for key in row_keys {
            let latest_position = {
                let Some(row_positions) = version_positions.get_mut(&key) else {
                    continue;
                };
                row_positions.sort_by_key(|position| {
                    let row = &rows[*position];
                    (row.created_tx.0, *position)
                });
                row_positions.last().copied()
            };
            if let Some(position) = latest_position {
                positions.insert(key, position);
            }
        }
    }

    pub fn remove_rows_by_id(&self, table: &str, row_ids: &HashSet<RowId>) {
        if row_ids.is_empty() {
            return;
        }
        let mut tables = self.tables.write();
        let Some(rows) = tables.get_mut(table) else {
            return;
        };
        rows.retain(|row| !row_ids.contains(&row.row_id));
        {
            let mut source_lsns = self.sync_source_lsns.write();
            if let Some(by_row) = source_lsns.get_mut(table) {
                by_row.retain(|row_id, _| !row_ids.contains(row_id));
                if by_row.is_empty() {
                    source_lsns.remove(table);
                }
            }
        }
        {
            let mut kinds = self.sync_source_kinds.write();
            if let Some(by_row) = kinds.get_mut(table) {
                by_row.retain(|row_id, _| !row_ids.contains(row_id));
                if by_row.is_empty() {
                    kinds.remove(table);
                }
            }
        }
        let mut positions = self.row_positions.write();
        let mut version_positions = self.row_version_positions.write();
        Self::rebuild_position_maps_for_table(table, rows, &mut positions, &mut version_positions);
    }

    pub fn remove_row_versions(&self, table: &str, versions: &HashSet<(RowId, TxId)>) {
        if versions.is_empty() {
            return;
        }
        let mut tables = self.tables.write();
        let Some(rows) = tables.get_mut(table) else {
            return;
        };
        rows.retain(|row| !versions.contains(&(row.row_id, row.created_tx)));
        self.retention_expiries
            .write()
            .retain(|(name, row, created), _| {
                name != table || !versions.contains(&(*row, *created))
            });
        let remaining_live_row_ids = rows
            .iter()
            .filter(|row| {
                row.deleted_tx.is_none()
                    || self.is_retention_expired(table, row.row_id, row.created_tx)
            })
            .map(|row| row.row_id)
            .collect::<HashSet<_>>();
        {
            let mut source_lsns = self.sync_source_lsns.write();
            if let Some(by_row) = source_lsns.get_mut(table) {
                by_row.retain(|row_id, _| remaining_live_row_ids.contains(row_id));
                if by_row.is_empty() {
                    source_lsns.remove(table);
                }
            }
        }
        {
            let mut kinds = self.sync_source_kinds.write();
            if let Some(by_row) = kinds.get_mut(table) {
                by_row.retain(|row_id, _| remaining_live_row_ids.contains(row_id));
                if by_row.is_empty() {
                    kinds.remove(table);
                }
            }
        }
        let mut positions = self.row_positions.write();
        let mut version_positions = self.row_version_positions.write();
        Self::rebuild_position_maps_for_table(table, rows, &mut positions, &mut version_positions);
    }

    pub fn rebuild_row_position_maps(&self) {
        let tables = self.tables.read();
        let total_rows = tables.values().map(Vec::len).sum();
        let mut positions = self.row_positions.write();
        let mut version_positions = self.row_version_positions.write();
        positions.clear();
        version_positions.clear();
        positions.reserve(total_rows);
        version_positions.reserve(total_rows);
        for (table, rows) in tables.iter() {
            Self::rebuild_position_maps_for_table(
                table,
                rows,
                &mut positions,
                &mut version_positions,
            );
        }
    }

    pub fn max_row_id(&self) -> RowId {
        self.tables
            .read()
            .values()
            .flat_map(|rows| rows.iter().map(|row| row.row_id))
            .max()
            .unwrap_or(RowId(0))
    }

    pub fn set_next_row_id(&self, next_row_id: RowId) {
        self.next_row_id.store(next_row_id.0, Ordering::SeqCst);
    }

    pub fn drop_table(&self, name: &str) {
        self.retention_expiries
            .write()
            .retain(|(table, _, _), _| table != name);
        self.table_meta.write().remove(name);
        // Drop all indexes whose key-table matches; releases BTreeMap storage.
        let mut indexes = self.indexes.write();
        indexes.remove(name);
        self.tables.write().remove(name);
        self.row_positions
            .write()
            .retain(|(table, _), _| table != name);
        self.row_version_positions
            .write()
            .retain(|(table, _), _| table != name);
        self.sync_source_lsns.write().remove(name);
        self.sync_source_kinds.write().remove(name);
    }

    /// Register a new index storage for (table, name). The IndexDecl must
    /// already be present in the table's TableMeta (callers should
    /// `register_index_meta` before `create_index_storage`).
    pub fn create_index_storage(
        &self,
        table: &str,
        name: &str,
        columns: Vec<(String, SortDirection)>,
    ) {
        self.indexes
            .write()
            .entry(table.to_string())
            .or_default()
            .insert(
                name.to_string(),
                if self.uses_disk_backed_committed_bodies() {
                    IndexStorage::new_source_backed(columns, false, false)
                } else {
                    IndexStorage::new(columns)
                },
            );
    }

    pub fn create_index_storage_with_budget(
        &self,
        table: &str,
        name: &str,
        columns: Vec<(String, SortDirection)>,
        budget: Arc<dyn ReadMemoryBudget>,
    ) {
        let storage = if self.uses_disk_backed_committed_bodies() {
            IndexStorage::new_source_backed(columns, false, false)
        } else {
            let mut storage = IndexStorage::new(columns);
            storage.current = MembershipImage::with_budget(budget);
            storage
        };
        self.indexes
            .write()
            .entry(table.to_owned())
            .or_default()
            .insert(name.to_owned(), storage);
    }

    /// Register storage for an automatically generated constraint index.
    /// Single-column constraints retain the exact hash representation;
    /// composite PK/FK/unique constraints retain an ordered tree so their
    /// contiguous prefixes are seekable.
    pub fn create_exact_index_storage(
        &self,
        table: &str,
        name: &str,
        columns: Vec<(String, SortDirection)>,
    ) {
        self.create_auto_index_storage_inner(table, name, columns, None);
    }

    pub fn create_auto_index_storage_with_budget(
        &self,
        table: &str,
        name: &str,
        columns: Vec<(String, SortDirection)>,
        budget: Arc<dyn ReadMemoryBudget>,
    ) {
        self.create_auto_index_storage_inner(table, name, columns, Some(budget));
    }

    fn create_auto_index_storage_inner(
        &self,
        table: &str,
        name: &str,
        columns: Vec<(String, SortDirection)>,
        budget: Option<Arc<dyn ReadMemoryBudget>>,
    ) {
        let exact_only = columns.len() == 1;
        let mut indexes = self.indexes.write();
        let table_indexes = indexes.entry(table.to_string()).or_default();
        if table_indexes.iter().any(|(existing_name, storage)| {
            existing_name != name && storage.automatic && storage.columns == columns
        }) {
            return;
        }
        if let Some(existing) = table_indexes.get(name) {
            if existing.exact_only() == exact_only && existing.columns == columns {
                return;
            }
            return;
        }
        let mut storage = if self.uses_disk_backed_committed_bodies() {
            IndexStorage::new_source_backed(columns, exact_only, true)
        } else {
            IndexStorage::new_automatic(columns)
        };
        if !exact_only && !self.uses_disk_backed_committed_bodies() {
            if let Some(budget) = budget {
                storage.current = MembershipImage::with_budget(budget);
            } else {
                // Database loading has no accountant at this API boundary.
                // Keep only the row-charged ordered tree; a bounded reader or
                // the first publication reconstructs a charged current root.
                storage.maintain_current = false;
                storage.current_complete = false;
            }
        }
        table_indexes.insert(name.to_string(), storage);
    }

    /// Remove an index storage. Called from DROP INDEX / CASCADE.
    pub fn drop_index_storage(&self, table: &str, name: &str) {
        self.indexes
            .write()
            .get_mut(table)
            .and_then(|indexes| indexes.remove(name));
    }

    /// Build (or rebuild) an index storage by scanning the current table rows.
    /// Used after Database::open completes rebuilding TableMeta.indexes but
    /// before the executor sees queries. Iterates in row_id-ascending order
    /// to preserve I18 tie-break stability.
    pub fn rebuild_index(&self, table: &str, name: &str) {
        self.rebuild_index_inner(table, name, false);
    }

    pub fn rebuild_index_preserving_current(&self, table: &str, name: &str) {
        self.rebuild_index_inner(table, name, true);
    }

    fn rebuild_index_inner(&self, table: &str, name: &str, preserve_current: bool) {
        if self.uses_disk_backed_committed_bodies() {
            return;
        }

        let mut indexes = self.indexes.write();
        let Some(table_indexes) = indexes.get_mut(table) else {
            return;
        };
        let Some(existing) = table_indexes.get(name) else {
            return;
        };
        let columns = existing.columns.clone();
        let mut rebuilt =
            IndexStorage::new_with_kind(columns.clone(), existing.exact_only, existing.automatic);
        if preserve_current {
            rebuilt.current = existing.current.clone();
            rebuilt.maintain_current = false;
            rebuilt.current_complete = existing.current_complete;
        } else if !existing.current_complete {
            rebuilt.maintain_current = false;
            rebuilt.current_complete = false;
        }
        let tables = self.tables.read();
        if let Some(rows) = tables.get(table) {
            let mut sorted: Vec<&VersionedRow> = rows.iter().collect();
            sorted.sort_by_key(|r| r.row_id);
            for row in sorted {
                let key = index_key_for_row(&columns, &row.values);
                rebuilt.insert_posting(
                    key,
                    IndexEntry {
                        row_id: row.row_id,
                        created_tx: row.created_tx,
                        deleted_tx: row.deleted_tx,
                    },
                );
            }
        }
        rebuilt.maintain_current = rebuilt.current_complete;
        table_indexes.insert(name.to_string(), rebuilt);
    }

    /// Build every physical index for a projected table before the caller
    /// publishes it.  Keeping this work outside the store locks prevents an
    /// index-planned reader from observing a newly published TableMeta before
    /// its matching postings exist.
    pub fn projected_index_storage(
        meta: &TableMeta,
        rows: &[VersionedRow],
    ) -> HashMap<String, IndexStorage> {
        Self::projected_index_storage_inner(meta, rows, None).expect("unbudgeted index projection")
    }

    pub fn projected_index_storage_with_budget(
        meta: &TableMeta,
        rows: &[VersionedRow],
        budget: Arc<dyn ReadMemoryBudget>,
    ) -> contextdb_core::Result<HashMap<String, IndexStorage>> {
        Self::projected_index_storage_inner(meta, rows, Some(budget))
    }

    fn projected_index_storage_inner(
        meta: &TableMeta,
        rows: &[VersionedRow],
        budget: Option<Arc<dyn ReadMemoryBudget>>,
    ) -> contextdb_core::Result<HashMap<String, IndexStorage>> {
        let mut indexes = HashMap::new();
        let mut automatic_columns = Vec::<Vec<(String, SortDirection)>>::new();
        for decl in &meta.indexes {
            // Match create_exact_index_storage: duplicate auto constraint
            // indexes with identical keys deliberately share no second
            // physical store.
            if decl.kind == IndexKind::Auto
                && automatic_columns
                    .iter()
                    .any(|columns| columns == &decl.columns)
            {
                continue;
            }
            let mut storage = match decl.kind {
                IndexKind::Auto => IndexStorage::new_automatic(decl.columns.clone()),
                IndexKind::UserDeclared => IndexStorage::new(decl.columns.clone()),
            };
            if decl.kind == IndexKind::Auto {
                automatic_columns.push(decl.columns.clone());
            }
            if let Some(budget) = &budget {
                storage.current = MembershipImage::with_budget(budget.clone());
            }
            indexes.insert(decl.name.clone(), storage);
        }

        let mut sorted: Vec<&VersionedRow> = rows.iter().collect();
        sorted.sort_by_key(|row| row.row_id);
        for storage in indexes.values_mut() {
            for row in &sorted {
                let key = index_key_for_row(&storage.columns, &row.values);
                storage.try_insert_posting(
                    key,
                    IndexEntry {
                        row_id: row.row_id,
                        created_tx: row.created_tx,
                        deleted_tx: row.deleted_tx,
                    },
                )?;
            }
        }
        Ok(indexes)
    }

    /// Construct one complete table replacement while no relational write
    /// locks are held.  Index construction is intentionally part of the
    /// fallible-preparation side of received-schema staging, never the
    /// post-durability publication side.
    pub fn table_projection(
        table: impl Into<String>,
        meta: TableMeta,
        rows: Vec<VersionedRow>,
    ) -> TableProjection {
        let indexes = Self::projected_index_storage(&meta, &rows);
        TableProjection {
            table: table.into(),
            meta,
            rows,
            indexes,
            sync_sources: Vec::new(),
        }
    }

    pub fn with_sync_sources(
        mut projection: TableProjection,
        sync_sources: Vec<(RowId, Lsn, SyncSourceKind)>,
    ) -> TableProjection {
        projection.sync_sources = sync_sources;
        projection
    }

    /// Schema preparation for durable stores never builds resident postings.
    pub fn projected_source_index_storage(meta: &TableMeta) -> HashMap<String, IndexStorage> {
        let mut indexes = HashMap::new();
        for declaration in &meta.indexes {
            let automatic = declaration.kind == IndexKind::Auto;
            if automatic
                && indexes.values().any(|storage: &IndexStorage| {
                    storage.automatic && storage.columns == declaration.columns
                })
            {
                continue;
            }
            indexes.insert(
                declaration.name.clone(),
                IndexStorage::new_source_backed(
                    declaration.columns.clone(),
                    automatic && declaration.columns.len() == 1,
                    automatic,
                ),
            );
        }
        indexes
    }

    fn position_maps_for_tables(tables: &HashMap<TableName, Vec<VersionedRow>>) -> RowPositionMaps {
        let capacity = tables.values().map(Vec::len).sum();
        let mut positions = HashMap::with_capacity(capacity);
        let mut version_positions = HashMap::with_capacity(capacity);
        for (table, rows) in tables {
            Self::rebuild_position_maps_for_table(
                table,
                rows,
                &mut positions,
                &mut version_positions,
            );
        }
        (positions, version_positions)
    }

    /// Snapshot and prebuild a complete received-schema relational replacement.
    /// All index construction, row-map construction, sidecar reconciliation,
    /// and drop lifecycle work happens here, before durability.
    pub fn prepare_received_schema_publication(
        &self,
        projections: Vec<TableProjection>,
        dropped_tables: impl IntoIterator<Item = String>,
    ) -> PreparedRelationalPublication {
        if self.uses_disk_backed_committed_bodies() {
            let mut table_meta = self.table_meta.read().clone();
            for table in dropped_tables {
                table_meta.remove(&table);
            }
            for projection in projections {
                table_meta.insert(projection.table, projection.meta);
            }
            let indexes = table_meta
                .iter()
                .map(|(table, meta)| (table.clone(), Self::projected_source_index_storage(meta)))
                .collect();
            return PreparedRelationalPublication {
                tables: HashMap::new(),
                table_meta,
                indexes,
                row_positions: HashMap::new(),
                row_version_positions: HashMap::new(),
                sync_source_lsns: HashMap::new(),
                sync_source_kinds: HashMap::new(),
            };
        }

        let mut tables = self.tables.read().clone();
        let mut table_meta = self.table_meta.read().clone();
        let mut source_lsns = self.sync_source_lsns.read().clone();
        let mut source_kinds = self.sync_source_kinds.read().clone();
        for table in dropped_tables {
            tables.remove(&table);
            table_meta.remove(&table);
            source_lsns.remove(&table);
            source_kinds.remove(&table);
        }
        for projection in projections {
            source_lsns.remove(&projection.table);
            source_kinds.remove(&projection.table);
            if !projection.sync_sources.is_empty() {
                let mut lsns = HashMap::with_capacity(projection.sync_sources.len());
                let mut kinds = HashMap::with_capacity(projection.sync_sources.len());
                for (row_id, lsn, kind) in projection.sync_sources {
                    lsns.insert(row_id, lsn);
                    kinds.insert(row_id, kind);
                }
                source_lsns.insert(projection.table.clone(), lsns);
                source_kinds.insert(projection.table.clone(), kinds);
            }
            tables.insert(projection.table.clone(), projection.rows);
            table_meta.insert(projection.table.clone(), projection.meta);
        }
        let indexes = table_meta
            .iter()
            .map(|(table, meta)| {
                let rows = tables.get(table).map(Vec::as_slice).unwrap_or_default();
                (table.clone(), Self::projected_index_storage(meta, rows))
            })
            .collect();
        if self.uses_disk_backed_committed_bodies() {
            for (table, rows) in &mut tables {
                if let Some(meta) = table_meta.get(table) {
                    let full_rows = std::mem::take(rows);
                    *rows = Self::compact_projection_rows(true, meta, full_rows);
                }
            }
        }
        let (row_positions, row_version_positions) = Self::position_maps_for_tables(&tables);
        PreparedRelationalPublication {
            tables,
            table_meta,
            indexes,
            row_positions,
            row_version_positions,
            sync_source_lsns: source_lsns,
            sync_source_kinds: source_kinds,
        }
    }

    /// Infallibly publish a fully prebuilt received-schema replacement.  Do
    /// not add any construction, collection, or per-row mutation here: this
    /// runs only after the matching durable transaction has succeeded.
    pub fn publish_prepared_received_schema(&self, publication: PreparedRelationalPublication) {
        let mut indexes = self.indexes.write();
        let mut tables = self.tables.write();
        let mut table_meta = self.table_meta.write();
        let mut positions = self.row_positions.write();
        let mut version_positions = self.row_version_positions.write();
        let mut source_lsns = self.sync_source_lsns.write();
        let mut source_kinds = self.sync_source_kinds.write();
        *indexes = publication.indexes;
        *tables = publication.tables;
        self.retention_expiries
            .write()
            .retain(|(table, row, created), _| {
                tables.get(table).is_some_and(|rows| {
                    rows.iter()
                        .any(|entry| entry.row_id == *row && entry.created_tx == *created)
                })
            });
        *table_meta = publication.table_meta;
        *positions = publication.row_positions;
        *version_positions = publication.row_version_positions;
        *source_lsns = publication.sync_source_lsns;
        *source_kinds = publication.sync_source_kinds;
    }

    fn publish_source_projections(
        &self,
        projections: Vec<TableProjection>,
        dropped: &HashSet<String>,
    ) {
        let mut all_indexes = self.indexes.write();
        let mut table_meta = self.table_meta.write();
        for table in dropped {
            all_indexes.remove(table);
            table_meta.remove(table);
        }
        for mut projection in projections {
            for storage in projection.indexes.values_mut() {
                storage.discard_resident_postings();
            }
            table_meta.insert(projection.table.clone(), projection.meta);
            all_indexes.insert(projection.table, projection.indexes);
        }
    }

    /// Publish complete replacements for every affected table.  This method
    /// has no validation or allocation path: callers prepare the complete
    /// projections before Redb commits, so once durability succeeds this is
    /// only lock-ordered memory replacement.
    pub fn publish_table_projections(&self, projections: Vec<TableProjection>) {
        if self.uses_disk_backed_committed_bodies() {
            self.publish_source_projections(projections, &HashSet::new());
            return;
        }

        if projections.is_empty() {
            return;
        }

        // Keep the established relational writer order for the entire batch,
        // rather than taking it once per table and briefly exposing a mixed
        // received-schema generation to a concurrent reader.
        let mut all_indexes = self.indexes.write();
        let mut tables = self.tables.write();
        let mut table_meta = self.table_meta.write();
        let mut positions = self.row_positions.write();
        let mut version_positions = self.row_version_positions.write();
        for projection in projections {
            let table = projection.table;
            let rows = Self::compact_projection_rows(
                self.uses_disk_backed_committed_bodies(),
                &projection.meta,
                projection.rows,
            );
            tables.insert(table.clone(), rows);
            table_meta.insert(table.clone(), projection.meta);
            all_indexes.insert(table.clone(), projection.indexes);
            positions.retain(|(entry_table, _), _| entry_table != &table);
            version_positions.retain(|(entry_table, _), _| entry_table != &table);
            if let Some(rows) = tables.get(&table) {
                Self::rebuild_position_maps_for_table(
                    &table,
                    rows,
                    &mut positions,
                    &mut version_positions,
                );
            }
        }
    }

    /// Publish one received-schema dependency closure under the same lock
    /// order as the complete-image path. Sync provenance belongs to each
    /// selected table, and dropped members disappear in this same memory
    /// publication after their durable transaction has committed.
    pub fn publish_received_table_projections(
        &self,
        projections: Vec<TableProjection>,
        dropped: &std::collections::HashSet<String>,
    ) {
        if self.uses_disk_backed_committed_bodies() {
            self.publish_source_projections(projections, dropped);
            return;
        }

        if projections.is_empty() && dropped.is_empty() {
            return;
        }
        let mut all_indexes = self.indexes.write();
        let mut tables = self.tables.write();
        let mut table_meta = self.table_meta.write();
        let mut positions = self.row_positions.write();
        let mut version_positions = self.row_version_positions.write();
        let mut source_lsns = self.sync_source_lsns.write();
        let mut source_kinds = self.sync_source_kinds.write();
        for table in dropped {
            all_indexes.remove(table);
            tables.remove(table);
            table_meta.remove(table);
            positions.retain(|(entry_table, _), _| entry_table != table);
            version_positions.retain(|(entry_table, _), _| entry_table != table);
            source_lsns.remove(table);
            source_kinds.remove(table);
        }
        for projection in projections {
            let table = projection.table;
            let rows = Self::compact_projection_rows(
                self.uses_disk_backed_committed_bodies(),
                &projection.meta,
                projection.rows,
            );
            tables.insert(table.clone(), rows);
            table_meta.insert(table.clone(), projection.meta);
            all_indexes.insert(table.clone(), projection.indexes);
            positions.retain(|(entry_table, _), _| entry_table != &table);
            version_positions.retain(|(entry_table, _), _| entry_table != &table);
            Self::rebuild_position_maps_for_table(
                &table,
                tables.get(&table).expect("published table exists"),
                &mut positions,
                &mut version_positions,
            );
            source_lsns.insert(
                table.clone(),
                projection
                    .sync_sources
                    .iter()
                    .map(|(row, lsn, _)| (*row, *lsn))
                    .collect(),
            );
            source_kinds.insert(
                table,
                projection
                    .sync_sources
                    .into_iter()
                    .map(|(row, _, kind)| (row, kind))
                    .collect(),
            );
        }
        self.retention_expiries
            .write()
            .retain(|(table, row, created), _| {
                tables.get(table).is_some_and(|rows| {
                    rows.iter()
                        .any(|entry| entry.row_id == *row && entry.created_tx == *created)
                })
            });
    }

    /// Replace one table's rows, metadata and physical indexes as one
    /// publication.  The expensive index construction is deliberately done
    /// by `projected_index_storage` before this method acquires the locks.
    pub fn publish_table_projection(
        &self,
        table: &str,
        meta: TableMeta,
        rows: Vec<VersionedRow>,
        indexes: HashMap<String, IndexStorage>,
    ) -> bool {
        if self.uses_disk_backed_committed_bodies() {
            let mut all_indexes = self.indexes.write();
            let mut table_meta = self.table_meta.write();
            if !table_meta.contains_key(table) {
                return false;
            }
            let mut indexes = indexes;
            for storage in indexes.values_mut() {
                storage.discard_resident_postings();
            }
            table_meta.insert(table.to_owned(), meta);
            all_indexes.insert(table.to_owned(), indexes);
            return true;
        }

        // This is the same lock order as apply_inserts/apply_deletes. Holding
        // all of them through replacement keeps row lookup maps aligned with
        // the rows an index posting names.
        let mut all_indexes = self.indexes.write();
        let mut tables = self.tables.write();
        let mut table_meta = self.table_meta.write();
        let mut positions = self.row_positions.write();
        let mut version_positions = self.row_version_positions.write();
        if !table_meta.contains_key(table) {
            return false;
        }
        let rows =
            Self::compact_projection_rows(self.uses_disk_backed_committed_bodies(), &meta, rows);
        tables.insert(table.to_string(), rows);
        table_meta.insert(table.to_string(), meta);
        all_indexes.insert(table.to_string(), indexes);
        positions.retain(|(entry_table, _), _| entry_table != table);
        version_positions.retain(|(entry_table, _), _| entry_table != table);
        if let Some(rows) = tables.get(table) {
            Self::rebuild_position_maps_for_table(
                table,
                rows,
                &mut positions,
                &mut version_positions,
            );
        }
        true
    }

    /// Introspect total postings across all indexes (including tombstones).
    /// Tests use this to confirm DROP TABLE releases index storage.
    pub fn introspect_indexes_total_entries(&self) -> u64 {
        self.indexes
            .read()
            .values()
            .flat_map(|indexes| indexes.values())
            .map(|s| s.total_entries_including_tombstones())
            .sum()
    }

    /// Bump the batch-level index-write lock counter. Called once per
    /// `apply_changes` batch to prove I14.
    pub fn bump_index_write_lock_count(&self) {
        self.index_write_lock_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn index_write_lock_count(&self) -> u64 {
        self.index_write_lock_count.load(Ordering::SeqCst)
    }

    pub fn bump_index_maintenance_visits(&self, delta: u64) {
        self.index_maintenance_visits
            .fetch_add(delta, Ordering::SeqCst);
    }

    pub fn index_maintenance_visits(&self) -> u64 {
        self.index_maintenance_visits.load(Ordering::SeqCst)
    }

    pub fn reset_index_maintenance_visits(&self) {
        self.index_maintenance_visits.store(0, Ordering::SeqCst);
    }

    pub fn bump_open_index_maintenance_visits(&self, delta: u64) {
        self.open_index_maintenance_visits
            .fetch_add(delta, Ordering::SeqCst);
    }

    pub fn open_index_maintenance_visits(&self) -> u64 {
        self.open_index_maintenance_visits.load(Ordering::SeqCst)
    }

    pub fn bump_scan_rows_touched(&self, rows: u64) {
        // Observability-only counter. Relaxed avoids the full SeqCst barrier on
        // the hot scan path (8 concurrent readers in the contention bench touch
        // this line on every probe); the test assertions reset then read after
        // all writer/reader threads join, so no cross-thread ordering is needed.
        self.scan_rows_touched.fetch_add(rows, Ordering::Relaxed);
    }

    pub fn scan_rows_touched(&self) -> u64 {
        self.scan_rows_touched.load(Ordering::Relaxed)
    }

    pub fn reset_scan_rows_touched(&self) {
        self.scan_rows_touched.store(0, Ordering::Relaxed);
    }

    pub fn bump_index_entries_touched(&self, entries: u64) {
        // Observability-only counter, on the same relaxed footing as the
        // scanned-row counter beside it.
        self.index_entries_touched
            .fetch_add(entries, Ordering::Relaxed);
    }

    pub fn index_entries_touched(&self) -> u64 {
        self.index_entries_touched.load(Ordering::Relaxed)
    }

    pub fn reset_index_entries_touched(&self) {
        self.index_entries_touched.store(0, Ordering::Relaxed);
    }

    pub fn alter_table_add_column(
        &self,
        table: &str,
        col: contextdb_core::ColumnDef,
    ) -> Result<(), String> {
        let mut meta = self.table_meta.write();
        let m = meta
            .get_mut(table)
            .ok_or_else(|| format!("table '{}' not found", table))?;
        if m.columns.iter().any(|c| c.name == col.name) {
            return Err(format!(
                "column '{}' already exists in table '{}'",
                col.name, table
            ));
        }
        m.columns.push(col);
        Ok(())
    }

    pub fn alter_table_drop_column(&self, table: &str, column: &str) -> Result<(), String> {
        {
            let mut meta = self.table_meta.write();
            let m = meta
                .get_mut(table)
                .ok_or_else(|| format!("table '{}' not found", table))?;
            let pos = m
                .columns
                .iter()
                .position(|c| c.name == column)
                .ok_or_else(|| {
                    format!("column '{}' does not exist in table '{}'", column, table)
                })?;
            if m.columns[pos].primary_key || m.primary_key_columns.iter().any(|c| c == column) {
                return Err(format!("cannot drop primary key column '{}'", column));
            }
            m.columns.remove(pos);
        }
        {
            let mut tables = self.tables.write();
            if let Some(rows) = tables.get_mut(table) {
                for row in rows.iter_mut() {
                    row.values.remove(column);
                }
            }
        }
        Ok(())
    }

    pub fn alter_table_rename_column(
        &self,
        table: &str,
        from: &str,
        to: &str,
    ) -> Result<(), String> {
        {
            let mut meta = self.table_meta.write();
            let m = meta
                .get_mut(table)
                .ok_or_else(|| format!("table '{}' not found", table))?;
            if m.columns.iter().any(|c| c.name == to) {
                return Err(format!(
                    "column '{}' already exists in table '{}'",
                    to, table
                ));
            }
            if m.primary_key_columns.iter().any(|c| c == from) {
                return Err(format!("cannot rename primary key column '{}'", from));
            }
            let col = m
                .columns
                .iter_mut()
                .find(|c| c.name == from)
                .ok_or_else(|| format!("column '{}' does not exist in table '{}'", from, table))?;
            if col.primary_key {
                return Err(format!("cannot rename primary key column '{}'", from));
            }
            col.name = to.to_string();
        }
        {
            let mut tables = self.tables.write();
            if let Some(rows) = tables.get_mut(table) {
                for row in rows.iter_mut() {
                    if let Some(val) = row.values.remove(from) {
                        row.values.insert(to.to_string(), val);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn is_immutable(&self, table: &str) -> bool {
        self.table_meta
            .read()
            .get(table)
            .is_some_and(|m| m.immutable)
    }

    pub fn validate_state_transition(
        &self,
        table: &str,
        column: &str,
        from: &str,
        to: &str,
    ) -> bool {
        self.table_meta
            .read()
            .get(table)
            .is_none_or(|meta| Self::validate_state_transition_for_meta(meta, column, from, to))
    }

    /// Validate against the metadata owned by the caller's committed image.
    pub fn validate_state_transition_for_meta(
        meta: &TableMeta,
        column: &str,
        from: &str,
        to: &str,
    ) -> bool {
        meta.state_machine
            .as_ref()
            .filter(|sm| sm.column == column)
            .is_none_or(|sm| {
                sm.transitions
                    .get(from)
                    .is_some_and(|targets| targets.iter().any(|target| target == to))
            })
    }

    pub fn table_names(&self) -> Vec<String> {
        let mut names: Vec<_> = if self.uses_disk_backed_committed_bodies() {
            self.table_meta.read().keys().cloned().collect()
        } else {
            self.tables.read().keys().cloned().collect()
        };
        names.sort();
        names
    }

    /// What the indexes of ONE table are holding right now, counted from their
    /// own structures and WITHOUT the fixed exact-probe bitmap, and how many
    /// indexes that is.
    ///
    /// `retained_structure_bytes` answers for the whole store, which cannot say
    /// what one more index on one table would cost. A consumer pricing a shape
    /// change before it happens needs exactly that: an index's postings are only
    /// part of it -- its tree's owned keys and its current membership image are
    /// admitted by this store when the index is built, outside whatever the
    /// caller reserved for the postings.
    ///
    /// The exact-probe bitmap is left out on purpose, and `TableMeta` owns it
    /// instead through `exact_probe_filter_bytes`. The two are multiplied by
    /// different things: an exact-only index allocates exactly one bitmap and a
    /// user-declared index allocates none, so averaging the bitmap over a
    /// table's existing indexes charges an arriving user-declared index 64 KiB
    /// it will never hold, and charges an arriving exact-only one a second copy
    /// of the bitmap its own metadata already carries.
    pub fn table_index_structure_bytes(&self, table: &str) -> (usize, usize) {
        let indexes = self.indexes.read();
        let Some(table_indexes) = indexes.get(table) else {
            return (0, 0);
        };
        let mut bytes = RetainedStructureBytes::default();
        for index in table_indexes.values() {
            index.retained_structure_bytes(&mut bytes);
        }
        (
            bytes
                .index_trees
                .saturating_add(bytes.index_exact)
                .saturating_add(bytes.index_membership),
            table_indexes.len(),
        )
    }

    /// What this store is holding right now, counted from its own structures.
    /// A declared `MEMORY_LIMIT` is kept only if the accountant's total covers
    /// this number, so a retention audit reads it back instead of trusting the
    /// ledger.
    pub fn retained_structure_bytes(&self) -> RetainedStructureBytes {
        let mut bytes = RetainedStructureBytes::default();
        {
            let tables = self.tables.read();
            bytes.row_versions = tables.iter().fold(
                hash_table_bytes::<TableName, Vec<VersionedRow>>(tables.len()),
                |total, (name, rows)| {
                    rows.iter().fold(
                        total.saturating_add(name.len()).saturating_add(
                            rows.capacity()
                                .saturating_mul(std::mem::size_of::<VersionedRow>()),
                        ),
                        |total, row| total.saturating_add(row_version_heap_bytes(row)),
                    )
                },
            );
        }
        {
            let positions = self.row_positions.read();
            bytes.row_positions = positions.keys().fold(
                hash_table_bytes::<(TableName, RowId), usize>(positions.len()),
                |total, (table, _)| total.saturating_add(table.len()),
            );
        }
        {
            let versions = self.row_version_positions.read();
            bytes.row_version_positions = versions.iter().fold(
                hash_table_bytes::<(TableName, RowId), Vec<usize>>(versions.len()),
                |total, ((table, _), positions)| {
                    total.saturating_add(table.len()).saturating_add(
                        positions
                            .capacity()
                            .saturating_mul(std::mem::size_of::<usize>()),
                    )
                },
            );
        }
        {
            let lsns = self.sync_source_lsns.read();
            let kinds = self.sync_source_kinds.read();
            bytes.sync_provenance = lsns
                .iter()
                .fold(
                    hash_table_bytes::<TableName, HashMap<RowId, Lsn>>(lsns.len()),
                    |total, (table, rows)| {
                        total
                            .saturating_add(table.len())
                            .saturating_add(hash_table_bytes::<RowId, Lsn>(rows.len()))
                    },
                )
                .saturating_add(kinds.iter().fold(
                    hash_table_bytes::<TableName, HashMap<RowId, SyncSourceKind>>(kinds.len()),
                    |total, (table, rows)| {
                        total
                            .saturating_add(table.len())
                            .saturating_add(hash_table_bytes::<RowId, SyncSourceKind>(rows.len()))
                    },
                ));
        }
        let all_indexes = self.indexes.read();
        if self.uses_disk_backed_committed_bodies() {
            bytes.index_descriptors = hash_table_bytes::<TableName, HashMap<String, IndexStorage>>(
                all_indexes.capacity(),
            );
        }
        for (table, indexes) in all_indexes.iter() {
            if self.uses_disk_backed_committed_bodies() {
                bytes.index_descriptors = bytes
                    .index_descriptors
                    .saturating_add(table.capacity())
                    .saturating_add(hash_table_bytes::<String, IndexStorage>(indexes.capacity()));
            }
            for (name, index) in indexes {
                if index.source_backed {
                    bytes.index_descriptors = bytes
                        .index_descriptors
                        .saturating_add(name.capacity())
                        .saturating_add(
                            index
                                .columns
                                .capacity()
                                .saturating_mul(std::mem::size_of::<(String, SortDirection)>()),
                        )
                        .saturating_add(
                            index
                                .columns
                                .iter()
                                .map(|(name, _)| name.capacity())
                                .sum::<usize>(),
                        );
                }
                index.retained_structure_bytes(&mut bytes);
            }
        }
        bytes
    }

    /// Actual credits held by current and captured membership owners. This
    /// differs from the physical inventory when an image has no budget.
    #[doc(hidden)]
    pub fn membership_held_charge_bytes_for_test(&self) -> (usize, usize) {
        let mut seen = std::collections::HashSet::new();
        let current = self.indexes.read().values().fold(0usize, |bytes, indexes| {
            indexes.values().fold(bytes, |bytes, index| {
                bytes.saturating_add(index.current.held_charge_bytes_for_test(&mut seen))
            })
        });
        let captures = self.membership_captures.lock();
        let captured = captures.roots.values().fold(0usize, |bytes, indexes| {
            indexes.values().fold(bytes, |bytes, roots| {
                roots.values().fold(bytes, |bytes, root| {
                    bytes
                        .saturating_add(root._credit.bytes_for_test())
                        .saturating_add(root.image.held_charge_bytes_for_test(&mut seen))
                })
            })
        });
        (current, captured)
    }

    /// Attribute current membership credits to their owning table.
    #[doc(hidden)]
    pub fn membership_held_charge_by_table_for_test(
        &self,
    ) -> std::collections::BTreeMap<String, usize> {
        let mut seen = std::collections::HashSet::new();
        self.indexes
            .read()
            .iter()
            .map(|(table, indexes)| {
                let bytes = indexes.values().fold(0usize, |bytes, index| {
                    bytes.saturating_add(index.current.held_charge_bytes_for_test(&mut seen))
                });
                (table.clone(), bytes)
            })
            .collect()
    }

    pub fn table_meta(&self, name: &str) -> Option<TableMeta> {
        self.table_meta.read().get(name).cloned()
    }

    pub fn table_has_state_machine(&self, name: &str) -> Option<bool> {
        self.table_meta
            .read()
            .get(name)
            .map(|meta| meta.state_machine.is_some())
    }
}

fn membership_key_credit(
    columns: &[(String, SortDirection)],
    values: &HashMap<String, Value>,
    budget: Arc<dyn ReadMemoryBudget>,
) -> contextdb_core::Result<ReadCredit> {
    let bytes = columns
        .iter()
        .fold(std::mem::size_of::<IndexKey>(), |bytes, (column, _)| {
            bytes
                .saturating_add(std::mem::size_of::<DirectedValue>())
                .saturating_add(values.get(column).map_or(0, Value::estimated_bytes))
        });
    membership_credit(budget, bytes)
}

fn membership_credit(
    budget: Arc<dyn ReadMemoryBudget>,
    bytes: usize,
) -> contextdb_core::Result<ReadCredit> {
    ReadCredit::try_new(
        budget,
        bytes,
        "relational_index",
        "membership",
        "Release old readers or raise MEMORY_LIMIT before changing indexed rows.",
    )
}

/// Build the directed IndexKey for a row's values given the index's column
/// declaration. Missing columns map to `Value::Null` (NULL partition).
pub fn index_key_for_row(
    columns: &[(String, SortDirection)],
    values: &HashMap<String, Value>,
) -> IndexKey {
    columns
        .iter()
        .map(|(col, dir)| {
            let v = values.get(col).cloned().unwrap_or(Value::Null);
            match dir {
                SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(v)),
                SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(v)),
            }
        })
        .collect()
}

/// Build the directed IndexKey for a Vec of Values (one per indexed column).
pub fn index_key_from_values(columns: &[(String, SortDirection)], values: &[Value]) -> IndexKey {
    columns
        .iter()
        .zip(values.iter())
        .map(|((_, dir), v)| match dir {
            SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(v.clone())),
            SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(v.clone())),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use contextdb_core::{Lsn, SnapshotId};

    fn row(row_id: RowId, created_tx: TxId, deleted_tx: Option<TxId>, label: &str) -> VersionedRow {
        VersionedRow {
            row_id,
            values: HashMap::from([("label".to_string(), Value::Text(label.to_string()))]),
            created_tx,
            deleted_tx,
            lsn: Lsn(created_tx.0),
            created_at: None,
        }
    }

    #[test]
    fn remove_row_versions_preserves_sorted_same_row_positions() {
        let store = RelationalStore::new();
        store
            .insert_loaded_row("items", row(RowId(1), TxId(3), None, "live"))
            .unwrap();
        store
            .insert_loaded_row("items", row(RowId(1), TxId(2), Some(TxId(3)), "old"))
            .unwrap();
        store
            .insert_loaded_row("items", row(RowId(2), TxId(1), None, "other"))
            .unwrap();
        store.rebuild_row_position_maps();

        let pruned = HashSet::from([(RowId(2), TxId(1))]);
        store.remove_row_versions("items", &pruned);

        let old = store
            .row_by_id("items", RowId(1), SnapshotId(2))
            .expect("older snapshot must still find the first version");
        assert_eq!(
            old.values.get("label"),
            Some(&Value::Text("old".to_string()))
        );

        let live = store
            .row_by_id("items", RowId(1), SnapshotId(3))
            .expect("newer snapshot must find the replacement version");
        assert_eq!(
            live.values.get("label"),
            Some(&Value::Text("live".to_string()))
        );

        let current = store
            .live_row_by_id("items", RowId(1))
            .expect("latest-row map must point at the live version");
        assert_eq!(
            current.values.get("label"),
            Some(&Value::Text("live".to_string()))
        );

        let exact = store
            .row_by_id_at_lsn_or_latest("items", RowId(1), Lsn(2))
            .expect("change-log lookup must resolve the exact retained version");
        assert_eq!(
            exact.values.get("label"),
            Some(&Value::Text("old".to_string()))
        );

        let fallback = store
            .row_by_id_at_lsn_or_latest("items", RowId(1), Lsn(99))
            .expect("a compacted exact version must fall back to the latest retained version");
        assert_eq!(
            fallback.values.get("label"),
            Some(&Value::Text("live".to_string()))
        );
    }
}
