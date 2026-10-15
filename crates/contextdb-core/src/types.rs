use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[doc(hidden)]
pub use crate::json_read_builder::{
    JsonReadAdmissionGuard, deserialize_json_in_read_scope, json_retained_heap_bytes,
};

pub type NodeId = Uuid;
pub type EdgeType = String;
pub type TableName = String;
pub type ColName = String;

#[repr(transparent)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct TxId(pub u64);

impl TxId {
    #[inline]
    pub const fn from_raw_wire(n: u64) -> Self {
        Self(n)
    }
    #[inline]
    pub const fn from_snapshot(s: SnapshotId) -> Self {
        Self(s.0)
    }
}

impl fmt::Display for TxId {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[repr(transparent)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct SnapshotId(pub u64);

impl SnapshotId {
    #[inline]
    pub const fn from_raw_wire(n: u64) -> Self {
        Self(n)
    }
    #[inline]
    pub const fn from_tx(t: TxId) -> Self {
        Self(t.0)
    }
}

impl fmt::Display for SnapshotId {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[repr(transparent)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct RowId(pub u64);

impl RowId {
    #[inline]
    pub const fn from_raw_wire(n: u64) -> Self {
        Self(n)
    }
}

impl fmt::Display for RowId {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[repr(transparent)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct Lsn(pub u64);

impl Lsn {
    #[inline]
    pub const fn from_raw_wire(n: u64) -> Self {
        Self(n)
    }
}

impl fmt::Display for Lsn {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[repr(transparent)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct Wallclock(pub u64);

type WallclockFn = Arc<dyn Fn() -> u64 + Send + Sync>;

thread_local! {
    static WALLCLOCK_TEST_CLOCK: RefCell<Option<WallclockFn>> = RefCell::new(None);
}

impl Wallclock {
    #[inline]
    pub const fn from_raw_wire(n: u64) -> Self {
        Self(n)
    }

    #[inline]
    pub fn now() -> Self {
        if let Some(clock) = Self::test_clock() {
            return Self(clock());
        }
        Self(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        )
    }

    /// Test seam: install a closure that replaces the `SystemTime::now()` read
    /// on the calling thread. Tests that spawn worker threads must install the
    /// seam inside each worker that is expected to observe mocked time.
    pub fn set_test_clock<F>(f: F)
    where
        F: Fn() -> u64 + Send + Sync + 'static,
    {
        WALLCLOCK_TEST_CLOCK.with(|slot| {
            *slot.borrow_mut() = Some(Arc::new(f));
        });
    }

    /// Test seam: drop any previously-installed clock closure.
    pub fn reset_test_clock() {
        WALLCLOCK_TEST_CLOCK.with(|slot| {
            *slot.borrow_mut() = None;
        });
    }

    /// Test seam: install a clock closure and get an RAII guard that restores
    /// the previously-installed clock (or none) when dropped — including
    /// during panic unwinding. Tests should prefer this over a trailing
    /// `reset_test_clock()`, which is skipped when an assertion panics first
    /// and leaks the override to later tests on the same thread under
    /// `--test-threads=1`.
    #[must_use]
    pub fn test_clock_guard<F>(f: F) -> WallclockTestClockGuard
    where
        F: Fn() -> u64 + Send + Sync + 'static,
    {
        let previous = WALLCLOCK_TEST_CLOCK.with(|slot| slot.borrow_mut().replace(Arc::new(f)));
        WallclockTestClockGuard { previous }
    }

    /// Test seam alias used by TU10 — same semantics as `reset_test_clock`.
    pub fn clear_test_clock() {
        Self::reset_test_clock();
    }

    /// Internal accessor for the installed test clock.
    #[inline]
    fn test_clock() -> Option<WallclockFn> {
        WALLCLOCK_TEST_CLOCK.with(|slot| slot.borrow().clone())
    }
}

/// RAII guard from [`Wallclock::test_clock_guard`]: restores the previously
/// installed thread-local test clock (or none) on drop, panic-safe.
pub struct WallclockTestClockGuard {
    previous: Option<WallclockFn>,
}

impl Drop for WallclockTestClockGuard {
    fn drop(&mut self) {
        let previous = self.previous.take();
        WALLCLOCK_TEST_CLOCK.with(|slot| {
            *slot.borrow_mut() = previous;
        });
    }
}

impl fmt::Display for Wallclock {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

pub struct AtomicTxId(AtomicU64);

impl AtomicTxId {
    pub const fn new(v: TxId) -> Self {
        Self(AtomicU64::new(v.0))
    }
    #[inline]
    pub fn load(&self, order: Ordering) -> TxId {
        TxId(self.0.load(order))
    }
    #[inline]
    pub fn store(&self, v: TxId, order: Ordering) {
        self.0.store(v.0, order);
    }
    #[inline]
    pub fn fetch_add(&self, delta: u64, order: Ordering) -> TxId {
        TxId(self.0.fetch_add(delta, order))
    }
    #[inline]
    pub fn fetch_max(&self, v: TxId, order: Ordering) -> TxId {
        TxId(self.0.fetch_max(v.0, order))
    }
    #[inline]
    pub fn compare_exchange(
        &self,
        current: TxId,
        new: TxId,
        success: Ordering,
        failure: Ordering,
    ) -> Result<TxId, TxId> {
        self.0
            .compare_exchange(current.0, new.0, success, failure)
            .map(TxId)
            .map_err(TxId)
    }
}

pub struct AtomicLsn(AtomicU64);

impl AtomicLsn {
    pub const fn new(v: Lsn) -> Self {
        Self(AtomicU64::new(v.0))
    }
    #[inline]
    pub fn load(&self, order: Ordering) -> Lsn {
        Lsn(self.0.load(order))
    }
    #[inline]
    pub fn store(&self, v: Lsn, order: Ordering) {
        self.0.store(v.0, order);
    }
    #[inline]
    pub fn fetch_add(&self, delta: u64, order: Ordering) -> Lsn {
        Lsn(self.0.fetch_add(delta, order))
    }
    #[inline]
    pub fn fetch_max(&self, v: Lsn, order: Ordering) -> Lsn {
        Lsn(self.0.fetch_max(v.0, order))
    }
    #[inline]
    pub fn compare_exchange(
        &self,
        current: Lsn,
        new: Lsn,
        success: Ordering,
        failure: Ordering,
    ) -> Result<Lsn, Lsn> {
        self.0
            .compare_exchange(current.0, new.0, success, failure)
            .map(Lsn)
            .map_err(Lsn)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Text(String),
    Uuid(Uuid),
    Timestamp(i64),
    /// JSON values are serialized as strings for bincode compatibility.
    /// `serde_json::Value` uses `deserialize_any` which bincode does not support.
    Json(#[serde(with = "json_as_string")] serde_json::Value),
    Vector(Vec<f32>),
    TxId(TxId),
}

mod json_as_string {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(value: &serde_json::Value, ser: S) -> Result<S::Ok, S::Error> {
        let s = serde_json::to_string(value).map_err(serde::ser::Error::custom)?;
        s.serialize(ser)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<serde_json::Value, D::Error> {
        let s = String::deserialize(de)?;
        if crate::json_read_builder::in_scope() {
            let result = super::deserialize_json_in_read_scope(&s);
            // The engine's outer String visitor separately admitted this
            // serialized input. Return its credit only after native drop.
            crate::json_read_builder::release_serialized_input(s);
            result.map_err(serde::de::Error::custom)
        } else {
            serde_json::from_str::<serde_json::Value>(&s).map_err(serde::de::Error::custom)
        }
    }
}

/// How many copies of a row's values the TABLE keeps: one, the row version in
/// the table's own vector.
///
/// This used to be two, and the second copy stood for "an index tree is keyed
/// by a whole clone of the value". Counted against the structures, that guess
/// was wrong in both directions. It was far too small for a table with more
/// than one index -- the cg `events` table declares nine, each cloning the
/// columns it keys on, so its context and connector columns are held ten times
/// over -- and too large for a table with none, which keeps its values exactly
/// once. Every index clone is now charged against the index that makes it, per
/// declared index and per key column ([`retained_index_posting_bytes`]), so a
/// CREATE INDEX that adds a tenth clone is charged for it and a table with no
/// index is charged for what it actually holds. A vector column's second copy
/// is the vector store's, charged where that store makes it.
pub const ROW_VALUE_RETENTIONS: usize = 1;

/// What the relational store keeps BESIDE the row version, for every row it
/// holds, regardless of how wide the row is.
///
/// Four side maps, none of which appears in the row's own bytes and none of
/// which is proportional to the row's content. Counted against the structures
/// on 2026-09-19, for a store holding cg-shaped event rows:
///
/// * `row_positions` -- one hashed entry per live row, whose key is the table
///   name and the row id. 57 B counted; 82 B allowed here, because hashbrown
///   rounds the bucket count to a power of two and a table half past a growth
///   holds twice its settled share.
/// * `row_version_positions` -- one hashed entry per row id, whose value is a
///   heap `Vec` that allocates room for four positions the first time one is
///   filed. 109 B counted, 146 B allowed.
/// * `sync_source_lsns` and `sync_source_kinds` -- one inner entry each per
///   row on a table that syncs. Zero on a local import, and charged anyway:
///   a synced row must not be the one that escapes the limit. 36 B each.
///
/// The table-name copy each of the two position maps keeps is charged
/// separately by [`retained_row_side_map_bytes`], because it is the one part
/// of this that depends on which table the row is in.
pub const ROW_SIDE_MAP_BYTES: usize = 82 + 146 + 36 + 36;

/// The slot the table's own `Vec<VersionedRow>` keeps for one row, with the
/// slack a doubling vector carries: half of a grown vector's slots are empty,
/// so a row's slot costs up to twice the struct. It is the row's header, not
/// its values -- those are charged by their own estimate -- and nothing else
/// charged it.
pub const ROW_VERSION_SLOT_BYTES: usize = 2 * std::mem::size_of::<VersionedRow>();

/// What the relational store keeps for one row of `table` besides the row's
/// own values: its slot in the table's vector ([`ROW_VERSION_SLOT_BYTES`]),
/// the four side maps ([`ROW_SIDE_MAP_BYTES`]) and the copy of the table name
/// that `row_positions` and `row_version_positions` each keep in their key.
///
/// Nesting those two maps by table -- the shape `sync_source_lsns` and
/// `sync_source_kinds` already use -- would remove both name copies instead of
/// charging them; it is worth about 72 B of the 14,833 B one cg event holds,
/// and it is a change to sixty call sites inside the relational store, so this
/// charges them.
pub fn retained_row_side_map_bytes(table: &str) -> usize {
    ROW_SIDE_MAP_BYTES
        .saturating_add(ROW_VERSION_SLOT_BYTES)
        .saturating_add(table.len().saturating_mul(2))
}

/// What holds ONE index posting besides the clone of its key: the map slot it
/// is filed in and the posting list under it.
///
/// An ordered index files the posting in a `BTreeMap`, whose key and value
/// slots and share of the node header come to 64 B; an exact-only index files
/// it in a hashed bucket, which comes to about 114 B. Both then allocate a
/// posting list with room for four postings -- 128 B -- the first time a
/// posting lands on that key. This is the larger of the two shapes, so neither
/// under-charges.
pub const INDEX_POSTING_SLOT_BYTES: usize = 114 + 128;

/// The fixed exact-probe filter an exact-only index allocates: a bitmap that
/// answers "this key is definitely not here" without touching the map.
///
/// It is 64 KiB per index, it does not shrink, and it is held for the life of
/// the index. Only an index that files EXACT postings -- an automatic
/// constraint index over a single column -- ever marks or probes one; an
/// ordered index used to allocate one anyway and could never read it, which is
/// 393,216 B on one cg-shaped `events` table, and no longer does. What remains
/// is allocated when the index is created and charged with the table's schema
/// at the same moment, so a CREATE INDEX that adds an exact index is charged
/// for its filter and a DROP gives back exactly what the CREATE took.
///
/// It is now VISIBLE to a declared limit, which it was not before: a store
/// with a 4 KiB limit declaring one table with a primary key was holding 64
/// KiB and reporting room. Whether a fixed 64 KiB negative-probe bitmap in
/// front of an identity-hashed map is the right size is a read-path question
/// this charge does not answer; it only stops the bytes being invisible.
pub const EXACT_PROBE_FILTER_BYTES: usize = 64 * 1024;

/// What one index posting for this row costs the store.
///
/// An index keeps a WHOLE clone of every value it keys on, filed under its own
/// slot, for every row. That clone is the retention `ROW_VALUE_RETENTIONS`
/// used to stand in for with a flat doubling of the row; charging it against
/// the index that makes it is what lets the charge follow the schema -- nine
/// indexes cost nine clones, none costs none, and a CREATE INDEX over an
/// existing table is charged for the postings it builds because the same
/// figure is what the table projection re-computes under the new schema.
///
/// A column the row does not carry still takes a slot in the key, as a NULL.
pub fn retained_index_posting_bytes(
    values: &HashMap<ColName, Value>,
    index: &crate::table_meta::IndexDecl,
) -> usize {
    index.columns.iter().fold(
        INDEX_POSTING_SLOT_BYTES.saturating_add(
            index
                .columns
                .len()
                .saturating_mul(std::mem::size_of::<DirectedValue>()),
        ),
        |bytes, (column, _)| {
            bytes.saturating_add(
                values
                    .get(column)
                    .map_or(Value::Null.estimated_bytes(), |value| {
                        value.estimated_bytes()
                    }),
            )
        },
    )
}

/// What every declared index on `meta` costs the store for one row.
pub fn retained_index_posting_bytes_for_meta(
    values: &HashMap<ColName, Value>,
    meta: &crate::table_meta::TableMeta,
) -> usize {
    meta.indexes.iter().fold(0usize, |bytes, index| {
        bytes.saturating_add(retained_index_posting_bytes(values, index))
    })
}

/// How many copies of an edge a store keeps.
///
/// An edge is retained in BOTH adjacency directions and each direction keeps
/// the whole entry -- its type, its property names and its property values
/// (`contextdb-graph`'s `apply_inserts_ref`). Measured against the allocator:
/// two bytes held for every property byte written.
pub const ADJACENCY_RETENTIONS: usize = 2;

/// What writing one edge costs the store BESIDES its two adjacency copies.
///
/// A write leaves a change-log record behind, and the record is held for as
/// long as the edge is. Measured against the allocator through the public
/// API, an edge of any property size costs about 1,610 bytes that neither
/// adjacency copy accounts for, and removing it costs about 256 more; a row
/// absorbs the same overhead inside the headroom `ROW_VALUE_RETENTIONS`
/// already leaves it, but an edge's two copies are exactly what it holds, so
/// there is no headroom and the overhead has to be charged. Rounded up to the
/// next power of two, which is margin over the measurement rather than a
/// number tuned to it -- the cost is fixed per edge, so it cannot make a
/// large property look cheap.
pub const EDGE_WRITE_OVERHEAD_BYTES: usize = 2048;

/// What holding one copy of a property NAME costs.
///
/// Measured against the allocator with the property names as the only thing
/// growing: an edge holds two bytes for every name byte written, one per
/// adjacency direction, so one copy costs one byte per byte. The fixed part
/// covers the map slot the name sits in and the allocator's rounding; the
/// per-byte part is charged at twice the measured cost so a name that is
/// re-allocated rather than sized exactly is still covered.
pub fn estimate_value_key_bytes(key: &str) -> usize {
    32 + key.len().saturating_mul(2)
}

/// What one retained `serde_json::Value` costs beneath its own slot.
///
/// Walked rather than inferred from the rendered length, for two reasons.
/// Rendering a document in order to measure it allocates the whole document
/// on an accounting path that runs before every write. And the rendered
/// length says nothing about the shape: measured against the allocator, one
/// rendered byte costs one byte as a long string and sixteen as deep nesting,
/// so no single multiple of the rendered length is both safe for the deep
/// document and usable for the long one -- the multiple that covers nesting
/// leaves an operator's limit holding a thirty-second of the documents it
/// says it will.
fn json_heap_bytes(value: &serde_json::Value) -> usize {
    json_retained_heap_bytes(value)
}

impl Value {
    /// What ONE retained copy of this value costs the process, for the
    /// accounting that decides whether a store may hold more.
    ///
    /// Every arm is measured against the allocator through the public API
    /// rather than guessed: a text value costs its own bytes once, a vector
    /// four bytes an element, and a document costs what its nodes and their
    /// strings really allocate.
    ///
    /// This is a PER-COPY number, and how many copies a store keeps is
    /// decided where the value is RETAINED, not here. Each retention boundary
    /// charges for the copies it keeps: `AdjEntry::estimated_bytes` charges
    /// both adjacency directions of an edge, and a row's admission charges the
    /// version the table keeps plus one clone for every index that keys on the
    /// column. What those boundaries answer must never be
    /// smaller than what is held -- a number below the truth does not fail
    /// loudly, it quietly admits more than the operator allowed -- and never
    /// so far above it that a limit refuses work the machine could have done.
    ///
    /// It is deliberately NOT the number a bounded read charges itself. A
    /// read holds one copy of what it pulled and measures that copy directly.
    pub fn estimated_bytes(&self) -> usize {
        match self {
            Value::Null => 8,
            Value::Bool(_) => 8,
            Value::Int64(_) => 16,
            Value::Float64(_) => 16,
            // Measured, not guessed. Inserting rows of growing text through
            // the public API and watching the allocator says one copy of a
            // stored text value costs the process exactly its own bytes. The
            // second copy an indexed column or an adjacency direction keeps
            // is charged where that copy is made, not here.
            Value::Text(s) => 64 + s.len(),
            Value::Uuid(_) => 32,
            Value::Timestamp(_) => 16,
            Value::Json(v) => 96 + std::mem::size_of::<serde_json::Value>() + json_heap_bytes(v),
            Value::Vector(values) => 24 + values.len().saturating_mul(std::mem::size_of::<f32>()),
            Value::TxId(_) => 16,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_uuid(&self) -> Option<&Uuid> {
        match self {
            Value::Uuid(u) => Some(u),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

/// Direction-aware total ordering over `Value`. Wraps `Value` so that
/// `BTreeMap` keys sort per the index's declared direction without separate
/// comparison logic in the planner.
#[derive(Debug, Clone)]
pub struct TotalOrdAsc(pub Value);

#[derive(Debug, Clone)]
pub struct TotalOrdDesc(pub Value);

#[derive(Debug, Clone)]
pub enum DirectedValue {
    Asc(TotalOrdAsc),
    Desc(TotalOrdDesc),
}

pub type IndexKey = Vec<DirectedValue>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ContextId(pub Uuid);

impl ContextId {
    pub fn new(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

/// A sync tenant identifier. A newtype (like [`ContextId`]) so a tenant id is
/// unambiguous at the call site of the sync surface and cannot be silently
/// swapped with another bare string. The inner value is private; construct one
/// with `TenantId::from(..)`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TenantId(String);

impl TenantId {
    /// Borrow the inner id (for wire subjects, tracing, and validation).
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Build a persisted/config key for this tenant. This is the ONE place the
    /// `"<prefix>:<tenant_id>"` key format is authored — the persisted watermark
    /// keys (`sync_push_watermark:*`, `sync_pull_watermark:*`,
    /// `sync_applied_push_watermark:*`) all route through here, so the on-disk
    /// format stays byte-identical and lives in a single location.
    pub fn config_key(&self, prefix: &str) -> String {
        format!("{prefix}:{}", self.0)
    }
}

impl From<String> for TenantId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for TenantId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<&String> for TenantId {
    fn from(value: &String) -> Self {
        Self(value.clone())
    }
}

impl AsRef<str> for TenantId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A random 128-bit token identifying one LIFE of a syncing database. It is
/// minted the first time a database syncs and stays fixed for that life; a
/// database that is wiped and recreated (its data gone but its transport
/// identity key-file reused) mints a NEW one, while a plain reopen of the same
/// data loads the same one back.
///
/// The hub keys its per-edge received-up-to record by `(node_id, incarnation)`,
/// so a rebuilt edge — same node id, fresh incarnation — is an unknown edge to
/// the hub and is answered "I hold nothing from you" rather than the prior
/// life's stale-high watermark. That closes the reincarnation false-confirmation
/// by construction: the stale record can never vouch for a batch the hub never
/// received.
///
/// It rides the wire as two positional `u64` halves (high, low) so the encoding
/// is deterministic across encoders regardless of any 128-bit integer support,
/// and is a fixed 32-hex-character string when it forms part of a config key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Incarnation(pub u128);

impl Incarnation {
    /// Mint a fresh random incarnation: 128 uniformly random bits drawn from the
    /// OS CSPRNG (no fixed version/variant bits). The chance of two lives colliding
    /// is negligible, which is exactly the property the per-edge keying needs.
    pub fn mint() -> Self {
        Self(rand::random::<u128>())
    }

    /// The fixed 32-character lowercase hex rendering used inside config keys.
    pub fn to_hex(self) -> String {
        format!("{:032x}", self.0)
    }

    /// Parse the hex rendering produced by [`Incarnation::to_hex`].
    pub fn from_hex(hex: &str) -> Option<Self> {
        u128::from_str_radix(hex, 16).ok().map(Self)
    }
}

impl fmt::Display for Incarnation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl Serialize for Incarnation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let hi = (self.0 >> 64) as u64;
        let lo = self.0 as u64;
        (hi, lo).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Incarnation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let (hi, lo) = <(u64, u64)>::deserialize(deserializer)?;
        Ok(Self((u128::from(hi) << 64) | u128::from(lo)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ScopeLabel(pub String);

impl ScopeLabel {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Principal {
    System,
    Agent(String),
    Human(String),
}

impl PartialEq for TotalOrdAsc {
    fn eq(&self, other: &Self) -> bool {
        value_total_cmp(&self.0, &other.0).is_eq()
    }
}
impl Eq for TotalOrdAsc {}
impl Ord for TotalOrdAsc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        value_total_cmp(&self.0, &other.0)
    }
}
impl PartialOrd for TotalOrdAsc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TotalOrdDesc {
    fn eq(&self, other: &Self) -> bool {
        value_total_cmp(&self.0, &other.0).is_eq()
    }
}
impl Eq for TotalOrdDesc {}
impl Ord for TotalOrdDesc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        value_total_cmp(&self.0, &other.0).reverse()
    }
}
impl PartialOrd for TotalOrdDesc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for DirectedValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DirectedValue::Asc(a), DirectedValue::Asc(b)) => a == b,
            (DirectedValue::Desc(a), DirectedValue::Desc(b)) => a == b,
            _ => false,
        }
    }
}
impl Eq for DirectedValue {}
impl Ord for DirectedValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (DirectedValue::Asc(a), DirectedValue::Asc(b)) => a.cmp(b),
            (DirectedValue::Desc(a), DirectedValue::Desc(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        }
    }
}
impl PartialOrd for DirectedValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Whether two values are the same index key: equal under the total order
/// every index posting and maintained count is keyed by. Unlike `==`, `-0.0`
/// and `0.0` are different keys and `NaN` is one key.
pub fn index_key_value_eq(a: &Value, b: &Value) -> bool {
    value_total_cmp(a, b).is_eq()
}

/// Total ordering over `Value`. Stub uses a simple discriminant tiebreak +
/// `f64::total_cmp` for Float64 + NULL-LAST-ASC semantics. Works uniformly
/// for every value type; impl may replace this with a more specialized lookup
/// in its own pass.
fn value_total_cmp(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    // NULL > any finite under ASC per shp_05 (NULLS LAST ASC).
    match (a.is_null(), b.is_null()) {
        (true, true) => return Ordering::Equal,
        (true, false) => return Ordering::Greater,
        (false, true) => return Ordering::Less,
        _ => {}
    }
    match (a, b) {
        (Value::Int64(x), Value::Int64(y)) => x.cmp(y),
        (Value::Float64(x), Value::Float64(y)) => x.total_cmp(y),
        (Value::Text(x), Value::Text(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Uuid(x), Value::Uuid(y)) => x.cmp(y),
        (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
        (Value::TxId(x), Value::TxId(y)) => x.0.cmp(&y.0),
        // Cross-variant comparisons (Int64 vs Text etc.) are unreachable
        // because DDL-time indexable-type enforcement guarantees all values
        // feeding a given IndexKey share the column's type.
        //
        // Json / Vector variants are unreachable because DDL5 / DDL6 reject
        // them with ColumnNotIndexable at CREATE INDEX time.
        //
        // F-S6: panic on internal invariant violation — better than silently
        // returning Ordering::Equal which corrupts BTree ordering.
        (a, b) => panic!(
            "value_total_cmp called on non-indexable / mismatched variants: \
             left={a:?}, right={b:?}; DDL should have rejected this"
        ),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VersionedRow {
    pub row_id: RowId,
    pub values: HashMap<ColName, Value>,
    pub created_tx: TxId,
    pub deleted_tx: Option<TxId>,
    pub lsn: Lsn,
    #[serde(default)]
    pub created_at: Option<Wallclock>,
}

impl VersionedRow {
    /// What holding this row costs the process.
    ///
    /// The retention boundary for a row: its values are charged
    /// `ROW_VALUE_RETENTIONS` copies -- one, the version the table keeps.
    /// What each declared index clones on top of that is charged against that
    /// index by [`retained_index_posting_bytes`], which needs the table's
    /// schema; this figure is the fallback for the sites that do not have it.
    pub fn estimated_bytes(&self) -> usize {
        64 + estimate_row_value_bytes(&self.values).saturating_mul(ROW_VALUE_RETENTIONS)
            + self.created_at.map(|_| 8).unwrap_or(0)
    }

    pub fn visible_at(&self, snapshot: SnapshotId) -> bool {
        self.created_tx.0 <= snapshot.0 && self.deleted_tx.is_none_or(|tx| tx.0 > snapshot.0)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdjEntry {
    pub source: NodeId,
    pub target: NodeId,
    pub edge_type: EdgeType,
    pub properties: HashMap<String, Value>,
    pub created_tx: TxId,
    pub deleted_tx: Option<TxId>,
    pub lsn: Lsn,
}

impl AdjEntry {
    /// What holding this edge costs the process.
    ///
    /// The retention boundary for an edge: the graph store puts the whole
    /// entry into the forward adjacency map AND into the reverse one, so the
    /// type, every property name and every property value -- of every variant
    /// -- is held twice. Charging one copy here and letting a single value
    /// arm carry the doubling is what left an edge carrying a vector or a
    /// document charged for half of what it holds. On top of the two copies
    /// it charges `EDGE_WRITE_OVERHEAD_BYTES` once, for the change-log record
    /// the write leaves behind.
    pub fn estimated_bytes(&self) -> usize {
        (96 + self.edge_type.len().saturating_mul(2) + estimate_row_value_bytes(&self.properties))
            .saturating_mul(ADJACENCY_RETENTIONS)
            .saturating_add(EDGE_WRITE_OVERHEAD_BYTES)
    }

    pub fn visible_at(&self, snapshot: SnapshotId) -> bool {
        self.created_tx.0 <= snapshot.0 && self.deleted_tx.is_none_or(|tx| tx.0 > snapshot.0)
    }
}

/// The route contract declared by a vector column or requested by one search.
///
/// A column declaration always stores one of these values. SQL omission is
/// normalized to [`VectorSearchMode::Auto`]; an operation-level absence is
/// represented separately by the caller (for example, `Option<Self>`) so it
/// can mean "use the column declaration" without adding another enum case.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub enum VectorSearchMode {
    /// Choose exact or maintained indexed work using the column's resolved
    /// `AUTO_INDEX_AT` and the aggregate allowed count across the requested scope.
    /// Exact fallback for an unavailable route must fit the caller's existing limits.
    #[default]
    Auto,
    /// Compare every allowed stored vector under the active limits. Quantized
    /// columns remain quantized; this never substitutes an approximate answer.
    Exact,
    /// Require a maintained route for the complete authorized scope, including
    /// small nonempty partitions. An unavailable route gives a typed refusal;
    /// the query never builds a graph, waits for a full build, or silently scans.
    Indexed,
}

impl VectorSearchMode {
    /// The canonical SQL spelling used by schema and inspection rendering.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "AUTO",
            Self::Exact => "EXACT",
            Self::Indexed => "INDEXED",
        }
    }
}

impl fmt::Display for VectorSearchMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One exact typed component of a local vector-partition identity.
///
/// The variants are deliberately limited to the declaration types whose
/// equality is stable. Keeping the type tag means, for example, integer `7`
/// can never become the same state as text `"7"` through formatting.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum VectorPartitionComponent {
    Uuid(Uuid),
    Text(String),
    Integer(i64),
    Boolean(bool),
    Timestamp(i64),
    TxId(TxId),
}

impl VectorPartitionComponent {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Uuid(value) => Some(Self::Uuid(*value)),
            Value::Text(value) => Some(Self::Text(value.clone())),
            Value::Int64(value) => Some(Self::Integer(*value)),
            Value::Bool(value) => Some(Self::Boolean(*value)),
            Value::Timestamp(value) => Some(Self::Timestamp(*value)),
            Value::TxId(value) => Some(Self::TxId(*value)),
            Value::Null | Value::Float64(_) | Value::Json(_) | Value::Vector(_) => None,
        }
    }

    fn heap_bytes(&self) -> usize {
        match self {
            Self::Text(value) => value.capacity(),
            Self::Uuid(_)
            | Self::Integer(_)
            | Self::Boolean(_)
            | Self::Timestamp(_)
            | Self::TxId(_) => 0,
        }
    }
}

/// The canonical typed identity of one local vector partition.
///
/// An empty tuple is the one logical state of an unpartitioned vector column.
/// Equality uses the typed components themselves, never a hash or rendered
/// JSON. `canonical_bytes` is a fixed, versioned encoding used anywhere the
/// identity crosses a persistence boundary.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct VectorPartitionKey(Vec<VectorPartitionComponent>);

impl VectorPartitionKey {
    const CANONICAL_VERSION: u8 = 1;

    pub fn unpartitioned() -> Self {
        Self::default()
    }

    pub fn from_components(components: Vec<VectorPartitionComponent>) -> Self {
        Self(components)
    }

    /// Build a key from already type-checked row values. `None` means one of
    /// the values is NULL or is not an allowed exact identity type.
    pub fn from_values(values: &[Value]) -> Option<Self> {
        values
            .iter()
            .map(VectorPartitionComponent::from_value)
            .collect::<Option<Vec<_>>>()
            .map(Self)
    }

    pub fn components(&self) -> &[VectorPartitionComponent] {
        &self.0
    }

    pub fn is_unpartitioned(&self) -> bool {
        self.0.is_empty()
    }

    /// Memory retained by this key itself, including a large TEXT payload.
    /// Hash-map/tree slot overhead is charged by the registry that owns it.
    pub fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(
                self.0
                    .capacity()
                    .saturating_mul(std::mem::size_of::<VectorPartitionComponent>()),
            )
            .saturating_add(
                self.0
                    .iter()
                    .map(VectorPartitionComponent::heap_bytes)
                    .sum::<usize>(),
            )
    }

    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(Self::CANONICAL_VERSION);
        bytes.extend_from_slice(&(self.0.len() as u64).to_be_bytes());
        for component in &self.0 {
            match component {
                VectorPartitionComponent::Uuid(value) => {
                    bytes.push(0);
                    bytes.extend_from_slice(value.as_bytes());
                }
                VectorPartitionComponent::Text(value) => {
                    bytes.push(1);
                    bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
                    bytes.extend_from_slice(value.as_bytes());
                }
                VectorPartitionComponent::Integer(value) => {
                    bytes.push(2);
                    bytes.extend_from_slice(&value.to_be_bytes());
                }
                VectorPartitionComponent::Boolean(value) => {
                    bytes.push(3);
                    bytes.push(u8::from(*value));
                }
                VectorPartitionComponent::Timestamp(value) => {
                    bytes.push(4);
                    bytes.extend_from_slice(&value.to_be_bytes());
                }
                VectorPartitionComponent::TxId(value) => {
                    bytes.push(5);
                    bytes.extend_from_slice(&value.0.to_be_bytes());
                }
            }
        }
        bytes
    }

    pub fn from_canonical_bytes(bytes: &[u8]) -> std::result::Result<Self, &'static str> {
        fn take<'a>(
            bytes: &'a [u8],
            cursor: &mut usize,
            count: usize,
        ) -> std::result::Result<&'a [u8], &'static str> {
            let end = cursor
                .checked_add(count)
                .ok_or("vector partition key length overflow")?;
            let value = bytes
                .get(*cursor..end)
                .ok_or("truncated vector partition key")?;
            *cursor = end;
            Ok(value)
        }

        fn read_u64(bytes: &[u8], cursor: &mut usize) -> std::result::Result<u64, &'static str> {
            let raw: [u8; 8] = take(bytes, cursor, 8)?
                .try_into()
                .map_err(|_| "invalid vector partition integer")?;
            Ok(u64::from_be_bytes(raw))
        }

        let mut cursor = 0usize;
        let version = *take(bytes, &mut cursor, 1)?
            .first()
            .ok_or("missing vector partition key version")?;
        if version != Self::CANONICAL_VERSION {
            return Err("unsupported vector partition key version");
        }
        let component_count = usize::try_from(read_u64(bytes, &mut cursor)?)
            .map_err(|_| "vector partition component count is too large")?;
        let mut components = Vec::new();
        components
            .try_reserve_exact(component_count)
            .map_err(|_| "vector partition component allocation failed")?;
        for _ in 0..component_count {
            let tag = *take(bytes, &mut cursor, 1)?
                .first()
                .ok_or("missing vector partition component tag")?;
            let component = match tag {
                0 => {
                    let raw: [u8; 16] = take(bytes, &mut cursor, 16)?
                        .try_into()
                        .map_err(|_| "invalid vector partition UUID")?;
                    VectorPartitionComponent::Uuid(Uuid::from_bytes(raw))
                }
                1 => {
                    let length = usize::try_from(read_u64(bytes, &mut cursor)?)
                        .map_err(|_| "vector partition text is too large")?;
                    let raw = take(bytes, &mut cursor, length)?;
                    let value = std::str::from_utf8(raw)
                        .map_err(|_| "vector partition text is not UTF-8")?;
                    VectorPartitionComponent::Text(value.to_owned())
                }
                2 => {
                    let raw: [u8; 8] = take(bytes, &mut cursor, 8)?
                        .try_into()
                        .map_err(|_| "invalid vector partition integer")?;
                    VectorPartitionComponent::Integer(i64::from_be_bytes(raw))
                }
                3 => match *take(bytes, &mut cursor, 1)?
                    .first()
                    .ok_or("missing vector partition boolean")?
                {
                    0 => VectorPartitionComponent::Boolean(false),
                    1 => VectorPartitionComponent::Boolean(true),
                    _ => return Err("invalid vector partition boolean"),
                },
                4 => {
                    let raw: [u8; 8] = take(bytes, &mut cursor, 8)?
                        .try_into()
                        .map_err(|_| "invalid vector partition timestamp")?;
                    VectorPartitionComponent::Timestamp(i64::from_be_bytes(raw))
                }
                5 => VectorPartitionComponent::TxId(TxId(read_u64(bytes, &mut cursor)?)),
                _ => return Err("unknown vector partition component tag"),
            };
            components.push(component);
        }
        if cursor != bytes.len() {
            return Err("trailing bytes in vector partition key");
        }
        Ok(Self(components))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VectorIndexRef {
    pub table: String,
    pub column: String,
}

impl VectorIndexRef {
    pub fn new(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            column: column.into(),
        }
    }
}

impl std::fmt::Display for VectorIndexRef {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}.{}", self.table, self.column)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorEntry {
    pub index: VectorIndexRef,
    pub row_id: RowId,
    pub vector: Vec<f32>,
    pub created_tx: TxId,
    pub deleted_tx: Option<TxId>,
    pub lsn: Lsn,
}

impl VectorEntry {
    pub fn estimated_bytes(&self) -> usize {
        24 + self.vector.len().saturating_mul(std::mem::size_of::<f32>())
    }

    pub fn visible_at(&self, snapshot: SnapshotId) -> bool {
        self.created_tx.0 <= snapshot.0 && self.deleted_tx.is_none_or(|tx| tx.0 > snapshot.0)
    }
}

/// What ONE copy of a set of named values costs, names included.
///
/// Per-copy, like `Value::estimated_bytes`: the retention boundary that calls
/// this multiplies by the copies it keeps.
pub fn estimate_row_value_bytes(values: &HashMap<ColName, Value>) -> usize {
    values.iter().fold(64, |acc, (key, value)| {
        acc.saturating_add(estimate_value_key_bytes(key) + value.estimated_bytes())
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraversalNode {
    pub id: NodeId,
    pub depth: u32,
    pub path: Vec<(NodeId, EdgeType)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraversalResult {
    pub nodes: Vec<TraversalNode>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpsertResult {
    Inserted,
    Updated,
    NoOp,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_search_mode_defaults_and_renders_canonically() {
        assert_eq!(VectorSearchMode::default(), VectorSearchMode::Auto);
        assert_eq!(VectorSearchMode::Auto.as_str(), "AUTO");
        assert_eq!(VectorSearchMode::Exact.to_string(), "EXACT");
        assert_eq!(VectorSearchMode::Indexed.to_string(), "INDEXED");
    }

    #[test]
    fn public_vector_errors_render_the_sql_column_identity() {
        let error = crate::Error::VectorIndexedRouteUnavailable {
            index: VectorIndexRef::new("documents", "embedding"),
        }
        .to_string();
        assert!(error.contains("documents.embedding"));
        assert!(!error.contains("VectorIndexRef"));
        assert!(!error.contains("table:"));
    }

    #[test]
    fn vector_partition_keys_keep_exact_types_and_round_trip_canonically() {
        let key = VectorPartitionKey::from_components(vec![
            VectorPartitionComponent::Uuid(
                Uuid::parse_str("b4abaf12-2f0a-4a3b-9d7a-c46e01390895").unwrap(),
            ),
            VectorPartitionComponent::Text("Case-Sensitive 7".to_owned()),
            VectorPartitionComponent::Integer(-7),
            VectorPartitionComponent::Boolean(true),
            VectorPartitionComponent::Timestamp(1_700_000_000_000),
            VectorPartitionComponent::TxId(TxId(99)),
        ]);
        let encoded = key.canonical_bytes();
        assert_eq!(
            VectorPartitionKey::from_canonical_bytes(&encoded).unwrap(),
            key
        );
        assert_ne!(
            VectorPartitionKey::from_values(&[Value::Int64(7)]).unwrap(),
            VectorPartitionKey::from_values(&[Value::Text("7".to_owned())]).unwrap()
        );
        assert!(VectorPartitionKey::from_values(&[Value::Null]).is_none());
    }

    #[test]
    fn unpartitioned_key_is_an_explicit_empty_canonical_tuple() {
        let key = VectorPartitionKey::unpartitioned();
        assert!(key.is_unpartitioned());
        assert_eq!(
            VectorPartitionKey::from_canonical_bytes(&key.canonical_bytes()).unwrap(),
            key
        );

        let mut trailing = key.canonical_bytes();
        trailing.push(0);
        assert!(VectorPartitionKey::from_canonical_bytes(&trailing).is_err());
    }

    #[test]
    fn wallclock_test_clock_guard_restores_on_panic_unwind() {
        // Baseline: no test clock installed on this thread.
        Wallclock::reset_test_clock();
        let real_before = Wallclock::now();

        let panicked = std::panic::catch_unwind(|| {
            let _guard = Wallclock::test_clock_guard(|| 42);
            assert_eq!(
                Wallclock::now(),
                Wallclock(42),
                "guard clock must be active"
            );
            panic!("deliberate unwind while the guard is held");
        });
        assert!(panicked.is_err(), "closure must have panicked");

        // The guard's Drop ran during unwinding: the mock must be gone and the
        // real clock restored (a real reading is >= the pre-guard reading and
        // far above the mock's 42).
        let real_after = Wallclock::now();
        assert!(
            real_after >= real_before,
            "after unwind the real clock must be back; got {real_after:?}"
        );
        assert_ne!(
            real_after,
            Wallclock(42),
            "mock clock must not survive unwind"
        );

        // Nesting: the guard restores the PREVIOUS clock, not merely none.
        let _outer = Wallclock::test_clock_guard(|| 7);
        {
            let _inner = Wallclock::test_clock_guard(|| 8);
            assert_eq!(Wallclock::now(), Wallclock(8));
        }
        assert_eq!(
            Wallclock::now(),
            Wallclock(7),
            "dropping the inner guard must restore the outer clock"
        );
    }

    #[test]
    fn value_conversion_helpers() {
        let text = Value::Text("abc".to_string());
        let int = Value::Int64(42);
        let id = Uuid::new_v4();
        let uuid = Value::Uuid(id);

        assert_eq!(text.as_text(), Some("abc"));
        assert_eq!(int.as_i64(), Some(42));
        assert_eq!(uuid.as_uuid(), Some(&id));
    }
}
