use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

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
        serde_json::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl Value {
    pub fn estimated_bytes(&self) -> usize {
        match self {
            Value::Null => 8,
            Value::Bool(_) => 8,
            Value::Int64(_) => 16,
            Value::Float64(_) => 16,
            Value::Text(s) => {
                if s.len() <= 16 {
                    32 + s.len().saturating_mul(8)
                } else if s.len() <= 128 {
                    512 + s.len().saturating_mul(64)
                } else {
                    1024 + s.len().saturating_mul(8)
                }
            }
            Value::Uuid(_) => 32,
            Value::Timestamp(_) => 16,
            Value::Json(v) => 96 + v.to_string().len().saturating_mul(32),
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
    pub fn estimated_bytes(&self) -> usize {
        64 + estimate_row_value_bytes(&self.values) + self.created_at.map(|_| 8).unwrap_or(0)
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
    pub fn estimated_bytes(&self) -> usize {
        96 + self.edge_type.len().saturating_mul(16) + estimate_row_value_bytes(&self.properties)
    }

    pub fn visible_at(&self, snapshot: SnapshotId) -> bool {
        self.created_tx.0 <= snapshot.0 && self.deleted_tx.is_none_or(|tx| tx.0 > snapshot.0)
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

pub fn estimate_row_value_bytes(values: &HashMap<ColName, Value>) -> usize {
    values.iter().fold(64, |acc, (key, value)| {
        acc.saturating_add(32 + key.len().saturating_mul(8) + value.estimated_bytes())
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
