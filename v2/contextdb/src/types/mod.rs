//! types: `Value`, the one `Error`, ids, table descriptions.
//!
//! It never holds behaviour: nothing here reads a store, checks a rule or decides an answer.

use std::fmt;
use std::ops::Range;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
pub use uuid::Uuid;

/// What every fallible call of this library returns.
pub type Result<T> = std::result::Result<T, Error>;

/// The position the engine gave a commit. Positions rise with every commit of one store, and a
/// position that reached the disk is never given out again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TxId(pub u64);

/// One stored or bound value.
///
/// `params!` and `Value::from` take these Rust types: `bool`, `i64` (not `i32`), `f64`, `&str`,
/// `String`, [`Uuid`], `Vec<f32>` and `serde_json::Value`. A timestamp is written as
/// `Value::Timestamp(milliseconds)`, and no value at all as `Value::Null`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// No value: SQL NULL. Fits a column of any type that is not declared NOT NULL.
    Null,
    /// A BOOLEAN.
    Bool(bool),
    /// An INTEGER: 64 bits, exact.
    Int64(i64),
    /// A REAL: 64 bits. An INTEGER written into a REAL column is stored as this.
    Float64(f64),
    /// TEXT, in UTF-8.
    Text(String),
    /// A UUID. Quoted text written into a UUID column is read as one, in either case.
    Uuid(Uuid),
    /// Milliseconds since the first moment of 1970, UTC. `NOW()`, `DEFAULT NOW()` and an
    /// ISO 8601 text written into a TIMESTAMP column all give this unit.
    Timestamp(i64),
    /// A JSON value, held as a `serde_json::Value`. Text written into a JSON column is read as
    /// JSON and comes back as this, so spacing and the order of an object's keys are not kept.
    Json(serde_json::Value),
    /// A vector of `f32`. Its length must equal the dimension its `VECTOR(n)` column declares;
    /// any other length is refused with [`Error::ColumnTypeMismatch`].
    Vector(Vec<f32>),
    /// The position of a commit, as [`TxId`]: what a `TXID` column holds.
    TxId(TxId),
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Int64(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Float64(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::Text(value.to_owned())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::Text(value)
    }
}

impl From<Uuid> for Value {
    fn from(value: Uuid) -> Self {
        Value::Uuid(value)
    }
}

impl From<serde_json::Value> for Value {
    fn from(value: serde_json::Value) -> Self {
        Value::Json(value)
    }
}

impl From<Vec<f32>> for Value {
    fn from(value: Vec<f32>) -> Self {
        Value::Vector(value)
    }
}

struct Brief(String, usize);

impl fmt::Write for Brief {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let mut end = text.len().min(64usize.saturating_sub(self.1));
        self.1 = self.1.saturating_add(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        self.0.push_str(&text[..end]);
        Ok(())
    }
}

impl Brief {
    fn finish(mut self) -> String {
        if self.1 > self.0.len() {
            let _ = fmt::write(&mut self.0, format_args!("... ({} bytes)", self.1));
        }
        self.0
    }
}

/// Error input: its first 64 bytes on a whole character, then its original byte length.
pub(crate) fn excerpt(value: impl fmt::Display) -> String {
    let mut brief = Brief(String::with_capacity(96), 0);
    let _ = fmt::write(&mut brief, format_args!("{value}"));
    brief.finish()
}

/// Values as one bounded error excerpt, without building their complete rendering.
pub(crate) fn shown(values: &[Value]) -> String {
    let mut brief = Brief(String::with_capacity(96), 0);
    for (at, value) in values.iter().enumerate() {
        if at > 0 {
            let _ = fmt::Write::write_str(&mut brief, ", ");
        }
        let _ = match value {
            Value::Text(text) => fmt::write(&mut brief, format_args!("{text}")),
            Value::Int64(n) | Value::Timestamp(n) => fmt::write(&mut brief, format_args!("{n}")),
            Value::Uuid(uuid) => fmt::write(&mut brief, format_args!("{uuid}")),
            other => fmt::write(&mut brief, format_args!("{other:?}")),
        };
    }
    brief.finish()
}

// A BTree node holds eleven pairs and at most 112 overhead bytes; non-root nodes hold five.
const JSON_OBJECT: u64 = 768;
const JSON_MEMBER: u64 = 256;
const _: () = assert!(JSON_OBJECT >= 11 * size_of::<(String, serde_json::Value)>() as u64 + 112);
const _: () = assert!(JSON_MEMBER >= JSON_OBJECT / 5);

/// The memory a value takes outside its own cell.
pub(crate) fn heap_bytes(value: &Value) -> u64 {
    fn json(value: &serde_json::Value) -> u64 {
        let cell = size_of::<serde_json::Value>();
        match value {
            serde_json::Value::String(text) => text.capacity() as u64,
            serde_json::Value::Array(items) => {
                (items.capacity() * cell) as u64 + items.iter().map(json).sum::<u64>()
            }
            serde_json::Value::Object(members) => {
                JSON_OBJECT
                    + JSON_MEMBER * members.len() as u64
                    + members
                        .iter()
                        .map(|(name, member)| name.capacity() as u64 + json(member))
                        .sum::<u64>()
            }
            _ => 0,
        }
    }
    match value {
        Value::Text(text) => text.capacity() as u64,
        Value::Vector(vector) => (vector.capacity() * size_of::<f32>()) as u64,
        Value::Json(value) => json(value),
        _ => 0,
    }
}

/// The most memory the JSON tree made from this text takes, worked out from the text alone,
/// so that it is counted before the tree is made. Outside strings, an object takes a table
/// node of its own and a share of the nodes its members fill, and every member and array
/// item takes a cell with the room its array may grow into; every string is its own bytes.
pub(crate) fn json_bytes(text: &[u8]) -> u64 {
    let (mut bytes, mut quoted, mut escaped) = (2 * text.len() as u64, false, false);
    for &byte in text {
        match byte {
            _ if escaped => escaped = false,
            b'\\' if quoted => escaped = true,
            b'"' => quoted = !quoted,
            _ if quoted => {}
            b'{' => bytes += JSON_OBJECT,
            b'[' | b',' | b':' => bytes += JSON_MEMBER,
            _ => {}
        }
    }
    bytes
}

mod error;
pub use error::Error;

#[cfg(test)]
mod tests;

/// Reads an ISO 8601 moment with its offset (`2026-01-01T02:00:00+02:00`,
/// `1970-01-01T00:00:00.250Z`) as milliseconds since the first moment of 1970, UTC. Digits past
/// the millisecond are dropped. `None` when the text is not such a moment.
pub(crate) fn timestamp_millis(text: &str) -> Option<i64> {
    if let Some(at) = ["-infinity", "infinity"].iter().position(|t| *t == text) {
        return Some([i64::MIN, i64::MAX][at]);
    }
    let (year, month, day) = (
        digits(text, 0..4)?,
        digits(text, 5..7)?,
        digits(text, 8..10)?,
    );
    let (hour, minute, second) = (
        digits(text, 11..13)?,
        digits(text, 14..16)?,
        digits(text, 17..19)?,
    );
    let marks = text.as_bytes();
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days = match month {
        2 => 28 + i64::from(leap),
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if [marks[4], marks[7], marks[13], marks[16]] != *b"--::"
        || !matches!(marks[10], b'T' | b't')
        || !(1..=12).contains(&month)
        || !(1..=month_days).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }

    let mut rest = &text[19..];
    let mut millis = 0;
    if let Some(fraction) = rest.strip_prefix('.') {
        let end = fraction
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(fraction.len());
        if end == 0 {
            return None;
        }
        millis = format!("{:0<3}", &fraction[..end.min(3)]).parse().ok()?;
        rest = &fraction[end..];
    }
    let offset_minutes = match rest.as_bytes() {
        b"Z" | b"z" => 0,
        [sign @ (b'+' | b'-'), _, _, b':', _, _] => {
            let minutes = digits(rest, 1..3)? * 60 + digits(rest, 4..6)?;
            if *sign == b'-' { -minutes } else { minutes }
        }
        _ => return None,
    };

    // Days since 1970 of a date in the Gregorian calendar, counting years from March so that
    // the leap day is the last day of its year.
    let (year, month) = if month > 2 {
        (year, month - 3)
    } else {
        (year - 1, month + 9)
    };
    let leap_days = year.div_euclid(4) - year.div_euclid(100) + year.div_euclid(400);
    let days = year * 365 + leap_days + (153 * month + 2) / 5 + day - 719_469;
    Some(((days * 24 + hour) * 60 + minute - offset_minutes) * 60_000 + second * 1_000 + millis)
}

/// The number written in these positions of the text, when every one of them is a digit.
fn digits(text: &str, positions: Range<usize>) -> Option<i64> {
    let part = text.get(positions)?;
    if part.bytes().all(|byte| byte.is_ascii_digit()) {
        part.parse().ok()
    } else {
        None
    }
}

/// Reads the text spelling of a vector, `[0.1, 0.2, 0.3]`: the one reader of it, for a value
/// written into a VECTOR column and for the vector of a search.
pub(crate) fn vector_from_text(text: &str) -> Option<Vec<f32>> {
    vector_numbers(text)?.collect()
}

/// The one vector-text reader, with no list allocated until its caller collects the numbers.
pub(crate) fn vector_numbers(text: &str) -> Option<impl Iterator<Item = Option<f32>> + Clone + '_> {
    let inside = text.trim().strip_prefix('[')?.strip_suffix(']')?;
    Some(inside.split(',').map(|number| number.trim().parse().ok()))
}

/// Brings a value to its column's type, or refuses it with `ColumnTypeMismatch`.
///
/// NULL fits every type here; whether the column takes it is the NOT NULL rule's business. The
/// only conversions are quoted text into a UUID, TIMESTAMP, JSON or VECTOR column and an INTEGER
/// into a REAL column. A vector of the column's length with a number that is not finite is
/// refused with `InvalidVectorValue`, however it was written: no search could measure it.
pub(crate) fn typed(table: &str, column: &ColumnDef, value: Value) -> Result<Value> {
    let converted = match (column.column_type, &value) {
        (_, Value::Null)
        | (ColumnType::Integer, Value::Int64(_))
        | (ColumnType::Real, Value::Float64(_))
        | (ColumnType::Text, Value::Text(_))
        | (ColumnType::Boolean, Value::Bool(_))
        | (ColumnType::Json, Value::Json(_))
        | (ColumnType::Uuid, Value::Uuid(_))
        | (ColumnType::Timestamp, Value::Timestamp(_))
        | (ColumnType::TxId, Value::TxId(_)) => return Ok(value),
        (ColumnType::Vector(length), Value::Vector(vector)) if vector.len() == length => {
            return finite(table, column, value);
        }
        (ColumnType::Real, Value::Int64(whole)) => Some(Value::Float64(*whole as f64)),
        (ColumnType::Uuid, Value::Text(text)) => Uuid::parse_str(text).ok().map(Value::Uuid),
        (ColumnType::Timestamp, Value::Text(text)) => timestamp_millis(text).map(Value::Timestamp),
        (ColumnType::Json, Value::Text(text)) => serde_json::from_str(text).ok().map(Value::Json),
        (ColumnType::Vector(length), Value::Text(text)) => vector_from_text(text)
            .filter(|vector| vector.len() == length)
            .map(Value::Vector),
        _ => None,
    };
    let converted = converted.ok_or_else(|| Error::ColumnTypeMismatch {
        table: table.to_owned(),
        column: column.name.clone(),
        expected: column.column_type.to_string(),
        actual: kind(&value),
    })?;
    finite(table, column, converted)
}

/// The value itself, unless it is a vector that holds a number that is not finite.
fn finite(table: &str, column: &ColumnDef, value: Value) -> Result<Value> {
    if let Value::Vector(vector) = &value
        && let Some(position) = vector.iter().position(|number| !number.is_finite())
    {
        return Err(Error::InvalidVectorValue {
            index: VectorIndexRef {
                table: table.to_owned(),
                column: column.name.clone(),
            },
            position,
        });
    }
    Ok(value)
}

/// What a refusal calls the value it was given. A vector says how long it is, since its length
/// is what a VECTOR column refuses it for.
fn kind(value: &Value) -> String {
    match value {
        Value::Null => "Null",
        Value::Bool(_) => "Bool",
        Value::Int64(_) => "Int64",
        Value::Float64(_) => "Float64",
        Value::Text(_) => "Text",
        Value::Uuid(_) => "Uuid",
        Value::Timestamp(_) => "Timestamp",
        Value::Json(_) => "Json",
        Value::Vector(vector) => return format!("Vector({})", vector.len()),
        Value::TxId(_) => "TxId",
    }
    .to_owned()
}

/// The declared type of a column, with the names SQL gives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    /// `INTEGER` or `INT`.
    Integer,
    /// `REAL` or `FLOAT`.
    Real,
    /// `TEXT`.
    Text,
    /// `BOOLEAN` or `BOOL`.
    Boolean,
    /// `JSON`.
    Json,
    /// `UUID`.
    Uuid,
    /// `VECTOR(n)`, with its dimension `n`.
    Vector(usize),
    /// `TIMESTAMP`.
    Timestamp,
    /// `TXID`: the number of a transaction, as [`TxId`].
    TxId,
}

/// The type as SQL names it.
impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ColumnType::Integer => "INTEGER",
            ColumnType::Real => "REAL",
            ColumnType::Text => "TEXT",
            ColumnType::Boolean => "BOOLEAN",
            ColumnType::Json => "JSON",
            ColumnType::Uuid => "UUID",
            ColumnType::Vector(size) => return write!(f, "VECTOR({size})"),
            ColumnType::Timestamp => "TIMESTAMP",
            ColumnType::TxId => "TXID",
        })
    }
}

/// A VECTOR column as a refusal names it: its table, then its own name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorIndexRef {
    pub table: String,
    pub column: String,
}

impl fmt::Display for VectorIndexRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.table, self.column)
    }
}

/// Why a `PARTITION_KEY` or `MAX_PARTITIONS` clause was refused. None of these carries a value
/// a statement bound, so printing one never echoes data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum VectorPartitionDeclarationIssue {
    /// A column of the key is not a column of the table.
    UnknownPartitionKeyColumn,
    /// A column of the key may hold NULL.
    NullablePartitionKeyColumn,
    /// A column of the key is REAL, JSON or VECTOR.
    UnsupportedPartitionKeyColumnType,
    /// The key names a column twice.
    DuplicatePartitionKeyColumn,
    /// The key names the VECTOR column it belongs to.
    VectorColumnInPartitionKey,
    /// `MAX_PARTITIONS` was written on a column with no `PARTITION_KEY`.
    MaxPartitionsWithoutPartitionKey,
    /// `MAX_PARTITIONS` is zero or negative.
    MaxPartitionsNotPositive,
    /// `MAX_PARTITIONS` is larger than the engine keeps.
    MaxPartitionsOutOfRange,
}

impl fmt::Display for VectorPartitionDeclarationIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnknownPartitionKeyColumn => {
                "PARTITION_KEY references a column that does not exist in the same table"
            }
            Self::NullablePartitionKeyColumn => "every PARTITION_KEY column must be NOT NULL",
            Self::UnsupportedPartitionKeyColumnType => {
                "PARTITION_KEY columns must use UUID, TEXT, INTEGER, BOOLEAN or TIMESTAMP"
            }
            Self::DuplicatePartitionKeyColumn => "a PARTITION_KEY column may appear only once",
            Self::VectorColumnInPartitionKey => {
                "PARTITION_KEY may not contain the vector column itself"
            }
            Self::MaxPartitionsWithoutPartitionKey => "MAX_PARTITIONS requires PARTITION_KEY",
            Self::MaxPartitionsNotPositive => "MAX_PARTITIONS must be a positive integer",
            Self::MaxPartitionsOutOfRange => {
                "MAX_PARTITIONS is outside the persisted numeric range"
            }
        })
    }
}

/// Why a VECTOR column's declaration was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum VectorPolicyDeclarationIssue {
    /// `VECTOR(0)`.
    DimensionsNotPositive,
    /// `WITH (...)` gives this member a value it does not take.
    InvalidMember { member: &'static str },
    /// `INDEXING_THRESHOLD` is zero or negative.
    IndexingThresholdNotPositive,
    /// `INDEXING_THRESHOLD` is larger than the engine keeps.
    IndexingThresholdOutOfRange,
    /// `PROBES` is not a whole number of 1 or more.
    ProbesNotPositive,
    /// A percentage of `COMPACTION` is not a whole number from 1 to 100.
    CompactionPercentOutOfRange,
}

impl fmt::Display for VectorPolicyDeclarationIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DimensionsNotPositive => f.write_str("vector dimensions must be at least 1"),
            Self::InvalidMember { member } => {
                write!(f, "{member} has an invalid vector policy value")
            }
            Self::IndexingThresholdNotPositive => {
                f.write_str("INDEXING_THRESHOLD must be a positive integer")
            }
            Self::IndexingThresholdOutOfRange => {
                f.write_str("INDEXING_THRESHOLD is outside the persisted numeric range")
            }
            Self::ProbesNotPositive => {
                f.write_str("PROBES takes a whole number of 1 or more, or DEFAULT")
            }
            Self::CompactionPercentOutOfRange => {
                f.write_str("vector compaction percentages must be integers from 1 through 100")
            }
        }
    }
}

/// How a similarity search is answered: what `SEARCH_MODE` declares for a column and what
/// `USE VECTOR` asks for in one statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VectorSearchMode {
    /// The engine chooses: through the index from the column's `INDEXING_THRESHOLD` vectors on,
    /// wherever the index is usable, and exactly otherwise. The answer is never short.
    #[default]
    Auto,
    /// Every allowed vector is compared: the true nearest rows.
    Exact,
    /// Only through the index; refused when the index cannot answer the statement now.
    Indexed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortDirection {
    #[default]
    Asc,
    Desc,
}

/// What a column takes when a row is written without a value for it.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnDefault {
    /// A fixed value. `schema` builds it when the table is declared and when it is loaded, and
    /// prints it back; `write` clones it into the row.
    Literal(Value),
    /// `DEFAULT NOW()`: `write` takes the clock's milliseconds when the row is written.
    Now,
}

/// The stored kind byte and SQL vocabulary for each schema namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub(crate) enum Kind {
    Table = 1,
    Trigger,
    EventType,
    Sink,
    Route,
    Schedule,
}
impl Kind {
    /// A decoded name can contain any character; a leading kind byte keeps namespaces disjoint.
    pub(crate) fn key(self, name: &str) -> String {
        format!("{}{name}", char::from(self as u8))
    }

    pub(crate) fn words(self) -> (&'static str, &'static str, &'static str) {
        match self {
            Self::Table => ("table", "CREATE TABLE", "DROP TABLE"),
            Self::Trigger => ("trigger", "CREATE TRIGGER", "DROP TRIGGER"),
            Self::EventType => ("event type", "CREATE EVENT TYPE", "DROP EVENT TYPE"),
            Self::Sink => ("sink", "CREATE SINK", "DROP SINK"),
            Self::Route => ("route", "CREATE ROUTE", "DROP ROUTE"),
            Self::Schedule => ("schedule", "CREATE SCHEDULE", "DROP SCHEDULE"),
        }
    }
}

/// One changed row supplied to a callback before the complete transaction is validated.
/// Writes through the supplied handle belong to this transaction; external effects must wait
/// until durable commit. Initial rows have depth one; callback writes advance it.
#[derive(Debug, Clone, PartialEq)]
pub struct TriggerContext {
    pub trigger_name: String,
    pub table: String,
    pub event: TriggerEvent,
    pub tx: TxId,
    pub depth: u32,
    pub row_values: std::collections::HashMap<String, Value>,
}

/// The changes a transactional trigger handles; deletion belongs to event types only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerEvent {
    Insert,
    Update,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventTypeTrigger {
    Insert,
    Update,
    Delete,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkType {
    Callback,
    Webhook,
}
/// How a VECTOR column keeps its vectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Quantization {
    /// Every number as it was written, in 32 bits.
    #[default]
    F32,
    /// One scale and one byte a number: a write keeps the nearest such vector.
    Sq8,
}

/// What a schedule does with ticks missed while it could not run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissedTickPolicy {
    SkipAndAudit,
    CatchUp,
    FailLoud,
}
/// Public descriptions, also used by the schema record codec.
macro_rules! declarations {
    ($($name:ident { $($field:ident: $kind:ty),* })*) => {$(
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name { $(pub $field: $kind,)* }
    )*};
}
declarations! {
    TriggerDecl { name: String, table: String, event: TriggerEvent, including_sync: bool }
    EventTypeDecl { name: String, table: String, event: EventTypeTrigger }
    SinkDecl { name: String, sink_type: SinkType, url: Option<String> }
    RouteWhereIn { column: String, values: Vec<String> }
    RouteDecl { name: String, event_type: String, sink: String, where_in: Option<RouteWhereIn> }
    ScheduleDecl { name: String, every_ms: u64, callback: String, missed_tick_policy: MissedTickPolicy, catch_up_within_seconds: Option<u32> }
    VectorOptions { column: String, partition_key_columns: Vec<String>, max_partitions: Option<u32>, search_mode: Option<VectorSearchMode>, quantization: Quantization }
}
/// The committed event declarations, each list in name order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeclaredEvents {
    pub triggers: Vec<TriggerDecl>,
    pub event_types: Vec<EventTypeDecl>,
    pub sinks: Vec<SinkDecl>,
    pub routes: Vec<RouteDecl>,
}
/// One column of a table as it was declared.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ColumnDef {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default: Option<ColumnDefault>,
}

/// One declared index: its name and its columns in order.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct IndexDecl {
    pub name: String,
    pub columns: Vec<(String, SortDirection)>,
    /// The keys of `(column -> 'key' -> ...)` when the index is over a path into its one JSON
    /// column; empty for an index over columns.
    pub json_path: Vec<String>,
}

/// A table's columns, indexes and declared options. `Database::declaration` also returns
/// references without propagation, a VECTOR column's INDEXING_THRESHOLD and COMPACTION, rank
/// policies, Context and ACL rules.
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct TableMeta {
    pub columns: Vec<ColumnDef>,
    pub unique_constraints: Vec<Vec<String>>,
    pub indexes: Vec<IndexDecl>,
    /// The table was declared `IMMUTABLE`.
    pub immutable: bool,
    /// Each of these is `None`, or empty, when the table's declaration does not write it.
    pub state_machine: Option<StateMachine>,
    /// The columns declared `IMMUTABLE`, and the one declared `EXPIRES`.
    pub immutable_columns: Vec<String>,
    pub expires_column: Option<String>,
    /// The edge types of `DAG(...)`.
    pub dag: Vec<String>,
    pub retain: Option<Retain>,
    pub history: Option<History>,
    pub sync: Option<SyncDirection>,
    pub conflict: Option<ConflictPolicy>,
    pub kept_counts: Option<KeptCounts>,
    /// Sorted, distinct members named by DELIVERY MANIFEST OVER.
    pub delivery_manifest: Vec<String>,
    pub edge_discard: Option<crate::sql::ast::EdgeDiscard>,
    pub propagation_rules: Vec<(Vec<String>, crate::sql::ast::Reference)>,
    pub vectors: Vec<VectorOptions>,
    pub scope_label: Option<(String, crate::sql::ast::ScopeLabels)>,
    pub propagate_edges: Vec<crate::sql::ast::EdgePropagation>,
    pub exclude_vector_states: Vec<String>,
}

/// `STATE MACHINE (column: state -> [next, ...], ...)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateMachine {
    pub column: String,
    pub transitions: Vec<(String, Vec<String>)>,
}

/// `RETAIN count unit [SYNC SAFE]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Retain {
    pub count: u64,
    pub unit: RetainUnit,
    pub sync_safe: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetainUnit {
    Seconds,
    Minutes,
    Hours,
    Days,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum History {
    All,
    CurrentOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDirection {
    Off,
    PushOnly,
    PullOnly,
    TwoWay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictPolicy {
    KeepFirst,
    KeepLatest,
}

/// `MAINTAIN COUNT NONE`, `ALL PREFIXES`, or the column lists written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeptCounts {
    None,
    AllPrefixes,
    Lists(Vec<Vec<(String, SortDirection)>>),
}

/// The Context a row belongs to, as its `CONTEXT_ID` column holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContextId(pub Uuid);

impl ContextId {
    /// The Context with this id.
    pub fn new(uuid: Uuid) -> ContextId {
        ContextId(uuid)
    }
}

/// One label of a table's `SCOPE_LABEL` column.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScopeLabel(pub String);

impl ScopeLabel {
    /// The label with this name.
    pub fn new(label: impl Into<String>) -> ScopeLabel {
        ScopeLabel(label.into())
    }
}

/// Who a handle acts for, as a table's access-control grants name them.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Principal {
    /// The system itself.
    System,
    /// An agent, by name.
    Agent(String),
    /// A person, by name.
    Human(String),
}

/// One durable action, delivered only after its rows have committed.
#[derive(Debug, Clone, PartialEq)]
pub struct SinkEvent {
    /// Stable receiver deduplication identity, retained across retries and reopen.
    pub action_id: u64,
    pub event_type: String,
    pub table: String,
    pub row_values: std::collections::HashMap<String, Value>,
    pub severity: String,
    /// The actual durable commit containing the action and its rows.
    pub at_lsn: TxId,
}
/// The host's acknowledgement of an external action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkError {
    Transient(String),
    /// The action may have succeeded; retry retains the same action identity.
    OutcomeUncertain(String),
    Permanent(String),
}
