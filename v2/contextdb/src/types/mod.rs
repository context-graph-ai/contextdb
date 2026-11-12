//! types: `Value`, the one `Error`, ids, table descriptions.
//!
//! It never holds behaviour: nothing here reads a store, checks a rule or decides an answer.

use std::fmt;
use std::ops::Range;
use std::path::PathBuf;

pub use uuid::Uuid;

/// What every fallible call of this library returns.
pub type Result<T> = std::result::Result<T, Error>;

/// The position the engine gave a commit. Positions rise with every commit of one store, and a
/// position that reached the disk is never given out again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxId(pub u64);

/// One stored or bound value.
///
/// `params!` and `Value::from` take these Rust types: `bool`, `i64` (not `i32`), `f64`, `&str`,
/// `String`, [`Uuid`], `Vec<f32>` and `serde_json::Value`. A timestamp is written as
/// `Value::Timestamp(milliseconds)`, and no value at all as `Value::Null`.
#[derive(Debug, Clone, PartialEq)]
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

/// Values as a refusal shows them, joined by a comma and a space; a text longer than 64 bytes
/// is cut there, at a whole character, and followed by `...` and its length in bytes.
pub(crate) fn shown(values: &[Value]) -> String {
    let each = values.iter().map(|value| match value {
        Value::Text(text) if text.len() > 64 => {
            let cut = (0..=64).rfind(|&at| text.is_char_boundary(at)).unwrap_or(0);
            format!("{}... ({} bytes)", &text[..cut], text.len())
        }
        Value::Text(text) => text.clone(),
        Value::Int64(number) | Value::Timestamp(number) => number.to_string(),
        Value::Uuid(uuid) => uuid.to_string(),
        other => format!("{other:?}"),
    });
    each.collect::<Vec<_>>().join(", ")
}

/// The memory a value takes outside its own cell.
pub(crate) fn heap_bytes(value: &Value) -> u64 {
    fn json(value: &serde_json::Value) -> u64 {
        let cell = size_of::<serde_json::Value>();
        match value {
            serde_json::Value::String(text) => text.len() as u64,
            serde_json::Value::Array(items) => {
                let items = items.iter();
                items.map(|item| cell as u64 + json(item)).sum()
            }
            serde_json::Value::Object(members) => {
                let members = members.iter();
                members
                    .map(|(name, member)| (name.len() + cell) as u64 + json(member))
                    .sum()
            }
            _ => 0,
        }
    }
    match value {
        Value::Text(text) => text.len() as u64,
        Value::Vector(vector) => size_of_val(vector.as_slice()) as u64,
        Value::Json(value) => json(value),
        _ => 0,
    }
}

/// The most memory the JSON tree made from this text takes, worked out from the text alone,
/// so that it is counted before the tree is made. Outside strings, an object takes a table
/// node of its own and a share of the nodes its members fill, and every member and array
/// item takes a cell with the room its array may grow into; every string is its own bytes.
pub(crate) fn json_bytes(text: &[u8]) -> u64 {
    let (mut bytes, mut quoted, mut escaped) = (text.len() as u64, false, false);
    for &byte in text {
        match byte {
            _ if escaped => escaped = false,
            b'\\' if quoted => escaped = true,
            b'"' => quoted = !quoted,
            _ if quoted => {}
            b'{' => bytes += 768,
            b'[' | b',' | b':' => bytes += 128,
            _ => {}
        }
    }
    bytes
}

/// Every refusal and failure of this library, by kind.
///
/// A caller matches on the kind; the sentence a person reads comes from `Display`. A statement
/// that is refused changes nothing, and inside a transaction the statements before it stand.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The text is not SQL this library reads: a syntax error. Returned by every `execute`,
    /// before anything runs. The text names what was not understood.
    ParseError(String),
    /// The statement reads as SQL and cannot be run as written. Returned by every `execute`:
    /// for a statement or clause that only a later version supports (the text names it and
    /// says so), a `$name` with no bound value, an INSERT whose values and columns differ in
    /// number, arithmetic on a value that is not a number, an integer out of range, or a
    /// division by zero.
    PlanError(String),
    /// The statement names a table that is not declared. Carries the bare table name. Returned
    /// by every `execute` and by `declaration`.
    TableNotFound(String),
    /// The statement or a declaration names a column its table does not have. For a name
    /// written with a qualifier that is no table or alias of the statement, `table` is the
    /// qualifier as written. Returned by every `execute`.
    ColumnNotFound { table: String, column: String },
    /// An INSERT or UPDATE wrote a value that is not of its column's type. The only
    /// conversions are quoted text into a UUID, TIMESTAMP, JSON or VECTOR column and an INTEGER
    /// into a REAL column; text that does not read as the column's type, and a vector whose
    /// length is not the declared dimension, are refused. `expected` is the column's SQL type
    /// and `actual` what was given.
    ColumnTypeMismatch {
        table: String,
        column: String,
        expected: String,
        actual: String,
    },
    /// An INSERT or UPDATE left no value in a column declared NOT NULL.
    ColumnNotNullable { table: String, column: String },
    /// A row with this primary key, or with these values under a UNIQUE rule, is already
    /// there. `column` names every column of the rule in declared order, joined by a comma and
    /// a space; `key` is the values the refused row has in them, as text.
    ///
    /// Returned by the INSERT or UPDATE when the other row is already visible to it. Returned
    /// by `Transaction::commit` when another commit took the key or the values while the
    /// transaction was open; then nothing of the transaction was committed.
    UniqueViolation {
        table: String,
        column: String,
        key: String,
    },
    /// A declaration that cannot stand. `reason` is a sentence for a person, not a table name:
    /// a `CREATE TABLE` of a name that is taken, a column declared twice, a second PRIMARY KEY,
    /// a DEFAULT that is not a constant or `NOW()`. Returned by the `execute` that declares.
    ///
    /// Also returned by `Transaction::commit` when a table or an index was declared while the
    /// transaction was open: nothing of it was committed, and it can be run again.
    SchemaInvalid { reason: String },
    /// A PRIMARY KEY, a UNIQUE rule or an index was declared over a JSON or VECTOR column, or a
    /// PRIMARY KEY or a UNIQUE rule over a REAL column. Returned by `CREATE TABLE` and
    /// `CREATE INDEX`.
    ColumnNotIndexable {
        table: String,
        column: String,
        column_type: ColumnType,
    },
    /// `CREATE INDEX` gave a name another index of the table already has.
    DuplicateIndex { table: String, index: String },
    /// `CREATE INDEX` gave a name that begins with `__pk_`, `__unique_` or `__fk_`, which the
    /// engine keeps for the indexes it makes itself. `prefix` is the one the name begins with.
    ReservedIndexName {
        table: String,
        name: String,
        prefix: String,
    },
    /// A SELECT's ORDER BY names something the engine does not sort by. It sorts by a column,
    /// by a name the SELECT list gives a column, and by `COALESCE(...)`, `NOW()` and
    /// arithmetic (`+`, `-`, `*`, `/`) over them.
    OrderByExpressionNotSupported,
    /// `Transaction::execute` was asked to write a row in a transaction that had declared a
    /// table or an index, or to declare one in a transaction that had written a row.
    /// Declarations and row writes commit in separate transactions.
    MixedSchemaAndRowWrite,
    /// `Transaction::commit` found that another commit had changed rows this transaction
    /// updated or deleted since the transaction read them, or rows it asked to stay as they
    /// were until it committed. `count` is the number of such rows. Nothing of the transaction
    /// was committed; run the whole transaction again. Only a transaction that was open across
    /// another commit can meet this.
    ConditionalUpdateConflict { count: u64 },
    /// A call was made on a `Database` after its `close`.
    DatabaseClosed,
    /// `ReadSession::execute` was given a statement that is not a SELECT. Nothing ran.
    ReadOnlySession,
    /// `ReadSession::open` found no store at this path. A read session never creates one;
    /// `Database::open` does.
    StoreMissing { path: PathBuf },
    /// `Database::open` found the store held for writing by another `Database`, in another
    /// process or in this one. `holder_pid` is the process that holds it, which is this
    /// process's own id when the other handle is in this process. A read session opens
    /// regardless.
    DatabaseLocked { holder_pid: u32, path: PathBuf },
    /// Stored data failed its check when it was read. Never returned for anything else. Any
    /// call that reads the store can return it: an open, an `execute`, a commit.
    StoreCorrupted { path: PathBuf, reason: String },
    /// The store's folder or file could not be created, read or written, and nothing says the
    /// stored data is damaged: for one, a full disk or a refused permission. Any call that
    /// touches the store can return it. `operation` is `"open"`, `"read"` or `"commit"`, and
    /// `reason` is the operating system's.
    StoreIo {
        operation: &'static str,
        path: PathBuf,
        reason: String,
    },
    /// Memory that grows with data would have passed the declared memory limit. Nothing was
    /// taken, committed work is intact, and the handle stays usable. Returned by an open whose
    /// limit is too small to open with, by any `execute` or commit, and by
    /// `Database::set_memory_limit` when more than the new limit is in use.
    ///
    /// `operation` says what was being done, `requested_bytes` what it asked for,
    /// `available_bytes` what was free and `budget_limit_bytes` the limit, all in bytes.
    MemoryBudgetExceeded {
        subsystem: &'static str,
        operation: &'static str,
        requested_bytes: u64,
        available_bytes: u64,
        budget_limit_bytes: u64,
        hint: &'static str,
    },
    /// A vector written into a VECTOR column, or the vector of a search, holds a number that
    /// is not finite. `position` counts its numbers from zero. Returned by the INSERT or UPDATE
    /// that wrote it, or by the SELECT that searched with it.
    InvalidVectorValue {
        index: VectorIndexRef,
        position: usize,
    },
    /// The vector of a search has a different length from the dimension its VECTOR column
    /// declares. `expected` is the declared dimension, `actual` the length given.
    VectorIndexDimensionMismatch {
        index: VectorIndexRef,
        expected: usize,
        actual: usize,
    },
    /// The column on the left of `<=>` is not a VECTOR column.
    UnknownVectorIndex { index: VectorIndexRef },
    /// A `PARTITION_KEY` or `MAX_PARTITIONS` clause that cannot stand; `issue` says why.
    /// Returned by `CREATE TABLE`, which then declares nothing.
    InvalidVectorPartitionDeclaration {
        index: VectorIndexRef,
        issue: VectorPartitionDeclarationIssue,
    },
    /// A VECTOR column whose declaration cannot stand; `issue` says why. Returned by
    /// `CREATE TABLE`, which then declares nothing.
    InvalidVectorPolicyDeclaration {
        index: VectorIndexRef,
        issue: VectorPolicyDeclarationIssue,
    },
    /// A write would have given the column more partitions than its `MAX_PARTITIONS` allows:
    /// a row with a vector arrived under partition key values no row with a vector has yet.
    /// Returned by the INSERT or UPDATE that brought the row, which then changes nothing and
    /// leaves its transaction usable. Returned by a commit only when another transaction made
    /// a partition while this one was open; nothing of that commit is written.
    VectorPartitionLimitExceeded {
        index: VectorIndexRef,
        max_partitions: u32,
    },
    /// A search asked for `INDEXED`, with `USE VECTOR INDEXED` or through the column's
    /// `SEARCH_MODE INDEXED`, and the index cannot answer it now: a partition it selects is
    /// behind its writes, or its filter on rows leaves the index nothing to choose among. Such
    /// a search is never answered with an exact scan under the indexed name; `EXACT` and
    /// `AUTO` answer, and a maintenance cycle brings the index up to its writes.
    VectorIndexedRouteUnavailable { index: VectorIndexRef },
    /// `USE VECTOR` was written in a SELECT that does not order by `<=>`.
    UseVectorRequiresVectorOrder,
    /// An UPDATE, a DELETE or an `ON CONFLICT DO UPDATE` named a table declared `IMMUTABLE`.
    ImmutableTable { table: String },
    /// A statement would change a column declared `IMMUTABLE`: an UPDATE that assigns it, or
    /// an ALTER that drops or renames it.
    ImmutableColumn { table: String, column: String },
    /// An UPDATE would move a row's state to one its table's `STATE MACHINE` does not list
    /// after the state the row is in.
    InvalidStateTransition {
        table: String,
        column: String,
        from: String,
        to: String,
    },
    /// A commit would leave a reference without its parent row. `still_referenced` says the
    /// parent row was removed or re-keyed while a row of `table` points at it; otherwise a
    /// row of `table` names a parent that is not there. `columns` are the referencing columns,
    /// joined by a comma and a space, and `key` the values they hold, as text.
    ForeignKeyViolation {
        table: String,
        columns: String,
        parent: String,
        still_referenced: bool,
        key: String,
    },
    /// An edge of a type its table lists under `DAG` would close a cycle.
    CycleDetected { table: String, edge_type: String },
    /// A propagation declared `ABORT ON FAILURE` met a row it could not move.
    PropagationAborted { table: String, reason: String },
    /// A write would store a row key, a partition-key tuple or the values of a kept count
    /// longer than the engine keeps: values that identify a row or a group and are kept whole.
    /// `key` is `primary key`, the VECTOR column's name or the count's list as declared. A
    /// value of any length may be indexed: an index is never the reason for this refusal.
    KeyTooLong {
        table: String,
        key: String,
        bytes: u64,
        limit: u64,
    },
    /// `DROP INDEX` named an index its table does not have, without `IF EXISTS`.
    IndexNotFound { table: String, index: String },
    /// `DROP COLUMN` without `CASCADE` named a column that `index` is over.
    ColumnInIndex {
        table: String,
        column: String,
        index: String,
    },
    /// An ALTER would drop or rename a column of a VECTOR column's `PARTITION_KEY`.
    /// `index` is the VECTOR column.
    VectorPartitionKeyInUse {
        table: String,
        column: String,
        index: String,
    },
    /// A DROP or an ALTER named a table, a column or an index that the rank policy `policy`
    /// reads.
    DropBlockedByRankPolicy {
        table: String,
        column: String,
        policy: String,
    },
    /// A `RANK_POLICY` whose declaration cannot stand; `reason` says why.
    RankPolicyInvalid {
        index: VectorIndexRef,
        reason: String,
    },
    /// A handle narrowed to some Contexts named, by its key, a row of another Context, or
    /// wrote a row outside them.
    ContextScopeViolation { table: String },
    /// A handle narrowed by scope labels named, by its key, a row its labels do not allow, or
    /// wrote one.
    ScopeLabelViolation { table: String },
    /// A handle's principal holds no grant for the row it named by its key.
    AclDenied { table: String },
    /// A table with an `ACL REFERENCES` column was read through a narrowed handle that names
    /// no principal able to hold grants.
    PrincipalRequired { table: String },
    /// A narrowed handle ran a statement only the handle that declared no narrowing may run.
    AdminRequired { statement: String },
    /// `CREATE PROCEDURE` or `CREATE FUNCTION`.
    StoredProcNotSupported,
    /// A statement wrote a `TXID` value above its own transaction's number.
    TxIdOutOfRange { value: u64, max: u64 },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ParseError(detail) => write!(f, "parse error: {detail}"),
            Error::PlanError(detail) => write!(f, "plan error: {detail}"),
            Error::TableNotFound(table) => write!(f, "table not found: {table}"),
            Error::ColumnNotFound { table, column } => {
                write!(f, "column not found: {table}.{column}")
            }
            Error::ColumnTypeMismatch {
                table,
                column,
                expected,
                actual,
            } => write!(
                f,
                "column type mismatch: {table}.{column} expected {expected}, got {actual}"
            ),
            Error::ColumnNotNullable { table, column } => {
                write!(f, "column {table}.{column} is NOT NULL")
            }
            Error::UniqueViolation { table, column, key } => {
                write!(
                    f,
                    "unique constraint violation: {table} ({column}) = ({key})"
                )
            }
            Error::SchemaInvalid { reason } => write!(f, "schema invalid: {reason}"),
            Error::ColumnNotIndexable {
                table,
                column,
                column_type,
            } => write!(
                f,
                "column not indexable: {table}.{column} has type {column_type:?}"
            ),
            Error::DuplicateIndex { table, index } => {
                write!(f, "duplicate index: {table}.{index}")
            }
            Error::ReservedIndexName {
                table,
                name,
                prefix,
            } => write!(
                f,
                "reserved index name: {table}.{name} uses reserved prefix `{prefix}`"
            ),
            Error::OrderByExpressionNotSupported => f.write_str(
                "ORDER BY requires a column reference, a SELECT-list alias naming one, \
                 COALESCE(...), or arithmetic (+, -, *, /); this sort key expression is not \
                 supported",
            ),
            Error::MixedSchemaAndRowWrite => f.write_str(
                "a transaction either declares tables or writes rows: commit this one and \
                 run the statement in the next",
            ),
            Error::ConditionalUpdateConflict { count } => write!(
                f,
                "conditional update conflict: {count} staged update(s) no longer matched at commit"
            ),
            Error::DatabaseClosed => f.write_str("database handle is closed"),
            Error::ReadOnlySession => f.write_str(
                "this statement writes and this session only reads: run it on a store opened for \
                 writing",
            ),
            Error::StoreMissing { path } => write!(
                f,
                "there is no store at this path: {}; opening it for writing creates it",
                path.display()
            ),
            Error::DatabaseLocked { holder_pid, path } => write!(
                f,
                "another session holds this store for writing: process {holder_pid} holds {}; \
                 open a read session to read it",
                path.display()
            ),
            Error::StoreCorrupted { path, reason } => {
                write!(f, "corrupt store at {}: {reason}", path.display())
            }
            Error::StoreIo {
                operation,
                path,
                reason,
            } => write!(
                f,
                "store I/O failed during {operation} at {}: {reason}",
                path.display()
            ),
            Error::MemoryBudgetExceeded {
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
                ..
            } => write!(
                f,
                "memory limit reached during {operation}: {requested_bytes} bytes were needed and \
                 {available_bytes} bytes of the {budget_limit_bytes}-byte limit were free. {hint}"
            ),
            Error::InvalidVectorValue { index, position } => write!(
                f,
                "vector {index} contains a non-finite component at position {position}"
            ),
            Error::VectorIndexDimensionMismatch {
                index,
                expected,
                actual,
            } => write!(
                f,
                "vector index dimension mismatch on {index}: expected {expected}, got {actual}"
            ),
            Error::UnknownVectorIndex { index } => write!(f, "unknown vector index {index}"),
            Error::InvalidVectorPartitionDeclaration { index, issue } => {
                write!(
                    f,
                    "invalid vector partition declaration on {index}: {issue}"
                )
            }
            Error::InvalidVectorPolicyDeclaration { index, issue } => {
                write!(f, "invalid vector policy declaration on {index}: {issue}")
            }
            Error::VectorPartitionLimitExceeded {
                index,
                max_partitions,
            } => write!(
                f,
                "vector partition limit reached on {index}: MAX_PARTITIONS is {max_partitions}; \
                 use fewer partition-key values, or declare the table with a larger MAX_PARTITIONS"
            ),
            Error::VectorIndexedRouteUnavailable { index } => write!(
                f,
                "{index} cannot be searched INDEXED for this statement now: its index is behind \
                 its writes, or the statement's filter leaves the index nothing to choose among; \
                 run a maintenance cycle, or use EXACT or AUTO"
            ),
            Error::UseVectorRequiresVectorOrder => {
                f.write_str("USE VECTOR requires ORDER BY embedding <=> $param in the same query")
            }
            Error::ImmutableTable { table } => write!(
                f,
                "table {table} is IMMUTABLE: its rows are inserted once and never updated or \
                 deleted"
            ),
            Error::ImmutableColumn { table, column } => write!(
                f,
                "column {table}.{column} is IMMUTABLE: it is set when the row is inserted and \
                 never changed"
            ),
            Error::InvalidStateTransition {
                table: t,
                column: c,
                from,
                to,
            } => {
                write!(f, "invalid state transition of {t}.{c}: {from} -> {to}")
            }
            Error::ForeignKeyViolation {
                table,
                columns,
                parent,
                still_referenced,
                key,
            } => match still_referenced {
                true => write!(
                    f,
                    "foreign key violation: {parent} is still referenced by {table} ({columns}) \
                     = ({key})"
                ),
                false => write!(
                    f,
                    "foreign key violation: {table} ({columns}) = ({key}) has no row in {parent}"
                ),
            },
            Error::CycleDetected { table, edge_type } => write!(
                f,
                "cycle detected: this {edge_type} edge of {table} would close a cycle"
            ),
            Error::PropagationAborted { table, reason } => {
                write!(f, "propagation aborted on {table}: {reason}")
            }
            Error::KeyTooLong {
                table,
                key,
                bytes,
                limit,
            } => write!(
                f,
                "key too long: the {key} of {table} takes {bytes} bytes and the limit is \
                 {limit}; these values identify a row or a group and are kept whole, so use a \
                 shorter one"
            ),
            Error::IndexNotFound { table, index } => write!(f, "index not found: {table}.{index}"),
            Error::ColumnInIndex {
                table,
                column,
                index,
            } => write!(
                f,
                "column {table}.{column} is used by the index {index}; drop the index first, or \
                 write CASCADE"
            ),
            Error::VectorPartitionKeyInUse {
                table,
                column,
                index,
            } => write!(
                f,
                "column {table}.{column} is in the PARTITION_KEY of {table}.{index}; to change \
                 the key, declare a new table with the key you want and copy the rows"
            ),
            Error::DropBlockedByRankPolicy {
                table,
                column,
                policy,
            } => write!(f, "{table}.{column} is read by the rank policy {policy}"),
            Error::RankPolicyInvalid { index, reason } => {
                write!(f, "invalid rank policy on {index}: {reason}")
            }
            Error::ContextScopeViolation { table } => write!(
                f,
                "context scope violation: this row of {table} is outside the Contexts this \
                 handle was given"
            ),
            Error::ScopeLabelViolation { table } => write!(
                f,
                "scope label violation: this row of {table} is outside the labels this handle \
                 was given"
            ),
            Error::AclDenied { table } => write!(
                f,
                "access denied: this handle's principal holds no grant for this row of {table}"
            ),
            Error::PrincipalRequired { table } => write!(
                f,
                "table {table} is access-controlled: read it through a handle that names a \
                 principal"
            ),
            Error::AdminRequired { statement } => write!(
                f,
                "{statement} reaches every Context, label and principal: run it through the \
                 handle that declared no narrowing"
            ),
            Error::StoredProcNotSupported => {
                f.write_str("stored procedures and functions are not supported")
            }
            Error::TxIdOutOfRange { value, max } => write!(
                f,
                "TXID {value} is above this transaction's own number, {max}"
            ),
        }
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests;

/// Reads an ISO 8601 moment with its offset (`2026-01-01T02:00:00+02:00`,
/// `1970-01-01T00:00:00.250Z`) as milliseconds since the first moment of 1970, UTC. Digits past
/// the millisecond are dropped. `None` when the text is not such a moment.
pub(crate) fn timestamp_millis(text: &str) -> Option<i64> {
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
    let inside = text.trim().strip_prefix('[')?.strip_suffix(']')?;
    inside
        .split(',')
        .map(|number| number.trim().parse().ok())
        .collect()
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
    /// `AUTO_INDEX_AT` is zero or negative.
    AutoIndexAtNotPositive,
    /// `AUTO_INDEX_AT` is larger than the engine keeps.
    AutoIndexAtOutOfRange,
    /// A percentage of `CONSOLIDATION` is not a whole number from 1 to 100.
    ConsolidationPercentOutOfRange,
}

impl fmt::Display for VectorPolicyDeclarationIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DimensionsNotPositive => f.write_str("vector dimensions must be at least 1"),
            Self::InvalidMember { member } => {
                write!(f, "{member} has an invalid vector policy value")
            }
            Self::AutoIndexAtNotPositive => f.write_str("AUTO_INDEX_AT must be a positive integer"),
            Self::AutoIndexAtOutOfRange => {
                f.write_str("AUTO_INDEX_AT is outside the persisted numeric range")
            }
            Self::ConsolidationPercentOutOfRange => {
                f.write_str("vector consolidation percentages must be integers from 1 through 100")
            }
        }
    }
}

/// How a similarity search is answered: what `SEARCH_MODE` declares for a column and what
/// `USE VECTOR` asks for in one statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VectorSearchMode {
    /// The engine chooses: through the index from the column's `AUTO_INDEX_AT` vectors on,
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

/// A table as it was declared, as far as this value has fields for: references, `PROPAGATE`,
/// `EXCLUDE VECTOR`, rank policies, the clauses of a VECTOR column, `DELIVERY MANIFEST`,
/// `EDGE DISCARD` and the rules of who may see a row are not in it; `Database::declaration`
/// returns them.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct TableMeta {
    pub columns: Vec<ColumnDef>,
    pub unique_constraints: Vec<Vec<String>>,
    pub indexes: Vec<IndexDecl>,
    /// The table was declared `IMMUTABLE`.
    pub immutable: bool,
    /// Each of these is `None`, or empty, when the table's declaration does not write it.
    pub state_machine: Option<StateMachine>,
    /// The edge types of `DAG(...)`.
    pub dag: Vec<String>,
    pub retain: Option<Retain>,
    pub history: Option<History>,
    pub sync: Option<SyncDirection>,
    pub conflict: Option<ConflictPolicy>,
    pub kept_counts: Option<KeptCounts>,
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
