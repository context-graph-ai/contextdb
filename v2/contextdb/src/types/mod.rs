//! types: `Value`, the one `Error`, ids, table descriptions.
//!
//! It never holds behaviour: nothing here reads a store, checks a rule or decides an answer.

use std::fmt;
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
    /// The position of a commit, as [`TxId`]. No column type of this version stores one.
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
    /// The statement or a declaration names a column its table does not have. Returned by
    /// every `execute`.
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
    /// a space.
    ///
    /// Returned by the INSERT or UPDATE when the other row is already visible to it. Returned
    /// by `Transaction::commit` when another commit took the key or the values while the
    /// transaction was open; then nothing of the transaction was committed.
    UniqueViolation { table: String, column: String },
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
    /// updated or deleted since the transaction read them. `count` is the number of such rows.
    /// Nothing of the transaction was committed; run the whole transaction again. Only a
    /// transaction that was open across another commit can meet this.
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
            Error::UniqueViolation { table, column } => {
                write!(f, "unique constraint violation: {table} ({column})")
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
        }
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests;

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
}

/// A table as it was declared.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct TableMeta {
    pub columns: Vec<ColumnDef>,
    pub unique_constraints: Vec<Vec<String>>,
    pub indexes: Vec<IndexDecl>,
}
