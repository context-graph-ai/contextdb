//! The library's refusals.

use super::*;

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
    /// Transactional triggers support INSERT and UPDATE only.
    TriggerEventUnsupported,
    /// No stored trigger has this name.
    TriggerNotDeclared { trigger_name: String },
    /// No stored sink has this name.
    SinkNotDeclared { sink_name: String },
    /// A principal-scoped handle cannot register another principal.
    SinkPrincipalMismatch,
    /// This open already has a callback registered for the trigger.
    TriggerAlreadyRegistered { trigger_name: String },
    /// A firing trigger has no host callback; nothing of the transaction commits.
    TriggerCallbackMissing { trigger_name: String },
    /// A write to a table whose declared trigger has no callback registered yet.
    EngineNotInitialized { operation: String },
    /// Callback writes exceeded the carried cascade bound; nothing commits.
    TriggerCascadeDepthExceeded { trigger_name: String, depth: u32 },
    /// A host callback panicked; nothing of its transaction commits.
    TriggerCallbackFailed {
        trigger_name: String,
        reason: String,
    },
    /// A callback handle was used outside its active thread, or a write waited past
    /// `callback_wait` behind a trigger's callback.
    CallbackActive { operation: String },
    /// A callback asked for what it may not do while it runs: a control operation, `SET`,
    /// `DROP TRIGGER`, a `PURGE` in a sink, or a write behind a schedule's callback.
    CallbackReentry { operation: String },
    /// Fail-loud schedules refuse multiple due ticks after consuming them.
    MissedTicksExceeded { ticks: u32, policy: String },
    /// The statement names a table that is not declared. Carries the bare table name. Returned
    /// by `execute`, `declaration`, `table_meta`, `explain` and `guard_row_conditions`.
    TableNotFound(String),
    /// The statement or a declaration names a column its table does not have. For a name
    /// written with a qualifier that is no table or alias of the statement, `table` is the
    /// qualifier as written. Returned by `execute`, `explain` and `guard_row_conditions`.
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
    /// `CREATE INDEX` gave a name that begins with `__pk_`, `__unique_`, `__fk_`,
    /// `__graph_` or `__retain_`, which the
    /// engine keeps for the indexes it makes itself. `prefix` is the one the name begins with.
    ReservedIndexName {
        table: String,
        name: String,
        prefix: String,
    },
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
    /// No writer is listening at this store.
    OwnerNotRunning,
    /// The writer is closing.
    OwnerNotServing,
    /// The host refused these opaque bytes.
    OwnerRefused { reason: String },
    /// The request or answer exceeds its byte ceiling.
    OwnerLimitExceeded,
    /// The connection ended before the answer was complete.
    OwnerDisconnected,
    /// `ReadSession::execute` was given a statement that is not read-only.
    /// SELECT, SHOW and EXPLAIN are read-only; writes and declarations are refused.
    ReadOnlySession,
    /// A purge cut off this held view before it could publish its answer.
    AuthoritativeErasure,
    /// Erasure cannot be staged in an explicit transaction.
    PurgeRequiresStandaloneExecution,
    /// Fetch was called after explicit close or a terminal refusal.
    CursorClosed,
    /// Only SELECT statements can be opened as cursors.
    CursorInvalidStatement,
    /// A held moment was supplied to a different store.
    SnapshotMismatch,
    /// A bounded read would exceed this declared ceiling; the session stays usable.
    /// `field` is `"work"`, `"result_rows"` or `"result_bytes"`; `limit` is its ceiling.
    ReadLimitExceeded { field: &'static str, limit: u64 },
    /// A statement ended by its timeout or the caller's cancellation (SQLSTATE 57014).
    QueryCanceled { statement_timeout: bool },
    /// `ReadSession::open` found no store at this path. A read session never creates one;
    /// `Database::open` does.
    StoreMissing { path: PathBuf },
    /// A writer open's deadline passed while another writer holds the store.
    DatabaseLocked { path: PathBuf },
    /// A writer open's deadline passed while readers hold the store.
    HeldByReaders { path: PathBuf },
    /// The caller cancelled its writer open while a writer holds the store.
    OpenCancelled { path: PathBuf },
    /// Stored data failed its check when it was read. Never returned for anything else. Any
    /// call that reads the store can return it: an open, an `execute`, a commit.
    StoreCorrupted { path: PathBuf, reason: String },
    /// The store's folder or file could not be created, read or written, and nothing says the
    /// stored data is damaged: for one, a full disk or a refused permission. Any call that
    /// touches the store can return it. `operation` is `"open"`, `"read"`, `"commit"` or `"channel"` (with an empty
    /// path for channel startup), and
    /// `reason` is the operating system's.
    StoreIo {
        operation: &'static str,
        path: PathBuf,
        reason: String,
    },
    /// A declared disk ceiling refused growth; committed work is intact and the handle stays usable.
    DiskLimitExceeded {
        operation: &'static str,
        current_bytes: u64,
        budget_limit_bytes: u64,
        hint: &'static str,
    },
    /// Memory that grows with data would have passed the declared memory limit. Nothing was
    /// taken, committed work is intact, and the handle stays usable. Returned by an open whose
    /// limit is too small to open with, by any `execute` or commit, and by
    /// `Database::set_memory_limit` when more than the new limit is in use.
    ///
    /// `operation` says what was being done, `requested_bytes` what it asked for,
    /// `available_bytes` what was free (or the rejected new limit for a `set_limit`
    /// refusal), and `budget_limit_bytes` the limit, all in bytes. `subsystem` identifies
    /// the budgeted part; `hint` is human-readable recovery advice, not a stable code.
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
    /// that wrote it, or by a planned nearest-first, limited similarity search. Ordinary
    /// distance expressions outside that search path do not perform this finite check.
    InvalidVectorValue {
        index: VectorIndexRef,
        position: usize,
    },
    /// The vector of a search has a different length from the dimension its VECTOR column
    /// declares. `expected` is the declared dimension, `actual` the length given.
    /// Applies to planned nearest-first, limited similarity searches. Other distance
    /// expressions return `PlanError` on a mismatch when a non-NULL row is evaluated.
    VectorIndexDimensionMismatch {
        index: VectorIndexRef,
        expected: usize,
        actual: usize,
    },
    /// A planned nearest-first, limited similarity search names a non-VECTOR column
    /// on the left of `<=>`. Other distance-expression paths use `PlanError` instead.
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
    /// Also returned when ALTER COLUMN SET MAX_PARTITIONS is below the current count.
    VectorPartitionLimitExceeded {
        index: VectorIndexRef,
        max_partitions: u32,
    },
    /// A search asked for `INDEXED`, with `USE VECTOR INDEXED` or through the column's
    /// `SEARCH_MODE INDEXED`, and a partition it selects is behind its writes. Such a search is
    /// never answered with an exact scan under the indexed name; `EXACT` and `AUTO` answer,
    /// and a maintenance cycle brings the index up to its writes.
    VectorIndexedRouteUnavailable { index: VectorIndexRef },
    /// A search asked for `INDEXED` with a filter on rows that no index or key serves for some
    /// partition it selects. Only the schema or the statement cures it.
    VectorIndexedFilterUnindexed { index: VectorIndexRef },
    /// INDEXED cannot screen partitions containing rows hidden from this reader.
    VectorIndexedNarrowed { index: VectorIndexRef },
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
    /// `USE RANK` named a sort key the searched column declares no rank policy for.
    RankPolicyNotFound {
        index: VectorIndexRef,
        sort_key: String,
    },
    /// A `RANK_POLICY` whose declaration cannot stand; `reason` says why.
    RankPolicyInvalid {
        index: VectorIndexRef,
        reason: String,
    },
    /// `USE RANK` was written in a SELECT that does not order by `<=>`.
    UseRankRequiresVectorOrder,
    /// `USE RANK` was written in a SELECT with no `LIMIT`.
    UseRankRequiresLimit,
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
    /// A graph pattern's upper bound is above the deepest walk the engine makes.
    BfsDepthExceeded { depth: u32, max: u32 },
    /// A variable-length graph pattern was written with no upper bound.
    UnboundedTraversal,
    /// `WITH RECURSIVE`.
    RecursiveCteNotSupported,
    /// A window function other than `ROW_NUMBER() OVER ([PARTITION BY ...] ORDER BY ...)`.
    WindowFunctionNotSupported { form: String },
    /// `EXISTS`, or a subquery that reads a row of the statement around it.
    SubqueryNotSupported,
    /// `CREATE PROCEDURE` or `CREATE FUNCTION`.
    StoredProcNotSupported,
    /// `column MATCH pattern`, a full-text search.
    FullTextSearchNotSupported,
    /// A statement wrote a `TXID` value above its own transaction's number.
    TxIdOutOfRange { value: u64, max: u64 },
}

fn column_name(table: &str, column: &str) -> String {
    let dot = if table.is_empty() { "" } else { "." };
    format!("{table}{dot}{column}")
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::MissedTicksExceeded { ticks, policy } => write!(
                f,
                "{ticks} missed ticks exceeded schedule policy '{policy}'"
            ),
            Error::ParseError(detail) => write!(f, "parse error: {detail}"),
            Error::TriggerNotDeclared { trigger_name } => {
                write!(f, "trigger not declared: {trigger_name}; declare it with CREATE TRIGGER")
            }
            Error::SinkNotDeclared { sink_name } => write!(f, "sink not declared: {sink_name}; declare it with CREATE SINK"),
            Error::SinkPrincipalMismatch => write!(
                f,
                "principal-scoped handles cannot register sinks for another principal"
            ),
            Error::TriggerAlreadyRegistered { trigger_name } => {
                write!(f, "trigger callback already registered: {trigger_name}")
            }
            Error::TriggerCallbackMissing { trigger_name } => {
                write!(f, "trigger callback missing: {trigger_name}; register it with register_transactional_trigger_callback")
            }
            Error::EngineNotInitialized { operation } => write!(f, "engine not initialized: {operation}"),
            Error::TriggerCascadeDepthExceeded {
                trigger_name,
                depth,
            } => write!(
                f,
                "trigger cascade depth exceeded for {trigger_name}: depth {depth}"
            ),
            Error::TriggerCallbackFailed {
                trigger_name,
                reason,
            } => write!(f, "trigger callback failed for {trigger_name}: {reason}"),
            Error::CallbackActive { operation } => write!(
                f,
                "callback handle cannot perform {operation} outside its active transaction thread"
            ),
            Error::CallbackReentry { operation } => write!(f, "refused in a callback: {operation}"),
            Error::TriggerEventUnsupported => {
                write!(f, "triggers support INSERT and UPDATE, not DELETE")
            }
            Error::PlanError(detail) => write!(f, "plan error: {detail}"),
            Error::TableNotFound(table) => write!(f, "table not found: {table}"),
            Error::ColumnNotFound { table, column } => {
                write!(f, "column not found: {}", column_name(table, column))
            }
            Error::ColumnTypeMismatch {
                table,
                column,
                expected,
                actual,
            } => write!(
                f,
                "column type mismatch: {} expected {expected}, got {actual}",
                column_name(table, column)
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
                write!(f, "duplicate index: {table}.{index}; drop it first or choose another name")
            }
            Error::ReservedIndexName {
                table,
                name,
                prefix,
            } => write!(
                f,
                "reserved index name: {table}.{name} uses reserved prefix `{prefix}`"
            ),
            Error::MixedSchemaAndRowWrite => f.write_str(
                "a transaction either declares tables or writes rows: commit this one and \
                 run the statement in the next",
            ),
            Error::ConditionalUpdateConflict { count } => write!(
                f,
                "conditional update conflict: {count} staged update(s) no longer matched at commit; read the rows again and retry"
            ),
            Error::CursorClosed => f.write_str("the cursor is closed; open a new one"),
            Error::CursorInvalidStatement => {
                f.write_str("only a SELECT reads through a cursor; run this statement with execute")
            }
            Error::SnapshotMismatch => f.write_str("the snapshot belongs to another store; take one from this store"),
            Error::ReadLimitExceeded { field, limit } => {
                write!(f, "read limit {field} exceeded ({limit}); raise ReadLimits::{field}")
            }
            Error::QueryCanceled { statement_timeout } => f.write_str(if *statement_timeout {
                "57014: canceling statement due to statement timeout (statement_timeout); raise ReadLimits::active_ms"
            } else {
                "57014: canceling statement due to user request"
            }),
            Error::OwnerNotRunning => f.write_str("owner_not_running: no writer is listening; start the writer and ask again"),
            Error::OwnerNotServing => f.write_str("owner_not_serving: the writer is closing; ask again when it is back"),
            Error::OwnerRefused { reason } => write!(f, "owner_refused: {reason}"),
            Error::OwnerLimitExceeded => {
                f.write_str("owner_limit_exceeded: request or answer is too large; ask for less or raise ReadLimits::result_bytes")
            }
            Error::OwnerDisconnected => f.write_str("owner_disconnected: the connection ended; ask again"),
            Error::DatabaseClosed => f.write_str("database handle is closed; open the store again"),
            Error::AuthoritativeErasure => {
                f.write_str("authoritative erasure invalidated this read; run it again")
            }
            Error::PurgeRequiresStandaloneExecution => {
                f.write_str("PURGE must run as a standalone statement: commit or roll back the transaction and run it alone")
            }
            Error::ReadOnlySession => f.write_str(
                "this statement writes and this session only reads: run it on a store opened for \
                 writing",
            ),
            Error::StoreMissing { path } => write!(
                f,
                "there is no store at this path: {}; opening it for writing creates it",
                path.display()
            ),
            Error::DatabaseLocked { path } => write!(
                f,
                "a writer holds the store at {}; open a read session to read",
                path.display()
            ),
            Error::HeldByReaders { path } => write!(
                f,
                "readers hold the store at {}; retry in a moment",
                path.display()
            ),
            Error::OpenCancelled { path } => {
                write!(f, "open cancelled while a writer holds {}", path.display())
            }
            Error::StoreCorrupted { path, reason } => {
write!(f, "corrupt store at {}: {reason}; preserve it before any reset", path.display())
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
            Error::DiskLimitExceeded {
                operation,
                current_bytes,
                budget_limit_bytes,
                hint,
            } => write!(
                f,
                "disk limit reached during {operation}: the file holds {current_bytes} bytes under the {budget_limit_bytes}-byte limit. {hint}"
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
                 its writes; run a maintenance cycle, or use EXACT or AUTO"
            ),
            Error::VectorIndexedFilterUnindexed { index } => write!(
                f,
                "{index} cannot be searched INDEXED with this filter: no index or key gives the \
                 rows the filter passes; declare an index that serves the filter, or use EXACT \
                 or AUTO"
            ),
            Error::VectorIndexedNarrowed { index } => write!(
                f,
                "{index} cannot be searched INDEXED by this narrowed reader; use EXACT or AUTO"
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
            Error::IndexNotFound { table, index } => {
                write!(f, "index not found: {}", column_name(table, index))
            }
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
            } => write!(f, "{table}.{column} is read by the rank policy {policy}; the policy is \
                            dropped with the column or the table that declares it"),
            Error::RankPolicyNotFound { index, sort_key } => {
                write!(f, "no rank policy named {sort_key} is declared on {index}")
            }
            Error::RankPolicyInvalid { index, reason } => {
                write!(f, "invalid rank policy on {index}: {reason}")
            }
            Error::UseRankRequiresVectorOrder => {
                f.write_str("USE RANK requires ORDER BY embedding <=> $param in the same query")
            }
            Error::UseRankRequiresLimit => f.write_str("USE RANK requires LIMIT"),
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
            Error::BfsDepthExceeded { depth, max } => write!(
                f,
                "graph traversal depth {depth} is above the limit of {max}; ask for at most {max} hops"
            ),
            Error::UnboundedTraversal => {
                f.write_str("a variable-length pattern needs an upper bound, as in ->{1,3}")
            }
            Error::RecursiveCteNotSupported => f.write_str("WITH RECURSIVE is not supported"),
            Error::WindowFunctionNotSupported { form } => write!(
                f,
                "window function not supported: {form}; the supported form is ROW_NUMBER() OVER \
                 ([PARTITION BY ...] ORDER BY ...)"
            ),
            Error::SubqueryNotSupported => f.write_str(
                "this subquery is not supported: a subquery stands inside IN, or alone as one \
                 value, and reads no row of the statement around it",
            ),
            Error::StoredProcNotSupported => {
                f.write_str("stored procedures and functions are not supported")
            }
            Error::FullTextSearchNotSupported => f.write_str(
                "full-text search (WHERE column MATCH pattern) is not supported; LIKE matches a \
                 pattern",
            ),
            Error::TxIdOutOfRange { value, max } => write!(
                f,
                "TXID {value} is above this transaction's own number, {max}"
            ),
        }
    }
}

impl std::error::Error for Error {}
