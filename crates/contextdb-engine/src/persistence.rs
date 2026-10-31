#![allow(clippy::field_reassign_with_default)] // Existing compatibility and test seam shape.
#![allow(clippy::drop_non_drop)] // Existing compatibility and test seam shape.
#![allow(clippy::match_like_matches_macro)] // Existing compatibility and test seam shape.
#![allow(clippy::too_many_arguments)] // Existing compatibility and test seam shape.
#![allow(clippy::type_complexity)] // Existing compatibility and test seam shape.

pub(crate) mod read_coordination;
mod reader_registration;
pub(crate) use reader_registration::*;
#[cfg(any(test, feature = "test-seams"))]
macro_rules! observed_graph_key {
    ("commit_index", "get", $key:expr, $role:expr) => {{
        let key: u64 = $key;
        crate::read_work_probe::note_graph(
            "commit_index",
            "get",
            &key.to_be_bytes(),
            Some(Lsn(key)),
            Some($role),
        );
        key
    }};
    ($family:expr, $operation:expr, $key:expr, $boundary:expr) => {
        observed_graph_storage_key($family, $operation, $key, $boundary)?
    };
    ($family:expr, "get", $key:expr) => {
        crate::read_work_probe::graph_storage_key($family, "get", $key)
    };
}
#[cfg(not(any(test, feature = "test-seams")))]
macro_rules! observed_graph_key {
    ($family:expr, $operation:expr, $key:expr, $boundary:expr) => {
        $key
    };
    ($family:expr, $operation:expr, $key:expr) => {
        $key
    };
}
pub(crate) mod content_identity;
pub(crate) mod content_size;
mod erasure_frontiers;
mod graph_history_read;
mod historical_index_commitment;
#[cfg(test)]
pub(crate) mod recorder_buffer;
use contextdb_core::read_memory::ReadMemoryBudget as _;
pub use historical_index_commitment::HistoricalIndexVerification;
mod graph_history_record;
mod graph_history_writer;
pub(crate) mod graph_read;
mod graph_reclaim_decode;
pub mod holder_presence;
#[cfg(unix)]
pub(crate) mod physical_admission;
pub(crate) mod purge_publication;
pub(crate) mod read_commit_directory;
#[cfg(target_os = "linux")]
mod reader_presence;
mod received_row_graph;
mod relational_posting_prune;
mod relational_version_manifest;
#[cfg(any(test, feature = "test-seams"))]
mod resident_history_test_seams;
pub(crate) mod row_graph;
mod schema_binding;
mod transaction_repair;
pub(crate) mod vector_current_identity;
pub(crate) mod vector_identity_range;
pub(crate) mod vector_ivf;
mod vector_membership_pages;
pub(crate) mod vector_membership_source;
pub(crate) mod vector_paged;
#[cfg(unix)]
mod wait_diagnostic;
#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod writer_wait_helper;
pub(crate) use schema_binding::VectorSchemaBinding;
mod cache_reserve;
mod custody_groups;
pub(crate) mod history_root_witness;
mod source_shared;
use source_shared::SourceShared;
mod authored_history;
pub(crate) mod blob_selector;
pub(crate) mod discard_history;
// Staged native storage admission is inactive until its integration prerequisites land.
pub(crate) mod native_storage_admission;
#[cfg(test)]
mod precommit_callback_atomicity_tests;
pub(crate) mod received_working;
pub(crate) mod received_working_graph;
pub(crate) mod received_working_source;
pub(crate) mod received_working_vector;
mod schema_projection;
pub(crate) mod writer_adapters;
pub(crate) mod writer_cleanup_progress;
mod writer_commit_directory;
mod writer_ddl_occurrences;
mod writer_ddl_summary;
mod writer_export;
mod writer_file;
mod writer_history;
mod writer_recovery;
mod writer_row_heads;
pub(crate) mod writer_source;
use writer_file::{LockedWriterFile, OpenedWriterDatabase};
pub(crate) use writer_recovery::WriterRecoveryBounds;
pub(crate) use writer_source::{WriterSnapshot, WriterSnapshotHandle, WriterSnapshotWeak};

use crate::blob_repository::{
    BlobAuthoritativePurgeProjection, apply_authoritative_purge_in_write,
};
use crate::composite_store::{
    ChangeLogEntry, PreparedVectorPartitionBinding, PreparedVectorPartitionMutation,
    PreparedVectorPartitionMutationBatch, sync_source_lsn_updates, vector_partition_key_for_row,
};
use crate::database::TriggerAuditEntry;
use crate::database::event_bus::{EventBusPersistenceCommit, PreparedSinkEvent, SinkQueueEntry};
use crate::database::trigger::TriggerPersistenceCommit;
use crate::direct_file_reader::DirectTypedImageDigest;
use crate::local_transport::{RuntimeDirectory, derive_channel_address};
use crate::sync_types::DdlChange;
#[cfg(target_os = "linux")]
use contextdb_core::read_contract::ReaderProcessIdentity;
use contextdb_core::read_contract::{
    ChannelAddress, DatabaseIdentity, HeldByReadersDetail, HeldByWriterDetail, LocalUserIdentity,
    OwnerReadCancellation, OwnerReadStatus, OwnerServingReason, OwnerServingState, ReadFailure,
    ReadFailureDetail, ReadFailureKind, ReaderBreadcrumb, WriterRunNumber,
};
use contextdb_core::{
    AdjEntry, ColumnType, DirectedValue, Error, ForeignKeyReference, IndexDecl, IndexKey,
    IndexKind, Lsn, NodeId, PropagationRule, RankPolicy, Result, RowId, SnapshotId,
    StateMachineConstraint, TableMeta, TotalOrdAsc, TotalOrdDesc, TxId, Value, VectorEntry,
    VectorIndexRef, VectorPartitionKey, VectorQuantization, VersionedRow,
    Wallclock,
};
use contextdb_relational::store::SyncSourceKind;
use contextdb_tx::WriteSet;
// Custody-only checked key reads.
mod custody_pending;
mod custody_records;
pub(crate) mod custody_source;
use contextdb_vector::store::{RawVectorDirectoryEntry, VectorPartitionRef};
use redb::{
    Key as RedbKey, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
    TableHandle, Value as RedbValue,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

thread_local! {
    /// How many hook-suppressed opens this thread is inside.
    static PANIC_REPORTS_SUPPRESSED: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// The panic hook is wrapped once per process: a panic on a thread that is
/// inside a hook-suppressed open is not reported, and every other panic goes
/// to the hook that was installed before. Nothing is swapped per open, so no
/// open waits for another.
static PANIC_HOOK_WRAPPED: std::sync::Once = std::sync::Once::new();
static READER_BREADCRUMB_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static READER_SNAPSHOT_SEQUENCE: AtomicU64 = AtomicU64::new(0);
#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    /// Application pages the fragmentation statistics walk visited on this
    /// thread. Production-dead: the maintenance growth proofs read it to show a
    /// cycle's statistics work does not grow with the store.
    static STORAGE_STATISTICS_PAGES_WALKED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// The subset of those pages walked while this thread held the database's
    /// pruning guard, which the import's vector-tail persistence also waits on.
    static STORAGE_STATISTICS_PAGES_WALKED_UNDER_PRUNING_GUARD: std::cell::Cell<u64> =
        const { std::cell::Cell::new(0) };
    static PRUNING_GUARD_HELD_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Storage statistics snapshots captured on this thread; each capture
    /// already counts the system tables and the allocator's free pages.
    static STORAGE_STATISTICS_CAPTURES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Production-dead marker: this thread holds the pruning guard while it lives.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub(crate) struct PruningGuardHeldForTest(());

thread_local! {
    // The provider runs synchronously during physical owner cursor admission.
    // That caller holds the commit mutex and may never wait on an OS registry lock.
    static OWNER_CURSOR_COORDINATION_NONBLOCKING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

struct OwnerCursorCoordinationScope(bool);

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static VECTOR_MEMBERSHIP_RECORDS_DECODED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static RELATIONAL_INDEX_REBUILD_ROWS_DECODED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
thread_local! {
    static PERSISTENCE_OWNERSHIP_READS_FOR_TEST: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

const COMPANION_RECORD_MAGIC: &[u8] = b"contextdb-companion";
const COMPANION_FILE_MAGIC: &[u8; 16] = b"CTXDB-COMPANION1";
const COMPANION_SLOT_PAYLOAD_CAPACITY: usize = 4 * 1024;
const COMPANION_SLOT_BYTES: usize = 4 + COMPANION_SLOT_PAYLOAD_CAPACITY + 32;
const COMPANION_FIRST_SLOT_OFFSET: usize = COMPANION_FILE_MAGIC.len();
const COMPANION_SELECTOR_OFFSET: usize = COMPANION_FIRST_SLOT_OFFSET + 2 * COMPANION_SLOT_BYTES;
const COMPANION_SELECTOR_MAGIC: &[u8; 8] = b"ACTIVEV1";
const COMPANION_SELECTOR_BODY_BYTES: usize = 24;
const COMPANION_SELECTOR_BYTES: usize = COMPANION_SELECTOR_BODY_BYTES + 32;
const COMPANION_FILE_BYTES: usize = COMPANION_SELECTOR_OFFSET + COMPANION_SELECTOR_BYTES;
const MAX_COMPANION_STARTUP_REASON_BYTES: usize = 256;
const COMPANION_PENDING_MAGIC: &[u8] = b"contextdb-replacement-v1";
const READER_SNAPSHOT_MAGIC: &[u8] = b"contextdb-reader-snapshot";
const READER_SNAPSHOT_VERSION: u16 = 1;
const RELATIONAL_INDEX_READY_VERSION: &[u8] = b"v3";
const RELATIONAL_INDEX_COUNT_IDENTITY_NAMESPACE: &[u8] = b"rel-index-count-v2";

/// One authenticated index-prefix count this write had to maintain.
///
/// A count is keyed by `(table, column list, prefix values)` — not by index —
/// so two indexes that share leading columns share one count and the engine
/// maintains it once. `uniquely_constrained` says the table declares a primary
/// key or a unique constraint over exactly this column list, which bounds the
/// count at 0 or 1 whatever the data is. It is computed from the declared
/// constraints, not from the index kind: the engine also derives indexes to
/// back foreign keys and graph edges, and those are not unique.
#[cfg(any(test, feature = "test-seams"))]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct IndexPrefixCountTouch {
    pub table: String,
    /// The columns this count keys on, in index order.
    pub prefix_columns: Vec<String>,
    pub uniquely_constrained: bool,
    /// Every index that maintains this one count, as
    /// `(index name, index width, how many of its columns this count keys on)`.
    pub maintained_by: Vec<(String, usize, usize)>,
    /// What the count became once the write settled: `Some(n)` for a stored
    /// count, `None` for one that reached zero and was removed.
    pub count_after_the_write: Option<u64>,
}

/// One ordered index posting a write maintained: which index of which table it
/// belongs to, and over which columns.
///
/// A posting is what `visit_relational_index_postings` walks to serve an
/// authenticated ordered range, so unlike a prefix count it is read by every
/// indexed range query. This names the index that pays for it.
#[cfg(any(test, feature = "test-seams"))]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct IndexPostingTouch {
    pub table: String,
    pub index: String,
    pub columns: Vec<String>,
    /// How many postings this index maintained in the observed window.
    pub postings: u64,
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static INDEX_POSTINGS: std::cell::RefCell<BTreeMap<(String, String), (Vec<String>, u64)>> =
        const { std::cell::RefCell::new(BTreeMap::new()) };
}

#[cfg(any(test, feature = "test-seams"))]
type IndexPrefixCountAttribution = (String, Vec<String>, bool, String, usize, usize);

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    /// Every visit, keyed by the encoded count key, so visits that land on one
    /// count collapse the way the write itself collapses them.
    static INDEX_PREFIX_COUNT_TOUCHES: std::cell::RefCell<
        BTreeMap<Vec<u8>, BTreeSet<IndexPrefixCountAttribution>>,
    > = const { std::cell::RefCell::new(BTreeMap::new()) };
    static INDEX_PREFIX_COUNT_VALUES: std::cell::RefCell<BTreeMap<Vec<u8>, Option<u64>>> =
        const { std::cell::RefCell::new(BTreeMap::new()) };
}

/// Where the wall time of one durable write went, stage by stage.
///
/// The three engine stages partition `flush_data_with_logs_and_sink_events`
/// from `begin_write` to the end of `commit()`: the rows, index postings and
/// logs the write set puts into the storage engine; the content-identity proof
/// maintenance over those same writes; and the storage commit itself. The
/// fourth is the write-set preparation that runs before the transaction opens,
/// which is where the declared table metadata is copied. Nanoseconds,
/// accumulated on the writing thread; nothing in production reads them.
#[cfg(any(test, feature = "test-seams"))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AppendStageNanos {
    pub write_set_preparation: u64,
    pub table_meta_clone: u64,
    pub rows_indexes_and_logs: u64,
    pub content_identity: u64,
    pub commit: u64,
}

#[cfg(any(test, feature = "test-seams"))]
pub(crate) enum AppendStage {
    WriteSetPreparation,
    TableMetaClone,
    RowsIndexesAndLogs,
    ContentIdentity,
    Commit,
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static APPEND_STAGE_NANOS: std::cell::Cell<AppendStageNanos> =
        const { std::cell::Cell::new(AppendStageNanos {
            write_set_preparation: 0,
            table_meta_clone: 0,
            rows_indexes_and_logs: 0,
            content_identity: 0,
            commit: 0,
        }) };
}
const RELATIONAL_INDEX_COUNT_READY_IDENTITY_NAMESPACE: &[u8] = b"rel-index-count-ready-v1";
const MAX_READER_SNAPSHOT_BYTES: u64 = 1024;

/// One durable posting key. Redb owns the B-tree and this wrapper gives it
/// exactly the relational index order, including SQL NULL and float ordering.
#[derive(Debug, Clone)]
struct DurableRelationalIndexKey(Vec<u8>);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RelationalIndexPosition {
    key: IndexKey,
    row_id: RowId,
    created_tx: TxId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EncodedRelationalIndexPosition {
    key: Vec<(bool, Value)>,
    row_id: RowId,
    created_tx: TxId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EncodedRelationalIndexCountKey {
    table: String,
    columns: Vec<(String, contextdb_core::SortDirection)>,
    prefix: Vec<(bool, Value)>,
}

/// Borrow the existing index-position encoding so callers can admit an owned
/// key before its copy. Physical type identity and ordering are unchanged.
#[derive(Debug)]
struct BorrowedRelationalIndexKey;

/// A cursor's native storage pin overlaps publication of its external hold.
pub(crate) struct OwnerCursorStoragePin {
    read: Arc<redb::ReadTransaction>,
    _registration: ReadSnapshotRegistrationGuard,
}

/// A failed nonblocking pin attempt retains the typed refusal and whether
/// admission can safely retry after releasing the engine commit boundary.
pub(crate) enum OwnerCursorPinAdmissionError {
    #[allow(
        dead_code,
        reason = "KEEP-GUARD: held for its Drop; releases what it holds when the owner drops"
    )]
    Busy(Error),
    Refused(Error),
}

/// One authenticated external reader root. The engine owns its identity and
/// lifetime; redb owns page reachability and deferred reclamation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RegisteredReaderSnapshot {
    pub(crate) physical_identity: (u64, u64),
    pub(crate) owner_user: LocalUserIdentity,
    pub(crate) database_identity: Option<DatabaseIdentity>,
    pub(crate) storage_snapshot: redb::StorageSnapshotIdentity,
    pub(crate) handle_identity: [u8; 16],
    pub(crate) process: ReaderBreadcrumb,
    pub(crate) descriptor_number: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReaderSnapshotRegistry {
    Complete(Vec<RegisteredReaderSnapshot>),
    Contended(PathBuf),
    Indeterminate(PathBuf),
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CompanionRecord {
    format_version: u16,
    generation: u64,
    database_identity: DatabaseIdentity,
    writer_run_number: WriterRunNumber,
    owner_user: LocalUserIdentity,
    channel_address: ChannelAddress,
    process_id: u32,
    owner_read_status: OwnerReadStatus,
    /// Which boot, and which physical data file, this writer run opened --
    /// published only once its open (and any repair) finished. A direct
    /// reader of a store whose writer died reads it without a repair only on
    /// that same boot and that same file.
    writer_boot: Option<WriterBootBinding>,
}

/// The boot a writer run opened its store on, and the exact data file it
/// opened: device, inode and the inode's birth time, so neither a replaced
/// file nor a reused inode number is mistaken for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WriterBootBinding {
    boot_id: [u8; 16],
    data_dev: u64,
    data_ino: u64,
    data_birth_nanos: u128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EncodedCompanionRecord {
    fields: CompanionRecord,
    checksummed_payload: Vec<u8>,
    stored_checksum: [u8; 32],
}

struct CompanionPayloadCursor<'a> {
    payload: &'a [u8],
    offset: usize,
}

/// What the writer holding this store recorded about serving inspection.
///
/// Two facts have to agree before a reader may believe a record. The
/// record itself is durable and is never unlinked, so the last writer's
/// state outlives that writer: on its own it could describe a process that is
/// gone. The advisory companion lock is what says the recorded owner is STILL
/// there, and probing it touches neither the store file nor a single byte of
/// its contents.
pub(crate) struct ObservedWriterState {
    pub(crate) identity: DatabaseIdentity,
    pub(crate) run: WriterRunNumber,
    pub(crate) held: bool,
    pub(crate) status: OwnerReadStatus,
}

/// The two coordination facts a store's companion carries as advisory
/// byte-range locks rather than as bytes.
///
/// Both live far past the end of the companion's record area, at offsets no
/// byte of the record will ever occupy, and neither is ever read or written:
/// taking one costs a lock call and nothing else, the companion's own content
/// is untouched, and the kernel drops the hold the moment the holder's process
/// dies. Linux ownership also takes an independent whole-file lock. macOS
/// ownership takes its own non-overlapping byte because whole-file locks
/// overlap byte-range holds there. All holds live on the companion, so nothing
/// here opens, reads, locks, or creates anything in the store folder.
///
/// `CLAIM_WINDOW` is held by a writer from the moment it claims the store
/// until it publishes its serving decision, so a caller arriving inside that
/// window has something to sleep on instead of a verdict formed before the
/// writer decided anything.
///
/// `READER_HOLD` is held, shared, by every direct reader for exactly as long
/// as it holds the committed image open, so a caller waiting to become the
/// writer sleeps on the readers themselves rather than on what happens to be
/// written down about them.
const COMPANION_CLAIM_WINDOW_LOCK_OFFSET: u64 = 1 << 40;
const COMPANION_READER_HOLD_LOCK_OFFSET: u64 = (1 << 40) + 1;
/// Held exclusive by a writer for exactly as long as it holds the store's
/// companion claim. Readers only ever ASK about it, so asking whether a writer
/// is live can never make a writer's own claim fail.
const COMPANION_OWNER_LOCK_OFFSET: u64 = (1 << 40) + 2;

/// Whether one advisory byte-range lock was taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeLockOutcome {
    Taken,
    HeldByAnother,
}

/// The two ways a caller can ask for one of these bytes: shared, which several
/// holders may have at once, and exclusive, which nobody else may hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeLockMode {
    Shared,
    Exclusive,
}

/// A pipe whose readable end a wait can sleep on and whose write end anybody
/// may poke to wake it.
///
/// A coordination wait sleeps on the kernel telling it something happened. A
/// caller cancelling, and a deadline arriving, are two more things that happen,
/// and they reach a sleeping wait the same way -- through a descriptor it is
/// already watching -- rather than by interrupting it. That is what lets a
/// wait be ended by its caller without a signal, a timer, or a thread left
/// behind holding the descriptor open.
#[cfg(unix)]
struct WakePipe {
    reader: std::os::fd::OwnedFd,
    writer: std::os::fd::OwnedFd,
}

/// A waker that pokes a wake pipe, so a future's completion reaches a wait
/// that is asleep on descriptors rather than on a condvar.
#[cfg(unix)]
struct PipeWaker(Arc<WakePipe>);

/// A coordination wait stays on its caller thread and rechecks the trusted
/// hold after waking. Linux watches companion notifications and the wake pipe.
/// macOS watches the wake pipe with a short lock-recheck interval so cancellation
/// can end the wait while the hold remains live. Other Unix targets keep the
/// blocking fallback, which notices cancellation at the next release.
#[cfg(unix)]
struct CoordinationWatch {
    wake: Arc<WakePipe>,
    #[cfg(target_os = "linux")]
    notifications: std::os::fd::OwnedFd,
    _listener: Option<contextdb_core::read_contract::CancellationListener>,
}

#[cfg(unix)]
enum CoordinationWatchFailure {
    Io(std::io::Error),
    Admission(contextdb_core::Error),
}

/// A live writer's claim on this store that has said nothing about serving
/// yet.
///
/// The trusted companion this was observed through travels with it, so the
/// wait that follows sleeps on the SAME open file description the observation
/// was made on: nothing can substitute the file underneath a caller between
/// seeing the claim and waiting for it.
pub(crate) struct UnsettledClaim {
    companion: File,
    /// The pathname the descriptor above was opened from, so the wait can
    /// watch the same file it is asking about.
    pathname: PathBuf,
}

/// How a wait inside a writer's claim window ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClaimSettlement {
    /// The holder published its decision. Ask the store again; its own answer
    /// is there now.
    Settled,
    /// The caller's own declared budget ran out with the claim still held, or
    /// the window itself could not be observed here. Either way a writer is
    /// holding this store, so the caller is owed a not-serving answer and
    /// never an absent one.
    StillHeld,
    Cancelled,
}

/// How many times one reader will re-observe a companion that changed hands
/// underneath it before giving no verdict at all. A handover is a moment, not
/// a state, so a couple of attempts settle it; a reader that still cannot see
/// one writer says nothing rather than guessing which one it saw.
const RECORDED_OWNER_OBSERVATION_ATTEMPTS: usize = 3;

/// A direct reader's proof that it is holding this store's committed image.
///
/// The hold is a shared advisory byte-range lock on the companion beside the
/// store -- taken before the committed file is opened and let go only after
/// the decoded image no longer needs it -- so it is exactly as long-lived as
/// the reader's grip on the store and not one instant longer. It is the thing
/// a would-be writer waits on, and unlike a breadcrumb it cannot fail to be
/// published and then leave a real reader invisible: the kernel takes it away
/// when this process dies, whatever else does or does not get cleaned up.
///
/// The store folder is untouched. This is the companion, beside the store.
struct ReaderStoreHold {
    file: File,
}

/// The committed image reconstructed by the side-effect-free persistence
/// loader. The direct backend will consume this type instead of reopening the
/// writable database path.
/// How many entries one sink's durable queue holds and the newest entry's
/// id: what a read image needs to report and continue the queue without
/// copying its payloads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SinkQueueExtent {
    pub(crate) depth: u64,
    pub(crate) newest_id: u64,
}

/// One bounded read of a durable sink queue (`load_sink_queue_window`):
/// entries with their ids and the decoded bytes each holds on `owner`.
pub(crate) struct SinkQueueWindowRead<T> {
    pub(crate) depth: u64,
    pub(crate) newest_id: u64,
    pub(crate) entries: Vec<T>,
    pub(crate) credits: Vec<usize>,
    pub(crate) owner: Option<crate::read_image_memory::ImageMemoryHandle>,
    pub(crate) exhausted: bool,
    pub(crate) refusal: Option<Error>,
}

/// What a page-on-demand open keeps of history instead of the history
/// itself: the authenticated maxima the open needs. Graph versions remain
/// in the pinned root and are validated by the querying operation.
#[derive(Debug, Clone, Default)]
pub(crate) struct ReadHistoryOnRequest {
    open_control: Option<content_identity::ValidatedHistoryOpenControl>,
    pub(crate) commit_index_last: Option<(Lsn, TxId)>,
    pub(crate) commit_index_first: Option<(Lsn, TxId)>,
    pub(crate) commit_index_count: usize,
    pub(crate) ddl_last_lsn: Option<Lsn>,
}

/// History read from the retained snapshot on request, with the charge that
/// admitted it. The charge is held for as long as the history is.
pub(crate) struct LoadedReadHistory {
    pub(crate) change_log: Vec<ChangeLogEntry>,
    pub(crate) ddl_log: Vec<(Lsn, DdlChange)>,
    pub(crate) charge: ReadHistoryCharge,
}

/// What a loaded history holds against the operation's memory limit: the slots
/// its collections keep, and every allocation its records were decoded into,
/// admitted as each was made. Both are given back when this is dropped.
pub(crate) struct ReadHistoryCharge {
    _slots: contextdb_core::read_memory::ReadCredit,
    _decoded: crate::read_image_memory::ImageMemoryHandle,
}

/// An operation-funded proof pool grows only as the authenticated walk needs
/// room. A refused growth keeps its original typed diagnosis.
struct ReadHistoryProofOwner {
    credit: parking_lot::Mutex<contextdb_core::read_memory::ReadCredit>,
    refusal: parking_lot::Mutex<Option<Error>>,
}

/// The most one history walk may hold to prove its committed range: the
/// root-to-leaf path it is standing on plus the chunks it keeps for reuse.
const READ_HISTORY_PROOF_POOL_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct ReadPersistenceImage {
    open_context: crate::executor::bounded::ReadOpenContext,
    content_identity: DirectTypedImageDigest,
    table_meta: HashMap<String, TableMeta>,
    relational_tables: HashMap<String, Vec<VersionedRow>>,
    lazy_relational: bool,
    read_bounds: WriterRecoveryBounds,
    vector_entries: Vec<VectorEntry>,
    /// Checksummed placement and visibility identities for lazy raw-vector
    /// point loads. Empty on the complete-hydration fallback.
    raw_vector_memberships: Vec<VectorPartitionMembershipRecord>,
    current_vectors: HashMap<(VectorIndexRef, RowId), Vec<f32>>,
    sync_source_lsns: HashMap<(String, RowId), Lsn>,
    sync_source_kinds: HashMap<(String, RowId), SyncSourceKind>,
    change_log: Vec<ChangeLogEntry>,
    ddl_log: Vec<(Lsn, DdlChange)>,
    commit_index: BTreeMap<Lsn, TxId>,
    /// Present when history stays in the store: the change log, schema-change
    /// log and commit index above are then empty and are read on request.
    history_on_request: Option<ReadHistoryOnRequest>,
    config_values: Vec<(String, Vec<u8>)>,
    sink_audit: Vec<(String, Vec<u8>)>,
    trigger_audit: Vec<(String, Vec<u8>)>,
    trigger_audit_stamps: Vec<(String, u64)>,
    /// Queued payloads. Empty after an open: payloads stay in the store and
    /// only [`Self::sink_queue_extents`] is read.
    sink_queues: BTreeMap<String, Vec<(u64, Vec<u8>)>>,
    sink_queue_extents: BTreeMap<String, SinkQueueExtent>,
    /// Lightweight, checksummed pointers to maintained vector generations.
    /// Graph bodies and journal pages remain in the store until a query
    /// actually chooses the dormant indexed route.
    /// The released source a dormant generation loader may reopen read-only.
    /// Hydration never carries a Redb handle into the committed image.
    source_path: Option<PathBuf>,
    /// How large the store file was when this image was read out of it.
    ///
    /// It is the same number a writer reports as disk usage -- the length of
    /// the same file -- measured once, where the reader had the file open,
    /// and carried with the image it belongs to. Absent only if the file's
    /// length could not be read at all.
    store_file_bytes: Option<u64>,
    // Admission outlives every allocation carried by this image.
    pub(crate) memory: crate::read_image_memory::ImageMemoryHandle,
}

/// Hydrated state consumed by the direct backend after the hydration
/// transaction, load lock, and runtime breadcrumb have been released. The
/// separate handoff retains the exact read-only database/descriptor and
/// registered storage root.
pub(crate) struct ReadPersistenceImageParts {
    pub(crate) open_context: crate::executor::bounded::ReadOpenContext,
    pub(crate) content_identity: DirectTypedImageDigest,
    pub(crate) table_meta: HashMap<String, TableMeta>,
    pub(crate) relational_tables: HashMap<String, Vec<VersionedRow>>,
    pub(crate) lazy_relational: bool,
    pub(crate) read_bounds: WriterRecoveryBounds,
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from read_image_decode.rs"
        )
    )]
    pub(crate) vector_entries: Vec<VectorEntry>,
    #[allow(
        dead_code,
        reason = "LAYOUT-ONLY: never read; removing it changes a layout that memory accounting may charge, so it waits for the measured accounting rerun"
    )]
    pub(crate) raw_vector_memberships: Vec<VectorPartitionMembershipRecord>,
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from read_image_decode.rs"
        )
    )]
    pub(crate) current_vectors: HashMap<(VectorIndexRef, RowId), Vec<f32>>,
    pub(crate) sync_source_lsns: HashMap<(String, RowId), Lsn>,
    pub(crate) sync_source_kinds: HashMap<(String, RowId), SyncSourceKind>,
    pub(crate) change_log: Vec<ChangeLogEntry>,
    pub(crate) ddl_log: Vec<(Lsn, DdlChange)>,
    pub(crate) commit_index: BTreeMap<Lsn, TxId>,
    pub(crate) history_on_request: Option<ReadHistoryOnRequest>,
    pub(crate) config_values: Vec<(String, Vec<u8>)>,
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from reader_registration.rs"
        )
    )]
    pub(crate) sink_audit: Vec<(String, Vec<u8>)>,
    pub(crate) trigger_audit: Vec<(String, Vec<u8>)>,
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from reader_registration.rs"
        )
    )]
    pub(crate) trigger_audit_stamps: Vec<(String, u64)>,
    pub(crate) sink_queues: BTreeMap<String, Vec<(u64, Vec<u8>)>>,
    pub(crate) sink_queue_extents: BTreeMap<String, SinkQueueExtent>,
    pub(crate) retained_source: RetainedReadSource,
    #[allow(
        dead_code,
        reason = "LAYOUT-ONLY: never read; removing it changes a layout that memory accounting may charge, so it waits for the measured accounting rerun"
    )]
    pub(crate) source_path: PathBuf,
    pub(crate) store_file_bytes: Option<u64>,
    // Admission outlives every allocation carried by this image.
    pub(crate) memory: crate::read_image_memory::ImageMemoryHandle,
}

pub(crate) struct ReadPersistenceReleaseReceipt {
    breadcrumb_path: Option<PathBuf>,
    source_accesses: u64,
    _seal: ReadPersistenceReleaseSeal,
}

struct ReadPersistenceReleaseSeal;

pub(crate) struct ReadPersistenceLoad {
    image: ReadPersistenceImage,
    release_receipt: ReadPersistenceReleaseReceipt,
    source: RetainedReadSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ReadImageRequiresWriterCause {
    #[error("non-monotonic commit index")]
    NonMonotonicCommitIndex,
    #[error("invalid vector reference")]
    InvalidVectorReference,
    #[error("relational and vector values diverge")]
    RowVectorDivergence,
    #[error("writable hydration would sanitize or supplement state")]
    WritableSanitizerWouldChange,
    #[error("commit index is incomplete")]
    MissingCommitIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ReadImageCorruptionCause {
    #[error("malformed durable record")]
    MalformedRecord,
    #[error("unsupported store layout")]
    LegacyLayout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(test)]
pub(crate) enum LoadReadImageErrorKind {
    RequiresWriter(ReadImageRequiresWriterCause),
    StoreMissing,
    Contention,
    Corrupt(ReadImageCorruptionCause),
    InvalidState,
    Io,
    Memory,
    Release,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LoadReadImageError {
    #[error("{0}")]
    Memory(Error),
    #[error("read-image release proof failed: {0}")]
    Release(String),
    #[error("direct read requires a writer ({cause}): {reason}")]
    RequiresWriter {
        cause: ReadImageRequiresWriterCause,
        reason: String,
    },
    #[error("read-image store is not there: {reason}")]
    StoreMissing { reason: String },
    #[error("read-image source is contended: {reason}")]
    Contention { reason: String },
    #[error("reader snapshot registry is contended through {}: {reason}", path.display())]
    RegistryContention { path: PathBuf, reason: String },
    #[error("read-image durable state is corrupt ({cause}): {reason}")]
    Corrupt {
        cause: ReadImageCorruptionCause,
        reason: String,
    },
    #[error("read-image durable state is invalid: {reason}")]
    InvalidState { reason: String },
    #[cfg(unix)]
    #[error("read-image I/O failed during {operation}: {reason}")]
    WaitIo {
        operation: &'static str,
        reason: physical_admission::WaitIo,
    },
    #[error("read-image I/O failed during {operation}: {reason}")]
    Io {
        operation: &'static str,
        reason: String,
    },
    #[error("read-image I/O failed during {operation} through {}: {reason}", path.display())]
    IoAtPath {
        operation: &'static str,
        path: PathBuf,
        reason: String,
    },
}

const READER_BREADCRUMB_MAGIC: &[u8] = b"contextdb-reader";
const ANONYMOUS_READER_BREADCRUMB: &[u8] = b"contextdb-reader-anonymous-v1";
const MAX_READER_PROCESS_NAME_BYTES: usize = 255;
const MAX_READER_BREADCRUMB_BYTES: usize = READER_BREADCRUMB_MAGIC.len()
    + std::mem::size_of::<u32>()
    + std::mem::size_of::<u64>()
    + std::mem::size_of::<u32>()
    + MAX_READER_PROCESS_NAME_BYTES;

/// A lock on the stable platform or supplied root, which contextdb never
/// creates or removes. Snapshot joins/leaves share it for root lifetime and
/// exclude peers through the store's own registry lock. Empty-root removal,
/// diagnostic breadcrumbs and general inventory take it exclusively.
/// Lock order: root lifetime, then per-store registry, then cleanup gate tries.
/// Root removal releases the shared hold before trying an exclusive hold.
#[cfg(unix)]
struct RuntimeRootSerializer {
    _directory: File,
}

#[cfg(not(unix))]
struct RuntimeRootSerializer;

struct ReaderBreadcrumbGuard {
    file: Option<File>,
    path: PathBuf,
    metadata: std::fs::Metadata,
    removed: bool,
    /// The registration root, when this process created it: the platform
    /// directory's `contextdb` child. An operator-supplied root is never ours.
    created_root: Option<PathBuf>,
    /// The runtime root whose serializer guards removing `created_root`.
    serializer_root: Option<PathBuf>,
}

struct ReaderSnapshotFileGuard {
    file: Option<File>,
    path: PathBuf,
    #[allow(
        dead_code,
        reason = "LAYOUT-ONLY: never read; removing it changes a layout that memory accounting may charge, so it waits for the measured accounting rerun"
    )]
    metadata: std::fs::Metadata,
    registry_lock: ReaderSnapshotRegistryLock,
    try_only_teardown: bool,
    opening_transaction:
        Option<contextdb_core::read_memory::ReadShared<read_coordination::RegistrationTransaction>>,
    /// What the last departing reader needs to prove no purge-capable writer
    /// exists: the store it reopens for the shared data-file gate, and the
    /// physical identity that reopened descriptor must name.
    database_path: PathBuf,
    physical_identity: (u64, u64),
    /// The registration root, when this process created it: the platform
    /// directory's `contextdb` child, never an operator-supplied directory.
    created_root: Option<PathBuf>,
    /// The runtime root whose serializer every leave takes before the
    /// registry lock.
    serializer_root: PathBuf,
}

/// Where one registration publishes: the store, the runtime directory and
/// whether this process created that directory, and the fence it must name.
struct ReaderRegistrationTarget<'a> {
    database_path: &'a Path,
    runtime: &'a RuntimeDirectory,
    // Explicit channel directories remain operator-owned even when the
    // platform-default directory happens to resolve to the same pathname.
    preserve_root: bool,
    fence: Option<&'a Mutex<purge_publication::Fence>>,
}

/// Serializes one physical registry's publication, scan, and authenticated
/// teardown windows. It is unlocked for the entire reader and writer lifetime.
struct ReaderSnapshotRegistryLock {
    file: File,
    path: PathBuf,
    metadata: std::fs::Metadata,
    locked: bool,
}

/// Session lifetime for the exact descriptor and snapshot registered during
/// hydration. The descriptor remains open after its shared load lock is
/// released, so rename, unlink, and replacement cannot change what this
/// registration names.
pub(crate) struct ReadSnapshotRegistrationGuard {
    descriptor: Arc<File>,
    #[cfg(target_os = "linux")]
    presence: Option<Arc<reader_presence::ReaderPresenceGuard>>,
    snapshot: RegisteredReaderSnapshot,
    registration: Option<Arc<ReaderSnapshotFileGuard>>,
    purge_publication: Option<Arc<Mutex<purge_publication::Fence>>>,
    other_registrations: Vec<Arc<ReaderSnapshotFileGuard>>,
    other_purge_publications: Vec<Arc<Mutex<purge_publication::Fence>>>,
    load_lock_held: bool,
    /// The reader hold taken with the data lock at admission; given back
    /// right after the data lock, never before it.
    reader_hold_held: bool,
    /// Its exact record had begun to be written when admission failed.
    record_write_started: bool,
    opening_transaction:
        Option<contextdb_core::read_memory::ReadShared<read_coordination::RegistrationTransaction>>,
}

#[derive(Debug)]
struct ReadSnapshotFileView {
    file: File,
}

/// A store read for its format check by a caller that holds no claim on it.
/// Field order is drop order: the handle goes before the registration that
/// protects its root.
struct ClassificationCapture {
    database: redb::ReadOnlyDatabase,
    #[cfg(unix)]
    snapshot: redb::StorageSnapshotIdentity,
    #[cfg(unix)]
    _registration: ReadSnapshotRegistrationGuard,
    #[cfg(not(unix))]
    _descriptor: File,
}

/// Why a format check could not read the store: the store's own answer,
/// which is reported as an open of that store would report it, or a failure
/// to coordinate with other readers and writers, which says nothing about
/// the store's format or health.
enum ClassificationOpenError {
    Refused(redb::DatabaseError),
    Coordination(Error),
}

#[cfg(any(test, feature = "test-seams"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReaderSnapshotRegistrationObservation {
    pub(crate) snapshot: RegisteredReaderSnapshot,
    pub(crate) path: Option<PathBuf>,
    pub(crate) descriptor_open: bool,
    pub(crate) load_lock_held: bool,
}

pub(crate) struct LockedReaderBreadcrumb {
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) path: PathBuf,
    pub(crate) breadcrumb: Option<ReaderBreadcrumb>,
}

mod retained_read_source;
pub use retained_read_source::*;

#[derive(Clone, Default)]
struct WriterWaitContext {
    stop: Option<OwnerReadCancellation>,
    deadline: Option<crate::ReaderWaitDeadline>,
    payer: Option<crate::memory_accounting::StoreAccountantHandle>,
}

thread_local! {
    static WRITER_WAIT: std::cell::RefCell<Option<WriterWaitContext>> =
        const { std::cell::RefCell::new(None) };
}

/// The caller's bounds and original writer payer for one writable open.
/// Scope and TLS custody only clone strong capabilities, without allocating.
pub(crate) struct WriterWaitScope {
    previous: Option<WriterWaitContext>,
}

/// The transaction a failed reader's record names: past every commit, with no
/// root, so it retains nothing and refuses nothing.
pub(crate) const FAILED_READER_TRANSACTION: u64 = u64::MAX;

/// How a wait for this store's direct readers to let go of it ended.
#[derive(Debug)]
pub enum ReaderReleaseWait {
    /// Nobody is holding the store any more. A writer may take it.
    Released,
    /// The caller asked to stop waiting, and is answered at once.
    Stopped,
    /// Who is holding the store cannot be observed at all, so this wait can
    /// say nothing about it. The caller decides what to do about that rather
    /// than being told a reassuring answer nobody checked.
    Unobservable(std::io::Error),
}

/// What an open says when its attempt finds nothing at the path.
///
/// The two answers are for two different callers. An opener that arrived here
/// because a store WAS there is describing a file that went away underneath a
/// door it was already committed to, and the existing taxonomy is what it has
/// always said. An opener that promised never to create a store is being asked
/// the question directly, and "there is no store here" is its answer -- typed,
/// naming the path, and never confused with a store that is there and cannot
/// be read.
#[derive(Clone, Copy, PartialEq, Eq)]
enum AbsentStoreAnswer {
    Taxonomy,
    Typed,
}

struct ReadImageSource {
    database_path: PathBuf,
    runtime_dir: Option<PathBuf>,
    database: Option<redb::ReadOnlyDatabase>,
    snapshot_registration: Option<ReadSnapshotRegistrationGuard>,
    /// This reader's own proof that it is holding the store, taken before the
    /// committed file is opened and let go only after it is closed. A
    /// would-be writer waits on THIS, so it exists whether or not the
    /// diagnostic breadcrumb beside it could be published.
    hold: Option<ReaderStoreHold>,
    breadcrumb: Option<ReaderBreadcrumbGuard>,
    source_accesses: u64,
    /// The store file's length, taken where the file is open, so the answer a
    /// reader gives about disk usage is the same measurement of the same file
    /// a writer gives.
    store_file_bytes: Option<u64>,
    // This final field keeps the one admitted charge alive until every
    // hydration handle, hold and breadcrumb descriptor above has closed.
    source_memory: Option<SourceShared<SourceMemoryReservation>>,
    #[cfg(any(test, feature = "test-seams"))]
    source_handles_dropped_for_test: bool,
    #[cfg(any(test, feature = "test-seams"))]
    source_released_for_test: bool,
}

struct SourceCacheFunding {
    selected: usize,
    fixed: usize,
}

struct SourceMemoryReservation {
    memory: crate::read_image_memory::ImageMemoryHandle,
    bytes: std::sync::atomic::AtomicUsize,
    cache_funding: std::sync::Mutex<SourceCacheFunding>,
}

/// An exact, descriptor-backed user-tree source retained for the complete
/// lifetime of a direct reader. It is established and registered while the
/// load lock is held, then serves checksum-verifying reads without reopening
/// the pathname or consulting historical system-tree pages.
/// How a read that reaches a relational index authenticates the index
/// table's physical root against its producer witness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IndexRootCheck {
    /// The read consumes physical postings: any mismatch refuses.
    Physical,
    /// The read asks only the canonical tree and reads no physical posting:
    /// a mismatch is disclosed, never served from.
    Canonical,
}

pub(crate) type RetainedReadSource = SourceShared<RetainedReadSourceData>;

pub(crate) struct RetainedReadSourceData {
    database_path: PathBuf,
    database: redb::ReadOnlyDatabase,
    snapshot: redb::StorageSnapshotIdentity,
    /// Authenticated committed transaction frontier of this exact pinned root.
    logical_frontier: SnapshotId,
    graph_lsn_frontier: Lsn,
    graph_retirement_frontier: u64,
    graph_occurrence_id_floor: u64,
    registration: Arc<ReadSnapshotRegistrationGuard>,
    content_identity: content_identity::ContentIdentityReadVerifier,
    history_open_control: content_identity::ValidatedHistoryOpenControl,
    /// The existing reader child that admitted this exact source capture.
    read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    /// Pages proved under this source's pinned root, shared by row and exact
    /// vector-body reads; retained bytes remain charged to the read session.
    content_proof: Mutex<content_identity::ReadAuthenticationNodes>,
    physical_rows: Mutex<Option<RetainedPhysicalRowScan>>,
    image_config_cache: Mutex<HashMap<String, Option<Vec<u8>>>>,
    pub(crate) native_ivf_cache: vector_ivf::NativeIvfCache,
    verified_ordered_runs: VerifiedOrderedRunCache,
    _source_memory: SourceShared<SourceMemoryReservation>,
}

/// The most proof pages one statement's open body read keeps between the
/// bodies it authenticates: room for the shared upper levels of the proof
/// tree, which every body's path crosses. Each page stays charged to the
/// session for as long as anything holds it -- through its own decode
/// admission, or through the resident set's charge when the reader keeps the
/// same page -- and unlike the reader's resident chunks it is not given back
/// to an ordinary allocation under pressure, only dropped whole when a proof
/// would otherwise be refused, so it is kept small; past it the read proves
/// against the resident chunks.
const RAW_VECTOR_BODY_SCAN_PROOF_BYTES: usize = 128 * 1024;
/// The direct source can reuse row proofs when its later exact fallback reads
/// the same vector bodies. This is a cap, not an up-front reservation.
const RETAINED_CONTENT_PROOF_BYTES: usize = 8 * 1024 * 1024;

/// One statement's open read of stored vector bodies on a retained image.
pub(crate) struct RetainedRawVectorBodyScan {
    producer_body_root: bool,
    selected_stream_identity: Option<RawVectorDirectoryEntry>,
    source: RetainedReadSource,
    vectors: Option<redb::ReadOnlyTable<&'static [u8], &'static [u8]>>,
    // Declared last so the tables above close before the transaction that
    // pins them does.
    _transaction: Arc<redb::ReadTransaction>,
}

/// Keeps the exact retained root and physical table open while a bounded
/// cursor advances through its rows.
struct RetainedPhysicalRowScan {
    table_name: String,
    relation: redb::ReadOnlyTable<&'static [u8], &'static [u8]>,
    _transaction: redb::ReadTransaction,
}

/// One writer statement's pinned view of vector bodies and their committed
/// content identity. Reusing the transaction and proof nodes makes exact
/// fallback work proportional to the candidates actually compared.
pub(crate) struct WritableRawVectorBodyScan {
    producer_body_root: bool,
    selected_stream_identity: Option<RawVectorDirectoryEntry>,
    source: Arc<RedbPersistence>,
    vectors: Option<redb::ReadOnlyTable<&'static [u8], &'static [u8]>>,
    proof: content_identity::ReadAuthenticationNodes,
    _transaction: Arc<redb::ReadTransaction>,
    _root: Option<contextdb_tx::native_capture::CaptureHandle>,
}

const VERIFIED_ORDERED_RUN_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;
const VERIFIED_ORDERED_RUN_BUILD_MAX_BYTES: usize = 8 * 1024 * 1024;
/// The retained payload of ONE authenticated working window: the proven
/// postings a cursor holds while it delivers them, and the most it has to
/// re-prove when that window is evicted. It is a declared bound, like the two
/// above it, and it is what makes the route's physical work a function of the
/// range rather than of its square: re-proving after an eviction costs at most
/// this many bytes of postings however long the range is.
///
/// It is a SCHEDULING cap and nothing else. It does not decide which keys or
/// results are admissible: the payload is admitted as it is kept, from a pool
/// that grows for it, so a window that fills this cap and a window that runs
/// out of budget both simply end and the range continues in the next one.
const VERIFIED_ORDERED_WINDOW_MAX_BYTES: usize = 256 * 1024;

struct VerifiedOrderedRunCache {
    memory: crate::read_image_memory::ImageMemoryHandle,
    max_bytes: usize,
    state: Arc<Mutex<VerifiedOrderedRunCacheState>>,
    /// Bumped when a cached window is dropped so a cursor still holding
    /// proved pages for that window re-reads them from the committed root.
    epoch: std::sync::atomic::AtomicU64,
}

#[cfg(any(test, feature = "test-seams"))]
mod verified_ordered_run_test_seams;

#[derive(Default)]
struct VerifiedOrderedRunCacheState {
    runs: HashMap<[u8; 32], Arc<VerifiedOrderedRun>>,
    insertion_order: VecDeque<[u8; 32]>,
    bytes: usize,
}

/// The statement funds every proof page as it is admitted. A failed growth
/// retains the original typed budget refusal instead of treating a partial
/// authenticated descent as an empty group range.
struct CommittedCountProofMemory {
    memory: crate::read_image_memory::ImageMemoryHandle,
    refusal: Mutex<Option<Error>>,
}

/// One bounded working window of proven postings. `more` says the authenticated
/// range continues past the last posting here, so the cursor opens the next
/// window from that posting rather than finishing its run.
struct VerifiedOrderedRun {
    postings: Vec<VerifiedOrderedPosting>,
    more: bool,
    quota_stopped: bool,
    _memory: Arc<VerifiedOrderedRunMemory>,
}

struct VerifiedOrderedPosting {
    source_key: Vec<u8>,
    row_id: RowId,
}

/// One build's reservation against the reader's own image memory. It begins
/// with nothing reserved and grows by the walk's actual demand -- up to,
/// never straight to, this cache's ceiling.
struct VerifiedOrderedRunMemory {
    memory: crate::read_image_memory::ImageMemoryHandle,
    /// The cache this reservation evicts from before it admits more. Weak, so
    /// a cached window's own reservation never keeps its cache alive.
    cache: std::sync::Weak<Mutex<VerifiedOrderedRunCacheState>>,
    ceiling: usize,
    bytes: std::sync::atomic::AtomicUsize,
}

/// The read and physical index a cursor's windows reconcile against, kept
/// open across the windows of one statement.
struct RetainedIndexRead {
    table: String,
    index: String,
    physical: redb::ReadOnlyTable<DurableRelationalIndexKey, &'static [u8]>,
    current: Option<redb::ReadOnlyTable<DurableRelationalIndexKey, &'static [u8]>>,
    _read: redb::ReadTransaction,
}

/// One cursor's bounded authentication pool and decoded tree pages. The
/// physical posting windows keep separate reservations, so evicting a window
/// never discards the shared upper proof path.
struct RetainedOrderedProof {
    table: String,
    index: String,
    snapshot: SnapshotId,
    cache: Arc<Mutex<VerifiedOrderedRunCacheState>>,
    epoch: u64,
    proof: content_identity::ProofMemory,
    batch: content_identity::OrderedProofBatch,
    _memory: Arc<VerifiedOrderedRunMemory>,
}

/// The exact durable source captured with a checkpoint's logical watermark.
/// Redb keeps this read transaction pinned while export streams batches, and
/// the verifier binds every copied vector projection to roots from that same
/// storage snapshot.
pub(crate) struct CheckpointReadSource {
    database_path: PathBuf,
    read: redb::ReadTransaction,
    content_identity: content_identity::ContentIdentityReadVerifier,
    /// The accountant every copied body is admitted through, so an export of
    /// bodies the directory no longer carries is bounded by the same budget
    /// the rest of the database answers to.
    accountant: Option<crate::memory_accounting::AccountantHandle>,
    decode_memory: crate::read_image_memory::ImageMemoryHandle,

    _writer_use: Option<crate::memory_accounting::WriterUse>,
}

/// A run holds exactly its decoder/copy owner through caller serialization.
pub(crate) struct ExportBodyAdmission {
    memory: crate::read_image_memory::ImageMemoryHandle,
    decoded: Option<crate::read_image_memory::ImageMemoryHandle>,
    decoded_controls: Option<contextdb_core::read_memory::ReadCredit>,
}

#[cfg(any(test, feature = "test-seams"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct SourceCacheChargeForTest {
    pub index_bytes: usize,
    pub remaining_base: usize,
    pub source_control: (usize, usize, usize),
    pub retained_control: (usize, usize, usize),
    pub preparation: (usize, usize, usize),
    pub preparation_invoice: usize,
    pub payer_identity: usize,
    pub allocations: [(usize, usize, usize); 140],
    pub allocation_count: usize,
}

mod read_image_decode;
pub use read_image_decode::*;

// Preserve native storage provenance without changing the shared error vocabulary.
const STORAGE_IO_ERROR_PREFIX: &str = "contextdb-engine-storage-io:";

#[cfg(test)]
mod cdb130_storage_error_tests;

#[derive(Clone, Copy)]
enum ReadCellShape {
    Null,
    Vector(usize),
    Other,
}

/// Why a record payload did not decode: a memory refusal or cancellation is
/// the caller's to report as itself; anything else is damage to the record.
enum RecordDecodeFailure {
    Refusal(Error),
    Damage(contextdb_core::error::DiagnosticText),
}

/// `decode_vector_partition_record_payload` for read paths that must never
/// relabel a declared-budget refusal as a corrupt store.
struct PayloadDecodeDiagnostic(Error);

/// A serde inspection form: TEXT is borrowed, and sequence elements are
/// consumed one at a time. In particular its visitor never reserves from the
/// stored sequence hint. Variant order follows VectorPartitionComponent.
#[allow(dead_code, reason = "WIRE-FIELD: part of a fixed encoded format")]
#[derive(Deserialize)]
enum InspectedVectorPartitionComponent<'a> {
    Uuid(uuid::Uuid),
    Text(#[serde(borrow)] &'a str),
    #[allow(
        dead_code,
        reason = "WIRE-FIELD: decoded from stored bytes; its fields fix the encoded format"
    )]
    Integer(i64),
    #[allow(
        dead_code,
        reason = "WIRE-FIELD: decoded from stored bytes; its fields fix the encoded format"
    )]
    Boolean(bool),
    #[allow(
        dead_code,
        reason = "WIRE-FIELD: decoded from stored bytes; its fields fix the encoded format"
    )]
    Timestamp(i64),
    #[allow(
        dead_code,
        reason = "WIRE-FIELD: decoded from stored bytes; its fields fix the encoded format"
    )]
    TxId(TxId),
}

struct InspectedVectorPartitionComponents {
    count: usize,
    text_bytes: usize,
}

/// Validate sequence elements without reserving from a durable size hint.
/// The count is actual consumed elements, never the advertised length.
struct InspectedSequence<T> {
    count: usize,
    marker: std::marker::PhantomData<T>,
}

/// Positional inspection forms of the canonical persisted vector. All route
/// strings borrow the authenticated input; payload sequences never reserve.
#[allow(
    dead_code,
    reason = "WIRE-FIELD: decoded from stored bytes; its fields fix the encoded `InspectedPersistedVector` format"
)]
#[derive(Deserialize)]
enum InspectedPersistedVector {
    F32(InspectedSequence<f32>),
    SQ8 {
        min: f32,
        max: f32,
        len: u32,
        payload: InspectedSequence<u8>,
    },
    SQ4 {
        min: f32,
        max: f32,
        len: u32,
        payload: InspectedSequence<u8>,
    },
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static VECTOR_JOURNAL_CLEANUP_RECORDS_READ: std::cell::Cell<u64> =
        const { std::cell::Cell::new(0) };
}

struct ReadRawVectorDirectory {
    ready: bool,
}

#[derive(Clone)]
struct ReadVectorRename {
    lsn: Lsn,
    from: VectorIndexRef,
    to: VectorIndexRef,
}

/// What kind of history, audit or queued record a file-reader open touched.
#[derive(Debug, Clone, Copy)]
pub(crate) enum OpenHistoryClass {
    ChangeLog,
    DdlLog,
    CommitIndex,
    /// A commit-index entry read by the open-time order diagnosis: its two
    /// fixed-size numbers compared and dropped, nothing decoded or kept.
    CommitIndexScanned,
    Audit,
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from read_image_decode.rs"
        )
    )]
    Queued,
}

/// The frontiers a reader exposes from history it leaves in the store, read
/// from the committed history rather than from whatever the stored tables
/// end with.
struct AuthenticatedHistoryFrontiers {
    control: content_identity::ValidatedHistoryOpenControl,
    commit_last: Option<(Lsn, TxId)>,
    ddl_last_lsn: Option<Lsn>,
}

/// Values needed to construct the committed image are held in one small
/// writer-maintained table. Other configuration stays behind the retained
/// snapshot for later point reads.
const READ_IMAGE_OPEN_CONFIG_KEYS: [&str; 16] = [
    "memory_limit",
    "disk_limit",
    crate::database::MAINTENANCE_POLL_INTERVAL_CONFIG_KEY,
    crate::database::STORAGE_COMPACTION_CONFIG_KEY,
    crate::database::VECTOR_SERVING_CACHE_CONFIG_KEY,
    crate::database::RETENTION_SYNC_PEER_CONFIG_KEY,
    TRIGGER_AUDIT_RING_CONFIG_KEY,
    TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY,
    "__cron_schedules",
    "__cron_audit",
    "__event_bus_event_types",
    "__event_bus_sinks",
    "__event_bus_routes",
    "__event_bus_next_action_id",
    "__trigger_declarations",
    "__trigger_including_sync",
];

const READ_IMAGE_OPEN_CONFIG_PREFIXES: [&str; 3] = [
    crate::database::event_bus::UNCERTAIN_COUNT_CONFIG_PREFIX,
    crate::database::event_bus::UNCONFIRMED_COUNT_CONFIG_PREFIX,
    crate::database::Database::DURABLE_LINEAGE_TABLE_GENERATION_PREFIX,
];

#[cfg(any(test, feature = "test-seams"))]
static LINEAGE_SCRATCH_REFUSAL: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[derive(Clone)]
struct CompanionState {
    needs_initialization: bool,
    active_slot: Option<usize>,
    active_record: Option<EncodedCompanionRecord>,
    pending_slot: Option<usize>,
    pending_replacement: Option<PendingReplacement>,
    pending_matches_store: Option<bool>,
}

#[derive(Clone)]
struct PendingReplacement {
    store_fingerprint: [u8; 32],
    intended_record: EncodedCompanionRecord,
    checksummed_payload: Vec<u8>,
    stored_checksum: [u8; 32],
}

enum ParsedCompanionSlot {
    Vacant,
    Record(EncodedCompanionRecord),
    Pending(PendingReplacement),
}

struct CompanionGuardState {
    file: File,
    companion: CompanionState,
}

/// Exclusive ownership of one verified, never-unlinked companion inode.
/// Separate opens in the same process contend exactly like separate
/// processes.
pub(crate) struct CompanionGuard {
    path: PathBuf,
    state: Mutex<CompanionGuardState>,
}

/// What the holder of a companion is about to do with it.
///
/// The trust boundary is the same either way: one current-user-owned private
/// regular file, no symlink followed, still the file at this pathname on both
/// sides of the probe. What differs is the owner's WRITE bit. A writer needs
/// it. A reader never writes a companion -- it reads the published record and
/// takes the shared side of an advisory byte, both of which a read-only
/// descriptor does -- so demanding it of a reader makes a store whose files
/// were sealed read-only unreadable, for a permission the reader was never
/// going to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompanionUse {
    Reading,
    Writing,
}

/// What to do with the store a standing replacement intent names when the
/// surviving generation is the prior one.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum AbandonedTarget {
    /// Establishing ownership: the recorded target is residue and goes.
    Remove,
    /// Re-recording: the recorded target is the live replacement and stays.
    Retain,
}

/// Why a companion inode is being opened. The variants differ only in what an
/// occupant of the companion pathname is allowed to be: an ordinary open
/// demands a recoverable generation, while a genuinely new store's companion
/// pathname can only be holding residue of a store that no longer exists.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CompanionAdmission {
    /// The companion must already exist and must carry a recoverable
    /// generation.
    Existing,
    /// Create the companion when it is missing; an existing one must still
    /// carry a recoverable generation.
    CreateIfMissing,
    /// The store itself is being created at this pathname, so anything
    /// already occupying the companion pathname described a store that is
    /// gone. It is re-initialized in place under this exclusive lock instead
    /// of making the pathname permanently unusable.
    NewStore,
}

mod companion_and_vector_source;
#[cfg(any(test, feature = "test-seams"))]
pub use companion_and_vector_source::*;
#[cfg(not(any(test, feature = "test-seams")))]
pub(crate) use companion_and_vector_source::*;

/// Test-only observability for the read-persistence coordination contract.
///
/// The production companion implementation calls the checkpoint at each
/// durable write boundary while this feature is enabled. The test process
/// drives the real writer in a child process and turns one checkpoint into a
/// process abort, so recovery is always exercised against the bytes the
/// production writer had actually reached. This module never manufactures a
/// lock, reader, companion record, or recovery result.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub mod read_persistence_test_scaffold {
    type CrashedCaptureHook = std::sync::Arc<dyn Fn(u32) -> bool + Send + Sync>;
    static CRASHED_CAPTURE_HOOK: std::sync::Mutex<Option<CrashedCaptureHook>> =
        std::sync::Mutex::new(None);
    static CRASHED_CAPTURE_ALLOWANCE: std::sync::Mutex<
        Option<std::sync::Arc<CrashedCaptureAllowance>>,
    > = std::sync::Mutex::new(None);

    /// Call `hook` with each checkpoint index while a crashed store's root is
    /// chosen -- 0 before the header, then one before each root page. A
    /// `true` answer cancels the read right there, as its caller would.
    /// Process-wide; install it only in a child process that exists to read.
    pub fn install_crashed_capture_hook_for_test(
        hook: impl Fn(u32) -> bool + Send + Sync + 'static,
    ) {
        *CRASHED_CAPTURE_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(std::sync::Arc::new(hook));
    }

    pub(super) fn crashed_capture_checkpoint_for_test(index: u32) -> bool {
        let hook = CRASHED_CAPTURE_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        hook.is_some_and(|hook| hook(index))
    }

    /// Fund the next crashed-store root capture from at most `bytes` of the
    /// reader's own budget, so a proof can make that capture's admission
    /// refuse. Process-wide, like the hook.
    pub fn arm_crashed_capture_byte_allowance_for_test(bytes: usize) {
        *CRASHED_CAPTURE_ALLOWANCE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some(std::sync::Arc::new(CrashedCaptureAllowance {
                inner: std::sync::Mutex::new(None),
                limit: bytes,
                held: std::sync::atomic::AtomicUsize::new(0),
            }));
    }

    /// Bytes the armed allowance still holds: what a refused or finished
    /// capture failed to give back.
    pub fn crashed_capture_bytes_held_for_test() -> Option<usize> {
        CRASHED_CAPTURE_ALLOWANCE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(|allowance| allowance.held.load(std::sync::atomic::Ordering::SeqCst))
    }

    pub(super) fn crashed_capture_memory_for_test(
        reader: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> contextdb_core::read_memory::ReadMemoryOwner {
        let reader: contextdb_core::read_memory::ReadMemoryOwner = reader.into();

        let allowance = CRASHED_CAPTURE_ALLOWANCE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        match allowance {
            Some(allowance) => {
                *allowance
                    .inner
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(reader);
                allowance.into()
            }
            None => reader,
        }
    }

    /// A reader budget that also refuses past a fixed allowance.
    #[derive(Debug)]
    pub(super) struct CrashedCaptureAllowance {
        inner: std::sync::Mutex<Option<contextdb_core::read_memory::ReadMemoryOwner>>,
        limit: usize,
        held: std::sync::atomic::AtomicUsize,
    }

    impl CrashedCaptureAllowance {
        fn reader(&self) -> contextdb_core::read_memory::ReadMemoryOwner {
            self.inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
                .expect("the allowance funds a capture only once given its reader")
        }
    }

    impl contextdb_core::read_memory::ReadMemoryBudget for CrashedCaptureAllowance {
        fn try_reserve(
            &self,
            bytes: usize,
            subsystem: &'static str,
            operation: &'static str,
            hint: &'static str,
        ) -> contextdb_core::Result<()> {
            use std::sync::atomic::Ordering;
            let held = self.held.load(Ordering::SeqCst);
            if held.saturating_add(bytes) > self.limit {
                return Err(contextdb_core::Error::MemoryBudgetExceeded {
                    subsystem: (subsystem.to_owned()).into(),
                    operation: (operation.to_owned()).into(),
                    requested_bytes: bytes,
                    available_bytes: self.limit.saturating_sub(held),
                    budget_limit_bytes: self.limit,
                    hint: (hint.to_owned()).into(),
                });
            }
            self.reader()
                .try_reserve(bytes, subsystem, operation, hint)?;
            self.held.fetch_add(bytes, Ordering::SeqCst);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            self.held
                .fetch_sub(bytes, std::sync::atomic::Ordering::SeqCst);
            self.reader().release(bytes);
        }
    }

    static READER_SEES_ANOTHER_BOOT: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);
    static READER_SEES_UNTRUSTED_FILESYSTEM: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);

    /// Make this process read its boot identity as a different boot's, as a
    /// reader does after the machine restarted. Process-wide; set it only in
    /// a child process that exists to read.
    pub fn set_reader_sees_another_boot_for_test(another: bool) {
        READER_SEES_ANOTHER_BOOT.store(another, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn reader_sees_another_boot_for_test() -> bool {
        READER_SEES_ANOTHER_BOOT.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Make this process treat every data file's filesystem as one whose
    /// page cache it does not trust. Process-wide, like the boot seam.
    pub fn set_reader_sees_untrusted_filesystem_for_test(untrusted: bool) {
        READER_SEES_UNTRUSTED_FILESYSTEM.store(untrusted, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn reader_sees_untrusted_filesystem_for_test() -> bool {
        READER_SEES_UNTRUSTED_FILESYSTEM.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct RetainedGraphPageReadObservation {
        pub offset: usize,
        pub returned_bytes: usize,
        pub source_page_bytes: usize,
        pub physical_page_lookups: usize,
        pub authenticated_body_bytes: usize,
    }

    type RetainedGraphPageReadObserver =
        std::sync::Arc<dyn Fn(RetainedGraphPageReadObservation) + Send + Sync>;

    thread_local! {
        static RETAINED_GRAPH_PAGE_READ_OBSERVER: std::cell::RefCell<Option<RetainedGraphPageReadObserver>> = const { std::cell::RefCell::new(None) };
    }

    pub fn observe_retained_graph_page_reads_for_test<T>(
        observer: RetainedGraphPageReadObserver,
        operation: impl FnOnce() -> T,
    ) -> T {
        struct Restore(Option<RetainedGraphPageReadObserver>);
        impl Drop for Restore {
            fn drop(&mut self) {
                RETAINED_GRAPH_PAGE_READ_OBSERVER.with(|slot| *slot.borrow_mut() = self.0.take());
            }
        }
        let _restore =
            Restore(RETAINED_GRAPH_PAGE_READ_OBSERVER.with(|slot| slot.replace(Some(observer))));
        operation()
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum WriterFileRecycleBoundary {
        BeforeViewClose,
        AfterViewClose,
        AfterViewOpen,
    }

    /// A weak observation cannot prolong physical file ownership.
    #[derive(Debug, Clone)]
    pub struct WriterFileLifetime(pub(super) std::sync::Weak<super::LockedWriterFile>);

    impl WriterFileLifetime {
        pub fn is_alive(&self) -> bool {
            self.0.strong_count() != 0
        }
    }

    #[derive(Debug, Clone)]
    pub struct WriterFileRecycleObservation {
        pub boundary: WriterFileRecycleBoundary,
        #[cfg(unix)]
        pub identity: (u64, u64),
        pub live_views: usize,
        pub owner: WriterFileLifetime,
    }

    type WriterRecycleObserver = std::sync::Arc<dyn Fn(WriterFileRecycleObservation) + Send + Sync>;

    thread_local! {
        static WRITER_RECYCLE_OBSERVER: std::cell::RefCell<Option<WriterRecycleObserver>> = const { std::cell::RefCell::new(None) };
    }

    pub fn observe_writer_file_recycle_for_test<T>(
        observer: WriterRecycleObserver,
        operation: impl FnOnce() -> T,
    ) -> T {
        struct Restore(Option<WriterRecycleObserver>);
        impl Drop for Restore {
            fn drop(&mut self) {
                WRITER_RECYCLE_OBSERVER.with(|slot| *slot.borrow_mut() = self.0.take());
            }
        }
        let _restore = Restore(WRITER_RECYCLE_OBSERVER.with(|slot| slot.replace(Some(observer))));
        operation()
    }

    /// Calls at the writer backend boundary, on the observing thread. End
    /// observations report successful completion; begin observations can be
    /// used by a child process to stop at an exact write or sync cut.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum WriterFileIoKindForTest {
        ReadBegin,
        ReadEnd,
        WriteBegin,
        WriteEnd,
        SyncBegin,
        SyncEnd,
    }

    #[derive(Debug, Clone, Copy)]
    pub struct WriterFileIoForTest {
        pub kind: WriterFileIoKindForTest,
        pub offset: u64,
        pub bytes: usize,
        /// `None` at begin, `Some(result.is_ok())` at end.
        pub success: Option<bool>,
    }

    pub type WriterFileIoObserverForTest =
        std::sync::Arc<dyn Fn(WriterFileIoForTest) + Send + Sync>;

    thread_local! {
        static WRITER_IO_OBSERVER: std::cell::RefCell<Option<WriterFileIoObserverForTest>> = const { std::cell::RefCell::new(None) };
    }

    /// Observe only this thread, restoring the enclosing observer on return
    /// or unwind. The observer owns any storage used for retained receipts.
    pub fn observe_writer_file_io_for_test<T>(
        observer: WriterFileIoObserverForTest,
        operation: impl FnOnce() -> T,
    ) -> T {
        struct Restore(Option<WriterFileIoObserverForTest>);
        impl Drop for Restore {
            fn drop(&mut self) {
                WRITER_IO_OBSERVER.with(|slot| *slot.borrow_mut() = self.0.take());
            }
        }
        let _restore = Restore(WRITER_IO_OBSERVER.with(|slot| slot.replace(Some(observer))));
        operation()
    }

    pub(super) fn note_writer_io_for_test(
        kind: WriterFileIoKindForTest,
        offset: u64,
        bytes: usize,
        success: Option<bool>,
    ) {
        let observer = WRITER_IO_OBSERVER.with(|slot| slot.borrow().clone());
        if let Some(observer) = observer {
            observer(WriterFileIoForTest {
                kind,
                offset,
                bytes,
                success,
            });
        }
    }

    static WRITER_DATA_SYNCS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    static WRITER_DATA_BYTES_WRITTEN: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);
    static WRITER_DATA_SYNC_CRASH_AT: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);

    /// What every writer in this process has handed the store file so far:
    /// `(data syncs, bytes written)`. Process-wide, so a measurement runs in a
    /// child process of its own.
    pub fn writer_file_io_for_test() -> (u64, u64) {
        (
            WRITER_DATA_SYNCS.load(std::sync::atomic::Ordering::SeqCst),
            WRITER_DATA_BYTES_WRITTEN.load(std::sync::atomic::Ordering::SeqCst),
        )
    }

    /// Abort the process at the next writer data sync, before it is issued:
    /// whatever the commit in flight has written is on its way to the disk,
    /// and nothing of it is yet known to be durable. Process-wide; arm it
    /// only in a child process that exists to crash.
    pub fn arm_writer_sync_crash_for_test() {
        let next = WRITER_DATA_SYNCS.load(std::sync::atomic::Ordering::SeqCst) + 1;
        WRITER_DATA_SYNC_CRASH_AT.store(next, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn note_writer_data_sync_for_test() {
        let sync = WRITER_DATA_SYNCS.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        if WRITER_DATA_SYNC_CRASH_AT.load(std::sync::atomic::Ordering::SeqCst) == sync {
            use std::io::Write as _;
            let mut stderr = std::io::stderr().lock();
            let _ = writeln!(stderr, "WRITER_SYNC_CRASH={sync}");
            let _ = stderr.flush();
            std::process::abort();
        }
    }

    static WRITER_PAGE_WRITES_LOST: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);

    /// Model a power loss inside the next commit: from now on only the file
    /// header's writes reach the store, every page write is lost, and the
    /// process aborts at the next sync. The header then names a commit whose
    /// pages never landed -- the torn commit a single-sync protocol can leave.
    /// Process-wide; arm it only in a child process that exists to crash.
    pub fn arm_writer_torn_commit_crash_for_test() {
        WRITER_PAGE_WRITES_LOST.store(true, std::sync::atomic::Ordering::SeqCst);
        arm_writer_sync_crash_for_test();
    }

    static WRITER_WRITES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    static WRITER_WRITE_CRASH_AT: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);

    /// Abort the process just before the `nth` writer write from now (1 is
    /// the next one) is handed to the store file, as a process killed at that
    /// instant leaves it: every earlier write is in the file, that one and
    /// every later one are not. Process-wide; arm it only in a child process
    /// that exists to crash.
    pub fn arm_writer_write_crash_for_test(nth: u64) {
        let next = WRITER_WRITES.load(std::sync::atomic::Ordering::SeqCst) + nth;
        WRITER_WRITE_CRASH_AT.store(next, std::sync::atomic::Ordering::SeqCst);
    }

    /// Writer writes handed to the store file so far in this process.
    pub fn writer_writes_for_test() -> u64 {
        WRITER_WRITES.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Count one writer write; false when the armed torn commit loses it.
    pub(super) fn note_writer_data_write_for_test(offset: u64, bytes: usize) -> bool {
        let write = WRITER_WRITES.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        if WRITER_WRITE_CRASH_AT.load(std::sync::atomic::Ordering::SeqCst) == write {
            use std::io::Write as _;
            let mut stderr = std::io::stderr().lock();
            let _ = writeln!(stderr, "WRITER_WRITE_CRASH={write}");
            let _ = stderr.flush();
            std::process::abort();
        }
        WRITER_DATA_BYTES_WRITTEN.fetch_add(bytes as u64, std::sync::atomic::Ordering::SeqCst);
        offset == 0 || !WRITER_PAGE_WRITES_LOST.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(super) fn note_writer_file_recycle_for_test(observation: WriterFileRecycleObservation) {
        let observer = WRITER_RECYCLE_OBSERVER.with(|slot| slot.borrow().clone());
        if let Some(observer) = observer {
            observer(observation);
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum WriterReaderSnapshotRegistryObservation {
        Complete(Vec<WriterReaderSnapshotObservation>),
        Indeterminate,
        Unavailable,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct WriterReaderSnapshotObservation {
        pub physical_identity: (u64, u64),
        pub database_identity: Option<contextdb_core::read_contract::DatabaseIdentity>,
        pub storage_transaction_id: u64,
        pub storage_root_page: Option<[u8; 8]>,
        pub handle_identity: [u8; 16],
        pub process_id: u32,
        pub descriptor_number: u32,
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct ExternalSnapshotRetentionObservation {
        pub snapshots: u64,
        pub reachable_pages: u64,
        pub referenced_pages: u64,
        pub retained_pages: u64,
        pub reachable_bytes: u64,
        pub referenced_bytes: u64,
        pub retained_bytes: u64,
        pub retained_page_allocations: Vec<[u8; 8]>,
    }

    type WriterReaderSnapshotObserver =
        std::sync::Arc<dyn Fn(WriterReaderSnapshotRegistryObservation) + Send + Sync>;

    thread_local! {
        static WRITER_READER_SNAPSHOT_OBSERVER: std::cell::RefCell<Option<WriterReaderSnapshotObserver>> = const { std::cell::RefCell::new(None) };
    }

    pub fn observe_writer_reader_snapshots_for_test<T>(
        observer: WriterReaderSnapshotObserver,
        operation: impl FnOnce() -> T,
    ) -> T {
        struct Restore(Option<WriterReaderSnapshotObserver>);
        impl Drop for Restore {
            fn drop(&mut self) {
                WRITER_READER_SNAPSHOT_OBSERVER.with(|slot| *slot.borrow_mut() = self.0.take());
            }
        }
        let _restore =
            Restore(WRITER_READER_SNAPSHOT_OBSERVER.with(|slot| slot.replace(Some(observer))));
        operation()
    }

    pub(super) fn note_writer_reader_snapshots_for_test(registry: &super::ReaderSnapshotRegistry) {
        let observation = match registry {
            super::ReaderSnapshotRegistry::Complete(snapshots) => {
                WriterReaderSnapshotRegistryObservation::Complete(
                    snapshots
                        .iter()
                        .map(|snapshot| WriterReaderSnapshotObservation {
                            physical_identity: snapshot.physical_identity,
                            database_identity: snapshot.database_identity,
                            storage_transaction_id: snapshot.storage_snapshot.transaction_id,
                            storage_root_page: snapshot
                                .storage_snapshot
                                .user_root
                                .map(|root| root.page),
                            handle_identity: snapshot.handle_identity,
                            process_id: snapshot.process.process_id,
                            descriptor_number: snapshot.descriptor_number,
                        })
                        .collect(),
                )
            }
            super::ReaderSnapshotRegistry::Contended(_)
            | super::ReaderSnapshotRegistry::Indeterminate(_) => {
                WriterReaderSnapshotRegistryObservation::Indeterminate
            }
            super::ReaderSnapshotRegistry::Unavailable => {
                WriterReaderSnapshotRegistryObservation::Unavailable
            }
        };
        let observer = WRITER_READER_SNAPSHOT_OBSERVER.with(|slot| slot.borrow().clone());
        if let Some(observer) = observer {
            observer(observation);
        }
    }

    pub fn arm_handle_recycle_reopen_fault_for_test() {
        super::arm_handle_recycle_reopen_fault_for_test();
    }

    use contextdb_core::read_contract::{
        ChannelAddress, DatabaseIdentity, LocalUserIdentity, OwnerReadStatus, ReaderBreadcrumb,
        WriterRunNumber,
    };
    use redb::{ReadableDatabase as _, ReadableTable as _, TableDefinition};
    #[cfg(any(test, feature = "test-seams"))]
    use std::collections::BTreeMap;
    use std::collections::{BTreeSet, HashMap, VecDeque};
    use std::io::{BufRead as _, Write as _};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex, OnceLock};

    /// Timeless companion codec choice: canonical resolved paths are hashed
    /// with BLAKE3 into the fixed 32-byte channel address.
    pub const COMPANION_CHANNEL_ADDRESS_CODEC: &str = "blake3-canonical-path-v1";

    /// Canonical companion payload fields. The byte codec is fixed as:
    /// `contextdb-companion`, little-endian format/generation, fixed identity
    /// arrays, little-endian user/process values, state and reason tags,
    /// little-endian reason length, then UTF-8 reason bytes. BLAKE3 covers the
    /// complete payload.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct CompanionRecordFields {
        pub format_version: u16,
        pub generation: u64,
        pub database_identity: DatabaseIdentity,
        pub writer_run_number: WriterRunNumber,
        pub owner_user: LocalUserIdentity,
        pub channel_address: ChannelAddress,
        pub process_id: u32,
        pub owner_read_status: OwnerReadStatus,
        pub writer_boot: Option<CompanionWriterBootFields>,
    }

    /// The boot and the exact data file a writer run opened its store on, as
    /// the companion record carries them.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct CompanionWriterBootFields {
        pub boot_id: [u8; 16],
        pub data_dev: u64,
        pub data_ino: u64,
        pub data_birth_nanos: u128,
    }

    /// Every durable boundary of a dual-slot companion update.  The
    /// production writer must checkpoint after completing each named action,
    /// before starting the next one.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum CompanionWriteBoundary {
        SlotPayloadWrite,
        SlotPayloadSync,
        SlotChecksumWrite,
        SlotChecksumSync,
        ActiveSlotWrite,
        ActiveSlotSync,
    }

    impl CompanionWriteBoundary {
        pub const ALL: [Self; 6] = [
            Self::SlotPayloadWrite,
            Self::SlotPayloadSync,
            Self::SlotChecksumWrite,
            Self::SlotChecksumSync,
            Self::ActiveSlotWrite,
            Self::ActiveSlotSync,
        ];
    }

    /// A successfully decoded durable companion record.  Inspection returns
    /// only a checksummed, complete record; a torn slot must recover to one
    /// of these rather than exposing partial bytes to the test.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct CompanionRecordObservation {
        pub fields: CompanionRecordFields,
        /// The exact durable record payload covered by `stored_checksum`.
        pub checksummed_payload: Vec<u8>,
        pub stored_checksum: [u8; 32],
    }

    /// Read-only capture of the exact replacement state that exists before
    /// any recovery opener can resolve or overwrite it.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ReplacementIntentObservation {
        pub active_record: Option<CompanionRecordObservation>,
        pub pending_intended_record: Option<CompanionRecordObservation>,
        pub pending_store_fingerprint: Option<[u8; 32]>,
        pub current_store_fingerprint: [u8; 32],
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum RawRedbWriterOpenObservation {
        Acquired,
        DatabaseAlreadyOpen,
        Other(String),
    }

    /// What Redb itself says when asked to open a store WITHOUT writing to it.
    ///
    /// `RepairAborted` is the answer that matters: a store whose last writer
    /// died with it open cannot be read at all until Redb's own crash repair
    /// runs, and that repair rewrites the file. Whether a root really is in
    /// that state is not something a proof can infer from the outside -- a
    /// healthy root and a crash-dirty one look alike from every angle that
    /// does not open them -- so a proof about what may or may not repair one
    /// has to be able to ask.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum RawRedbReadOnlyOpenObservation {
        Acquired,
        RepairAborted,
        Other(String),
    }

    fn companion_fields_for_production(fields: &CompanionRecordFields) -> super::CompanionRecord {
        super::CompanionRecord {
            format_version: fields.format_version,
            generation: fields.generation,
            database_identity: fields.database_identity,
            writer_run_number: fields.writer_run_number,
            owner_user: fields.owner_user,
            channel_address: fields.channel_address,
            process_id: fields.process_id,
            owner_read_status: fields.owner_read_status.clone(),
            writer_boot: fields.writer_boot.map(|boot| super::WriterBootBinding {
                boot_id: boot.boot_id,
                data_dev: boot.data_dev,
                data_ino: boot.data_ino,
                data_birth_nanos: boot.data_birth_nanos,
            }),
        }
    }

    fn companion_observation(record: super::EncodedCompanionRecord) -> CompanionRecordObservation {
        CompanionRecordObservation {
            fields: CompanionRecordFields {
                format_version: record.fields.format_version,
                generation: record.fields.generation,
                database_identity: record.fields.database_identity,
                writer_run_number: record.fields.writer_run_number,
                owner_user: record.fields.owner_user,
                channel_address: record.fields.channel_address,
                process_id: record.fields.process_id,
                owner_read_status: record.fields.owner_read_status,
                writer_boot: record
                    .fields
                    .writer_boot
                    .map(|boot| CompanionWriterBootFields {
                        boot_id: boot.boot_id,
                        data_dev: boot.data_dev,
                        data_ino: boot.data_ino,
                        data_birth_nanos: boot.data_birth_nanos,
                    }),
            },
            checksummed_payload: record.checksummed_payload,
            stored_checksum: record.stored_checksum,
        }
    }

    pub fn encode_companion_record_for_test(
        fields: &CompanionRecordFields,
    ) -> std::result::Result<CompanionRecordObservation, String> {
        super::prepare_companion_record_for_publication(&companion_fields_for_production(fields))
            .map(companion_observation)
    }

    pub fn decode_companion_record_for_test(
        payload: &[u8],
        checksum: [u8; 32],
    ) -> std::result::Result<CompanionRecordObservation, String> {
        super::recover_companion_record(payload, checksum).map(companion_observation)
    }

    /// Checkpoints in the destructive store replacement while the original
    /// companion guard is required to remain continuously owned.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum StoreReplacementBoundary {
        GuardAcquired,
        BeforeAtomicReplacement,
        AfterAtomicReplacement,
        ReplacementPublishedBeforeGuardRelease,
    }

    impl StoreReplacementBoundary {
        pub const ALL: [Self; 4] = [
            Self::GuardAcquired,
            Self::BeforeAtomicReplacement,
            Self::AfterAtomicReplacement,
            Self::ReplacementPublishedBeforeGuardRelease,
        ];
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum StoreReplacementEvent {
        Checkpoint(StoreReplacementBoundary),
        CompletedAfterGuardRelease,
        FinishedBeforeExpectedCheckpoint {
            expected: StoreReplacementBoundary,
        },
        UnexpectedCheckpoint {
            expected: StoreReplacementBoundary,
            actual: StoreReplacementBoundary,
        },
    }

    #[derive(Debug, Default)]
    struct StoreReplacementState {
        armed: bool,
        expected: VecDeque<StoreReplacementBoundary>,
        reached: Option<StoreReplacementBoundary>,
        released: bool,
        finished: bool,
        unexpected: Option<(StoreReplacementBoundary, StoreReplacementBoundary)>,
    }

    static WRITER_OPEN_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
    static COMPANION_EFFECTIVE_USER_SOURCE_CALLS: AtomicUsize = AtomicUsize::new(0);
    static ARMED_COMPANION_CRASH: Mutex<Option<CompanionWriteBoundary>> = Mutex::new(None);
    static COMPANION_EFFECTIVE_USER_OVERRIDE: Mutex<Option<LocalUserIdentity>> = Mutex::new(None);
    static READ_IMAGE_HYDRATION_PAUSE: AtomicBool = AtomicBool::new(false);
    static READ_IMAGE_SOURCE_EVENTS_BY_STORE: OnceLock<
        Mutex<HashMap<PathBuf, Vec<ReadImageSourceEvent>>>,
    > = OnceLock::new();

    fn read_image_source_events_by_store()
    -> &'static Mutex<HashMap<PathBuf, Vec<ReadImageSourceEvent>>> {
        READ_IMAGE_SOURCE_EVENTS_BY_STORE.get_or_init(|| Mutex::new(HashMap::new()))
    }
    static STORE_REPLACEMENT: OnceLock<(Mutex<StoreReplacementState>, Condvar)> = OnceLock::new();
    static READER_SOURCE_START_PAUSES: OnceLock<(Mutex<ReaderSourceStartPauseState>, Condvar)> =
        OnceLock::new();
    static TWO_READ_IMAGE_HYDRATIONS: OnceLock<(Mutex<TwoReadImageHydrationState>, Condvar)> =
        OnceLock::new();
    #[cfg(any(test, feature = "test-seams"))]
    static READER_SNAPSHOT_REGISTRY_SCANS: OnceLock<(
        Mutex<ReaderSnapshotRegistryScanState>,
        Condvar,
    )> = OnceLock::new();

    #[derive(Debug, Default)]
    struct TwoReadImageHydrationState {
        next_token: u64,
        active: Option<TwoReadImageHydrationRun>,
    }

    #[derive(Debug)]
    struct TwoReadImageHydrationRun {
        token: u64,
        next_participant: u64,
        started_attempts: usize,
        finished_attempts: usize,
        reached: Vec<ReadImageHydrationParticipantObservation>,
        released: BTreeSet<u64>,
        departed: BTreeSet<u64>,
        /// The store this hydration run belongs to, bound when the run is
        /// armed -- before any reader, intended or not, can reach the
        /// checkpoint. The whole estate runs in one process at default test
        /// parallelism, so an unrelated store's hydration checkpoint (a
        /// different test, running concurrently) must never be counted as
        /// this hydration run's second or third participant, however early
        /// it arrives -- it is filtered
        /// out in [`two_read_image_hydration_checkpoint_for_test`] instead.
        store: Option<PathBuf>,
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[derive(Debug, Default)]
    struct ReaderSnapshotRegistryScanState {
        #[cfg_attr(
            not(feature = "test-seams"),
            allow(
                dead_code,
                reason = "KEEP-SEAM: live only under the test-seams feature; used from durable_vector_loading.rs"
            )
        )]
        next_token: u64,
        active: BTreeMap<(u64, u64), ReaderSnapshotRegistryScanRun>,
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[derive(Debug)]
    struct ReaderSnapshotRegistryScanRun {
        token: u64,
        database_path: PathBuf,
        inspected_directory: Option<PathBuf>,
        resumed: bool,
        departed: bool,
    }

    /// The exact registry identity and directory observed by a production scan.
    #[cfg(any(test, feature = "test-seams"))]
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ReaderSnapshotRegistryScanObservation {
        pub database_path: PathBuf,
        pub physical_identity: (u64, u64),
        pub registry_directory: PathBuf,
    }

    /// History, audit and queued records a file-reader open decoded or
    /// copied, by what they are. None of it is needed to answer a query that
    /// does not ask for history, audit or queue contents.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct OpenHistoryWork {
        pub change_log_records: u64,
        pub ddl_log_records: u64,
        pub commit_index_records: u64,
        pub audit_records: u64,
        pub queued_records: u64,
        /// Stored bytes of those records the open decoded or copied.
        pub bytes: u64,
        /// Commit-index entries the open-time order diagnosis streamed past
        /// without decoding or keeping them. Not counted in `bytes`.
        pub commit_index_scanned: u64,
    }

    use super::OpenHistoryClass;

    thread_local! {
        static OPEN_HISTORY_WORK: std::cell::Cell<Option<OpenHistoryWork>> =
            const { std::cell::Cell::new(None) };
    }

    /// Run `operation` and report the history, audit and queued records it
    /// decoded or copied on this thread.
    pub fn observe_open_history_work_for_test<T>(
        operation: impl FnOnce() -> T,
    ) -> (T, OpenHistoryWork) {
        let previous = OPEN_HISTORY_WORK.with(|work| work.replace(Some(Default::default())));
        struct Restore(Option<OpenHistoryWork>);
        impl Drop for Restore {
            fn drop(&mut self) {
                OPEN_HISTORY_WORK.with(|work| work.set(self.0));
            }
        }
        let restore = Restore(previous);
        let value = operation();
        let observed = OPEN_HISTORY_WORK
            .with(|work| work.get())
            .unwrap_or_default();
        drop(restore);
        (value, observed)
    }

    pub(crate) fn note_open_history(class: OpenHistoryClass, bytes: usize) {
        OPEN_HISTORY_WORK.with(|work| {
            let Some(mut observed) = work.get() else {
                return;
            };
            match class {
                OpenHistoryClass::ChangeLog => observed.change_log_records += 1,
                OpenHistoryClass::DdlLog => observed.ddl_log_records += 1,
                OpenHistoryClass::CommitIndex => observed.commit_index_records += 1,
                OpenHistoryClass::Audit => observed.audit_records += 1,
                OpenHistoryClass::Queued => observed.queued_records += 1,
                OpenHistoryClass::CommitIndexScanned => observed.commit_index_scanned += 1,
            }
            observed.bytes = observed.bytes.saturating_add(bytes as u64);
            work.set(Some(observed));
        });
    }

    /// Where a reader registration can be paused while it acquires its
    /// registry lock.
    #[cfg(any(test, feature = "test-seams"))]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ReaderRegistrationLockPhase {
        /// The lock file's descriptor is open; nothing about it is validated.
        DescriptorOpened,
        /// The descriptor passed its trust checks and is not locked yet.
        Validated,
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[derive(Debug, Default)]
    struct ReaderRegistrationLockPauseState {
        #[cfg_attr(
            not(feature = "test-seams"),
            allow(
                dead_code,
                reason = "KEEP-SEAM: live only under the test-seams feature; used from durable_vector_loading.rs"
            )
        )]
        next_token: u64,
        active: BTreeMap<(u64, u64), ReaderRegistrationLockPauseRun>,
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[derive(Debug)]
    struct ReaderRegistrationLockPauseRun {
        token: u64,
        phase: ReaderRegistrationLockPhase,
        /// The one registering thread this pause run holds: the first to arrive.
        /// Every other registration for the store passes straight through.
        owner: Option<std::thread::ThreadId>,
        arrivals: u64,
        released: u64,
        /// Count later arrivals without pausing them.
        passthrough: bool,
    }

    #[cfg(any(test, feature = "test-seams"))]
    static READER_REGISTRATION_LOCK_PAUSES: OnceLock<(
        Mutex<ReaderRegistrationLockPauseState>,
        Condvar,
    )> = OnceLock::new();

    #[cfg(any(test, feature = "test-seams"))]
    fn reader_registration_lock_pauses()
    -> &'static (Mutex<ReaderRegistrationLockPauseState>, Condvar) {
        READER_REGISTRATION_LOCK_PAUSES.get_or_init(|| {
            (
                Mutex::new(ReaderRegistrationLockPauseState::default()),
                Condvar::new(),
            )
        })
    }

    /// Scoped controller for one reader registration paused at one phase of
    /// its registry-lock acquisition, on every attempt it makes.
    ///
    /// Dropping the controller releases the registration, so a failed
    /// assertion cannot strand a reader-open thread.
    #[cfg(any(test, feature = "test-seams"))]
    #[derive(Debug)]
    pub struct ReaderRegistrationLockPauseController {
        identity: (u64, u64),
        token: u64,
    }

    /// Pause the first reader registration to reach `phase` for this physical
    /// store, at that phase of every lock attempt it makes.
    #[cfg(all(unix, feature = "test-seams"))]
    pub fn pause_reader_registration_lock_for_test(
        database_path: &Path,
        phase: ReaderRegistrationLockPhase,
    ) -> std::io::Result<ReaderRegistrationLockPauseController> {
        use std::os::unix::fs::MetadataExt as _;

        let metadata = std::fs::metadata(database_path)?;
        let identity = (metadata.dev(), metadata.ino());
        let (state, _) = reader_registration_lock_pauses();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.active.contains_key(&identity) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "reader registration pause is already armed for this physical store",
            ));
        }
        state.next_token = state.next_token.wrapping_add(1);
        let token = state.next_token;
        state.active.insert(
            identity,
            ReaderRegistrationLockPauseRun {
                token,
                phase,
                owner: None,
                arrivals: 0,
                released: 0,
                passthrough: false,
            },
        );
        Ok(ReaderRegistrationLockPauseController { identity, token })
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn reader_registration_lock_checkpoint_for_test(
        identity: (u64, u64),
        phase: ReaderRegistrationLockPhase,
    ) {
        let (state, changed) = reader_registration_lock_pauses();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(run) = state.active.get_mut(&identity) else {
            return;
        };
        if run.phase != phase {
            return;
        }
        let current = std::thread::current().id();
        match run.owner {
            None => run.owner = Some(current),
            Some(owner) if owner != current => return,
            Some(_) => {}
        }
        run.arrivals += 1;
        if run.passthrough {
            run.released = run.arrivals;
        }
        let (token, arrival) = (run.token, run.arrivals);
        changed.notify_all();
        while state
            .active
            .get(&identity)
            .is_some_and(|run| run.token == token && run.released < arrival)
        {
            state = changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    impl ReaderRegistrationLockPauseController {
        /// Wait until the paused registration has reached its phase
        /// `attempt` times, counting from one. Returns false if it has not
        /// within `limit` -- a registration that gave up never arrives again.
        pub fn wait_for_attempt_for_test(&self, attempt: u64, limit: std::time::Duration) -> bool {
            let (state, changed) = reader_registration_lock_pauses();
            let state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let (state, _) = changed
                .wait_timeout_while(state, limit, |state| {
                    state
                        .active
                        .get(&self.identity)
                        .filter(|run| run.token == self.token)
                        .expect("reader registration pause was cancelled")
                        .arrivals
                        < attempt
                })
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state
                .active
                .get(&self.identity)
                .filter(|run| run.token == self.token)
                .expect("reader registration pause was cancelled")
                .arrivals
                >= attempt
        }

        /// Let the paused registration continue from every arrival so far.
        pub fn resume_for_test(&self) {
            let (state, changed) = reader_registration_lock_pauses();
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(run) = state
                .active
                .get_mut(&self.identity)
                .filter(|run| run.token == self.token)
            {
                run.released = run.arrivals;
                changed.notify_all();
            }
        }

        /// Release the paused registration and let every later attempt pass
        /// straight through, still counted.
        pub fn stop_pausing_for_test(&self) {
            let (state, changed) = reader_registration_lock_pauses();
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(run) = state
                .active
                .get_mut(&self.identity)
                .filter(|run| run.token == self.token)
            {
                run.passthrough = true;
                run.released = run.arrivals;
                changed.notify_all();
            }
        }

        /// How many times the paused registration has reached its phase.
        pub fn attempts_for_test(&self) -> u64 {
            let (state, _) = reader_registration_lock_pauses();
            let state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state
                .active
                .get(&self.identity)
                .filter(|run| run.token == self.token)
                .map_or(0, |run| run.arrivals)
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    impl Drop for ReaderRegistrationLockPauseController {
        fn drop(&mut self) {
            let (state, changed) = reader_registration_lock_pauses();
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.active.get(&self.identity).map(|run| run.token) == Some(self.token) {
                state.active.remove(&self.identity);
                changed.notify_all();
            }
        }
    }

    /// Scoped controller for one physical reader-snapshot registry scan.
    ///
    /// Dropping an armed controller resumes its matching scan. This keeps a
    /// failed test assertion from stranding a production writer-open thread.
    #[cfg(any(test, feature = "test-seams"))]
    #[derive(Debug)]
    pub struct ReaderSnapshotRegistryScanController {
        identity: (u64, u64),
        token: u64,
        finished: bool,
    }

    fn two_read_image_hydrations() -> &'static (Mutex<TwoReadImageHydrationState>, Condvar) {
        TWO_READ_IMAGE_HYDRATIONS.get_or_init(|| {
            (
                Mutex::new(TwoReadImageHydrationState::default()),
                Condvar::new(),
            )
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    fn reader_snapshot_registry_scans() -> &'static (Mutex<ReaderSnapshotRegistryScanState>, Condvar)
    {
        READER_SNAPSHOT_REGISTRY_SCANS.get_or_init(|| {
            (
                Mutex::new(ReaderSnapshotRegistryScanState::default()),
                Condvar::new(),
            )
        })
    }

    fn store_replacement() -> &'static (Mutex<StoreReplacementState>, Condvar) {
        STORE_REPLACEMENT
            .get_or_init(|| (Mutex::new(StoreReplacementState::default()), Condvar::new()))
    }

    /// Readers stopped at the start of the read-image source, inside the
    /// load-time hold and before any registration is published.
    ///
    /// A direct reader holds the store from before the committed file is
    /// opened until its registration is published and its authenticated root
    /// is in hand; that span is the whole of the window a would-be writer has
    /// to wait out. This stops the reading thread at the FIRST production
    /// point inside it -- source start, under the load-time hold, before the
    /// registration is published -- and lets the proof decide when it may
    /// carry on: the same shape as [`store_replacement_checkpoint_for_test`],
    /// per reader instead of per boundary. Nothing is manufactured: the reader
    /// is a real one, holding the store the way any reader does.
    ///
    /// Every list here is keyed by the store it belongs to. The whole estate
    /// runs in one process at default parallelism, so a single process-wide
    /// target path and a single ticket list would let one proof's wait consume
    /// another proof's reader and one proof's release let another proof's
    /// reader go -- two unrelated reader fixtures interfering with each other. A
    /// proof arms exactly one store and can only ever see and free the readers
    /// of that store.
    #[derive(Default)]
    struct ReaderSourceStartPauseState {
        next_ticket: u64,
        stores: HashMap<(PathBuf, ReaderPausePoint), PausedStoreReaders>,
    }

    #[derive(Default)]
    struct PausedStoreReaders {
        /// Tickets of this store's readers stopped right now.
        paused: Vec<u64>,
        /// Tickets this store's proof has already been handed, so the next ask
        /// answers with the next reader rather than one it already holds.
        handed: std::collections::BTreeSet<u64>,
        /// Tickets whose reader has been let go.
        let_go: std::collections::BTreeSet<u64>,
        /// Threads already stopped here once: a reader is stopped at a point
        /// on its first arrival only, so a route that asks again after its
        /// release is not stopped a second time.
        stopped_threads: std::collections::HashSet<std::thread::ThreadId>,
    }

    /// One proof's owned pause over ONE store's readers.
    ///
    /// Arming, waiting and releasing all go through this token, so a ticket
    /// belongs to the store that issued it and to nobody else. Dropping the
    /// token stops pausing that store's readers and frees every reader still
    /// stopped there, so a journey that came apart leaves no reading thread
    /// parked in a store.
    pub struct ReaderSourceStartPause {
        store: PathBuf,
        point: ReaderPausePoint,
    }

    /// Where a direct reader of an armed store is stopped.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum ReaderPausePoint {
        /// Where its read-image source starts: inside admission, holding the
        /// data lock, before its snapshot registration is exact.
        SourceStart,
        /// Just before it decodes its image: the heavy loading.
        BeforeDecode,
        /// Admission: it has asked whether a writer claimed the physical
        /// store and holds nothing yet. Armed for the physical store, so a
        /// reader through any name of it pauses.
        AfterPrecheck,
        /// Admission: it holds the reader hold shared and has given the gate
        /// back, and has not yet asked for the data lock. Physical, as above.
        AfterGate,
        /// A reader has finished its probe of whether a writer is live on this
        /// pathname, before deciding its route. Pathname-keyed.
        AfterProbe,
        /// A reader has published its provisional registration and holds
        /// neither the reader hold nor the data lock. Pathname-keyed.
        AfterProvisional,
        /// A reader has released the data lock, then the reader hold, and has
        /// not yet dropped its registration guard. Pathname-keyed.
        AfterDRelease,
        /// A reader is about to open or create its registry lock. Physical:
        /// armed by any name of the store.
        BeforeRegistration,
        /// The runtime root is held alive, before this store creates its
        /// registry descriptor or authoritative fence. Physical, as above.
        AfterRuntimeRootLifetimeHold,
        /// A real departing reader owns its store's registry exclusion,
        /// before removing its record. Physical, as above.
        LeavingWithRegistryExclusion,
    }

    impl ReaderPausePoint {
        fn is_physical(self) -> bool {
            matches!(
                self,
                Self::AfterPrecheck
                    | Self::AfterGate
                    | Self::BeforeRegistration
                    | Self::AfterRuntimeRootLifetimeHold
                    | Self::LeavingWithRegistryExclusion
            )
        }
    }

    /// The key a physical pause point is armed and met under: the inode, not
    /// a pathname, so every name of one store meets the same pause.
    fn physical_pause_key(identity: (u64, u64)) -> PathBuf {
        PathBuf::from(format!("\0physical-store/{}/{}", identity.0, identity.1))
    }

    /// The physical key of the store a pathname names, for arming.
    fn physical_pause_key_for(store_path: &Path) -> PathBuf {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            match std::fs::metadata(store_path) {
                Ok(metadata) => physical_pause_key((metadata.dev(), metadata.ino())),
                Err(_) => store_path.to_path_buf(),
            }
        }
        #[cfg(not(unix))]
        {
            store_path.to_path_buf()
        }
    }

    /// A reader inside physical admission reaches `point`.
    pub(crate) fn pause_reader_at_physical(identity: (u64, u64), point: ReaderPausePoint) {
        pause_reader_at(&physical_pause_key(identity), point);
    }

    fn reader_source_start_pauses() -> &'static (Mutex<ReaderSourceStartPauseState>, Condvar) {
        READER_SOURCE_START_PAUSES.get_or_init(|| {
            (
                Mutex::new(ReaderSourceStartPauseState::default()),
                Condvar::new(),
            )
        })
    }

    /// Stop every direct reader of THIS store at its read-image source start,
    /// inside the load-time hold and before registration is published, until
    /// the returned token lets it go.
    pub fn pause_readers_at_source_start_for_test(store_path: &Path) -> ReaderSourceStartPause {
        pause_readers_at_for_test(store_path, ReaderPausePoint::SourceStart)
    }

    /// Stop every direct reader of THIS store at `point` until the returned
    /// token lets it go.
    pub fn pause_readers_at_for_test(
        store_path: &Path,
        point: ReaderPausePoint,
    ) -> ReaderSourceStartPause {
        let keyed;
        let store_path = if point.is_physical() {
            keyed = physical_pause_key_for(store_path);
            keyed.as_path()
        } else {
            store_path
        };
        let (state, changed) = reader_source_start_pauses();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state
            .stores
            .entry((store_path.to_path_buf(), point))
            .or_insert_with(PausedStoreReaders::default);
        changed.notify_all();
        ReaderSourceStartPause {
            store: store_path.to_path_buf(),
            point,
        }
    }

    impl ReaderSourceStartPause {
        /// The store this token owns the paused readers of.
        pub fn store(&self) -> &Path {
            &self.store
        }

        /// Answer with the ticket of one of THIS store's stopped readers that
        /// this token has not been handed yet, waiting up to `within` for one
        /// to arrive. Another store's reader is never answered with, however
        /// many of them are stopped at the same moment.
        pub fn wait_for_paused_reader(&self, within: std::time::Duration) -> Option<u64> {
            let (state, changed) = reader_source_start_pauses();
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let deadline = std::time::Instant::now() + within;
            loop {
                if let Some(readers) = state.stores.get_mut(&(self.store.clone(), self.point))
                    && let Some(ticket) = readers
                        .paused
                        .iter()
                        .copied()
                        .find(|ticket| !readers.handed.contains(ticket))
                {
                    readers.handed.insert(ticket);
                    return Some(ticket);
                }
                let left = deadline.saturating_duration_since(std::time::Instant::now());
                if left.is_zero() {
                    return None;
                }
                let (next, _) = changed
                    .wait_timeout(state, left)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                state = next;
            }
        }

        /// Let one of THIS store's stopped readers carry on. A ticket issued
        /// for another store names nothing here and frees nobody.
        pub fn release(&self, ticket: u64) {
            let (state, changed) = reader_source_start_pauses();
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(readers) = state.stores.get_mut(&(self.store.clone(), self.point))
                && readers.paused.contains(&ticket)
            {
                readers.let_go.insert(ticket);
            }
            changed.notify_all();
        }
    }

    impl Drop for ReaderSourceStartPause {
        fn drop(&mut self) {
            let (state, changed) = reader_source_start_pauses();
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // Removing the entry both stops pausing later readers of this
            // store and frees every reader still stopped in it: a reader whose
            // store is no longer armed is no longer held by anybody.
            state.stores.remove(&(self.store.clone(), self.point));
            changed.notify_all();
        }
    }

    fn pause_reader_at_source_start(store_path: &Path) {
        pause_reader_at(store_path, ReaderPausePoint::SourceStart);
    }

    pub(super) fn pause_reader_at_probe(store_path: &Path) {
        pause_reader_at(store_path, ReaderPausePoint::AfterProbe);
    }

    pub(crate) fn pause_reader_at_point(store_path: &Path, point: ReaderPausePoint) {
        pause_reader_at(store_path, point);
    }

    static COMPLETE_HYDRATION_FORCED: OnceLock<Mutex<std::collections::HashSet<PathBuf>>> =
        OnceLock::new();

    /// While held, direct readers of THIS store decode the complete image
    /// (the established fallback) instead of loading rows on demand.
    pub struct CompleteHydrationGuard {
        store: PathBuf,
    }

    pub fn force_complete_hydration_for_test(store_path: &Path) -> CompleteHydrationGuard {
        COMPLETE_HYDRATION_FORCED
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(store_path.to_path_buf());
        CompleteHydrationGuard {
            store: store_path.to_path_buf(),
        }
    }

    impl Drop for CompleteHydrationGuard {
        fn drop(&mut self) {
            COMPLETE_HYDRATION_FORCED
                .get_or_init(Default::default)
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&self.store);
        }
    }

    pub(crate) fn complete_hydration_forced_for_test(store_path: &Path) -> bool {
        COMPLETE_HYDRATION_FORCED
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(store_path)
    }

    thread_local! {
        static LEAVE_PURGE_PENDING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }

    /// While held, the next authoritative PURGE run on this thread stops after
    /// its fence publication is Pending -- as if the writer died between
    /// publishing and committing it -- and returns its error. Writer recovery
    /// decides the Pending publication later, as it does after a crash.
    pub struct PurgePendingGuard(());

    pub fn leave_next_purge_pending_for_test() -> PurgePendingGuard {
        LEAVE_PURGE_PENDING.with(|cell| cell.set(true));
        PurgePendingGuard(())
    }

    impl Drop for PurgePendingGuard {
        fn drop(&mut self) {
            LEAVE_PURGE_PENDING.with(|cell| cell.set(false));
        }
    }

    pub(crate) fn take_purge_pending_for_test() -> bool {
        LEAVE_PURGE_PENDING.with(|cell| cell.replace(false))
    }

    /// A fault injected on the next direct reader of one store, on its
    /// registration write path under the data lock.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum RegistrationFault {
        /// The second publication root is left provisional.
        SecondRootStaysProvisional,
        /// The exact record is written torn.
        TornRecord,
        /// The record's sync fails.
        FdatasyncFails,
        /// The fence re-baseline's try fails.
        FenceTryFails,
        /// The root capture refuses.
        CaptureRefuses,
    }

    static REGISTRATION_FAULTS: OnceLock<Mutex<HashMap<PathBuf, RegistrationFault>>> =
        OnceLock::new();

    pub struct RegistrationFaultGuard {
        store: PathBuf,
    }

    pub fn inject_registration_fault_for_test(
        store_path: &Path,
        fault: RegistrationFault,
    ) -> RegistrationFaultGuard {
        REGISTRATION_FAULTS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(store_path.to_path_buf(), fault);
        RegistrationFaultGuard {
            store: store_path.to_path_buf(),
        }
    }

    impl Drop for RegistrationFaultGuard {
        fn drop(&mut self) {
            REGISTRATION_FAULTS
                .get_or_init(Default::default)
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&self.store);
        }
    }

    /// The fault armed for this store's direct readers while its guard lives.
    pub(crate) fn registration_fault_for_test(store_path: &Path) -> Option<RegistrationFault> {
        REGISTRATION_FAULTS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(store_path)
            .copied()
    }

    /// How registration of one store's readers went: descriptor opens, L0
    /// takes, and departure retries.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct RegistrationAttemptCounts {
        pub opens: u64,
        pub l0_takes: u64,
        pub retries: u64,
    }

    static REGISTRATION_ATTEMPTS: OnceLock<Mutex<HashMap<PathBuf, RegistrationAttemptCounts>>> =
        OnceLock::new();

    /// Keyed by the physical store, so every name of it shares one count.
    pub fn registration_attempt_counts_for_test(store_path: &Path) -> RegistrationAttemptCounts {
        REGISTRATION_ATTEMPTS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&physical_pause_key_for(store_path))
            .copied()
            .unwrap_or_default()
    }

    pub(crate) fn count_registration_attempt_for_test(
        identity: (u64, u64),
        change: impl FnOnce(&mut RegistrationAttemptCounts),
    ) {
        change(
            REGISTRATION_ATTEMPTS
                .get_or_init(Default::default)
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .entry(physical_pause_key(identity))
                .or_default(),
        );
    }

    pub(super) fn pause_reader_before_decode(store_path: &Path) {
        pause_reader_at(store_path, ReaderPausePoint::BeforeDecode);
    }

    fn pause_reader_at(store_path: &Path, point: ReaderPausePoint) {
        let (state, changed) = reader_source_start_pauses();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let store = (store_path.to_path_buf(), point);
        let Some(readers) = state.stores.get_mut(&store) else {
            return;
        };
        if !readers.stopped_threads.insert(std::thread::current().id()) {
            return;
        }
        let ticket = state.next_ticket;
        state.next_ticket = ticket.saturating_add(1);
        state
            .stores
            .get_mut(&store)
            .expect("the armed store was just looked up")
            .paused
            .push(ticket);
        changed.notify_all();
        loop {
            let released = state
                .stores
                .get_mut(&store)
                .is_none_or(|readers| readers.let_go.remove(&ticket));
            if released {
                break;
            }
            state = changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        if let Some(readers) = state.stores.get_mut(&store) {
            readers.paused.retain(|paused| *paused != ticket);
        }
        changed.notify_all();
    }

    /// What one writable open of an armed store did about the readers it met.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum WriterAdmissionEvent {
        /// It is waiting for the store to be handed to it.
        Waiting,
        /// It was refused because somebody holds the store.
        Refused,
        /// It owns the store's data lock.
        Acquired,
    }

    type WriterAdmissionEvents = HashMap<PathBuf, std::collections::VecDeque<WriterAdmissionEvent>>;

    static WRITER_ADMISSION_EVENTS: OnceLock<(Mutex<WriterAdmissionEvents>, Condvar)> =
        OnceLock::new();

    fn writer_admission_events() -> &'static (Mutex<WriterAdmissionEvents>, Condvar) {
        WRITER_ADMISSION_EVENTS.get_or_init(|| (Mutex::new(HashMap::new()), Condvar::new()))
    }

    /// One proof's record of what writable opens of ONE store did, in order.
    pub struct WriterAdmissionWatch {
        store: PathBuf,
    }

    /// Start recording writable-open admission events for THIS store.
    pub fn watch_writer_admission_for_test(store_path: &Path) -> WriterAdmissionWatch {
        let (events, changed) = writer_admission_events();
        events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(store_path.to_path_buf(), std::collections::VecDeque::new());
        changed.notify_all();
        WriterAdmissionWatch {
            store: store_path.to_path_buf(),
        }
    }

    impl WriterAdmissionWatch {
        /// The next event this store's writable opens recorded, waiting up to
        /// `within` for one. The bound is a failsafe, not a latency claim.
        pub fn next(&self, within: std::time::Duration) -> Option<WriterAdmissionEvent> {
            let (events, changed) = writer_admission_events();
            let mut events = events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let deadline = std::time::Instant::now() + within;
            loop {
                if let Some(event) = events
                    .get_mut(&self.store)
                    .and_then(std::collections::VecDeque::pop_front)
                {
                    return Some(event);
                }
                let left = deadline.saturating_duration_since(std::time::Instant::now());
                if left.is_zero() {
                    return None;
                }
                let (next, _) = changed
                    .wait_timeout(events, left)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                events = next;
            }
        }
    }

    impl Drop for WriterAdmissionWatch {
        fn drop(&mut self) {
            let (events, changed) = writer_admission_events();
            events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&self.store);
            changed.notify_all();
        }
    }

    pub(crate) fn note_writer_admission_for_test(store_path: &Path, event: WriterAdmissionEvent) {
        let (events, changed) = writer_admission_events();
        let mut events = events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(recorded) = events.get_mut(store_path) {
            recorded.push_back(event);
            changed.notify_all();
        }
    }

    /// How the admission bytes of ONE physical store have been used, by
    /// every opener in this process whatever name it used.
    pub fn physical_admission_counts_for_test(store_path: &Path) -> PhysicalAdmissionCounts {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            let Ok(metadata) = std::fs::metadata(store_path) else {
                return PhysicalAdmissionCounts::default();
            };
            super::physical_admission::counts_for_test((metadata.dev(), metadata.ino()))
        }
        #[cfg(not(unix))]
        {
            let _ = store_path;
            PhysicalAdmissionCounts::default()
        }
    }

    /// Wait until this physical store has refused at least `at_least` reader
    /// range requests in total, up to `within` (a failsafe, not a latency).
    pub fn wait_for_hold_range_refusals_for_test(
        store_path: &Path,
        at_least: u64,
        within: std::time::Duration,
    ) -> bool {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            let Ok(metadata) = std::fs::metadata(store_path) else {
                return false;
            };
            super::physical_admission::wait_for_counts_for_test(
                (metadata.dev(), metadata.ino()),
                |counts| counts.hold_range_refusals >= at_least,
                within,
            )
        }
        #[cfg(not(unix))]
        {
            let _ = (store_path, at_least, within);
            false
        }
    }

    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct PhysicalAdmissionCounts {
        /// Shared data-lock grants: a reader or cleaner entering admission.
        pub data_shared_grants: u64,
        /// Requests for the reader hold and gate together.
        pub gate_requests: u64,
        /// Those requests that were granted.
        pub hold_shared_grants: u64,
        /// Those requests that were refused (a writer holds the gate).
        pub hold_range_refusals: u64,
        /// Blocking acquisitions a writer made of the gate, the reader hold
        /// or the data lock: each is made once.
        pub writer_hold_waits: u64,
    }

    /// Hold the next waiting writer of THIS store after its wait returned and
    /// its helper (if any) was reaped, before its final cancellation and
    /// deadline check.
    pub struct WriterWaitHold {
        store: PathBuf,
    }

    #[derive(Default)]
    struct WriterWaitHoldState {
        armed: bool,
        reached: bool,
        released: bool,
    }

    static WRITER_WAIT_HOLDS: OnceLock<(Mutex<HashMap<PathBuf, WriterWaitHoldState>>, Condvar)> =
        OnceLock::new();

    fn writer_wait_holds() -> &'static (Mutex<HashMap<PathBuf, WriterWaitHoldState>>, Condvar) {
        WRITER_WAIT_HOLDS.get_or_init(|| (Mutex::new(HashMap::new()), Condvar::new()))
    }

    pub fn hold_writer_wait_before_linearization_for_test(store_path: &Path) -> WriterWaitHold {
        let (holds, changed) = writer_wait_holds();
        holds
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                store_path.to_path_buf(),
                WriterWaitHoldState {
                    armed: true,
                    ..Default::default()
                },
            );
        changed.notify_all();
        WriterWaitHold {
            store: store_path.to_path_buf(),
        }
    }

    impl WriterWaitHold {
        /// Whether the writer reached the hold within `within` (a failsafe).
        pub fn wait_reached(&self, within: std::time::Duration) -> bool {
            let (holds, changed) = writer_wait_holds();
            let mut holds = holds
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let deadline = std::time::Instant::now() + within;
            loop {
                if holds.get(&self.store).is_some_and(|hold| hold.reached) {
                    return true;
                }
                let left = deadline.saturating_duration_since(std::time::Instant::now());
                if left.is_zero() {
                    return false;
                }
                holds = changed
                    .wait_timeout(holds, left)
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .0;
            }
        }

        pub fn release(&self) {
            let (holds, changed) = writer_wait_holds();
            if let Some(hold) = holds
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get_mut(&self.store)
            {
                hold.released = true;
            }
            changed.notify_all();
        }
    }

    impl Drop for WriterWaitHold {
        fn drop(&mut self) {
            let (holds, changed) = writer_wait_holds();
            holds
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&self.store);
            changed.notify_all();
        }
    }

    /// The waiting writer reaches its linearization point.
    pub(crate) fn writer_wait_linearization_point_for_test(store_path: &Path) {
        let (holds, changed) = writer_wait_holds();
        let mut holds = holds
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match holds.get_mut(store_path) {
            Some(hold) if hold.armed => {
                hold.armed = false;
                hold.reached = true;
            }
            _ => return,
        }
        changed.notify_all();
        while holds
            .get(store_path)
            .is_some_and(|hold| hold.reached && !hold.released)
        {
            holds = changed
                .wait(holds)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    /// A fault injected into a writer's cancellable wait of one store.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum WaitFault {
        /// Killing the helper fails and its death cannot be confirmed.
        KillFailsAndHelperUnconfirmed,
        /// The per-thread preflight of the helper's kill-and-reap fails.
        PreflightFails,
    }

    static WAIT_FAULTS: OnceLock<Mutex<HashMap<PathBuf, WaitFault>>> = OnceLock::new();

    pub struct WaitFaultGuard {
        store: PathBuf,
    }

    /// Whether a new description of the store could take its data lock
    /// exclusive right now -- the lock a starting reader holds through its
    /// admission. Taken and given back at once.
    #[cfg(unix)]
    pub fn try_store_data_lock_for_test(store: &Path) -> bool {
        use super::physical_admission::{ByteLock, Granted, release_data, try_data};
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(store)
            .expect("open the store's data file");
        match try_data(&file, ByteLock::Exclusive).expect("ask for the data lock") {
            Granted::Yes => {
                let _ = release_data(&file);
                true
            }
            Granted::HeldByAnother => false,
        }
    }

    #[derive(Default)]
    struct ClassificationScanPauseState {
        state: std::sync::Mutex<(bool, bool)>,
        changed: std::sync::Condvar,
    }

    fn classification_scan_pause()
    -> &'static std::sync::Mutex<Option<std::sync::Arc<ClassificationScanPauseState>>> {
        static PAUSE: std::sync::Mutex<Option<std::sync::Arc<ClassificationScanPauseState>>> =
            std::sync::Mutex::new(None);
        &PAUSE
    }

    /// Stops the next format check in this process at the start of its table
    /// scan -- after its admission is over -- until let go or dropped.
    pub struct ClassificationScanPause {
        state: std::sync::Arc<ClassificationScanPauseState>,
    }

    impl ClassificationScanPause {
        /// Whether a format check reached the pause before the watchdog.
        pub fn wait_until_paused(&self, watchdog: std::time::Duration) -> bool {
            let guard = self
                .state
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let (guard, _) = self
                .state
                .changed
                .wait_timeout_while(guard, watchdog, |(reached, _)| !*reached)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.0
        }

        pub fn let_go(&self) {
            let mut guard = self
                .state
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.1 = true;
            self.state.changed.notify_all();
        }
    }

    impl Drop for ClassificationScanPause {
        fn drop(&mut self) {
            self.let_go();
        }
    }

    pub fn pause_next_classification_scan_for_test() -> ClassificationScanPause {
        let state = std::sync::Arc::new(ClassificationScanPauseState::default());
        *classification_scan_pause()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(std::sync::Arc::clone(&state));
        ClassificationScanPause { state }
    }

    pub(super) fn pause_classification_scan() {
        let Some(state) = classification_scan_pause()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        else {
            return;
        };
        let mut guard = state
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.0 = true;
        state.changed.notify_all();
        while !guard.1 {
            guard = state
                .changed
                .wait(guard)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    pub fn inject_wait_fault_for_test(store_path: &Path, fault: WaitFault) -> WaitFaultGuard {
        WAIT_FAULTS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(store_path.to_path_buf(), fault);
        WaitFaultGuard {
            store: store_path.to_path_buf(),
        }
    }

    impl Drop for WaitFaultGuard {
        fn drop(&mut self) {
            WAIT_FAULTS
                .get_or_init(Default::default)
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&self.store);
        }
    }

    pub(crate) fn wait_fault_for_test(store_path: &Path) -> Option<WaitFault> {
        WAIT_FAULTS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(store_path)
            .copied()
    }

    pub fn reset_writer_open_attempts_for_test() {
        WRITER_OPEN_ATTEMPTS.store(0, Ordering::SeqCst);
    }

    pub fn writer_open_attempts_for_test() -> usize {
        WRITER_OPEN_ATTEMPTS.load(Ordering::SeqCst)
    }

    pub fn set_companion_effective_user_identity_for_test(identity: LocalUserIdentity) {
        *COMPANION_EFFECTIVE_USER_OVERRIDE
            .lock()
            .expect("companion effective-user override mutex poisoned") = Some(identity);
    }

    pub fn clear_companion_effective_user_identity_for_test() {
        *COMPANION_EFFECTIVE_USER_OVERRIDE
            .lock()
            .expect("companion effective-user override mutex poisoned") = None;
    }

    pub fn companion_effective_user_identity_for_test() -> LocalUserIdentity {
        super::companion_effective_user_identity()
    }

    pub fn reset_companion_effective_user_source_calls_for_test() {
        COMPANION_EFFECTIVE_USER_SOURCE_CALLS.store(0, Ordering::SeqCst);
    }

    pub fn companion_effective_user_source_calls_for_test() -> usize {
        COMPANION_EFFECTIVE_USER_SOURCE_CALLS.load(Ordering::SeqCst)
    }

    pub(super) fn note_companion_effective_user_source_call_for_test() {
        COMPANION_EFFECTIVE_USER_SOURCE_CALLS.fetch_add(1, Ordering::SeqCst);
    }

    pub(super) fn companion_effective_user_override_for_test() -> Option<LocalUserIdentity> {
        *COMPANION_EFFECTIVE_USER_OVERRIDE
            .lock()
            .expect("companion effective-user override mutex poisoned")
    }

    pub(crate) fn note_writer_open_attempt_for_test() {
        WRITER_OPEN_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
    }

    pub fn arm_companion_crash_for_test(boundary: CompanionWriteBoundary) {
        *ARMED_COMPANION_CRASH
            .lock()
            .expect("companion crash seam mutex poisoned") = Some(boundary);
    }

    pub(super) fn companion_write_checkpoint(boundary: CompanionWriteBoundary) {
        let armed = ARMED_COMPANION_CRASH
            .lock()
            .expect("companion crash seam mutex poisoned")
            .take();
        if armed == Some(boundary) {
            let stderr = std::io::stderr();
            let mut stderr = stderr.lock();
            writeln!(stderr, "COMPANION_CRASH_AFTER={boundary:?}")
                .expect("write companion crash boundary marker");
            stderr
                .flush()
                .expect("flush companion crash boundary marker");
            std::process::abort();
        }
        if let Some(armed) = armed {
            *ARMED_COMPANION_CRASH
                .lock()
                .expect("companion crash seam mutex poisoned") = Some(armed);
        }
    }

    fn encode_hex(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0f) as usize] as char);
        }
        encoded
    }

    pub(super) fn companion_update_planned_for_test(record: &super::EncodedCompanionRecord) {
        if ARMED_COMPANION_CRASH
            .lock()
            .expect("companion crash seam mutex poisoned")
            .is_none()
        {
            return;
        }
        let stderr = std::io::stderr();
        let mut stderr = stderr.lock();
        writeln!(
            stderr,
            "COMPANION_PLANNED_RECORD={}:{}",
            encode_hex(&record.checksummed_payload),
            encode_hex(&record.stored_checksum)
        )
        .expect("write planned companion record marker");
        stderr
            .flush()
            .expect("flush planned companion record marker");
    }

    pub fn arm_store_replacement_sequence_for_test() {
        let (state, _) = store_replacement();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(!state.armed, "store replacement sequence is already armed");
        *state = StoreReplacementState {
            armed: true,
            expected: StoreReplacementBoundary::ALL.into_iter().collect(),
            reached: None,
            released: false,
            finished: false,
            unexpected: None,
        };
    }

    pub fn next_store_replacement_event_for_test() -> StoreReplacementEvent {
        let (state, changed) = store_replacement();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        loop {
            if let Some((expected, actual)) = state.unexpected.take() {
                *state = StoreReplacementState::default();
                changed.notify_all();
                return StoreReplacementEvent::UnexpectedCheckpoint { expected, actual };
            }
            if let Some(boundary) = state.reached {
                return StoreReplacementEvent::Checkpoint(boundary);
            }
            if state.finished {
                let event = match state.expected.front().copied() {
                    Some(expected) => {
                        StoreReplacementEvent::FinishedBeforeExpectedCheckpoint { expected }
                    }
                    None => StoreReplacementEvent::CompletedAfterGuardRelease,
                };
                *state = StoreReplacementState::default();
                changed.notify_all();
                return event;
            }
            state = changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    pub fn release_store_replacement_checkpoint_for_test() {
        let (state, changed) = store_replacement();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let reached = state
            .reached
            .take()
            .expect("store replacement checkpoint was not reached");
        assert_eq!(state.expected.pop_front(), Some(reached));
        state.released = true;
        changed.notify_all();
    }

    pub fn finish_store_replacement_sequence_for_test() {
        let (state, changed) = store_replacement();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.armed {
            state.finished = true;
            changed.notify_all();
        }
    }

    pub fn cancel_store_replacement_sequence_for_test() {
        let (state, changed) = store_replacement();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *state = StoreReplacementState::default();
        changed.notify_all();
    }

    #[cfg(any(test, feature = "test-seams"))]
    impl ReaderSnapshotRegistryScanController {
        /// Wait for the matching production scan to acquire its registry
        /// directory descriptor, before it opens the registry lock.
        pub fn wait_until_inspected_for_test(&self) -> ReaderSnapshotRegistryScanObservation {
            let (state, changed) = reader_snapshot_registry_scans();
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            loop {
                let run = state
                    .active
                    .get(&self.identity)
                    .filter(|run| run.token == self.token)
                    .expect("reader snapshot registry scan controller was cancelled");
                if let Some(registry_directory) = run.inspected_directory.clone() {
                    return ReaderSnapshotRegistryScanObservation {
                        database_path: run.database_path.clone(),
                        physical_identity: self.identity,
                        registry_directory,
                    };
                }
                state = changed
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }

        /// Permit the matching production scan to open the registry lock.
        pub fn resume_for_test(&self) {
            let (state, changed) = reader_snapshot_registry_scans();
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let run = state
                .active
                .get_mut(&self.identity)
                .filter(|run| run.token == self.token)
                .expect("reader snapshot registry scan controller was cancelled");
            assert!(
                run.inspected_directory.is_some(),
                "cannot resume a reader snapshot registry scan before inspection"
            );
            assert!(
                !run.resumed,
                "reader snapshot registry scan was resumed twice"
            );
            run.resumed = true;
            changed.notify_all();
        }

        /// Verify the matching scan passed its real checkpoint.
        ///
        /// The checkpoint sits before the registry lock opens, so this does
        /// not assert that the production scan completed after that boundary.
        pub fn finish_for_test(mut self) {
            let (state, changed) = reader_snapshot_registry_scans();
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            loop {
                let run = state
                    .active
                    .get(&self.identity)
                    .filter(|run| run.token == self.token)
                    .expect("reader snapshot registry scan controller was cancelled");
                assert!(
                    run.resumed,
                    "reader snapshot registry scan finished before it was resumed"
                );
                if run.departed {
                    state.active.remove(&self.identity);
                    changed.notify_all();
                    self.finished = true;
                    return;
                }
                state = changed
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    impl Drop for ReaderSnapshotRegistryScanController {
        fn drop(&mut self) {
            if self.finished {
                return;
            }
            let (state, changed) = reader_snapshot_registry_scans();
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.active.get(&self.identity).map(|run| run.token) == Some(self.token) {
                state.active.remove(&self.identity);
                changed.notify_all();
            }
        }
    }

    /// Pause one physical registry scan after it acquires its trusted registry
    /// directory descriptor and before it opens the per-store registry lock.
    #[cfg(all(unix, feature = "test-seams"))]
    pub fn pause_reader_snapshot_registry_scan_for_test(
        database_path: &Path,
    ) -> std::io::Result<ReaderSnapshotRegistryScanController> {
        use std::os::unix::fs::MetadataExt as _;

        let metadata = std::fs::metadata(database_path)?;
        let identity = (metadata.dev(), metadata.ino());
        let (state, _) = reader_snapshot_registry_scans();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.active.contains_key(&identity) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "reader snapshot registry scan controller is already armed for this physical store",
            ));
        }
        state.next_token = state.next_token.wrapping_add(1);
        let token = state.next_token;
        state.active.insert(
            identity,
            ReaderSnapshotRegistryScanRun {
                token,
                database_path: database_path.to_path_buf(),
                inspected_directory: None,
                resumed: false,
                departed: false,
            },
        );
        Ok(ReaderSnapshotRegistryScanController {
            identity,
            token,
            finished: false,
        })
    }

    #[cfg(all(not(unix), feature = "test-seams"))]
    pub fn pause_reader_snapshot_registry_scan_for_test(
        _database_path: &Path,
    ) -> std::io::Result<ReaderSnapshotRegistryScanController> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "reader snapshot registries require unix physical identities",
        ))
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn reader_snapshot_registry_directory_inspected_for_test(
        identity: (u64, u64),
        directory: &Path,
    ) {
        let (state, changed) = reader_snapshot_registry_scans();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(run) = state.active.get_mut(&identity) else {
            return;
        };
        // A controller claims one scan only. A second or resumed scan for the
        // same physical registry must continue without altering the claimed
        // scan or waiting on its controller.
        if run.inspected_directory.is_some() {
            return;
        }
        run.inspected_directory = Some(directory.to_path_buf());
        let token = run.token;
        changed.notify_all();
        while state
            .active
            .get(&identity)
            .is_some_and(|run| run.token == token && !run.resumed)
        {
            state = changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        if let Some(run) = state
            .active
            .get_mut(&identity)
            .filter(|run| run.token == token)
        {
            run.departed = true;
            changed.notify_all();
        }
    }

    pub(crate) fn store_replacement_checkpoint_for_test(boundary: StoreReplacementBoundary) {
        let (state, changed) = store_replacement();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !state.armed {
            return;
        }
        let Some(expected) = state.expected.front().copied() else {
            return;
        };
        if expected != boundary {
            state.unexpected = Some((expected, boundary));
            changed.notify_all();
            return;
        }
        state.reached = Some(boundary);
        state.released = false;
        changed.notify_all();
        while state.armed && !state.released {
            state = changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        state.released = false;
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct ReadImageObservation {
        pub vectors: std::collections::BTreeMap<(String, i64), Vec<f32>>,
        pub released_breadcrumb_path: Option<PathBuf>,
        pub source_accesses: u64,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum ReadImageSourceEvent {
        Started { breadcrumb_path: Option<PathBuf> },
        SourceAccess,
        SourceHandlesDropped,
        Released { breadcrumb_path: Option<PathBuf> },
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub fn take_verified_ordered_run_capacity_displacements_for_test() -> usize {
        super::verified_ordered_run_test_seams::take_capacity_displacements_for_test()
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub fn with_verified_ordered_window_bytes_for_test<T>(
        bytes: usize,
        action: impl FnOnce() -> T,
    ) -> T {
        super::verified_ordered_run_test_seams::with_window_bytes(bytes, action)
    }

    /// One store's read-image source history. Every observer names the store
    /// it watches, so a reader recorded on any thread reaches the observer for
    /// that store and only that one, and clearing a history never reaches past
    /// the store whose observer cleared it.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ReadImageSourceObserver {
        store_path: PathBuf,
    }

    impl ReadImageSourceObserver {
        /// Discard everything recorded for this observer's store, leaving
        /// every other store's history exactly as it was.
        pub fn reset(&self) {
            read_image_source_events_by_store()
                .lock()
                .expect("read-image source event mutex poisoned")
                .remove(&self.store_path);
        }

        /// This store's events in the order they were recorded, whichever
        /// thread recorded them.
        pub fn events(&self) -> Vec<ReadImageSourceEvent> {
            read_image_source_events_by_store()
                .lock()
                .expect("read-image source event mutex poisoned")
                .get(&self.store_path)
                .cloned()
                .unwrap_or_default()
        }
    }

    pub fn observe_read_image_source_events_for_test(store_path: &Path) -> ReadImageSourceObserver {
        ReadImageSourceObserver {
            store_path: store_path.to_path_buf(),
        }
    }

    fn note_read_image_source_event_for_test(store_path: &Path, event: ReadImageSourceEvent) {
        read_image_source_events_by_store()
            .lock()
            .expect("read-image source event mutex poisoned")
            .entry(store_path.to_path_buf())
            .or_default()
            .push(event);
    }

    pub(super) fn note_read_image_source_start_for_test(
        store_path: &Path,
        breadcrumb_path: Option<&Path>,
    ) {
        note_read_image_source_event_for_test(
            store_path,
            ReadImageSourceEvent::Started {
                breadcrumb_path: breadcrumb_path.map(Path::to_path_buf),
            },
        );
        pause_reader_at_source_start(store_path);
    }

    pub(super) fn note_read_image_source_access_for_test(store_path: &Path) {
        note_read_image_source_event_for_test(store_path, ReadImageSourceEvent::SourceAccess);
    }

    pub(super) fn note_read_image_source_handles_dropped_for_test(store_path: &Path) {
        note_read_image_source_event_for_test(
            store_path,
            ReadImageSourceEvent::SourceHandlesDropped,
        );
    }

    pub(super) fn note_read_image_source_release_for_test(
        store_path: &Path,
        breadcrumb_path: Option<&Path>,
    ) {
        note_read_image_source_event_for_test(
            store_path,
            ReadImageSourceEvent::Released {
                breadcrumb_path: breadcrumb_path.map(Path::to_path_buf),
            },
        );
    }

    #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
    pub enum LoadReadImageAdapterError {
        #[error("load_read_image production path is not implemented")]
        NotImplemented,
        #[error("read-image source failed: {0}")]
        Source(String),
        #[error("read-image release proof failed: {0}")]
        Release(String),
        #[error("requested vector is absent from the committed read image: {table}/{id}")]
        VectorNotFound { table: String, id: i64 },
    }

    /// Thin adapter over the sole `load_read_image` production path.
    ///
    /// It takes no runtime directory, so it exercises the platform default.
    /// Session-level supplied roots are exercised through `ReadSession`.
    pub fn load_read_image_for_test_adapter(
        path: &Path,
    ) -> std::result::Result<ReadImageObservation, LoadReadImageAdapterError> {
        match super::load_read_image(path) {
            Ok(load) => {
                let (mut image, receipt, source) = load.into_parts();
                receipt.validate_released().map_err(|error| match error {
                    super::LoadReadImageError::Release(error) => {
                        LoadReadImageAdapterError::Release(error)
                    }
                    other => LoadReadImageAdapterError::Release(other.to_string()),
                })?;
                if image.lazy_relational {
                    let (tables, vector_entries, current_vectors) = source
                        .compatibility_vector_projection_for_test(&image.table_meta, &image.ddl_log)
                        .map_err(|error| LoadReadImageAdapterError::Source(error.to_string()))?;
                    image.relational_tables = tables;
                    image.vector_entries = vector_entries;
                    image.current_vectors = current_vectors;
                }
                let vectors = super::compatibility_read_vector_projection(
                    &image.current_vectors,
                    &image.table_meta,
                    &image.relational_tables,
                )
                .map_err(|error| LoadReadImageAdapterError::Source(error.to_string()))?;
                Ok(ReadImageObservation {
                    vectors,
                    released_breadcrumb_path: receipt.breadcrumb_path().map(Path::to_path_buf),
                    source_accesses: receipt.source_accesses(),
                })
            }
            Err(super::LoadReadImageError::Release(error)) => {
                Err(LoadReadImageAdapterError::Release(error))
            }
            // Compatibility adaptation: this persistence-only proof adapter
            // exposes ordinary load failures as its established source string,
            // including typed categories it does not consume.
            Err(other) => Err(LoadReadImageAdapterError::Source(other.to_string())),
        }
    }

    pub fn load_read_image_vector_for_test(
        path: &Path,
        table: &str,
        id: i64,
    ) -> std::result::Result<Vec<f32>, LoadReadImageAdapterError> {
        let image = load_read_image_for_test_adapter(path)?;
        image
            .vectors
            .get(&(table.to_owned(), id))
            .cloned()
            .ok_or_else(|| LoadReadImageAdapterError::VectorNotFound {
                table: table.to_owned(),
                id,
            })
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ReadImageHydrationParticipantObservation {
        pub participant: u64,
        pub breadcrumb_path: Option<PathBuf>,
        pub breadcrumb: Option<ReaderBreadcrumb>,
    }

    /// One process-local controller for exactly two production hydration
    /// intervals. Each interval blocks at the real post-open/pre-decode
    /// checkpoint until its own participant is released. Dropping the
    /// controller cancels every waiter so a failed assertion cannot strand
    /// a read-only Redb handle or breadcrumb.
    #[derive(Debug)]
    pub struct TwoReadImageHydrationController {
        token: u64,
        next_observation: usize,
        finished: bool,
    }

    impl TwoReadImageHydrationController {
        pub fn next_participant_for_test(&mut self) -> ReadImageHydrationParticipantObservation {
            let (state, changed) = two_read_image_hydrations();
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            loop {
                let run = state
                    .active
                    .as_ref()
                    .filter(|run| run.token == self.token)
                    .expect("two-hydration controller was cancelled");
                if let Some(observation) = run.reached.get(self.next_observation).cloned() {
                    self.next_observation += 1;
                    return observation;
                }
                assert!(
                    !(run.finished_attempts > 0 && run.reached.len() < run.started_attempts),
                    "a production hydration finished before reaching its armed checkpoint"
                );
                state = changed
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }

        pub fn release_participant_for_test(&self, participant: u64) {
            let (state, changed) = two_read_image_hydrations();
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let run = state
                .active
                .as_mut()
                .filter(|run| run.token == self.token)
                .expect("two-hydration controller was cancelled");
            assert!(
                run.reached
                    .iter()
                    .any(|observation| observation.participant == participant),
                "cannot release a hydration participant before it reaches the checkpoint"
            );
            assert!(
                run.released.insert(participant),
                "hydration participant was released twice"
            );
            changed.notify_all();
        }

        pub fn finish_for_test(mut self) {
            let (state, changed) = two_read_image_hydrations();
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let run = state
                .active
                .take()
                .filter(|run| run.token == self.token)
                .expect("two-hydration controller was cancelled");
            changed.notify_all();
            drop(state);
            assert_eq!(run.reached.len(), 2, "both hydration checkpoints must run");
            assert_eq!(
                run.started_attempts, 2,
                "both hydration attempts must start"
            );
            assert_eq!(
                run.finished_attempts, 2,
                "both hydration attempts must finish"
            );
            assert_eq!(run.released.len(), 2, "both hydrations must be released");
            assert_eq!(
                run.departed.len(),
                2,
                "both hydration intervals must leave the checkpoint"
            );
            self.finished = true;
        }
    }

    impl Drop for TwoReadImageHydrationController {
        fn drop(&mut self) {
            if self.finished {
                return;
            }
            let (state, changed) = two_read_image_hydrations();
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.active.as_ref().map(|run| run.token) == Some(self.token) {
                state.active = None;
                changed.notify_all();
            }
        }
    }

    /// Arms the controller for exactly `store_path`, bound before this call
    /// returns -- before any reader, intended or not, can possibly reach the
    /// checkpoint. Binding it lazily from whichever reader arrives first let
    /// an unrelated store's hydration claim the run if it happened to reach
    /// the checkpoint before either intended participant did.
    pub fn arm_two_read_image_hydrations_for_test(
        store_path: &Path,
    ) -> TwoReadImageHydrationController {
        let (state, _) = two_read_image_hydrations();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            state.active.is_none(),
            "two-hydration controller is already armed"
        );
        state.next_token = state.next_token.wrapping_add(1);
        let token = state.next_token;
        state.active = Some(TwoReadImageHydrationRun {
            token,
            next_participant: 0,
            started_attempts: 0,
            finished_attempts: 0,
            reached: Vec::new(),
            released: BTreeSet::new(),
            departed: BTreeSet::new(),
            store: Some(store_path.to_path_buf()),
        });
        TwoReadImageHydrationController {
            token,
            next_observation: 0,
            finished: false,
        }
    }

    #[derive(Debug)]
    pub struct TwoReadImageHydrationAttemptGuard {
        token: u64,
    }

    impl Drop for TwoReadImageHydrationAttemptGuard {
        fn drop(&mut self) {
            let (state, changed) = two_read_image_hydrations();
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(run) = state.active.as_mut().filter(|run| run.token == self.token) {
                run.finished_attempts += 1;
                changed.notify_all();
            }
        }
    }

    pub fn begin_two_read_image_hydration_attempt_for_test() -> TwoReadImageHydrationAttemptGuard {
        let (state, _) = two_read_image_hydrations();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let run = state
            .active
            .as_mut()
            .expect("two-hydration controller is not armed");
        assert!(
            run.started_attempts < 2,
            "unexpected third hydration attempt"
        );
        run.started_attempts += 1;
        TwoReadImageHydrationAttemptGuard { token: run.token }
    }

    fn two_read_image_hydration_checkpoint_for_test(
        database_path: &Path,
        breadcrumb_path: Option<&Path>,
        breadcrumb: Option<&ReaderBreadcrumb>,
    ) {
        let (state, changed) = two_read_image_hydrations();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(run) = state.active.as_mut() else {
            return;
        };
        // The store is bound at arm time, before any reader can reach this
        // checkpoint, so an unrelated store arriving here -- however early --
        // always passes through untouched.
        if run.store.as_deref() != Some(database_path) {
            return;
        }
        assert!(
            run.reached.len() < 2,
            "two-hydration controller observed an unexpected third participant"
        );
        let participant = run.next_participant;
        run.next_participant = run.next_participant.wrapping_add(1);
        let token = run.token;
        run.reached.push(ReadImageHydrationParticipantObservation {
            participant,
            breadcrumb_path: breadcrumb_path.map(Path::to_path_buf),
            breadcrumb: breadcrumb.cloned(),
        });
        changed.notify_all();
        loop {
            let waiting = state
                .active
                .as_ref()
                .is_some_and(|run| run.token == token && !run.released.contains(&participant));
            if !waiting {
                break;
            }
            state = changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        if let Some(run) = state.active.as_mut().filter(|run| run.token == token) {
            run.departed.insert(participant);
            changed.notify_all();
        }
    }

    /// What a concurrent last reader removes, in the departure seam below.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ReaderFenceDepartureForTest {
        /// Every publication fence in the store's registry: a cleanup's unlink.
        Fence,
        /// The whole `contextdb` child of the platform directory.
        Root,
    }

    static READER_FENCE_DEPARTURE: std::sync::Mutex<Option<ReaderFenceDepartureForTest>> =
        std::sync::Mutex::new(None);

    /// Arm one departure: the next direct registration, after it has opened
    /// its fences and before it takes any registry lock, finds them removed.
    /// Whether an armed departure is still waiting to happen; false once a
    /// registration has opened its fences and the departure was carried out.
    pub fn reader_fence_departure_armed_for_test() -> bool {
        READER_FENCE_DEPARTURE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some()
    }

    pub fn arm_reader_fence_departure_for_test(departure: ReaderFenceDepartureForTest) {
        *READER_FENCE_DEPARTURE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(departure);
    }

    pub(crate) fn reader_fences_opened_for_test(
        runtimes: &[crate::local_transport::RuntimeDirectory],
        identity: (u64, u64),
        owner_cursor: bool,
    ) {
        let mut armed = READER_FENCE_DEPARTURE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // An owner cursor holds every registry lock here, so only a fence's
        // departure is exercised on its path.
        if owner_cursor && *armed != Some(ReaderFenceDepartureForTest::Fence) {
            return;
        }
        let Some(departure) = armed.take() else {
            return;
        };
        drop(armed);
        for runtime in runtimes {
            match departure {
                ReaderFenceDepartureForTest::Fence => {
                    let registry = super::reader_snapshot_directory(runtime.path(), identity);
                    for entry in std::fs::read_dir(&registry).into_iter().flatten().flatten() {
                        if super::purge_publication::is_fence_file_name(&entry.file_name()) {
                            let _ = std::fs::remove_file(entry.path());
                        }
                    }
                }
                ReaderFenceDepartureForTest::Root if runtime.root() != runtime.path() => {
                    let _ = std::fs::remove_dir_all(runtime.path());
                }
                ReaderFenceDepartureForTest::Root => {}
            }
        }
    }

    /// A purge parked holding its publication fences, before its durable
    /// commit begins the write that refreshes the reader registries.
    pub struct PurgeParkForTest {
        pub parked: std::sync::mpsc::Receiver<()>,
        pub resume: std::sync::mpsc::SyncSender<()>,
    }

    type PurgePark = (
        std::sync::mpsc::SyncSender<()>,
        std::sync::mpsc::Receiver<()>,
    );
    static PURGE_PARK_BEFORE_COMMIT: std::sync::Mutex<Option<PurgePark>> =
        std::sync::Mutex::new(None);

    /// Arm one park: the next purge stops after acquiring its fences and
    /// before its durable commit, and waits there until resumed.
    pub fn arm_purge_park_before_durable_commit_for_test() -> PurgeParkForTest {
        let (parked_sender, parked) = std::sync::mpsc::sync_channel(1);
        let (resume, resume_receiver) = std::sync::mpsc::sync_channel(1);
        *PURGE_PARK_BEFORE_COMMIT
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some((parked_sender, resume_receiver));
        PurgeParkForTest { parked, resume }
    }

    pub(crate) fn purge_before_durable_commit_for_test() {
        let armed = PURGE_PARK_BEFORE_COMMIT
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some((parked, resume)) = armed {
            let _ = parked.send(());
            let _ = resume.recv();
        }
    }

    thread_local! {
        static READER_JOIN_WAIT: std::cell::RefCell<Option<Box<dyn Fn()>>> =
            const { std::cell::RefCell::new(None) };
    }

    /// Run `hook` on this thread just before a join blocks on a runtime root's
    /// serializer that a leave is holding.
    pub fn install_reader_join_wait_hook_for_test(hook: Box<dyn Fn()>) {
        READER_JOIN_WAIT.with(|slot| *slot.borrow_mut() = Some(hook));
    }

    pub(crate) fn reader_join_wait_hook_installed_for_test() -> bool {
        READER_JOIN_WAIT.with(|slot| slot.borrow().is_some())
    }

    pub(crate) fn run_reader_join_wait_hook_for_test() {
        if let Some(hook) = READER_JOIN_WAIT.with(|slot| slot.borrow_mut().take()) {
            hook();
        }
    }

    /// A leave parked holding its runtime root's serializer.
    pub struct ReaderLeaveParkForTest {
        pub parked: std::sync::mpsc::Receiver<()>,
        pub resume: std::sync::mpsc::SyncSender<()>,
    }

    type ReaderLeavePark = (
        std::sync::mpsc::SyncSender<()>,
        std::sync::mpsc::Receiver<()>,
    );
    static READER_LEAVE_PARK: std::sync::Mutex<Option<ReaderLeavePark>> =
        std::sync::Mutex::new(None);

    /// Arm one park: the next registration leave stops right after taking its
    /// runtime root's serializer, and waits there until resumed.
    pub fn arm_reader_leave_park_for_test() -> ReaderLeaveParkForTest {
        let (parked_sender, parked) = std::sync::mpsc::sync_channel(1);
        let (resume, resume_receiver) = std::sync::mpsc::sync_channel(1);
        *READER_LEAVE_PARK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some((parked_sender, resume_receiver));
        ReaderLeaveParkForTest { parked, resume }
    }

    pub(crate) fn reader_leave_holding_serializer_for_test() {
        let armed = READER_LEAVE_PARK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some((parked, resume)) = armed {
            let _ = parked.send(());
            let _ = resume.recv();
        }
    }

    pub fn arm_read_image_hydration_pause_for_test() {
        assert!(
            !READ_IMAGE_HYDRATION_PAUSE.swap(true, Ordering::SeqCst),
            "read-image hydration pause is already armed"
        );
    }

    /// The real loader calls this only after its runtime breadcrumb exists
    /// and its `redb::ReadOnlyDatabase` has acquired the hydration lock, and
    /// before decoding or releasing either resource.
    pub(crate) fn read_image_hydration_checkpoint_for_test(
        database_path: &Path,
        breadcrumb_path: Option<&Path>,
        breadcrumb: Option<&ReaderBreadcrumb>,
    ) {
        two_read_image_hydration_checkpoint_for_test(database_path, breadcrumb_path, breadcrumb);
        if !READ_IMAGE_HYDRATION_PAUSE.swap(false, Ordering::SeqCst) {
            return;
        }
        match (breadcrumb_path, breadcrumb) {
            (Some(path), Some(breadcrumb)) => println!(
                "READ_IMAGE_HYDRATION_HELD={}:{}:{}:{}",
                encode_hex(path.as_os_str().as_encoded_bytes()),
                breadcrumb.process_id,
                breadcrumb.process_start.0,
                encode_hex(breadcrumb.process_name.as_bytes())
            ),
            (None, None) => println!("READ_IMAGE_HYDRATION_HELD=unverified"),
            _ => panic!("hydration checkpoint path and breadcrumb must appear together"),
        }
        std::io::stdout()
            .flush()
            .expect("flush read-image hydration marker");
        let mut command = String::new();
        std::io::stdin()
            .lock()
            .read_line(&mut command)
            .expect("read hydration release command");
        assert_eq!(command.trim(), "release");
    }

    /// Rewrites the runtime breadcrumb through the production breadcrumb
    /// codec/location helper. No test-side file format fallback is permitted.
    pub fn replace_reader_breadcrumb_for_test(
        database_path: &Path,
        runtime_directory: &Path,
        expected_current: &ReaderBreadcrumb,
        replacement: &ReaderBreadcrumb,
    ) -> std::result::Result<PathBuf, LoadReadImageAdapterError> {
        let mut matching = super::locked_reader_breadcrumbs(database_path, runtime_directory)
            .map_err(|error| LoadReadImageAdapterError::Source(error.to_string()))?
            .into_iter()
            .filter(|locked| locked.breadcrumb.as_ref() == Some(expected_current));
        let locked = matching.next().ok_or_else(|| {
            LoadReadImageAdapterError::Source(
                "the expected locked reader breadcrumb is absent".to_owned(),
            )
        })?;
        if matching.next().is_some() {
            return Err(LoadReadImageAdapterError::Source(
                "the expected reader breadcrumb is not unique".to_owned(),
            ));
        }
        let encoded = super::encode_reader_breadcrumb(replacement)
            .map_err(|error| LoadReadImageAdapterError::Source(error.to_string()))?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&locked.path)
            .map_err(|error| LoadReadImageAdapterError::Source(error.to_string()))?;
        file.write_all(&encoded)
            .and_then(|_| file.sync_data())
            .map_err(|error| LoadReadImageAdapterError::Source(error.to_string()))?;
        Ok(locked.path)
    }

    pub fn create_unlocked_reader_breadcrumb_for_test(
        database_path: &Path,
        runtime_directory: &Path,
        breadcrumb: &ReaderBreadcrumb,
    ) -> std::result::Result<PathBuf, String> {
        let path = super::next_reader_breadcrumb_path(database_path, runtime_directory, breadcrumb)
            .map_err(|error| error.to_string())?;
        let directory = path
            .parent()
            .ok_or_else(|| "reader breadcrumb has no runtime directory".to_owned())?;
        // Last-reader cleanup removes the addressed directory. Recreate the
        // stale-record fixture through the same private-directory primitive
        // as production, so the probe tests stale records rather than modes.
        super::create_reader_breadcrumb_directory(runtime_directory)
            .and_then(|()| super::create_reader_breadcrumb_directory(directory))
            .map_err(|error| error.to_string())?;
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options.open(&path).map_err(|error| error.to_string())?;
        let encoded =
            super::encode_reader_breadcrumb(breadcrumb).map_err(|error| error.to_string())?;
        file.write_all(&encoded)
            .and_then(|_| file.sync_data())
            .map_err(|error| error.to_string())?;
        Ok(path)
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct LockedReaderBreadcrumbObservation {
        pub path: PathBuf,
        pub breadcrumb: Option<ReaderBreadcrumb>,
    }

    pub fn locked_reader_breadcrumbs_for_test(
        database_path: &Path,
        runtime_directory: &Path,
    ) -> std::result::Result<Vec<LockedReaderBreadcrumbObservation>, String> {
        super::locked_reader_breadcrumbs(database_path, runtime_directory)
            .map(|locked| {
                locked
                    .into_iter()
                    .map(|locked| LockedReaderBreadcrumbObservation {
                        path: locked.path,
                        breadcrumb: locked.breadcrumb,
                    })
                    .collect()
            })
            .map_err(|error| error.to_string())
    }

    pub fn canonical_reader_breadcrumb_directory_for_test(
        database_path: &Path,
        runtime_directory: &Path,
    ) -> std::result::Result<PathBuf, String> {
        super::reader_breadcrumb_directory(database_path, runtime_directory)
            .map_err(|error| error.to_string())
    }

    /// The platform default runtime-state directory for default-only proofs.
    /// `None` means no usable per-user location is available.
    pub fn reader_breadcrumb_runtime_root_for_test() -> Option<PathBuf> {
        super::reader_breadcrumb_runtime_directory().map(|runtime| runtime.path().to_path_buf())
    }

    /// This is deliberately an observation door, not a record constructor.
    /// The real dual-slot decoder supplies it once the durable companion
    /// exists; until then the proof reports that the production record is
    /// absent rather than fabricating one in test code.
    pub fn inspect_companion_record_for_test(
        path: &Path,
    ) -> std::result::Result<CompanionRecordObservation, String> {
        super::inspect_companion_record(path).map(companion_observation)
    }

    pub fn store_fingerprint_for_test(path: &Path) -> std::result::Result<[u8; 32], String> {
        super::store_file_fingerprint(path).map_err(|error| error.to_string())
    }

    pub fn inspect_replacement_intent_for_test(
        path: &Path,
    ) -> std::result::Result<ReplacementIntentObservation, String> {
        let companion_path = super::companion_path(path);
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        let mut file = options
            .open(&companion_path)
            .map_err(|error| format!("open companion {}: {error}", companion_path.display()))?;
        let metadata = file
            .metadata()
            .map_err(|error| format!("inspect companion {}: {error}", companion_path.display()))?;
        super::validate_companion_metadata(
            &companion_path,
            &file,
            &metadata,
            super::CompanionUse::Reading,
        )
        .map_err(|error| error.to_string())?;
        let state = super::read_companion_state(&mut file)?;
        let active_record = state.active_record.map(companion_observation);
        let (pending_intended_record, pending_store_fingerprint) = state
            .pending_replacement
            .map(|pending| {
                (
                    Some(companion_observation(pending.intended_record)),
                    Some(pending.store_fingerprint),
                )
            })
            .unwrap_or((None, None));
        Ok(ReplacementIntentObservation {
            active_record,
            pending_intended_record,
            pending_store_fingerprint,
            current_store_fingerprint: store_fingerprint_for_test(path)?,
        })
    }

    /// One blocking wait a reader-release wait actually performed.
    ///
    /// A wait that sleeps until the kernel reports a holder let go blocks
    /// once per hold and supplies no deadline of its own. A wait that polls
    /// supplies a deadline every time, and that deadline IS the poll
    /// interval. Recording the shape of each block separates the two
    /// without measuring how long anything took.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ReaderReleaseBlockForTest {
        /// Blocked until the kernel reported that a real holder of the
        /// store let go or died. Nothing but that event decides when this
        /// wakes.
        UntilHolderReleased,
        /// Blocked with a deadline of the wait's own choosing, in
        /// milliseconds.
        UntilDeadline { after_ms: u64 },
        /// A writer's cancellable wait started its acquisition helper.
        HelperSpawned { pid: u32 },
        /// The writer entered its one park.
        ParkEntered,
        /// The writer's park returned.
        ParkReturned,
        /// The helper was reaped.
        Reaped,
        /// The writer decided its outcome at the linearization point.
        Linearized,
    }

    type RecordedBlocks = Vec<(std::thread::ThreadId, ReaderReleaseBlockForTest)>;

    fn reader_release_blocks() -> &'static Mutex<RecordedBlocks> {
        static BLOCKS: OnceLock<Mutex<RecordedBlocks>> = OnceLock::new();
        BLOCKS.get_or_init(|| Mutex::new(Vec::new()))
    }

    /// Start recording what reader-release waits block in, discarding
    /// anything recorded before. Recording is process-wide, so a proof
    /// that uses it owns its test binary.
    pub fn reset_reader_release_blocks_for_test() {
        reader_release_blocks()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }

    /// Every block reader-release waits have performed since the reset, in
    /// order.
    pub fn reader_release_blocks_for_test() -> Vec<ReaderReleaseBlockForTest> {
        reader_release_blocks()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|(_, block)| *block)
            .collect()
    }

    /// The blocks recorded by waits that ran on `thread`, in order, so a
    /// proof sharing its binary with other proofs reads only its own waits.
    /// How many writer-wait helper stacks this process has mapped and not
    /// yet unmapped. Process-wide, so a measurement runs in a child process.
    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    pub fn writer_wait_helper_stacks_mapped_for_test() -> usize {
        super::writer_wait_helper::HELPER_STACKS_MAPPED.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn reader_release_blocks_of_thread_for_test(
        thread: std::thread::ThreadId,
    ) -> Vec<ReaderReleaseBlockForTest> {
        reader_release_blocks()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|(recorded, _)| *recorded == thread)
            .map(|(_, block)| *block)
            .collect()
    }

    /// Production reader-release waits call this once for every block they
    /// perform, whatever mechanism they block on.
    pub(super) fn note_reader_release_block_for_test(block: ReaderReleaseBlockForTest) {
        reader_release_blocks()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push((std::thread::current().id(), block));
    }

    /// What answering reads has cost this thread in operating-system objects.
    ///
    /// A bounded statement on the committed-file route is answered on the
    /// caller's own thread, against a file the session already holds open.
    /// Starting an OS thread or opening a pipe to answer one is setup the
    /// statement never asked for, and on a busy machine it costs milliseconds
    /// where the read itself costs microseconds -- so a reader that pays it
    /// per statement pays it nine times to render one page.
    ///
    /// The counts are per thread, not per process, because the work being
    /// counted is done by whichever thread runs the statement. That keeps a
    /// proof honest beside every other test sharing the same binary.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct ReadOsSetupForTest {
        /// OS threads this thread started so a wait could be given a deadline.
        pub deadline_timer_threads: usize,
        /// Wake pipes this thread opened so a wait could sleep on a
        /// coordination hold. Arming one watch opens exactly one pipe.
        pub coordination_wake_pipes: usize,
        /// Results this thread published against the authoritative-erasure
        /// frontier. One per statement is the contract: a result is only
        /// answerable if nothing erased its snapshot's tables underneath it.
        pub erasure_publications: usize,
        /// Times this thread read the erasure frontier's durable state. This
        /// is the file work a publication genuinely owes, as against the
        /// thread and pipe above, which it does not.
        pub erasure_frontier_reads: usize,
    }

    thread_local! {
        static READ_OS_SETUP: std::cell::Cell<ReadOsSetupForTest> =
            const {
                std::cell::Cell::new(ReadOsSetupForTest {
                    deadline_timer_threads: 0,
                    coordination_wake_pipes: 0,
                    erasure_publications: 0,
                    erasure_frontier_reads: 0,
                })
            };
    }

    /// Start counting from zero on this thread, discarding anything counted
    /// before.
    pub fn reset_read_os_setup_for_test() {
        READ_OS_SETUP.with(|counted| counted.set(ReadOsSetupForTest::default()));
        crate::local_transport::reset_timer_threads_started_for_test();
    }

    /// What this thread has started or opened since the reset.
    ///
    /// Threads are counted where they are started. The deadline clock is the
    /// local transport's, and that module is audited to depend on nothing in
    /// storage, so it keeps its own count and this reads it rather than being
    /// told.
    pub fn read_os_setup_for_test() -> ReadOsSetupForTest {
        let mut setup = READ_OS_SETUP.with(std::cell::Cell::get);
        setup.deadline_timer_threads = crate::local_transport::timer_threads_started_for_test();
        setup
    }

    /// Production calls this once for every coordination wake pipe it actually
    /// opens.
    pub(crate) fn note_coordination_wake_pipe_for_test() {
        READ_OS_SETUP.with(|counted| {
            let mut setup = counted.get();
            setup.coordination_wake_pipes += 1;
            counted.set(setup);
        });
    }

    /// Production calls this once for every result it publishes against the
    /// authoritative-erasure frontier.
    pub(crate) fn note_erasure_publication_for_test() {
        READ_OS_SETUP.with(|counted| {
            let mut setup = counted.get();
            setup.erasure_publications += 1;
            counted.set(setup);
        });
    }

    /// Production calls this once for every read of the erasure frontier's
    /// durable state.
    pub(crate) fn note_erasure_frontier_read_for_test() {
        READ_OS_SETUP.with(|counted| {
            let mut setup = counted.get();
            setup.erasure_frontier_reads += 1;
            counted.set(setup);
        });
    }

    /// Bypasses every ContextDB companion check and asks Redb itself whether
    /// the supplied exact database pathname can be READ without repairing it.
    /// Opens read-only, so asking changes nothing.
    pub fn try_raw_redb_read_only_open_for_test(path: &Path) -> RawRedbReadOnlyOpenObservation {
        match super::redb_builder().open_read_only(path) {
            Ok(database) => {
                drop(database);
                RawRedbReadOnlyOpenObservation::Acquired
            }
            Err(redb::DatabaseError::RepairAborted) => {
                RawRedbReadOnlyOpenObservation::RepairAborted
            }
            Err(error) => RawRedbReadOnlyOpenObservation::Other(error.to_string()),
        }
    }

    /// Bypasses every ContextDB companion check and asks whether a writable
    /// owner can open the supplied exact database pathname: first the store's
    /// own data lock, the one every ContextDB opener of this physical file
    /// coordinates on, then Redb itself.
    pub fn try_raw_redb_writer_open_for_test(path: &Path) -> RawRedbWriterOpenObservation {
        #[cfg(unix)]
        let _data_lock = {
            use super::physical_admission::{ByteLock, Granted, try_data};
            let descriptor = match std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
            {
                Ok(descriptor) => descriptor,
                Err(error) => return RawRedbWriterOpenObservation::Other(error.to_string()),
            };
            match try_data(&descriptor, ByteLock::Exclusive) {
                Ok(Granted::Yes) => descriptor,
                Ok(Granted::HeldByAnother) => {
                    return RawRedbWriterOpenObservation::DatabaseAlreadyOpen;
                }
                Err(error) => return RawRedbWriterOpenObservation::Other(error.to_string()),
            }
        };
        match super::redb_builder().open(path) {
            Ok(database) => {
                drop(database);
                RawRedbWriterOpenObservation::Acquired
            }
            Err(redb::DatabaseError::DatabaseAlreadyOpen) => {
                RawRedbWriterOpenObservation::DatabaseAlreadyOpen
            }
            Err(error) => RawRedbWriterOpenObservation::Other(error.to_string()),
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum DurableStoreDamage {
        MalformedRecord,
        LegacyLayout,
        NonMonotonicCommitIndex,
        InvalidVectorReference,
        RowVectorDivergence,
        WritableSanitizerWouldChange,
        /// A NULL sitting in a full-precision vector cell whose column is
        /// declared `NOT NULL` -- state the schema forbids, so no writer
        /// could have produced it and no reader may serve it.
        NullInRequiredVectorCell,
        /// A value that is not an embedding sitting in a vector cell.
        NonVectorValueInVectorCell,
        /// The separately durable vector entry a required quantized column's
        /// stored placeholder stands for, removed from a live row while the
        /// row itself is left exactly as its writer wrote it. The schema still
        /// promises that row an embedding the file no longer holds.
        MissingQuantizedVectorEntry,
        /// Changes an SQ8 body without changing its committed identity leaf.
        Sq8VectorDivergence,
        /// Changes an SQ4 body without changing its committed identity leaf.
        Sq4VectorDivergence,
        /// Makes a live row body claim it was deleted while its committed
        /// logical leaf still says it is live.
        RowVisibilitySuppression,
        /// Removes one physical posting for a live row from a declared index,
        /// leaving the row itself and the committed canonical proof of that
        /// index range exactly as the writer wrote them. A reader that trusts
        /// its cached canonical run without reconciling it against the physical
        /// tree answers such a store one row short instead of reporting damage.
        MissingRelationalIndexPosting,
        /// Files one live row's identity under another key's posting, so the
        /// index claims a row sits somewhere its own stored values say it does
        /// not. A read that took a candidate's authorization from the index
        /// entry rather than from the row would publish a row its reader may
        /// not see.
        MisdirectedRelationalIndexPosting,
    }

    impl DurableStoreDamage {
        pub const ALL: [Self; 14] = [
            Self::MalformedRecord,
            Self::LegacyLayout,
            Self::NonMonotonicCommitIndex,
            Self::InvalidVectorReference,
            Self::RowVectorDivergence,
            Self::WritableSanitizerWouldChange,
            Self::NullInRequiredVectorCell,
            Self::NonVectorValueInVectorCell,
            Self::MissingQuantizedVectorEntry,
            Self::Sq8VectorDivergence,
            Self::Sq4VectorDivergence,
            Self::RowVisibilitySuppression,
            Self::MissingRelationalIndexPosting,
            Self::MisdirectedRelationalIndexPosting,
        ];
    }

    fn open_damage_fixture(path: &Path) -> std::result::Result<redb::Database, String> {
        super::redb_builder()
            .open(path)
            .map_err(|error| error.to_string())
    }

    fn damage_table_metadata(
        database: &redb::Database,
    ) -> std::result::Result<std::collections::HashMap<String, super::TableMeta>, String> {
        let read = database.begin_read().map_err(|error| error.to_string())?;
        let table = read
            .open_table(super::META_TABLE)
            .map_err(|error| error.to_string())?;
        let mut metadata = std::collections::HashMap::new();
        for entry in table.iter().map_err(|error| error.to_string())? {
            let (key, value) = entry.map_err(|error| error.to_string())?;
            if let Some(table_name) = key.value().strip_prefix("table:") {
                let (table_meta, _) =
                    super::RedbPersistence::decode_table_meta_versioned(value.value())
                        .map_err(|error| error.to_string())?;
                metadata.insert(table_name.to_owned(), table_meta);
            }
        }
        Ok(metadata)
    }

    fn damage_format_marker(
        database: &redb::Database,
        bytes: &[u8],
    ) -> std::result::Result<(), String> {
        let write = database.begin_write().map_err(|error| error.to_string())?;
        #[cfg(any(test, feature = "test-seams"))]
        let mut graph_publication_attempt =
            crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
        {
            let mut table = write
                .open_table(super::FORMAT_METADATA_TABLE)
                .map_err(|error| error.to_string())?;
            table
                .insert(super::FORMAT_VERSION_KEY, bytes)
                .map_err(|error| error.to_string())?;
        }
        {
            #[cfg(any(test, feature = "test-seams"))]
            let graph_commit_result = write.commit();
            #[cfg(any(test, feature = "test-seams"))]
            if graph_commit_result.is_ok() {
                graph_publication_attempt.committed();
            }
            graph_commit_result
        }
        .map_err(|error| error.to_string())
    }

    fn damage_commit_index(database: &redb::Database) -> std::result::Result<(), String> {
        let read = database.begin_read().map_err(|error| error.to_string())?;
        let table = read
            .open_table(super::COMMIT_INDEX_TABLE)
            .map_err(|error| error.to_string())?;
        let mut maximum_lsn = 0_u64;
        let mut maximum_tx = 0_u64;
        for entry in table.iter().map_err(|error| error.to_string())? {
            let (lsn, tx) = entry.map_err(|error| error.to_string())?;
            maximum_lsn = maximum_lsn.max(lsn.value());
            maximum_tx = maximum_tx.max(tx.value());
        }
        drop(table);
        drop(read);
        let first_lsn = maximum_lsn
            .checked_add(1)
            .ok_or_else(|| "fixture commit LSN overflow".to_owned())?;
        let second_lsn = maximum_lsn
            .checked_add(2)
            .ok_or_else(|| "fixture commit LSN overflow".to_owned())?;
        let first_tx = maximum_tx
            .checked_add(2)
            .ok_or_else(|| "fixture transaction overflow".to_owned())?;
        let second_tx = maximum_tx
            .checked_add(1)
            .ok_or_else(|| "fixture transaction overflow".to_owned())?;
        let write = database.begin_write().map_err(|error| error.to_string())?;
        #[cfg(any(test, feature = "test-seams"))]
        let mut graph_publication_attempt =
            crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
        {
            let mut table = write
                .open_table(super::COMMIT_INDEX_TABLE)
                .map_err(|error| error.to_string())?;
            table
                .insert(first_lsn, first_tx)
                .map_err(|error| error.to_string())?;
            table
                .insert(second_lsn, second_tx)
                .map_err(|error| error.to_string())?;
        }
        {
            #[cfg(any(test, feature = "test-seams"))]
            let graph_commit_result = write.commit();
            #[cfg(any(test, feature = "test-seams"))]
            if graph_commit_result.is_ok() {
                graph_publication_attempt.committed();
            }
            graph_commit_result
        }
        .map_err(|error| error.to_string())
    }

    fn first_vector_entry(
        database: &redb::Database,
        required_quantization: Option<super::VectorQuantization>,
    ) -> std::result::Result<(Vec<u8>, super::VectorEntry, super::VectorQuantization), String> {
        let metadata = damage_table_metadata(database)?;
        let quantizations = super::RedbPersistence::vector_quantization_map(&metadata);
        let read = database.begin_read().map_err(|error| error.to_string())?;
        let table = read
            .open_table(super::VECTORS_TABLE)
            .map_err(|error| error.to_string())?;
        for entry in table.iter().map_err(|error| error.to_string())? {
            let (key, value) = entry.map_err(|error| error.to_string())?;
            let vector = super::RedbPersistence::decode_vector_entry(value.value())
                .map_err(|error| error.to_string())?;
            let quantization = quantizations.get(&vector.index).copied().ok_or_else(|| {
                format!("fixture vector index has no metadata: {:?}", vector.index)
            })?;
            if required_quantization.is_none_or(|required| required == quantization) {
                return Ok((key.value().to_vec(), vector, quantization));
            }
        }
        Err("fixture contains no vector matching the requested quantization".to_owned())
    }

    fn damage_vector_reference(database: &redb::Database) -> std::result::Result<(), String> {
        let (old_key, mut vector, quantization) = first_vector_entry(database, None)?;
        vector.index = super::VectorIndexRef::new(
            "__missing_fixture_table",
            "__missing_fixture_vector_column",
        );
        let new_key = super::RedbPersistence::vector_key(&vector);
        let encoded = super::RedbPersistence::encode_vector_entry(&vector, quantization)
            .map_err(|error| error.to_string())?;
        let write = database.begin_write().map_err(|error| error.to_string())?;
        #[cfg(any(test, feature = "test-seams"))]
        let mut graph_publication_attempt =
            crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
        {
            let mut table = write
                .open_table(super::VECTORS_TABLE)
                .map_err(|error| error.to_string())?;
            table
                .remove(old_key.as_slice())
                .map_err(|error| error.to_string())?;
            table
                .insert(new_key.as_slice(), encoded.as_slice())
                .map_err(|error| error.to_string())?;
        }
        {
            #[cfg(any(test, feature = "test-seams"))]
            let graph_commit_result = write.commit();
            #[cfg(any(test, feature = "test-seams"))]
            if graph_commit_result.is_ok() {
                graph_publication_attempt.committed();
            }
            graph_commit_result
        }
        .map_err(|error| error.to_string())
    }

    /// Removes the stored vector entry of a required quantized column from the
    /// vector table and leaves the relational row untouched. A quantized
    /// column keeps no copy of the embedding in the row, so what remains is a
    /// live row whose `NOT NULL` column has nothing left to be served from --
    /// state no writer produces, diagnosed from the stored bytes alone.
    fn damage_missing_quantized_vector_entry(
        database: &redb::Database,
    ) -> std::result::Result<(), String> {
        let metadata = damage_table_metadata(database)?;
        let read = database.begin_read().map_err(|error| error.to_string())?;
        let table = read
            .open_table(super::VECTORS_TABLE)
            .map_err(|error| error.to_string())?;
        let mut removed = None;
        for entry in table.iter().map_err(|error| error.to_string())? {
            let (key, value) = entry.map_err(|error| error.to_string())?;
            let vector = super::RedbPersistence::decode_vector_entry(value.value())
                .map_err(|error| error.to_string())?;
            let required_and_quantized = metadata
                .get(&vector.index.table)
                .and_then(|table_meta| {
                    table_meta
                        .columns
                        .iter()
                        .find(|column| column.name == vector.index.column)
                })
                .is_some_and(|column| {
                    !column.nullable
                        && !matches!(column.quantization, super::VectorQuantization::F32)
                });
            if required_and_quantized {
                removed = Some(key.value().to_vec());
                break;
            }
        }
        drop(table);
        drop(read);
        let key = removed.ok_or_else(|| {
            "fixture holds no embedding for a quantized vector column declared NOT NULL".to_owned()
        })?;
        let write = database.begin_write().map_err(|error| error.to_string())?;
        #[cfg(any(test, feature = "test-seams"))]
        let mut graph_publication_attempt =
            crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
        {
            let mut table = write
                .open_table(super::VECTORS_TABLE)
                .map_err(|error| error.to_string())?;
            table
                .remove(key.as_slice())
                .map_err(|error| error.to_string())?;
        }
        {
            #[cfg(any(test, feature = "test-seams"))]
            let graph_commit_result = write.commit();
            #[cfg(any(test, feature = "test-seams"))]
            if graph_commit_result.is_ok() {
                graph_publication_attempt.committed();
            }
            graph_commit_result
        }
        .map_err(|error| error.to_string())
    }

    fn damage_vector_divergence(
        database: &redb::Database,
        wanted_quantization: super::VectorQuantization,
    ) -> std::result::Result<(), String> {
        let (key, mut vector, quantization) =
            first_vector_entry(database, Some(wanted_quantization))?;
        let component = vector
            .vector
            .first_mut()
            .ok_or_else(|| "fixture F32 vector is empty".to_owned())?;
        *component = if component.is_finite() {
            *component + 0.5
        } else {
            0.5
        };
        let encoded = super::RedbPersistence::encode_vector_entry(&vector, quantization)
            .map_err(|error| error.to_string())?;
        let write = database.begin_write().map_err(|error| error.to_string())?;
        #[cfg(any(test, feature = "test-seams"))]
        let mut graph_publication_attempt =
            crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
        {
            let mut table = write
                .open_table(super::VECTORS_TABLE)
                .map_err(|error| error.to_string())?;
            table
                .insert(key.as_slice(), encoded.as_slice())
                .map_err(|error| error.to_string())?;
        }
        {
            #[cfg(any(test, feature = "test-seams"))]
            let graph_commit_result = write.commit();
            #[cfg(any(test, feature = "test-seams"))]
            if graph_commit_result.is_ok() {
                graph_publication_attempt.committed();
            }
            graph_commit_result
        }
        .map_err(|error| error.to_string())
    }

    #[derive(Debug, Clone, Copy)]
    pub enum ExactVectorBodyDamage {
        MalformedBody,
        MissingBody,
        MismatchedLeaf,
    }

    /// Change one physical F32 body while preserving the committed logical
    /// root, so a subsequent exact read has to detect the disagreement.
    #[cfg(any(test, feature = "test-seams"))]
    pub fn prepare_exact_vector_body_damage_for_test(
        path: &std::path::Path,
        damage: ExactVectorBodyDamage,
    ) -> std::result::Result<(), String> {
        let database = open_damage_fixture(path)?;
        if matches!(damage, ExactVectorBodyDamage::MismatchedLeaf) {
            return damage_vector_divergence(&database, super::VectorQuantization::F32);
        }
        let (key, _, _) = first_vector_entry(&database, Some(super::VectorQuantization::F32))?;
        let write = database.begin_write().map_err(|error| error.to_string())?;
        #[cfg(any(test, feature = "test-seams"))]
        let mut graph_publication_attempt =
            crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
        {
            let mut table = write
                .open_table(super::VECTORS_TABLE)
                .map_err(|error| error.to_string())?;
            match damage {
                ExactVectorBodyDamage::MalformedBody => {
                    table
                        .insert(key.as_slice(), &[0xff, 0x00][..])
                        .map_err(|error| error.to_string())?;
                }
                ExactVectorBodyDamage::MissingBody => {
                    table
                        .remove(key.as_slice())
                        .map_err(|error| error.to_string())?;
                }
                ExactVectorBodyDamage::MismatchedLeaf => unreachable!(),
            }
        }
        {
            #[cfg(any(test, feature = "test-seams"))]
            let graph_commit_result = write.commit();
            #[cfg(any(test, feature = "test-seams"))]
            if graph_commit_result.is_ok() {
                graph_publication_attempt.committed();
            }
            graph_commit_result
        }
        .map_err(|error| error.to_string())
    }

    fn damage_row_visibility_suppression(
        database: &redb::Database,
    ) -> std::result::Result<(), String> {
        let (table_name, table_meta, key, mut row) = first_relational_row(database)?;
        if row.deleted_tx.is_some() {
            return Err("fixture's first relational row is already deleted".to_owned());
        }
        row.deleted_tx = Some(row.created_tx);
        write_relational_row(database, &table_name, &table_meta, &key, &row)
    }

    /// Every live posting of the busiest declared index in the store, in tree
    /// order, with the index's physical table name.
    fn busiest_index_postings(
        database: &redb::Database,
    ) -> std::result::Result<(String, Vec<(super::RelationalIndexPosition, Vec<u8>)>), String> {
        let metadata = damage_table_metadata(database)?;
        let mut table_names = metadata.keys().cloned().collect::<Vec<_>>();
        table_names.sort();
        let mut busiest: Option<(String, Vec<(super::RelationalIndexPosition, Vec<u8>)>)> = None;
        for table_name in table_names {
            let Some(meta) = metadata.get(&table_name) else {
                continue;
            };
            for declaration in &meta.indexes {
                let redb_name = super::RedbPersistence::relational_index_table_name(
                    &table_name,
                    &declaration.name,
                );
                let definition: TableDefinition<super::DurableRelationalIndexKey, &[u8]> =
                    TableDefinition::new(redb_name.as_str());
                let read = database.begin_read().map_err(|error| error.to_string())?;
                let table = match read.open_table(definition) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => continue,
                    Err(error) => return Err(error.to_string()),
                };
                let mut live = Vec::new();
                for entry in table.iter().map_err(|error| error.to_string())? {
                    let (key, value) = entry.map_err(|error| error.to_string())?;
                    let deleted =
                        super::RedbPersistence::decode_exact::<Option<super::TxId>>(value.value())
                            .map_err(|error| error.to_string())?;
                    if deleted.is_some() {
                        continue;
                    }
                    let position = key
                        .value()
                        .decode_canonical()
                        .map_err(|error| error.to_string())?;
                    live.push((position, value.value().to_vec()));
                }
                if busiest
                    .as_ref()
                    .is_none_or(|(_, most)| live.len() > most.len())
                {
                    busiest = Some((redb_name, live));
                }
            }
        }
        busiest.ok_or_else(|| "fixture has no live relational index posting".to_owned())
    }

    /// Files the first live posting whose key differs from the first key under
    /// that first key instead, so the index names a row that does not belong
    /// there while the row itself is untouched.
    fn damage_misdirected_relational_index_posting(
        database: &redb::Database,
    ) -> std::result::Result<(), String> {
        let (redb_name, postings) = busiest_index_postings(database)?;
        let Some((anchor, _)) = postings.first() else {
            return Err("fixture has no live relational index posting".to_owned());
        };
        let Some((stranger, visibility)) = postings
            .iter()
            .find(|(position, _)| position.key != anchor.key)
        else {
            return Err("fixture has only one distinct index key to misdirect".to_owned());
        };
        let key = super::DurableRelationalIndexKey::new(
            anchor.key.clone(),
            stranger.row_id,
            stranger.created_tx,
        )
        .map_err(|error| error.to_string())?;
        let definition: TableDefinition<super::DurableRelationalIndexKey, &[u8]> =
            TableDefinition::new(redb_name.as_str());
        let write = database.begin_write().map_err(|error| error.to_string())?;
        #[cfg(any(test, feature = "test-seams"))]
        let mut graph_publication_attempt =
            crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
        {
            let mut table = write
                .open_table(definition)
                .map_err(|error| error.to_string())?;
            table
                .insert(&key, visibility.as_slice())
                .map_err(|error| error.to_string())?;
        }
        {
            #[cfg(any(test, feature = "test-seams"))]
            let graph_commit_result = write.commit();
            #[cfg(any(test, feature = "test-seams"))]
            if graph_commit_result.is_ok() {
                graph_publication_attempt.committed();
            }
            graph_commit_result
        }
        .map_err(|error| error.to_string())
    }

    /// The declared relational index holding the most live postings, and the
    /// first of those postings. One rule, used both by the defect that removes
    /// a posting and by the proof that wants to name the index it damaged.
    fn busiest_live_relational_index(
        database: &redb::Database,
    ) -> std::result::Result<Option<BusiestLiveIndex>, String> {
        let metadata = damage_table_metadata(database)?;
        let mut table_names = metadata.keys().cloned().collect::<Vec<_>>();
        table_names.sort();
        let mut busiest: Option<BusiestLiveIndex> = None;
        for table_name in table_names {
            let Some(meta) = metadata.get(&table_name) else {
                continue;
            };
            for declaration in &meta.indexes {
                let redb_name = super::RedbPersistence::relational_index_table_name(
                    &table_name,
                    &declaration.name,
                );
                let definition: TableDefinition<super::DurableRelationalIndexKey, &[u8]> =
                    TableDefinition::new(redb_name.as_str());
                let read = database.begin_read().map_err(|error| error.to_string())?;
                let table = match read.open_table(definition) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => continue,
                    Err(error) => return Err(error.to_string()),
                };
                let mut live = 0usize;
                let mut first_live = None;
                for entry in table.iter().map_err(|error| error.to_string())? {
                    let (key, value) = entry.map_err(|error| error.to_string())?;
                    let deleted =
                        super::RedbPersistence::decode_exact::<Option<super::TxId>>(value.value())
                            .map_err(|error| error.to_string())?;
                    if deleted.is_some() {
                        continue;
                    }
                    live += 1;
                    if first_live.is_none() {
                        first_live = Some(key.value().0.clone());
                    }
                }
                if let Some(key) = first_live
                    && busiest
                        .as_ref()
                        .is_none_or(|busiest| live > busiest.live_postings)
                {
                    busiest = Some(BusiestLiveIndex {
                        live_postings: live,
                        table: table_name.clone(),
                        index: declaration.name.clone(),
                        redb_name,
                        first_live_key: key,
                    });
                }
            }
        }
        Ok(busiest)
    }

    struct BusiestLiveIndex {
        live_postings: usize,
        table: String,
        index: String,
        redb_name: String,
        first_live_key: Vec<u8>,
    }

    /// Removes one live posting from the busiest declared index in the store.
    /// The row it points at stays exactly where it is, so the store's logical
    /// content is untouched and only the physical index disagrees.
    fn damage_missing_relational_index_posting(
        database: &redb::Database,
    ) -> std::result::Result<(), String> {
        let Some(busiest) = busiest_live_relational_index(database)? else {
            return Err("fixture has no live relational index posting".to_owned());
        };
        let BusiestLiveIndex {
            redb_name,
            first_live_key: key,
            ..
        } = busiest;
        let definition: TableDefinition<super::DurableRelationalIndexKey, &[u8]> =
            TableDefinition::new(redb_name.as_str());
        let write = database.begin_write().map_err(|error| error.to_string())?;
        #[cfg(any(test, feature = "test-seams"))]
        let mut graph_publication_attempt =
            crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
        {
            let mut table = write
                .open_table(definition)
                .map_err(|error| error.to_string())?;
            table
                .remove(&super::DurableRelationalIndexKey(key))
                .map_err(|error| error.to_string())?;
        }
        {
            #[cfg(any(test, feature = "test-seams"))]
            let graph_commit_result = write.commit();
            #[cfg(any(test, feature = "test-seams"))]
            if graph_commit_result.is_ok() {
                graph_publication_attempt.committed();
            }
            graph_commit_result
        }
        .map_err(|error| error.to_string())
    }

    fn first_relational_row(
        database: &redb::Database,
    ) -> std::result::Result<(String, super::TableMeta, Vec<u8>, super::VersionedRow), String> {
        let metadata = damage_table_metadata(database)?;
        let mut table_names = metadata.keys().cloned().collect::<Vec<_>>();
        table_names.sort();
        let read = database.begin_read().map_err(|error| error.to_string())?;
        for table_name in table_names {
            let redb_name = super::RedbPersistence::rel_table_name(&table_name);
            let definition: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(redb_name.as_str());
            let table = match read.open_table(definition) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => continue,
                Err(error) => return Err(error.to_string()),
            };
            let mut entries = table.iter().map_err(|error| error.to_string())?;
            if let Some(entry) = entries.next() {
                let (key, value) = entry.map_err(|error| error.to_string())?;
                let table_meta = metadata
                    .get(&table_name)
                    .cloned()
                    .ok_or_else(|| "fixture table metadata disappeared".to_owned())?;
                let row =
                    super::RedbPersistence::decode_versioned_row(value.value(), Some(&table_meta))
                        .map_err(|error| error.to_string())?;
                return Ok((table_name, table_meta, key.value().to_vec(), row));
            }
        }
        Err("fixture contains no relational row".to_owned())
    }

    /// Overwrites one stored relational row with the supplied replacement.
    fn write_relational_row(
        database: &redb::Database,
        table_name: &str,
        table_meta: &super::TableMeta,
        key: &[u8],
        row: &super::VersionedRow,
    ) -> std::result::Result<(), String> {
        let encoded = super::RedbPersistence::encode_versioned_row(row, Some(table_meta))
            .map_err(|error| error.to_string())?;
        let redb_name = super::RedbPersistence::rel_table_name(table_name);
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(redb_name.as_str());
        let write = database.begin_write().map_err(|error| error.to_string())?;
        #[cfg(any(test, feature = "test-seams"))]
        let mut graph_publication_attempt =
            crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
        {
            let mut table = write
                .open_table(definition)
                .map_err(|error| error.to_string())?;
            table
                .insert(key, encoded.as_slice())
                .map_err(|error| error.to_string())?;
        }
        {
            #[cfg(any(test, feature = "test-seams"))]
            let graph_commit_result = write.commit();
            #[cfg(any(test, feature = "test-seams"))]
            if graph_commit_result.is_ok() {
                graph_publication_attempt.committed();
            }
            graph_commit_result
        }
        .map_err(|error| error.to_string())
    }

    /// Plants `planted` in the vector cell of the first stored row whose table
    /// declares a vector column the predicate accepts and which currently
    /// holds a real embedding there. Writing through the production row codec
    /// is what makes the damage durable: nothing marks the row, so a later
    /// open diagnoses it from the bytes alone.
    fn damage_vector_cell(
        database: &redb::Database,
        accepts: impl Fn(&contextdb_core::ColumnDef) -> bool,
        planted: super::Value,
        wanted: &str,
    ) -> std::result::Result<(), String> {
        let metadata = damage_table_metadata(database)?;
        let mut table_names = metadata.keys().cloned().collect::<Vec<_>>();
        table_names.sort();
        let read = database.begin_read().map_err(|error| error.to_string())?;
        for table_name in table_names {
            let table_meta = metadata
                .get(&table_name)
                .cloned()
                .ok_or_else(|| "fixture table metadata disappeared".to_owned())?;
            let Some(column) = table_meta
                .columns
                .iter()
                .find(|column| {
                    matches!(column.column_type, super::ColumnType::Vector(_)) && accepts(column)
                })
                .map(|column| column.name.clone())
            else {
                continue;
            };
            let redb_name = super::RedbPersistence::rel_table_name(&table_name);
            let definition: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(redb_name.as_str());
            let table = match read.open_table(definition) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => continue,
                Err(error) => return Err(error.to_string()),
            };
            let mut damaged = None;
            for entry in table.iter().map_err(|error| error.to_string())? {
                let (key, value) = entry.map_err(|error| error.to_string())?;
                let mut row =
                    super::RedbPersistence::decode_versioned_row(value.value(), Some(&table_meta))
                        .map_err(|error| error.to_string())?;
                if !matches!(row.values.get(&column), Some(super::Value::Vector(_))) {
                    continue;
                }
                row.values.insert(column.clone(), planted.clone());
                damaged = Some((key.value().to_vec(), row));
                break;
            }
            drop(table);
            if let Some((key, row)) = damaged {
                drop(read);
                return write_relational_row(database, &table_name, &table_meta, &key, &row);
            }
        }
        Err(format!(
            "fixture holds no embedding in a vector cell of {wanted}"
        ))
    }

    pub(super) fn damage_f32_row_projection_for_test(
        database: &redb::Database,
    ) -> std::result::Result<(), String> {
        let metadata = damage_table_metadata(database)?;
        let mut table_names = metadata.keys().cloned().collect::<Vec<_>>();
        table_names.sort();
        let read = database.begin_read().map_err(|error| error.to_string())?;
        for table_name in table_names {
            let table_meta = metadata
                .get(&table_name)
                .cloned()
                .ok_or_else(|| "fixture table metadata disappeared".to_owned())?;
            let Some(column) = table_meta
                .columns
                .iter()
                .find(|column| {
                    matches!(column.column_type, super::ColumnType::Vector(_))
                        && matches!(column.quantization, super::VectorQuantization::F32)
                })
                .map(|column| column.name.clone())
            else {
                continue;
            };
            let redb_name = super::RedbPersistence::rel_table_name(&table_name);
            let definition: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(redb_name.as_str());
            let table = match read.open_table(definition) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => continue,
                Err(error) => return Err(error.to_string()),
            };
            let mut damaged = None;
            for entry in table.iter().map_err(|error| error.to_string())? {
                let (key, value) = entry.map_err(|error| error.to_string())?;
                let mut row =
                    super::RedbPersistence::decode_versioned_row(value.value(), Some(&table_meta))
                        .map_err(|error| error.to_string())?;
                let Some(super::Value::Vector(vector)) = row.values.get_mut(&column) else {
                    continue;
                };
                let component = vector
                    .first_mut()
                    .ok_or_else(|| "fixture F32 row projection is empty".to_owned())?;
                *component = if component.is_finite() {
                    *component + 0.5
                } else {
                    0.5
                };
                damaged = Some((key.value().to_vec(), row));
                break;
            }
            drop(table);
            if let Some((key, row)) = damaged {
                drop(read);
                return write_relational_row(database, &table_name, &table_meta, &key, &row);
            }
        }
        Err("fixture holds no full-precision vector row projection".to_owned())
    }

    fn damage_writable_sanitizer(database: &redb::Database) -> std::result::Result<(), String> {
        let (table_name, table_meta, key, mut row) = first_relational_row(database)?;
        let mut column = "__unknown_fixture_column".to_owned();
        while row.values.contains_key(&column) {
            column.push('_');
        }
        row.values
            .insert(column, super::Value::Text("durable".to_owned()));
        let encoded = super::RedbPersistence::encode_versioned_row(&row, Some(&table_meta))
            .map_err(|error| error.to_string())?;
        let redb_name = super::RedbPersistence::rel_table_name(&table_name);
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(redb_name.as_str());
        let write = database.begin_write().map_err(|error| error.to_string())?;
        #[cfg(any(test, feature = "test-seams"))]
        let mut graph_publication_attempt =
            crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
        {
            let mut table = write
                .open_table(definition)
                .map_err(|error| error.to_string())?;
            table
                .insert(key.as_slice(), encoded.as_slice())
                .map_err(|error| error.to_string())?;
        }
        {
            #[cfg(any(test, feature = "test-seams"))]
            let graph_commit_result = write.commit();
            #[cfg(any(test, feature = "test-seams"))]
            if graph_commit_result.is_ok() {
                graph_publication_attempt.committed();
            }
            graph_commit_result
        }
        .map_err(|error| error.to_string())
    }

    /// Which relational index a posting-removal defect will act on: the one
    /// holding the most live postings, chosen exactly as
    /// [`DurableStoreDamage::MissingRelationalIndexPosting`] chooses it.
    ///
    /// A proof that damages "an index" and then reads through "an index" is
    /// only a proof if the two are the same one. This lets the proof say which
    /// index it expects to be damaged and fail loudly when the fixture picks a
    /// different one, instead of passing or failing for a reason nobody sees.
    pub fn busiest_relational_index_for_test(
        path: &Path,
    ) -> std::result::Result<(String, String), String> {
        let database = open_damage_fixture(path)?;
        let Some(busiest) = busiest_live_relational_index(&database)? else {
            return Err("fixture has no live relational index posting".to_owned());
        };
        Ok((busiest.table, busiest.index))
    }

    /// Rewrites one indexed column of a NAMED row's stored body, leaving every
    /// index that covers it saying what it said before.
    ///
    /// This is the divergence a reader cannot see coming. Moving a posting is
    /// caught by the window reconciliation, which compares the canonical
    /// posting tree against the physical index before a single identity is
    /// delivered. Changing the ROW leaves both of those agreeing with each
    /// other and disagreeing only with the body they point at -- so the only
    /// thing that can catch it is the check made where the row is published.
    pub fn contradict_indexed_column_for_test(
        path: &Path,
        table: &str,
        row_id: super::RowId,
        column: &str,
        value: super::Value,
    ) -> std::result::Result<(), String> {
        let database = open_damage_fixture(path)?;
        let metadata = damage_table_metadata(&database)?;
        let meta = metadata
            .get(table)
            .ok_or_else(|| format!("fixture has no table {table}"))?
            .clone();
        let redb_name = super::RedbPersistence::rel_table_name(table);
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(redb_name.as_str());
        let mut found = None;
        {
            let read = database.begin_read().map_err(|error| error.to_string())?;
            let rows = read
                .open_table(definition)
                .map_err(|error| error.to_string())?;
            for entry in rows.iter().map_err(|error| error.to_string())? {
                let (key, stored) = entry.map_err(|error| error.to_string())?;
                let row = super::RedbPersistence::decode_versioned_row(stored.value(), Some(&meta))
                    .map_err(|error| error.to_string())?;
                if row.row_id == row_id && row.deleted_tx.is_none() {
                    found = Some((key.value().to_vec(), row));
                    break;
                }
            }
        }
        let Some((key, mut row)) = found else {
            return Err(format!("table {table} has no live row {row_id}"));
        };
        if !row.values.contains_key(column) {
            return Err(format!("row {row_id} has no column {column}"));
        }
        row.values.insert(column.to_owned(), value);
        write_relational_row(&database, table, &meta, &key, &row)
    }

    /// Files one live posting of a NAMED index under another row's indexed key,
    /// leaving both row bodies exactly as they are.
    ///
    /// The defects above choose an index by counting live postings, which on a
    /// table whose primary key and declared index cover the same rows picks
    /// whichever is met first -- so a proof that must damage the index its read
    /// is served from, and must know which row it damaged, says both here
    /// rather than hoping. Only the physical index is touched: the canonical
    /// posting tree and the row bodies are untouched, which is the divergence a
    /// reader has to catch on its own.
    pub fn misfile_relational_index_posting_for_test(
        path: &Path,
        table: &str,
        index: &str,
        row_id: super::RowId,
        under_the_key_of_row_id: super::RowId,
    ) -> std::result::Result<(), String> {
        let database = open_damage_fixture(path)?;
        let metadata = damage_table_metadata(&database)?;
        let meta = metadata
            .get(table)
            .ok_or_else(|| format!("fixture has no table {table}"))?;
        if !meta.indexes.iter().any(|entry| entry.name == index) {
            return Err(format!("table {table} has no declared index {index}"));
        }
        let redb_name = super::RedbPersistence::relational_index_table_name(table, index);
        let definition: TableDefinition<super::DurableRelationalIndexKey, &[u8]> =
            TableDefinition::new(redb_name.as_str());
        let mut moved = None;
        let mut destination = None;
        {
            let read = database.begin_read().map_err(|error| error.to_string())?;
            let table = read
                .open_table(definition)
                .map_err(|error| error.to_string())?;
            for entry in table.iter().map_err(|error| error.to_string())? {
                let (key, value) = entry.map_err(|error| error.to_string())?;
                let deleted =
                    super::RedbPersistence::decode_exact::<Option<super::TxId>>(value.value())
                        .map_err(|error| error.to_string())?;
                if deleted.is_some() {
                    continue;
                }
                let position = key
                    .value()
                    .decode_canonical()
                    .map_err(|error| error.to_string())?;
                if position.row_id == row_id {
                    moved = Some((key.value().0.clone(), position, value.value().to_vec()));
                } else if position.row_id == under_the_key_of_row_id {
                    destination = Some(position.key.clone());
                }
            }
        }
        let Some((physical_key, position, visibility)) = moved else {
            return Err(format!(
                "index {index} has no live posting for row {row_id}"
            ));
        };
        let Some(destination) = destination else {
            return Err(format!(
                "index {index} has no live posting for row {under_the_key_of_row_id}"
            ));
        };
        let misfiled = super::DurableRelationalIndexKey::new(
            destination,
            position.row_id,
            position.created_tx,
        )
        .map_err(|error| error.to_string())?;
        let write = database.begin_write().map_err(|error| error.to_string())?;
        #[cfg(any(test, feature = "test-seams"))]
        let mut graph_publication_attempt =
            crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
        {
            let mut table = write
                .open_table(definition)
                .map_err(|error| error.to_string())?;
            table
                .remove(&super::DurableRelationalIndexKey(physical_key))
                .map_err(|error| error.to_string())?;
            table
                .insert(&misfiled, visibility.as_slice())
                .map_err(|error| error.to_string())?;
        }
        {
            #[cfg(any(test, feature = "test-seams"))]
            let graph_commit_result = write.commit();
            #[cfg(any(test, feature = "test-seams"))]
            if graph_commit_result.is_ok() {
                graph_publication_attempt.committed();
            }
            graph_commit_result
        }
        .map_err(|error| error.to_string())
    }

    /// Raw damage to one live posting of a relational index, made without the
    /// producer, so the index table's root no longer matches its witness.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum IndexPostingDamage {
        /// The row's live posting is removed.
        Remove,
        /// The row's live posting is marked retired.
        Hide,
    }

    pub use super::historical_index_commitment::{
        refuse_next_commitment_admission_for_test, skip_next_postings_fold_for_test,
        take_commitment_admitted_bytes_for_test,
    };

    pub use super::historical_index_commitment::skip_next_index_retirement_for_test;

    /// What a store still holds for `index` of `table`: whether its historical
    /// and current posting tables exist with any entry, and whether either of
    /// its commitment states is stored.
    pub fn historical_index_inventory_for_test(
        path: &Path,
        table: &str,
        index: &str,
    ) -> std::result::Result<(bool, bool, bool), String> {
        let database = open_damage_fixture(path)?;
        let read = database.begin_read().map_err(|error| error.to_string())?;
        let holds = |name: String| -> std::result::Result<bool, String> {
            let definition: TableDefinition<super::DurableRelationalIndexKey, &[u8]> =
                TableDefinition::new(name.as_str());
            match read.open_table(definition) {
                Ok(postings) => Ok(!redb::ReadableTableMetadata::is_empty(&postings)
                    .map_err(|error| error.to_string())?),
                Err(redb::TableError::TableDoesNotExist(_)) => Ok(false),
                Err(error) => Err(error.to_string()),
            }
        };
        Ok((
            holds(super::RedbPersistence::relational_index_table_name(
                table, index,
            ))?,
            holds(super::RedbPersistence::current_relational_index_table_name(
                table, index,
            ))?,
            super::historical_index_commitment::states_stored_for_test(&read, table, index)
                .map_err(|error| error.to_string())?,
        ))
    }

    /// Change the stored postings commitment of `index` of `table` without
    /// its publication, as storage damage would.
    pub fn damage_historical_index_commitment_for_test(
        path: &Path,
        table: &str,
        index: &str,
    ) -> std::result::Result<(), String> {
        let database = open_damage_fixture(path)?;
        let write = database.begin_write().map_err(|error| error.to_string())?;
        super::historical_index_commitment::damage_postings_state_for_test(&write, table, index)
            .map_err(|error| error.to_string())?;
        write.commit().map_err(|error| error.to_string())
    }

    /// Damage the live posting of `row_id` in `index` of `table`.
    pub fn damage_relational_index_posting_for_test(
        path: &Path,
        table: &str,
        index: &str,
        row_id: super::RowId,
        damage: IndexPostingDamage,
    ) -> std::result::Result<(), String> {
        let database = open_damage_fixture(path)?;
        let redb_name = super::RedbPersistence::relational_index_table_name(table, index);
        let definition: TableDefinition<super::DurableRelationalIndexKey, &[u8]> =
            TableDefinition::new(redb_name.as_str());
        let mut found = None;
        {
            let read = database.begin_read().map_err(|error| error.to_string())?;
            let index_table = read
                .open_table(definition)
                .map_err(|error| error.to_string())?;
            for entry in index_table.iter().map_err(|error| error.to_string())? {
                let (key, value) = entry.map_err(|error| error.to_string())?;
                let deleted =
                    super::RedbPersistence::decode_exact::<Option<super::TxId>>(value.value())
                        .map_err(|error| error.to_string())?;
                if deleted.is_some() {
                    continue;
                }
                let position = key
                    .value()
                    .decode_canonical()
                    .map_err(|error| error.to_string())?;
                if position.row_id == row_id {
                    found = Some((key.value().0.clone(), position.created_tx));
                    break;
                }
            }
        }
        let Some((physical_key, created_tx)) = found else {
            return Err(format!(
                "index {index} has no live posting for row {row_id}"
            ));
        };
        let write = database.begin_write().map_err(|error| error.to_string())?;
        {
            let mut index_table = write
                .open_table(definition)
                .map_err(|error| error.to_string())?;
            let key = super::DurableRelationalIndexKey(physical_key);
            match damage {
                IndexPostingDamage::Remove => {
                    index_table
                        .remove(&key)
                        .map_err(|error| error.to_string())?;
                }
                IndexPostingDamage::Hide => {
                    let retired = super::RedbPersistence::encode(&Some(super::TxId(
                        created_tx.0.saturating_add(1),
                    )))
                    .map_err(|error| error.to_string())?;
                    index_table
                        .insert(&key, retired.as_slice())
                        .map_err(|error| error.to_string())?;
                }
            }
        }
        write.commit().map_err(|error| error.to_string())
    }

    /// A raw entry added to the physical row table of `table`, made without
    /// the producer.
    pub fn damage_relational_row_table_for_test(
        path: &Path,
        table: &str,
    ) -> std::result::Result<(), String> {
        let database = open_damage_fixture(path)?;
        let name = super::RedbPersistence::rel_table_name(table);
        let write = database.begin_write().map_err(|error| error.to_string())?;
        {
            let mut rows = write
                .open_table(TableDefinition::<&[u8], &[u8]>::new(name.as_str()))
                .map_err(|error| error.to_string())?;
            rows.insert(b"\xffraw-damage".as_slice(), b"x".as_slice())
                .map_err(|error| error.to_string())?;
        }
        write.commit().map_err(|error| error.to_string())
    }

    /// How the commit-index entry of a store's last commit disagrees with the
    /// deleted edge whose tombstone that commit wrote.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum TombstoneCommitDamage {
        /// The entry names a different transaction than the tombstone's.
        MismatchedTransaction,
        /// The entry is gone.
        MissingEntry,
    }

    /// Rewrites the last commit-index entry -- the fixture's edge deletion --
    /// in the store AND in its committed identity, so the identity still
    /// authenticates the store and only the tombstone cross-check can see
    /// the disagreement. A later transaction keeps the index ordered.
    pub fn damage_last_commit_for_edge_tombstone_for_test(
        path: &Path,
        damage: TombstoneCommitDamage,
    ) -> std::result::Result<(), String> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|error| error.to_string())?;
        let owner = super::writer_file::LockedWriterFile::lock(file, path.parent())
            .map_err(|error| error.to_string())?;
        let database = super::redb_builder()
            .open_with_backend(owner.view())
            .map_err(|error| error.to_string())?;
        let (lsn, tx) = {
            let read = database.begin_read().map_err(|error| error.to_string())?;
            let table = read
                .open_table(super::COMMIT_INDEX_TABLE)
                .map_err(|error| error.to_string())?;
            let (lsn, tx) = table
                .last()
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "fixture has no committed transaction".to_owned())?;
            (lsn.value(), tx.value())
        };
        let write = database.begin_write().map_err(|error| error.to_string())?;
        #[cfg(any(test, feature = "test-seams"))]
        let mut graph_publication_attempt =
            crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
        let mutation = {
            let mut table = write
                .open_table(super::COMMIT_INDEX_TABLE)
                .map_err(|error| error.to_string())?;
            match damage {
                TombstoneCommitDamage::MismatchedTransaction => {
                    let other = tx
                        .checked_add(1)
                        .ok_or_else(|| "fixture transaction overflow".to_owned())?;
                    table
                        .insert(lsn, other)
                        .map_err(|error| error.to_string())?;
                    super::content_identity::commit_index_mutation(
                        contextdb_core::Lsn(lsn),
                        contextdb_core::TxId(other),
                    )
                }
                TombstoneCommitDamage::MissingEntry => {
                    table.remove(lsn).map_err(|error| error.to_string())?;
                    super::content_identity::remove_commit_index_mutation(contextdb_core::Lsn(lsn))
                }
            }
        };
        super::RedbPersistence::apply_content_identity_mutations_in_write(
            &write,
            &[mutation],
            &mut super::content_identity::ProofAdmission::new(None)
                .with_scratch_owner(Some(std::sync::Arc::downgrade(&owner))),
        )
        .map_err(|error| error.to_string())?;
        // Keep unrelated open-time controls consistent so the requested
        // tombstone/commit-index cross-check diagnoses this fixture.
        super::RedbPersistence::write_writer_recovery_bounds_in_write(&write, None)
            .map_err(|error| error.to_string())?;
        super::RedbPersistence::write_commit_directory_root_in_write(&write)
            .map_err(|error| error.to_string())?;
        super::RedbPersistence::write_read_bounds_roots_in_write(&write)
            .map_err(|error| error.to_string())?;
        {
            #[cfg(any(test, feature = "test-seams"))]
            let graph_commit_result = write.commit();
            #[cfg(any(test, feature = "test-seams"))]
            if graph_commit_result.is_ok() {
                graph_publication_attempt.committed();
            }
            graph_commit_result
        }
        .map_err(|error| error.to_string())
    }

    /// Creates the requested defect in Redb itself. No marker, sidecar, or
    /// test-only decoder participates in later diagnosis.
    pub fn prepare_durable_store_damage_for_test(
        path: &Path,
        damage: DurableStoreDamage,
    ) -> std::result::Result<(), String> {
        let database = open_damage_fixture(path)?;
        let result = match damage {
            DurableStoreDamage::MalformedRecord => {
                damage_format_marker(&database, &[0xff, 0x00, 0xfe])
            }
            DurableStoreDamage::LegacyLayout => {
                let legacy = super::RedbPersistence::encode(&"0.9.0".to_owned())
                    .map_err(|error| error.to_string())?;
                damage_format_marker(&database, &legacy)
            }
            DurableStoreDamage::NonMonotonicCommitIndex => damage_commit_index(&database),
            DurableStoreDamage::InvalidVectorReference => damage_vector_reference(&database),
            DurableStoreDamage::RowVectorDivergence => {
                damage_vector_divergence(&database, super::VectorQuantization::F32)
            }
            DurableStoreDamage::WritableSanitizerWouldChange => {
                damage_writable_sanitizer(&database)
            }
            DurableStoreDamage::NullInRequiredVectorCell => damage_vector_cell(
                &database,
                |column| {
                    !column.nullable
                        && matches!(column.quantization, super::VectorQuantization::F32)
                },
                super::Value::Null,
                "a full-precision vector column declared NOT NULL",
            ),
            DurableStoreDamage::NonVectorValueInVectorCell => damage_vector_cell(
                &database,
                |_| true,
                super::Value::Int64(7),
                "a vector column",
            ),
            DurableStoreDamage::MissingQuantizedVectorEntry => {
                damage_missing_quantized_vector_entry(&database)
            }
            DurableStoreDamage::Sq8VectorDivergence => {
                damage_vector_divergence(&database, super::VectorQuantization::SQ8)
            }
            DurableStoreDamage::Sq4VectorDivergence => {
                damage_vector_divergence(&database, super::VectorQuantization::SQ4)
            }
            DurableStoreDamage::RowVisibilitySuppression => {
                damage_row_visibility_suppression(&database)
            }
            DurableStoreDamage::MissingRelationalIndexPosting => {
                damage_missing_relational_index_posting(&database)
            }
            DurableStoreDamage::MisdirectedRelationalIndexPosting => {
                damage_misdirected_relational_index_posting(&database)
            }
        };
        drop(database);
        result
    }

    /// Damage to the stored history a reader serves on request. Each case
    /// changes only the physical history tables through a valid Redb write;
    /// the committed content identity is left exactly as the writer wrote it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum HistoryDamage {
        /// Removes the last ordinary row-insert record from the change log.
        RemovedRowInsertRecord,
        /// Removes the whole change-log table.
        MissingChangeLogTable,
        /// Adds a copy of the last change record under a new position.
        ExtraChangeRecord,
        /// Swaps the stored values of the first two schema statements that
        /// share one commit position.
        SwappedSameCommitSchemaStatements,
        /// Swaps the stored values of the first two change records that share
        /// one commit position and differ.
        SwappedSameCommitChangeRecords,
        /// Removes a commit-index entry that is not the last one.
        RemovedCommitIndexMember,
        /// Raises the transaction id of the last commit-index entry, keeping
        /// the index in order.
        RaisedLastCommitTransaction,
        /// Moves the last schema statement to a later commit position.
        MovedLastSchemaStatementLater,
        /// Rewrites a middle commit-index entry's transaction id to zero, out
        /// of transaction order.
        LoweredMiddleCommitTransaction,
        /// Appends one commit-index entry past the last one, in transaction
        /// order, that was never committed.
        AppendedUncommittedCommitEntry,
    }

    pub fn prepare_history_damage_for_test(
        path: &Path,
        damage: HistoryDamage,
    ) -> std::result::Result<(), String> {
        let database = open_damage_fixture(path)?;
        let result = damage_history(&database, damage);
        drop(database);
        result
    }

    fn damage_history(
        database: &redb::Database,
        damage: HistoryDamage,
    ) -> std::result::Result<(), String> {
        let text = |error: &dyn std::fmt::Display| error.to_string();
        let read = database.begin_read().map_err(|e| text(&e))?;
        let changes = match read.open_table(super::CHANGE_LOG_TABLE) {
            Ok(table) => table
                .iter()
                .map_err(|e| text(&e))?
                .map(|entry| {
                    entry
                        .map(|(key, value)| (key.value().to_owned(), value.value().to_vec()))
                        .map_err(|e| text(&e))
                })
                .collect::<std::result::Result<Vec<_>, _>>()?,
            Err(redb::TableError::TableDoesNotExist(_)) => Vec::new(),
            Err(error) => return Err(error.to_string()),
        };
        let schema = read
            .open_table(super::DDL_LOG_TABLE)
            .map_err(|e| text(&e))?
            .iter()
            .map_err(|e| text(&e))?
            .map(|entry| {
                entry
                    .map(|(key, value)| (key.value().to_owned(), value.value().to_vec()))
                    .map_err(|e| text(&e))
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let commits = read
            .open_table(super::COMMIT_INDEX_TABLE)
            .map_err(|e| text(&e))?
            .iter()
            .map_err(|e| text(&e))?
            .map(|entry| {
                entry
                    .map(|(lsn, tx)| (lsn.value(), tx.value()))
                    .map_err(|e| text(&e))
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(read);
        let same_commit_pair = |records: &[(String, Vec<u8>)]| {
            records
                .windows(2)
                .find(|pair| {
                    pair[0].0.split(':').next() == pair[1].0.split(':').next()
                        && pair[0].1 != pair[1].1
                })
                .map(|pair| (pair[0].clone(), pair[1].clone()))
        };
        let write = database.begin_write().map_err(|e| text(&e))?;
        #[cfg(any(test, feature = "test-seams"))]
        let mut graph_publication_attempt =
            crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
        match damage {
            HistoryDamage::RemovedRowInsertRecord => {
                let (key, _) = changes
                    .iter()
                    .rev()
                    .find(|(_, value)| {
                        matches!(
                            super::RedbPersistence::decode_stored_change_log(value)
                                .map(|record| record.entry),
                            Ok(super::ChangeLogEntry::RowInsert { .. })
                        )
                    })
                    .ok_or("fixture has no row-insert history record")?;
                let mut table = write
                    .open_table(super::CHANGE_LOG_TABLE)
                    .map_err(|e| text(&e))?;
                table.remove(key.as_str()).map_err(|e| text(&e))?;
            }
            HistoryDamage::MissingChangeLogTable => {
                if changes.is_empty() {
                    return Err("fixture has no change history to remove".to_owned());
                }
                write
                    .delete_table(super::CHANGE_LOG_TABLE)
                    .map_err(|e| text(&e))?;
            }
            HistoryDamage::ExtraChangeRecord => {
                let (key, value) = changes.last().ok_or("fixture has no change history")?;
                let lsn = key.split(':').next().ok_or("change key has no position")?;
                let mut table = write
                    .open_table(super::CHANGE_LOG_TABLE)
                    .map_err(|e| text(&e))?;
                table
                    .insert(format!("{lsn}:999999").as_str(), value.as_slice())
                    .map_err(|e| text(&e))?;
            }
            HistoryDamage::SwappedSameCommitSchemaStatements => {
                let (first, second) = same_commit_pair(&schema)
                    .ok_or("fixture has no two schema statements in one commit")?;
                let mut table = write
                    .open_table(super::DDL_LOG_TABLE)
                    .map_err(|e| text(&e))?;
                table
                    .insert(first.0.as_str(), second.1.as_slice())
                    .map_err(|e| text(&e))?;
                table
                    .insert(second.0.as_str(), first.1.as_slice())
                    .map_err(|e| text(&e))?;
            }
            HistoryDamage::SwappedSameCommitChangeRecords => {
                let (first, second) = same_commit_pair(&changes)
                    .ok_or("fixture has no two change records in one commit")?;
                let mut table = write
                    .open_table(super::CHANGE_LOG_TABLE)
                    .map_err(|e| text(&e))?;
                table
                    .insert(first.0.as_str(), second.1.as_slice())
                    .map_err(|e| text(&e))?;
                table
                    .insert(second.0.as_str(), first.1.as_slice())
                    .map_err(|e| text(&e))?;
            }
            HistoryDamage::RemovedCommitIndexMember => {
                if commits.len() < 3 {
                    return Err("fixture needs three commits".to_owned());
                }
                let (lsn, _) = commits[commits.len() / 2];
                let mut table = write
                    .open_table(super::COMMIT_INDEX_TABLE)
                    .map_err(|e| text(&e))?;
                table.remove(lsn).map_err(|e| text(&e))?;
            }
            HistoryDamage::RaisedLastCommitTransaction => {
                let (lsn, tx) = *commits.last().ok_or("fixture has no commits")?;
                let mut table = write
                    .open_table(super::COMMIT_INDEX_TABLE)
                    .map_err(|e| text(&e))?;
                table.insert(lsn, tx + 5).map_err(|e| text(&e))?;
            }
            HistoryDamage::LoweredMiddleCommitTransaction => {
                if commits.len() < 3 {
                    return Err("fixture needs three commits".to_owned());
                }
                let (lsn, _) = commits[commits.len() / 2];
                let mut table = write
                    .open_table(super::COMMIT_INDEX_TABLE)
                    .map_err(|e| text(&e))?;
                table.insert(lsn, 0).map_err(|e| text(&e))?;
            }
            HistoryDamage::AppendedUncommittedCommitEntry => {
                let (lsn, tx) = *commits.last().ok_or("fixture has no commits")?;
                let mut table = write
                    .open_table(super::COMMIT_INDEX_TABLE)
                    .map_err(|e| text(&e))?;
                table.insert(lsn + 1, tx + 1).map_err(|e| text(&e))?;
            }
            HistoryDamage::MovedLastSchemaStatementLater => {
                let (key, value) = schema.last().ok_or("fixture has no schema history")?;
                let lsn = key
                    .split(':')
                    .next()
                    .and_then(|lsn| lsn.parse::<u64>().ok())
                    .ok_or("schema key has no position")?;
                let mut table = write
                    .open_table(super::DDL_LOG_TABLE)
                    .map_err(|e| text(&e))?;
                table.remove(key.as_str()).map_err(|e| text(&e))?;
                table
                    .insert(format!("{:020}", lsn + 1000).as_str(), value.as_slice())
                    .map_err(|e| text(&e))?;
            }
        }
        {
            #[cfg(any(test, feature = "test-seams"))]
            let graph_commit_result = write.commit();
            #[cfg(any(test, feature = "test-seams"))]
            if graph_commit_result.is_ok() {
                graph_publication_attempt.committed();
            }
            graph_commit_result
        }
        .map_err(|e| text(&e))
    }

    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn load_vector_version_at_snapshot_for_test(
        path: &Path,
        index: &super::VectorIndexRef,
        row_id: super::RowId,
        snapshot: contextdb_core::SnapshotId,
    ) -> std::result::Result<super::VectorEntry, String> {
        let load = super::load_read_image(path).map_err(|error| error.to_string())?;
        let (image, receipt, source) = load.into_parts();
        receipt
            .validate_released()
            .map_err(|error| error.to_string())?;
        let column = image
            .table_meta
            .get(&index.table)
            .and_then(|table| {
                table
                    .columns
                    .iter()
                    .find(|column| column.name == index.column)
            })
            .ok_or_else(|| format!("fixture vector index has no metadata: {index}"))?;
        let dimension = match column.column_type {
            super::ColumnType::Vector(dimension) => dimension,
            _ => return Err(format!("fixture column is not a vector: {index}")),
        };
        let membership = source
            .with_read(|read| {
                let table = read
                    .open_table(super::VECTOR_PARTITION_MEMBERSHIP_TABLE)
                    .map_err(super::RedbPersistence::storage_error)?;
                let mut found = None;
                for item in table
                    .iter()
                    .map_err(super::RedbPersistence::storage_error)?
                {
                    let (_, value) = item.map_err(super::RedbPersistence::storage_error)?;
                    let member: super::VectorPartitionMembershipRecord =
                        super::decode_vector_partition_record_payload(
                            super::VectorPartitionRecordKind::Membership,
                            value.value(),
                        )?;
                    if member.index == *index
                        && member.row_id == row_id
                        && member.visible_from.0 <= snapshot.0
                        && member
                            .deleted_tx
                            .is_none_or(|deleted| deleted.0 > snapshot.0)
                        && found.as_ref().is_none_or(
                            |old: &super::VectorPartitionMembershipRecord| {
                                (
                                    member.visible_from,
                                    member.vector_created_tx,
                                    member.vector_lsn,
                                ) > (old.visible_from, old.vector_created_tx, old.vector_lsn)
                            },
                        )
                    {
                        found = Some(member);
                    }
                }
                found.ok_or_else(|| {
                    contextdb_core::Error::Other(
                        "fixture has no selected durable vector version".into(),
                    )
                })
            })
            .map_err(|error| error.to_string())?;
        let mut loaded = source
            .load_raw_vector_entries_for_memberships(
                std::slice::from_ref(&membership),
                dimension,
                column.quantization,
            )
            .map_err(|error| error.to_string())?;
        loaded
            .pop()
            .ok_or_else(|| "selected durable vector version was not returned".to_owned())
    }
}

/// Bound Redb's process-resident page cache independently of database size.
/// Large blob payloads remain disk-backed instead of accumulating in Redb's
/// one-GiB default cache.
const REDB_CACHE_BYTES: usize = 8 * 1024 * 1024;

/// The capacity a native page cache was installed with and the reserve it
/// settled to. Zero capacity means none was recorded.
#[derive(Default)]
struct CacheInstallation {
    capacity: std::sync::atomic::AtomicUsize,
    reserve: std::sync::atomic::AtomicUsize,
    owner: parking_lot::Mutex<Option<redb::CacheReserveOwner>>,
}

/// The smallest page cache a store is opened with.
///
/// Redb's cache is what lets a write transaction buffer its dirty pages; at
/// zero every buffered page write flushes synchronously, which is not a
/// smaller store but an unusably slow one. So the share a declared limit
/// allows is floored here, and a limit too small to hold the floor is refused
/// at open with the ordinary typed error rather than opening a store that
/// cannot work.
pub(crate) const REDB_CACHE_FLOOR_BYTES: usize = 256 * 1024;

pub(crate) const STORAGE_MAINTENANCE_PAGE_BATCH: usize = 64;
/// Per-commit reclaim limits of a maintenance reclaim step. Ordinary commits keep
/// `STORAGE_MAINTENANCE_PAGE_BATCH`; a reclaim-only maintenance commit has no foreground write to
/// delay, and 4,096 measured the same worst single-commit latency as 64.
const RECLAIM_EXTENTS_PER_STEP: usize = 4096;
const RECLAIM_ENTRIES_PER_STEP: usize = 4096;

thread_local! {
    /// Test-only injection seam: when armed, the NEXT [`RedbPersistence::
    /// compact`]'s post-compact handle recycle (close the redb handle, then
    /// reopen the same on-disk file) fails its reopen, so a test can assert
    /// the on-disk file stays intact and a fresh `RedbPersistence::open`
    /// still recovers every row after a mid-recycle failure. Armed only by
    /// `arm_handle_recycle_reopen_fault_for_test`; default off, so
    /// production reads a thread-local that is never set.
    static HANDLE_RECYCLE_REOPEN_FAULT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// One-shot failure immediately before the received-schema Redb commit.
    /// It proves no private stage publishes memory when durability aborts.
    static RECEIVED_SCHEMA_PRE_COMMIT_FAULT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    /// One-shot failure at the received-schema side-effect checkpoint. The
    /// checkpoint is after the complete core image has been assembled in the
    /// live Redb write transaction and immediately before sink-queue and
    /// trigger-audit persistence; a failed checkpoint must leave all of that
    /// one transaction uncommitted.
    static RECEIVED_SCHEMA_SIDE_EFFECT_PERSISTENCE_FAULT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// One-shot failure after an authoritative purge has staged every point
    /// removal and lifecycle write, immediately before its sole Redb commit.
    /// The write transaction must roll back every copy class together.
    static AUTHORITATIVE_PURGE_PRE_COMMIT_FAULT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static ERASURE_FIRST_TABLE_FAULT: std::cell::Cell<(bool,bool)> = const { std::cell::Cell::new((false,false)) };
}

const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const FORMAT_METADATA_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("metadata");
const RELATIONAL_INDEX_READY_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("__rel_index_ready");
const RELATIONAL_INDEX_COUNT_TABLE: TableDefinition<&[u8], u64> =
    TableDefinition::new("__rel_index_counts");
pub(crate) const CONFIG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("config");
const READ_OPEN_CONFIG_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("read_open_config");
const READ_OPEN_CONFIG_ROOT_KEY: &str = "";
const READ_OPEN_CONFIG_COMMITMENT_KEY: &str = "read_open_config_root_v1";
const READ_CUSTODY_CONFIG_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("read_custody_config");
/// Per-row lineage records grow with deleted history, so a committed image
/// reads them on request from this compact index instead of at open.
const READ_LINEAGE_CONFIG_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("read_lineage_config");
const READ_LINEAGE_CONFIG_COMMITMENT_KEY: &str = "read_lineage_config_root_v1";
const READ_CUSTODY_CONFIG_COMMITMENT_KEY: &str = "read_custody_config_root_v1";
// Derived selectors and their per-primary completion markers share one table.
// A missing selector must never read a neighboring large configuration payload.
const POLICY_HOLD_INDEX_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("sync_policy_hold_selection_v1");

const CHANGE_LOG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("change_log");
const DDL_LOG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("ddl_log");
const COMMIT_INDEX_TABLE: TableDefinition<u64, u64> = TableDefinition::new("commit_index");
const SINK_AUDIT_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("__sink_audit");
const TRIGGER_AUDIT_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("__trigger_audit");
/// When each durable `__trigger_audit` row was written, keyed identically to
/// the audit row itself. Kept beside the history rather than inside them so
/// existing on-disk audit payloads decode unchanged; retention reads it to
/// decide what has aged out. A row with no stamp is never pruned.
const TRIGGER_AUDIT_STAMPS_TABLE: TableDefinition<&str, u64> =
    TableDefinition::new("__trigger_audit_stamps");
const GRAPH_FWD_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("graph_fwd");
const GRAPH_REV_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("graph_rev");
const VECTORS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("vector_entries");
pub(crate) const VECTOR_PARTITION_MEMBERSHIP_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_partition_membership");
/// Small per-membership witnesses, stored away from vector body pages.
const VECTOR_OPEN_DIRECTORY_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_open_directory");
const VECTOR_OPEN_DIRECTORY_ROOT_KEY: &[u8] = b"";
/// The writer's running set commitments over the compact vector directory's
/// memberships and the copied custody index, one fixed-size record per copy.
/// Readers never open it: they recompute the commitment from the copy they
/// scan and compare it with the committed family root.
const COMPACT_INDEX_SET_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("compact_index_set_commitments");
const VECTOR_OPEN_DIRECTORY_COMMITMENT_KEY: &str = "vector_open_directory_root_v1";
const VECTOR_BODY_ROOT_FINGERPRINT_KEY: &str = "vector_body_root_v1";
const VECTOR_REFERENCED_BODY_COUNT_KEY: &str = "vector_referenced_body_count";
const VECTOR_PARTITION_JOURNAL_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_partition_journal");
const VECTOR_PARTITION_TOMBSTONE_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_partition_tombstones");
const SYNC_ROW_SOURCE_LSN_TABLE: TableDefinition<&[u8], u64> =
    TableDefinition::new("sync_row_source_lsn");
const SYNC_ROW_SOURCE_KIND_TABLE: TableDefinition<&[u8], u8> =
    TableDefinition::new("sync_row_source_kind");
const CONTENT_IDENTITY_NODES_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("__content_identity_nodes");
const CONTENT_IDENTITY_ROOTS_TABLE: TableDefinition<u8, &[u8]> =
    TableDefinition::new("__content_identity_roots");
const CONTENT_IDENTITY_METADATA_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("__content_identity_metadata");
const PURGE_PUBLICATION_METADATA_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("__purge_publication_metadata");
const PURGE_PUBLICATION_MARKER_KEY: &str = "committed";
const FORMAT_VERSION_KEY: &str = "format_version";
pub(crate) const CURRENT_FORMAT_VERSION: &str = "1.0.0";
pub(crate) const TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY: &str = "__trigger_audit_next_index";
pub(crate) const TRIGGER_AUDIT_RING_CONFIG_KEY: &str = "__trigger_audit_ring";
const VECTOR_PARTITION_RECORD_MAGIC: &[u8] = b"contextdb-vector-partition";
const VECTOR_PARTITION_RECORD_VERSION: u16 = 1;
// Roots published before the record-size correction used a 64 KiB body plus
// the authenticated header. Keep reading those immutable generations.

/// A pre2-sized persistence-owned reservation. Its accountant/use custody
/// is enclosed by PersistenceProofOwner until all persistence fields are freed.
struct PersistenceBytes {
    accountant: crate::memory_accounting::AccountantHandle,
    bytes: usize,
}

/// Only this private Arc owns persistence-wide writer custody. It has no
/// incoming Weak and no edge back to persistence or schema.
struct PersistenceProofOwner {
    accountant: Option<crate::memory_accounting::StoreAccountantHandle>,
    writer_use: Option<crate::memory_accounting::WriterUse>,
    control: Option<crate::memory_accounting::ControlCredit>,
}

/// One atomic word owns either an attach-time accountant Arc/use or an
/// admitted writer owner. The enclosing OnceLock remains the existing layout;
/// raw readers are immutable and preparation never allocates for them.
#[repr(transparent)]
struct ProofAccountantSlot(std::sync::atomic::AtomicPtr<()>);
const _: () = assert!(std::mem::align_of::<crate::memory_accounting::MemoryAccountant>() >= 8);
const _: () = assert!(std::mem::align_of::<PersistenceProofOwner>() >= 4);

struct DiagnosticIndexName<'a> {
    table: &'a str,
    index: &'a str,
}

pub struct RedbPersistence {
    path: std::path::PathBuf,
    /// An explicit runtime-state root; `None` selects the platform default.
    runtime_dir: Option<PathBuf>,
    // Field order preserves flush, data-file release, companion release on unwind.
    db: Arc<std::sync::RwLock<Option<redb::Database>>>,
    writer_file: std::sync::RwLock<Option<Arc<LockedWriterFile>>>,
    lock_file: Arc<std::sync::RwLock<Option<CompanionGuard>>>,
    storage_compaction: Mutex<(
        redb::CompactionCursor,
        redb::CompactionProgress,
        // The measurement the active sweep started on; `None` when unknown.
        Option<f64>,
        Option<PersistenceBytes>,
    )>,
    #[cfg(any(test, feature = "test-seams"))]
    handoff_prepare_io_fault: AtomicBool,

    writer_snapshots: Arc<parking_lot::Mutex<Vec<WriterSnapshotWeak>>>,
    writer_schema: parking_lot::Mutex<Option<Arc<writer_source::WriterSchema>>>,
    /// Writer charge metadata for this store's fixed reserve. Physical cache
    /// owners, rather than close's numeric release, own its actual receipt.
    cache_charge_bytes: std::sync::OnceLock<usize>,
    /// The native cache as installed: the capacity the builder and the
    /// startup settlement were given, and the whole reserve that capacity
    /// settled to. A handle recycle reinstalls the capacity, never the
    /// reserve, and refuses a reserve its funding does not cover.
    cache_installation: CacheInstallation,
    /// Process-residue metadata. The genuine credit is retained by the same
    /// shared physical cache parent as a declared writer's original invoice.
    cache_stripe_residue: std::sync::OnceLock<usize>,
    /// Set the first time a table's meta decodes only via an older
    /// `TableMeta`/`ColumnDef` layout (see
    /// [`Self::decode_table_meta_versioned`]) rather than the current one.
    /// `Database::open` refuses on this (`Error::LegacyVectorStoreDetected`);
    /// no opener reads past it. Never set back to `false` — one unsupported table
    /// is enough to mark the whole root.
    used_legacy_table_meta_layout: AtomicBool,
    pub(crate) native_ivf_cache: vector_ivf::NativeIvfCache,
    /// Captures the writer's published root for paged generation readers.
    paged_root_capture: std::sync::OnceLock<vector_paged::PagedRootCapture>,
    scoped_vector_root_capture: std::sync::OnceLock<vector_paged::ScopedWriterRootCapture>,
    saved_export_recovery_cutoff: std::sync::OnceLock<u64>,
    saved_export_abandonment_error: Arc<parking_lot::Mutex<Option<String>>>,
    /// The database's own memory accountant, attached once the owning
    /// `Database` has built it. Proof maintenance admits what it allocates
    /// through this, so a write that cannot fit the configured budget receives
    /// the existing typed refusal instead of allocating anyway.
    proof_accountant: std::sync::OnceLock<ProofAccountantSlot>,
}
pub(crate) struct RawSyncApplyStateDigest {
    pub(crate) digest: String,
    pub(crate) trigger_audit_entries: u64,
    pub(crate) sink_audit_entries: u64,
    pub(crate) sink_queue_entries: u64,
    pub(crate) tables: u64,
    pub(crate) retained_row_versions: u64,
    pub(crate) index_postings: u64,
    pub(crate) edges: u64,
    pub(crate) vectors: u64,
    pub(crate) ddl_log_entries: u64,
    pub(crate) change_log_entries: u64,
    pub(crate) commit_index_entries: u64,
    pub(crate) persisted_config_entries: u64,
}

/// Point-removed key counts from one scoped prune pass — real integer
/// counters, no rewritten-survivor cost, mirroring the `FkProbeStats`
/// precedent. Folded into the caller's per-table receipt
/// (`TableVersionCleanup`/`CurrencyCompactionReport`), and the bench's
/// proportionality assertions read these directly.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PruneScopedStats {
    pub row_keys_removed: u64,
    pub change_log_keys_removed: u64,
    pub vector_keys_removed: u64,
    pub edge_keys_removed: u64,
    /// Set when this pass's accounted content-identity proof-maintenance
    /// admission was refused by the memory or disk budget and the pass was
    /// retried unaccounted so the durable and in-memory removal still landed.
    /// `None` on the ordinary accounted path.
    pub proof_admission_refused: Option<contextdb_vector::VectorMaintenanceFailureDetails>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum VectorPartitionRecordKind {
    Membership = 1,
    Journal = 2,
    Tombstone = 3,
    BaseGeneration = 4,
    ChangeGeneration = 5,
    GenerationCatalog = 6,
    GraphPageManifest = 7,
}

/// One durable row-version/vector-version placement in the local two-level
/// registry. The key is partition-first for lazy partition loading; these
/// identity fields make key/value substitution detectable on load.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VectorPartitionMembershipRecord {
    pub(crate) index: VectorIndexRef,
    pub(crate) partition_key: VectorPartitionKey,
    pub(crate) row_id: RowId,
    pub(crate) row_created_tx: TxId,
    pub(crate) row_lsn: Lsn,
    pub(crate) vector_created_tx: TxId,
    pub(crate) vector_lsn: Lsn,
    pub(crate) visible_from: TxId,
    pub(crate) deleted_tx: Option<TxId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum VectorPartitionTombstoneCause {
    Delete,
    Move,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VectorPartitionTombstoneRecord {
    pub(crate) membership: VectorPartitionMembershipRecord,
    pub(crate) deleted_tx: TxId,
    pub(crate) lsn: Lsn,
    pub(crate) cause: VectorPartitionTombstoneCause,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum VectorPartitionJournalChange {
    Upsert(VectorPartitionMembershipRecord),
    Tombstone(VectorPartitionTombstoneRecord),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VectorPartitionJournalRecord {
    pub(crate) index: VectorIndexRef,
    pub(crate) partition_key: VectorPartitionKey,
    pub(crate) lsn: Lsn,
    pub(crate) ordinal: u64,
    pub(crate) change: VectorPartitionJournalChange,
}

pub(crate) struct IndexValidationFailure<'a> {
    declaration: &'a IndexDecl,
    column: Option<(&'a str, &'static str)>,
    parts: Option<usize>,
    suffix: &'static str,
}

/// A row/vector-version identity used by authoritative retention/purge.  A
/// selected identity invalidates every graph route for its partition: keeping
/// a graph that still contains the removed vector would be a silent result
/// omission or resurrection, so the next maintenance build owns replacement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VectorPartitionVersionIdentity {
    pub(crate) index: VectorIndexRef,
    pub(crate) partition_key: VectorPartitionKey,
    pub(crate) row_id: RowId,
    pub(crate) row_created_tx: TxId,
    pub(crate) row_lsn: Lsn,
    pub(crate) vector_created_tx: TxId,
    pub(crate) vector_lsn: Lsn,
}

/// The in-memory purge preparation can name a selected vector and its exact
/// local partition, but only persistence owns the durable row-version pairing
/// recorded beside that vector. Resolve this selector before the purge write
/// transaction so the destruction boundary receives immutable point
/// identities rather than rediscovering ownership while it mutates Redb.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VectorPartitionVersionSelector {
    pub(crate) index: VectorIndexRef,
    pub(crate) partition_key: VectorPartitionKey,
    pub(crate) row_id: RowId,
    pub(crate) vector_created_tx: TxId,
    pub(crate) vector_lsn: Lsn,
}

trait VectorPartitionRecordIndex {
    fn vector_partition_record_index(&self) -> &VectorIndexRef;
}

#[derive(Default)]
struct CompleteVectorPartitionImage {
    paged_memberships: bool,
    memberships: Vec<VectorPartitionMembershipRecord>,
    journal: Vec<VectorPartitionJournalRecord>,
    tombstones: Vec<VectorPartitionTombstoneRecord>,
}

pub(crate) struct SchemaDdlPersistence<'a> {
    pub(crate) event_bus: Option<&'a EventBusPersistenceCommit>,
    pub(crate) trigger: Option<&'a TriggerPersistenceCommit>,
}

#[derive(Default)]
pub(crate) struct FlushDataSnapshots<'a> {
    pub(crate) table_meta: Option<&'a HashMap<String, TableMeta>>,
    pub(crate) deleted_rows: Option<HashMap<String, HashMap<RowId, VersionedRow>>>,
}

pub(crate) struct LocalErasurePersistence<'a> {
    pub(crate) projection: &'a AuthoritativePurgePersistenceProjection,
    pub(crate) accountant: contextdb_vector::MemoryBudgetOwner,
    pub(crate) operation: &'static str,
}

pub(crate) struct FlushDataOptions<'a> {
    pub(crate) local_erasure: Option<LocalErasurePersistence<'a>>,
    pub(crate) sink_events: &'a [PreparedSinkEvent],
    pub(crate) trigger_audits: &'a [(u64, TriggerAuditEntry)],
    pub(crate) schema_ddl: SchemaDdlPersistence<'a>,
    pub(crate) max_sink_queue_depth: usize,
    pub(crate) snapshots: FlushDataSnapshots<'a>,
    /// A complete prepared received-schema image. When present, it replaces
    /// the ordinary incremental Redb writes in this same transaction; memory
    /// publication remains the transaction manager's after-apply work.
    pub(crate) received_schema: Option<&'a ReceivedSchemaPersistenceProjection>,
    /// The already validated row/declaration projection for ordinary vector
    /// commits. Persistence may encode it but must not derive partition keys
    /// from mutable row or vector state after Redb begins.
    pub(crate) partition_mutations: Option<&'a PreparedVectorPartitionMutationBatch>,
}

/// The durable half of a private received-schema stage. It is fully owned and
/// encoded before Redb begins its write transaction, so persistence cannot
/// regroup source DDL or consult live schema state while committing it.
pub(crate) struct ReceivedSchemaPersistenceProjection {
    pub(crate) row_adj_inserts: Vec<contextdb_tx::OwnedAdjEntry>,
    pub(crate) row_adj_deletes: Vec<(contextdb_tx::OwnedAdjEntry, TxId)>,
    pub(crate) dropped_row_graph: Vec<contextdb_tx::OwnedAdjEntry>,
    /// Every graph ownership lifetime ended by this unit, even if a later
    /// declaration restores the table's graph shape.
    pub(crate) retired_row_graph_tables: Vec<String>,
    /// The optional SAME public write opened during native received preparation.
    pub(crate) working_write: parking_lot::Mutex<Option<received_working::ReceivedWorkingWrite>>,
    /// A data-empty received declaration whose row/column projection stays
    /// unchanged carries metadata and protocol bookkeeping only. It must not
    /// reconstruct any row, index, graph or vector population.
    pub(crate) schema_noop: bool,
    pub(crate) affected_tables: HashSet<String>,
    pub(crate) replace_graph: bool,
    pub(crate) affected_graph_edge_types: HashSet<String>,
    pub(crate) graph_selection: Option<received_working_graph::WorkingGraphCapture>,
    pub(crate) graph_occurrences: bool,
    pub(crate) replace_vectors: bool,
    /// Independent writer-owned scratch for the derived complete metadata image.
    /// Payload-copy charges have already transferred to their own destinations.
    pub(crate) vector_image_memory: crate::read_image_memory::ImageMemoryHandle,
    /// Transaction-local cancellation receipts, consumed only after durability.
    /// The pre2-sized debit is contained by ReceivedSchemaStage's existing
    /// final `_vector_copy_owner`: its counted writer adapter outlives these
    /// entries and their backing, including drained-but-retained capacity.
    pub(crate) retired_vector_construction: parking_lot::Mutex<
        Vec<(
            VectorPartitionRef,
            Option<crate::memory_accounting::ContainedMemoryReservation>,
        )>,
    >,
    pub(crate) table_meta: HashMap<String, TableMeta>,
    /// Tables whose arriving unit changes row membership or row values. An
    /// unchanged index declaration over unchanged rows keeps its durable image.
    pub(crate) row_changed_tables: HashSet<String>,
    pub(crate) rows: HashMap<String, Vec<VersionedRow>>,
    pub(crate) row_records: Option<schema_projection::ReceivedRowRecords>,
    /// Read-only private image plus its original final owner in the engine
    /// stage. No second population-sized body array is allocated for durability.
    pub(crate) private_rows: Option<Arc<contextdb_relational::store::RelationalStore>>,
    pub(crate) authored_pending_rows:
        Vec<(String, Arc<crate::database::AuthoredRowShape>, VersionedRow)>,
    pub(crate) sync_sources: Vec<(String, RowId, Lsn, SyncSourceKind)>,
    pub(crate) sync_source_records: Option<schema_projection::ReceivedSyncSourceRecords>,
    pub(crate) edges: Vec<AdjEntry>,
    /// Canonical bodies and exact independent placements are supplied together.
    pub(crate) vectors: Vec<VectorEntry>,
    pub(crate) vector_memberships: Vec<VectorPartitionMembershipRecord>,
    pub(crate) vector_records: Option<schema_projection::ReceivedVectorRecords>,
    pub(crate) ddl_log: Vec<(Lsn, DdlChange)>,
    pub(crate) config_values: Vec<(String, Vec<u8>)>,
    pub(crate) config_max_u64_keys: Vec<String>,
    /// Hub-owned rows that must share this authenticated schema transaction
    /// but are not part of the received schema image. The node-contact row is
    /// the only current producer; keeping it incremental avoids rebuilding an
    /// otherwise unaffected table.
    pub(crate) operational_write_set: WriteSet,
    pub(crate) operational_table_meta: HashMap<String, TableMeta>,
    pub(crate) operational_deleted_rows: HashMap<String, HashMap<RowId, VersionedRow>>,
    /// A received DROP TABLE retires this generation's data history. Its
    /// row/vector entries must leave the durable log in the same transaction
    /// that installs the replacement schema image.
    pub(crate) structurally_dropped_tables: HashSet<String>,
    /// Last: the same Original survives private writes, projection payloads and
    /// their physical cleanup, including a failed terminal flush.
    pub(crate) preparation_owner: Option<received_working::ReceivedPreparationOwner>,
}

pub(crate) enum ReceivedRowsGuard<'a> {
    Owned(&'a HashMap<String, Vec<VersionedRow>>),
    Private(parking_lot::RwLockReadGuard<'a, HashMap<String, Vec<VersionedRow>>>),
}

/// The narrow durable payload for one locally authored SQL schema commit.
/// Unlike a received-schema projection, it updates only the schema entries
/// the local transaction changed and never reconstructs unrelated rows,
/// graph state, vectors, or sidecars.
pub(crate) struct LocalSchemaPersistenceProjection {
    /// Metadata for exactly the tables affected by this local DDL vector.
    pub(crate) affected_table_meta: HashMap<String, TableMeta>,
    /// The ordered DDL vector, all durably recorded at the owning commit LSN.
    pub(crate) ddl: Vec<DdlChange>,
    /// Values already encoded by the schema preparation phase.
    pub(crate) config_values: Vec<(String, Vec<u8>)>,
    /// DDL-only WriteSets do not carry a row from which persistence can infer
    /// this identity, so retain the transaction manager's visibility choice.
    pub(crate) commit_index_tx: TxId,
}

/// Fully prepared, crate-private durable mutation for one authoritative
/// purge.  It contains only persistence-owned/public value types: database
/// lineage structs are encoded by the caller before Redb starts its write
/// transaction.  The point identities keep the storage layer from reading or
/// rediscovering mutable database state while it commits.
pub(crate) struct AuthoritativePurgePersistenceProjection {
    pub(crate) row_versions: Vec<(String, RowId, TxId, Lsn)>,
    pub(crate) source_provenance: Vec<(String, RowId, Lsn, u8)>,
    pub(crate) vectors: Vec<VectorEntry>,
    pub(crate) vector_partition_identities: Vec<VectorPartitionVersionIdentity>,
    pub(crate) graph_entries: Vec<AdjEntry>,
    pub(crate) row_graph_entries: Vec<contextdb_tx::OwnedAdjEntry>,
    #[allow(
        dead_code,
        reason = "KEEP-GUARD: held for its Drop; releases what it holds when the owner drops"
    )]
    pub(crate) row_graph_credit: contextdb_core::read_memory::ReadCredit,
    /// Selected sink entries: sink, id and the BLAKE3 digest of the durable
    /// value the selection saw.
    pub(crate) sink_entries: Vec<(String, u64, [u8; 32])>,
    pub(crate) change_log_entries: Vec<ChangeLogEntry>,
    pub(crate) config_keys_removed: Vec<String>,
    pub(crate) lifecycle_records: Vec<(String, Vec<u8>)>,
    pub(crate) purge_delivery_items: Vec<(String, Vec<u8>)>,
    pub(crate) blob_purge: BlobAuthoritativePurgeProjection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedVersionedRow {
    row_id: contextdb_core::RowId,
    values: HashMap<String, PersistedValue>,
    created_tx: contextdb_core::TxId,
    deleted_tx: Option<contextdb_core::TxId>,
    lsn: Lsn,
    created_at: Option<contextdb_core::Wallclock>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum PersistedValue {
    Plain(Value),
    Vector(PersistedVector),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedVectorEntry {
    index: VectorIndexRef,
    row_id: contextdb_core::RowId,
    vector: PersistedVector,
    created_tx: contextdb_core::TxId,
    deleted_tx: Option<contextdb_core::TxId>,
    lsn: Lsn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SinkAuditEntry {
    lsn: Lsn,
    kind: SinkAuditKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum SinkAuditKind {
    QueueOverflow { sink: String, dropped_count: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum PersistedVector {
    F32(Vec<f32>),
    SQ8 {
        min: f32,
        max: f32,
        len: u32,
        payload: Vec<u8>,
    },
    SQ4 {
        min: f32,
        max: f32,
        len: u32,
        payload: Vec<u8>,
    },
}

/// Eleven-field ColumnDef shape without `context_id`/`scope_label`/`acl_ref`.
/// `RedbPersistence::decode_table_meta_versioned` recognizes this positional
/// layout with a separate struct so the opener can refuse the whole store;
/// it is not a truncated payload in the required schema layout.
#[derive(Debug, Clone, Deserialize)]
struct LegacyColumnDefV1 {
    name: String,
    column_type: ColumnType,
    nullable: bool,
    primary_key: bool,
    #[serde(default)]
    unique: bool,
    #[serde(default)]
    default: Option<String>,
    #[serde(default)]
    references: Option<ForeignKeyReference>,
    #[serde(default)]
    expires: bool,
    #[serde(default)]
    immutable: bool,
    #[serde(default)]
    quantization: VectorQuantization,
    #[serde(default)]
    rank_policy: Option<RankPolicy>,
}

/// The exact `TableMeta` layout `v1.0.0` wrote to disk — 11 fields, missing
/// `composite_foreign_keys`/`sync_direction`/`retain_declared_unit`/
/// `primary_key_columns`/`conflict_policy`.
#[derive(Debug, Clone, Deserialize)]
struct LegacyTableMetaV1 {
    columns: Vec<LegacyColumnDefV1>,
    immutable: bool,
    state_machine: Option<StateMachineConstraint>,
    #[serde(default)]
    dag_edge_types: Vec<String>,
    #[serde(default)]
    unique_constraints: Vec<Vec<String>>,
    natural_key_column: Option<String>,
    #[serde(default)]
    propagation_rules: Vec<PropagationRule>,
    #[serde(default)]
    default_ttl_seconds: Option<u64>,
    #[serde(default)]
    sync_safe: bool,
    #[serde(default)]
    expires_column: Option<String>,
    #[serde(default)]
    indexes: Vec<IndexDecl>,
}

type F32WitnessKey<'a> = (&'a str, RowId, TxId, Lsn, &'a str);
#[derive(Clone, Copy)]
enum F32Witness<'a> {
    Staged(&'a [f32]),
    Durable(&'a VectorIndexRef, RowId, TxId, Lsn),
}
type F32WitnessIndex<'a> = HashMap<F32WitnessKey<'a>, F32Witness<'a>>;
type F32RowOverlay<'a> = HashMap<String, std::borrow::Cow<'a, [f32]>>;

mod redb_open_and_format;

mod redb_flush;

mod redb_vector_partitions;

mod redb_prune_and_purge;

mod redb_rows_and_indexes;

struct PersistenceConstruction {
    source: *mut RedbPersistence,
    initialized: [bool; 19],
}

mod construction_and_event_sources;
#[cfg(any(test, feature = "test-seams"))]
pub(crate) use construction_and_event_sources::*;
#[cfg(not(any(test, feature = "test-seams")))]
use construction_and_event_sources::*;

#[cfg(all(test, unix))]
mod last_reader_registry_cleanup_proofs;

#[cfg(test)]
mod reader_breadcrumb_location_proofs;

#[cfg(test)]
mod recorded_owner_state_proofs;

pub(crate) const RETENTION_DEFERRED_EDGE_NODES: &str = "retention_deferred_edge_nodes";

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static RETENTION_RECLAIM_CRASH: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    /// Kill this process inside the one write transaction that installs a new
    /// `TableMeta` and everything that must land with it, so a proof can observe
    /// what a real crash leaves behind rather than argue the transaction is
    /// atomic from the source.
    ///
    /// The three boundaries bracket the maintained-count rebuild:
    /// `1` the new metadata rows are staged and the counts are not rebuilt yet,
    /// `2` the counts are rebuilt and nothing is committed, `3` the commit
    /// returned. A crash at `1` or `2` must leave the whole OLD state; at `3` the
    /// whole NEW state.
    ///
    /// Thread-scoped, like every other crash seam in this file. The whole
    /// `ALTER TABLE ... SET MAINTAIN COUNT` path runs on the arming thread --
    /// `with_db` and `allocate_ddl_lsn` both take an unsendable `FnOnce` and call
    /// it inline, so the write cannot leave that thread -- and libtest runs this
    /// crate's tests as parallel threads of ONE process, so a process-global cell
    /// would abort the whole binary at whichever unrelated store reached the
    /// boundary first.
    static TABLE_META_REWRITE_CRASH: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
}

// These cuts belong only to the narrow policy publisher. The global currency
// hook can be consumed by an earlier preparatory metadata transaction.
#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static POLICY_METADATA_FAULT: std::cell::Cell<(u8, bool)> = const {
        std::cell::Cell::new((0, false))
    };
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static CURRENCY_RECLAIM_FAULT: std::cell::Cell<(u8, bool)> = const {
        std::cell::Cell::new((0, false))
    };
    /// One injected failure of the stored-size read a table's size answer
    /// depends on, so a proof can show the failure reaching its caller instead
    /// of being folded into a smaller-looking table.
    static STORED_ROW_BYTES_FAULT: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
}

#[cfg(test)]
thread_local! {
    pub(crate) static COMPACTION_BATCH_OBSERVER: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(all(test, feature = "test-seams"))]
mod empty_declared_index_readiness_tests;

#[cfg(test)]
mod streamed_raw_loader_tests;

#[cfg(all(test, feature = "test-seams"))]
mod ordered_run_integrity_tests;

#[cfg(all(test, feature = "test-seams"))]
mod search_page_reuse_tests;

#[cfg(all(test, target_os = "linux"))]
mod coordination_error_tests;

#[cfg(all(test, target_os = "linux"))]
mod reader_contention_test_helper;

#[cfg(all(test, target_os = "linux"))]
mod owner_cursor_admission_tests;

#[cfg(all(test, target_os = "linux", feature = "test-seams"))]
mod reader_handoff_tests;

#[cfg(test)]
mod purge_error_typing_tests;

#[cfg(all(test, feature = "test-seams"))]
mod metadata_decode_guard_tests;

#[cfg(test)]
#[path = "persistence/retained_raw_body_guard_tests.rs"]
mod retained_raw_body_guard_tests;

#[cfg(test)]
#[path = "persistence/journal_record_validation_tests.rs"]
mod journal_record_validation_tests;

#[cfg(test)]
#[path = "persistence/body_placement_publication_tests.rs"]
mod body_placement_publication_tests;

#[cfg(test)]
#[path = "persistence/cdb141_integrity_revision_tests.rs"]
pub(crate) mod cdb141_integrity_revision_tests;

// The original raw fixture signatures are exposed to integration fixtures only
// by the existing feature. Normal consumers use the admitted captured core.
macro_rules! raw_persistence_surface {
    ($visibility:vis) => {
        #[allow(dead_code, reason = "KEEP-SEAM: generated by raw_persistence_surface!; some instantiations use these methods only in the test-seams build")]
        impl RedbPersistence {
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    $visibility fn create(path: &Path) -> Result<Self> {
        // This raw constructor declares its own default open budget at entry
        // when it has no enclosing operation. Decode never chooses a payer.
        let accountant = match crate::read_image_memory::active_owner() {
            Ok(owner) => owner.accountant.clone(),
            Err(_) => crate::memory_accounting::AccountantHandle::standalone(
                crate::read_session::DatabaseOpenOptions::default().memory_limit,
                true,
            )?,
        };
        let _decode_scope = if let Ok(owner) = crate::read_image_memory::active_owner() {
            owner.enter()
        } else {
            crate::read_image_memory::ImageMemory::enter_writer_operation(accountant.clone())?
        };
        Self::create_with_accountant(path, Some(accountant), REDB_CACHE_BYTES)
    }

    /// Open a store that must already be there, creating nothing.
    ///
    /// There is no existence question here and no second door: the ordinary
    /// open is attempted, and the attempt's own "nothing at this path" becomes
    /// [`Error::StoreMissing`] naming the store. Every refusal on this road
    /// leaves the directory as it found it -- including the companion this
    /// open takes for itself while it is still deciding, which is taken back
    /// before the refusal is returned.
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    $visibility fn open_existing_only(path: &Path) -> Result<Self> {
        // This raw constructor declares its own default open budget at entry
        // when it has no enclosing operation. Decode never chooses a payer.
        let accountant = match crate::read_image_memory::active_owner() {
            Ok(owner) => owner.accountant.clone(),
            Err(_) => crate::memory_accounting::AccountantHandle::standalone(
                crate::read_session::DatabaseOpenOptions::default().memory_limit,
                true,
            )?,
        };
        let _decode_scope = if let Ok(owner) = crate::read_image_memory::active_owner() {
            owner.enter()
        } else {
            crate::read_image_memory::ImageMemory::enter_writer_operation(accountant.clone())?
        };
        let persistence = Self::open_existing_only_with_cache(path, REDB_CACHE_BYTES)?;
        persistence.attach_memory_accountant(accountant);
        Ok(persistence)
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    $visibility fn open(path: &Path) -> Result<Self> {
        // This raw constructor declares its own default open budget at entry
        // when it has no enclosing operation. Decode never chooses a payer.
        let accountant = match crate::read_image_memory::active_owner() {
            Ok(owner) => owner.accountant.clone(),
            Err(_) => crate::memory_accounting::AccountantHandle::standalone(
                crate::read_session::DatabaseOpenOptions::default().memory_limit,
                true,
            )?,
        };
        let _decode_scope = if let Ok(owner) = crate::read_image_memory::active_owner() {
            owner.enter()
        } else {
            crate::read_image_memory::ImageMemory::enter_writer_operation(accountant.clone())?
        };
        let persistence = Self::open_with_cache(path, REDB_CACHE_BYTES)?;
        persistence.attach_memory_accountant(accountant);
        Ok(persistence)
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    $visibility fn load_relational_table(&self, name: &str) -> Result<Vec<VersionedRow>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table_meta = Self::load_table_meta_in_read_txn(&read_txn, name)?;
            let table_name = Self::rel_table_name(name);
            let table_def: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(table_name.as_str());
            match read_txn.open_table(table_def) {
                Ok(table) => {
                    let mut rows = Vec::new();
                    for entry in table.iter().map_err(Self::storage_error)? {
                        let (_, value) = entry.map_err(Self::storage_error)?;
                        rows.push(Self::decode_versioned_row(
                            value.value(),
                            table_meta.as_ref(),
                        )?);
                    }
                    Ok(rows)
                }
                Err(redb::TableError::TableDoesNotExist(_)) => Ok(Vec::new()),
                Err(redb::TableError::TableTypeMismatch { .. }) => {
                    Err(Self::unrecognized_relational_layout(name))
                }
                Err(err) => Err(Self::storage_error(err)),
            }
        })
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    $visibility fn load_all_tables(&self) -> Result<HashMap<String, Vec<VersionedRow>>> {
        let mut all_tables = HashMap::new();
        for name in self.load_all_table_meta()?.into_keys() {
            let rows = self.load_relational_table(&name)?;
            all_tables.insert(name, rows);
        }
        Ok(all_tables)
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    $visibility fn load_forward_edges(&self) -> Result<Vec<AdjEntry>> {
        self.load_graph_table(GRAPH_FWD_TABLE)
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    $visibility fn load_reverse_edges(&self) -> Result<Vec<AdjEntry>> {
        self.load_graph_table(GRAPH_REV_TABLE)
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    $visibility fn load_vectors(&self) -> Result<Vec<VectorEntry>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(VECTORS_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut vectors = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (_, value) = entry.map_err(Self::storage_error)?;
                vectors.push(Self::decode_vector_entry(value.value())?);
            }
            Ok(vectors)
        })
    }
        }
    };
}
#[cfg(not(any(test, feature = "test-seams")))]
raw_persistence_surface!(pub(crate));
#[cfg(any(test, feature = "test-seams"))]
raw_persistence_surface!(pub);

// These mutation helpers require the Database-attached writer accountant.
// External integration fixtures retain the original signatures only through
// the existing feature, matching the raw fixture visibility boundary.
macro_rules! persistence_mutation_surface {
    ($visibility:vis) => {
        #[allow(dead_code, reason = "KEEP-SEAM: generated by persistence_mutation_surface!; some instantiations use these methods only in the test-seams build")]
        impl RedbPersistence {
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    $visibility fn remove_table_data(&self, name: &str) -> Result<()> {
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;

#[cfg(any(test, feature = "test-seams"))]
let mut graph_publication_attempt = crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            self.visit_relational_rows_in_write(&write_txn, name, |row| {
                let key = crate::composite_store::retention_expiry_key(
                    name, row.row_id, row.created_tx,
                );
                let mut mutations = content_identity::config_mutations(&key, None);
                mutations.push(content_identity::remove_row_mutation(name, row.row_id));
                Self::apply_content_identity_mutations_in_write(
                    &write_txn, &mutations, &mut self.proof_admission(),
                ).map(|_| ())
            })?;
            let table_name = Self::rel_table_name(name);
            let table_def: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(table_name.as_str());
            self.retire_relational_manifests_in_write(&write_txn, name)?;
            content_size::reset_rows(&write_txn, name)?;
            match write_txn.delete_table(table_def) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(redb::TableError::TableTypeMismatch { .. }) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            let legacy_table_def: TableDefinition<u64, &[u8]> =
                TableDefinition::new(table_name.as_str());
            match write_txn.delete_table(legacy_table_def) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(redb::TableError::TableTypeMismatch { .. }) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            self.remove_table_source_provenance_in_write(&write_txn, name)?;
            self.remove_table_retention_expiries_in_write(&write_txn, name)?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
            { #[cfg(any(test, feature = "test-seams"))]
            graph_observation_checkpoint(10)?;
            let graph_commit_result = write_txn.commit();
#[cfg(any(test, feature = "test-seams"))]
if graph_commit_result.is_ok() { graph_publication_attempt.committed(); graph_observation_checkpoint(11)?; }
graph_commit_result }.map_err(Self::storage_error)?;
            Ok(())
        })
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    $visibility fn rewrite_table_rows(&self, name: &str, rows: &[VersionedRow]) -> Result<()> {
        // The rewrite's historical index commitments are paid by this writer.
        let _decode_scope = self.writer_decode_scope()?;
        let source_meta = self.schema_projection_metadata_point(name)?;
        let table_meta = source_meta.as_ref().map(|(meta, _)| meta);
        let sources = self.load_sync_source_lsns()?;
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;

#[cfg(any(test, feature = "test-seams"))]
let mut graph_publication_attempt = crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            Self::invalidate_retention_clock_in_write(&write_txn, name)?;
            let mut retired_tx = TxId(0);
            let mut retired_lsn = Lsn(0);
            let mut retired_row_id = RowId(0);
            self.visit_relational_rows_in_write(&write_txn, name, |row| {
                retired_tx = retired_tx.max(row.created_tx).max(row.deleted_tx.unwrap_or(TxId(0)));
                retired_lsn = retired_lsn.max(row.lsn);
                retired_row_id = retired_row_id.max(row.row_id);
                Self::raise_table_read_lsn_in_write(&write_txn, name, row.lsn)?;
                Self::apply_content_identity_mutations_in_write(
                    &write_txn,
                    &[content_identity::remove_row_mutation(name, row.row_id)],
                    &mut self.proof_admission(),
                ).map(|_| ())
            })?;
            // Postings and the row versions they project are committed to
            // separately and must agree: see `historical_index_commitment`.
            let mut commitments = historical_index_commitment::CommitmentBatch::for_writer()?;
            {
                let table_name = Self::rel_table_name(name);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                self.retain_authored_rows_in_write(&write_txn, name)?;
                self.retire_relational_manifests_in_write(&write_txn, name)?;
                content_size::reset_rows(&write_txn, name)?;
                write_txn
                    .delete_table(table_def)
                    .map_err(Self::storage_error)?;
                let legacy_table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                write_txn
                    .delete_table(legacy_table_def)
                    .map_err(Self::storage_error)?;
                let mut redb_table =
                    content_size::open_rows(&write_txn, table_def, table_meta)
                        .map_err(Self::storage_error)?;
                // The table is rewritten whole: the version side of every
                // index is folded afresh from the rows written here.
                for declaration in redb_table.indexes() {
                    commitments.reset(
                        &write_txn,
                        name,
                        declaration,
                        historical_index_commitment::Side::Versions,
                    )?;
                }
                for row in rows {
                    let encoded = Self::encode_versioned_row(row, table_meta)?;
                    let key = Self::rel_row_key(row);
                    {
                        let predecessor = redb_table.get(key.as_slice()).map_err(Self::storage_error)?;
                        self.replace_relational_manifest_in_write(&write_txn, name, key.as_slice(), predecessor.as_ref().map(|body| body.value()), Some(encoded.as_slice()))?;
                    }
                    let keys = content_size::VersionLife::keys_of(redb_table.indexes(), row)?;
                    redb_table.insert_version(
                        &mut commitments,
                        key.as_slice(),
                        encoded.as_slice(),
                        &content_size::VersionLife {
                            keys: &keys,
                            created: row.created_tx,
                            deleted: row.deleted_tx,
                        },
                    )?;
                }
            }
            if let Some(meta) = table_meta {
                Self::rebuild_relational_indexes_in_write(
                    &write_txn,
                    &self.path,
                    name,
                    meta,
                    Some(rows),
                    &mut self.proof_admission(),
                    Some(&mut commitments),
                )?;
                Self::apply_content_identity_mutations_in_write(
                    &write_txn,
                    &commitments.finish(&write_txn)?.into_parts().0,
                    &mut self.proof_admission(),
                )?;
            }
            Self::retain_sync_source_provenance_for_table_rows(&write_txn, name, rows)?;
            let mut mutations = Vec::new();
            let mut ordered_rows = rows.iter().collect::<Vec<_>>();
            ordered_rows.sort_by_key(|row| (row.lsn, row.created_tx));
            for row in ordered_rows {
                mutations.push(content_identity::row_mutation(
                    name,
                    row,
                    table_meta,
                )?);
            }
            let live_rows = rows
                .iter()
                .filter(|row| row.deleted_tx.is_none())
                .map(|row| row.row_id)
                .collect::<HashSet<_>>();
            mutations.extend(
                sources
                    .keys()
                    .filter(|(table, row_id)| table == name && !live_rows.contains(row_id))
                    .map(|(table, row_id)| {
                        content_identity::remove_sync_source_mutation(table, *row_id)
                    }),
            );
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &mutations,
                &mut self.proof_admission(),
            )?;
            for row in rows {
                Self::raise_table_read_lsn_in_write(&write_txn, name, row.lsn)?;
            }
            Self::raise_noncanonical_writer_floors_in_write(
                &write_txn,
                rows.iter()
                    .flat_map(|row| std::iter::once(row.created_tx).chain(row.deleted_tx))
                    .max()
                    .unwrap_or(TxId(0)).max(retired_tx),
                rows.iter()
                    .map(|row| row.lsn)
                    .max()
                    .unwrap_or(Lsn(0)).max(retired_lsn),
                rows.iter()
                    .map(|row| row.row_id)
                    .max()
                    .unwrap_or(RowId(0)).max(retired_row_id),
            )?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
            { #[cfg(any(test, feature = "test-seams"))]
            graph_observation_checkpoint(10)?;
            let graph_commit_result = write_txn.commit();
#[cfg(any(test, feature = "test-seams"))]
if graph_commit_result.is_ok() { graph_publication_attempt.committed(); graph_observation_checkpoint(11)?; }
graph_commit_result }.map_err(Self::storage_error)?;
            Ok(())
        })
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    $visibility fn rewrite_table_meta_rows_and_append_ddl_log(
        &self,
        name: &str,
        meta: &TableMeta,
        rows: &[VersionedRow],
        lsn: Lsn,
        ddl: &[DdlChange],
        config_values: Vec<(&str, Vec<u8>)>,
    ) -> Result<()> {
        // The rewrite's historical index commitments are paid by this writer.
        let _decode_scope = self.writer_decode_scope()?;
        let source_meta = self.schema_projection_metadata_point(name)?;
        let previous_meta = source_meta.as_ref().map(|(meta, _)| meta);
        let sources = self.load_sync_source_lsns()?;
        let vector_quantization = meta.columns.iter().filter_map(|column| {
            matches!(column.column_type, ColumnType::Vector(_)).then(|| (
                VectorIndexRef::new(name, &column.name), column.quantization,
            ))
        }).collect::<HashMap<_, _>>();
        let previous_vector_columns = previous_meta
            .as_ref()
            .map(|previous| {
                previous
                    .columns
                    .iter()
                    .filter(|column| matches!(column.column_type, ColumnType::Vector(_)))
                    .map(|column| column.name.clone())
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();
        let current_vector_columns = meta
            .columns
            .iter()
            .filter(|column| matches!(column.column_type, ColumnType::Vector(_)))
            .map(|column| column.name.clone())
            .collect::<HashSet<_>>();
        let vector_renames = ddl
            .iter()
            .filter_map(|change| {
                let DdlChange::AlterTable {
                    name: change_table,
                    constraints,
                    ..
                } = change
                else {
                    return None;
                };
                (change_table == name)
                    .then(|| read_vector_rename_constraint(constraints))
                    .flatten()
                    .filter(|(from, to)| {
                        previous_vector_columns.contains(from)
                            && current_vector_columns.contains(to)
                    })
            })
            .map(|(from, to)| {
                (
                    VectorIndexRef::new(name, from),
                    VectorIndexRef::new(name, to),
                )
            })
            .collect::<Vec<_>>();
        let renamed_from = vector_renames
            .iter()
            .map(|(from, _)| from.column.clone())
            .collect::<HashSet<_>>();
        let dropped_vector_columns = previous_vector_columns
            .difference(&current_vector_columns)
            .filter(|column| !renamed_from.contains(*column))
            .cloned()
            .collect::<HashSet<_>>();
        let schema =
            self.prepare_writer_schema_change(std::iter::empty(), std::iter::once((name, meta)))?;
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            if previous_meta.is_some_and(crate::database::has_graph_edge_table_shape)
                && !crate::database::has_graph_edge_table_shape(meta) {
                self.graph_writer_window(|| self.retire_row_graph_table_in_write(&write_txn, name, None))?;
            }


#[cfg(any(test, feature = "test-seams"))]
let mut graph_publication_attempt = crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            Self::invalidate_retention_clock_in_write(&write_txn, name)?;
            let mut identity_mutations = content_identity::schema_mutations(name, meta)?;
            self.visit_relational_rows_in_write(&write_txn, name, |row| {
                Self::apply_content_identity_mutations_in_write(
                    &write_txn,
                    &[content_identity::remove_row_mutation(name, row.row_id)],
                    &mut self.proof_admission(),
                ).map(|_| ())
            })?;
            let mut ordered_rows = rows.iter().collect::<Vec<_>>();
            ordered_rows.sort_by_key(|row| (row.lsn, row.created_tx));
            for row in ordered_rows {
                identity_mutations.push(content_identity::row_mutation(name, row, Some(meta))?);
            }
            if !dropped_vector_columns.is_empty() {
                self.remove_vector_partition_records_matching_all_in_write(
                    &write_txn,
                    name,
                    Some(&dropped_vector_columns),
                    &|index: &VectorIndexRef| {
                        index.table == name && dropped_vector_columns.contains(&index.column)
                    },
                )?;
            }
            self.rewrite_vector_entries_for_schema_in_write(
                &write_txn,
                name,
                &vector_renames,
                &dropped_vector_columns,
                &vector_quantization,
            )?;
            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::meta_key(name);
                // Keep the column incarnations the schema already allocated:
                // vector routes bind to them, and a raw declaration would
                // leave every saved generation without its column identity.
                let encoded_schema = self.schema_update_in_table(&meta_table, name, meta, ddl)?;
                let encoded = &encoded_schema.bytes;
                meta_table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
                vector_membership_pages::ensure_declared_routes(
                    &write_txn,
                    name,
                    meta,
                    self.proof_accountant(),
                )?;
            }
            // Postings and the row versions they project are committed to
            // separately and must agree: see `historical_index_commitment`.
            let mut commitments = historical_index_commitment::CommitmentBatch::for_writer()?;
            {
                let table_name = Self::rel_table_name(name);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                self.retain_authored_rows_in_write(&write_txn, name)?;
                self.retire_relational_manifests_in_write(&write_txn, name)?;
                content_size::reset_rows(&write_txn, name)?;
                write_txn
                    .delete_table(table_def)
                    .map_err(Self::storage_error)?;
                let legacy_table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                write_txn
                    .delete_table(legacy_table_def)
                    .map_err(Self::storage_error)?;
                let mut redb_table =
                    content_size::open_rows(&write_txn, table_def, Some(meta))
                        .map_err(Self::storage_error)?;
                // The table is rewritten whole: the version side of every
                // index is folded afresh from the rows written here.
                for declaration in redb_table.indexes() {
                    commitments.reset(
                        &write_txn,
                        name,
                        declaration,
                        historical_index_commitment::Side::Versions,
                    )?;
                }
                for row in rows {
                    let encoded = Self::encode_versioned_row(row, Some(meta))?;
                    let key = Self::rel_row_key(row);
                    {
                        let predecessor = redb_table.get(key.as_slice()).map_err(Self::storage_error)?;
                        self.replace_relational_manifest_in_write(&write_txn, name, key.as_slice(), predecessor.as_ref().map(|body| body.value()), Some(encoded.as_slice()))?;
                    }
                    let keys = content_size::VersionLife::keys_of(redb_table.indexes(), row)?;
                    redb_table.insert_version(
                        &mut commitments,
                        key.as_slice(),
                        encoded.as_slice(),
                        &content_size::VersionLife {
                            keys: &keys,
                            created: row.created_tx,
                            deleted: row.deleted_tx,
                        },
                    )?;
                }
            }
            // An index this rewrite stops declaring is retired here, while the
            // declaration that wrote its postings is still the one they match.
            if let Some(previous_meta) = previous_meta.as_ref() {
                Self::retire_undeclared_relational_indexes_in_write(
                    &write_txn,
                    &self.path,
                    name,
                    previous_meta,
                    meta,
                    &mut self.proof_admission(),
                )?;
            }
            #[cfg(any(test, feature = "test-seams"))]
            table_meta_rewrite_checkpoint_for_test(1);
            // The ready marker is a promise about the exact committed image,
            // including an index with no rows. Rebuild its posting tables in
            // this schema-rewrite transaction before publishing that marker.
            Self::rebuild_relational_indexes_in_write(
                &write_txn,
                &self.path,
                name,
                meta,
                Some(rows),
                &mut self.proof_admission(),
                Some(&mut commitments),
            )?;
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &commitments.finish(&write_txn)?.into_parts().0,
                &mut self.proof_admission(),
            )?;
            #[cfg(any(test, feature = "test-seams"))]
            table_meta_rewrite_checkpoint_for_test(2);
            Self::retain_sync_source_provenance_for_table_rows(&write_txn, name, rows)?;
            let live_rows = rows
                .iter()
                .filter(|row| row.deleted_tx.is_none())
                .map(|row| row.row_id)
                .collect::<HashSet<_>>();
            identity_mutations.extend(
                sources
                    .keys()
                    .filter(|(table, row_id)| table == name && !live_rows.contains(row_id))
                    .map(|(table, row_id)| {
                        content_identity::remove_sync_source_mutation(table, *row_id)
                    }),
            );
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in config_values {
                    Self::raise_config_identity_floors_in_write(&write_txn, key, &encoded)?;
                    Self::update_policy_hold_release_index(&write_txn, key, true)?;
                    config_table
                        .insert(key, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    identity_mutations
                        .extend(content_identity::config_mutations(key, Some(&encoded)));
                }
            }
            for (from, to) in &vector_renames {
                self.rename_vector_partition_index_in_write(&write_txn, from, to)?;
            }
            {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                for (index, change) in ddl.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(lsn, index, ddl.len());
                    self.append_ddl_summary_in_write(&write_txn, &key, change)?;
                    let encoded = Self::encode(change)?;
                    ddl_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    identity_mutations
                        .push(content_identity::encoded_history_mutation(&key, change)?);
                }
            }
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &identity_mutations,
                &mut self.proof_admission(),
            )?;
            for row in rows {
                Self::raise_table_read_lsn_in_write(&write_txn, name, row.lsn)?;
                Self::raise_noncanonical_writer_floors_in_write(
                    &write_txn,
                    row.created_tx.max(row.deleted_tx.unwrap_or(TxId(0))),
                    row.lsn,
                    row.row_id,
                )?;
            }
            Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
            { #[cfg(any(test, feature = "test-seams"))]
            graph_observation_checkpoint(10)?;
            let graph_commit_result = write_txn.commit();
#[cfg(any(test, feature = "test-seams"))]
if graph_commit_result.is_ok() { graph_publication_attempt.committed(); graph_observation_checkpoint(11)?; }
graph_commit_result }.map_err(Self::storage_error)?;
            self.complete_writer_schema_change(schema);
            #[cfg(any(test, feature = "test-seams"))]
            table_meta_rewrite_checkpoint_for_test(3);
            Ok(())
        })
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    $visibility fn remove_table_with_config_values_and_ddl_log(
        &self,
        name: &str,
        config_values: Vec<(&str, Vec<u8>)>,
        lsn: Lsn,
        ddl: &[DdlChange],
    ) -> Result<()> {
        let schema =
            self.prepare_writer_schema_change(std::iter::once(name), std::iter::empty())?;
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;

#[cfg(any(test, feature = "test-seams"))]
let mut graph_publication_attempt = crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let mut identity_mutations = content_identity::remove_schema_mutations(name);
            self.visit_relational_rows_in_write(&write_txn, name, |row| {
                let key = crate::composite_store::retention_expiry_key(
                    name, row.row_id, row.created_tx,
                );
                let mut mutations = content_identity::config_mutations(&key, None);
                mutations.push(content_identity::remove_row_mutation(name, row.row_id));
                Self::apply_content_identity_mutations_in_write(
                    &write_txn, &mutations, &mut self.proof_admission(),
                ).map(|_| ())
            })?;
            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::meta_key(name);
                meta_table
                    .remove(key.as_str())
                    .map_err(Self::storage_error)?;
            }
            let table_name = Self::rel_table_name(name);
            let table_def: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(table_name.as_str());
            self.retain_authored_rows_in_write(&write_txn, name)?;
            self.retire_relational_manifests_in_write(&write_txn, name)?;
            content_size::reset_rows(&write_txn, name)?;
            match write_txn.delete_table(table_def) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(redb::TableError::TableTypeMismatch { .. }) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            let legacy_table_def: TableDefinition<u64, &[u8]> =
                TableDefinition::new(table_name.as_str());
            match write_txn.delete_table(legacy_table_def) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(redb::TableError::TableTypeMismatch { .. }) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            self.remove_table_source_provenance_in_write(&write_txn, name)?;
            self.remove_table_retention_expiries_in_write(&write_txn, name)?;
            if !config_values.is_empty() {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in config_values {
                    Self::raise_config_identity_floors_in_write(&write_txn, key, &encoded)?;
                    Self::update_policy_hold_release_index(&write_txn, key, true)?;
                    config_table
                        .insert(key, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    identity_mutations
                        .extend(content_identity::config_mutations(key, Some(&encoded)));
                }
            }
            if !ddl.is_empty() {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                for (index, change) in ddl.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(lsn, index, ddl.len());
                    self.append_ddl_summary_in_write(&write_txn, &key, change)?;
                    let encoded = Self::encode(change)?;
                    ddl_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    identity_mutations
                        .push(content_identity::encoded_history_mutation(&key, change)?);
                }
            }
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &identity_mutations,
                &mut self.proof_admission(),
            )?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
            { #[cfg(any(test, feature = "test-seams"))]
            graph_observation_checkpoint(10)?;
            let graph_commit_result = write_txn.commit();
#[cfg(any(test, feature = "test-seams"))]
if graph_commit_result.is_ok() { graph_publication_attempt.committed(); graph_observation_checkpoint(11)?; }
graph_commit_result }.map_err(Self::storage_error)?;
            self.complete_writer_schema_change(schema);
            Ok(())
        })
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    $visibility fn remove_table_rewrite_aux_with_config_values_and_ddl_log(
        &self,
        name: &str,
        config_values: Vec<(&str, Vec<u8>)>,
        lsn: Lsn,
        ddl: &[DdlChange],
        row_graph_edges: &[contextdb_tx::OwnedAdjEntry],
    ) -> Result<()> {
        let schema =
            self.prepare_writer_schema_change(std::iter::once(name), std::iter::empty())?;
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;

#[cfg(any(test, feature = "test-seams"))]
let mut graph_publication_attempt = crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let mut identity_mutations = content_identity::remove_schema_mutations(name);
            self.visit_relational_rows_in_write(&write_txn, name, |row| {
                let key = crate::composite_store::retention_expiry_key(
                    name, row.row_id, row.created_tx,
                );
                let mut mutations = content_identity::config_mutations(&key, None);
                mutations.push(content_identity::remove_row_mutation(name, row.row_id));
                Self::apply_content_identity_mutations_in_write(
                    &write_txn, &mutations, &mut self.proof_admission(),
                ).map(|_| ())
            })?;
            self.remove_vector_partition_records_matching_all_in_write(
                &write_txn,
                name,
                None,
                &|index: &VectorIndexRef| index.table == name,
            )?;
            self.remove_vector_entries_with_identity_in_write(
                &write_txn, &|index| index.table == name, true, Some(name),
            )?;
            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::meta_key(name);
                meta_table
                    .remove(key.as_str())
                    .map_err(Self::storage_error)?;
            }
            let table_name = Self::rel_table_name(name);
            let table_def: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(table_name.as_str());
            self.retain_authored_rows_in_write(&write_txn, name)?;
            self.retire_relational_manifests_in_write(&write_txn, name)?;
            content_size::reset_rows(&write_txn, name)?;
            match write_txn.delete_table(table_def) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(redb::TableError::TableTypeMismatch { .. }) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            let legacy_table_def: TableDefinition<u64, &[u8]> =
                TableDefinition::new(table_name.as_str());
            match write_txn.delete_table(legacy_table_def) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(redb::TableError::TableTypeMismatch { .. }) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            self.remove_table_source_provenance_in_write(&write_txn, name)?;
            self.remove_table_retention_expiries_in_write(&write_txn, name)?;

            self.graph_writer_window(|| Self::erase_row_graph_in_write(&write_txn, row_graph_edges))?;

            if !config_values.is_empty() {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in config_values {
                    Self::raise_config_identity_floors_in_write(&write_txn, key, &encoded)?;
                    Self::update_policy_hold_release_index(&write_txn, key, true)?;
                    config_table
                        .insert(key, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    identity_mutations
                        .extend(content_identity::config_mutations(key, Some(&encoded)));
                }
            }
            if !ddl.is_empty() {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                for (index, change) in ddl.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(lsn, index, ddl.len());
                    self.append_ddl_summary_in_write(&write_txn, &key, change)?;
                    let encoded = Self::encode(change)?;
                    ddl_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    identity_mutations
                        .push(content_identity::encoded_history_mutation(&key, change)?);
                }
            }
            Self::apply_content_identity_mutations_in_write(
                &write_txn,
                &identity_mutations,
                &mut self.proof_admission(),
            )?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
            { #[cfg(any(test, feature = "test-seams"))]
            graph_observation_checkpoint(10)?;
            let graph_commit_result = write_txn.commit();
#[cfg(any(test, feature = "test-seams"))]
if graph_commit_result.is_ok() { graph_publication_attempt.committed(); graph_observation_checkpoint(11)?; }
graph_commit_result }.map_err(Self::storage_error)?;
            self.complete_writer_schema_change(schema);
            Ok(())
        })
    }
        }
    };
}
#[cfg(not(any(test, feature = "test-seams")))]
persistence_mutation_surface!(pub(crate));
#[cfg(any(test, feature = "test-seams"))]
persistence_mutation_surface!(pub);

#[cfg(all(test, feature = "test-seams"))]
#[path = "persistence/cdb141_ordered_direction_tests.rs"]
mod cdb141_ordered_direction_tests;

// CDB127 returned-loader rows extend the existing immutable mapped entrypoint;
// no new test body or allocator is introduced.
#[cfg(test)]
thread_local! {
    static RETURNED_LOADER_AFTER_RAW_OBSERVATION_FOR_TEST: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    static RETURNED_LOADER_PARTIAL_PANIC_FOR_TEST: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    static RETURNED_LOADER_OWNER_DROP_PANIC_FOR_TEST: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
mod bounded_journal_suffix_tests;

/// A synchronous loan from one captured storage transaction. Repeated count,
/// decode and export passes use this same root and declared operation reader.
pub(crate) struct OwnedEncodedEventSource<'s> {
    read: &'s redb::ReadTransaction,
    loaded: Option<&'s BTreeMap<String, Vec<(u64, Vec<u8>)>>>,
    read_memory: &'s contextdb_core::read_memory::ReadMemoryOwner,
}

/// A synchronous cold queue loan. Its transaction pins the actual storage
/// root throughout extent lookup and bounded decoding. Only owned bodies and
/// scalar continuation/identity may leave the higher-ranked loan.
pub(crate) struct OwnedColdEventSource<'s> {
    read: &'s redb::ReadTransaction,
}

struct OwnedEventReadKey {
    value: String,
    memory: contextdb_core::read_memory::ReadMemoryOwner,
    bytes: usize,
}

pub(crate) struct OwnedEncodedBytes {
    pub(crate) bytes: Vec<u8>,
    allocation: crate::memory_accounting::OwnedMemoryReservation,
}

struct OwnedEventEncodedLength(usize);

pub(crate) struct OwnedEventKey {
    pub(crate) value: String,
    #[allow(
        dead_code,
        reason = "KEEP-GUARD: held for its Drop; releases what it holds when the owner drops"
    )]
    allocation: crate::memory_accounting::OwnedMemoryReservation,
}

pub(crate) struct OwnedSinkQueueRemovals {
    pub(crate) mutations: Vec<content_identity::ContentIdentityMutation>,
    credits: Vec<crate::memory_accounting::OwnedMemoryReservation>,
    #[allow(
        dead_code,
        reason = "KEEP-GUARD: held for its Drop; releases what it holds when the owner drops"
    )]
    mutations_allocation: crate::memory_accounting::OwnedMemoryReservation,
    #[allow(
        dead_code,
        reason = "KEEP-GUARD: held for its Drop; releases what it holds when the owner drops"
    )]
    credits_allocation: crate::memory_accounting::OwnedMemoryReservation,
}

pub(crate) struct OriginalDecoded<T> {
    pub(crate) value: T,
    pub(crate) allocation: crate::memory_accounting::OwnedMemoryReservation,
}

/// A synchronous, stack-scoped native seed. Its pointer cannot leave this
/// private codec: all access occurs before decode returns, including unwind.
/// The returned typed value keeps its own exact original reservation.
/// Compile-time selection for the two audited, allocation-free streamed blob
/// projections. No blanket DeserializeOwned implementation is permitted.
pub(crate) trait StreamedBlobProjection: serde::de::DeserializeOwned {}

mod original_event_decode;

/// Operation-local preparation custody. All values produced by its private
/// helpers are consumed inside the enclosing synchronous flush; this frame
/// is declared before them and refunds only after their physical destruction.
pub(crate) struct OriginalPersistenceFrame<'o> {
    pub(crate) original: &'o crate::memory_accounting::OwnedMemoryReservation,
    work: std::cell::RefCell<crate::memory_accounting::OwnedMemoryReservation>,
}

mod original_frame_and_mutations;
pub(crate) use original_frame_and_mutations::*;

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    /// The one identity-mutation preparation site whose next growth this
    /// thread refuses, if any.
    static REFUSED_IDENTITY_GROWTH_SITE: std::cell::Cell<Option<&'static str>> =
        const { std::cell::Cell::new(None) };
}
// The legacy bridge uses the same writer operation that owns this synchronous
// flush. Its container is destroyed before that operation refunds its storage.
struct WriterMutationVector {
    values: Vec<content_identity::ContentIdentityMutation>,
    owner: crate::read_image_memory::ImageMemoryHandle,
}
struct OriginalMutationVector<'f, 'o> {
    values: Vec<content_identity::ContentIdentityMutation>,
    frame: &'f OriginalPersistenceFrame<'o>,
    funded_capacity: usize,
}

struct OriginalWindowMemory(std::cell::RefCell<crate::memory_accounting::OwnedMemoryReservation>);

mod original_relational_manifest;

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod reader_descriptor_ownership_proofs;

#[cfg(test)]
mod erasure_frontier_contract_tests;

#[cfg(test)]
#[path = "persistence/direct_source_contract_rows.rs"]
mod direct_source_contract_rows;

#[cfg(test)]
mod cache_mutation_funding_tests;

#[cfg(test)]
mod historical_root_guard_tests;

#[cfg(test)]
mod retention_retired_binding_guard_tests;

#[cfg(test)]
mod retention_obligations_guard_tests;

#[cfg(all(test, target_os = "macos"))]
mod macos_process_info_observation;

/// Exclusive ownership and error cleanup address the same lock. Reader
/// ownership probes query the separate owner byte without acquiring a hold. macOS whole-file locks overlap
/// the reader and claim-window bytes, so ownership uses a separate OFD byte.
/// An older whole-file owner still overlaps that byte and excludes a writer.
#[derive(Clone, Copy)]
enum CompanionOwnershipLock {
    Exclusive,
    Unlock,
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
#[path = "persistence/reader_coordination_wait_tests.rs"]
mod reader_coordination_wait_tests;
