use crate::blob_repository::{
    BlobAuthoritativePurgeProjection, apply_authoritative_purge_in_write,
};
use crate::composite_store::{ChangeLogEntry, sync_source_lsn_updates};
use crate::database::TriggerAuditEntry;
use crate::database::event_bus::{EventBusPersistenceCommit, PreparedSinkEvent, SinkQueueEntry};
use crate::database::trigger::TriggerPersistenceCommit;
use crate::sync_types::DdlChange;
use contextdb_core::{
    AdjEntry, ColumnType, Error, ForeignKeyReference, IndexDecl, Lsn, PropagationRule, RankPolicy,
    Result, RowId, StateMachineConstraint, TableMeta, TxId, Value, VectorEntry, VectorIndexRef,
    VectorQuantization, VersionedRow, Wallclock,
};
use contextdb_relational::store::SyncSourceKind;
use contextdb_tx::WriteSet;
use redb::{
    Key as RedbKey, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
    TableHandle, Value as RedbValue,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(target_os = "linux")]
use std::os::unix::fs::MetadataExt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// Serializes the process-global panic-hook swap in `open_hook_suppressed` so
/// two concurrent store opens cannot interleave `take_hook`/`set_hook` and
/// leave the no-op hook permanently installed.
static HOOK_SWAP: Mutex<()> = Mutex::new(());

/// Bound Redb's process-resident page cache independently of database size.
/// Large blob payloads remain disk-backed instead of accumulating in Redb's
/// one-GiB default cache.
const REDB_CACHE_BYTES: usize = 8 * 1024 * 1024;

fn redb_builder() -> redb::Builder {
    let mut builder = redb::Builder::new();
    builder.set_cache_size(REDB_CACHE_BYTES);
    builder
}

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

#[cfg(test)]
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
}

/// Arm the one-shot test injection seam: the NEXT `compact()` call on THIS
/// thread fails to reopen after closing the handle. Production-dead —
/// nothing but a test calls it.
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn arm_handle_recycle_reopen_fault_for_test() {
    HANDLE_RECYCLE_REOPEN_FAULT.with(|f| f.set(true));
}

/// Consume the armed injection flag, if any. Reads a thread-local that is
/// never set outside a test, so the production path takes the `false` arm.
fn take_handle_recycle_reopen_fault_for_test() -> bool {
    HANDLE_RECYCLE_REOPEN_FAULT.with(|f| f.replace(false))
}

#[cfg(test)]
pub(crate) fn arm_received_schema_pre_commit_fault_for_test() {
    RECEIVED_SCHEMA_PRE_COMMIT_FAULT.with(|fault| fault.set(true));
}

fn take_received_schema_pre_commit_fault_for_test() -> bool {
    RECEIVED_SCHEMA_PRE_COMMIT_FAULT.with(|fault| fault.replace(false))
}

/// Arm the one-shot received-schema side-effect checkpoint fault for this
/// test thread. This seam is absent from production builds.
#[cfg(test)]
pub(crate) fn arm_received_schema_side_effect_persistence_fault_for_test() {
    RECEIVED_SCHEMA_SIDE_EFFECT_PERSISTENCE_FAULT.with(|fault| fault.set(true));
}

#[cfg(test)]
fn take_received_schema_side_effect_persistence_fault_for_test() -> bool {
    RECEIVED_SCHEMA_SIDE_EFFECT_PERSISTENCE_FAULT.with(|fault| fault.replace(false))
}

#[cfg(test)]
pub(crate) fn arm_authoritative_purge_point_remove_persistence_failure_for_test() {
    AUTHORITATIVE_PURGE_PRE_COMMIT_FAULT.with(|fault| fault.set(true));
}

#[cfg(test)]
fn take_authoritative_purge_point_remove_persistence_failure_for_test() -> bool {
    AUTHORITATIVE_PURGE_PRE_COMMIT_FAULT.with(|fault| fault.replace(false))
}

const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const FORMAT_METADATA_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("metadata");
const CONFIG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("config");
const CHANGE_LOG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("change_log");
const DDL_LOG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("ddl_log");
const COMMIT_INDEX_TABLE: TableDefinition<u64, u64> = TableDefinition::new("commit_index");
const SINK_AUDIT_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("__sink_audit");
const TRIGGER_AUDIT_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("__trigger_audit");
/// When each durable `__trigger_audit` row was written, keyed identically to
/// the audit row itself. Kept beside the history rather than inside them so
/// existing on-disk audit payloads decode unchanged; retention reads it to
/// decide what has aged out. A legacy row with no stamp is never pruned.
const TRIGGER_AUDIT_STAMPS_TABLE: TableDefinition<&str, u64> =
    TableDefinition::new("__trigger_audit_stamps");
const GRAPH_FWD_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("graph_fwd");
const GRAPH_REV_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("graph_rev");
const VECTORS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("vector_entries");
const SYNC_ROW_SOURCE_LSN_TABLE: TableDefinition<&[u8], u64> =
    TableDefinition::new("sync_row_source_lsn");
const SYNC_ROW_SOURCE_KIND_TABLE: TableDefinition<&[u8], u8> =
    TableDefinition::new("sync_row_source_kind");
const FORMAT_VERSION_KEY: &str = "format_version";
pub(crate) const CURRENT_FORMAT_VERSION: &str = "1.0.0";
const TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY: &str = "__trigger_audit_next_index";
const TRIGGER_AUDIT_RING_CONFIG_KEY: &str = "__trigger_audit_ring";

fn sync_source_kind_to_u8(kind: SyncSourceKind) -> u8 {
    match kind {
        SyncSourceKind::Pulled => 0,
        SyncSourceKind::AcceptedLocal => 1,
        SyncSourceKind::AcceptedLocalPending => 2,
    }
}

pub struct RedbPersistence {
    path: std::path::PathBuf,
    lock_file: Mutex<Option<File>>,
    db: Mutex<Option<redb::Database>>,
    /// Set the first time a table's meta decodes only via the exact
    /// pre-`contextdb migrate` `v1.0.0` layout (see
    /// [`Self::decode_table_meta_versioned`]) rather than the current one.
    /// `Database::open` refuses on this (`Error::LegacyVectorStoreDetected`,
    /// naming `contextdb migrate`); the migrate command's own loader reads
    /// past it. Never set back to `false` — one legacy table is enough to
    /// mark the whole root.
    used_legacy_table_meta_layout: AtomicBool,
}

pub(crate) struct RawSyncApplyStateDigest {
    pub(crate) digest: String,
    pub(crate) trigger_audit_entries: u64,
    pub(crate) sink_audit_entries: u64,
    pub(crate) sink_queue_entries: u64,
}

fn digest_field(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn digest_redb_table<K: RedbKey + 'static, V: RedbValue + 'static>(
    read_txn: &redb::ReadTransaction,
    definition: TableDefinition<K, V>,
    label: &str,
    hasher: &mut blake3::Hasher,
) -> Result<u64> {
    digest_field(hasher, label.as_bytes());
    let table = match read_txn.open_table(definition) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
        Err(err) => {
            return Err(Error::Other(format!(
                "snapshot digest table open failed: {err}"
            )));
        }
    };
    let mut count = 0_u64;
    for entry in table
        .iter()
        .map_err(|err| Error::Other(format!("snapshot digest table iteration failed: {err}")))?
    {
        let (key, value) = entry
            .map_err(|err| Error::Other(format!("snapshot digest entry read failed: {err}")))?;
        let key_value = key.value();
        let value_value = value.value();
        let key_bytes = K::as_bytes(&key_value);
        let value_bytes = V::as_bytes(&value_value);
        digest_field(hasher, key_bytes.as_ref());
        digest_field(hasher, value_bytes.as_ref());
        count = count.saturating_add(1);
    }
    Ok(count)
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
}

pub(crate) struct SchemaDdlPersistence<'a> {
    pub(crate) event_bus: Option<&'a EventBusPersistenceCommit>,
    pub(crate) trigger: Option<&'a TriggerPersistenceCommit>,
}

#[derive(Default)]
pub(crate) struct FlushDataSnapshots {
    pub(crate) table_meta: Option<HashMap<String, TableMeta>>,
    pub(crate) deleted_rows: Option<HashMap<String, HashMap<RowId, VersionedRow>>>,
}

pub(crate) struct FlushDataOptions<'a> {
    pub(crate) sink_events: &'a [PreparedSinkEvent],
    pub(crate) trigger_audits: &'a [(u64, TriggerAuditEntry)],
    pub(crate) schema_ddl: SchemaDdlPersistence<'a>,
    pub(crate) max_sink_queue_depth: usize,
    pub(crate) snapshots: FlushDataSnapshots,
    /// A complete prepared received-schema image. When present, it replaces
    /// the ordinary incremental Redb writes in this same transaction; memory
    /// publication remains the transaction manager's after-apply work.
    pub(crate) received_schema: Option<&'a ReceivedSchemaPersistenceProjection>,
}

/// The durable half of a private received-schema stage. It is fully owned and
/// encoded before Redb begins its write transaction, so persistence cannot
/// regroup source DDL or consult live schema state while committing it.
pub(crate) struct ReceivedSchemaPersistenceProjection {
    pub(crate) table_meta: HashMap<String, TableMeta>,
    pub(crate) rows: HashMap<String, Vec<VersionedRow>>,
    pub(crate) sync_sources: Vec<(String, RowId, Lsn, SyncSourceKind)>,
    pub(crate) edges: Vec<AdjEntry>,
    pub(crate) vectors: Vec<VectorEntry>,
    pub(crate) ddl_log: Vec<(Lsn, DdlChange)>,
    pub(crate) config_values: Vec<(String, Vec<u8>)>,
    pub(crate) config_max_u64_keys: Vec<String>,
    /// A received DROP TABLE retires this generation's data history. Its
    /// row/vector entries must leave the durable log in the same transaction
    /// that installs the replacement schema image.
    pub(crate) structurally_dropped_tables: HashSet<String>,
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
    pub(crate) graph_entries: Vec<AdjEntry>,
    pub(crate) sink_entries: Vec<(String, u64, Vec<u8>)>,
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

impl PersistedVector {
    fn from_f32(vector: &[f32], quantization: VectorQuantization) -> Self {
        match quantization {
            VectorQuantization::F32 => PersistedVector::F32(vector.to_vec()),
            VectorQuantization::SQ8 => {
                let (min, max) = vector_min_max(vector);
                let range = max - min;
                let payload = if range <= f32::EPSILON {
                    vec![0; vector.len()]
                } else {
                    vector
                        .iter()
                        .map(|value| {
                            (((*value - min) / range) * 255.0).round().clamp(0.0, 255.0) as u8
                        })
                        .collect()
                };
                PersistedVector::SQ8 {
                    min,
                    max,
                    len: vector.len() as u32,
                    payload,
                }
            }
            VectorQuantization::SQ4 => {
                let (min, max) = vector_min_max(vector);
                let range = max - min;
                let mut payload = Vec::with_capacity(vector.len().div_ceil(2));
                let quantized = if range <= f32::EPSILON {
                    vec![0; vector.len()]
                } else {
                    vector
                        .iter()
                        .map(|value| {
                            (((*value - min) / range) * 15.0).round().clamp(0.0, 15.0) as u8
                        })
                        .collect::<Vec<_>>()
                };
                for pair in quantized.chunks(2) {
                    let hi = pair[0] & 0x0f;
                    let lo = pair.get(1).copied().unwrap_or(0) & 0x0f;
                    payload.push((hi << 4) | lo);
                }
                PersistedVector::SQ4 {
                    min,
                    max,
                    len: vector.len() as u32,
                    payload,
                }
            }
        }
    }

    fn to_f32(&self) -> Vec<f32> {
        match self {
            PersistedVector::F32(vector) => vector.clone(),
            PersistedVector::SQ8 {
                min,
                max,
                len,
                payload,
            } => {
                let range = *max - *min;
                payload
                    .iter()
                    .take(*len as usize)
                    .map(|byte| {
                        if range <= f32::EPSILON {
                            *min
                        } else {
                            *min + ((*byte as f32) / 255.0) * range
                        }
                    })
                    .collect()
            }
            PersistedVector::SQ4 {
                min,
                max,
                len,
                payload,
            } => {
                let range = *max - *min;
                let mut values = Vec::with_capacity(*len as usize);
                for byte in payload {
                    for q in [byte >> 4, byte & 0x0f] {
                        if values.len() == *len as usize {
                            break;
                        }
                        values.push(if range <= f32::EPSILON {
                            *min
                        } else {
                            *min + ((q as f32) / 15.0) * range
                        });
                    }
                }
                values
            }
        }
    }
}

fn vector_min_max(vector: &[f32]) -> (f32, f32) {
    let Some((first, rest)) = vector.split_first() else {
        return (0.0, 0.0);
    };
    rest.iter()
        .copied()
        .fold((*first, *first), |(min, max), value| {
            (min.min(value), max.max(value))
        })
}

/// The exact `ColumnDef` layout `v1.0.0` (git tag `v1.0.0`, commit `e6cf60c`)
/// wrote to disk — 11 fields, missing `context_id`/`scope_label`/`acl_ref`.
/// See `RedbPersistence::decode_table_meta_versioned` for why a genuinely
/// older-shaped payload needs its own struct rather than leaning on the
/// current `ColumnDef`'s own trailing-field tolerance (which only protects a
/// shorter CURRENT-shaped payload, never one from a release whose struct had
/// fewer fields than today's).
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

impl From<LegacyColumnDefV1> for contextdb_core::ColumnDef {
    fn from(legacy: LegacyColumnDefV1) -> Self {
        contextdb_core::ColumnDef {
            name: legacy.name,
            column_type: legacy.column_type,
            nullable: legacy.nullable,
            primary_key: legacy.primary_key,
            unique: legacy.unique,
            default: legacy.default,
            references: legacy.references,
            expires: legacy.expires,
            immutable: legacy.immutable,
            quantization: legacy.quantization,
            rank_policy: legacy.rank_policy,
            // Fields v1.0.0 never wrote: honest, unset defaults.
            context_id: false,
            scope_label: None,
            acl_ref: None,
        }
    }
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

impl From<LegacyTableMetaV1> for TableMeta {
    fn from(legacy: LegacyTableMetaV1) -> Self {
        TableMeta {
            columns: legacy.columns.into_iter().map(Into::into).collect(),
            immutable: legacy.immutable,
            state_machine: legacy.state_machine,
            dag_edge_types: legacy.dag_edge_types,
            unique_constraints: legacy.unique_constraints,
            natural_key_column: legacy.natural_key_column,
            propagation_rules: legacy.propagation_rules,
            default_ttl_seconds: legacy.default_ttl_seconds,
            sync_safe: legacy.sync_safe,
            expires_column: legacy.expires_column,
            indexes: legacy.indexes,
            // Fields v1.0.0 never wrote: honest, unset defaults.
            composite_foreign_keys: Vec::new(),
            sync_direction: None,
            retain_declared_unit: None,
            primary_key_columns: Vec::new(),
            conflict_policy: None,
            history_policy: None,
        }
    }
}

impl RedbPersistence {
    pub fn create(path: &Path) -> Result<Self> {
        let lock_file = Self::acquire_pid_lock(path)?;
        Self::create_with_lock(path, lock_file)
    }

    /// Recreate a store while holding its existing coordination lock. The lock
    /// file is never unlinked: its OS lock is the ownership boundary that
    /// prevents another process from opening the path between removal and the
    /// fresh format marker being written.
    pub fn recreate(path: &Path) -> Result<Self> {
        let lock_file = Self::acquire_pid_lock(path)?;
        if path.exists()
            && let Err(err) = std::fs::remove_file(path)
        {
            drop(lock_file);
            return Err(Self::storage_error(err));
        }
        Self::create_with_lock(path, lock_file)
    }

    fn create_with_lock(path: &Path, lock_file: File) -> Result<Self> {
        // `create` opens the file in place when it already exists, so it can hit
        // the same corrupt-file redb panic as `open` — guard it identically so
        // no open-time backtrace leaks to stderr.
        let created = Self::open_hook_suppressed(|| redb_builder().create(path));
        let create_result = match created {
            Ok(inner) => inner.map_err(|err| Self::storage_open_error(path, &lock_file, err)),
            Err(_) => Err(Error::StoreCorrupted {
                path: path.display().to_string(),
                reason: format!(
                    "metadata/format read panicked; store may be truncated or corrupt — {}",
                    Self::CORRUPT_STORE_NEXT_STEP
                ),
            }),
        };
        let result = match create_result {
            Ok(db) => match Self::write_format_marker(&db) {
                Ok(()) => Ok(Self {
                    path: path.to_path_buf(),
                    lock_file: Mutex::new(Some(lock_file)),
                    db: Mutex::new(Some(db)),
                    used_legacy_table_meta_layout: AtomicBool::new(false),
                }),
                Err(err) => {
                    drop(db);
                    drop(lock_file);
                    Err(err)
                }
            },
            Err(err) => {
                drop(lock_file);
                Err(err)
            }
        };
        if result.is_err() {
            Self::release_pid_lock(path);
        }
        result
    }

    pub fn open(path: &Path) -> Result<Self> {
        let lock_file = Self::acquire_pid_lock(path)?;
        let result = match Self::open_db_checked(path, &lock_file) {
            Ok(db) => match Self::validate_format_marker(&db, path) {
                Ok(()) => Ok(Self {
                    path: path.to_path_buf(),
                    lock_file: Mutex::new(Some(lock_file)),
                    db: Mutex::new(Some(db)),
                    used_legacy_table_meta_layout: AtomicBool::new(false),
                }),
                Err(err) => {
                    drop(db);
                    drop(lock_file);
                    Err(err)
                }
            },
            Err(err) => {
                drop(lock_file);
                Err(err)
            }
        };
        if result.is_err() {
            Self::release_pid_lock(path);
        }
        result
    }

    pub fn close(&self) {
        let db = self.db.lock().expect("redb mutex poisoned").take();
        drop(db);
        let lock_file = self
            .lock_file
            .lock()
            .expect("pid lock mutex poisoned")
            .take();
        if lock_file.is_some() {
            drop(lock_file);
            Self::release_pid_lock(&self.path);
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Whether any table's meta on this root decoded only via the legacy
    /// pre-`contextdb migrate` layout (see
    /// [`Self::decode_table_meta_versioned`]) rather than the current one.
    /// Set the first time [`Self::load_all_table_meta`] hits that fallback;
    /// never cleared. `Database::open` consults this to refuse a
    /// schema-level-legacy root instead of silently loading it.
    pub fn used_legacy_table_meta_layout(&self) -> bool {
        self.used_legacy_table_meta_layout.load(Ordering::Relaxed)
    }

    /// Whether the store at `path` is legacy-format: either its top-level
    /// format-version marker does not match [`CURRENT_FORMAT_VERSION`], or
    /// the marker matches but the underlying `TableMeta`/`ColumnDef` schema
    /// layout still only decodes via [`Self::decode_table_meta_versioned`]'s
    /// legacy fallback (the `v1.0.0` case this whole module exists for).
    ///
    /// Decided through a `redb::ReadOnlyDatabase` handle SPECIFICALLY so
    /// this check itself never mutates the file: a plain read-write
    /// `redb::Database::open` performs a housekeeping write on every open —
    /// independent of any application-level transaction — which would break
    /// `contextdb migrate`'s "refuse an already-current-format root
    /// untouched" contract if this detection used one.
    pub fn is_legacy_format_store(path: &Path) -> Result<bool> {
        // Suppressed + `catch_unwind`-guarded exactly like `open_db_checked`'s
        // read-write open: a truncated/corrupt file can trip redb's internal
        // page_manager assertion (a panic, not a clean `Err`) on the
        // READ-ONLY path too, and this call must never let that raw panic
        // (or its backtrace) escape to `repair`/`migrate`'s caller.
        let opened = Self::open_hook_suppressed(|| redb_builder().open_read_only(path));
        let ro = match opened {
            Ok(Ok(db)) => db,
            Ok(Err(err)) => return Err(Self::storage_error(err)),
            Err(_) => {
                return Err(Error::StoreCorrupted {
                    path: path.display().to_string(),
                    reason: format!(
                        "metadata/format read panicked; store may be truncated or corrupt — {}",
                        Self::CORRUPT_STORE_NEXT_STEP
                    ),
                });
            }
        };
        let read_txn = ro.begin_read().map_err(Self::storage_error)?;
        let format_table = match read_txn.open_table(FORMAT_METADATA_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(true),
            Err(err) => return Err(Self::storage_error(err)),
        };
        let Some(marker_bytes) = format_table
            .get(FORMAT_VERSION_KEY)
            .map_err(Self::storage_error)?
        else {
            return Ok(true);
        };
        let marker: String = Self::decode(marker_bytes.value())?;
        if marker != CURRENT_FORMAT_VERSION {
            return Ok(true);
        }

        let meta_table = match read_txn.open_table(META_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
            Err(err) => return Err(Self::storage_error(err)),
        };
        for entry in meta_table.iter().map_err(Self::storage_error)? {
            let (key, value) = entry.map_err(Self::storage_error)?;
            if key.value().strip_prefix("table:").is_some() {
                let (_, via_legacy) = Self::decode_table_meta_versioned(value.value())?;
                if via_legacy {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// PID-based advisory lock backed by an OS file lock.
    ///
    /// The lock file may remain after a crash; the kernel lock is released when
    /// the process exits, so stale files are reclaimed without unlink races.
    fn acquire_pid_lock(path: &Path) -> Result<File> {
        let lock_path = path.with_extension("lock");
        let mut file = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
        {
            Ok(file) => file,
            Err(err) => return Err(Self::storage_error(err)),
        };
        if !try_lock_exclusive(&file)? {
            let holder_pid = read_lock_pid_for_locked_file(&mut file).unwrap_or(0);
            return Err(Error::DatabaseLocked {
                holder_pid,
                path: path.to_path_buf(),
            });
        }
        if let Err(err) = file.set_len(0) {
            unlock_file(&file);
            return Err(Self::storage_error(err));
        }
        if let Err(err) = file.seek(SeekFrom::Start(0)) {
            unlock_file(&file);
            return Err(Self::storage_error(err));
        }
        if let Err(err) = write!(file, "{}", std::process::id()) {
            unlock_file(&file);
            return Err(Self::storage_error(err));
        }
        if let Err(err) = file.sync_all() {
            unlock_file(&file);
            return Err(Self::storage_error(err));
        }
        Ok(file)
    }

    fn release_pid_lock(path: &Path) {
        let _ = path;
    }

    /// One-line actionable recovery step appended to a corrupt-store error so
    /// the user is never left with a dead end: run `contextdb repair` to see
    /// what is salvageable (never modifies the store), or `contextdb reset
    /// --force` to recreate it once you've restored from a backup or a
    /// healthy sync peer if you need the data first.
    pub(crate) const CORRUPT_STORE_NEXT_STEP: &'static str = "run `contextdb repair <path>` to see what is \
        salvageable (it never modifies the store), or `contextdb reset <path> --force` to \
        recreate it — restore from a backup or a healthy sync peer first if you need the \
        existing data";

    /// Run a redb store-open closure with the default panic hook suppressed.
    ///
    /// redb can panic (an internal assertion) when a truncated/corrupt file has
    /// a valid-looking header but an inconsistent page layout. `catch_unwind`
    /// catches the unwind, but it does NOT stop the default panic hook from
    /// printing the raw crate location (`redb-x/.../page_manager.rs:NN`) to
    /// stderr first — an implementation leak into the user's terminal. Silence
    /// the hook for the minimal window around the open, then restore it.
    ///
    /// CAVEAT: `set_hook`/`take_hook` are process-global, so the swap is
    /// serialized by `HOOK_SWAP` to stop two concurrent opens from interleaving
    /// (which could permanently leave the no-op hook installed). The window is
    /// kept as tight as possible — only around the one open call. The mutex
    /// cannot protect an UNRELATED concurrent panic on another thread from being
    /// silenced during that tiny window; that is inherent to global-hook
    /// suppression and is accepted.
    fn open_hook_suppressed<T>(f: impl FnOnce() -> T) -> std::thread::Result<T> {
        // Poison-tolerant: a panic is exactly what we wrap, so a poisoned guard
        // is expected — recover the inner `()` and carry on.
        let _guard = HOOK_SWAP.lock().unwrap_or_else(|e| e.into_inner());
        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let outcome = catch_unwind(AssertUnwindSafe(f));
        std::panic::set_hook(prev_hook);
        outcome
    }

    fn open_db_checked(path: &Path, lock_file: &File) -> Result<redb::Database> {
        let opened = Self::open_hook_suppressed(|| redb_builder().open(path));
        match opened {
            Ok(Ok(db)) => Ok(db),
            Ok(Err(err)) if err.to_string().contains("already open") => {
                Err(Self::locked_database_error(path, lock_file))
            }
            Ok(Err(err)) => Err(Error::StoreCorrupted {
                path: path.display().to_string(),
                reason: format!(
                    "metadata/format could not be read: {err} — {}",
                    Self::CORRUPT_STORE_NEXT_STEP
                ),
            }),
            Err(_) => Err(Error::StoreCorrupted {
                path: path.display().to_string(),
                reason: format!(
                    "metadata/format read panicked; store may be truncated or corrupt — {}",
                    Self::CORRUPT_STORE_NEXT_STEP
                ),
            }),
        }
    }

    fn write_format_marker(db: &redb::Database) -> Result<()> {
        let write_txn = db.begin_write().map_err(Self::storage_error)?;
        {
            let mut table = write_txn
                .open_table(FORMAT_METADATA_TABLE)
                .map_err(Self::storage_error)?;
            let encoded = Self::encode(&CURRENT_FORMAT_VERSION.to_string())?;
            table
                .insert(FORMAT_VERSION_KEY, encoded.as_slice())
                .map_err(Self::storage_error)?;
        }
        write_txn.commit().map_err(Self::storage_error)?;
        Ok(())
    }

    fn validate_format_marker(db: &redb::Database, path: &Path) -> Result<()> {
        let read_txn = db.begin_read().map_err(|err| Error::StoreCorrupted {
            path: path.display().to_string(),
            reason: format!(
                "metadata read failed: {err} — {}",
                Self::CORRUPT_STORE_NEXT_STEP
            ),
        })?;
        let table = match read_txn.open_table(FORMAT_METADATA_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Err(Error::LegacyVectorStoreDetected {
                    found_format_marker: String::new(),
                    expected_release: CURRENT_FORMAT_VERSION.to_string(),
                });
            }
            Err(err) => {
                return Err(Error::StoreCorrupted {
                    path: path.display().to_string(),
                    reason: format!(
                        "metadata table could not be read: {err} — {}",
                        Self::CORRUPT_STORE_NEXT_STEP
                    ),
                });
            }
        };
        let value = table
            .get(FORMAT_VERSION_KEY)
            .map_err(|err| Error::StoreCorrupted {
                path: path.display().to_string(),
                reason: format!(
                    "metadata format_version could not be read: {err} — {}",
                    Self::CORRUPT_STORE_NEXT_STEP
                ),
            })?
            .ok_or_else(|| Error::StoreCorrupted {
                path: path.display().to_string(),
                reason: format!(
                    "metadata table is missing format_version — {}",
                    Self::CORRUPT_STORE_NEXT_STEP
                ),
            })?;
        let marker: String = Self::decode(value.value()).map_err(|err| Error::StoreCorrupted {
            path: path.display().to_string(),
            reason: format!(
                "metadata format_version is corrupt: {err} — {}",
                Self::CORRUPT_STORE_NEXT_STEP
            ),
        })?;
        if marker == CURRENT_FORMAT_VERSION {
            Ok(())
        } else {
            Err(Error::LegacyVectorStoreDetected {
                found_format_marker: marker,
                expected_release: CURRENT_FORMAT_VERSION.to_string(),
            })
        }
    }

    pub fn flush_data(&self, ws: &WriteSet) -> Result<()> {
        self.flush_data_with_logs(ws, &[])
    }

    pub fn flush_data_with_logs(&self, ws: &WriteSet, change_log: &[ChangeLogEntry]) -> Result<()> {
        self.flush_data_with_logs_and_sink_events(
            ws,
            change_log,
            FlushDataOptions {
                sink_events: &[],
                trigger_audits: &[],
                schema_ddl: SchemaDdlPersistence {
                    event_bus: None,
                    trigger: None,
                },
                max_sink_queue_depth: usize::MAX,
                snapshots: FlushDataSnapshots::default(),
                received_schema: None,
            },
        )
    }

    pub(crate) fn flush_data_with_logs_and_sink_events(
        &self,
        ws: &WriteSet,
        change_log: &[ChangeLogEntry],
        options: FlushDataOptions<'_>,
    ) -> Result<()> {
        let table_meta = match options.snapshots.table_meta {
            Some(table_meta) => table_meta,
            None => self.load_all_table_meta()?,
        };
        let deleted_rows_snapshot = options.snapshots.deleted_rows;
        let sink_events = options.sink_events;
        let trigger_audits = options.trigger_audits;
        let schema_ddl = options.schema_ddl;
        let max_sink_queue_depth = options.max_sink_queue_depth;
        if let Some(received_schema) = options.received_schema {
            return self.flush_received_schema_stage(
                ws,
                change_log,
                received_schema,
                sink_events,
                trigger_audits,
                max_sink_queue_depth,
            );
        }
        let has_vector_changes = !ws.vector_deletes.is_empty()
            || !ws.vector_inserts.is_empty()
            || !ws.vector_moves.is_empty();
        let vector_quantization = if has_vector_changes {
            Self::vector_quantization_map(&table_meta)
        } else {
            HashMap::new()
        };
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;

            let mut relational_deletes_by_table = BTreeMap::<&str, Vec<(RowId, TxId)>>::new();
            for (table, row_id, deleted_tx) in &ws.relational_deletes {
                relational_deletes_by_table
                    .entry(table.as_str())
                    .or_default()
                    .push((*row_id, *deleted_tx));
            }
            for (table, deletes) in relational_deletes_by_table {
                let table_name = Self::rel_table_name(table);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut redb_table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                let mut encoded = Vec::new();
                for (row_id, deleted_tx) in deletes {
                    if let Some(row) = deleted_rows_snapshot
                        .as_ref()
                        .and_then(|by_table| by_table.get(table))
                        .and_then(|by_row| by_row.get(&row_id))
                    {
                        if row.deleted_tx.is_some() {
                            continue;
                        }
                        let key = Self::rel_row_key(row);
                        Self::encode_versioned_row_with_deleted_tx_into(
                            row,
                            Some(deleted_tx),
                            table_meta.get(table),
                            &mut encoded,
                        )?;
                        redb_table
                            .insert(key.as_slice(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        continue;
                    }
                    let (lower, upper) = Self::rel_row_key_range(row_id);
                    let latest_version = {
                        let mut range = redb_table
                            .range(lower.as_slice()..upper.as_slice())
                            .map_err(Self::storage_error)?;
                        range
                            .next_back()
                            .transpose()
                            .map_err(Self::storage_error)?
                            .map(|(key, value)| {
                                let row = Self::decode_versioned_row(
                                    value.value(),
                                    table_meta.get(table),
                                )?;
                                Ok::<_, Error>((key.value().to_vec(), row))
                            })
                            .transpose()?
                    };
                    let Some((key, mut row)) = latest_version else {
                        continue;
                    };
                    if row.row_id != row_id || row.deleted_tx.is_some() {
                        continue;
                    }
                    row.deleted_tx = Some(deleted_tx);
                    Self::encode_versioned_row_into(&row, table_meta.get(table), &mut encoded)?;
                    redb_table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            let mut relational_inserts_by_table = BTreeMap::<&str, Vec<&VersionedRow>>::new();
            for (table, row) in &ws.relational_inserts {
                relational_inserts_by_table
                    .entry(table.as_str())
                    .or_default()
                    .push(row);
            }
            for (table, rows) in relational_inserts_by_table {
                let table_name = Self::rel_table_name(table);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut redb_table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                let mut encoded = Vec::new();
                for row in rows {
                    Self::encode_versioned_row_into(row, table_meta.get(table), &mut encoded)?;
                    let key = Self::rel_row_key(row);
                    redb_table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            let (clear_source_lsns, set_source_lsns) = sync_source_lsn_updates(ws);
            if !clear_source_lsns.is_empty() || !set_source_lsns.is_empty() {
                let mut source_lsn_table = write_txn
                    .open_table(SYNC_ROW_SOURCE_LSN_TABLE)
                    .map_err(Self::storage_error)?;
                let mut source_kind_table = write_txn
                    .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
                    .map_err(Self::storage_error)?;
                for (table, row_id) in clear_source_lsns {
                    let key = Self::sync_row_source_lsn_key(&table, row_id);
                    source_lsn_table
                        .remove(key.as_slice())
                        .map_err(Self::storage_error)?;
                    source_kind_table
                        .remove(key.as_slice())
                        .map_err(Self::storage_error)?;
                }
                for (table, row_id, source_lsn, kind) in set_source_lsns {
                    let key = Self::sync_row_source_lsn_key(&table, row_id);
                    source_lsn_table
                        .insert(key.as_slice(), source_lsn.0)
                        .map_err(Self::storage_error)?;
                    source_kind_table
                        .insert(key.as_slice(), kind)
                        .map_err(Self::storage_error)?;
                }
            }

            if !ws.adj_deletes.is_empty() || !ws.adj_inserts.is_empty() {
                let mut fwd_table = write_txn
                    .open_table(GRAPH_FWD_TABLE)
                    .map_err(Self::storage_error)?;
                let mut rev_table = write_txn
                    .open_table(GRAPH_REV_TABLE)
                    .map_err(Self::storage_error)?;

                let mut encoded = Vec::new();
                for (source, edge_type, target, deleted_tx) in &ws.adj_deletes {
                    let mut live_versions = Vec::new();
                    for entry in fwd_table.iter().map_err(Self::storage_error)? {
                        let (key, value) = entry.map_err(Self::storage_error)?;
                        let edge: AdjEntry = Self::decode(value.value())?;
                        if edge.source == *source
                            && edge.target == *target
                            && edge.edge_type == *edge_type
                            && edge.deleted_tx.is_none()
                        {
                            live_versions.push((key.value().to_vec(), edge));
                        }
                    }
                    if live_versions.is_empty() {
                        return Err(Error::NotFound(format!(
                            "edge {source} -[{edge_type}]-> {target} in graph_fwd"
                        )));
                    }
                    for (fwd_key, mut edge) in live_versions {
                        edge.deleted_tx = Some(*deleted_tx);
                        Self::encode_into(&edge, &mut encoded)?;
                        let rev_key = Self::graph_rev_key(&edge);

                        fwd_table
                            .insert(fwd_key.as_slice(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        rev_table
                            .insert(rev_key.as_slice(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }

                for entry in &ws.adj_inserts {
                    Self::encode_into(entry, &mut encoded)?;
                    let fwd_key = Self::graph_fwd_key(entry);
                    let rev_key = Self::graph_rev_key(entry);
                    fwd_table
                        .insert(fwd_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    rev_table
                        .insert(rev_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            if has_vector_changes {
                let mut vectors_table = write_txn
                    .open_table(VECTORS_TABLE)
                    .map_err(Self::storage_error)?;

                for (index, row_id, deleted_tx) in &ws.vector_deletes {
                    let mut live_versions = Vec::new();
                    for entry in vectors_table.iter().map_err(Self::storage_error)? {
                        let (key, value) = entry.map_err(Self::storage_error)?;
                        let vector_entry = Self::decode_vector_entry(value.value())?;
                        if vector_entry.index == *index
                            && vector_entry.row_id == *row_id
                            && vector_entry.deleted_tx.is_none()
                        {
                            live_versions.push((key.value().to_vec(), vector_entry));
                        }
                    }
                    if live_versions.is_empty() {
                        return Err(Error::NotFound(format!("vector row {row_id}")));
                    }
                    for (key, mut entry) in live_versions {
                        entry.deleted_tx = Some(*deleted_tx);
                        let quantization = vector_quantization
                            .get(&entry.index)
                            .copied()
                            .unwrap_or_default();
                        let encoded = Self::encode_vector_entry(&entry, quantization)?;
                        vectors_table
                            .insert(key.as_slice(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }

                for entry in &ws.vector_inserts {
                    let quantization = vector_quantization
                        .get(&entry.index)
                        .copied()
                        .unwrap_or_default();
                    let encoded = Self::encode_vector_entry(entry, quantization)?;
                    let key = Self::vector_key(entry);
                    vectors_table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }

                for (index, old_row_id, new_row_id, tx) in &ws.vector_moves {
                    let mut live_versions = Vec::new();
                    for entry in vectors_table.iter().map_err(Self::storage_error)? {
                        let (key, value) = entry.map_err(Self::storage_error)?;
                        let vector_entry = Self::decode_vector_entry(value.value())?;
                        if vector_entry.index == *index
                            && vector_entry.row_id == *old_row_id
                            && vector_entry.deleted_tx.is_none()
                        {
                            live_versions.push((key.value().to_vec(), vector_entry));
                        }
                    }
                    if live_versions.is_empty() {
                        return Err(Error::NotFound(format!("vector row {old_row_id}")));
                    }
                    for (old_key, mut old_entry) in live_versions {
                        old_entry.deleted_tx = Some(*tx);
                        let quantization = vector_quantization
                            .get(&old_entry.index)
                            .copied()
                            .unwrap_or_default();
                        let old_encoded = Self::encode_vector_entry(&old_entry, quantization)?;
                        vectors_table
                            .insert(old_key.as_slice(), old_encoded.as_slice())
                            .map_err(Self::storage_error)?;

                        let mut new_entry = old_entry;
                        new_entry.row_id = *new_row_id;
                        new_entry.created_tx = *tx;
                        new_entry.deleted_tx = None;
                        new_entry.lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                        let new_key = Self::vector_key(&new_entry);
                        let new_encoded = Self::encode_vector_entry(&new_entry, quantization)?;
                        vectors_table
                            .insert(new_key.as_slice(), new_encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }
            }

            if let (Some(lsn), Some(tx)) = (ws.commit_lsn, Self::write_set_visibility_tx(ws)) {
                let mut table = write_txn
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                table.insert(lsn.0, tx.0).map_err(Self::storage_error)?;
            }

            if !change_log.is_empty() {
                let mut table = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                let mut encoded = Vec::new();
                let mut key = String::with_capacity(Self::change_log_entry_key_len());
                for (index, entry) in change_log.iter().enumerate() {
                    Self::write_change_log_entry_key(lsn, index, &mut key);
                    Self::encode_into(entry, &mut encoded)?;
                    table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            if !sink_events.is_empty() {
                let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                let mut overflow_audits = Vec::new();
                let mut by_sink: BTreeMap<&str, Vec<(usize, &SinkQueueEntry)>> = BTreeMap::new();
                for (index, (sink, entry)) in sink_events.iter().enumerate() {
                    by_sink
                        .entry(sink.as_str())
                        .or_default()
                        .push((index, entry));
                }
                for (sink, entries) in by_sink {
                    let table_name = Self::sink_queue_table_name(sink);
                    let table_def: TableDefinition<u64, &[u8]> =
                        TableDefinition::new(table_name.as_str());
                    let mut table = write_txn
                        .open_table(table_def)
                        .map_err(Self::storage_error)?;
                    let mut existing = table.len().map_err(Self::storage_error)? as usize;
                    for (index, entry) in entries {
                        let overflow_id = if existing >= max_sink_queue_depth {
                            table
                                .first()
                                .map_err(Self::storage_error)?
                                .map(|(key, _)| key.value())
                        } else {
                            None
                        };
                        if let Some(id) = overflow_id {
                            table.remove(id).map_err(Self::storage_error)?;
                            existing = existing.saturating_sub(1);
                            overflow_audits.push((index, sink.to_string()));
                        }
                        let encoded = Self::encode(entry)?;
                        table
                            .insert(entry.id, encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        existing = existing.saturating_add(1);
                    }
                }
                if !overflow_audits.is_empty() {
                    let mut audit_table = write_txn
                        .open_table(SINK_AUDIT_TABLE)
                        .map_err(Self::storage_error)?;
                    for (index, sink) in overflow_audits {
                        let key = Self::sink_audit_key(lsn, index, &sink);
                        let audit = SinkAuditEntry {
                            lsn,
                            kind: SinkAuditKind::QueueOverflow {
                                sink,
                                dropped_count: 1,
                            },
                        };
                        let encoded = Self::encode(&audit)?;
                        audit_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }
            }

            if !trigger_audits.is_empty() {
                {
                    let mut audit_table = write_txn
                        .open_table(TRIGGER_AUDIT_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut stamps = write_txn
                        .open_table(TRIGGER_AUDIT_STAMPS_TABLE)
                        .map_err(Self::storage_error)?;
                    let stamped_at = Wallclock::now().0;
                    for (index, entry) in trigger_audits {
                        let key = Self::trigger_audit_key(*index, &entry.trigger_name);
                        let encoded = Self::encode(entry)?;
                        audit_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        stamps
                            .insert(key.as_str(), stamped_at)
                            .map_err(Self::storage_error)?;
                    }
                }
                {
                    let mut config_table = write_txn
                        .open_table(CONFIG_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut ring: Vec<TriggerAuditEntry> = config_table
                        .get(TRIGGER_AUDIT_RING_CONFIG_KEY)
                        .map_err(Self::storage_error)?
                        .map(|value| Self::decode(value.value()))
                        .transpose()?
                        .unwrap_or_default();
                    ring.extend(trigger_audits.iter().map(|(_, entry)| entry.clone()));
                    let overflow = ring
                        .len()
                        .saturating_sub(crate::database::trigger::TRIGGER_AUDIT_RING_CAPACITY);
                    if overflow > 0 {
                        ring.drain(0..overflow);
                    }
                    let current_next_index = config_table
                        .get(TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY)
                        .map_err(Self::storage_error)?
                        .map(|value| Self::decode::<u64>(value.value()))
                        .transpose()?
                        .unwrap_or(0);
                    let next_index = trigger_audits
                        .iter()
                        .map(|(index, _)| index.saturating_add(1))
                        .max()
                        .unwrap_or(0)
                        .max(current_next_index);
                    let encoded_ring = Self::encode(&ring)?;
                    config_table
                        .insert(TRIGGER_AUDIT_RING_CONFIG_KEY, encoded_ring.as_slice())
                        .map_err(Self::storage_error)?;
                    let encoded_next_index = Self::encode(&next_index)?;
                    config_table
                        .insert(
                            TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY,
                            encoded_next_index.as_slice(),
                        )
                        .map_err(Self::storage_error)?;
                }
            }

            if schema_ddl.event_bus.is_some() || schema_ddl.trigger.is_some() {
                {
                    let mut config_table = write_txn
                        .open_table(CONFIG_TABLE)
                        .map_err(Self::storage_error)?;
                    if let Some(event_bus_ddl) = schema_ddl.event_bus {
                        for (key, encoded) in &event_bus_ddl.config_values {
                            config_table
                                .insert(key.as_str(), encoded.as_slice())
                                .map_err(Self::storage_error)?;
                        }
                    }
                    if let Some(trigger_ddl) = schema_ddl.trigger {
                        for (key, encoded) in &trigger_ddl.config_values {
                            config_table
                                .insert(key.as_str(), encoded.as_slice())
                                .map_err(Self::storage_error)?;
                        }
                    }
                }
                let ddl_entries = schema_ddl
                    .event_bus
                    .into_iter()
                    .flat_map(|commit| commit.ddl.iter())
                    .chain(
                        schema_ddl
                            .trigger
                            .into_iter()
                            .flat_map(|commit| commit.ddl.iter()),
                    )
                    .collect::<Vec<_>>();
                if !ddl_entries.is_empty() {
                    let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                    let mut ddl_table = write_txn
                        .open_table(DDL_LOG_TABLE)
                        .map_err(Self::storage_error)?;
                    for (index, change) in ddl_entries.iter().enumerate() {
                        let key = Self::ddl_log_key_for_index(lsn, index, ddl_entries.len());
                        let encoded = Self::encode(change)?;
                        ddl_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }
            }

            if !ws.config_writes.is_empty() {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in &ws.config_writes {
                    let encoded = if ws.config_max_u64_keys.iter().any(|max_key| max_key == key) {
                        let incoming = Self::decode::<u64>(encoded)?;
                        let current = config_table
                            .get(key.as_str())
                            .map_err(Self::storage_error)?
                            .map(|value| Self::decode::<u64>(value.value()))
                            .transpose()?
                            .unwrap_or(0);
                        Self::encode(&current.max(incoming))?
                    } else {
                        encoded.clone()
                    };
                    config_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    /// Commit a complete received-schema projection in the transaction that
    /// also records its finalized WriteSet identity.  The normal incremental
    /// writer cannot be used here: a DROP or same-name recreation needs the
    /// durable image to become the prepared full replacement atomically.
    fn flush_received_schema_stage(
        &self,
        ws: &WriteSet,
        change_log: &[ChangeLogEntry],
        stage: &ReceivedSchemaPersistenceProjection,
        sink_events: &[PreparedSinkEvent],
        trigger_audits: &[(u64, TriggerAuditEntry)],
        max_sink_queue_depth: usize,
    ) -> Result<()> {
        let prior_tables = self.load_all_table_meta()?;
        let vector_quantization = Self::vector_quantization_map(&stage.table_meta);
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;

            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                for table in prior_tables.keys() {
                    let key = Self::meta_key(table);
                    meta_table
                        .remove(key.as_str())
                        .map_err(Self::storage_error)?;
                }
                for (table, meta) in &stage.table_meta {
                    let key = Self::meta_key(table);
                    let encoded = Self::encode(meta)?;
                    meta_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            let mut relational_tables = prior_tables
                .keys()
                .chain(stage.table_meta.keys())
                .collect::<HashSet<_>>()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            relational_tables.sort();
            relational_tables.dedup();
            for table in relational_tables {
                let table_name = Self::rel_table_name(&table);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                match write_txn.delete_table(table_def) {
                    Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(redb::TableError::TableTypeMismatch { .. }) => {}
                    Err(error) => return Err(Self::storage_error(error)),
                }
                let legacy_table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                match write_txn.delete_table(legacy_table_def) {
                    Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(redb::TableError::TableTypeMismatch { .. }) => {}
                    Err(error) => return Err(Self::storage_error(error)),
                }
            }
            for (table, rows) in &stage.rows {
                let table_name = Self::rel_table_name(table);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut redb_table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                for row in rows {
                    let encoded = Self::encode_versioned_row(row, stage.table_meta.get(table))?;
                    let key = Self::rel_row_key(row);
                    redb_table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            let _ = write_txn.delete_table(SYNC_ROW_SOURCE_LSN_TABLE);
            let _ = write_txn.delete_table(SYNC_ROW_SOURCE_KIND_TABLE);
            {
                let mut lsn_table = write_txn
                    .open_table(SYNC_ROW_SOURCE_LSN_TABLE)
                    .map_err(Self::storage_error)?;
                let mut kind_table = write_txn
                    .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
                    .map_err(Self::storage_error)?;
                for (table, row_id, lsn, kind) in &stage.sync_sources {
                    let key = Self::sync_row_source_lsn_key(table, *row_id);
                    lsn_table
                        .insert(key.as_slice(), lsn.0)
                        .map_err(Self::storage_error)?;
                    kind_table
                        .insert(key.as_slice(), sync_source_kind_to_u8(*kind))
                        .map_err(Self::storage_error)?;
                }
            }

            let _ = write_txn.delete_table(GRAPH_FWD_TABLE);
            let _ = write_txn.delete_table(GRAPH_REV_TABLE);
            {
                let mut fwd = write_txn
                    .open_table(GRAPH_FWD_TABLE)
                    .map_err(Self::storage_error)?;
                let mut rev = write_txn
                    .open_table(GRAPH_REV_TABLE)
                    .map_err(Self::storage_error)?;
                for edge in &stage.edges {
                    let encoded = Self::encode(edge)?;
                    let fwd_key = Self::graph_fwd_key(edge);
                    let rev_key = Self::graph_rev_key(edge);
                    fwd.insert(fwd_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    rev.insert(rev_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            let _ = write_txn.delete_table(VECTORS_TABLE);
            {
                let mut vectors = write_txn
                    .open_table(VECTORS_TABLE)
                    .map_err(Self::storage_error)?;
                for entry in &stage.vectors {
                    let quantization = vector_quantization
                        .get(&entry.index)
                        .copied()
                        .unwrap_or_default();
                    let encoded = Self::encode_vector_entry(entry, quantization)?;
                    let key = Self::vector_key(entry);
                    vectors
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            let _ = write_txn.delete_table(DDL_LOG_TABLE);
            {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let mut grouped = BTreeMap::<Lsn, Vec<&DdlChange>>::new();
                for (lsn, ddl) in &stage.ddl_log {
                    grouped.entry(*lsn).or_default().push(ddl);
                }
                for (lsn, ddl) in grouped {
                    for (index, change) in ddl.iter().enumerate() {
                        let key = Self::ddl_log_key_for_index(lsn, index, ddl.len());
                        let encoded = Self::encode(*change)?;
                        ddl_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }
            }

            if let (Some(lsn), Some(tx)) = (ws.commit_lsn, Self::write_set_visibility_tx(ws)) {
                let mut index = write_txn
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                index.insert(lsn.0, tx.0).map_err(Self::storage_error)?;
            }
            if !stage.structurally_dropped_tables.is_empty() {
                // Decode first and point-remove second: Redb does not permit
                // mutating the table while its iterator holds value borrows.
                // Both phases remain inside this received-schema write
                // transaction, so a later failure cannot leave a new schema
                // image paired with old-generation change history.
                let retired_keys = {
                    let log = write_txn
                        .open_table(CHANGE_LOG_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut retired_keys = Vec::new();
                    for item in log.iter().map_err(Self::storage_error)? {
                        let (key, value) = item.map_err(Self::storage_error)?;
                        let entry = Self::decode::<ChangeLogEntry>(value.value())?;
                        if Self::change_log_entry_references_any_table(
                            &entry,
                            &stage.structurally_dropped_tables,
                        ) {
                            retired_keys.push(key.value().to_string());
                        }
                    }
                    retired_keys
                };
                if !retired_keys.is_empty() {
                    let mut log = write_txn
                        .open_table(CHANGE_LOG_TABLE)
                        .map_err(Self::storage_error)?;
                    for key in retired_keys {
                        log.remove(key.as_str()).map_err(Self::storage_error)?;
                    }
                }
            }
            if !change_log.is_empty() {
                let mut log = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                let mut key = String::with_capacity(Self::change_log_entry_key_len());
                let mut encoded = Vec::new();
                for (index, entry) in change_log.iter().enumerate() {
                    Self::write_change_log_entry_key(lsn, index, &mut key);
                    Self::encode_into(entry, &mut encoded)?;
                    log.insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            {
                let mut config = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, value) in &stage.config_values {
                    let value = if stage
                        .config_max_u64_keys
                        .iter()
                        .any(|max_key| max_key == key)
                    {
                        let incoming = Self::decode::<u64>(value)?;
                        let current = config
                            .get(key.as_str())
                            .map_err(Self::storage_error)?
                            .map(|value| Self::decode::<u64>(value.value()))
                            .transpose()?
                            .unwrap_or(0);
                        Self::encode(&current.max(incoming))?
                    } else {
                        value.clone()
                    };
                    config
                        .insert(key.as_str(), value.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            // The complete received-schema core image now exists only inside
            // this live Redb transaction. Sink queues and trigger audits for
            // received rows belong immediately below this checkpoint and must
            // commit with that image, never in a later transaction.
            #[cfg(test)]
            if take_received_schema_side_effect_persistence_fault_for_test() {
                return Err(Error::Other(
                    "injected received-schema side-effect persistence failure".to_string(),
                ));
            }
            if !sink_events.is_empty() {
                let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                let mut overflow_audits = Vec::new();
                let mut by_sink: BTreeMap<&str, Vec<(usize, &SinkQueueEntry)>> = BTreeMap::new();
                for (index, (sink, entry)) in sink_events.iter().enumerate() {
                    by_sink
                        .entry(sink.as_str())
                        .or_default()
                        .push((index, entry));
                }
                for (sink, entries) in by_sink {
                    let table_name = Self::sink_queue_table_name(sink);
                    let table_def: TableDefinition<u64, &[u8]> =
                        TableDefinition::new(table_name.as_str());
                    let mut table = write_txn
                        .open_table(table_def)
                        .map_err(Self::storage_error)?;
                    let mut existing = table.len().map_err(Self::storage_error)? as usize;
                    for (index, entry) in entries {
                        let overflow_id = if existing >= max_sink_queue_depth {
                            table
                                .first()
                                .map_err(Self::storage_error)?
                                .map(|(key, _)| key.value())
                        } else {
                            None
                        };
                        if let Some(id) = overflow_id {
                            table.remove(id).map_err(Self::storage_error)?;
                            existing = existing.saturating_sub(1);
                            overflow_audits.push((index, sink.to_string()));
                        }
                        let encoded = Self::encode(entry)?;
                        table
                            .insert(entry.id, encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        existing = existing.saturating_add(1);
                    }
                }
                if !overflow_audits.is_empty() {
                    let mut audit_table = write_txn
                        .open_table(SINK_AUDIT_TABLE)
                        .map_err(Self::storage_error)?;
                    for (index, sink) in overflow_audits {
                        let key = Self::sink_audit_key(lsn, index, &sink);
                        let audit = SinkAuditEntry {
                            lsn,
                            kind: SinkAuditKind::QueueOverflow {
                                sink,
                                dropped_count: 1,
                            },
                        };
                        let encoded = Self::encode(&audit)?;
                        audit_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }
            }
            if !trigger_audits.is_empty() {
                {
                    let mut audit_table = write_txn
                        .open_table(TRIGGER_AUDIT_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut stamps = write_txn
                        .open_table(TRIGGER_AUDIT_STAMPS_TABLE)
                        .map_err(Self::storage_error)?;
                    let stamped_at = Wallclock::now().0;
                    for (index, entry) in trigger_audits {
                        let key = Self::trigger_audit_key(*index, &entry.trigger_name);
                        let encoded = Self::encode(entry)?;
                        audit_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        stamps
                            .insert(key.as_str(), stamped_at)
                            .map_err(Self::storage_error)?;
                    }
                }
                {
                    let mut config_table = write_txn
                        .open_table(CONFIG_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut ring: Vec<TriggerAuditEntry> = config_table
                        .get(TRIGGER_AUDIT_RING_CONFIG_KEY)
                        .map_err(Self::storage_error)?
                        .map(|value| Self::decode(value.value()))
                        .transpose()?
                        .unwrap_or_default();
                    ring.extend(trigger_audits.iter().map(|(_, entry)| entry.clone()));
                    let overflow = ring
                        .len()
                        .saturating_sub(crate::database::trigger::TRIGGER_AUDIT_RING_CAPACITY);
                    if overflow > 0 {
                        ring.drain(0..overflow);
                    }
                    let current_next_index = config_table
                        .get(TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY)
                        .map_err(Self::storage_error)?
                        .map(|value| Self::decode::<u64>(value.value()))
                        .transpose()?
                        .unwrap_or(0);
                    let next_index = trigger_audits
                        .iter()
                        .map(|(index, _)| index.saturating_add(1))
                        .max()
                        .unwrap_or(0)
                        .max(current_next_index);
                    let encoded_ring = Self::encode(&ring)?;
                    config_table
                        .insert(TRIGGER_AUDIT_RING_CONFIG_KEY, encoded_ring.as_slice())
                        .map_err(Self::storage_error)?;
                    let encoded_next_index = Self::encode(&next_index)?;
                    config_table
                        .insert(
                            TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY,
                            encoded_next_index.as_slice(),
                        )
                        .map_err(Self::storage_error)?;
                }
            }
            if take_received_schema_pre_commit_fault_for_test() {
                return Err(Error::Other(
                    "injected received-schema Redb pre-commit failure".to_string(),
                ));
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    /// Persist a locally authored schema stage without replacing the received
    /// schema image. The caller has already prepared the exact affected table
    /// metadata and encoded sidecars; this transaction deliberately leaves
    /// all unrelated relation rows and durable state untouched.
    pub(crate) fn flush_local_schema_stage(
        &self,
        ws: &WriteSet,
        stage: &LocalSchemaPersistenceProjection,
    ) -> Result<()> {
        let lsn = ws.commit_lsn.ok_or_else(|| {
            Error::Other("local schema stage is missing its commit LSN".to_string())
        })?;
        if stage.ddl.is_empty() {
            return Err(Error::Other(
                "local schema stage contains no DDL".to_string(),
            ));
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;

            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                for (table, meta) in &stage.affected_table_meta {
                    let key = Self::meta_key(table);
                    let encoded = Self::encode(meta)?;
                    meta_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            // `open_table` is both the Redb table creation path and a no-op
            // for an existing relation. Do not delete, scan, or rewrite rows:
            // another committed local table must remain byte-for-byte intact.
            for table in stage.affected_table_meta.keys() {
                let table_name = Self::rel_table_name(table);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let _relation = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
            }

            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in &stage.config_values {
                    config_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                for (index, change) in stage.ddl.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(lsn, index, stage.ddl.len());
                    let encoded = Self::encode(change)?;
                    ddl_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            {
                let mut index = write_txn
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                index
                    .insert(lsn.0, stage.commit_index_tx.0)
                    .map_err(Self::storage_error)?;
            }

            if take_received_schema_pre_commit_fault_for_test() {
                return Err(Error::Other(
                    "injected received-schema Redb pre-commit failure".to_string(),
                ));
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    fn change_log_entry_references_any_table(
        entry: &ChangeLogEntry,
        tables: &HashSet<String>,
    ) -> bool {
        match entry {
            ChangeLogEntry::RowInsert { table, .. } | ChangeLogEntry::RowDelete { table, .. } => {
                tables.contains(table)
            }
            ChangeLogEntry::VectorInsert { index, .. }
            | ChangeLogEntry::VectorDelete { index, .. } => tables.contains(&index.table),
            ChangeLogEntry::EdgeInsert { .. } | ChangeLogEntry::EdgeDelete { .. } => false,
        }
    }
    pub fn flush_table_meta(&self, name: &str, meta: &TableMeta) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::meta_key(name);
                let encoded = Self::encode(meta)?;
                meta_table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    /// Durable CREATE TABLE foundation: the table declaration, its one
    /// authoritative schema generation, immutable DDL provenance, and the
    /// matching DDL log entry must become visible together. This prevents
    /// CREATE from retaining the former meta-then-log crash window.
    pub fn flush_table_meta_with_config_values_and_append_ddl_log(
        &self,
        name: &str,
        meta: &TableMeta,
        config_values: Vec<(&str, Vec<u8>)>,
        lsn: Lsn,
        ddl: &DdlChange,
    ) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::meta_key(name);
                let encoded = Self::encode(meta)?;
                meta_table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in config_values {
                    config_table
                        .insert(key, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::ddl_log_key(lsn);
                let encoded = Self::encode(ddl)?;
                ddl_table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn remove_table_meta(&self, name: &str) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::meta_key(name);
                meta_table
                    .remove(key.as_str())
                    .map_err(Self::storage_error)?;
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn flush_config_value<T: serde::Serialize>(&self, key: &str, value: &T) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                let encoded = Self::encode(value)?;
                config_table
                    .insert(key, encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub(crate) fn encode_config_value<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
        Self::encode(value)
    }

    pub(crate) fn decode_config_value<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        Self::decode(bytes)
    }

    /// Strict config decode for callers whose durable bytes are themselves an
    /// authority boundary. Unlike the compatibility decoder above, a valid
    /// prefix followed by stray bytes is corrupt rather than silently accepted.
    pub(crate) fn decode_config_value_exact<T: serde::de::DeserializeOwned>(
        bytes: &[u8],
    ) -> Result<T> {
        Self::decode_exact(bytes)
    }

    pub fn flush_encoded_config_values_and_append_ddl_log(
        &self,
        config_values: Vec<(&str, Vec<u8>)>,
        lsn: Lsn,
        ddl: &[DdlChange],
    ) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in config_values {
                    config_table
                        .insert(key, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                for (index, change) in ddl.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(lsn, index, ddl.len());
                    let encoded = Self::encode(change)?;
                    ddl_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn flush_encoded_config_values(&self, config_values: Vec<(&str, Vec<u8>)>) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in config_values {
                    config_table
                        .insert(key, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn remove_config_value(&self, key: &str) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                config_table.remove(key).map_err(Self::storage_error)?;
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn load_sink_queue<T: serde::de::DeserializeOwned>(&self, sink: &str) -> Result<Vec<T>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table_name = Self::sink_queue_table_name(sink);
            let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new(table_name.as_str());
            let table = match read_txn.open_table(table_def) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (_, value) = entry.map_err(Self::storage_error)?;
                entries.push(Self::decode(value.value())?);
            }
            Ok(entries)
        })
    }

    pub fn update_sink_queue_entry<T: serde::Serialize>(
        &self,
        sink: &str,
        remove_id: Option<u64>,
        put: Option<(u64, &T)>,
    ) -> Result<()> {
        if remove_id.is_none() && put.is_none() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let table_name = Self::sink_queue_table_name(sink);
                let table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                if let Some(id) = remove_id {
                    table.remove(id).map_err(Self::storage_error)?;
                }
                if let Some((id, entry)) = put {
                    let encoded = Self::encode(entry)?;
                    table
                        .insert(id, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn remove_sink_queue_entries(&self, sink: &str, remove_ids: &[u64]) -> Result<()> {
        if remove_ids.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let table_name = Self::sink_queue_table_name(sink);
                let table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                for id in remove_ids {
                    table.remove(*id).map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn append_change_log(&self, lsn: Lsn, entries: &[ChangeLogEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut table = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let mut encoded = Vec::new();
                let mut key = String::with_capacity(Self::change_log_entry_key_len());
                for (index, entry) in entries.iter().enumerate() {
                    Self::write_change_log_entry_key(lsn, index, &mut key);
                    Self::encode_into(entry, &mut encoded)?;
                    table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn append_ddl_log(&self, lsn: Lsn, change: &DdlChange) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::ddl_log_key(lsn);
                let encoded = Self::encode(change)?;
                table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn append_trigger_audit(&self, index: u64, entry: &TriggerAuditEntry) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut table = write_txn
                    .open_table(TRIGGER_AUDIT_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::trigger_audit_key(index, &entry.trigger_name);
                let encoded = Self::encode(entry)?;
                table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
                let mut stamps = write_txn
                    .open_table(TRIGGER_AUDIT_STAMPS_TABLE)
                    .map_err(Self::storage_error)?;
                stamps
                    .insert(key.as_str(), Wallclock::now().0)
                    .map_err(Self::storage_error)?;
            }
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                let mut ring: Vec<TriggerAuditEntry> = config_table
                    .get(TRIGGER_AUDIT_RING_CONFIG_KEY)
                    .map_err(Self::storage_error)?
                    .map(|value| Self::decode(value.value()))
                    .transpose()?
                    .unwrap_or_default();
                ring.push(entry.clone());
                let overflow = ring
                    .len()
                    .saturating_sub(crate::database::trigger::TRIGGER_AUDIT_RING_CAPACITY);
                if overflow > 0 {
                    ring.drain(0..overflow);
                }
                let current_next_index = config_table
                    .get(TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY)
                    .map_err(Self::storage_error)?
                    .map(|value| Self::decode::<u64>(value.value()))
                    .transpose()?
                    .unwrap_or(0);
                let next_index = index.saturating_add(1).max(current_next_index);
                let encoded_ring = Self::encode(&ring)?;
                config_table
                    .insert(TRIGGER_AUDIT_RING_CONFIG_KEY, encoded_ring.as_slice())
                    .map_err(Self::storage_error)?;
                let encoded_next_index = Self::encode(&next_index)?;
                config_table
                    .insert(
                        TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY,
                        encoded_next_index.as_slice(),
                    )
                    .map_err(Self::storage_error)?;
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn remove_table_data(&self, name: &str) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            let table_name = Self::rel_table_name(name);
            let table_def: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(table_name.as_str());
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
            Self::remove_sync_source_provenance_for_table(&write_txn, name)?;
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn remove_table_with_config_values_and_ddl_log(
        &self,
        name: &str,
        config_values: Vec<(&str, Vec<u8>)>,
        lsn: Lsn,
        ddl: &[DdlChange],
    ) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
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
            Self::remove_sync_source_provenance_for_table(&write_txn, name)?;
            if !config_values.is_empty() {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in config_values {
                    config_table
                        .insert(key, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            if !ddl.is_empty() {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                for (index, change) in ddl.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(lsn, index, ddl.len());
                    let encoded = Self::encode(change)?;
                    ddl_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn remove_table_rewrite_aux_with_config_values_and_ddl_log(
        &self,
        name: &str,
        config_values: Vec<(&str, Vec<u8>)>,
        lsn: Lsn,
        ddl: &[DdlChange],
        graph_edges: &[AdjEntry],
        vectors: &[VectorEntry],
    ) -> Result<()> {
        let mut table_meta = self.load_all_table_meta()?;
        table_meta.remove(name);
        let vector_quantization = Self::vector_quantization_map(&table_meta);
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
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
            Self::remove_sync_source_provenance_for_table(&write_txn, name)?;

            let _ = write_txn.delete_table(GRAPH_FWD_TABLE);
            let _ = write_txn.delete_table(GRAPH_REV_TABLE);
            {
                let mut fwd_table = write_txn
                    .open_table(GRAPH_FWD_TABLE)
                    .map_err(Self::storage_error)?;
                let mut rev_table = write_txn
                    .open_table(GRAPH_REV_TABLE)
                    .map_err(Self::storage_error)?;
                for entry in graph_edges {
                    let encoded = Self::encode(entry)?;
                    let fwd_key = Self::graph_fwd_key(entry);
                    let rev_key = Self::graph_rev_key(entry);
                    fwd_table
                        .insert(fwd_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    rev_table
                        .insert(rev_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            let _ = write_txn.delete_table(VECTORS_TABLE);
            {
                let mut table = write_txn
                    .open_table(VECTORS_TABLE)
                    .map_err(Self::storage_error)?;
                for entry in vectors {
                    let quantization = vector_quantization
                        .get(&entry.index)
                        .copied()
                        .unwrap_or_default();
                    let encoded = Self::encode_vector_entry(entry, quantization)?;
                    let key = Self::vector_key(entry);
                    table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            if !config_values.is_empty() {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in config_values {
                    config_table
                        .insert(key, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            if !ddl.is_empty() {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                for (index, change) in ddl.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(lsn, index, ddl.len());
                    let encoded = Self::encode(change)?;
                    ddl_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn rewrite_table_rows(&self, name: &str, rows: &[VersionedRow]) -> Result<()> {
        let table_meta = self.load_all_table_meta()?;
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let table_name = Self::rel_table_name(name);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let _ = write_txn.delete_table(table_def);
                let legacy_table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let _ = write_txn.delete_table(legacy_table_def);
                let mut redb_table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                for row in rows {
                    let encoded = Self::encode_versioned_row(row, table_meta.get(name))?;
                    let key = Self::rel_row_key(row);
                    redb_table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            Self::retain_sync_source_provenance_for_table_rows(&write_txn, name, rows)?;
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn rewrite_vectors(&self, vectors: &[VectorEntry]) -> Result<()> {
        let table_meta = self.load_all_table_meta()?;
        let vector_quantization = Self::vector_quantization_map(&table_meta);
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            let _ = write_txn.delete_table(VECTORS_TABLE);
            {
                let mut table = write_txn
                    .open_table(VECTORS_TABLE)
                    .map_err(Self::storage_error)?;
                for entry in vectors {
                    let quantization = vector_quantization
                        .get(&entry.index)
                        .copied()
                        .unwrap_or_default();
                    let encoded = Self::encode_vector_entry(entry, quantization)?;
                    let key = Self::vector_key(entry);
                    table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one schema rewrite transaction must replace metadata, rows, vectors, config, and its DDL log together"
    )]
    pub fn rewrite_table_meta_rows_vectors_and_append_ddl_log(
        &self,
        name: &str,
        meta: &TableMeta,
        rows: &[VersionedRow],
        vectors: &[VectorEntry],
        lsn: Lsn,
        ddl: &[DdlChange],
        config_values: Vec<(&str, Vec<u8>)>,
    ) -> Result<()> {
        let mut table_meta = self.load_all_table_meta()?;
        table_meta.insert(name.to_string(), meta.clone());
        let vector_quantization = Self::vector_quantization_map(&table_meta);
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::meta_key(name);
                let encoded = Self::encode(meta)?;
                meta_table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            {
                let table_name = Self::rel_table_name(name);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let _ = write_txn.delete_table(table_def);
                let legacy_table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let _ = write_txn.delete_table(legacy_table_def);
                let mut redb_table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                for row in rows {
                    let encoded = Self::encode_versioned_row(row, Some(meta))?;
                    let key = Self::rel_row_key(row);
                    redb_table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            Self::retain_sync_source_provenance_for_table_rows(&write_txn, name, rows)?;
            let _ = write_txn.delete_table(VECTORS_TABLE);
            {
                let mut table = write_txn
                    .open_table(VECTORS_TABLE)
                    .map_err(Self::storage_error)?;
                for entry in vectors {
                    let quantization = vector_quantization
                        .get(&entry.index)
                        .copied()
                        .unwrap_or_default();
                    let encoded = Self::encode_vector_entry(entry, quantization)?;
                    let key = Self::vector_key(entry);
                    table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in config_values {
                    config_table
                        .insert(key, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                for (index, change) in ddl.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(lsn, index, ddl.len());
                    let encoded = Self::encode(change)?;
                    ddl_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn rewrite_graph_edges(&self, edges: &[AdjEntry]) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            let _ = write_txn.delete_table(GRAPH_FWD_TABLE);
            let _ = write_txn.delete_table(GRAPH_REV_TABLE);
            {
                let mut fwd_table = write_txn
                    .open_table(GRAPH_FWD_TABLE)
                    .map_err(Self::storage_error)?;
                let mut rev_table = write_txn
                    .open_table(GRAPH_REV_TABLE)
                    .map_err(Self::storage_error)?;

                for entry in edges {
                    let encoded = Self::encode(entry)?;
                    let fwd_key = Self::graph_fwd_key(entry);
                    let rev_key = Self::graph_rev_key(entry);
                    fwd_table
                        .insert(fwd_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    rev_table
                        .insert(rev_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn load_all_table_meta(&self) -> Result<HashMap<String, TableMeta>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let meta_table = match read_txn.open_table(META_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut tables = HashMap::new();
            for entry in meta_table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                if let Some(name) = key.strip_prefix("table:") {
                    let (meta, via_legacy) = Self::decode_table_meta_versioned(value.value())?;
                    if via_legacy {
                        self.used_legacy_table_meta_layout
                            .store(true, Ordering::Relaxed);
                    }
                    tables.insert(name.to_string(), meta);
                }
            }
            Ok(tables)
        })
    }

    /// Every config value whose key starts with `prefix`, decoded, in key order.
    /// The config keys are sorted, so a range from `prefix` and a stop at the
    /// first key that no longer shares it collects exactly the contiguous run.
    pub fn load_config_values_with_prefix<T: serde::de::DeserializeOwned>(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, T)>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let config_table = match read_txn.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut out = Vec::new();
            for entry in config_table.range(prefix..).map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                if !key.starts_with(prefix) {
                    break;
                }
                out.push((key.to_string(), Self::decode(value.value())?));
            }
            Ok(out)
        })
    }

    /// Every raw config value whose key starts with `prefix`, in key order.
    /// Callers that treat the stored bytes as an authority boundary can apply
    /// their own exact decoder without broadening generic config compatibility.
    pub(crate) fn load_config_values_raw_with_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let config_table = match read_txn.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut out = Vec::new();
            for entry in config_table.range(prefix..).map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                if !key.starts_with(prefix) {
                    break;
                }
                out.push((key.to_string(), value.value().to_vec()));
            }
            Ok(out)
        })
    }

    pub fn load_config_value<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let config_table = match read_txn.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let value = match config_table.get(key).map_err(Self::storage_error)? {
                Some(value) => Some(Self::decode(value.value())?),
                None => None,
            };
            Ok(value)
        })
    }

    pub fn load_relational_table(&self, name: &str) -> Result<Vec<VersionedRow>> {
        let (rows, migrate_legacy_table) = self.with_db(|db| {
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
                    return Ok((rows, false));
                }
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok((Vec::new(), false)),
                Err(redb::TableError::TableTypeMismatch { .. }) => {}
                Err(err) => return Err(Self::storage_error(err)),
            };

            let legacy_table_def: TableDefinition<u64, &[u8]> =
                TableDefinition::new(table_name.as_str());
            let table = match read_txn.open_table(legacy_table_def) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok((Vec::new(), false)),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut rows = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (_, value) = entry.map_err(Self::storage_error)?;
                rows.push(Self::decode_versioned_row(
                    value.value(),
                    table_meta.as_ref(),
                )?);
            }
            Ok((rows, true))
        })?;
        if migrate_legacy_table {
            self.rewrite_table_rows(name, &rows)?;
        }
        Ok(rows)
    }

    pub fn load_all_tables(&self) -> Result<HashMap<String, Vec<VersionedRow>>> {
        let mut all_tables = HashMap::new();
        for name in self.load_all_table_meta()?.into_keys() {
            let rows = self.load_relational_table(&name)?;
            all_tables.insert(name, rows);
        }
        Ok(all_tables)
    }

    pub fn load_forward_edges(&self) -> Result<Vec<AdjEntry>> {
        self.load_graph_table(GRAPH_FWD_TABLE)
    }

    pub fn load_reverse_edges(&self) -> Result<Vec<AdjEntry>> {
        self.load_graph_table(GRAPH_REV_TABLE)
    }

    pub fn load_vectors(&self) -> Result<Vec<VectorEntry>> {
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

    pub fn load_sync_source_lsns(&self) -> Result<HashMap<(String, RowId), Lsn>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(SYNC_ROW_SOURCE_LSN_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut source_lsns = HashMap::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                if let Some((table, row_id)) = Self::decode_sync_row_source_lsn_key(key.value()) {
                    source_lsns.insert((table, row_id), Lsn(value.value()));
                }
            }
            Ok(source_lsns)
        })
    }

    pub fn load_sync_source_kinds(&self) -> Result<HashMap<(String, RowId), u8>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(SYNC_ROW_SOURCE_KIND_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut kinds = HashMap::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                if let Some((table, row_id)) = Self::decode_sync_row_source_lsn_key(key.value()) {
                    kinds.insert((table, row_id), value.value());
                }
            }
            Ok(kinds)
        })
    }

    pub fn load_change_log(&self) -> Result<Vec<ChangeLogEntry>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(CHANGE_LOG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (_, value) = entry.map_err(Self::storage_error)?;
                entries.push(Self::decode(value.value())?);
            }
            Ok(entries)
        })
    }

    /// Rewrite the persisted change log to exactly `entries`, in order. Used by
    /// currency-table version compaction to drop, in lockstep with the pruned
    /// superseded row versions, the `RowInsert`/`RowDelete` entries that
    /// referenced them — so `changes_since`'s change-log replay (which reads the
    /// row version AT each logged LSN) never encounters an entry whose version
    /// was removed and silently substitutes the latest one. The table is fully
    /// cleared and rebuilt from `entries`; keys are re-derived as
    /// `{lsn:020}:{index}` with `index` restarting per LSN (entries sharing an
    /// LSN are contiguous, exactly as the commit path wrote them), so the load
    /// order is preserved byte-for-byte with what a fresh sequence of commits
    /// would have produced.
    pub fn rewrite_change_log(&self, entries: &[ChangeLogEntry]) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            match write_txn.delete_table(CHANGE_LOG_TABLE) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            {
                let mut table = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let mut key = String::with_capacity(Self::change_log_entry_key_len());
                let mut encoded = Vec::new();
                let mut prev_lsn: Option<Lsn> = None;
                let mut index = 0usize;
                for entry in entries {
                    let lsn = entry.lsn();
                    if prev_lsn == Some(lsn) {
                        index += 1;
                    } else {
                        index = 0;
                        prev_lsn = Some(lsn);
                    }
                    Self::write_change_log_entry_key(lsn, index, &mut key);
                    Self::encode_into(entry, &mut encoded)?;
                    table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn load_ddl_log(&self) -> Result<Vec<(Lsn, DdlChange)>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(DDL_LOG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                let lsn = key
                    .split_once(':')
                    .map(|(lsn, _)| lsn)
                    .unwrap_or(key)
                    .parse::<u64>()
                    .map_err(|err| Error::Other(format!("invalid ddl log key: {err}")))?;
                entries.push((Lsn(lsn), Self::decode(value.value())?));
            }
            Ok(entries)
        })
    }

    pub fn load_trigger_audit_state(
        &self,
        ring_capacity: usize,
    ) -> Result<(Vec<TriggerAuditEntry>, u64)> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            if let Ok(config_table) = read_txn.open_table(CONFIG_TABLE) {
                let ring = config_table
                    .get(TRIGGER_AUDIT_RING_CONFIG_KEY)
                    .map_err(Self::storage_error)?
                    .map(|value| Self::decode::<Vec<TriggerAuditEntry>>(value.value()))
                    .transpose()?;
                let next_index = config_table
                    .get(TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY)
                    .map_err(Self::storage_error)?
                    .map(|value| Self::decode::<u64>(value.value()))
                    .transpose()?;
                if let (Some(mut ring), Some(next_index)) = (ring, next_index) {
                    let overflow = ring.len().saturating_sub(ring_capacity);
                    if overflow > 0 {
                        ring.drain(0..overflow);
                    }
                    return Ok((ring, next_index));
                }
            }

            let table = match read_txn.open_table(TRIGGER_AUDIT_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok((Vec::new(), 0)),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut ring = VecDeque::new();
            let mut next_index = 0;
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                if let Some(index) = key
                    .value()
                    .split(':')
                    .next()
                    .and_then(|raw| raw.parse::<u64>().ok())
                {
                    next_index = next_index.max(index.saturating_add(1));
                }
                ring.push_back(Self::decode(value.value())?);
                if ring.len() > ring_capacity {
                    ring.pop_front();
                }
            }
            Ok((ring.into_iter().collect(), next_index))
        })
    }

    /// Delete every durable audit row written before `cutoff_millis`,
    /// whatever table it came from — including rows a DROP TABLE orphaned.
    /// Returns the number removed. Rows written before stamps existed carry no
    /// stamp and are left alone rather than guessed at.
    pub fn prune_trigger_audit_history(&self, cutoff_millis: u64) -> Result<u64> {
        self.with_db(|db| {
            // Find the expired keys under a READ transaction first. The
            // maintenance loop calls this every tick, and on the overwhelmingly
            // common tick nothing has aged out — committing an empty write
            // transaction each time would be a durable write per tick forever
            // for no effect.
            let expired = {
                let read_txn = db.begin_read().map_err(Self::storage_error)?;
                match read_txn.open_table(TRIGGER_AUDIT_STAMPS_TABLE) {
                    Ok(stamps) => stamps
                        .iter()
                        .map_err(Self::storage_error)?
                        .filter_map(|entry| entry.ok())
                        .filter(|(_, stamped_at)| stamped_at.value() < cutoff_millis)
                        .map(|(key, _)| key.value().to_string())
                        .collect::<Vec<_>>(),
                    Err(redb::TableError::TableDoesNotExist(_)) => Vec::new(),
                    Err(err) => return Err(Self::storage_error(err)),
                }
            };
            if expired.is_empty() {
                return Ok(0);
            }

            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            let mut removed = 0u64;
            {
                let mut stamps = write_txn
                    .open_table(TRIGGER_AUDIT_STAMPS_TABLE)
                    .map_err(Self::storage_error)?;
                let mut audit_table = write_txn
                    .open_table(TRIGGER_AUDIT_TABLE)
                    .map_err(Self::storage_error)?;
                for key in expired {
                    audit_table
                        .remove(key.as_str())
                        .map_err(Self::storage_error)?;
                    stamps.remove(key.as_str()).map_err(Self::storage_error)?;
                    removed = removed.saturating_add(1);
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(removed)
        })
    }

    pub fn load_trigger_audit_history(&self) -> Result<Vec<TriggerAuditEntry>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(TRIGGER_AUDIT_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (_, value) = entry.map_err(Self::storage_error)?;
                entries.push(Self::decode(value.value())?);
            }
            Ok(entries)
        })
    }

    pub fn load_commit_index(&self) -> Result<BTreeMap<Lsn, TxId>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(COMMIT_INDEX_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(BTreeMap::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut index = BTreeMap::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (lsn, tx) = entry.map_err(Self::storage_error)?;
                index.insert(Lsn(lsn.value()), TxId(tx.value()));
            }
            Ok(index)
        })
    }

    pub fn flush_commit_index_entries(&self, entries: &BTreeMap<Lsn, TxId>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut table = write_txn
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                for (lsn, tx) in entries {
                    table.insert(lsn.0, tx.0).map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn rewrite_pruned_state(
        &self,
        table_rows: &HashMap<String, Vec<VersionedRow>>,
        vectors: &[VectorEntry],
        edges: &[AdjEntry],
    ) -> Result<()> {
        let table_meta = self.load_all_table_meta()?;
        let vector_quantization = Self::vector_quantization_map(&table_meta);
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;

            for (name, rows) in table_rows {
                let table_name = Self::rel_table_name(name);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
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
                let mut redb_table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                for row in rows {
                    let encoded = Self::encode_versioned_row(row, table_meta.get(name))?;
                    let key = Self::rel_row_key(row);
                    redb_table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
                Self::retain_sync_source_provenance_for_table_rows(&write_txn, name, rows)?;
            }

            match write_txn.delete_table(VECTORS_TABLE) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            {
                let mut table = write_txn
                    .open_table(VECTORS_TABLE)
                    .map_err(Self::storage_error)?;
                for entry in vectors {
                    let quantization = vector_quantization
                        .get(&entry.index)
                        .copied()
                        .unwrap_or_default();
                    let encoded = Self::encode_vector_entry(entry, quantization)?;
                    let key = Self::vector_key(entry);
                    table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            match write_txn.delete_table(GRAPH_FWD_TABLE) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            match write_txn.delete_table(GRAPH_REV_TABLE) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            {
                let mut fwd_table = write_txn
                    .open_table(GRAPH_FWD_TABLE)
                    .map_err(Self::storage_error)?;
                let mut rev_table = write_txn
                    .open_table(GRAPH_REV_TABLE)
                    .map_err(Self::storage_error)?;

                for entry in edges {
                    let encoded = Self::encode(entry)?;
                    let fwd_key = Self::graph_fwd_key(entry);
                    let rev_key = Self::graph_rev_key(entry);
                    fwd_table
                        .insert(fwd_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    rev_table
                        .insert(rev_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn rewrite_commit_index(&self, entries: &BTreeMap<Lsn, TxId>) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            let _ = write_txn.delete_table(COMMIT_INDEX_TABLE);
            {
                let mut table = write_txn
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                for (lsn, tx) in entries {
                    table.insert(lsn.0, tx.0).map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    /// Remove exactly the named relational row VERSIONS, in ONE redb write
    /// transaction, together with the change-log entries that referenced
    /// them and the sync-source-lsn tracking rows that lost their last live
    /// version. Nothing else in the file is read or rewritten: a table's
    /// surviving rows stay exactly where they are, and vectors, edges, and
    /// the commit index are untouched (see [`Self::prune_vectors_and_edges_scoped`]
    /// for those).
    ///
    /// `row_keys` are `(table, row_id, created_tx, lsn)` — the full
    /// relational key, so each is a point `remove` with no scan needed to
    /// find it (`rel_row_key`, computed from a value already in hand — the
    /// `VersionedRow` being pruned).
    ///
    /// `pruned_change_keys` are the `(table, row_id, lsn)` identities of the
    /// same pruned versions, used to find which change-log entries
    /// referenced them. This does NOT recompute each entry's on-disk
    /// `(lsn, index)` coordinate from an in-memory recount: the moment a
    /// scoped pass leaves holes (rather than the old wholesale rewrite's
    /// full re-densify every pass), an in-memory recount goes stale on the
    /// SECOND pass over an lsn group and can silently orphan a change-log
    /// entry (see `changes_since`'s replay, which reads the row version AT
    /// each logged LSN and substitutes the latest on a miss). Instead, for
    /// every LSN any pruned version's change-log entry names, this reads
    /// that LSN group's entries AS THEY ACTUALLY STAND ON DISK right now,
    /// drops the ones naming a pruned version, and rewrites ONLY that LSN
    /// group, densely renumbered from zero — exactly as a fresh sequence of
    /// commits would have written them, and correct regardless of how many
    /// prior scoped passes already touched that group. Cost is proportional
    /// to the number of DISTINCT lsns touched (each an O(entries-in-that-
    /// commit) read+rewrite), never to the whole change log.
    ///
    /// `orphaned_source_lsn_rows` are the `(table, row_id)` pairs that lost
    /// their last LIVE version this pass — looked up by key
    /// (`sync_row_source_lsn_key`) rather than by iterating the table, as
    /// `retain_sync_source_lsns_for_table_rows` does today.
    pub fn prune_versions_scoped(
        &self,
        row_keys: &[(String, RowId, TxId, Lsn)],
        pruned_change_keys: &[(String, RowId, Lsn)],
        orphaned_source_lsn_rows: &[(String, RowId)],
    ) -> Result<PruneScopedStats> {
        if row_keys.is_empty()
            && pruned_change_keys.is_empty()
            && orphaned_source_lsn_rows.is_empty()
        {
            return Ok(PruneScopedStats::default());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            let mut change_log_keys_removed = 0u64;

            // CHANGE-LOG-FIRST within this one transaction, for the same
            // reason the shipped wholesale path orders it first: an orphaned
            // change-log entry corrupts replay, while an orphaned row
            // version is inert. Merging both into ONE transaction (rather
            // than the two separate transactions the wholesale path used)
            // removes the interleaving window entirely — after a crash the
            // file has either both removals or neither.
            if !pruned_change_keys.is_empty() {
                let mut affected: BTreeMap<Lsn, HashSet<(String, RowId)>> = BTreeMap::new();
                for (table, row_id, lsn) in pruned_change_keys {
                    affected
                        .entry(*lsn)
                        .or_default()
                        .insert((table.clone(), *row_id));
                }
                match write_txn.open_table(CHANGE_LOG_TABLE) {
                    Ok(mut table) => {
                        let mut key_buf = String::with_capacity(Self::change_log_entry_key_len());
                        for (lsn, pruned_here) in &affected {
                            let prefix = format!("{:020}:", lsn.0);
                            let existing: Vec<(String, Vec<u8>)> = {
                                let mut entries = Vec::new();
                                for entry in table
                                    .range(prefix.as_str()..)
                                    .map_err(Self::storage_error)?
                                {
                                    let (key, value) = entry.map_err(Self::storage_error)?;
                                    let key = key.value();
                                    if !key.starts_with(prefix.as_str()) {
                                        break;
                                    }
                                    entries.push((key.to_string(), value.value().to_vec()));
                                }
                                entries
                            };
                            let mut survivors: Vec<Vec<u8>> = Vec::with_capacity(existing.len());
                            for (key, bytes) in existing {
                                let decoded: ChangeLogEntry = Self::decode(&bytes)?;
                                let referenced = match &decoded {
                                    ChangeLogEntry::RowInsert { table, row_id, .. }
                                    | ChangeLogEntry::RowDelete { table, row_id, .. } => {
                                        pruned_here.contains(&(table.clone(), *row_id))
                                    }
                                    _ => false,
                                };
                                table.remove(key.as_str()).map_err(Self::storage_error)?;
                                if referenced {
                                    change_log_keys_removed =
                                        change_log_keys_removed.saturating_add(1);
                                } else {
                                    survivors.push(bytes);
                                }
                            }
                            for (index, bytes) in survivors.into_iter().enumerate() {
                                Self::write_change_log_entry_key(*lsn, index, &mut key_buf);
                                table
                                    .insert(key_buf.as_str(), bytes.as_slice())
                                    .map_err(Self::storage_error)?;
                            }
                        }
                    }
                    Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(err) => return Err(Self::storage_error(err)),
                }
            }

            let mut opened_rel_tables: HashMap<String, redb::Table<'_, &[u8], &[u8]>> =
                HashMap::new();
            for (table, row_id, created_tx, lsn) in row_keys {
                if !opened_rel_tables.contains_key(table) {
                    let table_name = Self::rel_table_name(table);
                    let table_def: TableDefinition<&[u8], &[u8]> =
                        TableDefinition::new(table_name.as_str());
                    match write_txn.open_table(table_def) {
                        Ok(redb_table) => {
                            opened_rel_tables.insert(table.clone(), redb_table);
                        }
                        Err(redb::TableError::TableDoesNotExist(_)) => continue,
                        Err(err) => return Err(Self::storage_error(err)),
                    }
                }
                if let Some(redb_table) = opened_rel_tables.get_mut(table) {
                    let key = Self::rel_row_key_from_parts(*row_id, *created_tx, *lsn);
                    redb_table
                        .remove(key.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            drop(opened_rel_tables);

            if !orphaned_source_lsn_rows.is_empty() {
                match (
                    write_txn.open_table(SYNC_ROW_SOURCE_LSN_TABLE),
                    write_txn.open_table(SYNC_ROW_SOURCE_KIND_TABLE),
                ) {
                    (Ok(mut lsn_table), Ok(mut kind_table)) => {
                        for (table_name, row_id) in orphaned_source_lsn_rows {
                            let key = Self::sync_row_source_lsn_key(table_name, *row_id);
                            lsn_table
                                .remove(key.as_slice())
                                .map_err(Self::storage_error)?;
                            kind_table
                                .remove(key.as_slice())
                                .map_err(Self::storage_error)?;
                        }
                    }
                    (Err(redb::TableError::TableDoesNotExist(_)), _) => {}
                    (_, Err(redb::TableError::TableDoesNotExist(_))) => {}
                    (Err(err), _) | (_, Err(err)) => return Err(Self::storage_error(err)),
                }
            }

            write_txn.commit().map_err(Self::storage_error)?;
            Ok(PruneScopedStats {
                row_keys_removed: row_keys.len() as u64,
                change_log_keys_removed,
                vector_keys_removed: 0,
                edge_keys_removed: 0,
            })
        })
    }

    /// Apply the durable half of one authoritative purge in exactly one Redb
    /// write transaction.  This is deliberately not composed from scoped
    /// pruning helpers: the permanent lifecycle record must become durable
    /// atomically with every selected copy's disappearance.
    pub(crate) fn commit_authoritative_purge(
        &self,
        projection: &AuthoritativePurgePersistenceProjection,
    ) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;

            // A selected change-log occurrence is identified by its full
            // value inside its LSN group.  Read and re-densify only touched
            // groups so earlier scoped maintenance cannot leave an orphaned
            // or mis-indexed survivor.
            let mut selected_by_lsn = BTreeMap::<Lsn, Vec<ChangeLogEntry>>::new();
            for entry in &projection.change_log_entries {
                selected_by_lsn
                    .entry(entry.lsn())
                    .or_default()
                    .push(entry.clone());
            }
            if !selected_by_lsn.is_empty() {
                let mut table = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let mut key_buf = String::with_capacity(Self::change_log_entry_key_len());
                for (lsn, mut witnesses) in selected_by_lsn {
                    let prefix = format!("{:020}:", lsn.0);
                    let existing = {
                        let mut entries = Vec::new();
                        for entry in table
                            .range(prefix.as_str()..)
                            .map_err(Self::storage_error)?
                        {
                            let (key, value) = entry.map_err(Self::storage_error)?;
                            if !key.value().starts_with(prefix.as_str()) {
                                break;
                            }
                            entries.push((key.value().to_string(), value.value().to_vec()));
                        }
                        entries
                    };
                    let mut survivors = Vec::with_capacity(existing.len());
                    for (key, bytes) in existing {
                        let decoded: ChangeLogEntry = Self::decode(&bytes)?;
                        if let Some(position) = witnesses.iter().position(|wanted| *wanted == decoded)
                        {
                            witnesses.remove(position);
                        } else {
                            survivors.push(bytes);
                        }
                        table.remove(key.as_str()).map_err(Self::storage_error)?;
                    }
                    if !witnesses.is_empty() {
                        return Err(Error::SyncError(
                            "authoritative purge change-log witness is missing from its LSN group"
                                .to_string(),
                        ));
                    }
                    for (index, bytes) in survivors.into_iter().enumerate() {
                        Self::write_change_log_entry_key(lsn, index, &mut key_buf);
                        table
                            .insert(key_buf.as_str(), bytes.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }
            }

            let mut relational_tables: HashMap<String, redb::Table<'_, &[u8], &[u8]>> =
                HashMap::new();
            for (table, row_id, created_tx, lsn) in &projection.row_versions {
                if !relational_tables.contains_key(table) {
                    let name = Self::rel_table_name(table);
                    let definition: TableDefinition<&[u8], &[u8]> =
                        TableDefinition::new(name.as_str());
                    relational_tables.insert(
                        table.clone(),
                        write_txn.open_table(definition).map_err(Self::storage_error)?,
                    );
                }
                let key = Self::rel_row_key_from_parts(*row_id, *created_tx, *lsn);
                let removed = relational_tables
                    .get_mut(table)
                    .expect("opened relational table")
                    .remove(key.as_slice())
                    .map_err(Self::storage_error)?;
                if removed.is_none() {
                    return Err(Error::SyncError(
                        "authoritative purge selected relational version disappeared before durable commit"
                            .to_string(),
                    ));
                }
            }
            drop(relational_tables);

            if !projection.source_provenance.is_empty() {
                let mut lsns = write_txn
                    .open_table(SYNC_ROW_SOURCE_LSN_TABLE)
                    .map_err(Self::storage_error)?;
                let mut kinds = write_txn
                    .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
                    .map_err(Self::storage_error)?;
                for (table, row_id, expected_lsn, expected_kind) in &projection.source_provenance {
                    let key = Self::sync_row_source_lsn_key(table, *row_id);
                    let lsn = lsns
                        .get(key.as_slice())
                        .map_err(Self::storage_error)?
                        .map(|value| value.value());
                    let kind = kinds
                        .get(key.as_slice())
                        .map_err(Self::storage_error)?
                        .map(|value| value.value());
                    if lsn != Some(expected_lsn.0) || kind != Some(*expected_kind) {
                        return Err(Error::SyncError(
                            "authoritative purge source-provenance witness changed before durable commit"
                                .to_string(),
                        ));
                    }
                    lsns.remove(key.as_slice()).map_err(Self::storage_error)?;
                    kinds.remove(key.as_slice()).map_err(Self::storage_error)?;
                }
            }

            if !projection.vectors.is_empty() {
                let mut vectors = write_txn.open_table(VECTORS_TABLE).map_err(Self::storage_error)?;
                for entry in &projection.vectors {
                    let key = Self::vector_key(entry);
                    let removed = vectors.remove(key.as_slice()).map_err(Self::storage_error)?;
                    if removed.is_none() {
                        return Err(Error::SyncError(
                            "authoritative purge selected vector occurrence disappeared before durable commit"
                                .to_string(),
                        ));
                    }
                }
            }

            if !projection.graph_entries.is_empty() {
                let mut forward = write_txn.open_table(GRAPH_FWD_TABLE).map_err(Self::storage_error)?;
                let mut reverse = write_txn.open_table(GRAPH_REV_TABLE).map_err(Self::storage_error)?;
                for entry in &projection.graph_entries {
                    let forward_key = Self::graph_fwd_key(entry);
                    let reverse_key = Self::graph_rev_key(entry);
                    let removed_forward = forward
                        .remove(forward_key.as_slice())
                        .map_err(Self::storage_error)?;
                    let removed_reverse = reverse
                        .remove(reverse_key.as_slice())
                        .map_err(Self::storage_error)?;
                    if removed_forward.is_none() || removed_reverse.is_none() {
                        return Err(Error::SyncError(
                            "authoritative purge selected graph occurrence disappeared before durable commit"
                                .to_string(),
                        ));
                    }
                }
            }

            for (sink, queue_id, expected_bytes) in &projection.sink_entries {
                let name = Self::sink_queue_table_name(sink);
                let definition: TableDefinition<u64, &[u8]> = TableDefinition::new(name.as_str());
                let mut queue = write_txn.open_table(definition).map_err(Self::storage_error)?;
                let bytes = queue
                    .get(*queue_id)
                    .map_err(Self::storage_error)?
                    .map(|value| value.value().to_vec());
                if bytes.as_deref() != Some(expected_bytes.as_slice()) {
                    return Err(Error::SyncError(
                        "authoritative purge selected sink occurrence disappeared before durable commit"
                            .to_string(),
                    ));
                }
                queue.remove(*queue_id).map_err(Self::storage_error)?;
            }

            {
                let mut config = write_txn.open_table(CONFIG_TABLE).map_err(Self::storage_error)?;
                for key in &projection.config_keys_removed {
                    config.remove(key.as_str()).map_err(Self::storage_error)?;
                }
                for (key, bytes) in &projection.lifecycle_records {
                    config
                        .insert(key.as_str(), bytes.as_slice())
                        .map_err(Self::storage_error)?;
                }
                for (key, bytes) in &projection.purge_delivery_items {
                    let previous = config
                        .insert(key.as_str(), bytes.as_slice())
                        .map_err(Self::storage_error)?;
                    if previous.is_some() {
                        return Err(Error::SyncError(format!(
                            "authoritative purge delivery journal collision at {key}"
                        )));
                    }
                }
            }

            apply_authoritative_purge_in_write(&write_txn, &projection.blob_purge)?;

            #[cfg(test)]
            if take_authoritative_purge_point_remove_persistence_failure_for_test() {
                return Err(Error::Other(
                    "authoritative purge point-remove persistence failure injected".to_string(),
                ));
            }
            write_txn.commit().map_err(Self::storage_error)
        })
    }

    /// Remove exactly the named vectors and edges, in ONE redb write
    /// transaction. Used by the retention pass when pruned rows carried
    /// vectors or graph nodes, and by version cleanup ONLY for the vector
    /// copies attached to released row versions — version cleanup never
    /// touches edges (edge identity is `(source, target, edge_type)` plus
    /// its own `created_tx`/`lsn`, self-owned and versioned by no relational
    /// row), so its caller always passes an empty `edges` slice.
    pub fn prune_vectors_and_edges_scoped(
        &self,
        vectors: &[VectorEntry],
        edges: &[AdjEntry],
    ) -> Result<PruneScopedStats> {
        if vectors.is_empty() && edges.is_empty() {
            return Ok(PruneScopedStats::default());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            if !vectors.is_empty() {
                match write_txn.open_table(VECTORS_TABLE) {
                    Ok(mut table) => {
                        for entry in vectors {
                            let key = Self::vector_key(entry);
                            table.remove(key.as_slice()).map_err(Self::storage_error)?;
                        }
                    }
                    Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(err) => return Err(Self::storage_error(err)),
                }
            }
            if !edges.is_empty() {
                let fwd = write_txn.open_table(GRAPH_FWD_TABLE);
                let rev = write_txn.open_table(GRAPH_REV_TABLE);
                match (fwd, rev) {
                    (Ok(mut fwd), Ok(mut rev)) => {
                        for entry in edges {
                            let fwd_key = Self::graph_fwd_key(entry);
                            let rev_key = Self::graph_rev_key(entry);
                            fwd.remove(fwd_key.as_slice())
                                .map_err(Self::storage_error)?;
                            rev.remove(rev_key.as_slice())
                                .map_err(Self::storage_error)?;
                        }
                    }
                    (Err(redb::TableError::TableDoesNotExist(_)), _)
                    | (_, Err(redb::TableError::TableDoesNotExist(_))) => {}
                    (Err(err), _) | (_, Err(err)) => return Err(Self::storage_error(err)),
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(PruneScopedStats {
                row_keys_removed: 0,
                change_log_keys_removed: 0,
                vector_keys_removed: vectors.len() as u64,
                edge_keys_removed: (edges.len() as u64).saturating_mul(2),
            })
        })
    }

    /// Remove exactly the named commit-index entries — the scoped
    /// counterpart of `rewrite_commit_index`, used when a version-cleanup
    /// pass only made a handful of LSNs' commit-index entries redundant
    /// (their change-log entries are all gone and nothing else names them).
    /// Point removes; nothing else in the index is read or rewritten.
    pub fn remove_commit_index_entries_scoped(&self, lsns: &[Lsn]) -> Result<u64> {
        if lsns.is_empty() {
            return Ok(0);
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            let mut removed = 0u64;
            match write_txn.open_table(COMMIT_INDEX_TABLE) {
                Ok(mut table) => {
                    for lsn in lsns {
                        if table.remove(lsn.0).map_err(Self::storage_error)?.is_some() {
                            removed = removed.saturating_add(1);
                        }
                    }
                }
                Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(removed)
        })
    }

    /// Compacts the store to its minimal on-disk size, THEN recycles the
    /// redb handle (closes it and reopens the same, now-minimal, file) —
    /// checkpoint export runs this on the finished artifact so
    /// `bytes_written` reflects content, not allocator slack from the
    /// batched copy.
    ///
    /// The recycle exists because redb retains in-process allocator/region
    /// bookkeeping sized to a database's historical peak allocation, and a
    /// file-level `compact()` does not reset it even though it fully
    /// normalizes the on-disk btree and file size — confirmed by direct
    /// measurement: two databases compacted down to a byte-identical
    /// on-disk btree, one carrying a much larger prior-churn history, still
    /// showed a large gap in per-transaction commit cost after compaction
    /// alone; closing and reopening the handle closed that gap. A
    /// long-running embedded consumer has no other opportunity to do this
    /// (it cannot process-restart), so the recycle happens HERE,
    /// transparently, under the SAME locks compaction itself already
    /// holds: no other caller can observe (or race) an intermediate closed
    /// state, because every access to the underlying handle — this
    /// function included — goes through `self.db`'s mutex.
    ///
    /// A deliberate working-headroom margin between the shrink and the
    /// recycle was tried and measured NOT to help: redb's own close-time
    /// bookkeeping (`ensure_allocator_state_table_and_trim` in `Drop for
    /// redb::Database`) regrows a maximally-shrunk file to roughly the
    /// same final size whether or not a margin was left beforehand, so the
    /// margin only added the cost of two extra write transactions for no
    /// measured benefit — removed. The steady-state cost story for a file
    /// carrying a large amount of PRE-cleanup history (never yet compacted
    /// before) is the still-open retrofit-scenario question the version-
    /// cleanup-scaling bench's A2 arm measures directly.
    ///
    /// Returns the recycle's own duration in microseconds, separate from
    /// compaction's own timing (which the caller already measures around
    /// the whole call), matching the existing convention of reporting every
    /// compaction-adjacent cost as its own explicit number.
    ///
    /// On a reopen failure, the file itself is untouched (compaction had
    /// already finished; nothing more is written to it here) and this
    /// `RedbPersistence` is left closed — [`Error::StoreHandleRecycleFailed`]
    /// names the failure; a fresh `RedbPersistence::open`/`Database::open`
    /// on the same path recovers all data once this instance's lock is
    /// released via `close()`.
    pub(crate) fn compact(&self) -> Result<u64> {
        let lock_guard = self.lock_file.lock().expect("pid lock mutex poisoned");
        if lock_guard.is_none() {
            return Err(Error::Other("database persistence is closed".to_string()));
        }
        let mut db_guard = self.db.lock().expect("redb mutex poisoned");
        let db = db_guard
            .as_mut()
            .ok_or_else(|| Error::Other("database persistence is closed".to_string()))?;
        while db.compact().map_err(Self::storage_error)? {}

        let recycle_started = std::time::Instant::now();
        let old = db_guard.take();
        drop(old);
        if take_handle_recycle_reopen_fault_for_test() {
            return Err(Error::StoreHandleRecycleFailed {
                path: self.path.display().to_string(),
                reason: "injected reopen failure (test seam)".to_string(),
            });
        }
        let reopened = Self::open_hook_suppressed(|| redb_builder().open(&self.path));
        match reopened {
            Ok(Ok(fresh)) => {
                *db_guard = Some(fresh);
                Ok(recycle_started.elapsed().as_micros() as u64)
            }
            Ok(Err(err)) => Err(Error::StoreHandleRecycleFailed {
                path: self.path.display().to_string(),
                reason: err.to_string(),
            }),
            Err(_) => Err(Error::StoreHandleRecycleFailed {
                path: self.path.display().to_string(),
                reason: "reopen panicked; store may be truncated or corrupt".to_string(),
            }),
        }
    }

    /// Fraction of the on-disk file that is free/fragmented pages (dead space):
    /// `fragmented / (stored + metadata + fragmented)`. Used to decide whether a
    /// version-compaction pass should follow up with a full redb `compact()` to
    /// reclaim the freed pages. Reads stats via a short write transaction that is
    /// immediately aborted, so it never mutates the store.
    pub(crate) fn fragmentation_ratio(&self) -> Result<f64> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            let stats = write_txn.stats().map_err(Self::storage_error)?;
            let stored = stats.stored_bytes();
            let metadata = stats.metadata_bytes();
            let fragmented = stats.fragmented_bytes();
            write_txn.abort().map_err(Self::storage_error)?;
            let total = stored.saturating_add(metadata).saturating_add(fragmented);
            if total == 0 {
                Ok(0.0)
            } else {
                Ok(fragmented as f64 / total as f64)
            }
        })
    }

    // --- Checkpoint-export seams ------------------------------------------
    //
    // Raw dumps pair with the existing raw config writer so export can copy
    // dynamic-key state (tenant watermarks, per-sink queues, audit rings)
    // without decoding it. The append-batch writers stream per-category
    // batches into a fresh artifact; unlike the `rewrite_*` family they never
    // delete-then-rewrite a full slice.

    pub(crate) fn dump_config_raw(&self) -> Result<Vec<(String, Vec<u8>)>> {
        self.dump_str_keyed_table_raw(CONFIG_TABLE)
    }

    fn digest_canonical_relational_table(
        &self,
        read_txn: &redb::ReadTransaction,
        table_name: &str,
        hasher: &mut blake3::Hasher,
    ) -> Result<u64> {
        digest_field(hasher, table_name.as_bytes());
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(table_name);
        let table = read_txn
            .open_table(definition)
            .map_err(Self::storage_error)?;
        let mut count = 0_u64;
        for entry in table.iter().map_err(Self::storage_error)? {
            let (key, value) = entry.map_err(Self::storage_error)?;
            digest_field(hasher, key.value());
            let row: PersistedVersionedRow = Self::decode(value.value())?;
            rmp_serde::encode::write(
                &mut *hasher,
                &(
                    row.row_id,
                    row.created_tx,
                    row.deleted_tx,
                    row.lsn,
                    row.created_at,
                ),
            )
            .map_err(|err| Error::Other(format!("failed to fingerprint relational row: {err}")))?;
            let mut columns = row.values.into_iter().collect::<Vec<_>>();
            columns.sort_by(|left, right| left.0.cmp(&right.0));
            rmp_serde::encode::write(&mut *hasher, &(columns.len() as u64)).map_err(|err| {
                Error::Other(format!("failed to fingerprint relational columns: {err}"))
            })?;
            for (column, value) in columns {
                digest_field(hasher, column.as_bytes());
                rmp_serde::encode::write(&mut *hasher, &value).map_err(|err| {
                    Error::Other(format!("failed to fingerprint relational value: {err}"))
                })?;
            }
            count = count.saturating_add(1);
        }
        Ok(count)
    }

    /// Fingerprint every Redb table an authenticated schema or data apply can
    /// mutate. Relational rows are decoded and hashed with column-sorted
    /// values because snapshot export re-encodes their `HashMap`; every other
    /// category remains an exact persisted key/value-byte hash. Dynamic
    /// relational and sink-queue table names are part of the digest, so
    /// creating or removing an empty table is still visible.
    pub(crate) fn raw_sync_apply_state_digest(&self) -> Result<RawSyncApplyStateDigest> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let mut hasher = blake3::Hasher::new();
            digest_field(&mut hasher, b"contextdb.raw-sync-apply-state.v2");

            digest_redb_table(&read_txn, META_TABLE, "meta", &mut hasher)?;
            digest_redb_table(
                &read_txn,
                FORMAT_METADATA_TABLE,
                "format-metadata",
                &mut hasher,
            )?;
            digest_redb_table(&read_txn, CONFIG_TABLE, "config", &mut hasher)?;
            digest_redb_table(&read_txn, CHANGE_LOG_TABLE, "change-log", &mut hasher)?;
            digest_redb_table(&read_txn, DDL_LOG_TABLE, "ddl-log", &mut hasher)?;
            digest_redb_table(&read_txn, COMMIT_INDEX_TABLE, "commit-index", &mut hasher)?;
            let sink_audit_entries =
                digest_redb_table(&read_txn, SINK_AUDIT_TABLE, "sink-audit", &mut hasher)?;
            let trigger_audit_entries =
                digest_redb_table(&read_txn, TRIGGER_AUDIT_TABLE, "trigger-audit", &mut hasher)?;
            digest_redb_table(
                &read_txn,
                TRIGGER_AUDIT_STAMPS_TABLE,
                "trigger-audit-stamps",
                &mut hasher,
            )?;
            digest_redb_table(&read_txn, GRAPH_FWD_TABLE, "graph-forward", &mut hasher)?;
            digest_redb_table(&read_txn, GRAPH_REV_TABLE, "graph-reverse", &mut hasher)?;
            digest_redb_table(&read_txn, VECTORS_TABLE, "vectors", &mut hasher)?;
            digest_redb_table(
                &read_txn,
                SYNC_ROW_SOURCE_LSN_TABLE,
                "sync-row-source-lsn",
                &mut hasher,
            )?;
            digest_redb_table(
                &read_txn,
                SYNC_ROW_SOURCE_KIND_TABLE,
                "sync-row-source-kind",
                &mut hasher,
            )?;

            let mut dynamic_names = read_txn
                .list_tables()
                .map_err(Self::storage_error)?
                .map(|handle| handle.name().to_string())
                .filter(|name| name.starts_with("rel_") || name.starts_with("__sink_queue_"))
                .collect::<Vec<_>>();
            dynamic_names.sort();
            let mut sink_queue_entries = 0_u64;
            for name in dynamic_names {
                if name.starts_with("rel_") {
                    self.digest_canonical_relational_table(&read_txn, &name, &mut hasher)?;
                } else {
                    let definition: TableDefinition<u64, &[u8]> =
                        TableDefinition::new(name.as_str());
                    sink_queue_entries = sink_queue_entries.saturating_add(digest_redb_table(
                        &read_txn,
                        definition,
                        &name,
                        &mut hasher,
                    )?);
                }
            }

            Ok(RawSyncApplyStateDigest {
                digest: hasher.finalize().to_hex().to_string(),
                trigger_audit_entries,
                sink_audit_entries,
                sink_queue_entries,
            })
        })
    }

    pub(crate) fn dump_trigger_audit_raw(&self) -> Result<Vec<(String, Vec<u8>)>> {
        self.dump_str_keyed_table_raw(TRIGGER_AUDIT_TABLE)
    }

    pub(crate) fn dump_trigger_audit_stamps_raw(&self) -> Result<Vec<(String, u64)>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(TRIGGER_AUDIT_STAMPS_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                entries.push((key.value().to_string(), value.value()));
            }
            Ok(entries)
        })
    }

    pub(crate) fn dump_sink_audit_raw(&self) -> Result<Vec<(String, Vec<u8>)>> {
        self.dump_str_keyed_table_raw(SINK_AUDIT_TABLE)
    }

    fn dump_str_keyed_table_raw(
        &self,
        definition: TableDefinition<&str, &[u8]>,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(definition) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                entries.push((key.value().to_string(), value.value().to_vec()));
            }
            Ok(entries)
        })
    }

    pub(crate) fn append_trigger_audit_raw(&self, entries: &[(String, Vec<u8>)]) -> Result<()> {
        self.append_str_keyed_table_raw(TRIGGER_AUDIT_TABLE, entries)
    }

    pub(crate) fn append_trigger_audit_stamps_raw(&self, entries: &[(String, u64)]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut table = write_txn
                    .open_table(TRIGGER_AUDIT_STAMPS_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, value) in entries {
                    table
                        .insert(key.as_str(), *value)
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub(crate) fn append_sink_audit_raw(&self, entries: &[(String, Vec<u8>)]) -> Result<()> {
        self.append_str_keyed_table_raw(SINK_AUDIT_TABLE, entries)
    }

    fn append_str_keyed_table_raw(
        &self,
        definition: TableDefinition<&str, &[u8]>,
        entries: &[(String, Vec<u8>)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut table = write_txn
                    .open_table(definition)
                    .map_err(Self::storage_error)?;
                for (key, value) in entries {
                    table
                        .insert(key.as_str(), value.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub(crate) fn sink_queue_names(&self) -> Result<Vec<String>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let mut names = Vec::new();
            for handle in read_txn.list_tables().map_err(Self::storage_error)? {
                if let Some(sink) = handle.name().strip_prefix("__sink_queue_") {
                    names.push(sink.to_string());
                }
            }
            Ok(names)
        })
    }

    pub(crate) fn dump_sink_queue_raw(&self, sink: &str) -> Result<Vec<(u64, Vec<u8>)>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table_name = Self::sink_queue_table_name(sink);
            let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new(table_name.as_str());
            let table = match read_txn.open_table(table_def) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                entries.push((key.value(), value.value().to_vec()));
            }
            Ok(entries)
        })
    }

    pub(crate) fn append_sink_queue_raw(
        &self,
        sink: &str,
        entries: &[(u64, Vec<u8>)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let table_name = Self::sink_queue_table_name(sink);
                let table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                for (id, value) in entries {
                    table
                        .insert(*id, value.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub(crate) fn append_table_rows_batch(
        &self,
        name: &str,
        meta: Option<&TableMeta>,
        rows: &[VersionedRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let table_name = Self::rel_table_name(name);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut redb_table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                let mut encoded = Vec::new();
                for row in rows {
                    Self::encode_versioned_row_into(row, meta, &mut encoded)?;
                    let key = Self::rel_row_key(row);
                    redb_table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub(crate) fn append_sync_source_provenance_batch(
        &self,
        entries: &[(String, RowId, Lsn, u8)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut lsns = write_txn
                    .open_table(SYNC_ROW_SOURCE_LSN_TABLE)
                    .map_err(Self::storage_error)?;
                let mut kinds = write_txn
                    .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
                    .map_err(Self::storage_error)?;
                for (table, row_id, lsn, kind) in entries {
                    let key = Self::sync_row_source_lsn_key(table, *row_id);
                    lsns.insert(key.as_slice(), lsn.0)
                        .map_err(Self::storage_error)?;
                    kinds
                        .insert(key.as_slice(), *kind)
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)
        })
    }

    pub(crate) fn append_graph_edges_batch(&self, edges: &[AdjEntry]) -> Result<()> {
        if edges.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut fwd_table = write_txn
                    .open_table(GRAPH_FWD_TABLE)
                    .map_err(Self::storage_error)?;
                let mut rev_table = write_txn
                    .open_table(GRAPH_REV_TABLE)
                    .map_err(Self::storage_error)?;
                let mut encoded = Vec::new();
                for entry in edges {
                    Self::encode_into(entry, &mut encoded)?;
                    let fwd_key = Self::graph_fwd_key(entry);
                    let rev_key = Self::graph_rev_key(entry);
                    fwd_table
                        .insert(fwd_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    rev_table
                        .insert(rev_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub(crate) fn append_vector_entries_batch(
        &self,
        entries: &[VectorEntry],
        quantization: &HashMap<VectorIndexRef, VectorQuantization>,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut table = write_txn
                    .open_table(VECTORS_TABLE)
                    .map_err(Self::storage_error)?;
                for entry in entries {
                    let quantization = quantization.get(&entry.index).copied().unwrap_or_default();
                    let encoded = Self::encode_vector_entry(entry, quantization)?;
                    let key = Self::vector_key(entry);
                    table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    /// Each entry carries its per-LSN key index, tracked by the caller across
    /// batches so a multi-entry commit split over two batches never collides.
    pub(crate) fn append_change_log_entries_batch(
        &self,
        entries: &[(usize, ChangeLogEntry)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut table = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let mut encoded = Vec::new();
                let mut key = String::with_capacity(Self::change_log_entry_key_len());
                for (index, entry) in entries {
                    Self::write_change_log_entry_key(entry.lsn(), *index, &mut key);
                    Self::encode_into(entry, &mut encoded)?;
                    table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub(crate) fn append_ddl_log_entries(&self, entries: &[(Lsn, DdlChange)]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                // The log is LSN-ordered; key same-LSN runs exactly as the
                // commit-time writer does (plain key for a single change,
                // indexed keys for a multi-change commit).
                let mut start = 0usize;
                while start < entries.len() {
                    let lsn = entries[start].0;
                    let mut end = start + 1;
                    while end < entries.len() && entries[end].0 == lsn {
                        end += 1;
                    }
                    let count = end - start;
                    for (offset, (_, change)) in entries[start..end].iter().enumerate() {
                        let key = Self::ddl_log_key_for_index(lsn, offset, count);
                        let encoded = Self::encode(change)?;
                        ddl_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                    start = end;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    fn load_graph_table(&self, definition: TableDefinition<&[u8], &[u8]>) -> Result<Vec<AdjEntry>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(definition) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (_, value) = entry.map_err(Self::storage_error)?;
                entries.push(Self::decode(value.value())?);
            }
            Ok(entries)
        })
    }

    fn load_table_meta_in_read_txn(
        read_txn: &redb::ReadTransaction,
        name: &str,
    ) -> Result<Option<TableMeta>> {
        let meta_table = match read_txn.open_table(META_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(err) => return Err(Self::storage_error(err)),
        };
        let key = Self::meta_key(name);
        meta_table
            .get(key.as_str())
            .map_err(Self::storage_error)?
            .map(|value| Self::decode_table_meta_versioned(value.value()).map(|(meta, _)| meta))
            .transpose()
    }

    pub(crate) fn vector_quantization_map(
        table_meta: &HashMap<String, TableMeta>,
    ) -> HashMap<VectorIndexRef, VectorQuantization> {
        let mut indexes = HashMap::new();
        for (table, meta) in table_meta {
            for column in &meta.columns {
                if matches!(column.column_type, ColumnType::Vector(_)) {
                    indexes.insert(
                        VectorIndexRef::new(table.clone(), column.name.clone()),
                        column.quantization,
                    );
                }
            }
        }
        indexes
    }

    fn write_set_visibility_tx(ws: &WriteSet) -> Option<TxId> {
        ws.relational_inserts
            .iter()
            .flat_map(|(_, row)| std::iter::once(row.created_tx).chain(row.deleted_tx))
            .chain(
                ws.relational_deletes
                    .iter()
                    .map(|(_, _, deleted_tx)| *deleted_tx),
            )
            .chain(
                ws.adj_inserts
                    .iter()
                    .flat_map(|entry| std::iter::once(entry.created_tx).chain(entry.deleted_tx)),
            )
            .chain(
                ws.adj_deletes
                    .iter()
                    .map(|(_, _, _, deleted_tx)| *deleted_tx),
            )
            .chain(
                ws.vector_inserts
                    .iter()
                    .flat_map(|entry| std::iter::once(entry.created_tx).chain(entry.deleted_tx)),
            )
            .chain(
                ws.vector_deletes
                    .iter()
                    .map(|(_, _, deleted_tx)| *deleted_tx),
            )
            .chain(ws.vector_moves.iter().map(|(_, _, _, tx)| *tx))
            .max()
    }

    fn column_quantization(meta: Option<&TableMeta>, column_name: &str) -> VectorQuantization {
        meta.and_then(|meta| {
            meta.columns
                .iter()
                .find(|column| {
                    column.name == column_name
                        && matches!(column.column_type, ColumnType::Vector(_))
                })
                .map(|column| column.quantization)
        })
        .unwrap_or_default()
    }

    fn encode_versioned_row(row: &VersionedRow, meta: Option<&TableMeta>) -> Result<Vec<u8>> {
        let mut encoded = Vec::new();
        Self::encode_versioned_row_into(row, meta, &mut encoded)?;
        Ok(encoded)
    }

    fn encode_versioned_row_into(
        row: &VersionedRow,
        meta: Option<&TableMeta>,
        encoded: &mut Vec<u8>,
    ) -> Result<()> {
        Self::encode_versioned_row_with_deleted_tx_into(row, row.deleted_tx, meta, encoded)
    }

    fn encode_versioned_row_with_deleted_tx_into(
        row: &VersionedRow,
        deleted_tx: Option<TxId>,
        meta: Option<&TableMeta>,
        encoded: &mut Vec<u8>,
    ) -> Result<()> {
        let values = row
            .values
            .iter()
            .map(|(column, value)| {
                let persisted = match value {
                    Value::Vector(vector) => {
                        let quantization = Self::column_quantization(meta, column);
                        if matches!(quantization, VectorQuantization::F32) {
                            PersistedValue::Vector(PersistedVector::from_f32(vector, quantization))
                        } else {
                            PersistedValue::Plain(Value::Null)
                        }
                    }
                    _ => PersistedValue::Plain(value.clone()),
                };
                (column.clone(), persisted)
            })
            .collect::<HashMap<_, _>>();
        Self::encode_into(
            &PersistedVersionedRow {
                row_id: row.row_id,
                values,
                created_tx: row.created_tx,
                deleted_tx,
                lsn: row.lsn,
                created_at: row.created_at,
            },
            encoded,
        )
    }

    fn decode_versioned_row(bytes: &[u8], _meta: Option<&TableMeta>) -> Result<VersionedRow> {
        let persisted: PersistedVersionedRow = Self::decode(bytes)?;
        let values = persisted
            .values
            .into_iter()
            .map(|(column, value)| {
                let value = match value {
                    PersistedValue::Plain(value) => value,
                    PersistedValue::Vector(vector) => Value::Vector(vector.to_f32()),
                };
                (column, value)
            })
            .collect::<HashMap<_, _>>();
        Ok(VersionedRow {
            row_id: persisted.row_id,
            values,
            created_tx: persisted.created_tx,
            deleted_tx: persisted.deleted_tx,
            lsn: persisted.lsn,
            created_at: persisted.created_at,
        })
    }

    fn encode_vector_entry(
        entry: &VectorEntry,
        quantization: VectorQuantization,
    ) -> Result<Vec<u8>> {
        Self::encode(&PersistedVectorEntry {
            index: entry.index.clone(),
            row_id: entry.row_id,
            vector: PersistedVector::from_f32(&entry.vector, quantization),
            created_tx: entry.created_tx,
            deleted_tx: entry.deleted_tx,
            lsn: entry.lsn,
        })
    }

    fn decode_vector_entry(bytes: &[u8]) -> Result<VectorEntry> {
        let persisted: PersistedVectorEntry = Self::decode(bytes)?;
        Ok(VectorEntry {
            index: persisted.index,
            row_id: persisted.row_id,
            vector: persisted.vector.to_f32(),
            created_tx: persisted.created_tx,
            deleted_tx: persisted.deleted_tx,
            lsn: persisted.lsn,
        })
    }

    fn rel_table_name(name: &str) -> String {
        format!("rel_{name}")
    }

    fn sink_queue_table_name(sink: &str) -> String {
        format!("__sink_queue_{sink}")
    }

    fn sink_audit_key(lsn: Lsn, index: usize, sink: &str) -> String {
        format!("{:020}:{index:06}:{sink}", lsn.0)
    }

    fn trigger_audit_key(index: u64, trigger: &str) -> String {
        format!("{index:020}:{trigger}")
    }

    fn rel_row_key(row: &VersionedRow) -> Vec<u8> {
        Self::rel_row_key_from_parts(row.row_id, row.created_tx, row.lsn)
    }

    /// The same key `rel_row_key` derives from a `VersionedRow`, built from
    /// the raw identity parts directly — for a scoped prune, which has the
    /// pruned version's `(row_id, created_tx, lsn)` in hand without needing
    /// the whole row.
    fn rel_row_key_from_parts(row_id: RowId, created_tx: TxId, lsn: Lsn) -> Vec<u8> {
        let mut key = Vec::with_capacity(24);
        key.extend_from_slice(&row_id.0.to_be_bytes());
        key.extend_from_slice(&created_tx.0.to_be_bytes());
        key.extend_from_slice(&lsn.0.to_be_bytes());
        key
    }

    fn sync_row_source_lsn_key(table: &str, row_id: RowId) -> Vec<u8> {
        let mut key = Vec::with_capacity(table.len() + 8);
        key.extend_from_slice(table.as_bytes());
        key.extend_from_slice(&row_id.0.to_be_bytes());
        key
    }

    fn decode_sync_row_source_lsn_key(key: &[u8]) -> Option<(String, RowId)> {
        let split = key.len().checked_sub(8)?;
        let (table, row_id) = key.split_at(split);
        let table = String::from_utf8(table.to_vec()).ok()?;
        let row_id = RowId(u64::from_be_bytes(row_id.try_into().ok()?));
        Some((table, row_id))
    }

    fn remove_sync_source_provenance_for_table(
        write_txn: &redb::WriteTransaction,
        table: &str,
    ) -> Result<()> {
        let mut source_lsn_table = write_txn
            .open_table(SYNC_ROW_SOURCE_LSN_TABLE)
            .map_err(Self::storage_error)?;
        let lsn_keys_to_remove =
            Self::sync_source_lsn_keys_for_table(&source_lsn_table, table, |_| true)?;
        let mut source_kind_table = write_txn
            .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
            .map_err(Self::storage_error)?;
        let kind_keys_to_remove =
            Self::sync_source_kind_keys_for_table(&source_kind_table, table, |_| true)?;
        for key in lsn_keys_to_remove {
            source_lsn_table
                .remove(key.as_slice())
                .map_err(Self::storage_error)?;
        }
        for key in kind_keys_to_remove {
            source_kind_table
                .remove(key.as_slice())
                .map_err(Self::storage_error)?;
        }
        Ok(())
    }

    fn retain_sync_source_provenance_for_table_rows(
        write_txn: &redb::WriteTransaction,
        table: &str,
        rows: &[VersionedRow],
    ) -> Result<()> {
        let live_row_ids = rows
            .iter()
            .filter(|row| row.deleted_tx.is_none())
            .map(|row| row.row_id)
            .collect::<HashSet<_>>();
        let mut source_lsn_table = write_txn
            .open_table(SYNC_ROW_SOURCE_LSN_TABLE)
            .map_err(Self::storage_error)?;
        let lsn_keys_to_remove =
            Self::sync_source_lsn_keys_for_table(&source_lsn_table, table, |row_id| {
                !live_row_ids.contains(&row_id)
            })?;
        let mut source_kind_table = write_txn
            .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
            .map_err(Self::storage_error)?;
        let kind_keys_to_remove =
            Self::sync_source_kind_keys_for_table(&source_kind_table, table, |row_id| {
                !live_row_ids.contains(&row_id)
            })?;
        for key in lsn_keys_to_remove {
            source_lsn_table
                .remove(key.as_slice())
                .map_err(Self::storage_error)?;
        }
        for key in kind_keys_to_remove {
            source_kind_table
                .remove(key.as_slice())
                .map_err(Self::storage_error)?;
        }
        Ok(())
    }

    fn sync_source_lsn_keys_for_table(
        source_lsn_table: &impl ReadableTable<&'static [u8], u64>,
        table: &str,
        should_remove: impl Fn(RowId) -> bool,
    ) -> Result<Vec<Vec<u8>>> {
        let mut keys = Vec::new();
        for entry in source_lsn_table.iter().map_err(Self::storage_error)? {
            let (key, _) = entry.map_err(Self::storage_error)?;
            if let Some((entry_table, row_id)) = Self::decode_sync_row_source_lsn_key(key.value())
                && entry_table == table
                && should_remove(row_id)
            {
                keys.push(key.value().to_vec());
            }
        }
        Ok(keys)
    }

    fn sync_source_kind_keys_for_table(
        source_kind_table: &impl ReadableTable<&'static [u8], u8>,
        table: &str,
        should_remove: impl Fn(RowId) -> bool,
    ) -> Result<Vec<Vec<u8>>> {
        let mut keys = Vec::new();
        for entry in source_kind_table.iter().map_err(Self::storage_error)? {
            let (key, _) = entry.map_err(Self::storage_error)?;
            if let Some((entry_table, row_id)) = Self::decode_sync_row_source_lsn_key(key.value())
                && entry_table == table
                && should_remove(row_id)
            {
                keys.push(key.value().to_vec());
            }
        }
        Ok(keys)
    }

    fn rel_row_key_range(row_id: RowId) -> ([u8; 8], [u8; 8]) {
        let lower = row_id.0.to_be_bytes();
        let upper = row_id.0.saturating_add(1).to_be_bytes();
        (lower, upper)
    }

    fn meta_key(name: &str) -> String {
        format!("table:{name}")
    }

    fn change_log_entry_key_len() -> usize {
        20 + 1 + 6
    }

    fn write_change_log_entry_key(lsn: Lsn, index: usize, key: &mut String) {
        key.clear();
        write!(key, "{:020}:{index:06}", lsn.0).expect("writing change-log key to String");
    }

    fn ddl_log_key(lsn: Lsn) -> String {
        format!("{:020}", lsn.0)
    }

    fn ddl_log_key_for_index(lsn: Lsn, index: usize, count: usize) -> String {
        if count <= 1 {
            Self::ddl_log_key(lsn)
        } else {
            format!("{:020}:{index:06}", lsn.0)
        }
    }

    fn graph_fwd_key(entry: &AdjEntry) -> Vec<u8> {
        let mut key = Vec::with_capacity(48 + entry.edge_type.len());
        key.extend_from_slice(entry.source.as_bytes());
        key.extend_from_slice(entry.target.as_bytes());
        key.extend_from_slice(entry.edge_type.as_bytes());
        key.push(0);
        key.extend_from_slice(&entry.created_tx.0.to_be_bytes());
        key.extend_from_slice(&entry.lsn.0.to_be_bytes());
        key
    }

    fn graph_rev_key(entry: &AdjEntry) -> Vec<u8> {
        let mut key = Vec::with_capacity(48 + entry.edge_type.len());
        key.extend_from_slice(entry.target.as_bytes());
        key.extend_from_slice(entry.source.as_bytes());
        key.extend_from_slice(entry.edge_type.as_bytes());
        key.push(0);
        key.extend_from_slice(&entry.created_tx.0.to_be_bytes());
        key.extend_from_slice(&entry.lsn.0.to_be_bytes());
        key
    }

    fn vector_key(entry: &VectorEntry) -> Vec<u8> {
        let mut key = Vec::with_capacity(entry.index.table.len() + entry.index.column.len() + 34);
        key.extend_from_slice(entry.index.table.as_bytes());
        key.push(0);
        key.extend_from_slice(entry.index.column.as_bytes());
        key.push(0);
        key.extend_from_slice(&entry.row_id.0.to_be_bytes());
        key.extend_from_slice(&entry.created_tx.0.to_be_bytes());
        key.extend_from_slice(&entry.lsn.0.to_be_bytes());
        key
    }

    fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
        bincode::serde::encode_to_vec(value, bincode::config::standard())
            .map_err(|err| Error::Other(format!("bincode encode error: {err}")))
    }

    fn encode_into<T: serde::Serialize>(value: &T, encoded: &mut Vec<u8>) -> Result<()> {
        encoded.clear();
        bincode::serde::encode_into_std_write(value, encoded, bincode::config::standard())
            .map(|_| ())
            .map_err(|err| Error::Other(format!("bincode encode error: {err}")))
    }

    fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        let (value, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map_err(|err| Error::Other(format!("bincode decode error: {err}")))?;
        Ok(value)
    }

    fn decode_exact<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        let (value, consumed) =
            bincode::serde::decode_from_slice(bytes, bincode::config::standard())
                .map_err(|err| Error::Other(format!("bincode decode error: {err}")))?;
        if consumed != bytes.len() {
            return Err(Error::Other(
                "bincode exact decode left trailing bytes".to_string(),
            ));
        }
        Ok(value)
    }

    /// Decode one `TableMeta` blob, trying the CURRENT layout first and
    /// falling back to the exact `v1.0.0` layout on the SPECIFIC decode
    /// failure that layout produces. Returns whether the legacy fallback
    /// fired, so callers can refuse (`Database::open`) or proceed
    /// (`contextdb migrate`) accordingly.
    ///
    /// bincode's struct-as-tuple encoding carries no field-count marker, so
    /// a `Vec<ColumnDef>`/`TableMeta` decoder that optimistically reads past
    /// its OWN declared trailing fields does not cleanly stop at "no more
    /// fields for me" — it keeps consuming bytes that actually belong to the
    /// NEXT `ColumnDef` (or the next `TableMeta` field), and only surfaces an
    /// error once one of those borrowed bytes fails to satisfy the type it's
    /// forced into (observed as `InvalidBooleanValue` when a borrowed length
    /// or variant-tag byte lands on a `bool` field). The current
    /// `TableMeta`/`ColumnDef` `Deserialize` impls' own trailing-field
    /// `unwrap_or_default()` therefore only protects a genuinely SHORTER
    /// CURRENT-shaped payload (e.g. one written before the most recent
    /// additive field) — never a payload from a release whose `ColumnDef`
    /// had fewer fields than today's, which is exactly the `v1.0.0` case
    /// this fallback exists for.
    fn decode_table_meta_versioned(bytes: &[u8]) -> Result<(TableMeta, bool)> {
        match Self::decode::<TableMeta>(bytes) {
            Ok(meta) => Ok((meta, false)),
            Err(_) => {
                let legacy: LegacyTableMetaV1 = Self::decode(bytes)?;
                Ok((legacy.into(), true))
            }
        }
    }

    pub(crate) fn storage_error(err: impl std::fmt::Display) -> Error {
        let msg = err.to_string();
        if msg.contains("lock") || msg.contains("already open") {
            Error::Other(format!(
                "database is locked (another process may have it open): {msg}"
            ))
        } else {
            Error::Other(format!("redb error: {msg}"))
        }
    }

    fn storage_open_error(path: &Path, lock_file: &File, err: impl std::fmt::Display) -> Error {
        let msg = err.to_string();
        if msg.contains("already open") {
            Self::locked_database_error(path, lock_file)
        } else {
            Self::storage_error(msg)
        }
    }

    fn locked_database_error(path: &Path, lock_file: &File) -> Error {
        let holder_pid = active_file_lock_owner_pid(path)
            .or_else(|| {
                let mut lock_file = lock_file.try_clone().ok()?;
                read_lock_pid_for_locked_file(&mut lock_file)
            })
            .unwrap_or(0);
        Error::DatabaseLocked {
            holder_pid,
            path: path.to_path_buf(),
        }
    }

    pub(crate) fn with_db<T>(&self, f: impl FnOnce(&redb::Database) -> Result<T>) -> Result<T> {
        let lock_guard = self.lock_file.lock().expect("pid lock mutex poisoned");
        if lock_guard.is_none() {
            return Err(Error::Other("database persistence is closed".to_string()));
        }
        let db_guard = self.db.lock().expect("redb mutex poisoned");
        let db = db_guard
            .as_ref()
            .ok_or_else(|| Error::Other("database persistence is closed".to_string()))?;
        f(db)
    }
}

impl Drop for RedbPersistence {
    fn drop(&mut self) {
        let db = self.db.lock().expect("redb mutex poisoned").take();
        drop(db);
        if let Some(file) = self
            .lock_file
            .lock()
            .expect("pid lock mutex poisoned")
            .take()
        {
            drop(file);
            Self::release_pid_lock(&self.path);
        }
    }
}

fn read_lock_pid(file: &mut File) -> Option<u32> {
    let mut contents = String::new();
    file.seek(SeekFrom::Start(0)).ok()?;
    file.read_to_string(&mut contents).ok()?;
    contents.trim().parse::<u32>().ok()
}

fn read_lock_pid_for_locked_file(file: &mut File) -> Option<u32> {
    #[cfg(target_os = "linux")]
    {
        if let Some(pid) = active_lock_owner_pid(file) {
            return Some(pid);
        }
    }
    read_lock_pid_after_holder_settles(file)
}

fn read_lock_pid_after_holder_settles(file: &mut File) -> Option<u32> {
    let first = read_lock_pid(file);
    let mut latest = first;
    for _ in 0..50 {
        std::thread::sleep(std::time::Duration::from_millis(1));
        let current = read_lock_pid(file);
        if current.is_some() {
            latest = current;
        }
        if current.is_some() && current != first {
            return current;
        }
    }
    latest
}

#[cfg(target_os = "linux")]
fn active_lock_owner_pid(file: &File) -> Option<u32> {
    let metadata = file.metadata().ok()?;
    let (dev_major, dev_minor) = linux_dev_major_minor(metadata.dev());
    let inode = metadata.ino();
    let locks = std::fs::read_to_string("/proc/locks").ok()?;
    for line in locks.lines() {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields.len() < 6 || fields[3] != "WRITE" {
            continue;
        }
        let Ok(pid) = fields[4].parse::<u32>() else {
            continue;
        };
        let Some((lock_major, lock_minor, lock_inode)) = parse_proc_lock_device_inode(fields[5])
        else {
            continue;
        };
        if lock_major == dev_major && lock_minor == dev_minor && lock_inode == inode {
            return Some(pid);
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn active_file_lock_owner_pid(path: &Path) -> Option<u32> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .ok()?;
    active_lock_owner_pid(&file)
}

#[cfg(not(target_os = "linux"))]
fn active_file_lock_owner_pid(_path: &Path) -> Option<u32> {
    None
}

#[cfg(target_os = "linux")]
fn parse_proc_lock_device_inode(value: &str) -> Option<(u64, u64, u64)> {
    let mut parts = value.split(':');
    let major = u64::from_str_radix(parts.next()?, 16).ok()?;
    let minor = u64::from_str_radix(parts.next()?, 16).ok()?;
    let inode = parts.next()?.parse::<u64>().ok()?;
    Some((major, minor, inode))
}

#[cfg(target_os = "linux")]
fn linux_dev_major_minor(dev: u64) -> (u64, u64) {
    let major = ((dev >> 8) & 0xfff) | ((dev >> 32) & !0xfff);
    let minor = (dev & 0xff) | ((dev >> 12) & !0xff);
    (major, minor)
}

fn try_lock_exclusive(file: &File) -> Result<bool> {
    match fs2::FileExt::try_lock_exclusive(file) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => Ok(false),
        Err(err) => Err(Error::Other(format!("pid lock error: {err}"))),
    }
}

fn unlock_file(file: &File) {
    let _ = fs2::FileExt::unlock(file);
}
