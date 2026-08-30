use contextdb_core::read_contract::{
    CursorExpiryKind, DeadlineClock, DeadlineWait, ReadFailureDetail, ReadFailureKind,
    ReadFailureLimit, ReadLimits,
};
use contextdb_core::{
    Direction, Error, HistoryPolicy, IndexKind, Lsn, PropagationRule, RetainUnit, ScopeLabelKind,
    SortDirection, SyncDirection, TableMeta, TxId, Value, VectorQuantization,
};
use contextdb_engine::direct_file_reader::test_seams::{
    CancellationOperation, DirectCancellation, DirectChangeState, DirectColumnReference,
    DirectConfigurationState, DirectCursorPage, DirectCursorUnavailable, DirectEventTypeStatus,
    DirectEventsStatus, DirectFileReader, DirectFileReaderError, DirectImageMetadataKind,
    DirectImageState, DirectIndexColumn, DirectIndexDirection, DirectMaintenanceStatus,
    DirectMetadataBody, DirectMetadataRequest, DirectMetadataResponse, DirectOwnedImage,
    DirectPropagationRule, DirectRankPolicy, DirectReaderConfig, DirectReaderCounters,
    DirectReferencePropagation, DirectRepairRequiredCause, DirectRetainPolicy, DirectRouteStatus,
    DirectScheduleStatus, DirectSchema, DirectSchemaColumn, DirectSchemaIndex,
    DirectScopeLabelKind, DirectSinkStatus, DirectSnapshot, DirectStateMachine,
    DirectStoreDiagnostic, DirectStoredEdge, DirectStoredRow, DirectStoredVector, DirectSyncSource,
    DirectSyncState, DirectTableSyncPolicy, DirectTypedImageDigest, DirectVectorQuantization,
    HydrationCheckpoint, HydrationObserver, cancellation_observations_for_test,
    canonical_metadata_bytes_for_test, counters_for_test, cursor_resource_witness_for_test,
    open_for_test, open_with_hydration_observer_for_test, owned_image_for_test,
    reset_counters_for_test, verified_image_digest_for_test, verified_image_digest_wire_for_test,
};
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    DurableStoreDamage, ReadImageSourceEvent, arm_read_image_hydration_pause_for_test,
    prepare_durable_store_damage_for_test, read_image_source_events_for_test,
    reset_read_image_source_events_for_test,
};
use contextdb_engine::read_contract::{encode_cursor_page, encode_query_result};
use contextdb_engine::{Database, MaintenancePolicy};
use contextdb_vector::stored_vector_value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::OsString;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use syn::visit::{self, Visit};
use syn::{Fields, ImplItem, Item, ItemImpl, Type, Visibility};
use uuid::Uuid;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

const CHILD_ROLE_ENV: &str = "CONTEXTDB_DIRECT_FILE_CHILD_ROLE";
const CHILD_PATH_ENV: &str = "CONTEXTDB_DIRECT_FILE_CHILD_PATH";
const CHILD_RUNTIME_ENV: &str = "CONTEXTDB_DIRECT_FILE_CHILD_RUNTIME";
const CHILD_SQL_ENV: &str = "CONTEXTDB_DIRECT_FILE_CHILD_SQL";
const CHILD_ID_ENV: &str = "CONTEXTDB_DIRECT_FILE_CHILD_ID";
const CHILD_PAYLOAD_ENV: &str = "CONTEXTDB_DIRECT_FILE_CHILD_PAYLOAD";
const CHILD_DAMAGE_ENV: &str = "CONTEXTDB_DIRECT_FILE_CHILD_DAMAGE";
const CHILD_HYDRATION_STARTED: &str = "READ_IMAGE_HYDRATION_HELD=";
const CHILD_RELEASE_SEALED: &str = "DIRECT_FILE_RELEASE_SEALED";
const CHILD_TARGET_CONSTRUCTED: &str = "DIRECT_FILE_TARGET_CONSTRUCTED";
const CHILD_EXTERNALLY_USABLE: &str = "DIRECT_FILE_EXTERNALLY_USABLE";
const CHILD_SESSION_ACTIVE: &str = "DIRECT_FILE_SESSION_ACTIVE";
const CHILD_TYPED_DIGEST: &str = "DIRECT_FILE_TYPED_DIGEST=";
const CHILD_QUERY_OK: &str = "DIRECT_FILE_SESSION_QUERY_OK";
const CHILD_FINISH_HYDRATION: &str = "release";
const CHILD_CONSTRUCT_TARGET: &str = "construct-target";
const CHILD_PUBLISH: &str = "publish";
const CHILD_QUERY: &str = "query";
const CHILD_CLOSE: &str = "close";

static DIRECT_FILE_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();
static DIRECT_FILE_FIXTURE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn serialise_direct_file_test() -> MutexGuard<'static, ()> {
    DIRECT_FILE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Default)]
struct ManualClock {
    now: AtomicU64,
}

impl ManualClock {
    fn advance(&self, delta_ms: u64) {
        self.now.fetch_add(delta_ms, Ordering::SeqCst);
    }
}

impl DeadlineClock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn generous_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 10_000,
        result_bytes: 8 * 1024 * 1024,
        work: 1_000_000,
        active_ms: 10_000,
        memory: 32 * 1024 * 1024,
        cursor_page_rows: 2,
        cursor_page_bytes: 2 * 1024 * 1024,
        cursor_idle_ms: 10_000,
        cursor_lifetime_ms: 20_000,
    }
}

fn reader_config(
    clock: Arc<ManualClock>,
    runtime_directory: &Path,
    limits: ReadLimits,
) -> DirectReaderConfig {
    DirectReaderConfig::new(limits, clock, runtime_directory.to_path_buf())
}

fn default_reader_config(clock: Arc<ManualClock>, runtime_directory: &Path) -> DirectReaderConfig {
    reader_config(clock, runtime_directory, generous_limits())
}

#[derive(Default)]
struct RecordingObserver {
    checkpoints: Mutex<Vec<HydrationCheckpoint>>,
}

impl HydrationObserver for RecordingObserver {
    fn checkpoint(&self, checkpoint: HydrationCheckpoint) {
        match &checkpoint {
            HydrationCheckpoint::PersistenceReleaseSealed { breadcrumb_path }
            | HydrationCheckpoint::BackendTargetConstructed { breadcrumb_path }
            | HydrationCheckpoint::ExternallyUsable { breadcrumb_path } => {
                if let Some(path) = breadcrumb_path {
                    assert!(
                        fs::symlink_metadata(path).is_err(),
                        "the exact persistence breadcrumb is absent before publication"
                    );
                }
            }
        }
        self.checkpoints
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(checkpoint);
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ExpectedQuery {
    label: String,
    sql: String,
    params: HashMap<String, Value>,
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone)]
struct FixtureRow {
    id: Uuid,
    ordinal: i64,
    payload: String,
    status: String,
    note: String,
    parent_id: Uuid,
    immutable_tag: String,
    work_ledger_blob_identity: String,
    reference_like_scalar: String,
    f32: Vec<f32>,
    sq8: Vec<f32>,
    sq4: Vec<f32>,
}

#[derive(Clone)]
struct Fixture {
    identity_seed: String,
    token: String,
    root: PathBuf,
    runtime_directory: PathBuf,
    path: PathBuf,
    parent_table: String,
    outcomes_table: String,
    rows_table: String,
    edges_table: String,
    rows: Vec<FixtureRow>,
    queries: Vec<ExpectedQuery>,
    cursor_sql: String,
    cursor_columns: Vec<String>,
    cursor_rows: Vec<Vec<Value>>,
    schemas: Vec<DirectSchema>,
    changes: DirectChangeState,
    sync: DirectSyncState,
    configuration: DirectConfigurationState,
    events: DirectEventsStatus,
    maintenance: DirectMaintenanceStatus,
    expected_relational_rows: Vec<DirectStoredRow>,
    expected_graph_edges: Vec<DirectStoredEdge>,
    expected_vectors: Vec<DirectStoredVector>,
    explain_sql: String,
    explain_physical_plan: String,
    explain_index: Option<String>,
}

impl Fixture {
    fn first_row(&self) -> &FixtureRow {
        &self.rows[0]
    }

    fn relation_sql(&self) -> String {
        format!(
            "SELECT id, payload, work_ledger_blob_identity, reference_like_scalar FROM {} WHERE id = $id",
            self.rows_table
        )
    }

    fn relation_params(&self) -> HashMap<String, Value> {
        HashMap::from([("id".to_owned(), Value::Uuid(self.first_row().id))])
    }
}

fn seeded_uuid(identity_seed: &str, parts: &[&str]) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"contextdb-direct-file-fixture-v2");
    for component in std::iter::once(identity_seed).chain(parts.iter().copied()) {
        hasher.update(&(component.len() as u64).to_le_bytes());
        hasher.update(component.as_bytes());
    }
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn runtime_fixture_seed(root: &Path, label: &str) -> String {
    let sequence = DIRECT_FILE_FIXTURE_SEQUENCE.fetch_add(1, Ordering::SeqCst);
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"contextdb-direct-file-runtime-fixture-seed-v1");
    hasher.update(root.as_os_str().as_encoded_bytes());
    hasher.update(&std::process::id().to_le_bytes());
    hasher.update(&sequence.to_le_bytes());
    hasher.update(label.as_bytes());
    let seed = hasher.finalize().to_hex().to_string();
    eprintln!("DIRECT_FILE_FIXTURE_SEED {label}={seed}");
    seed
}

fn dynamic_vector(seed: u128, dimension: usize, ordinal: usize) -> Vec<f32> {
    (0..dimension)
        .map(|index| {
            let shift = ((index * 11 + ordinal * 7) % 96) as u32;
            let base = ((seed >> shift) & 0x1f) as f32 / 31.0 - 0.5;
            if index == ordinal % dimension {
                base + 1.5
            } else {
                base
            }
        })
        .collect()
}

fn direct_quantization(quantization: VectorQuantization) -> DirectVectorQuantization {
    match quantization {
        VectorQuantization::F32 => DirectVectorQuantization::F32,
        VectorQuantization::SQ8 => DirectVectorQuantization::Sq8,
        VectorQuantization::SQ4 => DirectVectorQuantization::Sq4,
    }
}

fn sync_direction_word(direction: SyncDirection) -> String {
    match direction {
        SyncDirection::None => "sync_off",
        SyncDirection::Push => "push_only",
        SyncDirection::Pull => "pull_only",
        SyncDirection::Both => "two_way",
    }
    .to_owned()
}

fn conflict_policy_word(meta: &TableMeta) -> Option<String> {
    meta.conflict_policy
        .and_then(|policy| policy.declared_clause())
        .map(|clause| match clause {
            "SYNC CONFLICT KEEP FIRST" => "keep_first".to_owned(),
            "SYNC CONFLICT KEEP LATEST" => "keep_latest".to_owned(),
            other => panic!("unexpected declared conflict clause: {other}"),
        })
}

fn propagation_sort_key(rule: &DirectPropagationRule) -> (u8, String, String) {
    match rule {
        DirectPropagationRule::Edge {
            edge_type,
            on_state,
            ..
        } => (0, on_state.clone(), edge_type.clone()),
        DirectPropagationRule::ForeignKey {
            column, on_state, ..
        } => (1, on_state.clone(), column.clone()),
        DirectPropagationRule::VectorExclusion { on_state } => (2, on_state.clone(), String::new()),
    }
}

fn schema_from_meta(table: &str, meta: &TableMeta) -> DirectSchema {
    let columns = meta
        .columns
        .iter()
        .map(|column| {
            let propagation = meta.propagation_rules.iter().find_map(|rule| match rule {
                PropagationRule::ForeignKey {
                    fk_column,
                    trigger_state,
                    target_state,
                    max_depth,
                    abort_on_failure,
                    ..
                } if fk_column == &column.name => Some(DirectReferencePropagation {
                    on_state: trigger_state.clone(),
                    set_state: target_state.clone(),
                    max_depth: *max_depth,
                    abort_on_failure: *abort_on_failure,
                }),
                _ => None,
            });
            DirectSchemaColumn {
                name: column.name.clone(),
                data_type: contextdb_engine::cli_render::render_column_type(&column.column_type),
                nullable: column.nullable,
                primary_key: column.primary_key,
                unique: column.unique,
                immutable: column.immutable,
                expires: column.expires,
                default: column.default.clone(),
                references: column
                    .references
                    .as_ref()
                    .map(|reference| DirectColumnReference {
                        table: reference.table.clone(),
                        column: reference.column.clone(),
                        propagation,
                    }),
                quantization: matches!(column.column_type, contextdb_core::ColumnType::Vector(_))
                    .then(|| direct_quantization(column.quantization)),
                rank: column.rank_policy.as_ref().map(|rank| DirectRankPolicy {
                    sort_key: rank.sort_key.clone(),
                    formula: rank.formula.clone(),
                    joined_table: rank.joined_table.clone(),
                    joined_column: rank.joined_column.clone(),
                }),
                scope_label: column.scope_label.as_ref().map(|scope| match scope {
                    ScopeLabelKind::Simple { write_labels } => DirectScopeLabelKind::Simple {
                        write_labels: write_labels.clone(),
                    },
                    ScopeLabelKind::Split {
                        read_labels,
                        write_labels,
                    } => DirectScopeLabelKind::Split {
                        read_labels: read_labels.clone(),
                        write_labels: write_labels.clone(),
                    },
                }),
                acl_ref: column.acl_ref.as_ref().map(|acl| DirectColumnReference {
                    table: acl.ref_table.clone(),
                    column: acl.ref_column.clone(),
                    propagation: None,
                }),
            }
        })
        .collect::<Vec<_>>();
    let primary_key = if meta.primary_key_columns.is_empty() {
        meta.columns
            .iter()
            .filter(|column| column.primary_key)
            .map(|column| column.name.clone())
            .collect()
    } else {
        meta.primary_key_columns.clone()
    };
    let indexes = meta
        .indexes
        .iter()
        .filter(|index| index.kind != IndexKind::Auto)
        .map(|index| DirectSchemaIndex {
            name: index.name.clone(),
            columns: index
                .columns
                .iter()
                .map(|(column, direction)| DirectIndexColumn {
                    column: column.clone(),
                    direction: match direction {
                        SortDirection::Asc => DirectIndexDirection::Asc,
                        SortDirection::Desc => DirectIndexDirection::Desc,
                    },
                })
                .collect(),
        })
        .collect();
    let state_machine = meta.state_machine.as_ref().map(|state| DirectStateMachine {
        column: state.column.clone(),
        transitions: state
            .transitions
            .iter()
            .map(|(from, to)| (from.clone(), to.clone()))
            .collect(),
    });
    let retain = meta.default_ttl_seconds.map(|seconds| {
        let (window, unit) = match meta.retain_declared_unit {
            Some(unit) => (seconds / unit.seconds_multiplier(), unit.sql().to_owned()),
            None => (seconds, RetainUnit::Seconds.sql().to_owned()),
        };
        DirectRetainPolicy {
            window,
            unit,
            seconds,
            sync_safe: meta.sync_safe,
        }
    });
    let history = meta.history_policy.map(|policy| match policy {
        HistoryPolicy::All => "ALL".to_owned(),
        HistoryPolicy::CurrentOnly => "CURRENT_ONLY".to_owned(),
    });
    let mut propagate = meta
        .propagation_rules
        .iter()
        .map(|rule| match rule {
            PropagationRule::Edge {
                edge_type,
                direction,
                trigger_state,
                target_state,
                max_depth,
                abort_on_failure,
            } => DirectPropagationRule::Edge {
                edge_type: edge_type.clone(),
                direction: match direction {
                    Direction::Incoming => "INCOMING",
                    Direction::Outgoing => "OUTGOING",
                    Direction::Both => "BOTH",
                }
                .to_owned(),
                on_state: trigger_state.clone(),
                set_state: target_state.clone(),
                max_depth: *max_depth,
                abort_on_failure: *abort_on_failure,
            },
            PropagationRule::VectorExclusion { trigger_state } => {
                DirectPropagationRule::VectorExclusion {
                    on_state: trigger_state.clone(),
                }
            }
            PropagationRule::ForeignKey {
                fk_column,
                referenced_table,
                referenced_column,
                trigger_state,
                target_state,
                max_depth,
                abort_on_failure,
            } => DirectPropagationRule::ForeignKey {
                column: fk_column.clone(),
                references_table: referenced_table.clone(),
                references_column: referenced_column.clone(),
                on_state: trigger_state.clone(),
                set_state: target_state.clone(),
                max_depth: *max_depth,
                abort_on_failure: *abort_on_failure,
            },
        })
        .collect::<Vec<_>>();
    propagate.sort_by_key(propagation_sort_key);
    DirectSchema {
        table: table.to_owned(),
        immutable: meta.immutable,
        columns,
        primary_key,
        indexes,
        state_machine,
        retain,
        history,
        sync_direction: meta.sync_direction.map(sync_direction_word),
        conflict_policy: conflict_policy_word(meta),
        dag_edge_types: meta.dag_edge_types.clone(),
        propagate,
        ddl: contextdb_engine::cli_render::render_table_meta(table, meta),
    }
}

fn execute_mutation(
    db: &Database,
    sql: &str,
    params: &HashMap<String, Value>,
    commit_index: &mut Vec<(Lsn, TxId)>,
) {
    db.execute(sql, params)
        .unwrap_or_else(|error| panic!("fixture mutation `{sql}`: {error}"));
    record_commit(db, commit_index);
}

fn record_commit(db: &Database, commit_index: &mut Vec<(Lsn, TxId)>) {
    let point = (db.current_lsn(), db.committed_watermark());
    if point.0 != Lsn(0) && commit_index.last().map(|last| last.0) != Some(point.0) {
        commit_index.push(point);
    }
}

fn capture_query(
    db: &Database,
    label: impl Into<String>,
    sql: String,
    params: HashMap<String, Value>,
) -> ExpectedQuery {
    let result = db
        .execute(&sql, &params)
        .unwrap_or_else(|error| panic!("capture fixture query `{sql}`: {error}"));
    ExpectedQuery {
        label: label.into(),
        sql,
        params,
        columns: result.columns,
        rows: result.rows,
    }
}

fn seed_fixture_from_seed(
    outer_root: &Path,
    extension: &str,
    identity_seed: &str,
    seed_writer_policy: Option<MaintenancePolicy>,
) -> Fixture {
    let nonce = seeded_uuid(identity_seed, &["fixture", extension]);
    let token = nonce.simple().to_string();
    let parent_table = format!("parents_{token}");
    let outcomes_table = format!("outcomes_{token}");
    let rows_table = format!("rows_{token}");
    let edges_table = format!("edges_{token}");
    let f32_column = format!("f32_{token}");
    let sq8_column = format!("sq8_{token}");
    let sq4_column = format!("sq4_{token}");
    let payload_index = format!("idx_payload_status_{token}");
    let outcome_index = format!("idx_outcome_decision_{token}");
    let edge_types = [
        format!("CITES_{token}").to_uppercase(),
        format!("BASED_ON_{token}").to_uppercase(),
    ];
    let event_type = format!("event_{token}");
    let sink = format!("sink_{token}");
    let route = format!("route_{token}");
    let schedule = format!("schedule_{token}");
    let callback = format!("callback_{token}");
    let root = outer_root.join(format!("source_{token}"));
    let runtime_directory = outer_root.join(format!("runtime_{token}"));
    fs::create_dir(&root).expect("create dynamic source directory");
    fs::create_dir(&runtime_directory).expect("create dynamic runtime directory");
    let nested = root.join(format!("nested_{token}"));
    fs::create_dir(&nested).expect("create recursive manifest directory");
    let identity_file = nested.join(format!("identity_{token}.txt"));
    fs::write(&identity_file, format!("identity:{token}:{extension}"))
        .expect("write recursive manifest identity");
    #[cfg(unix)]
    std::os::unix::fs::symlink(
        identity_file.file_name().expect("identity file name"),
        nested.join(format!("identity_link_{token}")),
    )
    .expect("create manifest link");
    let path = root.join(format!("store_{token}.{extension}"));
    let dimensions = [
        5 + ((nonce.as_u128() >> 13) as usize % 4),
        5 + (nonce.as_u128() as usize % 4),
        5 + ((nonce.as_u128() >> 7) as usize % 4),
    ];
    let parent_ids = [
        seeded_uuid(identity_seed, &[extension, "parent", "0"]),
        seeded_uuid(identity_seed, &[extension, "parent", "1"]),
    ];
    let rows = (0..3)
        .map(|ordinal| {
            let ordinal_text = ordinal.to_string();
            let id = seeded_uuid(identity_seed, &[extension, "row", &ordinal_text]);
            let payload_id = seeded_uuid(identity_seed, &[extension, "payload", &ordinal_text]);
            let reference_id = seeded_uuid(identity_seed, &[extension, "reference", &ordinal_text]);
            let repeated = if ordinal == 1 {
                65_537
            } else {
                19 + ordinal * 13
            };
            FixtureRow {
                id,
                ordinal: ordinal as i64,
                payload: format!(
                    "payload-{token}-{ordinal}-{}-{}",
                    payload_id.simple(),
                    "x".repeat(repeated)
                ),
                status: "active".to_owned(),
                note: format!("note-{token}-{ordinal}"),
                parent_id: parent_ids[ordinal % parent_ids.len()],
                immutable_tag: format!("immutable-{token}-{ordinal}"),
                work_ledger_blob_identity: format!(
                    "blob_ref:{}",
                    blake3::hash(format!("{token}:{id}:{ordinal}").as_bytes()).to_hex()
                ),
                reference_like_scalar: format!(
                    "reference-like://{}/{}",
                    reference_id.simple(),
                    ordinal
                ),
                f32: dynamic_vector(id.as_u128().rotate_left(37), dimensions[0], ordinal + 1),
                sq8: dynamic_vector(id.as_u128(), dimensions[1], ordinal + 2),
                sq4: dynamic_vector(id.as_u128().rotate_left(19), dimensions[2], ordinal + 3),
            }
        })
        .collect::<Vec<_>>();

    let parent_ddl = format!(
        "CREATE TABLE {parent_table} (id UUID PRIMARY KEY, status TEXT NOT NULL DEFAULT 'active', label TEXT UNIQUE) STATE MACHINE (status: active -> [archived, completed]) SYNC PULL ONLY SYNC CONFLICT KEEP FIRST"
    );
    let outcomes_ddl = format!(
        "CREATE TABLE {outcomes_table} (id UUID PRIMARY KEY, decision_id UUID, success REAL NOT NULL DEFAULT 1.0) HISTORY ALL SYNC OFF"
    );
    let rows_ddl = format!(
        "CREATE TABLE {rows_table} (\
         id UUID PRIMARY KEY, \
         ordinal INTEGER NOT NULL, \
         payload TEXT NOT NULL UNIQUE DEFAULT 'fallback-{token}', \
         status TEXT NOT NULL DEFAULT 'active', \
         note TEXT, \
         parent_id UUID REFERENCES {parent_table}(id) ON STATE archived PROPAGATE SET invalidated MAX DEPTH 3 ABORT ON FAILURE, \
         immutable_tag TEXT NOT NULL IMMUTABLE, \
         work_ledger_blob_identity TEXT NOT NULL, \
         reference_like_scalar TEXT, \
         {f32_column} VECTOR({}) RANK_POLICY (JOIN {outcomes_table} ON decision_id, FORMULA '{{vector_score}} * coalesce({{success}}, 1.0)', SORT_KEY weighted_{token}), \
         {sq8_column} VECTOR({}) WITH (quantization = 'SQ8'), \
         {sq4_column} VECTOR({}) WITH (quantization = 'SQ4')\
         ) STATE MACHINE (status: active -> [invalidated, superseded], invalidated -> [superseded]) \
         RETAIN 3 DAYS SYNC SAFE \
         HISTORY CURRENT ONLY \
         SYNC PUSH ONLY \
         SYNC CONFLICT KEEP LATEST \
         PROPAGATE ON EDGE {} INCOMING STATE invalidated SET invalidated MAX DEPTH 5 ABORT ON FAILURE \
         PROPAGATE ON STATE invalidated EXCLUDE VECTOR",
        dimensions[0], dimensions[1], dimensions[2], edge_types[0]
    );
    let edges_ddl = format!(
        "CREATE TABLE {edges_table} (id UUID PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL) DAG('{}', '{}')",
        edge_types[0], edge_types[1]
    );

    let db = Database::open(&path)
        .unwrap_or_else(|error| panic!("create dynamic fixture {}: {error}", path.display()));
    if let Some(policy) = seed_writer_policy {
        db.set_maintenance_policy(policy);
    }
    let mut commit_index = Vec::new();
    for ddl in [&parent_ddl, &outcomes_ddl] {
        execute_mutation(&db, ddl, &HashMap::new(), &mut commit_index);
    }
    execute_mutation(
        &db,
        &format!("CREATE INDEX {outcome_index} ON {outcomes_table} (decision_id ASC)"),
        &HashMap::new(),
        &mut commit_index,
    );
    for ddl in [&rows_ddl, &edges_ddl] {
        execute_mutation(&db, ddl, &HashMap::new(), &mut commit_index);
    }
    execute_mutation(
        &db,
        &format!("CREATE INDEX {payload_index} ON {rows_table} (payload DESC, status ASC)"),
        &HashMap::new(),
        &mut commit_index,
    );
    execute_mutation(
        &db,
        &format!("CREATE EVENT TYPE {event_type} WHEN INSERT ON {rows_table}"),
        &HashMap::new(),
        &mut commit_index,
    );
    execute_mutation(
        &db,
        &format!("CREATE SINK {sink} TYPE callback"),
        &HashMap::new(),
        &mut commit_index,
    );
    execute_mutation(
        &db,
        &format!("CREATE ROUTE {route} EVENT {event_type} TO {sink}"),
        &HashMap::new(),
        &mut commit_index,
    );
    for (ordinal, parent_id) in parent_ids.iter().copied().enumerate() {
        execute_mutation(
            &db,
            &format!(
                "INSERT INTO {parent_table} (id, status, label) VALUES ($id, $status, $label)"
            ),
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(parent_id)),
                ("status".to_owned(), Value::Text("active".to_owned())),
                (
                    "label".to_owned(),
                    Value::Text(format!("parent-{token}-{ordinal}")),
                ),
            ]),
            &mut commit_index,
        );
    }
    for (ordinal, row) in rows.iter().enumerate() {
        let outcome_id = seeded_uuid(identity_seed, &[extension, "outcome", &ordinal.to_string()]);
        execute_mutation(
            &db,
            &format!(
                "INSERT INTO {outcomes_table} (id, decision_id, success) VALUES ($id, $decision_id, $success)"
            ),
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(outcome_id)),
                ("decision_id".to_owned(), Value::Uuid(row.id)),
                (
                    "success".to_owned(),
                    Value::Float64((ordinal + 2) as f64 / 4.0),
                ),
            ]),
            &mut commit_index,
        );
        execute_mutation(
            &db,
            &format!(
                "INSERT INTO {rows_table} (id, ordinal, payload, status, note, parent_id, immutable_tag, work_ledger_blob_identity, reference_like_scalar, {f32_column}, {sq8_column}, {sq4_column}) \
                 VALUES ($id, $ordinal, $payload, $status, $note, $parent_id, $immutable_tag, $work_ledger_blob_identity, $reference_like_scalar, $f32, $sq8, $sq4)"
            ),
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(row.id)),
                ("ordinal".to_owned(), Value::Int64(row.ordinal)),
                ("payload".to_owned(), Value::Text(row.payload.clone())),
                ("status".to_owned(), Value::Text(row.status.clone())),
                ("note".to_owned(), Value::Text(row.note.clone())),
                ("parent_id".to_owned(), Value::Uuid(row.parent_id)),
                (
                    "immutable_tag".to_owned(),
                    Value::Text(row.immutable_tag.clone()),
                ),
                (
                    "work_ledger_blob_identity".to_owned(),
                    Value::Text(row.work_ledger_blob_identity.clone()),
                ),
                (
                    "reference_like_scalar".to_owned(),
                    Value::Text(row.reference_like_scalar.clone()),
                ),
                ("f32".to_owned(), Value::Vector(row.f32.clone())),
                ("sq8".to_owned(), Value::Vector(row.sq8.clone())),
                ("sq4".to_owned(), Value::Vector(row.sq4.clone())),
            ]),
            &mut commit_index,
        );
    }
    let graph_specs = [
        (rows[0].id, rows[1].id, edge_types[0].clone()),
        (rows[1].id, rows[2].id, edge_types[1].clone()),
    ];
    let mut expected_graph_edges = Vec::new();
    for (ordinal, (source, target, edge_type)) in graph_specs.iter().enumerate() {
        let edge_id = seeded_uuid(identity_seed, &[extension, "edge", &ordinal.to_string()]);
        execute_mutation(
            &db,
            &format!(
                "INSERT INTO {edges_table} (id, source_id, target_id, edge_type) VALUES ($id, $source, $target, $edge_type)"
            ),
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(edge_id)),
                ("source".to_owned(), Value::Uuid(*source)),
                ("target".to_owned(), Value::Uuid(*target)),
                ("edge_type".to_owned(), Value::Text(edge_type.clone())),
            ]),
            &mut commit_index,
        );
        let properties = HashMap::from([
            (
                format!("evidence_{token}"),
                Value::Text(format!("edge-evidence-{token}-{ordinal}")),
            ),
            (
                format!("weight_{token}"),
                Value::Float64((ordinal + 3) as f64 / 7.0),
            ),
        ]);
        let graph_tx = db.begin().expect("begin graph-property transaction");
        db.insert_edge(
            graph_tx,
            *source,
            *target,
            edge_type.clone(),
            properties.clone(),
        )
        .expect("insert graph edge with owned properties");
        db.commit(graph_tx)
            .expect("commit graph edge with owned properties");
        record_commit(&db, &mut commit_index);
        expected_graph_edges.push(DirectStoredEdge {
            source: *source,
            target: *target,
            edge_type: edge_type.clone(),
            properties: properties.into_iter().collect(),
        });
    }
    execute_mutation(
        &db,
        &format!("CREATE SCHEDULE {schedule} EVERY '7 HOURS' TX ({callback})"),
        &HashMap::new(),
        &mut commit_index,
    );
    let memory_limit = 96 * 1024 * 1024;
    let disk_limit = 512 * 1024 * 1024;
    db.set_memory_limit(Some(memory_limit))
        .expect("persist non-default memory limit");
    record_commit(&db, &mut commit_index);
    db.set_disk_limit(Some(disk_limit))
        .expect("persist non-default disk limit");
    record_commit(&db, &mut commit_index);

    let table_names = [
        parent_table.clone(),
        outcomes_table.clone(),
        rows_table.clone(),
        edges_table.clone(),
    ];
    let mut schemas = table_names
        .iter()
        .map(|table| {
            let meta = db
                .table_meta(table)
                .unwrap_or_else(|| panic!("fixture schema exists for {table}"));
            schema_from_meta(table, &meta)
        })
        .collect::<Vec<_>>();
    schemas.sort_by(|left, right| left.table.cmp(&right.table));

    let parent_query = capture_query(
        &db,
        "all parents",
        format!("SELECT id, status, label FROM {parent_table} ORDER BY id"),
        HashMap::new(),
    );
    let outcomes_query = capture_query(
        &db,
        "all rank outcomes",
        format!("SELECT id, decision_id, success FROM {outcomes_table} ORDER BY id"),
        HashMap::new(),
    );
    let rows_query = capture_query(
        &db,
        "all relational and vector values",
        format!(
            "SELECT id, ordinal, payload, status, note, parent_id, immutable_tag, work_ledger_blob_identity, reference_like_scalar, {f32_column}, {sq8_column}, {sq4_column} FROM {rows_table} ORDER BY ordinal"
        ),
        HashMap::new(),
    );
    let edges_query = capture_query(
        &db,
        "all graph-table rows",
        format!("SELECT id, source_id, target_id, edge_type FROM {edges_table} ORDER BY id"),
        HashMap::new(),
    );
    let graph_queries = graph_specs
        .iter()
        .enumerate()
        .map(|(ordinal, (source, _, edge_type))| {
            capture_query(
                &db,
                format!("graph edge {ordinal}"),
                format!(
                    "SELECT target FROM GRAPH_TABLE({edges_table} MATCH (a)-[:{edge_type}]->(b) WHERE a.id = $source COLUMNS (b.id AS target))"
                ),
                HashMap::from([("source".to_owned(), Value::Uuid(*source))]),
            )
        })
        .collect::<Vec<_>>();
    let vector_queries = [
        (&f32_column, rows[0].f32.clone()),
        (&sq8_column, rows[0].sq8.clone()),
        (&sq4_column, rows[0].sq4.clone()),
    ]
    .into_iter()
    .map(|(column, probe)| {
        capture_query(
            &db,
            format!("vector search {column}"),
            format!("SELECT id, {column} FROM {rows_table} ORDER BY {column} <=> $probe LIMIT 3"),
            HashMap::from([("probe".to_owned(), Value::Vector(probe))]),
        )
    })
    .collect::<Vec<_>>();
    let explain_sql = format!("SELECT id, payload FROM {rows_table} WHERE id = $id");
    let explain_result = db
        .execute(
            &explain_sql,
            &HashMap::from([("id".to_owned(), Value::Uuid(rows[0].id))]),
        )
        .expect("capture exact explain route");
    let explain_physical_plan = explain_result.trace.physical_plan.to_owned();
    let explain_index = explain_result.trace.index_used;

    let cursor_sql =
        format!("SELECT id, ordinal, payload, note FROM {rows_table} ORDER BY ordinal");
    let cursor_result = db
        .execute(&cursor_sql, &HashMap::new())
        .expect("capture exact cursor rows");
    let cursor_columns = cursor_result.columns;
    let cursor_rows = cursor_result.rows;

    let (changes, arrivals) = db.changes_since_with_arrivals(Lsn(0));
    let changes_state = DirectChangeState {
        current_lsn: db.current_lsn(),
        committed_watermark: db.committed_watermark(),
        // Filled in below from the store as it stands after the writer is
        // gone: the live counter has moved past its last durable trace, and a
        // reader answers from what the store kept.
        next_tx: TxId(0),
        // Observed from the store, not accumulated while writing it: an
        // expectation assembled from counters the fixture watched go by is an
        // expectation of the fixture's own bookkeeping, which no reader can
        // answer with.
        commit_index: db.commit_index_for_test(),
        rows: changes.rows.clone(),
        edges: changes.edges.clone(),
        vectors: changes.vectors.clone(),
        ddl: changes.ddl.clone(),
        ddl_lsn: changes.ddl_lsn.clone(),
    };
    let mut sync_sources = arrivals
        .into_iter()
        .map(|(row_lsn, source_lsn)| DirectSyncSource {
            row_lsn,
            source_lsn,
        })
        .collect::<Vec<_>>();
    sync_sources.sort_by_key(|source| source.row_lsn);
    let mut sync_tables = schemas
        .iter()
        .map(|schema| DirectTableSyncPolicy {
            table: schema.table.clone(),
            direction: schema
                .sync_direction
                .clone()
                .unwrap_or_else(|| "two_way".to_owned()),
            conflict_policy: schema
                .conflict_policy
                .clone()
                .unwrap_or_else(|| "keep_first".to_owned()),
        })
        .collect::<Vec<_>>();
    sync_tables.sort_by(|left, right| left.table.cmp(&right.table));
    let sync = DirectSyncState {
        watermark: db.sync_watermark(),
        sources: sync_sources,
        tables: sync_tables,
    };
    let mut expected_sync_sources = changes
        .rows
        .iter()
        .map(|row| DirectSyncSource {
            row_lsn: row.lsn,
            source_lsn: None,
        })
        .collect::<Vec<_>>();
    expected_sync_sources.sort_by_key(|source| source.row_lsn);
    expected_sync_sources.dedup_by_key(|source| source.row_lsn);
    assert_eq!(sync.sources, expected_sync_sources);
    assert_eq!(sync.tables.len(), 4);
    let configuration = DirectConfigurationState {
        memory_limit_bytes: db.memory_limit(),
        disk_limit_bytes: db.disk_limit(),
        schemas: schemas.clone(),
    };
    assert_eq!(configuration.memory_limit_bytes, Some(memory_limit));
    assert_eq!(configuration.disk_limit_bytes, Some(disk_limit));

    let event_bus = db.event_bus_status();
    let events = DirectEventsStatus {
        event_types: event_bus
            .event_types
            .into_iter()
            .map(|event| DirectEventTypeStatus {
                name: event.name,
                trigger: event.trigger,
                table: event.table,
            })
            .collect(),
        sinks: event_bus
            .sinks
            .into_iter()
            .map(|sink| DirectSinkStatus {
                name: sink.name,
                sink_type: sink.sink_type,
                callback_registered: sink.callback_registered,
                delivered: sink.metrics.delivered,
                queued: sink.metrics.queued,
                retried: sink.metrics.retried,
                permanent_failures: sink.metrics.permanent_failures,
                examined: sink.metrics.examined,
            })
            .collect(),
        routes: event_bus
            .routes
            .into_iter()
            .map(|route| DirectRouteStatus {
                name: route.name,
                event_type: route.event_type,
                sink: route.sink,
            })
            .collect(),
        schedules: db
            .cron_status()
            .into_iter()
            .map(|schedule| DirectScheduleStatus {
                name: schedule.name,
                every: schedule.every_text,
                callback: schedule.callback,
                callback_registered: schedule.callback_registered,
                next_fire_at_ms: schedule.next_fire_at_ms,
                last_fire_at_ms: schedule.last_fire_at_ms,
                fire_count: schedule.fire_count,
            })
            .collect(),
    };
    assert_eq!(
        events.event_types,
        vec![DirectEventTypeStatus {
            name: event_type.clone(),
            trigger: "INSERT".to_owned(),
            table: rows_table.clone(),
        }]
    );
    assert_eq!(
        events.sinks,
        vec![DirectSinkStatus {
            name: sink.clone(),
            sink_type: "CALLBACK".to_owned(),
            callback_registered: false,
            delivered: 0,
            queued: rows.len() as u64,
            retried: 0,
            permanent_failures: 0,
            examined: 0,
        }]
    );
    assert_eq!(
        events.routes,
        vec![DirectRouteStatus {
            name: route.clone(),
            event_type: event_type.clone(),
            sink: sink.clone(),
        }]
    );
    assert_eq!(events.schedules.len(), 1);
    assert_eq!(events.schedules[0].name, schedule);
    assert_eq!(events.schedules[0].callback, callback);
    assert_eq!(events.schedules[0].every, "7 HOURS");
    assert!(!events.schedules[0].callback_registered);
    assert_ne!(events.schedules[0].next_fire_at_ms, 0);
    assert_eq!(events.schedules[0].last_fire_at_ms, None);
    assert_eq!(events.schedules[0].fire_count, 0);
    let maintenance = DirectMaintenanceStatus {
        policy: "engine_owned".to_owned(),
        running: false,
        retention_enabled: true,
        currency_compaction_enabled: true,
        active_maintenance_loops: 0,
    };
    if let Some(policy) = seed_writer_policy {
        assert_eq!(db.maintenance_status().policy, policy);
    }

    let snapshot = db.snapshot();
    let mut expected_relational_rows = table_names
        .iter()
        .flat_map(|table| {
            db.scan(table, snapshot)
                .unwrap_or_else(|error| panic!("scan complete fixture table {table}: {error}"))
                .into_iter()
                .map(|row| DirectStoredRow {
                    table: table.clone(),
                    row_id: row.row_id,
                    values: row.values.into_iter().collect(),
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    expected_relational_rows
        .sort_by(|left, right| (&left.table, left.row_id).cmp(&(&right.table, right.row_id)));
    assert_eq!(expected_relational_rows.len(), 10);
    assert_eq!(expected_graph_edges.len(), 2);
    assert_eq!(expected_graph_edges[0].properties.len(), 2);
    assert_eq!(expected_graph_edges[1].properties.len(), 2);

    let vector_column_position = BTreeMap::from([
        (f32_column.clone(), 9_usize),
        (sq8_column.clone(), 10_usize),
        (sq4_column.clone(), 11_usize),
    ]);
    let mut expected_vectors = changes
        .vectors
        .iter()
        .map(|change| {
            let owner_id = expected_relational_rows
                .iter()
                .find(|row| row.table == rows_table && row.row_id == change.row_id)
                .and_then(|row| row.values.get("id"))
                .and_then(|value| match value {
                    Value::Uuid(id) => Some(*id),
                    _ => None,
                })
                .unwrap_or_else(|| {
                    panic!(
                        "vector change maps to a persisted owner row: {:?}",
                        change.index
                    )
                });
            let fixture_row = rows
                .iter()
                .find(|row| row.id == owner_id)
                .unwrap_or_else(|| {
                    panic!("vector change maps to a dynamic row: {:?}", change.index)
                });
            let result_row = rows_query
                .rows
                .iter()
                .find(|values| values[0] == Value::Uuid(fixture_row.id))
                .expect("captured row contains vector owner");
            let position = vector_column_position[&change.index.column];
            let Value::Vector(values) = &result_row[position] else {
                panic!("captured vector column is a vector")
            };
            DirectStoredVector {
                index: change.index.clone(),
                row_id: change.row_id,
                quantization: if change.index.column == f32_column {
                    DirectVectorQuantization::F32
                } else if change.index.column == sq8_column {
                    DirectVectorQuantization::Sq8
                } else {
                    DirectVectorQuantization::Sq4
                },
                values: values.clone(),
            }
        })
        .collect::<Vec<_>>();
    expected_vectors.sort_by(|left, right| {
        (&left.index.table, &left.index.column, left.row_id).cmp(&(
            &right.index.table,
            &right.index.column,
            right.row_id,
        ))
    });
    assert_eq!(expected_vectors.len(), rows.len() * 3);
    assert_eq!(
        expected_vectors
            .iter()
            .filter(|vector| vector.quantization == DirectVectorQuantization::F32)
            .count(),
        rows.len()
    );
    assert_eq!(
        expected_vectors
            .iter()
            .filter(|vector| vector.quantization == DirectVectorQuantization::Sq8)
            .count(),
        rows.len()
    );
    assert_eq!(
        expected_vectors
            .iter()
            .filter(|vector| vector.quantization == DirectVectorQuantization::Sq4)
            .count(),
        rows.len()
    );

    db.close()
        .unwrap_or_else(|error| panic!("close dynamic fixture: {error}"));

    // The expectation above was taken from a writer that was still open. A
    // reader answers a store whose writer is gone, so the fixture proves here
    // that this state survives the close unchanged -- otherwise it would be
    // asking readers for something only a live writer ever held.
    let reopened =
        Database::open(&path).unwrap_or_else(|error| panic!("reopen the fixture store: {error}"));
    let mut changes_state = changes_state;
    changes_state.next_tx = reopened.next_tx();
    assert_eq!(
        reopened.commit_index_for_test(),
        changes_state.commit_index,
        "the commit index a reader can see must be the one the writer reported"
    );
    assert_eq!(
        reopened.current_lsn(),
        changes_state.current_lsn,
        "the current LSN must survive the writer that reached it"
    );
    assert_eq!(
        reopened.committed_watermark(),
        changes_state.committed_watermark,
        "the committed watermark must survive the writer that committed it"
    );
    reopened
        .close()
        .unwrap_or_else(|error| panic!("close the reopened fixture store: {error}"));

    let mut queries = vec![parent_query, outcomes_query, rows_query, edges_query];
    queries.extend(graph_queries);
    queries.extend(vector_queries);
    Fixture {
        identity_seed: identity_seed.to_owned(),
        token,
        root,
        runtime_directory,
        path,
        parent_table,
        outcomes_table,
        rows_table,
        edges_table,
        rows,
        queries,
        cursor_sql,
        cursor_columns,
        cursor_rows,
        schemas,
        changes: changes_state,
        sync,
        configuration,
        events,
        maintenance,
        expected_relational_rows,
        expected_graph_edges,
        expected_vectors,
        explain_sql,
        explain_physical_plan,
        explain_index,
    }
}

fn seed_fixture_with_extension(outer_root: &Path, extension: &str, label: &str) -> Fixture {
    let identity_seed = runtime_fixture_seed(outer_root, &format!("{label}:{extension}"));
    seed_fixture_from_seed(outer_root, extension, &identity_seed, None)
}

fn seed_fixture(root: &Path, label: &str) -> Fixture {
    seed_fixture_with_extension(root, "db", label)
}

fn seed_fixture_after_caller_driven_writer(root: &Path) -> Fixture {
    let label = "caller-driven-writer-policy-is-handle-only";
    let identity_seed = runtime_fixture_seed(root, label);
    seed_fixture_from_seed(
        root,
        "db",
        &identity_seed,
        Some(MaintenancePolicy::CallerDriven),
    )
}

fn direct_open_with_limits(
    fixture: &Fixture,
    clock: Arc<ManualClock>,
    limits: ReadLimits,
) -> DirectFileReader {
    open_for_test(
        &fixture.path,
        reader_config(clock, &fixture.runtime_directory, limits),
    )
    .unwrap_or_else(|error| {
        panic!(
            "direct file route must hydrate the committed image for {}: {error}",
            fixture.path.display()
        )
    })
}

fn direct_open(fixture: &Fixture, clock: Arc<ManualClock>) -> DirectFileReader {
    direct_open_with_limits(fixture, clock, generous_limits())
}

fn assert_query(reader: &DirectFileReader, expected: &ExpectedQuery) -> (Vec<u8>, DirectSnapshot) {
    let result = reader
        .execute(&expected.sql, &expected.params)
        .unwrap_or_else(|error| panic!("execute {}: {error}", expected.label));
    assert_eq!(
        result.result.columns, expected.columns,
        "{} columns",
        expected.label
    );
    assert_eq!(result.result.rows, expected.rows, "{} rows", expected.label);
    assert_eq!(
        result.canonical_bytes,
        encode_query_result(&result.result)
            .expect("shared canonical encoder supplies ordinary-result bytes")
    );
    (result.canonical_bytes, result.snapshot)
}

fn expected_table_names(fixture: &Fixture) -> Vec<String> {
    let mut names = vec![
        fixture.parent_table.clone(),
        fixture.outcomes_table.clone(),
        fixture.rows_table.clone(),
        fixture.edges_table.clone(),
    ];
    names.sort();
    names
}

fn metadata_responses(fixture: &Fixture, reader: &DirectFileReader) -> Vec<DirectMetadataResponse> {
    let tables = reader
        .metadata(DirectMetadataRequest::Tables)
        .unwrap_or_else(|error| panic!("project tables: {error}"));
    assert_eq!(
        tables.body,
        DirectMetadataBody::Tables {
            items: expected_table_names(fixture),
            has_more: false,
        }
    );

    let mut responses = vec![tables];
    for expected in &fixture.schemas {
        let schema = reader
            .metadata(DirectMetadataRequest::Schema {
                table: expected.table.clone(),
            })
            .unwrap_or_else(|error| panic!("project schema {}: {error}", expected.table));
        assert_eq!(
            schema.body,
            DirectMetadataBody::Schema {
                schema: expected.clone(),
            }
        );
        responses.push(schema);
    }

    let explain = reader
        .metadata(DirectMetadataRequest::Explain {
            sql: fixture.explain_sql.clone(),
        })
        .unwrap_or_else(|error| panic!("project explain: {error}"));
    assert_eq!(
        explain.body,
        DirectMetadataBody::Explain {
            sql: fixture.explain_sql.clone(),
            physical_plan: fixture.explain_physical_plan.clone(),
            index: fixture.explain_index.clone(),
        }
    );
    responses.push(explain);

    let events = reader
        .metadata(DirectMetadataRequest::EventsStatus)
        .unwrap_or_else(|error| panic!("project events status: {error}"));
    assert_eq!(
        events.body,
        DirectMetadataBody::EventsStatus {
            status: fixture.events.clone(),
            has_more: false,
            continuation: None,
        }
    );
    responses.push(events);

    let maintenance = reader
        .metadata(DirectMetadataRequest::MaintenanceStatus)
        .unwrap_or_else(|error| panic!("project maintenance status: {error}"));
    assert_eq!(
        maintenance.body,
        DirectMetadataBody::MaintenanceStatus {
            status: fixture.maintenance.clone(),
        }
    );
    responses.push(maintenance);

    for (kind, expected) in [
        (
            DirectImageMetadataKind::Sync,
            DirectImageState::Sync(fixture.sync.clone()),
        ),
        (
            DirectImageMetadataKind::ChangeLog,
            DirectImageState::ChangeLog(fixture.changes.clone()),
        ),
        (
            DirectImageMetadataKind::Configuration,
            DirectImageState::Configuration(fixture.configuration.clone()),
        ),
    ] {
        let state = reader
            .metadata(DirectMetadataRequest::ImageState { kind })
            .unwrap_or_else(|error| panic!("project {kind:?}: {error}"));
        assert_eq!(
            state.body,
            DirectMetadataBody::ImageState { state: expected }
        );
        responses.push(state);
    }
    for response in &responses {
        assert_eq!(
            response.canonical_bytes,
            canonical_metadata_bytes_for_test(&response.body)
                .expect("shared route-neutral metadata encoder supplies exact bytes")
        );
    }
    responses
}

fn sorted_image_rows(image: &DirectOwnedImage) -> Vec<DirectStoredRow> {
    let mut rows = image.relational_rows.clone();
    rows.sort_by(|left, right| (&left.table, left.row_id).cmp(&(&right.table, right.row_id)));
    rows
}

fn sorted_image_edges(image: &DirectOwnedImage) -> Vec<DirectStoredEdge> {
    let mut edges = image.graph_edges.clone();
    edges.sort_by(|left, right| {
        (&left.edge_type, left.source, left.target).cmp(&(
            &right.edge_type,
            right.source,
            right.target,
        ))
    });
    edges
}

fn sorted_expected_edges(fixture: &Fixture) -> Vec<DirectStoredEdge> {
    let mut edges = fixture.expected_graph_edges.clone();
    edges.sort_by(|left, right| {
        (&left.edge_type, left.source, left.target).cmp(&(
            &right.edge_type,
            right.source,
            right.target,
        ))
    });
    edges
}

fn sorted_image_vectors(image: &DirectOwnedImage) -> Vec<DirectStoredVector> {
    let mut vectors = image.vectors.clone();
    vectors.sort_by(|left, right| {
        (&left.index.table, &left.index.column, left.row_id).cmp(&(
            &right.index.table,
            &right.index.column,
            right.row_id,
        ))
    });
    vectors
}

fn assert_complete_owned_image(fixture: &Fixture, reader: &DirectFileReader) -> DirectSnapshot {
    let image = owned_image_for_test(reader)
        .unwrap_or_else(|error| panic!("observe complete owned image: {error}"));
    assert_eq!(sorted_image_rows(&image), fixture.expected_relational_rows);
    assert_eq!(sorted_image_edges(&image), sorted_expected_edges(fixture));
    assert_eq!(sorted_image_vectors(&image), fixture.expected_vectors);
    assert_eq!(image.changes, fixture.changes);
    assert_eq!(image.sync, fixture.sync);
    assert_eq!(image.configuration, fixture.configuration);
    assert_eq!(image.events, fixture.events);
    assert_eq!(image.maintenance, fixture.maintenance);
    image.snapshot
}

fn exact_empty_page(columns: &[String], snapshot: DirectSnapshot) -> DirectCursorPage {
    let page = contextdb_core::read_contract::CursorPage {
        columns: columns.to_vec(),
        rows: Vec::new(),
        has_more: false,
    };
    DirectCursorPage {
        canonical_bytes: encode_cursor_page(&page)
            .expect("shared encoder supplies empty terminal-page bytes"),
        page,
        snapshot,
    }
}

fn cursor_pages(fixture: &Fixture, reader: &DirectFileReader) -> Vec<DirectCursorPage> {
    let one = NonZeroUsize::new(1).expect("one row is nonzero");
    let mut opened = reader
        .open_cursor(&fixture.cursor_sql, &HashMap::new(), one)
        .unwrap_or_else(|error| panic!("open direct cursor: {error}"));
    let mut pages = vec![opened.first_page];
    while pages.last().expect("first cursor page").page.has_more {
        pages.push(
            opened
                .cursor
                .fetch(Some(one))
                .unwrap_or_else(|error| panic!("fetch direct cursor: {error}")),
        );
    }
    assert_eq!(pages.len(), fixture.cursor_rows.len());
    for (ordinal, page) in pages.iter().enumerate() {
        assert_eq!(page.page.columns, fixture.cursor_columns);
        assert_eq!(page.page.rows, vec![fixture.cursor_rows[ordinal].clone()]);
        assert_eq!(page.page.has_more, ordinal + 1 < fixture.cursor_rows.len());
        assert_eq!(
            page.canonical_bytes,
            encode_cursor_page(&page.page).expect("shared encoder supplies cursor bytes")
        );
    }
    let snapshot = pages[0].snapshot;
    let empty = exact_empty_page(&fixture.cursor_columns, snapshot);
    assert_eq!(
        opened
            .cursor
            .fetch(Some(one))
            .expect("exhausted fetch is empty success"),
        empty
    );
    opened
        .cursor
        .close()
        .expect("close after exhaustion is a no-op success");
    assert_eq!(
        opened
            .cursor
            .fetch(Some(one))
            .expect("close after exhaustion preserves empty-success state"),
        empty
    );
    pages
}

#[derive(Debug, Clone, PartialEq)]
struct ReadEvidence {
    queries: Vec<(Vec<u8>, DirectSnapshot)>,
    metadata: Vec<DirectMetadataResponse>,
    cursor: Vec<DirectCursorPage>,
    owned_snapshot: DirectSnapshot,
    typed_digest: DirectTypedImageDigest,
}

fn representative_reads(fixture: &Fixture, reader: &DirectFileReader) -> ReadEvidence {
    let queries = fixture
        .queries
        .iter()
        .map(|expected| assert_query(reader, expected))
        .collect::<Vec<_>>();
    let metadata = metadata_responses(fixture, reader);
    let cursor = cursor_pages(fixture, reader);
    let owned_snapshot = assert_complete_owned_image(fixture, reader);
    let typed_digest = verified_image_digest_for_test(reader)
        .unwrap_or_else(|error| panic!("verify complete typed image identity: {error}"));
    let snapshot = queries[0].1;
    for (_, actual) in &queries {
        assert_eq!(*actual, snapshot);
    }
    for response in &metadata {
        assert_eq!(response.snapshot, snapshot);
    }
    for page in &cursor {
        assert_eq!(page.snapshot, snapshot);
    }
    assert_eq!(owned_snapshot, snapshot);
    assert_eq!(typed_digest.complete, snapshot.image_id);
    ReadEvidence {
        queries,
        metadata,
        cursor,
        owned_snapshot,
        typed_digest,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ManifestKind {
    Directory,
    File,
    Link,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ManifestEntry {
    relative_path: PathBuf,
    kind: ManifestKind,
    mode: u32,
    link_target: Option<PathBuf>,
    file_hash: Option<[u8; 32]>,
}

fn manifest_mode(metadata: &fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        metadata.permissions().mode()
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        0
    }
}

fn collect_manifest(root: &Path, path: &Path, entries: &mut Vec<ManifestEntry>) {
    let metadata = fs::symlink_metadata(path)
        .unwrap_or_else(|error| panic!("inspect {}: {error}", path.display()));
    let file_type = metadata.file_type();
    let kind = if file_type.is_dir() {
        ManifestKind::Directory
    } else if file_type.is_file() {
        ManifestKind::File
    } else if file_type.is_symlink() {
        ManifestKind::Link
    } else {
        ManifestKind::Other
    };
    let relative_path = path
        .strip_prefix(root)
        .unwrap_or_else(|error| panic!("{} is outside {}: {error}", path.display(), root.display()))
        .to_path_buf();
    let link_target = file_type.is_symlink().then(|| {
        fs::read_link(path).unwrap_or_else(|error| panic!("read link {}: {error}", path.display()))
    });
    let file_hash = file_type.is_file().then(|| {
        *blake3::hash(
            &fs::read(path).unwrap_or_else(|error| panic!("hash {}: {error}", path.display())),
        )
        .as_bytes()
    });
    entries.push(ManifestEntry {
        relative_path,
        kind,
        mode: manifest_mode(&metadata),
        link_target,
        file_hash,
    });
    if file_type.is_dir() {
        let mut children = fs::read_dir(path)
            .unwrap_or_else(|error| panic!("list {}: {error}", path.display()))
            .map(|entry| entry.unwrap_or_else(|error| panic!("read directory entry: {error}")))
            .collect::<Vec<_>>();
        children.sort_by_key(|entry| entry.file_name());
        for child in children {
            collect_manifest(root, &child.path(), entries);
        }
    }
}

fn directory_manifest(root: &Path) -> Vec<ManifestEntry> {
    let mut entries = Vec::new();
    collect_manifest(root, root, &mut entries);
    entries
}

fn appended_companion(path: &Path) -> PathBuf {
    let mut file_name: OsString = path.file_name().expect("store file name").to_os_string();
    file_name.push(".lock");
    path.with_file_name(file_name)
}

fn assert_companions_unchanged(
    before: &[ManifestEntry],
    after: &[ManifestEntry],
    fixture: &Fixture,
) {
    assert_eq!(after, before, "the recursive source tree changed");
    for companion in [
        appended_companion(&fixture.path),
        fixture.path.with_extension("lock"),
    ] {
        let relative = companion
            .strip_prefix(&fixture.root)
            .expect("candidate companion is beside the store");
        assert_eq!(
            before.iter().find(|entry| entry.relative_path == relative),
            after.iter().find(|entry| entry.relative_path == relative),
            "candidate companion changed: {}",
            companion.display()
        );
    }
}

fn detach_store_tree(fixture: &Fixture) {
    let parent = fixture.root.parent().expect("source parent");
    let parked = parent.join(format!("detached_source_{}", fixture.token));
    fs::rename(&fixture.root, &parked)
        .unwrap_or_else(|error| panic!("detach {}: {error}", fixture.root.display()));
    assert!(fs::symlink_metadata(&fixture.root).is_err());
    assert!(fs::symlink_metadata(&fixture.path).is_err());
    fs::remove_dir_all(&parked)
        .unwrap_or_else(|error| panic!("remove detached tree {}: {error}", parked.display()));
}

fn assert_exact_released_source_trace(events: &[ReadImageSourceEvent]) -> (Option<PathBuf>, usize) {
    assert!(
        events.len() >= 4,
        "start, access, handles-dropped, and release are required"
    );
    let breadcrumb_path = match &events[0] {
        ReadImageSourceEvent::Started { breadcrumb_path } => breadcrumb_path.clone(),
        other => panic!("first persistence event must record breadcrumb outcome: {other:?}"),
    };
    let access_count = events.len() - 3;
    assert_eq!(
        &events[1..1 + access_count],
        vec![ReadImageSourceEvent::SourceAccess; access_count],
        "only persistence-owned source accesses may occur before handle release"
    );
    assert_eq!(
        events[1 + access_count],
        ReadImageSourceEvent::SourceHandlesDropped
    );
    assert_eq!(
        events[2 + access_count],
        ReadImageSourceEvent::Released {
            breadcrumb_path: breadcrumb_path.clone(),
        }
    );
    if let Some(path) = breadcrumb_path.as_deref() {
        assert!(fs::symlink_metadata(path).is_err());
    }
    (breadcrumb_path, access_count)
}

#[test]
fn released_image_is_the_first_gate_and_survives_complete_source_removal() {
    let _serial = serialise_direct_file_test();
    for extension in ["db", "redb"] {
        let root = tempfile::tempdir().expect("scratch directory");
        let fixture = seed_fixture_with_extension(
            root.path(),
            extension,
            "post-release-complete-source-tree-removal",
        );
        reset_read_image_source_events_for_test();
        let observer = Arc::new(RecordingObserver::default());
        let reader = open_with_hydration_observer_for_test(
            &fixture.path,
            default_reader_config(Arc::new(ManualClock::default()), &fixture.runtime_directory),
            Arc::clone(&observer) as Arc<dyn HydrationObserver>,
        )
        .unwrap_or_else(|error| panic!("publish released image: {error}"));
        let checkpoints = observer
            .checkpoints
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(checkpoints.len(), 3);
        let breadcrumb = assert_exact_released_source_trace(&read_image_source_events_for_test()).0;
        assert_eq!(
            checkpoints[0],
            HydrationCheckpoint::PersistenceReleaseSealed {
                breadcrumb_path: breadcrumb.clone(),
            }
        );
        assert_eq!(
            checkpoints[1],
            HydrationCheckpoint::BackendTargetConstructed {
                breadcrumb_path: breadcrumb.clone(),
            }
        );
        assert_eq!(
            checkpoints[2],
            HydrationCheckpoint::ExternallyUsable {
                breadcrumb_path: breadcrumb,
            }
        );
        drop(checkpoints);

        let released_events = read_image_source_events_for_test();
        let (_, source_access_count) = assert_exact_released_source_trace(&released_events);
        detach_store_tree(&fixture);
        let first = representative_reads(&fixture, &reader);
        let second = representative_reads(&fixture, &reader);
        assert_eq!(second, first);
        assert_eq!(read_image_source_events_for_test(), released_events);
        assert_eq!(
            released_events
                .iter()
                .filter(|event| matches!(event, ReadImageSourceEvent::SourceAccess))
                .count(),
            source_access_count
        );
    }
}

#[test]
fn direct_read_keeps_recursive_bytes_modes_links_and_companions_identical() {
    let _serial = serialise_direct_file_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let fixture = seed_fixture(root.path(), "recursive-manifest-identity");
    let before = directory_manifest(&fixture.root);
    let reader = direct_open(&fixture, Arc::new(ManualClock::default()));
    let _ = representative_reads(&fixture, &reader);
    let after = directory_manifest(&fixture.root);
    assert_companions_unchanged(&before, &after, &fixture);
}

#[test]
fn caller_driven_writer_policy_is_forgotten_by_the_fresh_direct_session() {
    let _serial = serialise_direct_file_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let fixture = seed_fixture_after_caller_driven_writer(root.path());
    let reopened = Database::open(&fixture.path)
        .unwrap_or_else(|error| panic!("ordinary fresh reopen: {error}"));
    assert_eq!(
        reopened.maintenance_status().policy,
        MaintenancePolicy::EngineOwned
    );
    reopened.close().expect("close ordinary comparison handle");

    reset_counters_for_test().unwrap_or_else(|error| panic!("install subsystem probes: {error}"));
    let reader = direct_open(&fixture, Arc::new(ManualClock::default()));
    let response = reader
        .metadata(DirectMetadataRequest::MaintenanceStatus)
        .unwrap_or_else(|error| panic!("direct maintenance status: {error}"));
    assert_eq!(
        response.body,
        DirectMetadataBody::MaintenanceStatus {
            status: fixture.maintenance.clone(),
        }
    );
    assert_eq!(fixture.maintenance.policy, "engine_owned");
    assert!(!fixture.maintenance.running);
    assert_eq!(fixture.maintenance.active_maintenance_loops, 0);
    let counters =
        counters_for_test().unwrap_or_else(|error| panic!("read subsystem probes: {error}"));
    assert_eq!(counters.maintenance_worker_starts, 0);
    assert_eq!(counters.background_worker_starts, 0);
}

#[derive(Default)]
struct PathCollector {
    paths: Vec<Vec<String>>,
    skip_test_modules: bool,
}

impl PathCollector {
    fn production_only() -> Self {
        Self {
            paths: Vec::new(),
            skip_test_modules: true,
        }
    }
}

impl<'ast> Visit<'ast> for PathCollector {
    fn visit_path(&mut self, path: &'ast syn::Path) {
        self.paths.push(
            path.segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect(),
        );
        visit::visit_path(self, path);
    }

    fn visit_item_mod(&mut self, module: &'ast syn::ItemMod) {
        if self.skip_test_modules
            && matches!(
                module.ident.to_string().as_str(),
                "test_seams" | "no_test_seams_contract"
            )
        {
            return;
        }
        visit::visit_item_mod(self, module);
    }
}

fn path_equals(path: &[String], expected: &[&str]) -> bool {
    path.len() == expected.len()
        && path
            .iter()
            .zip(expected)
            .all(|(actual, expected)| actual == expected)
}

fn legal_direct_dependency(path: &[String]) -> bool {
    let Some(first) = path.first().map(String::as_str) else {
        return false;
    };
    if path.len() == 1 {
        // `syn` reports primitive types and local bindings as one-segment paths too. Keep those
        // names explicit so imported or qualified subsystem dependencies remain closed by default.
        return matches!(
            first,
            "Arc"
                | "AsRef"
                | "BackendNeutralReadExecutionTargetStub"
                | "BackendTargetConstructionReceipt"
                | "BTreeMap"
                | "BTreeSet"
                | "Clone"
                | "ContextId"
                | "Copy"
                | "CursorPage"
                | "DeadlineClock"
                | "Debug"
                | "Default"
                | "DirectChangeState"
                | "DirectColumnReference"
                | "DirectConfigurationState"
                | "DirectCursor"
                | "DirectCursorMemoryReservation"
                | "DirectCursorOpen"
                | "DirectCursorOwnedResources"
                | "DirectCursorPage"
                | "DirectCursorResourceWitness"
                | "DirectCursorSnapshotRetention"
                | "DirectCursorState"
                | "DirectCursorUnavailable"
                | "DirectEventTypeStatus"
                | "DirectEventsStatus"
                | "DirectFileReader"
                | "DirectFileReaderError"
                | "DirectImageMetadataKind"
                | "DirectImageState"
                | "DirectIndexColumn"
                | "DirectIndexDirection"
                | "DirectMaintenanceStatus"
                | "DirectMetadataBody"
                | "DirectMetadataRequest"
                | "DirectMetadataResponse"
                | "DirectOwnedImage"
                | "DirectPropagationRule"
                | "DirectQuery"
                | "DirectRankPolicy"
                | "DirectReaderConfig"
                | "DirectReaderCounters"
                | "DirectReaderPrerequisite"
                | "DirectReferencePropagation"
                | "DirectRepairRequiredCause"
                | "DirectRetainPolicy"
                | "DirectRouteStatus"
                | "DirectScheduleStatus"
                | "DirectSchema"
                | "DirectSchemaColumn"
                | "DirectSchemaIndex"
                | "DirectScopeLabelKind"
                | "DirectSinkStatus"
                | "DirectSnapshot"
                | "DirectStateMachine"
                | "DirectStoredEdge"
                | "DirectStoredRow"
                | "DirectStoredVector"
                | "DirectStoreDiagnostic"
                | "DirectSyncSource"
                | "DirectSyncState"
                | "DirectTableSyncPolicy"
                | "DirectTypedImageDigest"
                | "DirectVectorQuantization"
                | "Eq"
                | "Err"
                | "HashMap"
                | "HydrationCheckpoint"
                | "HydrationObservation"
                | "HydrationObserver"
                | "HydrationPhase"
                | "Lsn"
                | "NonZeroUsize"
                | "None"
                | "Ok"
                | "Option"
                | "OwnerReadCancellation"
                | "Path"
                | "PathBuf"
                | "PartialEq"
                | "Principal"
                | "QueryResult"
                | "ReadFailure"
                | "ReadLimits"
                | "Result"
                | "RowChange"
                | "EdgeChange"
                | "VectorChange"
                | "DdlChange"
                | "RowId"
                | "ScopeLabel"
                | "Send"
                | "Self"
                | "Some"
                | "String"
                | "Sync"
                | "TxId"
                | "Uuid"
                | "Value"
                | "Vec"
                | "VectorIndexRef"
                | "bool"
                | "breadcrumb_path"
                | "cancellation"
                | "continuation"
                | "cfg"
                | "checkpoint"
                | "clock"
                | "config"
                | "contextdb_core"
                | "contexts"
                | "constructed"
                | "crate"
                | "cursor"
                | "derive"
                | "doc"
                | "dyn"
                | "error"
                | "f32"
                | "failure"
                | "feature"
                | "fmt"
                | "formatter"
                | "limits"
                | "load"
                | "observation"
                | "observer"
                | "params"
                | "path"
                | "pending_image"
                | "phase"
                | "principal"
                | "reader"
                | "release_receipt"
                | "request"
                | "rows"
                | "runtime_directory"
                | "scope_labels"
                | "self"
                | "sql"
                | "std"
                | "str"
                | "super"
                | "u8"
                | "u32"
                | "u64"
                | "usize"
                | "uuid"
                | "construct_backend_neutral_target"
                | "consume_released_persistence_load"
                | "execute_on_backend_neutral_target"
                | "fetch_backend_neutral_cursor"
                | "handoff_to_backend_neutral_reader"
                | "open_cursor_on_backend_neutral_target"
                | "drop"
                | "write"
        );
    }
    if first == "std" {
        return [
            &["std"][..],
            &["std", "collections"][..],
            &["std", "collections", "BTreeMap"][..],
            &["std", "collections", "HashMap"][..],
            &["std", "error"][..],
            &["std", "error", "Error"][..],
            &["std", "fmt"][..],
            &["std", "fs", "symlink_metadata"][..],
            &["std", "io", "ErrorKind"][..],
            &["std", "io", "ErrorKind", "NotFound"][..],
            &["std", "num"][..],
            &["std", "num", "NonZeroUsize"][..],
            &["std", "path"][..],
            &["std", "path", "Path"][..],
            &["std", "path", "PathBuf"][..],
            &["std", "result"][..],
            &["std", "result", "Result"][..],
            &["std", "sync"][..],
            &["std", "sync", "Arc"][..],
            &["std", "sync", "Weak"][..],
        ]
        .iter()
        .any(|expected| path_equals(path, expected));
    }
    if first == "contextdb_core" {
        return [
            &["contextdb_core"][..],
            &["contextdb_core", "Lsn"][..],
            &["contextdb_core", "RowId"][..],
            &["contextdb_core", "TxId"][..],
            &["contextdb_core", "Value"][..],
            &["contextdb_core", "VectorIndexRef"][..],
            &["contextdb_core", "read_contract"][..],
            &["contextdb_core", "read_contract", "CursorPage"][..],
            &["contextdb_core", "read_contract", "DeadlineClock"][..],
            &["contextdb_core", "read_contract", "OwnerReadCancellation"][..],
            &["contextdb_core", "read_contract", "ReadFailure"][..],
            &["contextdb_core", "read_contract", "ReadLimits"][..],
        ]
        .iter()
        .any(|expected| path_equals(path, expected));
    }
    if first == "uuid" {
        return path.get(1).is_some_and(|part| part == "Uuid");
    }
    if first != "crate" {
        return matches!(
            first,
            "Arc"
                | "BackendTargetConstructionReceipt"
                | "DirectCursorState"
                | "DirectCursorUnavailable"
                | "DirectFileReader"
                | "DirectFileReaderError"
                | "DirectImageState"
                | "DirectMetadataBody"
                | "DirectReaderPrerequisite"
                | "DirectRepairRequiredCause"
                | "DirectStoreDiagnostic"
                | "DirectVectorQuantization"
                | "HydrationCheckpoint"
                | "HydrationObservation"
                | "HydrationPhase"
                | "Option"
                | "OwnerReadCancellation"
                | "Path"
                | "ReadFailure"
                | "Result"
                | "Self"
                | "String"
                | "Vec"
                | "fmt"
        );
    }
    if path_equals(path, &["crate", "QueryResult"])
        || path_equals(path, &["crate", "persistence", "load_read_image"])
        || path_equals(path, &["crate", "executor", "ReadExecutionTarget"])
    {
        return true;
    }
    if path.get(1).is_some_and(|part| part == "sync_types") {
        return [
            &["crate", "sync_types", "DdlChange"][..],
            &["crate", "sync_types", "EdgeChange"][..],
            &["crate", "sync_types", "RowChange"][..],
            &["crate", "sync_types", "VectorChange"][..],
        ]
        .iter()
        .any(|expected| path_equals(path, expected));
    }
    if path.get(1).is_some_and(|part| part == "persistence") {
        return [
            &["crate", "persistence", "ReadPersistenceImage"][..],
            &["crate", "persistence", "ReadPersistenceLoad"][..],
            &["crate", "persistence", "ReadPersistenceReleaseReceipt"][..],
        ]
        .iter()
        .any(|expected| path_equals(path, expected));
    }
    false
}

fn direct_module_files() -> Vec<PathBuf> {
    fn visit_file(path: PathBuf, files: &mut Vec<PathBuf>, seen: &mut BTreeSet<PathBuf>) {
        if !seen.insert(path.clone()) {
            return;
        }
        let syntax = syn::parse_file(
            &fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display())),
        )
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));
        files.push(path.clone());
        for item in syntax.items {
            let Item::Mod(module) = item else { continue };
            if module.content.is_some() {
                continue;
            }
            let parent = path.parent().expect("direct module parent");
            let flat = parent.join(format!("{}.rs", module.ident));
            let nested = parent
                .join(path.file_stem().expect("direct module stem"))
                .join(format!("{}.rs", module.ident));
            if flat.is_file() {
                visit_file(flat, files, seen);
            } else if nested.is_file() {
                visit_file(nested, files, seen);
            } else {
                panic!("out-of-line direct module {} has no source", module.ident);
            }
        }
    }
    let mut files = Vec::new();
    visit_file(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/direct_file_reader.rs"),
        &mut files,
        &mut BTreeSet::new(),
    );
    files.sort();
    files
}

fn assert_fields_use_legal_memory_dependencies(path: &Path, names: &[&str]) {
    let syntax = syn::parse_file(
        &fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display())),
    )
    .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));
    let inspect_fields = |owner: &str, fields: &Fields| {
        for field in fields {
            let mut collector = PathCollector::default();
            collector.visit_type(&field.ty);
            for dependency in collector.paths {
                assert!(
                    legal_direct_dependency(&dependency),
                    "{} field {:?} uses a dependency outside the direct allowlist: {}",
                    owner,
                    field.ident,
                    dependency.join("::")
                );
            }
        }
    };
    for item in &syntax.items {
        match item {
            Item::Struct(item) if names.contains(&item.ident.to_string().as_str()) => {
                inspect_fields(&item.ident.to_string(), &item.fields);
            }
            Item::Enum(item) if names.contains(&item.ident.to_string().as_str()) => {
                for variant in &item.variants {
                    inspect_fields(
                        &format!("{}::{}", item.ident, variant.ident),
                        &variant.fields,
                    );
                }
            }
            _ => {}
        }
    }
}

fn visit_nested_items<'a>(items: &'a [Item], visitor: &mut impl FnMut(&'a Item)) {
    for item in items {
        visitor(item);
        if let Item::Mod(module) = item
            && let Some((_, nested)) = &module.content
        {
            visit_nested_items(nested, visitor);
        }
    }
}

#[derive(Default)]
struct CfgAttributeCollector {
    cfg_attributes: Vec<String>,
}

impl<'ast> Visit<'ast> for CfgAttributeCollector {
    fn visit_attribute(&mut self, attribute: &'ast syn::Attribute) {
        if attribute.path().is_ident("cfg") || attribute.path().is_ident("cfg_attr") {
            self.cfg_attributes.push(
                attribute
                    .meta
                    .require_list()
                    .map(|list| list.tokens.to_string())
                    .unwrap_or_else(|_| attribute.path().segments[0].ident.to_string()),
            );
        }
        visit::visit_attribute(self, attribute);
    }
}

#[derive(Default)]
struct MacroCollector {
    definitions: Vec<String>,
    invocations: Vec<(String, String)>,
}

impl<'ast> Visit<'ast> for MacroCollector {
    fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
        if let Some(name) = &item.ident {
            self.definitions.push(name.to_string());
        }
        visit::visit_item_macro(self, item);
    }

    fn visit_macro(&mut self, invocation: &'ast syn::Macro) {
        let name = invocation
            .path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>()
            .join("::");
        self.invocations.push((name, invocation.tokens.to_string()));
        visit::visit_macro(self, invocation);
    }
}

fn assert_no_use_alias_or_glob(tree: &syn::UseTree, source: &Path) {
    match tree {
        syn::UseTree::Path(path) => assert_no_use_alias_or_glob(&path.tree, source),
        syn::UseTree::Group(group) => {
            for item in &group.items {
                assert_no_use_alias_or_glob(item, source);
            }
        }
        syn::UseTree::Name(_) => {}
        syn::UseTree::Rename(rename) => panic!(
            "{} aliases dependency {} as {}; direct dependencies must remain AST-visible",
            source.display(),
            rename.ident,
            rename.rename
        ),
        syn::UseTree::Glob(_) => panic!(
            "{} uses a glob import; the direct dependency allowlist requires exact names",
            source.display()
        ),
    }
}

fn item_identity(item: &Item) -> Option<String> {
    match item {
        Item::Const(item) => Some(item.ident.to_string()),
        Item::Enum(item) => Some(item.ident.to_string()),
        Item::Fn(item) => Some(item.sig.ident.to_string()),
        Item::Impl(item) => {
            let Type::Path(target) = item.self_ty.as_ref() else {
                return Some("<non-path impl>".to_owned());
            };
            target
                .path
                .segments
                .last()
                .map(|part| part.ident.to_string())
        }
        Item::Mod(item) => Some(item.ident.to_string()),
        Item::Struct(item) => Some(item.ident.to_string()),
        Item::Trait(item) => Some(item.ident.to_string()),
        Item::Type(item) => Some(item.ident.to_string()),
        Item::Use(_) => Some("<use>".to_owned()),
        _ => None,
    }
}

fn test_observation_item(item: &Item) -> bool {
    matches!(
        item_identity(item).as_deref(),
        Some(
            "DirectCursorResourceWitness"
                | "DirectReaderCounters"
                | "HydrationCheckpoint"
                | "HydrationObservation"
                | "HydrationObserver"
                | "test_seams"
                | "no_test_seams_contract"
        )
    )
}

fn assert_core_has_no_cfg_or_macro_dependency_bypass(source: &Path, syntax: &syn::File) {
    for item in &syntax.items {
        if let Item::Use(item_use) = item {
            assert_no_use_alias_or_glob(&item_use.tree, source);
        }
        if test_observation_item(item) {
            continue;
        }
        if let Item::Fn(function) = item {
            let name = function.sig.ident.to_string().to_ascii_lowercase();
            assert!(
                !name.contains("decode") && !name.contains("deserialize"),
                "{} defines a local persistence decoder: {}",
                source.display(),
                function.sig.ident
            );
        }
        let mut cfg = CfgAttributeCollector::default();
        cfg.visit_item(item);
        assert!(
            cfg.cfg_attributes.is_empty(),
            "{} core item {} changes by cfg: {:?}",
            source.display(),
            item_identity(item).unwrap_or_else(|| "<anonymous>".to_owned()),
            cfg.cfg_attributes
        );
        let mut macros = MacroCollector::default();
        macros.visit_item(item);
        assert!(
            macros.definitions.is_empty(),
            "{} defines a macro in the direct production module: {:?}",
            source.display(),
            macros.definitions
        );
        for (name, tokens) in macros.invocations {
            assert_eq!(
                name, "write",
                "only formatting writes are legal in direct core"
            );
            for forbidden in [
                "Database",
                "RedbPersistence",
                "BlobRepository",
                "BlobReadSession",
                "BlobExportSnapshot",
                "iroh",
                "open_read_only",
                "begin_read",
                "decode",
            ] {
                assert!(
                    !tokens.contains(forbidden),
                    "{} hides forbidden dependency {forbidden} in a macro body",
                    source.display()
                );
            }
        }
    }
}

#[derive(Default)]
struct CallSequenceCollector {
    events: Vec<String>,
}

#[derive(Default)]
struct MethodCallCollector {
    methods: Vec<String>,
}

impl<'ast> Visit<'ast> for MethodCallCollector {
    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        self.methods.push(call.method.to_string());
        visit::visit_expr_method_call(self, call);
    }
}

impl<'ast> Visit<'ast> for CallSequenceCollector {
    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        if let syn::Expr::Path(function) = call.func.as_ref()
            && let Some(name) = function.path.segments.last()
            && matches!(
                name.ident.to_string().as_str(),
                "drop" | "construct_backend_neutral_target"
            )
        {
            self.events.push(name.ident.to_string());
        }
        visit::visit_expr_call(self, call);
    }

    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        let method = call.method.to_string();
        if method == "checkpoint" {
            let mut paths = PathCollector::default();
            for argument in &call.args {
                paths.visit_expr(argument);
            }
            let phase = paths
                .paths
                .iter()
                .find(|path| path.first().is_some_and(|part| part == "HydrationPhase"))
                .and_then(|path| path.last())
                .cloned()
                .unwrap_or_else(|| "Unknown".to_owned());
            self.events.push(format!("checkpoint:{phase}"));
        } else if method == "publish" {
            self.events.push(method);
        }
        visit::visit_expr_method_call(self, call);
    }
}

fn assert_no_feature_core_journey(syntax: &syn::File) {
    let module = syntax
        .items
        .iter()
        .find_map(|item| match item {
            Item::Mod(module) if module.ident == "no_test_seams_contract" => Some(module),
            _ => None,
        })
        .expect("direct module carries one no-test-seams causal journey");
    let cfg = module
        .attrs
        .iter()
        .find(|attribute| attribute.path().is_ident("cfg"))
        .and_then(|attribute| attribute.meta.require_list().ok())
        .map(|list| list.tokens.to_string())
        .expect("no-test-seams journey has an exact cfg");
    for required in ["all", "test", "not", "feature", "\"test-seams\""] {
        assert!(
            cfg.contains(required),
            "no-test-seams cfg omits {required}: {cfg}"
        );
    }
    let (_, items) = module
        .content
        .as_ref()
        .expect("no-test-seams journey is inline");
    let test = items
        .iter()
        .find_map(|item| match item {
            Item::Fn(function)
                if function.sig.ident
                    == "production_cfg_open_and_execute_the_same_owned_image_path" =>
            {
                Some(function)
            }
            _ => None,
        })
        .expect("no-test-seams journey has the causal open/execute test");
    let mut paths = PathCollector::default();
    paths.visit_block(&test.block);
    assert!(paths.paths.iter().any(|path| {
        path.len() >= 2
            && path[path.len() - 2] == "DirectFileReader"
            && path[path.len() - 1] == "open"
    }));
    let mut methods = MethodCallCollector::default();
    methods.visit_block(&test.block);
    assert!(methods.methods.iter().any(|method| method == "execute"));
}

fn assert_shared_prerequisite_shapes() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let persistence_path = manifest.join("src/persistence.rs");
    let persistence =
        syn::parse_file(&fs::read_to_string(&persistence_path).expect("read persistence source"))
            .expect("parse persistence source");
    let mut receipt_names = Vec::new();
    let mut damage_enum = false;
    let mut damage_fixture = false;
    visit_nested_items(&persistence.items, &mut |item| match item {
        Item::Struct(item) if item.ident == "ReadPersistenceReleaseReceipt" => {
            let fields_private = match &item.fields {
                Fields::Named(fields) => fields
                    .named
                    .iter()
                    .all(|field| matches!(field.vis, Visibility::Inherited)),
                Fields::Unnamed(fields) => fields
                    .unnamed
                    .iter()
                    .all(|field| matches!(field.vis, Visibility::Inherited)),
                Fields::Unit => true,
            };
            assert!(
                fields_private,
                "release receipt must be privately constructible"
            );
            receipt_names.push(item.ident.to_string());
        }
        Item::Enum(item) if item.ident == "DurableStoreDamage" => {
            damage_enum = true;
        }
        Item::Fn(item) if item.sig.ident == "prepare_durable_store_damage_for_test" => {
            damage_fixture = true;
        }
        _ => {}
    });
    assert_eq!(
        receipt_names.len(),
        1,
        "persistence release receipt prerequisite"
    );
    assert!(damage_enum, "persistence-owned damage kind prerequisite");
    assert!(
        damage_fixture,
        "persistence-owned durable damage fixture prerequisite"
    );

    let executor = syn::parse_file(
        &fs::read_to_string(manifest.join("src/executor.rs")).expect("read executor source"),
    )
    .expect("parse executor source");
    let mut executor_has_target = false;
    visit_nested_items(&executor.items, &mut |item| {
        executor_has_target |= matches!(item, Item::Struct(item) if item.ident == "ReadExecutionTarget")
            || matches!(item, Item::Enum(item) if item.ident == "ReadExecutionTarget")
            || matches!(item, Item::Trait(item) if item.ident == "ReadExecutionTarget");
    });

    let direct_path = manifest.join("src/direct_file_reader.rs");
    let direct = syn::parse_file(&fs::read_to_string(&direct_path).expect("read direct source"))
        .expect("parse direct source");
    assert_core_has_no_cfg_or_macro_dependency_bypass(&direct_path, &direct);
    assert_no_feature_core_journey(&direct);

    let observation = direct
        .items
        .iter()
        .find_map(|item| match item {
            Item::Struct(item) if item.ident == "HydrationObservation" => Some(item),
            _ => None,
        })
        .expect("private hydration observation wrapper");
    assert!(matches!(observation.vis, Visibility::Inherited));
    let mut observation_paths = PathCollector::default();
    observation_paths.visit_item_struct(observation);
    for forbidden in [
        "BackendTargetConstructionReceipt",
        "DirectOwnedImage",
        "ReadExecutionTarget",
        "ReadPersistenceImage",
        "ReadPersistenceReleaseReceipt",
    ] {
        assert!(
            !observation_paths
                .paths
                .iter()
                .any(|path| path.last().is_some_and(|part| part == forbidden)),
            "test observation must not own or receive {forbidden}"
        );
    }
    let observation_impl = direct
        .items
        .iter()
        .find_map(|item| match item {
            Item::Impl(item) => match item.self_ty.as_ref() {
                Type::Path(target)
                    if target
                        .path
                        .segments
                        .last()
                        .is_some_and(|part| part.ident == "HydrationObservation") =>
                {
                    Some(item)
                }
                _ => None,
            },
            _ => None,
        })
        .expect("private hydration observation implementation");
    let mut observation_impl_paths = PathCollector::default();
    observation_impl_paths.visit_item_impl(observation_impl);
    assert!(
        observation_impl_paths.paths.iter().all(|path| {
            path.first().is_none_or(|part| part != "crate") || path_equals(path, &["crate"])
        }),
        "test observation implementation cannot call any crate subsystem"
    );

    let receipt = direct
        .items
        .iter()
        .find_map(|item| match item {
            Item::Struct(item) if item.ident == "BackendTargetConstructionReceipt" => Some(item),
            _ => None,
        })
        .expect("direct route owns one private target-construction receipt");
    assert!(matches!(receipt.vis, Visibility::Inherited));
    let Fields::Named(receipt_fields) = &receipt.fields else {
        panic!("target-construction receipt has named private fields")
    };
    assert!(
        receipt_fields
            .named
            .iter()
            .all(|field| matches!(field.vis, Visibility::Inherited)),
        "target-construction receipt fields remain privately constructible"
    );
    let mut receipt_dependencies = Vec::new();
    for field in &receipt_fields.named {
        let mut paths = PathCollector::default();
        paths.visit_type(&field.ty);
        receipt_dependencies.extend(paths.paths);
    }
    let receipt_uses_shared_target = receipt_dependencies
        .iter()
        .any(|path| path_equals(path, &["crate", "executor", "ReadExecutionTarget"]));
    assert!(
        receipt_dependencies
            .iter()
            .any(|path| { path.last().is_some_and(|part| part == "DirectOwnedImage") })
    );
    assert!(receipt_dependencies.iter().any(|path| {
        path.last()
            .is_some_and(|part| part == "DirectTypedImageDigest")
    }));

    let handoff = direct
        .items
        .iter()
        .find_map(|item| match item {
            Item::Fn(function) if function.sig.ident == "handoff_to_backend_neutral_reader" => {
                Some(function)
            }
            _ => None,
        })
        .expect("direct route has one sealed target handoff");
    let mut signature_paths = PathCollector::default();
    signature_paths.visit_signature(&handoff.sig);
    assert!(signature_paths.paths.iter().any(|path| {
        path_equals(
            path,
            &["crate", "persistence", "ReadPersistenceReleaseReceipt"],
        )
    }));
    let mut calls = CallSequenceCollector::default();
    calls.visit_block(&handoff.block);
    assert_eq!(
        calls.events,
        vec![
            "drop",
            "checkpoint:PersistenceReleaseSealed",
            "construct_backend_neutral_target",
            "checkpoint:BackendTargetConstructed",
            "publish",
            "checkpoint:ExternallyUsable",
        ],
        "the AST-enforced handoff consumes release, constructs, then publishes"
    );

    let consume = direct
        .items
        .iter()
        .find_map(|item| match item {
            Item::Fn(function) if function.sig.ident == "consume_released_persistence_load" => {
                Some(function)
            }
            _ => None,
        })
        .expect("direct route consumes ReadPersistenceLoad exactly once");
    let mut consume_paths = PathCollector::default();
    consume_paths.visit_item_fn(consume);
    assert!(
        consume_paths
            .paths
            .iter()
            .any(|path| { path_equals(path, &["crate", "persistence", "ReadPersistenceLoad"]) })
    );
    let into_parts_calls = consume
        .block
        .stmts
        .iter()
        .filter_map(|statement| match statement {
            syn::Stmt::Local(local) => local.init.as_ref().map(|init| init.expr.as_ref()),
            _ => None,
        })
        .filter(|expression| {
            let mut methods = MethodCallCollector::default();
            methods.visit_expr(expression);
            methods.methods.iter().any(|method| method == "into_parts")
        })
        .count();
    assert_eq!(into_parts_calls, 1);

    assert!(
        executor_has_target,
        "shared bounded ReadExecutionTarget prerequisite"
    );
    assert!(
        receipt_uses_shared_target,
        "the private construction receipt must own crate::executor::ReadExecutionTarget"
    );
}

fn assert_no_writable_subsystems(counters: &DirectReaderCounters) {
    assert_eq!(counters.persistence_writer_open_attempts, 0);
    assert_eq!(counters.persistence_companion_mutations, 0);
    assert_eq!(counters.writable_database_opens, 0);
    assert_eq!(counters.persistence_writer_opens, 0);
    assert_eq!(counters.plugin_starts, 0);
    assert_eq!(counters.sync_client_starts, 0);
    assert_eq!(counters.cron_worker_starts, 0);
    assert_eq!(counters.maintenance_worker_starts, 0);
    assert_eq!(counters.event_delivery_starts, 0);
    assert_eq!(counters.trigger_callback_starts, 0);
    assert_eq!(counters.background_worker_starts, 0);
    assert_eq!(counters.blob_repository_opens, 0);
    assert_eq!(counters.owner_service_starts, 0);
}

#[test]
fn direct_module_uses_only_the_sanctioned_read_dependencies_and_real_probes() {
    let files = direct_module_files();
    let mut loader_calls = 0;
    for source in &files {
        let syntax = syn::parse_file(
            &fs::read_to_string(source)
                .unwrap_or_else(|error| panic!("read {}: {error}", source.display())),
        )
        .unwrap_or_else(|error| panic!("parse {}: {error}", source.display()));
        assert_core_has_no_cfg_or_macro_dependency_bypass(source, &syntax);
        let mut collector = PathCollector::production_only();
        collector.visit_file(&syntax);
        let mut illegal_dependencies = BTreeSet::new();
        for dependency in collector.paths {
            if path_equals(&dependency, &["crate", "persistence", "load_read_image"]) {
                loader_calls += 1;
            }
            if !legal_direct_dependency(&dependency) {
                illegal_dependencies.insert(dependency.join("::"));
            }
        }
        assert!(
            illegal_dependencies.is_empty(),
            "{} uses dependencies outside the direct allowlist: {illegal_dependencies:?}",
            source.display()
        );
        for item in &syntax.items {
            let Item::Impl(ItemImpl { self_ty, items, .. }) = item else {
                continue;
            };
            let Type::Path(target) = self_ty.as_ref() else {
                continue;
            };
            if target
                .path
                .segments
                .last()
                .is_none_or(|part| part.ident != "DirectFileReader")
            {
                continue;
            }
            for item in items {
                let ImplItem::Fn(method) = item else { continue };
                let mut paths = PathCollector::default();
                paths.visit_block(&method.block);
                if paths
                    .paths
                    .iter()
                    .any(|path| path_equals(path, &["crate", "persistence", "load_read_image"]))
                {
                    assert_eq!(method.sig.ident, "open_inner");
                }
            }
        }
        assert_fields_use_legal_memory_dependencies(
            source,
            &[
                "BackendTargetConstructionReceipt",
                "DirectCursor",
                "DirectCursorMemoryReservation",
                "DirectCursorOwnedResources",
                "DirectCursorSnapshotRetention",
                "DirectCursorState",
                "DirectFileReader",
                "DirectOwnedImage",
                "HydrationObservation",
            ],
        );
    }
    assert_eq!(loader_calls, 1);
    assert_shared_prerequisite_shapes();

    let _serial = serialise_direct_file_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let fixture = seed_fixture(root.path(), "writable-subsystem-containment");
    reset_read_image_source_events_for_test();
    reset_counters_for_test().unwrap_or_else(|error| panic!("install real probes: {error}"));
    let reader = direct_open(&fixture, Arc::new(ManualClock::default()));
    let _ = assert_query(&reader, &fixture.queries[0]);
    let _ = assert_exact_released_source_trace(&read_image_source_events_for_test());
    let counters = counters_for_test().unwrap_or_else(|error| panic!("read real probes: {error}"));
    assert_no_writable_subsystems(&counters);
}

struct PipeHydrationObserver;

impl PipeHydrationObserver {
    fn wait_for(command: &str) {
        let mut actual = String::new();
        std::io::stdin()
            .read_line(&mut actual)
            .expect("read controller command");
        assert_eq!(actual.trim(), command);
    }
}

impl HydrationObserver for PipeHydrationObserver {
    fn checkpoint(&self, checkpoint: HydrationCheckpoint) {
        let marker = match &checkpoint {
            HydrationCheckpoint::PersistenceReleaseSealed { breadcrumb_path } => {
                assert_eq!(
                    assert_exact_released_source_trace(&read_image_source_events_for_test()).0,
                    breadcrumb_path.clone(),
                    "sealed receipt preserves persistence's optional breadcrumb outcome"
                );
                if let Some(path) = breadcrumb_path {
                    assert!(fs::symlink_metadata(path).is_err());
                }
                CHILD_RELEASE_SEALED
            }
            HydrationCheckpoint::BackendTargetConstructed { breadcrumb_path } => {
                if let Some(path) = breadcrumb_path {
                    assert!(fs::symlink_metadata(path).is_err());
                }
                CHILD_TARGET_CONSTRUCTED
            }
            HydrationCheckpoint::ExternallyUsable { breadcrumb_path } => {
                if let Some(path) = breadcrumb_path {
                    assert!(fs::symlink_metadata(path).is_err());
                }
                CHILD_EXTERNALLY_USABLE
            }
        };
        println!("{marker}");
        std::io::stdout().flush().expect("flush hydration marker");
        match checkpoint {
            HydrationCheckpoint::PersistenceReleaseSealed { .. } => {
                Self::wait_for(CHILD_CONSTRUCT_TARGET)
            }
            HydrationCheckpoint::BackendTargetConstructed { .. } => Self::wait_for(CHILD_PUBLISH),
            HydrationCheckpoint::ExternallyUsable { .. } => {}
        }
    }
}

fn child_path() -> PathBuf {
    PathBuf::from(std::env::var_os(CHILD_PATH_ENV).expect("child store path"))
}

fn child_runtime_directory() -> PathBuf {
    PathBuf::from(std::env::var_os(CHILD_RUNTIME_ENV).expect("child runtime directory"))
}

#[test]
fn direct_file_reader_child() {
    if std::env::var(CHILD_ROLE_ENV).ok().as_deref() != Some("reader") {
        return;
    }
    let sql = std::env::var(CHILD_SQL_ENV).expect("child query");
    let id = Uuid::parse_str(&std::env::var(CHILD_ID_ENV).expect("child id")).expect("child UUID");
    let payload = std::env::var(CHILD_PAYLOAD_ENV).expect("child payload");
    arm_read_image_hydration_pause_for_test();
    let reader = open_with_hydration_observer_for_test(
        child_path(),
        default_reader_config(Arc::new(ManualClock::default()), &child_runtime_directory()),
        Arc::new(PipeHydrationObserver),
    )
    .unwrap_or_else(|error| panic!("child direct hydration: {error}"));
    println!("{CHILD_SESSION_ACTIVE}");
    println!(
        "{CHILD_TYPED_DIGEST}{}",
        verified_image_digest_wire_for_test(&reader)
            .unwrap_or_else(|error| panic!("child verifies complete typed image: {error}"))
    );
    std::io::stdout().flush().expect("flush session marker");
    loop {
        let mut command = String::new();
        std::io::stdin()
            .read_line(&mut command)
            .expect("read child command");
        match command.trim() {
            CHILD_QUERY => {
                let result = reader
                    .execute(&sql, &HashMap::from([("id".to_owned(), Value::Uuid(id))]))
                    .unwrap_or_else(|error| panic!("query released child image: {error}"));
                assert_eq!(
                    result.result.rows,
                    vec![vec![Value::Uuid(id), Value::Text(payload.clone())]]
                );
                println!("{CHILD_QUERY_OK}");
                std::io::stdout().flush().expect("flush query marker");
            }
            CHILD_CLOSE => return,
            other => panic!("unexpected child command: {other}"),
        }
    }
}

struct ChildReader {
    child: Child,
    stdout: BufReader<ChildStdout>,
    finished: bool,
}

impl ChildReader {
    fn wait_for(&mut self, expected: &str) {
        loop {
            let mut line = String::new();
            let count = self.stdout.read_line(&mut line).expect("read child marker");
            assert_ne!(count, 0, "child exited before marker {expected}");
            if line.trim() == expected {
                return;
            }
        }
    }

    fn wait_for_prefix(&mut self, expected: &str) -> String {
        loop {
            let mut line = String::new();
            let count = self.stdout.read_line(&mut line).expect("read child marker");
            assert_ne!(count, 0, "child exited before marker prefix {expected}");
            if line.trim().starts_with(expected) {
                return line.trim().to_owned();
            }
        }
    }

    fn send(&mut self, command: &str) {
        let stdin = self.child.stdin.as_mut().expect("child stdin");
        writeln!(stdin, "{command}").expect("write child command");
        stdin.flush().expect("flush child command");
    }

    fn close(mut self) {
        self.send(CHILD_CLOSE);
        let status = self.child.wait().expect("wait for child");
        self.finished = true;
        assert!(status.success(), "child exits cleanly: {status}");
    }
}

impl Drop for ChildReader {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn spawn_child_reader_with_runtime_and_payload(
    fixture: &Fixture,
    runtime_directory: &Path,
    expected_payload: &str,
) -> ChildReader {
    let mut child = Command::new(std::env::current_exe().expect("current test binary"))
        .arg("--exact")
        .arg("direct_file_reader_child")
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, "reader")
        .env(CHILD_PATH_ENV, &fixture.path)
        .env(CHILD_RUNTIME_ENV, runtime_directory)
        .env(
            CHILD_SQL_ENV,
            format!(
                "SELECT id, payload FROM {} WHERE id = $id",
                fixture.rows_table
            ),
        )
        .env(CHILD_ID_ENV, fixture.first_row().id.to_string())
        .env(CHILD_PAYLOAD_ENV, expected_payload)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn child reader");
    let stdout = child.stdout.take().expect("child stdout");
    ChildReader {
        child,
        stdout: BufReader::new(stdout),
        finished: false,
    }
}

/// Start a child reader whose DEFAULT per-user runtime location is the
/// pathname given.
///
/// A reader writes itself down in that default, whatever runtime directory it
/// was handed for the owner channel, so the default is the only thing that
/// decides whether it can publish a breadcrumb at all. A journey that needs
/// publication to genuinely fail has to make THAT location unusable; pointing
/// only the handed directory at something unusable leaves the reader
/// publishing normally in the default and proves nothing.
fn spawn_child_reader_with_default_runtime_location(
    fixture: &Fixture,
    default_runtime_location: &Path,
) -> ChildReader {
    let mut child = Command::new(std::env::current_exe().expect("current test binary"))
        .arg("--exact")
        .arg("direct_file_reader_child")
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, "reader")
        .env(CHILD_PATH_ENV, &fixture.path)
        .env(CHILD_RUNTIME_ENV, default_runtime_location)
        .env("XDG_RUNTIME_DIR", default_runtime_location)
        .env(
            CHILD_SQL_ENV,
            format!(
                "SELECT id, payload FROM {} WHERE id = $id",
                fixture.rows_table
            ),
        )
        .env(CHILD_ID_ENV, fixture.first_row().id.to_string())
        .env(CHILD_PAYLOAD_ENV, &fixture.first_row().payload)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn child reader");
    let stdout = child.stdout.take().expect("child stdout");
    ChildReader {
        child,
        stdout: BufReader::new(stdout),
        finished: false,
    }
}

fn spawn_child_reader(fixture: &Fixture) -> ChildReader {
    spawn_child_reader_with_runtime_and_payload(
        fixture,
        &fixture.runtime_directory,
        &fixture.first_row().payload,
    )
}

fn assert_writer_is_held_by_hydrating_readers(path: &Path) -> (u64, usize) {
    match Database::open(path) {
        Err(Error::ReadFailure(failure)) if failure.kind() == ReadFailureKind::HeldByReaders => {
            let ReadFailureDetail::HeldByReaders(detail) = failure.detail() else {
                panic!("held_by_readers must carry concrete holder detail")
            };
            (
                detail.observed_direct_readers,
                detail.verified_readers.len(),
            )
        }
        Ok(db) => {
            let _ = db.close();
            panic!("writer opened while a reader still hydrated")
        }
        Err(error) => panic!("expected typed held_by_readers, got {error}"),
    }
}

fn change_source_row(fixture: &Fixture) -> String {
    let changed = format!(
        "changed-after-release-{}",
        seeded_uuid(&fixture.identity_seed, &["writer-change", "payload"]).simple()
    );
    let db = Database::open(&fixture.path)
        .unwrap_or_else(|error| panic!("writer opens after release: {error}"));
    db.execute(
        &format!(
            "UPDATE {} SET payload = $payload WHERE id = $id",
            fixture.rows_table
        ),
        &HashMap::from([
            ("payload".to_owned(), Value::Text(changed.clone())),
            ("id".to_owned(), Value::Uuid(fixture.first_row().id)),
        ]),
    )
    .expect("writer changes mutable source row");
    db.close().expect("close post-release writer");
    changed
}

fn assert_writer_opens_after_release(path: &Path) {
    let database = Database::open(path)
        .unwrap_or_else(|error| panic!("writer must open after sealed hydration release: {error}"));
    database.close().expect("close post-release writer proof");
}

fn assert_complete_digest_wire(wire: &str) {
    let payload = wire
        .strip_prefix(CHILD_TYPED_DIGEST)
        .expect("typed digest marker prefix");
    let families = payload
        .split(';')
        .map(|part| part.split_once('=').expect("typed family digest"))
        .collect::<Vec<_>>();
    assert_eq!(
        families.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
        vec![
            "relational",
            "graph",
            "f32",
            "sq8",
            "sq4",
            "schema_indexes",
            "policy",
            "sync",
            "change_ddl",
            "configuration",
            "status",
            "complete",
        ]
    );
    for (family, digest) in families {
        assert_eq!(digest.len(), 64, "{family} digest width");
        assert!(
            digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "{family} digest is lowercase hexadecimal"
        );
    }
}

#[test]
fn one_reader_releases_the_real_writer_lock_before_external_publication() {
    let _serial = serialise_direct_file_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let fixture = seed_fixture(root.path(), "single-reader-release-before-publication");
    let mut reader = spawn_child_reader(&fixture);
    let _ = reader.wait_for_prefix(CHILD_HYDRATION_STARTED);
    let held = assert_writer_is_held_by_hydrating_readers(&fixture.path);
    assert_eq!(held.0, 1);
    reader.send(CHILD_FINISH_HYDRATION);
    reader.wait_for(CHILD_RELEASE_SEALED);
    let changed = change_source_row(&fixture);
    reader.send(CHILD_CONSTRUCT_TARGET);
    reader.wait_for(CHILD_TARGET_CONSTRUCTED);
    reader.send(CHILD_PUBLISH);
    reader.wait_for(CHILD_EXTERNALLY_USABLE);
    reader.wait_for(CHILD_SESSION_ACTIVE);
    let original_digest = reader.wait_for_prefix(CHILD_TYPED_DIGEST);
    assert_complete_digest_wire(&original_digest);
    reader.send(CHILD_QUERY);
    reader.wait_for(CHILD_QUERY_OK);

    let mut fresh =
        spawn_child_reader_with_runtime_and_payload(&fixture, &fixture.runtime_directory, &changed);
    let _ = fresh.wait_for_prefix(CHILD_HYDRATION_STARTED);
    let _ = assert_writer_is_held_by_hydrating_readers(&fixture.path);
    fresh.send(CHILD_FINISH_HYDRATION);
    fresh.wait_for(CHILD_RELEASE_SEALED);
    assert_writer_opens_after_release(&fixture.path);
    fresh.send(CHILD_CONSTRUCT_TARGET);
    fresh.wait_for(CHILD_TARGET_CONSTRUCTED);
    fresh.send(CHILD_PUBLISH);
    fresh.wait_for(CHILD_EXTERNALLY_USABLE);
    fresh.wait_for(CHILD_SESSION_ACTIVE);
    let changed_digest = fresh.wait_for_prefix(CHILD_TYPED_DIGEST);
    assert_complete_digest_wire(&changed_digest);
    assert_ne!(changed_digest, original_digest);
    fresh.send(CHILD_QUERY);
    fresh.wait_for(CHILD_QUERY_OK);
    reader.close();
    fresh.close();
}

#[test]
fn multiple_child_readers_hold_only_their_real_hydration_windows() {
    let _serial = serialise_direct_file_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let fixture = seed_fixture(root.path(), "multiple-child-reader-release-windows");
    let mut first = spawn_child_reader(&fixture);
    let mut second = spawn_child_reader(&fixture);
    let _ = first.wait_for_prefix(CHILD_HYDRATION_STARTED);
    let _ = second.wait_for_prefix(CHILD_HYDRATION_STARTED);
    let _ = assert_writer_is_held_by_hydrating_readers(&fixture.path);

    first.send(CHILD_FINISH_HYDRATION);
    first.wait_for(CHILD_RELEASE_SEALED);
    let _ = assert_writer_is_held_by_hydrating_readers(&fixture.path);

    second.send(CHILD_FINISH_HYDRATION);
    second.wait_for(CHILD_RELEASE_SEALED);
    let _ = change_source_row(&fixture);

    let mut digests = Vec::new();
    for reader in [&mut first, &mut second] {
        reader.send(CHILD_CONSTRUCT_TARGET);
        reader.wait_for(CHILD_TARGET_CONSTRUCTED);
        reader.send(CHILD_PUBLISH);
        reader.wait_for(CHILD_EXTERNALLY_USABLE);
        reader.wait_for(CHILD_SESSION_ACTIVE);
        let digest = reader.wait_for_prefix(CHILD_TYPED_DIGEST);
        assert_complete_digest_wire(&digest);
        digests.push(digest);
        reader.send(CHILD_QUERY);
        reader.wait_for(CHILD_QUERY_OK);
    }
    assert_eq!(digests[0], digests[1]);
    first.close();
    second.close();
}

#[test]
fn absent_best_effort_breadcrumb_keeps_the_real_lock_and_generic_holder_diagnostic() {
    let _serial = serialise_direct_file_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let fixture = seed_fixture(root.path(), "best-effort-breadcrumb-absence");
    let unusable_runtime = root.path().join(format!("runtime_file_{}", fixture.token));
    let runtime_bytes = format!("unusable-runtime:{}", fixture.identity_seed).into_bytes();
    fs::write(&unusable_runtime, &runtime_bytes).expect("create unusable runtime location");

    // The unusable pathname is this child's DEFAULT per-user runtime location,
    // which is where a reader writes itself down. Handing it only as the
    // channel directory would leave the child publishing normally in the real
    // default, and this journey would be about a reader that CAN be named.
    let mut reader = spawn_child_reader_with_default_runtime_location(&fixture, &unusable_runtime);
    assert_eq!(
        reader.wait_for_prefix(CHILD_HYDRATION_STARTED),
        format!("{CHILD_HYDRATION_STARTED}unverified")
    );
    assert_eq!(
        assert_writer_is_held_by_hydrating_readers(&fixture.path),
        (1, 0),
        "an absent breadcrumb reports one generic lock holder and no fabricated identity"
    );
    reader.send(CHILD_FINISH_HYDRATION);
    reader.wait_for(CHILD_RELEASE_SEALED);
    assert_writer_opens_after_release(&fixture.path);
    reader.send(CHILD_CONSTRUCT_TARGET);
    reader.wait_for(CHILD_TARGET_CONSTRUCTED);
    reader.send(CHILD_PUBLISH);
    reader.wait_for(CHILD_EXTERNALLY_USABLE);
    reader.wait_for(CHILD_SESSION_ACTIVE);
    let digest = reader.wait_for_prefix(CHILD_TYPED_DIGEST);
    assert_complete_digest_wire(&digest);
    reader.send(CHILD_QUERY);
    reader.wait_for(CHILD_QUERY_OK);
    reader.close();

    assert_eq!(
        fs::read(&unusable_runtime).expect("runtime location remains caller-owned"),
        runtime_bytes
    );
    assert!(
        fs::metadata(&unusable_runtime)
            .expect("unusable runtime still exists")
            .is_file()
    );
}

#[cfg(unix)]
struct PermissionRestore(Vec<(PathBuf, fs::Permissions)>);

#[cfg(unix)]
impl PermissionRestore {
    fn capture(paths: &[&Path]) -> Self {
        Self(
            paths
                .iter()
                .map(|path| {
                    (
                        (*path).to_path_buf(),
                        fs::metadata(path)
                            .unwrap_or_else(|error| {
                                panic!("permissions {}: {error}", path.display())
                            })
                            .permissions(),
                    )
                })
                .collect(),
        )
    }
}

#[cfg(unix)]
impl Drop for PermissionRestore {
    fn drop(&mut self) {
        for (path, permissions) in &self.0 {
            let _ = fs::set_permissions(path, permissions.clone());
        }
    }
}

#[cfg(unix)]
#[test]
fn read_only_file_and_directory_support_the_complete_direct_surface() {
    assert_ne!(
        nix::unistd::geteuid().as_raw(),
        0,
        "permissions proof must run as an unprivileged user"
    );
    let _serial = serialise_direct_file_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let fixture = seed_fixture(root.path(), "unix-read-only-complete-surface");
    let before = directory_manifest(&fixture.root);
    let _restore = PermissionRestore::capture(&[&fixture.root, &fixture.path]);
    fs::set_permissions(&fixture.path, fs::Permissions::from_mode(0o444))
        .expect("store is read-only");
    fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o555))
        .expect("source directory is read-only");
    let reader = direct_open(&fixture, Arc::new(ManualClock::default()));
    let _ = representative_reads(&fixture, &reader);
    let after = directory_manifest(&fixture.root);
    let mut expected = before;
    for entry in &mut expected {
        if entry.relative_path.as_os_str().is_empty() {
            entry.mode = (entry.mode & !0o777) | 0o555;
        }
        if entry.relative_path
            == fixture
                .path
                .strip_prefix(&fixture.root)
                .expect("store relative")
        {
            entry.mode = (entry.mode & !0o777) | 0o444;
        }
    }
    assert_eq!(after, expected);
}

/// The damage cases this estate drives through a fresh process, each paired
/// with the wire name its child is told and the repair diagnosis the reader
/// must answer with -- `None` where the damage carries its own non-repairable
/// diagnostic. Persistence owns the enum; pairing the cases in a table instead
/// of matching on it keeps a case persistence carries for a dedicated pin of
/// its own from failing this file to build.
const DURABLE_DAMAGE_CASES: [(DurableStoreDamage, &str, Option<DirectRepairRequiredCause>); 6] = [
    (
        DurableStoreDamage::MalformedRecord,
        "malformed-record",
        None,
    ),
    (DurableStoreDamage::LegacyLayout, "legacy-layout", None),
    (
        DurableStoreDamage::NonMonotonicCommitIndex,
        "nonmonotonic-commit-index",
        Some(DirectRepairRequiredCause::NonMonotonicCommitIndex),
    ),
    (
        DurableStoreDamage::InvalidVectorReference,
        "invalid-vector-reference",
        Some(DirectRepairRequiredCause::InvalidVectorReference),
    ),
    (
        DurableStoreDamage::RowVectorDivergence,
        "row-vector-divergence",
        Some(DirectRepairRequiredCause::RowVectorDivergence),
    ),
    (
        DurableStoreDamage::WritableSanitizerWouldChange,
        "writable-sanitization",
        Some(DirectRepairRequiredCause::WritableSanitizerWouldChange),
    ),
];

fn damage_wire(case: DurableStoreDamage) -> &'static str {
    DURABLE_DAMAGE_CASES
        .into_iter()
        .find(|(candidate, _, _)| *candidate == case)
        .map(|(_, wire, _)| wire)
        .unwrap_or_else(|| panic!("no wire name for persisted damage case {case:?}"))
}

fn parse_damage(value: &str) -> DurableStoreDamage {
    DURABLE_DAMAGE_CASES
        .into_iter()
        .find(|(_, wire, _)| *wire == value)
        .map(|(case, _, _)| case)
        .unwrap_or_else(|| panic!("unknown persisted damage case: {value}"))
}

fn repair_cause_for_damage(case: DurableStoreDamage) -> Option<DirectRepairRequiredCause> {
    DURABLE_DAMAGE_CASES
        .into_iter()
        .find(|(candidate, _, _)| *candidate == case)
        .and_then(|(_, _, cause)| cause)
}

/// Resolves a damage case this estate needs by the name persistence gives it.
/// A seam persistence does not carry yet reports itself as the missing
/// prerequisite it is -- naming the state it has to plant -- instead of taking
/// the whole target down with a build error.
fn required_damage_case(name: &str, plants: &str) -> DurableStoreDamage {
    DurableStoreDamage::ALL
        .iter()
        .copied()
        .find(|case| format!("{case:?}") == name)
        .unwrap_or_else(|| {
            panic!(
                "persistence carries no `{name}` damage case; this pin needs a production-dead \
                 Redb seam of that name that plants {plants}"
            )
        })
}

fn assert_damage_diagnostic(case: DurableStoreDamage, error: DirectFileReaderError) {
    let message = error.to_string();
    match (case, error) {
        (
            DurableStoreDamage::MalformedRecord,
            DirectFileReaderError::CorruptStore(DirectStoreDiagnostic::MalformedRecord, _),
        ) => assert!(!message.contains("--write")),
        (
            DurableStoreDamage::LegacyLayout,
            DirectFileReaderError::LegacyLayout(DirectStoreDiagnostic::LegacyLayout, _),
        ) => assert!(!message.contains("--write")),
        (case, DirectFileReaderError::DirectReadRequiresWriter { failure, cause }) => {
            assert_eq!(failure.kind(), ReadFailureKind::DirectReadRequiresWriter);
            let expected = repair_cause_for_damage(case).unwrap_or_else(|| {
                panic!("non-repairable damage has its own diagnostic: {case:?}")
            });
            assert_eq!(cause, expected);
            assert!(message.contains("--write"));
        }
        (case, actual) => panic!("wrong diagnostic for {case:?}: {actual:?}"),
    }
}

#[test]
fn direct_file_damage_child() {
    if std::env::var(CHILD_ROLE_ENV).ok().as_deref() != Some("damage") {
        return;
    }
    let case = parse_damage(&std::env::var(CHILD_DAMAGE_ENV).expect("child damage case"));
    let error = match open_for_test(
        child_path(),
        default_reader_config(Arc::new(ManualClock::default()), &child_runtime_directory()),
    ) {
        Ok(_) => panic!("damaged store must not open directly"),
        Err(error) => error,
    };
    assert_damage_diagnostic(case, error);
}

fn manifest_paths(entries: &[ManifestEntry]) -> Vec<(PathBuf, ManifestKind)> {
    entries
        .iter()
        .map(|entry| (entry.relative_path.clone(), entry.kind.clone()))
        .collect()
}

#[test]
fn every_damage_case_is_durable_and_diagnosed_in_a_fresh_process_without_repair() {
    let _serial = serialise_direct_file_test();
    for (case, _, _) in DURABLE_DAMAGE_CASES {
        let root = tempfile::tempdir().expect("scratch directory");
        let namespace = format!("fresh-process-durable-damage-{}", damage_wire(case));
        let fixture = seed_fixture(root.path(), &namespace);
        let before_preparation = directory_manifest(&fixture.root);
        prepare_durable_store_damage_for_test(&fixture.path, case)
            .unwrap_or_else(|error| panic!("persistence prepares {case:?} durably: {error}"));
        let damaged = directory_manifest(&fixture.root);
        assert_eq!(
            manifest_paths(&damaged),
            manifest_paths(&before_preparation),
            "damage fixture must not add a marker sidecar"
        );
        let status = Command::new(std::env::current_exe().expect("current test binary"))
            .arg("--exact")
            .arg("direct_file_damage_child")
            .arg("--nocapture")
            .env(CHILD_ROLE_ENV, "damage")
            .env(CHILD_PATH_ENV, &fixture.path)
            .env(CHILD_RUNTIME_ENV, &fixture.runtime_directory)
            .env(CHILD_DAMAGE_ENV, damage_wire(case))
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .expect("run fresh damage diagnosis process");
        assert!(status.success(), "fresh child diagnoses {case:?}: {status}");
        assert_companions_unchanged(&damaged, &directory_manifest(&fixture.root), &fixture);
    }
}

#[test]
fn existing_readers_and_cursor_keep_original_snapshot_and_canonical_bytes_after_write() {
    let _serial = serialise_direct_file_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let fixture = seed_fixture(root.path(), "snapshot-after-separate-writer-change");
    let clock = Arc::new(ManualClock::default());
    reset_read_image_source_events_for_test();
    let first = direct_open(&fixture, Arc::clone(&clock));
    let second = direct_open(&fixture, Arc::clone(&clock));
    let released_source_events = read_image_source_events_for_test();
    let first_before = representative_reads(&fixture, &first);
    let second_before = representative_reads(&fixture, &second);
    assert_eq!(first_before, second_before);

    let one = NonZeroUsize::new(1).expect("one row");
    let mut existing = first
        .open_cursor(&fixture.cursor_sql, &HashMap::new(), one)
        .expect("open cursor before source change");
    let mut pages = vec![existing.first_page];
    let changed = change_source_row(&fixture);
    while pages.last().expect("cursor first page").page.has_more {
        pages.push(
            existing
                .cursor
                .fetch(Some(one))
                .expect("fetch pinned cursor"),
        );
    }
    assert_eq!(pages, first_before.cursor);
    assert_eq!(representative_reads(&fixture, &first), first_before);
    assert_eq!(representative_reads(&fixture, &second), second_before);
    assert_eq!(read_image_source_events_for_test(), released_source_events);

    let fresh = direct_open(&fixture, Arc::clone(&clock));
    let fresh_digest = verified_image_digest_for_test(&fresh)
        .expect("fresh reader verifies changed complete image");
    assert_ne!(fresh_digest, first_before.typed_digest);
    assert_ne!(fresh_digest.complete, first_before.owned_snapshot.image_id);
    let fresh_row = fresh
        .execute(&fixture.relation_sql(), &fixture.relation_params())
        .expect("fresh reader sees post-release writer change");
    assert_eq!(
        fresh_row.result.rows,
        vec![vec![
            Value::Uuid(fixture.first_row().id),
            Value::Text(changed),
            Value::Text(fixture.first_row().work_ledger_blob_identity.clone()),
            Value::Text(fixture.first_row().reference_like_scalar.clone()),
        ]]
    );
}

fn assert_cancelled<T>(result: Result<T, DirectFileReaderError>, operation: &str) {
    match result {
        Err(DirectFileReaderError::Cancelled) => {}
        Err(other) => panic!("pre-cancelled {operation} returned {other:?}"),
        Ok(_) => panic!("pre-cancelled {operation} began source work"),
    }
}

fn observed_counters(context: &str) -> DirectReaderCounters {
    counters_for_test().unwrap_or_else(|error| panic!("{context}: {error}"))
}

fn assert_cursor_resources_retained(
    witness: &contextdb_engine::direct_file_reader::test_seams::DirectCursorResourceWitness,
) {
    assert!(witness.snapshot_is_retained());
    assert!(witness.memory_is_retained());
}

fn assert_cursor_resources_released(
    witness: &contextdb_engine::direct_file_reader::test_seams::DirectCursorResourceWitness,
) {
    assert!(!witness.snapshot_is_retained());
    assert!(!witness.memory_is_retained());
}

fn assert_closed_cursor_aftermath(
    cursor: &mut contextdb_engine::direct_file_reader::test_seams::DirectCursor,
    rows: NonZeroUsize,
) {
    match cursor.fetch(Some(rows)) {
        Err(DirectFileReaderError::CursorUnavailable(DirectCursorUnavailable::Closed)) => {}
        other => panic!("fetch after terminal cursor release must be closed: {other:?}"),
    }
    match cursor.close() {
        Err(DirectFileReaderError::CursorUnavailable(DirectCursorUnavailable::Closed)) => {}
        other => panic!("close after terminal cursor release must be closed: {other:?}"),
    }
}

#[test]
fn pre_cancelled_calls_stop_execute_open_and_fetch_before_bounded_source_work() {
    let _serial = serialise_direct_file_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let fixture = seed_fixture(root.path(), "caller-cancellation-before-source-work");
    reset_counters_for_test()
        .unwrap_or_else(|error| panic!("install cancellation probes: {error}"));
    let reader = direct_open(&fixture, Arc::new(ManualClock::default()));

    let execute_cancel = DirectCancellation::new();
    execute_cancel.cancel();
    let before_execute = observed_counters("before cancelled execute").bounded_source_touches;
    assert_cancelled(
        reader.execute_with_cancellation(
            &fixture.relation_sql(),
            &fixture.relation_params(),
            &execute_cancel,
        ),
        "execute",
    );
    assert_eq!(
        observed_counters("after cancelled execute").bounded_source_touches,
        before_execute
    );

    let open_cancel = DirectCancellation::new();
    open_cancel.cancel();
    let before_open = observed_counters("before cancelled open").bounded_source_touches;
    assert_cancelled(
        reader.open_cursor_with_cancellation(
            &fixture.cursor_sql,
            &HashMap::new(),
            NonZeroUsize::new(1).expect("one row"),
            &open_cancel,
        ),
        "cursor open",
    );
    assert_eq!(
        observed_counters("after cancelled open").bounded_source_touches,
        before_open
    );

    let one = NonZeroUsize::new(1).expect("one row");
    let mut opened = reader
        .open_cursor(&fixture.cursor_sql, &HashMap::new(), one)
        .expect("open cursor for cancelled fetch");
    let witness = cursor_resource_witness_for_test(&opened.cursor)
        .expect("capture cancellation-preserved resources");
    assert_cursor_resources_retained(&witness);
    let fetch_cancel = DirectCancellation::new();
    fetch_cancel.cancel();
    let before_fetch = observed_counters("before cancelled fetch");
    assert_cancelled(
        opened
            .cursor
            .fetch_with_cancellation(Some(one), &fetch_cancel),
        "cursor fetch",
    );
    let after_cancel = observed_counters("after cancelled fetch");
    assert_eq!(
        after_cancel.bounded_source_touches,
        before_fetch.bounded_source_touches
    );
    assert_eq!(after_cancel.active_cursors, before_fetch.active_cursors);
    assert_eq!(
        after_cancel.retained_cursor_bytes,
        before_fetch.retained_cursor_bytes
    );
    assert_cursor_resources_retained(&witness);
    let resumed = opened
        .cursor
        .fetch(Some(one))
        .expect("cancelled cursor resumes");
    assert_eq!(resumed.page.rows, vec![fixture.cursor_rows[1].clone()]);
    opened.cursor.close().expect("close resumed cursor");
    assert_cursor_resources_released(&witness);
}

#[test]
fn convenience_calls_create_three_fresh_independent_cancellation_tokens() {
    let _serial = serialise_direct_file_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let fixture = seed_fixture(root.path(), "independent-convenience-tokens");
    reset_counters_for_test().unwrap_or_else(|error| panic!("install token probes: {error}"));
    let reader = direct_open(&fixture, Arc::new(ManualClock::default()));
    reader
        .execute(&fixture.relation_sql(), &fixture.relation_params())
        .expect("convenience execute");
    let one = NonZeroUsize::new(1).expect("one row");
    let mut opened = reader
        .open_cursor(&fixture.cursor_sql, &HashMap::new(), one)
        .expect("convenience cursor open");
    opened
        .cursor
        .fetch(Some(one))
        .expect("convenience cursor fetch");
    let observations = cancellation_observations_for_test()
        .unwrap_or_else(|error| panic!("read token probes: {error}"));
    assert_eq!(
        observations
            .iter()
            .map(|observation| observation.operation)
            .collect::<Vec<_>>(),
        vec![
            CancellationOperation::Execute,
            CancellationOperation::CursorOpen,
            CancellationOperation::CursorFetch,
        ]
    );
    assert_eq!(
        observations
            .iter()
            .map(|observation| observation.initially_cancelled)
            .collect::<Vec<_>>(),
        vec![false, false, false]
    );
    assert_eq!(
        observations
            .iter()
            .map(|observation| observation.identity)
            .collect::<BTreeSet<_>>()
            .len(),
        3
    );
    opened.cursor.close().expect("close convenience cursor");
}

#[test]
fn terminal_page_immediately_releases_image_memory_and_cursor_state() {
    let _serial = serialise_direct_file_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let fixture = seed_fixture(root.path(), "cursor-terminal-release");
    reset_counters_for_test().unwrap_or_else(|error| panic!("install cursor probes: {error}"));
    let reader = direct_open(&fixture, Arc::new(ManualClock::default()));
    let one = NonZeroUsize::new(1).expect("one row");
    let mut opened = reader
        .open_cursor(&fixture.cursor_sql, &HashMap::new(), one)
        .expect("open accounted cursor");
    let witness = cursor_resource_witness_for_test(&opened.cursor)
        .expect("capture unforgeable active cursor resources");
    assert_cursor_resources_retained(&witness);
    let after_open = observed_counters("after cursor open");
    assert_eq!(after_open.active_cursors, 1);
    assert_ne!(after_open.retained_cursor_bytes, 0);
    let retained = after_open.retained_cursor_bytes;
    let mut terminal = opened.first_page;
    while terminal.page.has_more {
        terminal = opened
            .cursor
            .fetch(Some(one))
            .expect("fetch to terminal page");
    }
    let after_terminal = observed_counters("after terminal page");
    assert_eq!(after_terminal.active_cursors, 0);
    assert_eq!(after_terminal.retained_cursor_bytes, 0);
    assert_eq!(
        after_terminal.released_cursor_bytes - after_open.released_cursor_bytes,
        retained
    );
    assert_cursor_resources_released(&witness);
    let empty = exact_empty_page(&fixture.cursor_columns, terminal.snapshot);
    assert_eq!(
        opened
            .cursor
            .fetch(Some(one))
            .expect("empty success after terminal"),
        empty
    );
    let after_empty = observed_counters("after empty success");
    assert_eq!(after_empty, after_terminal);
    opened
        .cursor
        .close()
        .expect("close exhausted cursor is no-op");
    assert_eq!(observed_counters("after exhausted close"), after_terminal);
    opened
        .cursor
        .close()
        .expect("repeated close after exhaustion is a no-op");
    assert_eq!(
        opened
            .cursor
            .fetch(Some(one))
            .expect("empty success remains frozen"),
        empty
    );
}

#[test]
fn explicit_close_before_exhaustion_releases_and_later_fetch_is_closed() {
    let _serial = serialise_direct_file_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let fixture = seed_fixture(root.path(), "cursor-explicit-close-release");
    reset_counters_for_test().unwrap_or_else(|error| panic!("install cursor probes: {error}"));
    let reader = direct_open(&fixture, Arc::new(ManualClock::default()));
    let one = NonZeroUsize::new(1).expect("one row");
    let mut opened = reader
        .open_cursor(&fixture.cursor_sql, &HashMap::new(), one)
        .expect("open cursor");
    let witness =
        cursor_resource_witness_for_test(&opened.cursor).expect("capture explicit-close resources");
    assert_cursor_resources_retained(&witness);
    let active = observed_counters("active cursor");
    let retained = active.retained_cursor_bytes;
    opened.cursor.close().expect("explicit close");
    let closed = observed_counters("closed cursor");
    assert_eq!(closed.active_cursors, 0);
    assert_eq!(closed.retained_cursor_bytes, 0);
    assert_eq!(
        closed.released_cursor_bytes - active.released_cursor_bytes,
        retained
    );
    assert_cursor_resources_released(&witness);
    assert_closed_cursor_aftermath(&mut opened.cursor, one);
    match opened.cursor.close() {
        Err(DirectFileReaderError::CursorUnavailable(DirectCursorUnavailable::Closed)) => {}
        other => panic!("repeated explicit close must stay closed: {other:?}"),
    }
}

#[test]
fn cursor_page_byte_refusal_is_terminal_and_releases_the_cursor_charge() {
    let _serial = serialise_direct_file_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let fixture = seed_fixture(root.path(), "cursor-page-byte-terminal-refusal");
    let baseline = direct_open(&fixture, Arc::new(ManualClock::default()));
    let baseline_pages = cursor_pages(&fixture, &baseline);
    let first_bytes = baseline_pages[0].canonical_bytes.len() as u64;
    let second_bytes = baseline_pages[1].canonical_bytes.len() as u64;
    assert!(
        second_bytes > first_bytes,
        "second dynamic row is the exact wide-row gate"
    );

    reset_counters_for_test().unwrap_or_else(|error| panic!("install cursor probes: {error}"));
    let mut limits = generous_limits();
    limits.cursor_page_bytes = first_bytes;
    let reader = direct_open_with_limits(&fixture, Arc::new(ManualClock::default()), limits);
    let one = NonZeroUsize::new(1).expect("one row");
    let mut opened = reader
        .open_cursor(&fixture.cursor_sql, &HashMap::new(), one)
        .expect("first narrow page fits exactly");
    let witness =
        cursor_resource_witness_for_test(&opened.cursor).expect("capture refusal resources");
    assert_cursor_resources_retained(&witness);
    let active = observed_counters("before page refusal");
    let retained = active.retained_cursor_bytes;
    let error = match opened.cursor.fetch(Some(one)) {
        Err(DirectFileReaderError::ReadFailure(failure)) => failure,
        other => panic!("wide second page must return bounded refusal: {other:?}"),
    };
    assert_eq!(error.kind(), ReadFailureKind::OwnerLimitExceeded);
    let ReadFailureDetail::OwnerLimitExceeded(detail) = error.detail() else {
        panic!("page refusal carries owner-limit detail")
    };
    assert_eq!(detail.limit, ReadFailureLimit::CursorPageBytes);
    assert_eq!(detail.value, first_bytes);
    assert_eq!(
        detail
            .required
            .as_ref()
            .map(|required| required.required_bytes),
        Some(second_bytes)
    );
    assert_eq!(
        detail
            .statement
            .as_ref()
            .map(|statement| statement.statement.as_str()),
        Some(fixture.cursor_sql.as_str())
    );
    let refused = observed_counters("after page refusal");
    assert_eq!(refused.active_cursors, 0);
    assert_eq!(refused.retained_cursor_bytes, 0);
    assert_eq!(
        refused.released_cursor_bytes - active.released_cursor_bytes,
        retained
    );
    assert_eq!(refused.cursor_refusals, active.cursor_refusals + 1);
    assert_cursor_resources_released(&witness);
    assert_closed_cursor_aftermath(&mut opened.cursor, one);
}

#[test]
fn manual_clock_idle_expiry_is_terminal_and_releases_owned_resources() {
    let _serial = serialise_direct_file_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let fixture = seed_fixture(root.path(), "cursor-manual-idle-expiry");
    reset_counters_for_test().unwrap_or_else(|error| panic!("install cursor probes: {error}"));
    let clock = Arc::new(ManualClock::default());
    let mut limits = generous_limits();
    limits.cursor_idle_ms = 5;
    limits.cursor_lifetime_ms = 20;
    let reader = direct_open_with_limits(&fixture, Arc::clone(&clock), limits);
    let one = NonZeroUsize::new(1).expect("one row");
    let mut opened = reader
        .open_cursor(&fixture.cursor_sql, &HashMap::new(), one)
        .expect("open idle-expiry cursor");
    let witness =
        cursor_resource_witness_for_test(&opened.cursor).expect("capture idle-expiry resources");
    assert_cursor_resources_retained(&witness);
    let active = observed_counters("before idle expiry");
    let retained = active.retained_cursor_bytes;
    clock.advance(6);
    let failure = match opened.cursor.fetch(Some(one)) {
        Err(DirectFileReaderError::ReadFailure(failure)) => failure,
        other => panic!("idle-expired cursor must return typed expiry: {other:?}"),
    };
    assert_eq!(failure.kind(), ReadFailureKind::CursorExpired);
    assert_eq!(
        failure.detail(),
        &ReadFailureDetail::CursorExpired {
            expiry: CursorExpiryKind::Idle,
        }
    );
    assert_cursor_resources_released(&witness);
    let expired = observed_counters("after idle expiry");
    assert_eq!(expired.active_cursors, 0);
    assert_eq!(expired.retained_cursor_bytes, 0);
    assert_eq!(
        expired.released_cursor_bytes - active.released_cursor_bytes,
        retained
    );
    assert_closed_cursor_aftermath(&mut opened.cursor, one);
}

#[test]
fn manual_clock_lifetime_expiry_survives_activity_then_releases_owned_resources() {
    let _serial = serialise_direct_file_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let fixture = seed_fixture(root.path(), "cursor-manual-lifetime-expiry");
    reset_counters_for_test().unwrap_or_else(|error| panic!("install cursor probes: {error}"));
    let clock = Arc::new(ManualClock::default());
    let mut limits = generous_limits();
    limits.cursor_idle_ms = 7;
    limits.cursor_lifetime_ms = 10;
    let reader = direct_open_with_limits(&fixture, Arc::clone(&clock), limits);
    let one = NonZeroUsize::new(1).expect("one row");
    let mut opened = reader
        .open_cursor(&fixture.cursor_sql, &HashMap::new(), one)
        .expect("open lifetime-expiry cursor");
    let witness = cursor_resource_witness_for_test(&opened.cursor)
        .expect("capture lifetime-expiry resources");
    let active = observed_counters("before lifetime activity");
    let retained = active.retained_cursor_bytes;
    clock.advance(6);
    let active_page = opened
        .cursor
        .fetch(Some(one))
        .expect("activity refreshes idle deadline");
    assert_eq!(active_page.page.rows, vec![fixture.cursor_rows[1].clone()]);
    assert_cursor_resources_retained(&witness);
    clock.advance(5);
    let failure = match opened.cursor.fetch(Some(one)) {
        Err(DirectFileReaderError::ReadFailure(failure)) => failure,
        other => panic!("lifetime-expired cursor must return typed expiry: {other:?}"),
    };
    assert_eq!(failure.kind(), ReadFailureKind::CursorExpired);
    assert_eq!(
        failure.detail(),
        &ReadFailureDetail::CursorExpired {
            expiry: CursorExpiryKind::Lifetime,
        }
    );
    assert_cursor_resources_released(&witness);
    let expired = observed_counters("after lifetime expiry");
    assert_eq!(expired.active_cursors, 0);
    assert_eq!(expired.retained_cursor_bytes, 0);
    assert_eq!(
        expired.released_cursor_bytes - active.released_cursor_bytes,
        retained
    );
    assert_closed_cursor_aftermath(&mut opened.cursor, one);
}

/// A store seeded with the two row shapes a caller actually writes into a
/// nullable F32 vector column: one row that has an embedding, and one row that
/// has none and binds the absent embedding explicitly. The writer is closed
/// before the fixture is handed back, so the direct reader is the only thing
/// that touches the file afterwards.
struct AbsentEmbeddingStore {
    root: PathBuf,
    runtime_directory: PathBuf,
    path: PathBuf,
    table: String,
    column: String,
    embedded_id: Uuid,
    absent_id: Uuid,
    embedding: Vec<f32>,
}

const ABSENT_EMBEDDING_DIMENSION: usize = 6;

/// Seeds the store above with the vector column declared exactly as the caller
/// asks for it. `second_embedding` is the value the second row binds; `None`
/// gives that row an embedding of its own, which is what a `NOT NULL` column
/// requires and what a damage seam needs to find a legal vector cell to
/// replace.
fn seed_embedding_store(
    outer_root: &Path,
    label: &str,
    column_constraint: &str,
    second_embedding: Option<Value>,
) -> AbsentEmbeddingStore {
    let identity_seed = runtime_fixture_seed(outer_root, label);
    let nonce = seeded_uuid(&identity_seed, &["absent-embedding"]);
    let token = nonce.simple().to_string();
    let table = format!("evidence_{token}");
    let column = format!("embedding_{token}");
    let root = outer_root.join(format!("source_{token}"));
    let runtime_directory = outer_root.join(format!("runtime_{token}"));
    fs::create_dir(&root).expect("create source directory");
    fs::create_dir(&runtime_directory).expect("create runtime directory");
    let path = root.join(format!("store_{token}.db"));
    let embedded_id = seeded_uuid(&identity_seed, &["absent-embedding", "embedded"]);
    let absent_id = seeded_uuid(&identity_seed, &["absent-embedding", "absent"]);
    let embedding = dynamic_vector(embedded_id.as_u128(), ABSENT_EMBEDDING_DIMENSION, 1);

    let db = Database::open(&path)
        .unwrap_or_else(|error| panic!("create store {}: {error}", path.display()));
    let mut commit_index = Vec::new();
    execute_mutation(
        &db,
        &format!(
            "CREATE TABLE {table} (id UUID PRIMARY KEY, note TEXT NOT NULL, {column} VECTOR({ABSENT_EMBEDDING_DIMENSION}){column_constraint})"
        ),
        &HashMap::new(),
        &mut commit_index,
    );
    let second_value = second_embedding.unwrap_or_else(|| {
        Value::Vector(dynamic_vector(
            absent_id.as_u128(),
            ABSENT_EMBEDDING_DIMENSION,
            2,
        ))
    });
    for (id, note, value) in [
        (
            embedded_id,
            format!("embedded-{token}"),
            Value::Vector(embedding.clone()),
        ),
        (absent_id, format!("absent-{token}"), second_value),
    ] {
        execute_mutation(
            &db,
            &format!("INSERT INTO {table} (id, note, {column}) VALUES ($id, $note, $embedding)"),
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(id)),
                ("note".to_owned(), Value::Text(note)),
                ("embedding".to_owned(), value),
            ]),
            &mut commit_index,
        );
    }
    drop(db);

    AbsentEmbeddingStore {
        root,
        runtime_directory,
        path,
        table,
        column,
        embedded_id,
        absent_id,
        embedding,
    }
}

/// The nullable shape the round-3 pins read: the second row binds the absent
/// embedding the caller hands over, on a vector column that accepts NULL.
fn seed_absent_embedding_store(
    outer_root: &Path,
    label: &str,
    absent_embedding: Value,
) -> AbsentEmbeddingStore {
    seed_embedding_store(outer_root, label, "", Some(absent_embedding))
}

fn stored_row_by_identity<'a>(
    image: &'a DirectOwnedImage,
    table: &str,
    id: Uuid,
) -> &'a DirectStoredRow {
    image
        .relational_rows
        .iter()
        .find(|row| row.table == table && row.values.get("id") == Some(&Value::Uuid(id)))
        .unwrap_or_else(|| panic!("owned image carries row {id} of {table}"))
}

/// An absent embedding is a value, not damage. A row whose nullable F32 vector
/// column holds NULL is the ordinary state of evidence written before an
/// embedding exists, so an operator reading that file directly gets the row
/// back with the NULL still on it -- no writable open, no repair, and nothing
/// written to the file to make the read possible.
#[test]
fn a_row_with_no_embedding_keeps_the_store_directly_readable() {
    let _serial = serialise_direct_file_test();
    let scratch = tempfile::tempdir().expect("scratch directory");
    let store = seed_absent_embedding_store(
        scratch.path(),
        "absent-embedding-is-directly-readable",
        Value::Null,
    );
    let before = directory_manifest(&store.root);

    let reader = open_for_test(
        &store.path,
        default_reader_config(Arc::new(ManualClock::default()), &store.runtime_directory),
    )
    .unwrap_or_else(|error| {
        panic!("a row without an embedding keeps the store directly readable: {error:?}")
    });
    let image = owned_image_for_test(&reader).expect("observe the owned image");
    drop(reader);

    let absent = stored_row_by_identity(&image, &store.table, store.absent_id);
    assert_eq!(
        absent.values.get(&store.column),
        Some(&Value::Null),
        "the absent embedding is served back as the NULL that was stored, not dropped"
    );
    let embedded = stored_row_by_identity(&image, &store.table, store.embedded_id);
    assert_eq!(
        embedded.values.get(&store.column),
        Some(&Value::Vector(store.embedding.clone())),
        "the neighbouring embedding is unchanged"
    );
    assert_eq!(
        image
            .vectors
            .iter()
            .map(|vector| (vector.index.column.clone(), vector.values.clone()))
            .collect::<Vec<_>>(),
        vec![(store.column.clone(), store.embedding.clone())],
        "only the row that has an embedding contributes one"
    );
    assert_eq!(
        directory_manifest(&store.root),
        before,
        "reading the file directly leaves every byte of the source tree alone"
    );
}

/// The damage case that plants a value that is not an embedding in a vector
/// cell whose column accepts an absent one.
const NON_VECTOR_VALUE_IN_VECTOR_CELL: &str = "NonVectorValueInVectorCell";

/// Widening a vector column to accept an absent embedding must not widen it to
/// accept anything: a value that is not an embedding, sitting where one
/// belongs, is state the column cannot hold, so the reader answers with the
/// repair diagnosis rather than serving the row.
///
/// The value is planted through a production-dead damage seam. It used to be
/// planted through a public INSERT, which only ever worked because the vector
/// write door let a scalar through -- a defect that door now refuses, so a
/// fixture built on that write succeeding would have held the defect open.
#[test]
fn a_vector_column_holding_a_non_vector_value_still_refuses_a_direct_read() {
    let _serial = serialise_direct_file_test();
    let case = required_damage_case(
        NON_VECTOR_VALUE_IN_VECTOR_CELL,
        "a value that is not an embedding in a vector cell",
    );
    let scratch = tempfile::tempdir().expect("scratch directory");
    let store = seed_embedding_store(
        scratch.path(),
        "non-vector-value-in-a-vector-cell",
        "",
        None,
    );
    prepare_durable_store_damage_for_test(&store.path, case)
        .unwrap_or_else(|error| panic!("persistence plants {case:?} durably: {error}"));
    let damaged = directory_manifest(&store.root);

    match open_for_test(
        &store.path,
        default_reader_config(Arc::new(ManualClock::default()), &store.runtime_directory),
    ) {
        Err(DirectFileReaderError::DirectReadRequiresWriter { failure, cause }) => {
            assert_eq!(failure.kind(), ReadFailureKind::DirectReadRequiresWriter);
            assert_eq!(
                cause,
                DirectRepairRequiredCause::WritableSanitizerWouldChange
            );
        }
        Ok(_) => panic!("a value that is not an embedding must not be served directly"),
        Err(other) => panic!("a value that is not an embedding needs a writer: {other:?}"),
    }

    assert_eq!(
        directory_manifest(&store.root),
        damaged,
        "refusing the read leaves every byte of the source tree alone"
    );
}

/// Accepting an absent embedding must not blind the reader to a vector that
/// genuinely disagrees with the row that owns it: that store is still repaired
/// by a writer before it can be read.
#[test]
fn a_row_with_no_embedding_does_not_hide_a_diverged_vector() {
    let _serial = serialise_direct_file_test();
    let scratch = tempfile::tempdir().expect("scratch directory");
    let store = seed_absent_embedding_store(
        scratch.path(),
        "absent-embedding-beside-a-diverged-vector",
        Value::Null,
    );
    prepare_durable_store_damage_for_test(&store.path, DurableStoreDamage::RowVectorDivergence)
        .unwrap_or_else(|error| panic!("diverge the stored vector: {error}"));

    match open_for_test(
        &store.path,
        default_reader_config(Arc::new(ManualClock::default()), &store.runtime_directory),
    ) {
        Err(DirectFileReaderError::DirectReadRequiresWriter { failure, cause }) => {
            assert_eq!(failure.kind(), ReadFailureKind::DirectReadRequiresWriter);
            assert_eq!(cause, DirectRepairRequiredCause::RowVectorDivergence);
        }
        Ok(_) => panic!("a diverged vector must not be served directly"),
        Err(other) => panic!("a diverged vector must still ask for a writer: {other:?}"),
    }
}

/// The damage case that plants a NULL where a full-precision vector column
/// declared NOT NULL must hold a vector.
const NULL_IN_REQUIRED_VECTOR_CELL: &str = "NullInRequiredVectorCell";

/// NOT NULL still means NOT NULL on a vector column. An absent embedding is a
/// legal value only where the column accepts one; a NULL sitting in a
/// full-precision `VECTOR(n) NOT NULL` cell is state the file cannot legally
/// hold, so a writable open would rewrite it -- and that is exactly the state
/// the direct reader refuses to serve, naming the writer as the next step
/// instead of handing an operator a row the schema forbids.
#[test]
fn a_null_in_a_required_vector_cell_still_needs_a_writer() {
    let _serial = serialise_direct_file_test();
    let case = required_damage_case(
        NULL_IN_REQUIRED_VECTOR_CELL,
        "a NULL in a full-precision vector cell whose column is declared NOT NULL",
    );
    let scratch = tempfile::tempdir().expect("scratch directory");
    let store = seed_embedding_store(
        scratch.path(),
        "null-in-a-required-vector-cell",
        " NOT NULL",
        None,
    );
    prepare_durable_store_damage_for_test(&store.path, case)
        .unwrap_or_else(|error| panic!("persistence plants {case:?} durably: {error}"));
    let damaged = directory_manifest(&store.root);

    match open_for_test(
        &store.path,
        default_reader_config(Arc::new(ManualClock::default()), &store.runtime_directory),
    ) {
        Err(DirectFileReaderError::DirectReadRequiresWriter { failure, cause }) => {
            assert_eq!(failure.kind(), ReadFailureKind::DirectReadRequiresWriter);
            assert_eq!(
                cause,
                DirectRepairRequiredCause::WritableSanitizerWouldChange
            );
        }
        Ok(_) => panic!("a NULL in a NOT NULL vector cell must not be served directly"),
        Err(other) => panic!("a NULL in a NOT NULL vector cell needs a writer: {other:?}"),
    }

    assert_eq!(
        directory_manifest(&store.root),
        damaged,
        "refusing the read leaves every byte of the source tree alone"
    );
}

/// The nullable column keeps the round-3 answer while the required column
/// refuses: the two live in the same estate and the reader tells them apart by
/// the declaration, not by the value it finds.
#[test]
fn a_required_vector_column_and_a_nullable_one_are_told_apart_by_their_declaration() {
    let _serial = serialise_direct_file_test();
    let scratch = tempfile::tempdir().expect("scratch directory");
    let nullable =
        seed_absent_embedding_store(scratch.path(), "nullable-beside-required", Value::Null);
    let reader = open_for_test(
        &nullable.path,
        default_reader_config(
            Arc::new(ManualClock::default()),
            &nullable.runtime_directory,
        ),
    )
    .unwrap_or_else(|error| panic!("a nullable vector column still reads directly: {error:?}"));
    let image = owned_image_for_test(&reader).expect("observe the owned image");
    drop(reader);
    assert_eq!(
        stored_row_by_identity(&image, &nullable.table, nullable.absent_id)
            .values
            .get(&nullable.column),
        Some(&Value::Null),
        "the nullable column serves the absent embedding back as NULL"
    );
}

/// The two quantizations that move a column's embedding out of the relational
/// row and into an independently durable vector entry, each with the SQL word
/// it is declared by and a label a fixture path can carry.
const QUANTIZED_DECLARATIONS: [(&str, &str, VectorQuantization); 2] = [
    ("SQ8", "sq8", VectorQuantization::SQ8),
    ("SQ4", "sq4", VectorQuantization::SQ4),
];

/// The trailing part of a vector column's declaration: the quantization that
/// keeps its embedding outside the row, plus whatever nullability the caller
/// wants on top of it.
fn quantized_declaration(quantization: &str, nullability: &str) -> String {
    format!(" WITH (quantization = '{quantization}'){nullability}")
}

/// Every vector the image carries for one column, ordered by the row that
/// owns it.
fn image_vectors_for_column<'a>(
    image: &'a DirectOwnedImage,
    table: &str,
    column: &str,
) -> Vec<&'a DirectStoredVector> {
    let mut vectors = image
        .vectors
        .iter()
        .filter(|vector| vector.index.table == table && vector.index.column == column)
        .collect::<Vec<_>>();
    vectors.sort_by_key(|vector| vector.row_id);
    vectors
}

/// A quantized column keeps its embedding in a vector entry and leaves the
/// relational cell empty, so what an operator reading the file directly gets
/// back is the entry, not the cell. That is today's contract for a column
/// declared `NOT NULL`, and it has to be pinned as a positive: without it, a
/// reader that refused every quantized column would satisfy the refusal pin
/// below while serving nobody.
#[test]
fn a_required_quantized_column_reads_back_directly_when_its_vector_is_there() {
    let _serial = serialise_direct_file_test();
    let scratch = tempfile::tempdir().expect("scratch directory");
    for (word, label, quantization) in QUANTIZED_DECLARATIONS {
        let store = seed_embedding_store(
            scratch.path(),
            &format!("required-{label}-with-its-durable-vector"),
            &quantized_declaration(word, " NOT NULL"),
            None,
        );
        let before = directory_manifest(&store.root);

        let reader = open_for_test(
            &store.path,
            default_reader_config(Arc::new(ManualClock::default()), &store.runtime_directory),
        )
        .unwrap_or_else(|error| {
            panic!("a required {word} column reads back through the direct reader: {error:?}")
        });
        let image = owned_image_for_test(&reader).expect("observe the owned image");
        drop(reader);

        let embedded = stored_row_by_identity(&image, &store.table, store.embedded_id);
        let absent = stored_row_by_identity(&image, &store.table, store.absent_id);
        let quantized = stored_vector_value(&store.embedding, quantization);
        assert_eq!(
            embedded.values.get(&store.column),
            Some(&Value::Vector(quantized.clone())),
            "the required column is served from the vector entry its placeholder stands for"
        );
        match absent.values.get(&store.column) {
            Some(Value::Vector(vector)) => {
                assert_eq!(
                    vector.len(),
                    ABSENT_EMBEDDING_DIMENSION,
                    "the neighbouring row keeps the column's declared dimension"
                );
                assert_ne!(
                    vector, &quantized,
                    "each row is served its own embedding, not a shared one"
                );
            }
            other => panic!(
                "every row of a required {word} column is served the embedding it promises: {other:?}"
            ),
        }
        let vectors = image_vectors_for_column(&image, &store.table, &store.column);
        assert_eq!(
            vectors
                .iter()
                .map(|vector| (vector.row_id, vector.quantization))
                .collect::<Vec<_>>(),
            {
                let mut owners = vec![
                    (embedded.row_id, direct_quantization(quantization)),
                    (absent.row_id, direct_quantization(quantization)),
                ];
                owners.sort_by_key(|(row_id, _)| *row_id);
                owners
            },
            "every row of a required {word} column owns the embedding the column promises"
        );
        let embedded_vector = vectors
            .iter()
            .find(|vector| vector.row_id == embedded.row_id)
            .expect("the embedded row owns a vector");
        assert_eq!(
            embedded_vector.values, quantized,
            "the embedding arrives as the column's own quantizer stored it"
        );
        assert_eq!(
            directory_manifest(&store.root),
            before,
            "reading the file directly leaves every byte of the source tree alone"
        );
    }
}

/// The other half of the same reading. Where a quantized column accepts an
/// absent embedding, a row that binds NULL never had a vector entry written
/// for it, and absence is a value rather than damage -- so the store stays
/// directly readable and the neighbouring embedding is served unchanged. The
/// reader has to tell this apart from a required column by the declaration,
/// not by finding an empty cell.
#[test]
fn a_nullable_quantized_column_with_no_embedding_stays_directly_readable() {
    let _serial = serialise_direct_file_test();
    let scratch = tempfile::tempdir().expect("scratch directory");
    for (word, label, quantization) in QUANTIZED_DECLARATIONS {
        let store = seed_embedding_store(
            scratch.path(),
            &format!("nullable-{label}-with-an-absent-embedding"),
            &quantized_declaration(word, ""),
            Some(Value::Null),
        );
        let before = directory_manifest(&store.root);

        let reader = open_for_test(
            &store.path,
            default_reader_config(Arc::new(ManualClock::default()), &store.runtime_directory),
        )
        .unwrap_or_else(|error| panic!("a nullable {word} column still reads directly: {error:?}"));
        let image = owned_image_for_test(&reader).expect("observe the owned image");
        drop(reader);

        let embedded = stored_row_by_identity(&image, &store.table, store.embedded_id);
        let absent = stored_row_by_identity(&image, &store.table, store.absent_id);
        assert_eq!(
            absent.values.get(&store.column),
            None,
            "a nullable {word} column serves the row without an embedding on it"
        );
        assert!(
            matches!(absent.values.get("note"), Some(Value::Text(note)) if note.starts_with("absent-")),
            "the rest of the row is served whole"
        );
        let quantized = stored_vector_value(&store.embedding, quantization);
        assert_eq!(
            embedded.values.get(&store.column),
            Some(&Value::Vector(quantized.clone())),
            "the neighbouring embedding is served unchanged"
        );
        let vectors = image_vectors_for_column(&image, &store.table, &store.column);
        assert_eq!(
            vectors
                .iter()
                .map(|vector| (vector.row_id, vector.values.clone()))
                .collect::<Vec<_>>(),
            vec![(embedded.row_id, quantized)],
            "only the row that has an embedding contributes one"
        );
        assert_eq!(
            directory_manifest(&store.root),
            before,
            "reading the file directly leaves every byte of the source tree alone"
        );
    }
}

/// The damage case that removes the separately durable vector entry a
/// required quantized cell's stored placeholder stands for.
const MISSING_QUANTIZED_VECTOR_ENTRY: &str = "MissingQuantizedVectorEntry";

/// A quantized vector column declared `NOT NULL` promises every live row an
/// embedding. The relational cell holds NULL only as a placeholder for the
/// separately durable vector entry the row is served from, so damage that
/// removes that entry leaves the file unable to keep the promise the schema
/// records. An operator reading such a file directly must be told a writer is
/// the next step, not handed a row whose required column has quietly
/// vanished -- the same declaration-integrity promise a required
/// full-precision column already keeps.
#[test]
fn a_required_quantized_column_without_its_durable_vector_still_needs_a_writer() {
    let _serial = serialise_direct_file_test();
    let case = required_damage_case(
        MISSING_QUANTIZED_VECTOR_ENTRY,
        "a live row on a required quantized vector column whose separately durable vector entry \
         has been removed",
    );
    let scratch = tempfile::tempdir().expect("scratch directory");
    for (word, label, _) in QUANTIZED_DECLARATIONS {
        let store = seed_embedding_store(
            scratch.path(),
            &format!("required-{label}-without-its-durable-vector"),
            &quantized_declaration(word, " NOT NULL"),
            None,
        );
        prepare_durable_store_damage_for_test(&store.path, case)
            .unwrap_or_else(|error| panic!("persistence plants {case:?} durably: {error}"));
        let damaged = directory_manifest(&store.root);

        match open_for_test(
            &store.path,
            default_reader_config(Arc::new(ManualClock::default()), &store.runtime_directory),
        ) {
            Err(DirectFileReaderError::DirectReadRequiresWriter { failure, .. }) => {
                assert_eq!(failure.kind(), ReadFailureKind::DirectReadRequiresWriter);
            }
            Ok(_) => panic!(
                "a required {word} column whose embedding the file no longer holds must not be \
                 served directly"
            ),
            Err(other) => panic!(
                "a required {word} column whose embedding the file no longer holds needs a \
                 writer: {other:?}"
            ),
        }

        assert_eq!(
            directory_manifest(&store.root),
            damaged,
            "refusing the read leaves every byte of the source tree alone"
        );
    }
}
