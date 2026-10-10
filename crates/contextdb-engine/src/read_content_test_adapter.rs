//! On-demand committed-store inspection, compiled only with `test-seams`.
//! This disposable validation view is never a production reader, cache or root.
//! The integration test owns the canonical encoding and hashes. No maintained
//! identity is manufactured here. Materialization cost is outside measured open.
use crate::database::{Database, MaintenancePolicy};
use crate::direct_file_reader::{
    DirectChangeState, DirectConfigurationState, DirectEventsStatus, DirectMaintenanceStatus,
    DirectStoredEdge, DirectStoredRow, DirectStoredVector, DirectSyncSource, DirectSyncState,
    DirectTableSyncPolicy, DirectTypedImageDigest, DirectVectorQuantization,
};
use crate::persistence::ReadPersistenceImageParts;
use crate::read_image::{project_events, project_schema};
use contextdb_core::{
    IndexKind, Lsn, MaintainedCounts, Result, RowId, SortDirection, TableMeta, TxId, Value,
    VectorIndexRef, VectorQuantization,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct CommittedContentForTest {
    pub maintained_digest: DirectTypedImageDigest,
    pub relational_rows: Vec<DirectStoredRow>,
    pub graph_edges: Vec<DirectStoredEdge>,
    pub vectors: Vec<DirectStoredVector>,
    pub changes: DirectChangeState,
    pub sync: DirectSyncState,
    pub configuration: DirectConfigurationState,
    pub events: DirectEventsStatus,
    pub maintenance: DirectMaintenanceStatus,
    /// Full durable bytes for state whose projection otherwise exposes only
    /// counts. Tests can prove a queue/audit/config mutation actually persisted.
    pub durable_auxiliary: BTreeMap<String, Vec<u8>>,
    /// Independently enumerated logical records used to recompute the
    /// maintained family roots. This list contains no identity-table bytes.
    pub logical_records: Vec<LogicalContentRecordForTest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalContentRecordForTest {
    pub family: u8,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

fn logical_key(parts: &[&[u8]]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for part in parts {
        encoded.extend_from_slice(&(part.len() as u64).to_le_bytes());
        encoded.extend_from_slice(part);
    }
    encoded
}

fn logical_json<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let value = serde_json::to_value(value)
        .map_err(|error| contextdb_core::Error::Other(error.to_string()))?;
    serde_json::to_vec(&independently_canonicalize_json(value))
        .map_err(|error| contextdb_core::Error::Other(error.to_string()))
}

fn logical_hash_parts(domain: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(domain);
    for part in parts {
        hash.update(&(part.len() as u64).to_le_bytes());
        hash.update(part);
    }
    *hash.finalize().as_bytes()
}

fn independently_canonicalize_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => serde_json::Value::Array(
            values
                .into_iter()
                .map(independently_canonicalize_json)
                .collect(),
        ),
        serde_json::Value::Object(values) => {
            let mut entries = values.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, independently_canonicalize_json(value)))
                    .collect(),
            )
        }
        scalar => scalar,
    }
}

fn logical_record(
    family: u8,
    key: impl Into<Vec<u8>>,
    value: impl Into<Vec<u8>>,
) -> LogicalContentRecordForTest {
    LogicalContentRecordForTest {
        family,
        key: key.into(),
        value: value.into(),
    }
}

#[derive(Serialize)]
struct IndependentRelationalIndexCountKey {
    table: String,
    columns: Vec<(String, SortDirection)>,
    prefix: Vec<(bool, Value)>,
}

#[derive(Serialize, Deserialize)]
struct IndependentRelationalIndexPosition {
    key: Vec<(bool, Value)>,
    row_id: RowId,
    created_tx: TxId,
}

fn independently_reconstruct_relational_index_posting_records(
    table_meta: &HashMap<String, TableMeta>,
    rows: &[DirectStoredRow],
) -> Result<Vec<LogicalContentRecordForTest>> {
    const POSTING_MAGIC: &[u8] = b"\xffcontextdb-index-posting\0";
    let mut records = Vec::new();
    let mut schemas = table_meta.iter().collect::<Vec<_>>();
    schemas.sort_by(|left, right| left.0.cmp(right.0));
    for (table, meta) in schemas {
        for declaration in meta.indexes.iter().filter(|declaration| {
            matches!(declaration.kind, IndexKind::Auto | IndexKind::UserDeclared)
                && !declaration.columns.is_empty()
        }) {
            for row in rows.iter().filter(|row| row.table == *table) {
                let key = declaration
                    .columns
                    .iter()
                    .map(|(column, direction)| {
                        (
                            matches!(direction, SortDirection::Desc),
                            row.values.get(column).cloned().unwrap_or(Value::Null),
                        )
                    })
                    .collect();
                let order_key = bincode::serde::encode_to_vec(
                    IndependentRelationalIndexPosition {
                        key,
                        row_id: row.row_id,
                        // Current logical membership must not depend on the
                        // transaction that happened to create the row.
                        created_tx: TxId(0),
                    },
                    bincode::config::standard(),
                )
                .map_err(|error| contextdb_core::Error::Other(error.to_string()))?;
                let mut logical_key = POSTING_MAGIC.to_vec();
                logical_key.extend_from_slice(&(table.len() as u32).to_be_bytes());
                logical_key.extend_from_slice(table.as_bytes());
                logical_key.extend_from_slice(&(declaration.name.len() as u32).to_be_bytes());
                logical_key.extend_from_slice(declaration.name.as_bytes());
                logical_key.push(1);
                logical_key.extend_from_slice(&order_key);
                records.push(logical_record(5, logical_key, []));
            }
        }
    }
    Ok(records)
}

/// The oracle's own reading of the rule that decides whether a count exists at
/// all: a live-row count over a column list is not maintained when the table
/// already declares a primary key or a unique constraint over exactly that
/// column SET and no column in it admits NULL, because such a count can only be
/// 0 or 1 and the index postings already say which.
///
/// Written here from the durable declaration, deliberately NOT by calling the
/// production predicate, so that a production rule which starts or stops
/// dropping a count disagrees with this reconstruction instead of matching it
/// by construction. Column order is irrelevant to the bound, so the comparison
/// is over sets; a NULL-admitting column takes no part in the uniqueness check,
/// so it disqualifies the whole list.
fn independent_prefix_is_constraint_bounded(
    meta: &TableMeta,
    columns: &[(String, SortDirection)],
) -> bool {
    let names = columns
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<BTreeSet<_>>();
    if names.len() != columns.len() {
        return false;
    }
    let declared_not_null = names.iter().all(|wanted| {
        meta.columns
            .iter()
            .any(|column| column.name == *wanted && (!column.nullable || column.primary_key))
    });
    if !declared_not_null {
        return false;
    }
    let primary_key = meta
        .primary_key_columns
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if !primary_key.is_empty() && primary_key == names {
        return true;
    }
    if meta.unique_constraints.iter().any(|constraint| {
        constraint
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            == names
    }) {
        return true;
    }
    columns.len() == 1
        && meta
            .columns
            .iter()
            .any(|column| column.name == columns[0].0 && (column.primary_key || column.unique))
}

fn independently_reconstruct_relational_index_count_records(
    table_meta: &HashMap<String, TableMeta>,
    rows: &[DirectStoredRow],
) -> Result<Vec<LogicalContentRecordForTest>> {
    const COUNT_NAMESPACE: &[u8] = b"rel-index-count-v2";
    const READY_NAMESPACE: &[u8] = b"rel-index-count-ready-v1";
    const READY_VALUE: &[u8] = b"v3";

    let mut records = Vec::new();
    let mut schemas = table_meta.iter().collect::<Vec<_>>();
    schemas.sort_by(|left, right| left.0.cmp(right.0));
    for (table, meta) in schemas {
        records.push(logical_record(
            10,
            logical_key(&[READY_NAMESPACE, table.as_bytes()]),
            READY_VALUE,
        ));

        // The durable declaration is the oracle's source. Do not call the
        // production auto-index, membership or count-key helpers: this
        // reconstruction must disagree when maintained count state is wrong.
        //
        // Read independently of `maintained_counts::maintained_count_member`:
        // an absent clause means the table maintains NO prefix count (owner
        // ruling 2026-09-19), `ALL PREFIXES` means every prefix of every
        // index, and an explicit set means exactly the listed column lists.
        let declared_member = |columns: &[(String, SortDirection)]| -> bool {
            match meta.maintained_counts.as_ref() {
                None => false,
                Some(MaintainedCounts::None) => false,
                Some(MaintainedCounts::AllPrefixes) => true,
                Some(MaintainedCounts::Lists(lists)) => {
                    lists.iter().any(|list| list.as_slice() == columns)
                }
            }
        };
        let mut declarations = Vec::new();
        for index in meta.indexes.iter().filter(|index| {
            matches!(index.kind, IndexKind::Auto | IndexKind::UserDeclared)
                && !index.columns.is_empty()
        }) {
            if !declarations.contains(&index.columns) {
                declarations.push(index.columns.clone());
            }
        }

        let mut counts = BTreeMap::<Vec<u8>, HashSet<RowId>>::new();
        for row in rows.iter().filter(|row| row.table == *table) {
            for columns in &declarations {
                let directed = columns
                    .iter()
                    .map(|(column, direction)| {
                        (
                            matches!(direction, SortDirection::Desc),
                            row.values.get(column).cloned().unwrap_or(Value::Null),
                        )
                    })
                    .collect::<Vec<_>>();
                for prefix_len in 1..=directed.len() {
                    if independent_prefix_is_constraint_bounded(meta, &columns[..prefix_len]) {
                        continue;
                    }
                    if !declared_member(&columns[..prefix_len]) {
                        continue;
                    }
                    let encoded = bincode::serde::encode_to_vec(
                        &IndependentRelationalIndexCountKey {
                            table: table.to_string(),
                            columns: columns[..prefix_len].to_vec(),
                            prefix: directed[..prefix_len].to_vec(),
                        },
                        bincode::config::standard(),
                    )
                    .map_err(|error| contextdb_core::Error::Other(error.to_string()))?;
                    counts.entry(encoded).or_default().insert(row.row_id);
                }
            }
        }
        for (key, row_ids) in counts {
            let count = u64::try_from(row_ids.len())
                .map_err(|error| contextdb_core::Error::Other(error.to_string()))?;
            records.push(logical_record(
                10,
                logical_key(&[COUNT_NAMESPACE, &key]),
                count.to_le_bytes(),
            ));
        }
    }
    Ok(records)
}

fn independently_enumerate_auxiliary_records(
    parts: &ReadPersistenceImageParts,
) -> Result<Vec<LogicalContentRecordForTest>> {
    let mut records = Vec::new();

    let edges = project_edges(parts)?;
    for edge in edges {
        let properties = edge.properties.into_iter().collect::<BTreeMap<_, _>>();
        records.push(logical_record(
            1,
            logical_key(&[
                edge.edge_type.as_bytes(),
                edge.source.as_bytes(),
                edge.target.as_bytes(),
            ]),
            logical_json(&properties)?,
        ));
    }

    for vector in project_vectors(parts) {
        let family = match vector.quantization {
            DirectVectorQuantization::F32 => 2,
            DirectVectorQuantization::Sq8 => 3,
            DirectVectorQuantization::Sq4 => 4,
        };
        let mut value = Vec::with_capacity(8 + vector.values.len() * 4);
        value.extend_from_slice(&(vector.values.len() as u64).to_le_bytes());
        for component in vector.values {
            value.extend_from_slice(&component.to_bits().to_le_bytes());
        }
        records.push(logical_record(
            family,
            logical_key(&[
                vector.index.table.as_bytes(),
                vector.index.column.as_bytes(),
                &vector.row_id.0.to_le_bytes(),
            ]),
            value,
        ));
    }

    for vector in &parts.vector_entries {
        let quantization = parts
            .table_meta
            .get(&vector.index.table)
            .and_then(|meta| {
                meta.columns
                    .iter()
                    .find(|column| column.name == vector.index.column)
                    .map(|column| column.quantization)
            })
            .unwrap_or_default();
        let quantization_tag = match quantization {
            VectorQuantization::F32 => 0,
            VectorQuantization::SQ8 => 1,
            VectorQuantization::SQ4 => 2,
        };
        let key = logical_hash_parts(
            b"contextdb-vector-version-key-v1",
            &[
                vector.index.table.as_bytes(),
                vector.index.column.as_bytes(),
                &vector.row_id.0.to_le_bytes(),
                &vector.created_tx.0.to_le_bytes(),
                &vector.lsn.0.to_le_bytes(),
                &[quantization_tag],
            ],
        );
        let mut value = Vec::with_capacity(8 + vector.vector.len() * 4);
        value.extend_from_slice(&(vector.vector.len() as u64).to_le_bytes());
        for component in &vector.vector {
            value.extend_from_slice(&component.to_bits().to_le_bytes());
        }
        records.push(logical_record(8, key, value));
    }

    let mut schemas = parts.table_meta.iter().collect::<Vec<_>>();
    schemas.sort_by(|left, right| left.0.cmp(right.0));
    for (name, meta) in schemas {
        // One schema record, in the one family that holds schema records. The
        // published `schema_and_indexes`, `sync` and `status` fields derive from
        // that family's root rather than holding a copy of the record.
        records.push(logical_record(6, name.as_bytes(), logical_json(meta)?));
    }

    for ((table, row_id), source_lsn) in &parts.sync_source_lsns {
        let mut key = table.as_bytes().to_vec();
        key.extend_from_slice(&row_id.0.to_be_bytes());
        let kind = parts
            .sync_source_kinds
            .get(&(table.clone(), *row_id))
            .map(|kind| match kind {
                contextdb_relational::store::SyncSourceKind::Pulled => 0,
                contextdb_relational::store::SyncSourceKind::AcceptedLocal => 1,
                contextdb_relational::store::SyncSourceKind::AcceptedLocalPending => 2,
            })
            .unwrap_or(0);
        let mut value = source_lsn.0.to_le_bytes().to_vec();
        value.push(kind);
        records.push(logical_record(7, key, value));
    }

    // Each history record is committed at the position its author's writer
    // stores it under: a change at `{lsn:020}:{ordinal:06}`, its ordinal
    // counting from zero within its LSN; a schema statement at `{lsn:020}`
    // when it is alone at its LSN and at `{lsn:020}:{ordinal:06}` otherwise;
    // a commit at its LSN.
    let mut at_lsn = BTreeMap::<u64, usize>::new();
    for (lsn, _) in &parts.ddl_log {
        *at_lsn.entry(lsn.0).or_default() += 1;
    }
    let mut ddl_ordinal = (None, 0usize);
    for (lsn, change) in &parts.ddl_log {
        ddl_ordinal = match ddl_ordinal {
            (Some(previous), ordinal) if previous == lsn.0 => (Some(lsn.0), ordinal + 1),
            _ => (Some(lsn.0), 0),
        };
        let position = if at_lsn[&lsn.0] == 1 {
            format!("{:020}", lsn.0)
        } else {
            format!("{:020}:{:06}", lsn.0, ddl_ordinal.1)
        };
        records.push(logical_record(
            8,
            logical_history_key(2, position.as_bytes()),
            logical_json(change)?,
        ));
    }
    for (lsn, tx) in &parts.commit_index {
        records.push(logical_record(
            8,
            logical_history_key(0, &lsn.0.to_be_bytes()),
            tx.0.to_le_bytes(),
        ));
    }

    for (key, value) in &parts.config_values {
        // One config record, in the Configuration family alone.
        records.push(logical_record(9, key.as_bytes(), value.clone()));
    }
    for (key, value) in &parts.sink_audit {
        records.push(logical_record(
            10,
            logical_key(&[b"sink-audit", key.as_bytes()]),
            value.clone(),
        ));
    }
    for (key, value) in &parts.trigger_audit {
        records.push(logical_record(
            10,
            logical_key(&[b"trigger-audit", key.as_bytes()]),
            value.clone(),
        ));
    }
    for (key, stamp) in &parts.trigger_audit_stamps {
        records.push(logical_record(
            10,
            logical_key(&[b"trigger-stamp", key.as_bytes()]),
            stamp.to_le_bytes(),
        ));
    }
    for (sink, entries) in &parts.sink_queues {
        for (id, value) in entries {
            let key = format!("{}:{sink}:{id}", sink.len());
            let entry = crate::persistence::RedbPersistence::decode_config_value::<
                crate::database::event_bus::SinkQueueEntry,
            >(value)?;
            records.push(logical_record(
                10,
                logical_key(&[b"sink-queue", key.as_bytes()]),
                logical_json(&entry)?,
            ));
        }
    }
    Ok(records)
}

// The oracle decodes physical bodies into its own wire representation. It
// does not read identity leaves or call the production version-manifest hash.
#[derive(Serialize, Deserialize)]
struct IndependentVersion {
    row_id: RowId,
    values: HashMap<String, IndependentStoredValue>,
    created_tx: TxId,
    deleted_tx: Option<TxId>,
    lsn: Lsn,
    created_at: Option<contextdb_core::Wallclock>,
}

#[derive(Serialize, Deserialize)]
enum IndependentStoredValue {
    Plain(Value),
    Vector(IndependentStoredVector),
}

#[derive(Serialize, Deserialize)]
enum IndependentStoredVector {
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

fn independently_reconstruct_version_records(
    bodies: Vec<(String, Vec<u8>)>,
) -> Result<Vec<LogicalContentRecordForTest>> {
    let mut records = Vec::new();
    for (table, body) in bodies {
        let (row, consumed): (IndependentVersion, usize) =
            bincode::serde::decode_from_slice(&body, bincode::config::standard())
                .map_err(|error| contextdb_core::Error::Other(error.to_string()))?;
        if consumed != body.len() {
            return Err(contextdb_core::Error::Other(
                "oracle row has trailing bytes".into(),
            ));
        }
        let mut identity = Vec::new();
        for value in [row.row_id.0, row.created_tx.0, row.lsn.0] {
            identity.extend_from_slice(&value.to_be_bytes());
        }
        let mut hash = blake3::Hasher::new();
        hash.update(b"contextdb.relational.version.v1");
        hash.update(&(table.len() as u32).to_be_bytes());
        hash.update(table.as_bytes());
        hash.update(&identity);
        for value in [
            row.created_at.map(|stamp| stamp.0),
            row.deleted_tx.map(|tx| tx.0),
        ] {
            hash.update(&[u8::from(value.is_some())]);
            hash.update(&value.unwrap_or(0).to_be_bytes());
        }
        hash.update(&(row.values.len() as u64).to_be_bytes());
        for (name, value) in row.values.into_iter().collect::<BTreeMap<_, _>>() {
            let encoded = bincode::serde::encode_to_vec(value, bincode::config::standard())
                .map_err(|error| contextdb_core::Error::Other(error.to_string()))?;
            hash.update(&(name.len() as u32).to_be_bytes());
            hash.update(name.as_bytes());
            hash.update(&encoded[..1]);
            hash.update(&((encoded.len() - 1) as u64).to_be_bytes());
            hash.update(&encoded[1..]);
        }
        let mut coordinate = vec![1];
        coordinate.extend_from_slice(&(table.len() as u32).to_be_bytes());
        coordinate.extend_from_slice(table.as_bytes());
        coordinate.extend_from_slice(&identity);
        coordinate.push(u8::from(row.deleted_tx.is_some()));
        coordinate.extend_from_slice(&row.deleted_tx.map(|tx| tx.0).unwrap_or(0).to_be_bytes());
        coordinate.extend_from_slice(hash.finalize().as_bytes());
        records.push(logical_record(8, logical_history_key(3, &coordinate), [1]));
    }
    Ok(records)
}

/// A history record's logical key: the ordered-history marker, its kind
/// (0 commit, 1 change, 2 schema) and its stored position.
fn logical_history_key(kind: u8, position: &[u8]) -> Vec<u8> {
    let mut key = b"\xffcontextdb-history\0".to_vec();
    key.push(kind);
    key.extend_from_slice(position);
    key
}

pub fn committed_content_for_test(
    path: &Path,
    operation_read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    declared_read_memory: Arc<crate::vector_read_memory::DeclaredReadMemory>,
) -> Result<CommittedContentForTest> {
    let open_context = crate::executor::bounded::ReadOpenContext::new(
        operation_read_memory.clone(),
        crate::read_session::ReadSessionOptions::default().limits,
        crate::read_session::read_clock(),
        contextdb_core::read_contract::OwnerReadCancellation::new(),
    )?;
    let load = crate::persistence::load_read_image_at_registration_with_context(
        path,
        None,
        None,
        None,
        open_context,
    )
    .map_err(|e| contextdb_core::Error::Other(e.to_string()))?;
    let (mut image, receipt, retained_source) = load.into_parts();
    receipt
        .validate_released()
        .map_err(|e| contextdb_core::Error::Other(e.to_string()))?;
    let maintained_digest = image.content_identity_for_test();
    image.hydrate_logical_content_for_test(&retained_source)?;
    let parts = image.into_runtime_parts(retained_source);
    let identity_table_meta = parts.table_meta.clone();
    let version_records = independently_reconstruct_version_records(
        parts
            .retained_source
            .relational_version_bodies_for_test(&identity_table_meta)?,
    )?;
    let mut logical_records = independently_enumerate_auxiliary_records(&parts)?;
    logical_records.extend(version_records);
    for (position, body) in parts.retained_source.change_history_bodies_for_test()? {
        logical_records.push(logical_record(
            8,
            logical_history_key(1, position.as_bytes()),
            body,
        ));
    }
    let encode = |value: &dyn std::fmt::Debug| format!("{value:?}").into_bytes();
    // These are witnesses of committed auxiliary mutations, not canonical root
    // encodings. Ordered table entries are supplied by the persistence decoder.
    let mut queue_contents = BTreeMap::new();
    for (sink, entries) in &parts.sink_queues {
        let mut decoded = Vec::new();
        for (key, bytes) in entries {
            let entry = crate::persistence::RedbPersistence::decode_config_value::<
                crate::database::event_bus::SinkQueueEntry,
            >(bytes)?;
            // serde_json's map representation sorts object keys, so a map's
            // per-process bincode iteration order cannot fake changed content.
            let value = serde_json::to_value(entry)
                .map_err(|e| contextdb_core::Error::Other(e.to_string()))?;
            decoded.push((*key, value));
        }
        queue_contents.insert(sink.clone(), decoded);
    }
    let canonical_queues = serde_json::to_vec(&queue_contents)
        .map_err(|e| contextdb_core::Error::Other(e.to_string()))?;
    let auxiliary = BTreeMap::from([
        ("configuration".into(), encode(&parts.config_values)),
        ("sink_audit".into(), encode(&parts.sink_audit)),
        ("trigger_audit".into(), encode(&parts.trigger_audit)),
        ("trigger_stamps".into(), encode(&parts.trigger_audit_stamps)),
        ("sink_queues".into(), encode(&parts.sink_queues)),
        ("queue_contents".into(), canonical_queues),
    ]);
    let graph = project_edges(&parts)?;
    let vectors = project_vectors(&parts);
    let commits = parts
        .commit_index
        .iter()
        .map(|(lsn, tx)| (*lsn, *tx))
        .collect();
    let memory_limit = parts
        .config_values
        .iter()
        .find(|(key, _)| key == "memory_limit")
        .map(|(_, bytes)| crate::persistence::RedbPersistence::decode_config_value::<usize>(bytes))
        .transpose()?;
    let vector_columns = parts
        .table_meta
        .iter()
        .map(|(table, meta)| {
            (
                table.clone(),
                meta.columns
                    .iter()
                    .filter(|column| {
                        matches!(column.column_type, contextdb_core::ColumnType::Vector(_))
                    })
                    .map(|column| column.name.clone())
                    .collect::<HashSet<_>>(),
            )
        })
        .collect::<HashMap<_, _>>();
    let accountant = parts.memory.accountant.clone();
    let read_memory = operation_read_memory;
    // Test-only use of the existing committed decoder target provides metadata
    // and history semantics; no replacement full image enters a reader's life.
    let database = Database::open_committed_image(
        parts,
        Arc::new(crate::plugin::CorePlugin),
        accountant,
        declared_read_memory,
    )?;
    let mut content = project_test_content(
        &database,
        graph,
        vectors,
        commits,
        maintained_digest,
        read_memory,
    )?;
    content.configuration.memory_limit_bytes = memory_limit;
    content.durable_auxiliary = auxiliary;
    for row in &content.relational_rows {
        let values = row
            .values
            .iter()
            .filter(|(name, value)| {
                !matches!(value, Value::Vector(_))
                    && !vector_columns
                        .get(&row.table)
                        .is_some_and(|columns| columns.contains(name.as_str()))
            })
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        logical_records.push(logical_record(
            0,
            logical_key(&[row.table.as_bytes(), &row.row_id.0.to_le_bytes()]),
            logical_json(&values)?,
        ));
    }
    logical_records.extend(independently_reconstruct_relational_index_count_records(
        &identity_table_meta,
        &content.relational_rows,
    )?);
    logical_records.extend(independently_reconstruct_relational_index_posting_records(
        &identity_table_meta,
        &content.relational_rows,
    )?);
    content.logical_records = logical_records;
    Ok(content)
}

fn project_edges(parts: &ReadPersistenceImageParts) -> Result<Vec<DirectStoredEdge>> {
    // One edge identity can carry several stored occurrences -- a later write
    // that adds properties does not erase the one that introduced the edge --
    // and an image holds current state, so the latest occurrence is the one
    // an edge reads as.
    let mut current = BTreeMap::new();
    let budget: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget> =
        Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
    let mut cursor =
        crate::persistence::graph_read::GraphReadRoot::Image(parts.retained_source.clone())
            .cursor(
                false,
                std::ops::Bound::Unbounded,
                std::ops::Bound::Unbounded,
                budget,
            )?;
    while let Some(window) = cursor.next_edge()? {
        let edge = window.edge();
        if edge.deleted_tx.is_some() {
            continue;
        }
        let identity = (edge.edge_type.clone(), edge.source, edge.target);
        let order = (edge.lsn, edge.created_tx);
        match current.get(&identity) {
            Some((seen, _)) if *seen >= order => {}
            _ => {
                current.insert(identity, (order, edge.clone()));
            }
        }
    }
    let mut edges = current
        .into_values()
        .map(|(_, edge)| DirectStoredEdge {
            source: edge.source,
            target: edge.target,
            edge_type: edge.edge_type.clone(),
            properties: edge
                .properties
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
        })
        .collect::<Vec<_>>();
    edges.sort_by(|left, right| {
        (&left.edge_type, left.source, left.target).cmp(&(
            &right.edge_type,
            right.source,
            right.target,
        ))
    });
    Ok(edges)
}

fn project_vectors(parts: &ReadPersistenceImageParts) -> Vec<DirectStoredVector> {
    let mut vectors = parts
        .current_vectors
        .iter()
        .map(|((index, row_id), values)| DirectStoredVector {
            index: index.clone(),
            row_id: *row_id,
            quantization: quantization_for_index(parts, index),
            values: values.clone(),
        })
        .collect::<Vec<_>>();
    vectors.sort_by(|left, right| {
        (&left.index.table, &left.index.column, left.row_id).cmp(&(
            &right.index.table,
            &right.index.column,
            right.row_id,
        ))
    });
    vectors
}

fn quantization_for_index(
    parts: &ReadPersistenceImageParts,
    index: &VectorIndexRef,
) -> DirectVectorQuantization {
    parts
        .table_meta
        .get(&index.table)
        .and_then(|meta| {
            meta.columns
                .iter()
                .find(|column| column.name == index.column)
                .map(|column| direct_quantization(column.quantization))
        })
        .unwrap_or(DirectVectorQuantization::F32)
}

fn direct_quantization(quantization: VectorQuantization) -> DirectVectorQuantization {
    match quantization {
        VectorQuantization::F32 => DirectVectorQuantization::F32,
        VectorQuantization::SQ8 => DirectVectorQuantization::Sq8,
        VectorQuantization::SQ4 => DirectVectorQuantization::Sq4,
    }
}

fn project_test_content(
    database: &Database,
    graph_edges: Vec<DirectStoredEdge>,
    vectors: Vec<DirectStoredVector>,
    commit_index: Vec<(Lsn, TxId)>,
    maintained_digest: DirectTypedImageDigest,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<CommittedContentForTest> {
    let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
    let snapshot_id = database.snapshot();
    let mut table_names = database.table_names();
    table_names.sort();

    let mut relational_rows = Vec::new();
    for table in &table_names {
        for row in database.scan_with_read_memory(
            table,
            snapshot_id,
            &mut vector_copy_owner,
            read_memory.clone(),
        )? {
            relational_rows.push(DirectStoredRow {
                table: table.clone(),
                row_id: row.row_id,
                values: row.values.into_iter().collect(),
            });
        }
    }
    relational_rows
        .sort_by(|left, right| (&left.table, left.row_id).cmp(&(&right.table, right.row_id)));

    let schemas = table_names
        .iter()
        .filter_map(|table| {
            database
                .table_meta(table)
                .map(|meta| project_schema(table, &meta))
        })
        .collect::<Vec<_>>();

    let (changes, arrivals) = database.changes_since_with_arrivals_with_read_memory(
        Lsn(0),
        &mut vector_copy_owner,
        read_memory,
    )?;
    let changes_state = DirectChangeState {
        current_lsn: database.current_lsn(),
        committed_watermark: database.committed_watermark(),
        next_tx: database.next_tx(),
        commit_index,
        rows: changes.rows.clone(),
        edges: changes.edges.clone(),
        vectors: changes.vectors.clone(),
        ddl: changes.ddl.clone(),
        ddl_lsn: changes.ddl_lsn.clone(),
    };

    let mut sources = arrivals
        .into_iter()
        .map(|(row_lsn, source_lsn)| DirectSyncSource {
            row_lsn,
            source_lsn,
        })
        .collect::<Vec<_>>();
    sources.sort_by_key(|source| source.row_lsn);
    let mut tables = schemas
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
    tables.sort_by(|left, right| left.table.cmp(&right.table));
    let sync = DirectSyncState {
        watermark: database.sync_watermark(),
        sources,
        tables,
    };

    let configuration = DirectConfigurationState {
        memory_limit_bytes: database.memory_limit(),
        disk_limit_bytes: database.disk_limit(),
        schemas,
    };

    let events = project_events(database);

    // Runtime fields are the fresh handle's own: this session started no
    // maintenance and inherits no caller's policy. Retention and currency are
    // declarations, so they come from the image's own schemas.
    let status = database.maintenance_status();
    let maintenance = DirectMaintenanceStatus {
        policy: match status.policy {
            MaintenancePolicy::EngineOwned => "engine_owned".to_owned(),
            MaintenancePolicy::CallerDriven => "caller_driven".to_owned(),
        },
        running: false,
        retention_enabled: status.retention_enabled,
        currency_compaction_enabled: status.currency_compaction_enabled,
        active_maintenance_loops: 0,
    };

    Ok(CommittedContentForTest {
        maintained_digest,
        relational_rows,
        graph_edges,
        vectors,
        changes: changes_state,
        sync,
        configuration,
        events,
        maintenance,
        durable_auxiliary: BTreeMap::new(),
        logical_records: Vec::new(),
    })
}
