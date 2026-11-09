//! Storage-layer routes on `RedbPersistence`. A consumer reaches the type
//! through `RedbPersistence::recreate`, which makes a fresh store; the seeded
//! fixture then writes one table declaration, one row with its change-log
//! entry, one DDL entry, one configuration value, one commit-index entry and
//! one sink-queue entry through the same public writes.

use super::{Row, call};
use contextdb_core::{AdjEntry, Lsn, RowId, TableMeta, TxId, Value, VectorEntry, VersionedRow};
use contextdb_engine::composite_store::ChangeLogEntry;
use contextdb_engine::persistence::RedbPersistence;
use contextdb_engine::sync_types::DdlChange;
use contextdb_engine::{Database, TriggerAuditEntry, TriggerAuditStatus};
use contextdb_tx::write_set::WriteSet;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

struct Raw {
    _dir: tempfile::TempDir,
    path: PathBuf,
    store: RedbPersistence,
    meta: TableMeta,
    ddl: DdlChange,
}

/// The `notes` declaration and its CREATE, as an ordinary database records
/// them.
fn declaration() -> (TableMeta, DdlChange) {
    let db = Database::open_memory();
    super::fixture::run(
        &db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, bucket INTEGER, body TEXT)",
    );
    let meta = db.table_meta("notes").expect("fixture declaration");
    let ddl = db
        .ddl_log_since(Lsn(0))
        .expect("fixture DDL")
        .into_iter()
        .next()
        .expect("fixture CREATE");
    (meta, ddl)
}

fn empty() -> Raw {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path = dir.path().join("raw.db");
    // `recreate` replaces a store that exists, so the fixture creates one first.
    Database::open(&path)
        .expect("fixture store")
        .close()
        .expect("fixture store closes");
    let store = RedbPersistence::recreate(&path).expect("fixture storage store");
    let (meta, ddl) = declaration();
    Raw {
        _dir: dir,
        path,
        store,
        meta,
        ddl,
    }
}

fn row(id: u64) -> VersionedRow {
    VersionedRow {
        row_id: RowId(id),
        values: HashMap::from([
            ("id".to_string(), Value::Int64(id as i64)),
            ("bucket".to_string(), Value::Int64(1)),
            ("body".to_string(), Value::Text(format!("stored-{id}"))),
        ]),
        created_tx: TxId(1),
        deleted_tx: None,
        lsn: Lsn(1),
        created_at: None,
    }
}

fn write_set(id: u64) -> WriteSet {
    let mut ws = WriteSet::default();
    ws.relational_inserts.push(("notes".to_string(), row(id)));
    ws.commit_lsn = Some(Lsn(id));
    ws
}

fn insert_entry(id: u64) -> ChangeLogEntry {
    ChangeLogEntry::RowInsert {
        table: "notes".to_string(),
        row_id: RowId(id),
        lsn: Lsn(id),
    }
}

fn audit() -> TriggerAuditEntry {
    TriggerAuditEntry {
        trigger_name: "notes_written".to_string(),
        firing_tx: TxId(1),
        firing_lsn: Lsn(1),
        depth: 0,
        cascade_row_count: 0,
        status: TriggerAuditStatus::Fired,
    }
}

fn seeded() -> Raw {
    let raw = empty();
    raw.store
        .flush_table_meta("notes", &raw.meta)
        .expect("seed the table declaration");
    raw.store
        .flush_data_with_logs(&write_set(1), &[insert_entry(1)])
        .expect("seed one row and its change-log entry");
    raw.store
        .append_ddl_log(Lsn(1), &raw.ddl)
        .expect("seed one DDL entry");
    raw.store
        .flush_config_value("route.probe", &7u64)
        .expect("seed one configuration value");
    raw.store
        .flush_commit_index_entries(&BTreeMap::from([(Lsn(1), TxId(1))]))
        .expect("seed one commit-index entry");
    raw.store
        .update_sink_queue_entry(
            "note_sink",
            None,
            Some((1, &"queued".to_string())),
            false,
            1,
        )
        .expect("seed one sink-queue entry");
    raw.store
        .append_trigger_audit(0, &audit())
        .expect("seed one trigger audit entry");
    raw
}

fn vector() -> VectorEntry {
    VectorEntry {
        index: contextdb_core::VectorIndexRef::new("notes", "embedding"),
        row_id: RowId(1),
        vector: vec![1.0, 0.0, 0.0],
        created_tx: TxId(1),
        deleted_tx: None,
        lsn: Lsn(1),
    }
}

fn edge() -> AdjEntry {
    AdjEntry {
        source: super::fixture::source_node(),
        target: super::fixture::target_node(),
        edge_type: super::fixture::EDGE_TYPE.to_string(),
        properties: HashMap::new(),
        created_tx: TxId(1),
        deleted_tx: None,
        lsn: Lsn(1),
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub fn rows() -> Vec<Row> {
    vec![
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::recreate",
            drive: || {
                let dir = tempfile::tempdir().expect("fixture directory");
                let path = dir.path().join("raw.db");
                call(|| RedbPersistence::recreate(&path))
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::is_legacy_format_store",
            drive: || {
                let raw = seeded();
                let path = raw.path.clone();
                drop(raw.store);
                call(|| RedbPersistence::is_legacy_format_store(&path))
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::flush_table_meta",
            drive: || {
                let raw = empty();
                call(|| raw.store.flush_table_meta("notes", &raw.meta))
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::flush_table_meta_with_config_values_and_append_ddl_log",
            drive: || {
                let raw = empty();
                call(|| {
                    raw.store
                        .flush_table_meta_with_config_values_and_append_ddl_log(
                            "notes",
                            &raw.meta,
                            vec![("route.probe", 7u64.to_le_bytes().to_vec())],
                            Lsn(1),
                            &raw.ddl,
                        )
                })
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::remove_table_meta",
            drive: || {
                let raw = seeded();
                call(|| raw.store.remove_table_meta("notes"))
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::load_all_table_meta",
            drive: || {
                let raw = seeded();
                call(|| raw.store.load_all_table_meta())
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::flush_data",
            drive: || {
                let raw = seeded();
                call(|| raw.store.flush_data(&write_set(2)))
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::flush_data_with_logs",
            drive: || {
                let raw = seeded();
                call(|| {
                    raw.store
                        .flush_data_with_logs(&write_set(2), &[insert_entry(2)])
                })
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::append_change_log",
            drive: || {
                let raw = seeded();
                call(|| raw.store.append_change_log(Lsn(2), &[insert_entry(2)]))
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::load_change_log",
            drive: || {
                let raw = seeded();
                call(|| raw.store.load_change_log())
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::append_ddl_log",
            drive: || {
                let raw = seeded();
                call(|| raw.store.append_ddl_log(Lsn(2), &raw.ddl))
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::load_ddl_log",
            drive: || {
                let raw = seeded();
                call(|| raw.store.load_ddl_log())
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::flush_config_value",
            drive: || {
                let raw = seeded();
                call(|| raw.store.flush_config_value("route.other", &9u64))
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::flush_encoded_config_values",
            drive: || {
                let raw = seeded();
                call(|| {
                    raw.store
                        .flush_encoded_config_values(vec![("route.encoded", vec![1, 2, 3])])
                })
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::flush_encoded_config_values_and_append_ddl_log",
            drive: || {
                let raw = seeded();
                call(|| {
                    raw.store.flush_encoded_config_values_and_append_ddl_log(
                        vec![("route.encoded", vec![1, 2, 3])],
                        Lsn(2),
                        std::slice::from_ref(&raw.ddl),
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::load_config_value",
            drive: || {
                let raw = seeded();
                call(|| raw.store.load_config_value::<u64>("route.probe"))
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::load_config_values_with_prefix",
            drive: || {
                let raw = seeded();
                call(|| raw.store.load_config_values_with_prefix::<u64>("route."))
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::remove_config_value",
            drive: || {
                let raw = seeded();
                call(|| raw.store.remove_config_value("route.probe"))
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::flush_commit_index_entries",
            drive: || {
                let raw = seeded();
                call(|| {
                    raw.store
                        .flush_commit_index_entries(&BTreeMap::from([(Lsn(2), TxId(2))]))
                })
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::rewrite_commit_index",
            drive: || {
                let raw = seeded();
                call(|| {
                    raw.store
                        .rewrite_commit_index(&BTreeMap::from([(Lsn(1), TxId(1))]))
                })
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::load_commit_index",
            drive: || {
                let raw = seeded();
                call(|| raw.store.load_commit_index())
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::remove_commit_index_entries_scoped",
            drive: || {
                let raw = seeded();
                call(|| raw.store.remove_commit_index_entries_scoped(&[Lsn(1)]))
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::update_sink_queue_entry",
            drive: || {
                let raw = seeded();
                call(|| {
                    raw.store.update_sink_queue_entry(
                        "note_sink",
                        Some(1),
                        Some((2, &"requeued".to_string())),
                        false,
                        0,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::load_sink_queue",
            drive: || {
                let raw = seeded();
                call(|| raw.store.load_sink_queue::<String>("note_sink"))
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::remove_sink_queue_entries",
            drive: || {
                let raw = seeded();
                call(|| raw.store.remove_sink_queue_entries("note_sink", &[1], 0))
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::append_trigger_audit",
            drive: || {
                let raw = seeded();
                call(|| raw.store.append_trigger_audit(1, &audit()))
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::load_trigger_audit_history",
            drive: || {
                let raw = seeded();
                call(|| raw.store.load_trigger_audit_history())
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::load_trigger_audit_state",
            drive: || {
                let raw = seeded();
                call(|| raw.store.load_trigger_audit_state(16))
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::prune_trigger_audit_history",
            drive: || {
                let raw = seeded();
                call(|| raw.store.prune_trigger_audit_history(u64::MAX))
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::load_sync_source_lsns",
            drive: || {
                let raw = seeded();
                call(|| raw.store.load_sync_source_lsns())
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::load_sync_source_kinds",
            drive: || {
                let raw = seeded();
                call(|| raw.store.load_sync_source_kinds())
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::prune_versions_scoped",
            drive: || {
                let raw = seeded();
                call(|| {
                    raw.store.prune_versions_scoped(
                        &[("notes".to_string(), RowId(1), TxId(1), Lsn(1))],
                        &[("notes".to_string(), RowId(1), Lsn(1))],
                        &[],
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::prune_vector_versions_scoped",
            drive: || {
                let raw = seeded();
                call(|| raw.store.prune_vector_versions_scoped(&[]))
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::prune_vectors_and_edges_scoped",
            drive: || {
                let raw = seeded();
                call(|| {
                    raw.store
                        .prune_vectors_and_edges_scoped(&[vector()], &[edge()])
                })
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::rewrite_vectors",
            drive: || {
                let raw = seeded();
                call(|| raw.store.rewrite_vectors(&[vector()]))
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::rewrite_graph_edges",
            drive: || {
                let raw = seeded();
                call(|| raw.store.rewrite_graph_edges(&[edge()]))
            },
        },
        Row {
            route: "contextdb_engine::persistence::RedbPersistence::rewrite_pruned_state",
            drive: || {
                let raw = seeded();
                call(|| {
                    raw.store.rewrite_pruned_state(
                        &HashMap::from([("notes".to_string(), vec![row(1)])]),
                        &[vector()],
                        &[edge()],
                    )
                })
            },
        },
    ]
}
