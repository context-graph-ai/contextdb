//! `Database` routes: opens, SQL, transactions, rows, graph, vectors,
//! history, maintenance, limits, sync bookkeeping, custody, callbacks,
//! export and snapshot inspection.

use super::fixture::{
    EDGE_TYPE, Store, TENANT, memory_store, none, notes_index, params, run, source_node,
    target_node,
};
use super::{Row, call, call_unit};
use contextdb_core::types::{ContextId, Principal, ScopeLabel};
use contextdb_core::{Incarnation, Lsn, TenantId, Value, VectorPartitionKey};
use contextdb_engine::database::{DeliveryManifest, SnapshotInspector};
use contextdb_engine::plugin::CorePlugin;
use contextdb_engine::sync_types::NaturalKey;
use contextdb_engine::vector_read_memory::DeclaredReadMemory;
use contextdb_engine::{
    Database, DatabaseOpenOptions, SemanticQuery, SinkEvent, TriggerAuditFilter,
    VectorPartitionRef, WriterVectorSession,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

const READER_LIMIT: u64 = 64 * 1024 * 1024;

fn note(id: i64) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_string(), Value::Int64(id)),
        ("bucket".to_string(), Value::Int64(id % 2)),
        ("body".to_string(), Value::Text(format!("note-{id}"))),
    ])
}

fn key(id: i64) -> NaturalKey {
    NaturalKey::single("id".to_string(), Value::Int64(id))
}

fn contexts() -> BTreeSet<ContextId> {
    BTreeSet::from([ContextId::new(uuid::Uuid::from_u128(7))])
}

fn labels() -> BTreeSet<ScopeLabel> {
    BTreeSet::from([ScopeLabel::new("route-owner")])
}

/// A store whose root table declares a delivery manifest over its member
/// table, with one committed unit.
fn custody_store() -> Store {
    let store = Store::open();
    run(
        &store.db,
        "CREATE TABLE records (id INTEGER PRIMARY KEY, body TEXT) SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE DELIVERY MANIFEST OVER record_parts",
    );
    run(
        &store.db,
        "CREATE TABLE record_parts (id INTEGER PRIMARY KEY, record_id INTEGER REFERENCES records(id), body TEXT) SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST",
    );
    store
}

fn stage_unit(db: &Database, root: i64) -> contextdb_core::TxId {
    let tx = db.begin().expect("custody fixture transaction");
    db.insert_row(
        tx,
        "records",
        HashMap::from([
            ("id".to_string(), Value::Int64(root)),
            ("body".to_string(), Value::Text("root".into())),
        ]),
    )
    .expect("custody fixture root row");
    db.insert_row(
        tx,
        "record_parts",
        HashMap::from([
            ("id".to_string(), Value::Int64(root * 10)),
            ("record_id".to_string(), Value::Int64(root)),
            ("body".to_string(), Value::Text("member".into())),
        ]),
    )
    .expect("custody fixture member row");
    tx
}

fn manifest(root: i64) -> DeliveryManifest<'static> {
    DeliveryManifest {
        root_table: "records",
        root_key: key(root),
        members: vec![("record_parts", key(root * 10))],
    }
}

/// A store with a declared trigger, event type, sink and route, so callback
/// registration and audit history have something to bind to.
fn event_store() -> Store {
    let store = Store::open();
    for sql in [
        "CREATE TRIGGER notes_written ON notes WHEN INSERT",
        "CREATE EVENT TYPE note_written WHEN INSERT ON notes",
        "CREATE SINK note_sink TYPE callback",
        "CREATE ROUTE note_route EVENT note_written TO note_sink",
    ] {
        run(&store.db, sql);
    }
    store
}

pub fn rows() -> Vec<Row> {
    vec![
        // Opens. A file open of an existing populated store decodes its
        // catalog, history and saved graphs.
        Row {
            route: "contextdb_engine::Database::open",
            drive: || {
                let (_dir, path) = Store::closed();
                call(|| Database::open(&path))
            },
        },
        Row {
            route: "contextdb_engine::Database::open_with_options",
            drive: || {
                let (_dir, path) = Store::closed();
                call(|| Database::open_with_options(&path, DatabaseOpenOptions::default()))
            },
        },
        Row {
            route: "contextdb_engine::Database::open_with_writer_vector_session",
            drive: || {
                let (_dir, path) = Store::closed();
                let session = WriterVectorSession::default();
                call(|| {
                    Database::open_with_writer_vector_session(
                        &path,
                        DatabaseOpenOptions::default(),
                        &session,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::open_with_contexts",
            drive: || {
                let (_dir, path) = Store::closed();
                call(|| Database::open_with_contexts(&path, contexts()))
            },
        },
        Row {
            route: "contextdb_engine::Database::open_with_scope_labels",
            drive: || {
                let (_dir, path) = Store::closed();
                call(|| Database::open_with_scope_labels(&path, labels()))
            },
        },
        Row {
            route: "contextdb_engine::Database::open_as_principal",
            drive: || {
                let (_dir, path) = Store::closed();
                call(|| Database::open_as_principal(&path, Principal::Agent("route".into())))
            },
        },
        Row {
            route: "contextdb_engine::Database::open_with_constraints",
            drive: || {
                let (_dir, path) = Store::closed();
                call(|| {
                    Database::open_with_constraints(
                        &path,
                        Some(contexts()),
                        Some(labels()),
                        Some(Principal::Agent("route".into())),
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::open_with_plugin",
            drive: || {
                let (_dir, path) = Store::closed();
                call(|| Database::open_with_plugin(&path, Arc::new(CorePlugin)))
            },
        },
        Row {
            route: "contextdb_engine::database::open_with_startup_limits",
            drive: || {
                let (_dir, path) = Store::closed();
                call(|| {
                    contextdb_engine::database::open_with_startup_limits(
                        &path,
                        Arc::new(CorePlugin),
                        Some(512 * 1024 * 1024),
                        None,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::database::open_memory_with_startup_limit",
            drive: || {
                call(|| {
                    let db = contextdb_engine::database::open_memory_with_startup_limit(
                        Arc::new(CorePlugin),
                        Some(512 * 1024 * 1024),
                    )?;
                    super::fixture::populate(&db);
                    db.execute("SELECT id, body FROM notes ORDER BY id", &none())
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::open_memory",
            drive: || {
                call(|| {
                    let db = Database::open_memory();
                    super::fixture::populate(&db);
                    db.execute("SELECT id, body FROM notes ORDER BY id", &none())
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::open_memory_with_contexts",
            drive: || {
                call(|| {
                    let db = Database::open_memory_with_contexts(contexts());
                    db.execute(
                        "CREATE TABLE plain (id INTEGER PRIMARY KEY, body TEXT)",
                        &none(),
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::open_memory_with_scope_labels",
            drive: || {
                call(|| {
                    let db = Database::open_memory_with_scope_labels(labels());
                    db.execute(
                        "CREATE TABLE plain (id INTEGER PRIMARY KEY, body TEXT)",
                        &none(),
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::open_memory_as_principal",
            drive: || {
                call(|| {
                    let db = Database::open_memory_as_principal(Principal::Agent("route".into()));
                    db.execute(
                        "CREATE TABLE plain (id INTEGER PRIMARY KEY, body TEXT)",
                        &none(),
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::open_memory_with_constraints",
            drive: || {
                call(|| {
                    let db = Database::open_memory_with_constraints(
                        Some(contexts()),
                        Some(labels()),
                        Some(Principal::Agent("route".into())),
                    );
                    db.execute(
                        "CREATE TABLE plain (id INTEGER PRIMARY KEY, body TEXT)",
                        &none(),
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::open_memory_with_plugin",
            drive: || {
                call(|| {
                    let db = Database::open_memory_with_plugin(Arc::new(CorePlugin))?;
                    super::fixture::populate(&db);
                    db.execute("SELECT id FROM notes", &none())
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::force_reset",
            drive: || {
                let (_dir, path) = Store::closed();
                call(|| Database::force_reset(&path))
            },
        },
        // SQL.
        Row {
            route: "contextdb_engine::Database::execute",
            drive: || {
                let store = Store::open();
                call(|| {
                    store
                        .db
                        .execute("SELECT id, body FROM notes ORDER BY id", &none())?;
                    store.db.execute(
                        "SELECT id, bucket, embedding FROM breadth_rows ORDER BY id",
                        &none(),
                    )?;
                    store.db.execute(
                        "SELECT id FROM notes ORDER BY embedding <=> $query LIMIT 2",
                        &params(&[("query", Value::Vector(vec![1.0, 1.0, 0.5]))]),
                    )?;
                    store.db.execute(
                        "INSERT INTO notes (id, bucket, body) VALUES (40, 0, 'added')",
                        &none(),
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::execute_with_read_memory",
            drive: || {
                let store = Store::open();
                call(|| {
                    let memory = store.db.declared_reader_memory()?;
                    store.db.execute_with_read_memory(
                        "SELECT id, body FROM notes ORDER BY id",
                        &none(),
                        memory,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::execute_at_snapshot",
            drive: || {
                let store = Store::open();
                let snapshot = store.db.snapshot();
                call(|| {
                    store.db.execute_at_snapshot(
                        "SELECT id, body FROM notes ORDER BY id",
                        &none(),
                        snapshot,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::execute_in_tx",
            drive: || {
                let store = Store::open();
                let tx = store.db.begin().expect("fixture transaction");
                call(|| {
                    store.db.execute_in_tx(
                        tx,
                        "INSERT INTO notes (id, bucket, body) VALUES (41, 1, 'in tx')",
                        &none(),
                    )?;
                    store
                        .db
                        .execute_in_tx(tx, "SELECT id, body FROM notes ORDER BY id", &none())
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::explain",
            drive: || {
                let store = Store::open();
                call(|| store.db.explain("SELECT id FROM notes WHERE bucket = 1"))
            },
        },
        Row {
            route: "contextdb_engine::Database::explain_output",
            drive: || {
                let store = Store::open();
                call(|| {
                    store
                        .db
                        .explain_output("SELECT id FROM notes WHERE bucket = 1")
                })
            },
        },
        // Transactions and rows.
        Row {
            route: "contextdb_engine::Database::commit",
            drive: || {
                let store = Store::open();
                let tx = store.db.begin().expect("fixture transaction");
                store
                    .db
                    .insert_row(tx, "notes", note(42))
                    .expect("fixture staged row");
                call(|| store.db.commit(tx))
            },
        },
        Row {
            route: "contextdb_engine::Database::commit_returning_txid",
            drive: || {
                let store = Store::open();
                let tx = store.db.begin().expect("fixture transaction");
                store
                    .db
                    .insert_row(tx, "notes", note(43))
                    .expect("fixture staged row");
                call(|| store.db.commit_returning_txid(tx))
            },
        },
        Row {
            route: "contextdb_engine::Database::insert_row",
            drive: || {
                let store = Store::open();
                let tx = store.db.begin().expect("fixture transaction");
                call(|| store.db.insert_row(tx, "notes", note(44)))
            },
        },
        Row {
            route: "contextdb_engine::Database::upsert_row",
            drive: || {
                let store = Store::open();
                let tx = store.db.begin().expect("fixture transaction");
                call(|| store.db.upsert_row(tx, "notes", "id", note(2)))
            },
        },
        Row {
            route: "contextdb_engine::Database::delete_row",
            drive: || {
                let store = Store::open();
                let row = store.row_id(3);
                let tx = store.db.begin().expect("fixture transaction");
                call(|| store.db.delete_row(tx, "notes", row))
            },
        },
        Row {
            route: "contextdb_engine::Database::guard_row_conditions_in_tx",
            drive: || {
                let store = Store::open();
                let tx = store.db.begin().expect("fixture transaction");
                call(|| {
                    store.db.guard_row_conditions_in_tx(
                        tx,
                        "notes",
                        "id",
                        &Value::Int64(2),
                        &[("body".to_string(), Value::Text("revised".into()))],
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::scan",
            drive: || {
                let store = Store::open();
                let snapshot = store.db.snapshot();
                call(|| store.db.scan("notes", snapshot))
            },
        },
        Row {
            route: "contextdb_engine::Database::scan_filter",
            drive: || {
                let store = Store::open();
                let snapshot = store.db.snapshot();
                call(|| store.db.scan_filter("notes", snapshot, &|_| true))
            },
        },
        Row {
            route: "contextdb_engine::Database::point_lookup",
            drive: || {
                let store = Store::open();
                let snapshot = store.db.snapshot();
                call(|| {
                    store
                        .db
                        .point_lookup("notes", "id", &Value::Int64(2), snapshot)
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::snapshot_at",
            drive: || {
                let store = Store::open();
                let lsn = store.db.current_lsn();
                call(|| {
                    let snapshot = store.db.snapshot_at(lsn);
                    store
                        .db
                        .execute_at_snapshot("SELECT id FROM notes", &none(), snapshot)
                })
            },
        },
        // Graph.
        Row {
            route: "contextdb_engine::Database::insert_edge",
            drive: || {
                let store = Store::open();
                let tx = store.db.begin().expect("fixture transaction");
                call(|| {
                    store.db.insert_edge(
                        tx,
                        target_node(),
                        source_node(),
                        EDGE_TYPE.to_string(),
                        HashMap::new(),
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::delete_edge",
            drive: || {
                let store = Store::open();
                let tx = store.db.begin().expect("fixture transaction");
                call(|| {
                    store
                        .db
                        .delete_edge(tx, source_node(), target_node(), EDGE_TYPE)
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::edge_count",
            drive: || {
                let store = Store::open();
                let snapshot = store.db.snapshot();
                call(|| store.db.edge_count(source_node(), EDGE_TYPE, snapshot))
            },
        },
        Row {
            route: "contextdb_engine::Database::get_edge_properties",
            drive: || {
                let store = Store::open();
                let snapshot = store.db.snapshot();
                call(|| {
                    store
                        .db
                        .get_edge_properties(source_node(), target_node(), EDGE_TYPE, snapshot)
                })
            },
        },
        // Vectors.
        Row {
            route: "contextdb_engine::Database::insert_vector",
            drive: || {
                let store = Store::open();
                let tx = store.db.begin().expect("fixture transaction");
                let row = store
                    .db
                    .insert_row(tx, "notes", note(45))
                    .expect("fixture staged row");
                call(|| {
                    store
                        .db
                        .insert_vector(tx, notes_index(), row, vec![4.0, 1.0, 0.5])
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::delete_vector",
            drive: || {
                let store = Store::open();
                let row = store.row_id(1);
                let tx = store.db.begin().expect("fixture transaction");
                call(|| store.db.delete_vector(tx, notes_index(), row))
            },
        },
        Row {
            route: "contextdb_engine::Database::query_vector",
            drive: || {
                let store = Store::open();
                let snapshot = store.db.snapshot();
                call(|| {
                    store
                        .db
                        .query_vector(notes_index(), &[1.0, 1.0, 0.5], 2, None, snapshot)
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::semantic_search",
            drive: || {
                let store = Store::open();
                call(|| {
                    store.db.semantic_search(SemanticQuery::new(
                        "notes",
                        "embedding",
                        vec![1.0, 1.0, 0.5],
                        2,
                    ))
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::has_live_vector",
            drive: || {
                let store = Store::open();
                let row = store.row_id(1);
                let snapshot = store.db.snapshot();
                call(|| store.db.has_live_vector(row, snapshot))
            },
        },
        Row {
            route: "contextdb_engine::Database::live_vector_entry",
            drive: || {
                let store = Store::open();
                let row = store.row_id(1);
                let snapshot = store.db.snapshot();
                call(|| store.db.live_vector_entry(row, snapshot))
            },
        },
        Row {
            route: "contextdb_engine::Database::detach_writer_vector_session",
            drive: || {
                let (_dir, path) = Store::closed();
                let session = WriterVectorSession::default();
                let db = Database::open_with_writer_vector_session(
                    &path,
                    DatabaseOpenOptions::default(),
                    &session,
                )
                .expect("fixture session open");
                run(
                    &db,
                    "INSERT INTO notes (id, bucket, body, embedding) VALUES (46, 0, 'tail', [5.0, 1.0, 0.5])",
                );
                let route =
                    VectorPartitionRef::new(notes_index(), VectorPartitionKey::unpartitioned());
                call_unit(|| db.detach_writer_vector_session(&session, &route))
            },
        },
        Row {
            route: "contextdb_engine::Database::reattach_writer_vector_session",
            drive: || {
                let (_dir, path) = Store::closed();
                let session = WriterVectorSession::default();
                let db = Database::open_with_writer_vector_session(
                    &path,
                    DatabaseOpenOptions::default(),
                    &session,
                )
                .expect("fixture session open");
                let route =
                    VectorPartitionRef::new(notes_index(), VectorPartitionKey::unpartitioned());
                db.detach_writer_vector_session(&session, &route);
                call_unit(|| db.reattach_writer_vector_session(&session))
            },
        },
        // History and change feeds.
        Row {
            route: "contextdb_engine::Database::change_log_since",
            drive: || {
                let store = Store::open();
                call(|| store.db.change_log_since(Lsn(0)))
            },
        },
        Row {
            route: "contextdb_engine::Database::change_log_rows_since",
            drive: || {
                let store = Store::open();
                call(|| store.db.change_log_rows_since(Lsn(0)))
            },
        },
        Row {
            route: "contextdb_engine::Database::ddl_log_since",
            drive: || {
                let store = Store::open();
                call(|| store.db.ddl_log_since(Lsn(0)))
            },
        },
        Row {
            route: "contextdb_engine::Database::changes_since",
            drive: || {
                let store = Store::open();
                call(|| store.db.changes_since(Lsn(0)))
            },
        },
        Row {
            route: "contextdb_engine::Database::changes_since_with_arrivals",
            drive: || {
                let store = Store::open();
                call(|| store.db.changes_since_with_arrivals(Lsn(0)))
            },
        },
        Row {
            route: "contextdb_engine::Database::row_version_arrived_by_sync",
            drive: || {
                let store = Store::open();
                call(|| store.db.row_version_arrived_by_sync("notes", &key(2)))
            },
        },
        Row {
            route: "contextdb_engine::Database::row_change_arrived_by_sync",
            drive: || {
                let store = Store::open();
                let changes = store.db.changes_since(Lsn(0)).expect("fixture change set");
                let row = changes.rows.first().cloned().expect("fixture row change");
                call(|| store.db.row_change_arrived_by_sync(&row))
            },
        },
        Row {
            route: "contextdb_engine::Database::vector_change_arrived_by_sync",
            drive: || {
                let store = Store::open();
                let changes = store.db.changes_since(Lsn(0)).expect("fixture change set");
                let vector = changes
                    .vectors
                    .first()
                    .cloned()
                    .expect("fixture vector change");
                call(|| store.db.vector_change_arrived_by_sync(&vector))
            },
        },
        // Maintenance, compaction and limits.
        Row {
            route: "contextdb_engine::Database::run_maintenance_cycle",
            drive: || {
                let store = Store::open();
                call(|| store.db.run_maintenance_cycle())
            },
        },
        Row {
            route: "contextdb_engine::Database::run_maintenance_cycle_with_read_memory",
            drive: || {
                let store = Store::open();
                call(|| {
                    let memory = store.db.declared_reader_memory()?;
                    store.db.run_maintenance_cycle_with_read_memory(memory)
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::run_pruning_cycle",
            drive: || {
                let store = Store::open();
                call(|| store.db.run_pruning_cycle())
            },
        },
        Row {
            route: "contextdb_engine::Database::run_pruning_cycle_checked",
            drive: || {
                let store = Store::open();
                call(|| store.db.run_pruning_cycle_checked())
            },
        },
        Row {
            route: "contextdb_engine::Database::compact_now",
            drive: || {
                let store = Store::open();
                call(|| store.db.compact_now())
            },
        },
        Row {
            route: "contextdb_engine::Database::compact_currency_versions",
            drive: || {
                let store = Store::open();
                call(|| store.db.compact_currency_versions())
            },
        },
        Row {
            route: "contextdb_engine::Database::set_maintenance_policy",
            drive: || {
                let store = Store::open();
                call_unit(|| {
                    store
                        .db
                        .set_maintenance_policy(contextdb_engine::MaintenancePolicy::CallerDriven)
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::set_maintenance_poll_interval",
            drive: || {
                let store = Store::open();
                call(|| {
                    store
                        .db
                        .set_maintenance_poll_interval(Duration::from_secs(3600))
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::set_pruning_interval",
            drive: || {
                let store = Store::open();
                call_unit(|| store.db.set_pruning_interval(Duration::from_secs(3600)))
            },
        },
        Row {
            route: "contextdb_engine::Database::set_storage_compaction",
            drive: || {
                let store = Store::open();
                call(|| {
                    store.db.set_storage_compaction(
                        &contextdb_parser::StorageCompactionSetting::Default,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::set_vector_serving_cache_bytes",
            drive: || {
                let store = Store::open();
                call(|| {
                    store
                        .db
                        .set_vector_serving_cache_bytes(Some(4 * 1024 * 1024))
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::set_memory_limit",
            drive: || {
                let store = Store::open();
                call(|| store.db.set_memory_limit(Some(512 * 1024 * 1024)))
            },
        },
        Row {
            route: "contextdb_engine::Database::set_disk_limit",
            drive: || {
                let store = Store::open();
                call(|| store.db.set_disk_limit(Some(4 * 1024 * 1024 * 1024)))
            },
        },
        Row {
            route: "contextdb_engine::Database::check_disk_budget",
            drive: || {
                let store = Store::open();
                store
                    .db
                    .set_disk_limit(Some(4 * 1024 * 1024 * 1024))
                    .expect("fixture disk limit");
                call(|| store.db.check_disk_budget("route check"))
            },
        },
        Row {
            route: "contextdb_engine::Database::disk_file_size",
            drive: || {
                let store = Store::open();
                call(|| Ok::<_, ()>(store.db.disk_file_size()))
            },
        },
        Row {
            route: "contextdb_engine::Database::table_size_estimate",
            drive: || {
                let store = Store::open();
                call(|| store.db.table_size_estimate("notes"))
            },
        },
        Row {
            route: "contextdb_engine::Database::table_size_estimates",
            drive: || {
                let store = Store::open();
                call(|| store.db.table_size_estimates())
            },
        },
        // Sync bookkeeping a consumer reads.
        Row {
            route: "contextdb_engine::Database::memory_held_received_schema_units",
            drive: || {
                let store = Store::open();
                call(|| {
                    store
                        .db
                        .memory_held_received_schema_units(&TenantId::from(TENANT))
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::policy_held_received_tables",
            drive: || {
                let store = Store::open();
                call(|| store.db.policy_held_received_tables())
            },
        },
        Row {
            route: "contextdb_engine::Database::persisted_sync_watermarks",
            drive: || {
                let store = Store::open();
                call(|| store.db.persisted_sync_watermarks(&TenantId::from(TENANT)))
            },
        },
        Row {
            route: "contextdb_engine::Database::persisted_sync_pending_push_confirmation",
            drive: || {
                let store = Store::open();
                call(|| {
                    store
                        .db
                        .persisted_sync_pending_push_confirmation(&TenantId::from(TENANT))
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::persisted_sync_pull_cursor",
            drive: || {
                let store = Store::open();
                call(|| store.db.persisted_sync_pull_cursor(&TenantId::from(TENANT)))
            },
        },
        Row {
            route: "contextdb_engine::Database::persisted_sync_applied_push_watermark",
            drive: || {
                let store = Store::open();
                call(|| {
                    store
                        .db
                        .persisted_sync_applied_push_watermark(&TenantId::from(TENANT))
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::persisted_sync_applied_push_watermark_for_node",
            drive: || {
                let store = Store::open();
                call(|| {
                    store.db.persisted_sync_applied_push_watermark_for_node(
                        &TenantId::from(TENANT),
                        "route-node",
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::persisted_sync_applied_push_watermark_for_node_incarnation",
            drive: || {
                let store = Store::open();
                call(|| {
                    store
                        .db
                        .persisted_sync_applied_push_watermark_for_node_incarnation(
                            &TenantId::from(TENANT),
                            "route-node",
                            Incarnation(1),
                        )
                })
            },
        },
        // Custody.
        Row {
            route: "contextdb_engine::Database::register_delivery_manifest",
            drive: || {
                let store = custody_store();
                let tx = stage_unit(&store.db, 1);
                call(|| store.db.register_delivery_manifest(tx, manifest(1)))
            },
        },
        Row {
            route: "contextdb_engine::Database::delivery_status",
            drive: || {
                // A manifest is registered only through authenticated sync, so
                // the declared root is read with committed rows and no unit.
                let store = custody_store();
                let tx = stage_unit(&store.db, 1);
                store.db.commit(tx).expect("fixture custody commit");
                call(|| store.db.delivery_status("records"))
            },
        },
        Row {
            route: "contextdb_engine::Database::delivery_outcome",
            drive: || {
                let store = custody_store();
                let tx = stage_unit(&store.db, 1);
                store.db.commit(tx).expect("fixture custody commit");
                call(|| store.db.delivery_outcome("records", &key(1)))
            },
        },
        // Callbacks and their audit trail.
        Row {
            route: "contextdb_engine::Database::register_trigger_callback",
            drive: || {
                let store = event_store();
                call(|| {
                    store
                        .db
                        .register_trigger_callback("notes_written", |_, _| Ok(()))?;
                    store.db.execute(
                        "INSERT INTO notes (id, bucket, body) VALUES (47, 1, 'triggered')",
                        &none(),
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::register_transactional_trigger_callback",
            drive: || {
                let store = event_store();
                call(|| {
                    store
                        .db
                        .register_transactional_trigger_callback("notes_written", |_, _| Ok(()))?;
                    store.db.execute(
                        "INSERT INTO notes (id, bucket, body) VALUES (48, 0, 'triggered')",
                        &none(),
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::trigger_audit_history",
            drive: || {
                let store = event_store();
                store
                    .db
                    .register_trigger_callback("notes_written", |_, _| Ok(()))
                    .expect("fixture trigger callback");
                run(
                    &store.db,
                    "INSERT INTO notes (id, bucket, body) VALUES (49, 1, 'audited')",
                );
                call(|| {
                    store.db.trigger_audit_history(TriggerAuditFilter {
                        trigger_name: None,
                        status: None,
                    })
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::register_sink",
            drive: || {
                let store = event_store();
                store
                    .db
                    .register_trigger_callback("notes_written", |_, _| Ok(()))
                    .expect("fixture trigger callback");
                call(|| {
                    store
                        .db
                        .register_sink("note_sink", None, |_: &SinkEvent| Ok(()))?;
                    store.db.execute(
                        "INSERT INTO notes (id, bucket, body) VALUES (50, 0, 'routed')",
                        &none(),
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::register_cron_callback",
            drive: || {
                let store = Store::open();
                call(|| store.db.register_cron_callback("route_tick", |_| Ok(())))
            },
        },
        // Checkpoint export and the inspector over its artifact.
        Row {
            route: "contextdb_engine::Database::export_snapshot",
            drive: || {
                // Both a file-backed and an in-memory store export.
                let store = Store::open();
                let memory = memory_store();
                let artifact = store.sibling("export.db");
                let from_memory = store.sibling("export-from-memory.db");
                call(|| {
                    store.db.export_snapshot(&artifact)?;
                    memory.export_snapshot(&from_memory)
                })
            },
        },
        Row {
            route: "contextdb_engine::Database::export_snapshot_with_read_memory",
            drive: || {
                let store = Store::open();
                let artifact = store.sibling("export.db");
                call(|| {
                    let memory = store.db.declared_reader_memory()?;
                    store.db.export_snapshot_with_read_memory(&artifact, memory)
                })
            },
        },
        Row {
            route: "contextdb_engine::database::SnapshotInspector::open",
            drive: || {
                let (_dir, path) = Store::closed();
                call(|| {
                    let root = DeclaredReadMemory::from_reader_limit(READER_LIMIT)?;
                    SnapshotInspector::open(&path, root.child()?, root)
                })
            },
        },
        Row {
            route: "contextdb_engine::database::SnapshotInspector::inspect_key",
            drive: || {
                let (_dir, path) = Store::closed();
                let root = DeclaredReadMemory::from_reader_limit(READER_LIMIT)
                    .expect("fixture reader root");
                let inspector = SnapshotInspector::open(
                    &path,
                    root.child().expect("fixture reader"),
                    root.clone(),
                )
                .expect("fixture inspector");
                call(|| {
                    inspector.inspect_key("notes", &key(2), &["body".to_string()], root.child()?)
                })
            },
        },
        Row {
            route: "contextdb_engine::database::SnapshotInspector::inspect_blob",
            drive: || {
                let (_dir, path) = Store::closed();
                let root = DeclaredReadMemory::from_reader_limit(READER_LIMIT)
                    .expect("fixture reader root");
                let inspector =
                    SnapshotInspector::open(&path, root.child().expect("fixture reader"), root)
                        .expect("fixture inspector");
                call(|| inspector.inspect_blob([7; 32]))
            },
        },
        Row {
            route: "contextdb_engine::database::SnapshotInspector::inspect_sync_apply_state",
            drive: || {
                let (_dir, path) = Store::closed();
                let root = DeclaredReadMemory::from_reader_limit(READER_LIMIT)
                    .expect("fixture reader root");
                let inspector =
                    SnapshotInspector::open(&path, root.child().expect("fixture reader"), root)
                        .expect("fixture inspector");
                call(|| inspector.inspect_sync_apply_state())
            },
        },
    ]
}
