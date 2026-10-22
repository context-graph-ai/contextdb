//! Consolidation declarations preserve populated data and authored history.
use super::*;
use redb::{ReadableDatabase, TableHandle};
use std::cell::Cell;
thread_local! { static PROJECTED_ROWS: Cell<u64> = const { Cell::new(0) }; }
pub(super) fn note_projection_rows(rows: u64) {
    PROJECTED_ROWS.with(|count| count.set(count.get() + rows));
}
fn take_projected_rows() -> u64 {
    PROJECTED_ROWS.with(|count| count.replace(0))
}
fn empty() -> HashMap<String, Value> {
    HashMap::new()
}
fn index(column: &str) -> VectorIndexRef {
    VectorIndexRef::new("breadth_rows", column)
}
fn run(db: &Database, sql: &str) {
    db.execute(sql, &empty())
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
}
fn column(db: &Database) -> contextdb_core::ColumnDef {
    db.table_meta("breadth_rows")
        .unwrap()
        .columns
        .into_iter()
        .find(|column| column.name == "embedding")
        .unwrap()
}
fn canonical<T: serde::Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).unwrap()
}
fn record_authored(db: &Database, after: &mut Lsn, units: &mut Vec<ChangeSet>) {
    units.push(db.changes_since(*after).unwrap());
    *after = db.current_lsn();
}
fn seed(db: &Database, rows: usize) -> Vec<ChangeSet> {
    let mut units = Vec::new();
    let mut after = Lsn(0);
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    run(
        db,
        "CREATE TABLE breadth_rows (id INTEGER PRIMARY KEY, bucket INTEGER, body TEXT, embedding VECTOR(3) SEARCH_MODE INDEXED HNSW (EF_SEARCH = 7), compressed VECTOR(3) WITH (quantization = 'SQ8') SEARCH_MODE INDEXED HNSW (EF_SEARCH = 7)) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
    );
    record_authored(db, &mut after, &mut units);
    run(db, "CREATE INDEX breadth_bucket ON breadth_rows (bucket)");
    record_authored(db, &mut after, &mut units);
    for id in 1..=rows {
        let x = id as f32;
        db.execute("INSERT INTO breadth_rows (id, bucket, body, embedding, compressed) VALUES ($id, $bucket, $body, $vector, $vector)",
            &HashMap::from([
                ("id".into(), Value::Int64(id as i64)),
                ("bucket".into(), Value::Int64((id % 2) as i64)),
                ("body".into(), Value::Text(format!("authored-{id}"))),
                ("vector".into(), Value::Vector(vec![x, rows as f32 + 1.0 - x, 1.0])),
            ])).unwrap();
        record_authored(db, &mut after, &mut units);
    }
    run(
        db,
        "UPDATE breadth_rows SET body = 'replacement' WHERE id = 1",
    );
    record_authored(db, &mut after, &mut units);
    // Populate authored custody BEFORE the narrow ALTER. An empty-family
    // assertion would reject valid stores and miss the move-out regression.
    run(db, "ALTER TABLE breadth_rows ADD COLUMN marker TEXT");
    record_authored(db, &mut after, &mut units);
    for _ in 0..8 {
        if ["embedding", "compressed"].iter().all(|name| {
            db.__debug_vector_hnsw_build_serial_for_test(index(name))
                .is_some()
        }) {
            return units;
        }
        db.run_maintenance_cycle().unwrap();
    }
    panic!("fixture must have both saved serving graphs before ALTER");
}
fn visible(db: &Database) -> Vec<Vec<Value>> {
    db.execute(
        "SELECT id, bucket, body, embedding, compressed, marker FROM breadth_rows ORDER BY id",
        &empty(),
    )
    .unwrap()
    .rows()
    .to_vec()
}
fn graphs(db: &Database) -> Vec<(Option<u64>, Option<u64>)> {
    ["embedding", "compressed"]
        .iter()
        .map(|name| {
            (
                db.__debug_vector_hnsw_build_serial_for_test(index(name)),
                db.__debug_vector_hnsw_topology_digest_for_test(index(name)),
            )
        })
        .collect()
}
fn physical_roots(db: &Database) -> BTreeMap<String, Option<Option<[u8; 32]>>> {
    let Some(persistence) = &db.persistence else {
        return BTreeMap::new();
    };
    persistence
        .with_db(|store| {
            let read = store.begin_read().unwrap();
            let mut roots = BTreeMap::new();
            for table in read.list_tables().unwrap() {
                let name = table.name();
                if name.starts_with("rel_")
                    || name.starts_with("__rel_index_")
                    || name.starts_with("vector_partition_")
                    || name.starts_with("content_size_")
                    || matches!(
                        name,
                        "vector_entries" | "vector_open_directory" | "__authored_row_bodies"
                    )
                {
                    roots.insert(name.to_owned(), read.normal_table_root(name).unwrap());
                }
            }
            Ok(roots)
        })
        .unwrap()
}
fn authored_rows(db: &Database) -> Vec<(u64, u64, u64, serde_json::Value)> {
    let memory = db.declared_reader_memory().unwrap();
    let decode = crate::read_image_memory::ImageMemory::operation_reader(
        db.accountant.clone(),
        memory.clone(),
    )
    .unwrap();
    let _decode = decode.enter();
    db.change_log_since(Lsn(0))
        .unwrap()
        .into_iter()
        .filter_map(|entry| {
            let ChangeLogEntry::RowInsert { table, row_id, lsn } = entry else {
                return None;
            };
            if table != "breadth_rows" {
                return None;
            }
            let row = db
                .row_for_change(&table, row_id, lsn, memory.clone(), &|| Ok(()), None)
                .unwrap()
                .expect("an immutable authored row cannot disappear after a metadata ALTER");
            let scalar = ["id", "bucket", "body"]
                .into_iter()
                .map(|name| {
                    (
                        name.to_owned(),
                        row.values.get(name).cloned().unwrap_or(Value::Null),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            Some((row_id.0, row.created_tx.0, lsn.0, canonical(&scalar)))
        })
        .collect()
}
fn declared_identity(db: &Database) -> Option<crate::direct_file_reader::DirectTypedImageDigest> {
    db.persistence.as_ref().map(|p| {
        p.with_db(|store| {
            crate::persistence::RedbPersistence::load_content_identity_from_read(
                &store.begin_read().unwrap(),
            )
        })
        .unwrap()
    })
}
fn assert_frontier(db: &Database) {
    if let Some(p) = &db.persistence {
        p.with_db(|store| {
            let read = store.begin_read().unwrap();
            let table = read
                .open_table(redb::TableDefinition::<&str, u64>::new(
                    "committed_read_lsn_bounds",
                ))
                .unwrap();
            use redb::ReadableTable;
            assert_eq!(
                table.get("breadth_rows").unwrap().unwrap().value(),
                db.current_lsn().0,
                "metadata and authored occurrence advance the table read frontier together"
            );
            Ok(())
        })
        .unwrap();
        let root = db.writer_read_root().unwrap().unwrap();
        let recovery = RedbPersistence::writer_recovery_floors_in_read(&root).unwrap();
        assert!(recovery.source_lsn_floor >= db.current_lsn().0);
    }
}

#[test]
fn consolidation_only_preserves_rows_history_graphs_and_old_snapshot_without_projection() {
    for file in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("consolidation.db");
        let db = if file {
            Database::open(&path).unwrap()
        } else {
            Database::open_memory()
        };
        let _ = seed(&db, 24);
        let rows = visible(&db);
        let authored = authored_rows(&db);
        let roots = physical_roots(&db);
        let serving = graphs(&db);
        let old_root = db.writer_read_root().unwrap();
        let snapshot = db.snapshot();
        let old_metadata = canonical(&db.table_meta("breadth_rows").unwrap());
        let revision = column(&db).vector_policy_revision;
        let mut occurrences = db.ddl_log_since(Lsn(0)).unwrap().len();
        for (members, expected) in [
            (
                "(CHANGE_PERCENT = 11, TOMBSTONE_PERCENT = 17)",
                (Some(11), Some(17), false),
            ),
            ("(CHANGE_PERCENT = 19)", (Some(19), Some(17), false)),
            ("(CHANGE_PERCENT = 19)", (Some(19), Some(17), false)),
            ("NONE", (None, None, true)),
            ("DEFAULT", (None, None, false)),
        ] {
            take_projected_rows();
            db.relational_store.reset_index_maintenance_visits();
            let identity = declared_identity(&db);
            run(
                &db,
                &format!(
                    "ALTER TABLE breadth_rows ALTER COLUMN embedding SET CONSOLIDATION {members}"
                ),
            );
            let projected = take_projected_rows();
            let posting_work = db.relational_store.index_maintenance_visits();
            occurrences += 1;
            assert_eq!(db.ddl_log_since(Lsn(0)).unwrap().len(), occurrences);
            let c = column(&db);
            assert_eq!(
                (
                    c.consolidation_change_percent,
                    c.consolidation_tombstone_percent,
                    c.consolidation_disabled
                ),
                expected
            );
            assert_eq!(c.vector_policy_revision, revision);
            assert_eq!(c.hnsw_ef_search, Some(7));
            assert_eq!(
                projected, 0,
                "consolidation-only publication must not decode row versions"
            );
            assert_eq!(
                posting_work, 0,
                "consolidation-only publication must not rebuild postings"
            );
            assert_eq!(physical_roots(&db), roots);
            assert_eq!(graphs(&db), serving);
            assert_eq!(visible(&db), rows);
            assert_eq!(authored_rows(&db), authored);
            assert_frontier(&db);
            if let (Some(before), Some(after)) = (identity, declared_identity(&db)) {
                assert_eq!(
                    (after.relational, after.f32_vectors, after.sq8_vectors),
                    (before.relational, before.f32_vectors, before.sq8_vectors)
                );
                assert_ne!(
                    after.change_and_ddl, before.change_and_ddl,
                    "a repeated declaration still authors its own occurrence"
                );
            }
            if let Some(root) = &old_root {
                assert_eq!(
                    canonical(root.borrow_captured_table_meta("breadth_rows").unwrap()),
                    old_metadata
                );
                let current = db.writer_read_root().unwrap().unwrap();
                assert_eq!(
                    canonical(current.borrow_captured_table_meta("breadth_rows").unwrap()),
                    canonical(&db.table_meta("breadth_rows").unwrap())
                );
            }
            assert_eq!(
                db.execute_at_snapshot(
                    "SELECT body FROM breadth_rows WHERE id = 1",
                    &empty(),
                    snapshot
                )
                .unwrap()
                .rows(),
                vec![vec![Value::Text("replacement".into())]]
            );
        }
        let metadata = canonical(&db.table_meta("breadth_rows").unwrap());
        let history = canonical(&db.ddl_log_since(Lsn(0)).unwrap());
        drop(old_root);
        db.close().unwrap();
        drop(db);
        if file {
            let reopened = Database::open(&path).unwrap();
            assert_eq!(
                canonical(&reopened.table_meta("breadth_rows").unwrap()),
                metadata
            );
            assert_eq!(canonical(&reopened.ddl_log_since(Lsn(0)).unwrap()), history);
            assert_eq!(visible(&reopened), rows);
            assert_eq!(authored_rows(&reopened), authored);
            reopened.close().unwrap();
        }
    }
}

#[test]
fn refused_consolidation_keeps_old_declaration_history_and_graph_authoritative() {
    for file in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("refusal.db");
        let db = if file {
            Database::open(&path).unwrap()
        } else {
            Database::open_memory()
        };
        let _ = seed(&db, 24);
        let schema = canonical(&db.table_meta("breadth_rows").unwrap());
        let rows = visible(&db);
        let authored = authored_rows(&db);
        let history = canonical(&db.ddl_log_since(Lsn(0)).unwrap());
        let serving = graphs(&db);
        let roots = physical_roots(&db);
        let old_root = db.writer_read_root().unwrap();
        let old_limit = db.memory_limit();
        let cap = db.finalization.accountant.usage().used;
        db.set_memory_limit(Some(cap)).unwrap();
        let refusal = db
            .execute(
                "ALTER TABLE breadth_rows ALTER COLUMN embedding SET CONSOLIDATION (TOMBSTONE_PERCENT = 19)",
                &empty(),
            )
            .unwrap_err();
        db.set_memory_limit(old_limit).unwrap();
        match refusal {
            Error::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
            } => {
                assert_eq!(
                    budget_limit_bytes, cap,
                    "retain the actual original writer cap"
                );
                assert!(requested_bytes > available_bytes);
                assert!(
                    !subsystem.is_empty() && !operation.is_empty() && !hint.is_empty(),
                    "retain typed phase and remedy fields"
                );
            }
            other => {
                panic!("writer pressure must remain the original typed memory refusal: {other:?}")
            }
        }
        assert_eq!(canonical(&db.table_meta("breadth_rows").unwrap()), schema);
        assert_eq!(visible(&db), rows);
        assert_eq!(authored_rows(&db), authored);
        assert_eq!(canonical(&db.ddl_log_since(Lsn(0)).unwrap()), history);
        assert_eq!(graphs(&db), serving);
        assert_eq!(physical_roots(&db), roots);
        if let Some(root) = &old_root {
            assert_eq!(
                canonical(root.borrow_captured_table_meta("breadth_rows").unwrap()),
                schema
            );
        }
        drop(old_root);
        db.close().unwrap();
        drop(db);
        if file {
            let reopened = Database::open(&path).unwrap();
            assert_eq!(
                canonical(&reopened.table_meta("breadth_rows").unwrap()),
                schema
            );
            assert_eq!(canonical(&reopened.ddl_log_since(Lsn(0)).unwrap()), history);
            assert_eq!(visible(&reopened), rows);
            assert_eq!(authored_rows(&reopened), authored);
            reopened.close().unwrap();
        }
    }
}

#[test]
fn consolidation_occurrences_replay_with_the_same_authored_ddl_and_row_history() {
    let dir = tempfile::tempdir().unwrap();
    let source = Database::open(dir.path().join("source.db")).unwrap();
    let mut units = seed(&source, 24);
    let authored = authored_rows(&source);
    let mut after = source.current_lsn();
    for members in [
        "(CHANGE_PERCENT = 11, TOMBSTONE_PERCENT = 17)",
        "(TOMBSTONE_PERCENT = 19)",
        "(TOMBSTONE_PERCENT = 19)",
    ] {
        run(
            &source,
            &format!("ALTER TABLE breadth_rows ALTER COLUMN embedding SET CONSOLIDATION {members}"),
        );
        record_authored(&source, &mut after, &mut units);
    }
    let replay = Database::open_memory();
    replay.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    for unit in units {
        replay
            .apply_changes(
                unit,
                &ConflictPolicies::uniform(crate::sync_types::ConflictPolicy::LatestWins),
            )
            .unwrap();
    }
    assert_eq!(
        canonical(&replay.table_meta("breadth_rows").unwrap()),
        canonical(&source.table_meta("breadth_rows").unwrap())
    );
    assert_eq!(
        canonical(&replay.ddl_log_since(Lsn(0)).unwrap()),
        canonical(&source.ddl_log_since(Lsn(0)).unwrap())
    );
    assert_eq!(
        authored_rows(&replay)
            .iter()
            .map(|row| &row.3)
            .collect::<Vec<_>>(),
        authored.iter().map(|row| &row.3).collect::<Vec<_>>()
    );
    assert_eq!(visible(&replay), visible(&source));
    source.close().unwrap();
    replay.close().unwrap();
}

#[test]
fn consolidation_transaction_rollback_does_not_publish_local_autocommit_metadata() {
    for file in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = if file {
            Database::open(dir.path().join("transaction.db")).unwrap()
        } else {
            Database::open_memory()
        };
        let _ = seed(&db, 24);
        let before = canonical(&db.table_meta("breadth_rows").unwrap());
        let history = canonical(&db.ddl_log_since(Lsn(0)).unwrap());
        let rows = visible(&db);
        let tx = db.begin().unwrap();
        db.execute_in_tx(
            tx,
            "ALTER TABLE breadth_rows ALTER COLUMN embedding SET CONSOLIDATION NONE",
            &empty(),
        )
        .unwrap();
        assert_eq!(canonical(&db.table_meta("breadth_rows").unwrap()), before);
        db.rollback(tx).unwrap();
        assert_eq!(canonical(&db.table_meta("breadth_rows").unwrap()), before);
        assert_eq!(canonical(&db.ddl_log_since(Lsn(0)).unwrap()), history);
        assert_eq!(visible(&db), rows);
        let tx = db.begin().unwrap();
        db.execute_in_tx(
            tx,
            "ALTER TABLE breadth_rows ALTER COLUMN embedding SET CONSOLIDATION NONE",
            &empty(),
        )
        .unwrap();
        db.commit(tx).unwrap();
        assert!(column(&db).consolidation_disabled);
        db.close().unwrap();
    }
}
