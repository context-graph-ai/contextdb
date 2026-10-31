//! Policy 25: local search-breadth declarations preserve data, authored history,
//! saved graphs and old readers. The work counter observes the existing broad
//! projection; it grants no memory and knows no replacement implementation.
use super::*;
use redb::{ReadableDatabase, TableHandle};
use std::cell::Cell;

thread_local! {
    static PROJECTED_ROWS: Cell<u64> = const { Cell::new(0) };
}
pub(crate) fn note_projection_rows(rows: u64) {
    PROJECTED_ROWS.with(|count| count.set(count.get().checked_add(rows).unwrap()));
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
    seed_with_saved_graphs(db, rows, true)
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
// Root equality detects even a delete/reinsert of identical logical values.
// All selected families are established names in the frozen storage source.
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
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
fn assert_binding_unchanged(
    db: &Database,
    before: &Option<crate::persistence::VectorSchemaBinding>,
) {
    let after = db
        .persistence
        .as_ref()
        .map(|p| p.vector_schema_binding(&index("embedding")).unwrap());
    assert_eq!(
        &after, before,
        "breadth cannot invent a column incarnation or table generation"
    );
}

// Address every immutable authored row occurrence through the same source
// lookup the production history exporter uses. Scalar values remain comparable
// across F32/SQ8 physical representations; identities are compared on one store.
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
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
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
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
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn assert_frontier(db: &Database) {
    if let Some(p) = &db.persistence {
        p.with_db(|store| {
            let read = store.begin_read().unwrap();
            let table = read
                .open_table(redb::TableDefinition::<&str, u64>::new(
                    "committed_read_lsn_bounds",
                ))
                .unwrap();
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

// New function: existing declaration rows do not combine populated historical
// custody, physical roots, repeated no-op occurrences and operation counts.
// Decisive mutant: route the validated EF-only ALTER through the old projection.

// New function: mapped policy/export tests do not preserve the SAME authored
// row images through a later shape change. Mutant: publish a generation that
// moves resident values out without replacing rows; or skip authored custody.

// New function: the narrow route must distinguish post-merge semantics and
// explicit staging. Mutant: classify every SET IVF AST as probes-only.

// New function: declaration validation tests do not prove real writer pressure
// leaves the same durable data and occurrence history. This exercises an actual
// cap, not a fake payer or an implementation-specific injected preparation name.

// New function: direct apply cannot certify authenticated pull's provenance and
// history filtering. Mutant: omit the no-op occurrence or its DDL sidecar from
// transport, or resolve retired authored values under the latest declaration.

#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn received_private_vector_and_history_images_start_no_worker_on_prepare_rollback_or_commit() {
    for file in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let source = Database::open_memory();
        source.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        run(
            &source,
            "CREATE TABLE received_rows (id INTEGER PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE INDEXED) SYNC TWO WAY SYNC CONFLICT KEEP LATEST HISTORY CURRENT ONLY",
        );
        run(
            &source,
            "INSERT INTO received_rows VALUES (1, [1.0, 2.0, 3.0])",
        );
        // Real authored extraction, handed to the same prepare/registration/
        // commit path as apply_changes. No fabricated received image.
        let changes = source.changes_since(Lsn(0)).unwrap();
        assert!(!changes.ddl.is_empty() && !changes.rows.is_empty());
        let db = if file {
            Database::open(dir.path().join("received.db")).unwrap()
        } else {
            Database::open_memory()
        };
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        crate::read_probe::reset_thread_maintenance_worker_starts();
        let before = db.current_lsn();
        for rollback in [true, false] {
            let reached_prepare = Cell::new(false);
            let reader = db.declared_reader_memory().unwrap();
            let mut vector_copy = contextdb_engine::vector_read_memory::admission(reader.clone());
            let result = db.commit_received_schema_stage(
                &changes,
                None,
                &[],
                &HashMap::new(),
                SyncAdoption::Continuing,
                SyncApplyRole::PullLeg,
                None,
                false,
                None,
                None,
                None,
                None,
                Some(&ConflictPolicies::uniform(ConflictPolicy::LatestWins)),
                |_| Ok(None),
                || {
                    reached_prepare.set(true);
                    assert_eq!(
                        crate::read_probe::thread_maintenance_worker_starts(),
                        0,
                        "received private preparation starts no maintenance worker"
                    );
                    assert!(
                        db.table_meta("received_rows").is_none(),
                        "prepare stays private"
                    );
                    if rollback {
                        Err(Error::Other(
                            "exercise received rollback after preparation".into(),
                        ))
                    } else {
                        Ok(())
                    }
                },
                &mut vector_copy,
                reader,
            );
            assert!(
                reached_prepare.get(),
                "actual received stage was registered"
            );
            assert_eq!(crate::read_probe::thread_maintenance_worker_starts(), 0);
            if rollback {
                let error = result.unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("exercise received rollback after preparation")
                );
                assert!(db.table_meta("received_rows").is_none());
                assert_eq!(db.current_lsn(), before);
                assert!(db.ddl_log_since(Lsn(0)).unwrap().is_empty());
            } else {
                result.unwrap();
                assert_eq!(
                    canonical(&db.table_meta("received_rows").unwrap()),
                    canonical(&source.table_meta("received_rows").unwrap())
                );
                assert_eq!(
                    db.execute("SELECT id, embedding FROM received_rows", &empty())
                        .unwrap()
                        .rows(),
                    vec![vec![Value::Int64(1), Value::Vector(vec![1.0, 2.0, 3.0])]]
                );
                assert_eq!(db.ddl_log_since(Lsn(0)).unwrap().len(), changes.ddl.len());
            }
        }
        db.close().unwrap();
        source.close().unwrap();
    }
}

#[test]
fn maintenance_start_probe_counts_only_the_calling_thread() {
    crate::read_probe::reset_thread_maintenance_worker_starts();
    std::thread::spawn(|| {
        let db = Database::open_memory();
        // Real engine-owned registration starts a real worker on this thread.
        run(&db, "CREATE TABLE probe_rows (id INTEGER PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE INDEXED)");
        assert!(crate::read_probe::thread_maintenance_worker_starts() > 0);
        db.close().unwrap();
    }).join().unwrap();
    assert_eq!(crate::read_probe::thread_maintenance_worker_starts(), 0);
}

#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn received_history_policy_alter_starts_no_worker_and_preserves_rows_on_rollback() {
    for file in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let source = Database::open_memory();
        source.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        run(
            &source,
            "CREATE TABLE policy_rows (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        );
        run(&source, "INSERT INTO policy_rows VALUES (1, 'authored')");
        let db = if file {
            Database::open(dir.path().join("policy.db")).unwrap()
        } else {
            Database::open_memory()
        };
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.apply_changes(
            source.changes_since(Lsn(0)).unwrap(),
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();
        let source_before = source.current_lsn();
        run(&source, "ALTER TABLE policy_rows SET HISTORY CURRENT ONLY");
        let changes = source.changes_since(source_before).unwrap();
        assert_eq!(changes.ddl.len(), 1);
        assert!(changes.rows.is_empty());
        let before = canonical(&db.table_meta("policy_rows").unwrap());
        let history = canonical(&db.ddl_log_since(Lsn(0)).unwrap());
        let rows = db
            .execute("SELECT id, body FROM policy_rows", &empty())
            .unwrap()
            .rows()
            .to_vec();
        crate::read_probe::reset_thread_maintenance_worker_starts();
        for rollback in [true, false] {
            let prepared = Cell::new(false);
            let reader = db.declared_reader_memory().unwrap();
            let mut vector_copy = contextdb_engine::vector_read_memory::admission(reader.clone());
            let result = db.commit_received_schema_stage(
                &changes,
                None,
                &[],
                &HashMap::new(),
                SyncAdoption::Continuing,
                SyncApplyRole::PullLeg,
                None,
                false,
                None,
                None,
                None,
                None,
                Some(&ConflictPolicies::uniform(ConflictPolicy::LatestWins)),
                |_| Ok(None),
                || {
                    prepared.set(true);
                    assert_eq!(crate::read_probe::thread_maintenance_worker_starts(), 0);
                    assert_eq!(canonical(&db.table_meta("policy_rows").unwrap()), before);
                    if rollback {
                        Err(Error::Other("exercise received history rollback".into()))
                    } else {
                        Ok(())
                    }
                },
                &mut vector_copy,
                reader,
            );
            assert!(prepared.get());
            assert_eq!(crate::read_probe::thread_maintenance_worker_starts(), 0);
            assert_eq!(
                db.execute("SELECT id, body FROM policy_rows", &empty())
                    .unwrap()
                    .rows(),
                rows
            );
            if rollback {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("exercise received history rollback")
                );
                assert_eq!(canonical(&db.table_meta("policy_rows").unwrap()), before);
                assert_eq!(canonical(&db.ddl_log_since(Lsn(0)).unwrap()), history);
            } else {
                result.unwrap();
                assert_eq!(
                    canonical(&db.table_meta("policy_rows").unwrap()),
                    canonical(&source.table_meta("policy_rows").unwrap())
                );
                assert_eq!(
                    db.ddl_log_since(Lsn(0)).unwrap().len(),
                    history.as_array().unwrap().len() + 1
                );
            }
        }
        db.close().unwrap();
        source.close().unwrap();
    }
}

// One authenticated pull leg from a hub to a file-backed edge, through the
// real server and client doors.
#[cfg(all(feature = "sync-orchestration", feature = "test-seams"))]
struct AuthenticatedPull {
    client: crate::SyncClient,
    server: Arc<crate::SyncServer>,
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

#[cfg(all(feature = "sync-orchestration", feature = "test-seams"))]
impl AuthenticatedPull {
    fn start(source: &Arc<Database>, edge: &Arc<Database>, tenant: &str) -> Self {
        use crate::transport::in_process::InProcessBroker;
        use crate::{FabricIdentity, SyncClient, SyncServer};
        use contextdb_core::TenantId;
        let broker = InProcessBroker::new();
        let identity = Arc::new(FabricIdentity::generate());
        let node = identity.node_id();
        let server = Arc::new(
            SyncServer::with_authenticated_transport_and_identity_for_test(
                source.clone(),
                broker.server_as(&node),
                TenantId::from(tenant),
                node,
                identity,
            )
            .unwrap(),
        );
        let shutdown = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let server = server.clone();
            let shutdown = shutdown.clone();
            async move { server.run_until(shutdown).await }
        });
        let identity = Arc::new(FabricIdentity::generate());
        let client = SyncClient::with_authenticated_transport_and_identity_for_test(
            edge.clone(),
            broker.client_as(&identity.node_id()),
            TenantId::from(tenant),
            identity,
        )
        .unwrap();
        Self {
            client,
            server,
            shutdown,
            task,
        }
    }

    async fn pull(&self) {
        self.client.pull_default().await.unwrap();
    }

    async fn stop(self) {
        drop(self.client);
        self.shutdown.store(true, Ordering::SeqCst);
        self.task.await.unwrap();
        drop(self.server);
    }
}

#[cfg(all(feature = "sync-orchestration", feature = "test-seams"))]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn archived_body_count(db: &Database) -> u64 {
    let Some(persistence) = &db.persistence else {
        return 0;
    };
    persistence
        .with_db(|store| {
            use redb::ReadableTableMetadata;
            let read = store.begin_read().unwrap();
            match read.open_table(redb::TableDefinition::<&str, &[u8]>::new(
                "__authored_row_bodies",
            )) {
                Ok(table) => Ok(table.len().unwrap()),
                Err(redb::TableError::TableDoesNotExist(_)) => Ok(0),
                Err(error) => panic!("read the authored body archive: {error:?}"),
            }
        })
        .unwrap()
}

#[cfg(all(feature = "sync-orchestration", feature = "test-seams"))]
fn writes_to(work: &ReceivedSchemaApplyObservationForTest, table: &str) -> u64 {
    work.artifact_writes
        .iter()
        .filter(|(write, _)| write.table == table)
        .map(|(_, count)| *count)
        .sum()
}

// A received search-breadth or consolidation declaration changes metadata
// only. Applying it must neither copy the relation's rows into the authored
// body archive nor rewrite the relation: the received-apply observer counts
// the actual durable writes of each delivered statement.
#[cfg(all(feature = "sync-orchestration", feature = "test-seams"))]
#[tokio::test]
async fn received_metadata_only_alters_archive_and_rewrite_no_row_population() {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(Database::open_memory());
    source.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    run(
        &source,
        "CREATE TABLE breadth_rows (id INTEGER PRIMARY KEY, bucket INTEGER, body TEXT, embedding VECTOR(3) SEARCH_MODE INDEXED IVF (PROBES = 7), compressed VECTOR(3) WITH (quantization = 'SQ8') SEARCH_MODE INDEXED IVF (PROBES = 7)) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
    );
    for id in 1..=24i64 {
        let x = id as f32;
        source
            .execute(
                "INSERT INTO breadth_rows (id, bucket, body, embedding, compressed) VALUES ($id, $bucket, $body, $vector, $vector)",
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("bucket".into(), Value::Int64(id % 2)),
                    ("body".into(), Value::Text(format!("authored-{id}"))),
                    ("vector".into(), Value::Vector(vec![x, 25.0 - x, 1.0])),
                ]),
            )
            .unwrap();
    }
    run(
        &source,
        "UPDATE breadth_rows SET body = 'replacement' WHERE id = 1",
    );
    let edge = Arc::new(Database::open(dir.path().join("edge.db")).unwrap());
    edge.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let pull = AuthenticatedPull::start(&source, &edge, "metadata-only-archive");
    pull.pull().await;
    let authored = authored_rows(&edge);
    assert_eq!(authored.len(), 25, "24 inserts and one replacement arrived");
    for statement in [
        "ALTER TABLE breadth_rows ALTER COLUMN embedding SET IVF (PROBES = 11)",
        "ALTER TABLE breadth_rows ALTER COLUMN compressed SET IVF (PROBES = 11)",
        "ALTER TABLE breadth_rows ALTER COLUMN embedding SET CONSOLIDATION (CHANGE_PERCENT = 11)",
    ] {
        let archived = archived_body_count(&edge);
        let roots = physical_roots(&edge);
        let held = edge.ddl_log_since(Lsn(0)).unwrap().len();
        run(&source, statement);
        let observer = ReceivedSchemaApplyObserverForTest::new();
        let scope = edge.observe_received_schema_applies_for_test(&observer);
        pull.pull().await;
        drop(scope);
        let work = observer
            .observation()
            .unwrap_or_else(|| panic!("`{statement}` reached the real received-schema stage"));
        assert_eq!(
            edge.ddl_log_since(Lsn(0)).unwrap().len(),
            held + 1,
            "`{statement}` was delivered as one authored occurrence"
        );
        assert_eq!(
            writes_to(&work, "__authored_row_bodies"),
            0,
            "`{statement}` must not archive the row population"
        );
        assert_eq!(archived_body_count(&edge), archived);
        assert_eq!(
            writes_to(&work, "rel_breadth_rows"),
            0,
            "`{statement}` must not rewrite stored rows"
        );
        assert_eq!(
            (
                work.durable_table_rewrites,
                work.durable_rows_rewritten,
                work.staged_rows_copied
            ),
            (0, 0, 0),
            "`{statement}` must not replace or copy the relation: {work:?}"
        );
        assert_eq!(physical_roots(&edge), roots);
        assert_eq!(authored_rows(&edge), authored);
    }
    // Control: the same observer does see a received shape change replace
    // the stored rows, so the zeros above are measurements, not blind spots.
    run(&source, "ALTER TABLE breadth_rows DROP COLUMN bucket");
    let observer = ReceivedSchemaApplyObserverForTest::new();
    let scope = edge.observe_received_schema_applies_for_test(&observer);
    pull.pull().await;
    drop(scope);
    let work = observer
        .observation()
        .expect("the received DROP reached the real received-schema stage");
    assert!(
        writes_to(&work, "rel_breadth_rows") > 0,
        "a received DROP COLUMN rewrites the stored rows: {work:?}"
    );
    pull.stop().await;
    edge.close().unwrap();
    source.close().unwrap();
}

// Every authored row occurrence of `table`, addressed through the history
// exporter's own lookup: (row id, creating transaction, LSN, scalar values).
#[cfg(all(feature = "sync-orchestration", feature = "test-seams"))]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn authored_images(
    db: &Database,
    table_name: &str,
    columns: &[&str],
) -> Vec<(u64, u64, u64, BTreeMap<String, Value>)> {
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
            if table != table_name {
                return None;
            }
            let row = db
                .row_for_change(&table, row_id, lsn, memory.clone(), &|| Ok(()), None)
                .unwrap()
                .expect("an authored row occurrence stays addressable in history");
            let values = columns
                .iter()
                .map(|name| {
                    (
                        (*name).to_owned(),
                        row.values.get(*name).cloned().unwrap_or(Value::Null),
                    )
                })
                .collect();
            Some((row_id.0, row.created_tx.0, lsn.0, values))
        })
        .collect()
}

// HISTORY ALL is the default: a received DROP COLUMN changes the live shape,
// never rows already authored. Every earlier occurrence still reads back with
// its original body bytes under the same row id, creating transaction and LSN,
// after the pull and after the edge restarts. Covered for a plain relation and
// for one whose quantized vector column is stored as a placement.
#[cfg(all(feature = "sync-orchestration", feature = "test-seams"))]
#[tokio::test]
async fn received_drop_column_keeps_each_earlier_row_body_under_its_exact_identity() {
    for quantized in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(Database::open_memory());
        source.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        let vector_column = if quantized {
            ", compressed VECTOR(3) WITH (quantization = 'SQ8') SEARCH_MODE INDEXED"
        } else {
            ""
        };
        run(
            &source,
            &format!(
                "CREATE TABLE history_rows (id INTEGER PRIMARY KEY, body TEXT, note TEXT{vector_column}) SYNC TWO WAY SYNC CONFLICT KEEP LATEST"
            ),
        );
        let body = |id: i64| format!("original body {id} \u{00fc}\u{2603}");
        let mut expected = Vec::new();
        for id in 1..=6i64 {
            let mut params = HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("body".into(), Value::Text(body(id))),
                ("note".into(), Value::Text(format!("note-{id}"))),
            ]);
            let sql = if quantized {
                params.insert(
                    "vector".into(),
                    Value::Vector(vec![id as f32, 7.0 - id as f32, 1.0]),
                );
                "INSERT INTO history_rows (id, body, note, compressed) VALUES ($id, $body, $note, $vector)"
            } else {
                "INSERT INTO history_rows (id, body, note) VALUES ($id, $body, $note)"
            };
            source.execute(sql, &params).unwrap();
            expected.push((id, body(id)));
        }
        run(
            &source,
            "UPDATE history_rows SET body = 'second version of 2' WHERE id = 2",
        );
        expected.push((2, "second version of 2".into()));
        let edge_path = dir.path().join("edge.db");
        let edge = Arc::new(Database::open(&edge_path).unwrap());
        edge.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        let pull = AuthenticatedPull::start(&source, &edge, "drop-keeps-history");
        pull.pull().await;
        let meta = edge.table_meta("history_rows").unwrap();
        assert_eq!(
            meta.history_policy, None,
            "the table declares no history policy"
        );
        assert_eq!(
            contextdb_core::DEFAULT_HISTORY_POLICY,
            contextdb_core::HistoryPolicy::All
        );
        let before = authored_images(&edge, "history_rows", &["id", "body"]);
        assert_eq!(
            before
                .iter()
                .map(|(_, _, _, values)| (values["id"].clone(), values["body"].clone()))
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|(id, body)| (Value::Int64(*id), Value::Text(body.clone())))
                .collect::<Vec<_>>(),
            "the edge holds every authored occurrence, both versions of id 2 included"
        );
        run(&source, "ALTER TABLE history_rows DROP COLUMN body");
        pull.pull().await;
        assert!(
            edge.table_meta("history_rows")
                .unwrap()
                .columns
                .iter()
                .all(|column| column.name != "body"),
            "the received DROP changed the live shape"
        );
        assert_eq!(
            edge.execute("SELECT id, note FROM history_rows ORDER BY id", &empty())
                .unwrap()
                .rows()
                .len(),
            6
        );
        assert_eq!(
            authored_images(&edge, "history_rows", &["id", "body"]),
            before,
            "each earlier occurrence keeps its original body under the same row, transaction and LSN (quantized={quantized})"
        );
        pull.stop().await;
        edge.close().unwrap();
        drop(edge);
        let reopened = Database::open(&edge_path).unwrap();
        assert_eq!(
            authored_images(&reopened, "history_rows", &["id", "body"]),
            before,
            "the original bodies survive the edge restart (quantized={quantized})"
        );
        reopened.close().unwrap();
        source.close().unwrap();
    }
}
fn seed_with_saved_graphs(db: &Database, rows: usize, saved_graphs: bool) -> Vec<ChangeSet> {
    let mut units = Vec::new();
    let mut after = Lsn(0);
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    run(
        db,
        "CREATE TABLE breadth_rows (id INTEGER PRIMARY KEY, bucket INTEGER, body TEXT, embedding VECTOR(3) SEARCH_MODE INDEXED IVF (PROBES = 7), compressed VECTOR(3) WITH (quantization = 'SQ8') SEARCH_MODE INDEXED IVF (PROBES = 7)) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
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
    if !saved_graphs {
        assert!(
            graphs(db)
                .iter()
                .all(|(generation, _)| generation.is_none()),
            "the pre-query control must not have a published clustered index"
        );
        return units;
    }
    for _ in 0..8 {
        if ["embedding", "compressed"]
            .iter()
            .all(|name| db.__debug_vector_indexed_available_for_test(index(name)))
        {
            return units;
        }
        db.run_maintenance_cycle().unwrap();
    }
    panic!("fixture must have both published clustered indexes before ALTER");
}
fn graphs(db: &Database) -> Vec<(Option<u64>, bool)> {
    ["embedding", "compressed"]
        .iter()
        .map(|name| {
            (
                db.__debug_vector_index_generation_for_test(index(name)),
                db.__debug_vector_indexed_available_for_test(index(name)),
            )
        })
        .collect()
}
