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
fn alter(db: &Database, members: &str) {
    run(
        db,
        &format!("ALTER TABLE breadth_rows ALTER COLUMN embedding SET HNSW {members}"),
    );
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
fn seed_with_saved_graphs(db: &Database, rows: usize, saved_graphs: bool) -> Vec<ChangeSet> {
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
    if !saved_graphs {
        assert!(
            graphs(db).iter().all(|(serial, _)| serial.is_none()),
            "the pre-query control must not have a materialized serving graph"
        );
        return units;
    }
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
fn assert_query_breadth(db: &Database, name: &str, breadth: usize) {
    db.execute(
        &format!(
            "SELECT id FROM breadth_rows ORDER BY {name} <=> $query USE VECTOR INDEXED LIMIT 1"
        ),
        &HashMap::from([("query".into(), Value::Vector(vec![1.0, 120.0, 1.0]))]),
    )
    .unwrap();
    let trace = db
        .__debug_last_query_vector_trace_for_test(db.declared_reader_memory().unwrap())
        .unwrap()
        .unwrap();
    assert!(
        trace.used_hnsw,
        "the fixture must exercise graph search: {trace:?}"
    );
    assert_eq!(
        trace.hnsw_candidate_count, breadth,
        "the newly opened query uses actual declared breadth, without a hidden profile floor"
    );
}
// Root equality detects even a delete/reinsert of identical logical values.
// All selected families are established names in the frozen storage source.
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

// New function: existing declaration rows do not combine populated historical
// custody, physical roots, repeated no-op occurrences and operation counts.
// Decisive mutant: route the validated EF-only ALTER through the old projection.
#[test]
fn ef_search_only_keeps_populated_rows_postings_and_graphs_in_place() {
    for (file, count) in [(false, 120), (false, 240), (true, 120), (true, 240)] {
        let dir = tempfile::tempdir().unwrap();
        let db = if file {
            Database::open(dir.path().join("source.db")).unwrap()
        } else {
            Database::open_memory()
        };
        let _ = seed(&db, count);
        let rows = visible(&db);
        let serving = graphs(&db);
        assert!(
            serving
                .iter()
                .all(|(serial, topology)| serial.is_some() && topology.is_some())
        );
        let roots = physical_roots(&db);
        if file {
            assert!(roots.contains_key("rel_breadth_rows"));
            assert!(roots.contains_key("__authored_row_bodies"));
            assert!(
                roots
                    .keys()
                    .any(|name| name.starts_with("__rel_index_") && name.len() > 64)
            );
            assert!(roots.contains_key("vector_entries"));
        }
        let binding = db
            .persistence
            .as_ref()
            .map(|p| p.vector_schema_binding(&index("embedding")).unwrap());
        let old_revision = column(&db).vector_policy_revision;
        let old_root = db.writer_read_root().unwrap();
        let old_meta = old_root
            .as_ref()
            .map(|root| canonical(root.borrow_captured_table_meta("breadth_rows").unwrap()));
        let old_charge = old_root.as_ref().map(|root| root.control_bytes());
        if let Some(root) = &old_root {
            assert!(
                root.control_bytes() > root.descriptor_control_bytes(),
                "the retained snapshot includes paid schema metadata"
            );
        }
        let old_snapshot = db.snapshot();
        let before = db.current_lsn();
        let mut occurrences = db.ddl_log_since(Lsn(0)).unwrap().len();
        for (members, expected) in [
            ("(EF_SEARCH = 11)", Some(11)),
            ("(EF_SEARCH = 11)", Some(11)), // authored no-op
            ("(M = DEFAULT, EF_SEARCH = 13)", Some(13)), // merged partial
            ("(EF_SEARCH = DEFAULT)", None),
            ("DEFAULT", None), // authored no-op group default
            ("(EF_SEARCH = 7)", Some(7)),
            ("DEFAULT", None), // group default changes only breadth
        ] {
            take_projected_rows();
            db.relational_store.reset_index_maintenance_visits();
            let old_policy = column(&db).hnsw_ef_search;
            let identity_before = declared_identity(&db);
            alter(&db, members);
            let projection_work = take_projected_rows();
            let posting_work = db.relational_store.index_maintenance_visits();
            occurrences += 1;
            assert_frontier(&db);
            if let (Some(before), Some(after)) = (identity_before, declared_identity(&db)) {
                assert_eq!(
                    (after.relational, after.f32_vectors, after.sq8_vectors),
                    (before.relational, before.f32_vectors, before.sq8_vectors)
                );
                assert_ne!(
                    after.change_and_ddl, before.change_and_ddl,
                    "no-op authored occurrences still change history identity"
                );
                if old_policy != expected {
                    assert_ne!(
                        after.policy, before.policy,
                        "new breadth participates in canonical policy identity"
                    );
                    assert_ne!(after.schema_and_indexes, before.schema_and_indexes);
                } else {
                    assert_eq!(after.policy, before.policy);
                    assert_eq!(after.schema_and_indexes, before.schema_and_indexes);
                }
            }
            assert_eq!(
                db.ddl_log_since(Lsn(0)).unwrap().len(),
                occurrences,
                "every authored statement survives, even a no-op"
            );
            assert_eq!(column(&db).hnsw_ef_search, expected);
            assert_eq!(column(&db).vector_policy_revision, old_revision);
            assert_eq!(visible(&db), rows);
            assert_eq!(graphs(&db), serving);
            assert_binding_unchanged(&db, &binding);
            if let Some(current) = db.writer_read_root().unwrap() {
                assert_eq!(
                    canonical(current.borrow_captured_table_meta("breadth_rows").unwrap()),
                    canonical(&db.table_meta("breadth_rows").unwrap()),
                    "the new durable writer capture must publish the same declaration as live SQL"
                );
            }
            if let Some(breadth) = expected {
                assert_query_breadth(&db, "embedding", breadth as usize);
            }
            assert_eq!(
                db.execute_at_snapshot(
                    "SELECT id, body FROM breadth_rows WHERE id = 1",
                    &empty(),
                    old_snapshot
                )
                .unwrap()
                .rows(),
                vec![vec![Value::Int64(1), Value::Text("replacement".into())]]
            );
            if let Some(root) = &old_root {
                assert_eq!(
                    Some(root.control_bytes()),
                    old_charge,
                    "the old metadata receipt survives every successor publication"
                );
                assert_eq!(
                    canonical(root.borrow_captured_table_meta("breadth_rows").unwrap()),
                    *old_meta.as_ref().unwrap()
                );
                assert_eq!(
                    root.borrow_captured_table_meta("breadth_rows")
                        .unwrap()
                        .columns
                        .iter()
                        .find(|c| c.name == "embedding")
                        .unwrap()
                        .hnsw_ef_search,
                    Some(7)
                );
            }
            assert_eq!(
                physical_roots(&db),
                roots,
                "existing authored custody is preserved; data/index/vector roots must not be rewritten"
            );
            assert_eq!(
                projection_work, 0,
                "EF-only metadata publication must not visit retained row versions"
            );
            assert_eq!(
                posting_work, 0,
                "EF-only metadata publication must not rebuild relational postings"
            );
        }
        take_projected_rows();
        db.relational_store.reset_index_maintenance_visits();
        run(
            &db,
            "ALTER TABLE breadth_rows ALTER COLUMN compressed SET HNSW (EF_SEARCH = 11)",
        );
        assert_eq!(
            take_projected_rows(),
            0,
            "quantized breadth changes also avoid row projection"
        );
        assert_eq!(db.relational_store.index_maintenance_visits(), 0);
        assert_eq!(physical_roots(&db), roots);
        assert_eq!(graphs(&db), serving);
        assert_eq!(visible(&db), rows);
        assert_query_breadth(&db, "compressed", 11);
        assert_eq!(db.ddl_log_since(before).unwrap().len(), 8);
        drop(old_root);
        db.close().unwrap();
    }
}

// New function: mapped policy/export tests do not preserve the SAME authored
// row images through a later shape change. Mutant: publish a generation that
// moves resident values out without replacing rows; or skip authored custody.
#[test]
fn ef_search_history_survives_later_shape_change_reopen_export_and_replay() {
    for file in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.db");
        let db = if file {
            Database::open(&path).unwrap()
        } else {
            Database::open_memory()
        };
        let mut units = seed(&db, 24);
        let authored = authored_rows(&db);
        assert_eq!(
            authored.len(),
            25,
            "24 initial rows and one replacement are distinct authored occurrences"
        );
        let authored_values = authored
            .iter()
            .map(|entry| entry.3.clone())
            .collect::<Vec<_>>();
        let expected_authored = (1..=24)
            .chain(std::iter::once(1))
            .enumerate()
            .map(|(ordinal, id)| {
                canonical(&BTreeMap::from([
                    ("id", Value::Int64(id)),
                    ("bucket", Value::Int64(id % 2)),
                    (
                        "body",
                        Value::Text(if ordinal == 24 {
                            "replacement".into()
                        } else {
                            format!("authored-{id}")
                        }),
                    ),
                ]))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            authored_values, expected_authored,
            "the pre-ALTER control contains every original value, including both versions of id 1"
        );
        let old_snapshot = db.snapshot();
        let old_root = db.writer_read_root().unwrap();
        let start = db.current_lsn();
        let mut after = start;
        alter(&db, "(EF_SEARCH = 11)");
        record_authored(&db, &mut after, &mut units);
        alter(&db, "(EF_SEARCH = 11)");
        record_authored(&db, &mut after, &mut units);
        assert_eq!(authored_rows(&db), authored);
        assert_eq!(
            db.execute_at_snapshot(
                "SELECT id, body FROM breadth_rows WHERE id = 1",
                &empty(),
                old_snapshot
            )
            .unwrap()
            .rows(),
            vec![vec![Value::Int64(1), Value::Text("replacement".into())]]
        );
        run(&db, "ALTER TABLE breadth_rows DROP COLUMN body");
        record_authored(&db, &mut after, &mut units);
        assert_eq!(
            authored_rows(&db),
            authored,
            "later projection must retain every original row body exactly once"
        );
        assert_eq!(db.ddl_log_since(start).unwrap().len(), 3);
        if let Some(root) = &old_root {
            let meta = root.borrow_captured_table_meta("breadth_rows").unwrap();
            assert!(meta.columns.iter().any(|c| c.name == "body"));
            assert_eq!(
                meta.columns
                    .iter()
                    .find(|c| c.name == "embedding")
                    .unwrap()
                    .hnsw_ef_search,
                Some(7)
            );
        }
        let expected_schema = canonical(&db.table_meta("breadth_rows").unwrap());
        let expected_ddl = canonical(&db.ddl_log_since(Lsn(0)).unwrap());
        let expected_rows = {
            let fixture_answer_98 = db
            .execute(
                "SELECT id, bucket, embedding, compressed, marker FROM breadth_rows ORDER BY id",
                &empty(),
            )
            .unwrap();
            fixture_answer_98.rows().to_vec()
        };
        // Replay the source's exact ordered changes, never manufacture a new
        // CREATE with the final schema and compare unrelated history digests.
        let expected_identity = declared_identity(&db);
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
            authored_rows(&replay)
                .into_iter()
                .map(|entry| entry.3)
                .collect::<Vec<_>>(),
            authored_values,
            "replaying the exact authored units preserves both original and replaced row values exactly once"
        );
        assert_eq!(
            canonical(&replay.table_meta("breadth_rows").unwrap()),
            expected_schema
        );
        assert_eq!(replay.execute("SELECT id, bucket, embedding, compressed, marker FROM breadth_rows ORDER BY id", &empty()).unwrap().rows(), expected_rows);
        assert_eq!(
            canonical(&replay.ddl_log_since(Lsn(0)).unwrap()),
            expected_ddl
        );
        let exported = dir.path().join("history-export.db");
        db.export_snapshot(&exported).unwrap();
        let restored = Database::open(&exported).unwrap();
        assert_eq!(authored_rows(&restored), authored);
        assert_eq!(
            canonical(&restored.ddl_log_since(Lsn(0)).unwrap()),
            expected_ddl
        );
        assert_eq!(
            canonical(&restored.table_meta("breadth_rows").unwrap()),
            expected_schema
        );
        assert_eq!(restored.execute("SELECT id, bucket, embedding, compressed, marker FROM breadth_rows ORDER BY id", &empty()).unwrap().rows(), expected_rows);
        if expected_identity.is_some() {
            assert_eq!(declared_identity(&restored), expected_identity);
        }
        restored.close().unwrap();
        drop(old_root);
        db.close().unwrap();
        drop(db);
        if file {
            let reopened = Database::open(&path).unwrap();
            assert_eq!(authored_rows(&reopened), authored);
            assert_eq!(
                canonical(&reopened.ddl_log_since(Lsn(0)).unwrap()),
                expected_ddl
            );
            assert_eq!(
                canonical(&reopened.table_meta("breadth_rows").unwrap()),
                expected_schema
            );
            assert_eq!(reopened.execute("SELECT id, bucket, embedding, compressed, marker FROM breadth_rows ORDER BY id", &empty()).unwrap().rows(), expected_rows);
            assert_eq!(declared_identity(&reopened), expected_identity);
            reopened.close().unwrap();
        }
    }
}

// New function: the narrow route must distinguish post-merge semantics and
// explicit staging. Mutant: classify every SET HNSW AST as EF-only.
#[test]
fn topology_radius_structural_and_transactional_alters_keep_general_semantics() {
    for (file, saved_graphs) in [(false, false), (false, true), (true, false), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let db = if file {
            Database::open(dir.path().join("control.db")).unwrap()
        } else {
            Database::open_memory()
        };
        let _ = seed_with_saved_graphs(&db, 24, saved_graphs);
        let authored = authored_rows(&db);
        let rows = visible(&db);
        let original = column(&db).vector_policy_revision;
        take_projected_rows();
        alter(&db, "(M = 24, EF_SEARCH = 11)");
        assert!(
            take_projected_rows() > 0,
            "the general projection control must exercise the live work counter"
        );
        assert!(column(&db).vector_policy_revision > original);
        assert_eq!(column(&db).hnsw_m, Some(24));
        let revised = column(&db).vector_policy_revision;
        let serving = graphs(&db);
        let roots = physical_roots(&db);
        take_projected_rows();
        alter(&db, "(M = 24, EF_SEARCH = 13)");
        assert_eq!(
            take_projected_rows(),
            0,
            "an unchanged explicit topology member does not disqualify a merged EF-only result"
        );
        assert_eq!(column(&db).vector_policy_revision, revised);
        assert_eq!(column(&db).hnsw_ef_search, Some(13));
        assert_eq!(graphs(&db), serving);
        assert_eq!(physical_roots(&db), roots);
        take_projected_rows();
        alter(&db, "(EF_CONSTRUCTION = 400)");
        assert!(
            take_projected_rows() > 0,
            "the general projection control must exercise the live work counter"
        );
        assert!(column(&db).vector_policy_revision > revised);
        assert_eq!(column(&db).hnsw_ef_construction, Some(400));
        take_projected_rows();
        alter(&db, "(SEARCH_RADIUS = 0.5)");
        assert!(
            take_projected_rows() > 0,
            "the general projection control must exercise the live work counter"
        );
        assert_eq!(column(&db).hnsw_search_radius_millionths, Some(500_000));
        let revised = column(&db).vector_policy_revision;
        alter(&db, "DEFAULT");
        let reset = column(&db);
        assert_eq!(
            (
                reset.hnsw_m,
                reset.hnsw_ef_construction,
                reset.hnsw_ef_search,
                reset.hnsw_search_radius_millionths
            ),
            (None, None, None, None)
        );
        assert!(
            reset.vector_policy_revision > revised,
            "group DEFAULT clearing topology is not EF-only"
        );
        let before = canonical(&db.ddl_log_since(Lsn(0)).unwrap());
        let tx = db.begin().unwrap();
        db.execute_in_tx(
            tx,
            "ALTER TABLE breadth_rows ALTER COLUMN embedding SET HNSW (EF_SEARCH = 19)",
            &empty(),
        )
        .unwrap();
        assert_eq!(
            column(&db).hnsw_ef_search,
            None,
            "staged policy cannot leak into autocommit readers"
        );
        assert_eq!(authored_rows(&db), authored);
        assert_eq!(visible(&db), rows);
        db.rollback(tx).unwrap();
        assert_eq!(column(&db).hnsw_ef_search, None);
        assert_eq!(authored_rows(&db), authored);
        assert_eq!(visible(&db), rows);
        assert_eq!(canonical(&db.ddl_log_since(Lsn(0)).unwrap()), before);
        let tx = db.begin().unwrap();
        db.execute_in_tx(
            tx,
            "ALTER TABLE breadth_rows ALTER COLUMN embedding SET HNSW (EF_SEARCH = 19)",
            &empty(),
        )
        .unwrap();
        db.commit(tx).unwrap();
        assert_eq!(column(&db).hnsw_ef_search, Some(19));
        assert_eq!(
            db.ddl_log_since(Lsn(0)).unwrap().len(),
            before.as_array().unwrap().len() + 1
        );
        assert_eq!(authored_rows(&db), authored);
        assert_eq!(visible(&db), rows);
        run(&db, "ALTER TABLE breadth_rows ADD COLUMN added INTEGER");
        assert_eq!(
            db.execute("SELECT added FROM breadth_rows WHERE id = 1", &empty())
                .unwrap()
                .rows(),
            vec![vec![Value::Null]]
        );
        assert_eq!(
            db.execute("SELECT body FROM breadth_rows WHERE id = 1", &empty())
                .unwrap()
                .rows(),
            vec![vec![Value::Text("replacement".into())]]
        );
        db.close().unwrap();
    }
}

// New function: declaration validation tests do not prove real writer pressure
// leaves the same durable data and occurrence history. This exercises an actual
// cap, not a fake payer or an implementation-specific injected preparation name.
#[test]
fn refused_ef_search_keeps_old_declaration_history_and_saved_graph_authoritative() {
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
                "ALTER TABLE breadth_rows ALTER COLUMN embedding SET HNSW (EF_SEARCH = 19)",
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

// New function: direct apply cannot certify authenticated pull's provenance and
// history filtering. Mutant: omit the no-op occurrence or its DDL sidecar from
// transport, or resolve retired authored values under the latest declaration.
#[cfg(all(feature = "sync-orchestration", feature = "test-seams"))]
#[tokio::test]
async fn authenticated_pull_preserves_the_same_authored_history_through_ef_search_and_drop() {
    use crate::transport::in_process::InProcessBroker;
    use crate::{FabricIdentity, SyncClient, SyncServer};
    use contextdb_core::TenantId;
    for file in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(if file {
            Database::open(dir.path().join("hub.db")).unwrap()
        } else {
            Database::open_memory()
        });
        let _ = seed(&source, 24);
        let authored = authored_rows(&source)
            .into_iter()
            .map(|entry| entry.3)
            .collect::<Vec<_>>();
        assert_eq!(authored.len(), 25);
        let edge_path = dir.path().join("edge.db");
        let edge = Arc::new(Database::open(&edge_path).unwrap());
        edge.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        let broker = InProcessBroker::new();
        let identity = Arc::new(FabricIdentity::generate());
        let node = identity.node_id();
        let server = Arc::new(
            SyncServer::with_authenticated_transport_and_identity_for_test(
                source.clone(),
                broker.server_as(&node),
                TenantId::from("breadth-history"),
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
            TenantId::from("breadth-history"),
            identity,
        )
        .unwrap();
        client.pull_default().await.unwrap();
        let before = edge.current_lsn();
        let receiver_authored = authored_rows(&edge);
        assert_eq!(
            receiver_authored
                .iter()
                .map(|entry| entry.3.clone())
                .collect::<Vec<_>>(),
            authored
        );
        for sql in [
            "ALTER TABLE breadth_rows ALTER COLUMN embedding SET HNSW (EF_SEARCH = 11)",
            "ALTER TABLE breadth_rows ALTER COLUMN embedding SET HNSW (EF_SEARCH = 11)",
            "ALTER TABLE breadth_rows DROP COLUMN body",
        ] {
            run(&source, sql);
            client.pull_default().await.unwrap();
            assert_eq!(
                authored_rows(&edge),
                receiver_authored,
                "the exact same receiver history survives each policy and shape publication"
            );
        }
        assert_eq!(
            edge.ddl_log_since(before).unwrap().len(),
            3,
            "the no-op declaration is an authored transport occurrence too"
        );
        assert_eq!(column(&edge).hnsw_ef_search, Some(11));
        assert_eq!(
            canonical(&edge.table_meta("breadth_rows").unwrap()),
            canonical(&source.table_meta("breadth_rows").unwrap())
        );
        let receiver_ddl = canonical(&edge.ddl_log_since(Lsn(0)).unwrap());
        let expected_rows = {
            let fixture_answer_99 = source
            .execute(
                "SELECT id, bucket, embedding, compressed, marker FROM breadth_rows ORDER BY id",
                &empty(),
            )
            .unwrap();
            fixture_answer_99.rows().to_vec()
        };
        assert_eq!(
            edge.execute(
                "SELECT id, bucket, embedding, compressed, marker FROM breadth_rows ORDER BY id",
                &empty()
            )
            .unwrap()
            .rows(),
            expected_rows
        );
        drop(client);
        shutdown.store(true, Ordering::SeqCst);
        task.await.unwrap();
        drop(server);
        edge.close().unwrap();
        drop(edge);
        let reopened = Database::open(&edge_path).unwrap();
        assert_eq!(authored_rows(&reopened), receiver_authored);
        assert_eq!(
            canonical(&reopened.ddl_log_since(Lsn(0)).unwrap()),
            receiver_ddl
        );
        assert_eq!(reopened.execute("SELECT id, bucket, embedding, compressed, marker FROM breadth_rows ORDER BY id", &empty()).unwrap().rows(), expected_rows);
        reopened.close().unwrap();
        source.close().unwrap();
    }
}
