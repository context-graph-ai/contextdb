#![cfg(feature = "test-seams")]

use super::*;
use crate::work_ledger::{BlobHash, InputRef, JobSpec, install_work_ledger_schema, submit_job};
use std::{collections::HashMap, path::PathBuf};

const TABLE: &str = "purge_contract_rows";

struct FileFixture {
    _root: tempfile::TempDir,
    path: PathBuf,
    db: Database,
}

#[derive(Debug, Clone, PartialEq)]
struct ObservablePurgeState {
    rows: Vec<Vec<Value>>,
    history: Vec<RowChange>,
    current_lsn: Lsn,
    selected_one: DurableDeletionStateSnapshot,
    selected_two: DurableDeletionStateSnapshot,
    survivor: DurableDeletionStateSnapshot,
}

fn params() -> HashMap<String, Value> {
    HashMap::new()
}

fn file_fixture() -> FileFixture {
    let root = tempfile::tempdir().expect("create public purge fixture directory");
    let path = root.path().join("public-purge.redb");
    let db = Database::open(&path).expect("open public purge fixture");
    db.execute(
        "CREATE TABLE purge_contract_rows (id INTEGER PRIMARY KEY, cohort TEXT)",
        &params(),
    )
    .expect("create public purge fixture table");
    for (id, cohort) in [(1, "selected"), (2, "selected"), (3, "survivor")] {
        db.execute(
            "INSERT INTO purge_contract_rows (id, cohort) VALUES ($id, $cohort)",
            &HashMap::from([
                ("id".to_string(), Value::Int64(id)),
                ("cohort".to_string(), Value::Text(cohort.to_string())),
            ]),
        )
        .expect("seed public purge fixture row");
    }
    FileFixture {
        _root: root,
        path,
        db,
    }
}

fn ledger_file_fixture() -> FileFixture {
    let root = tempfile::tempdir().expect("create work-ledger purge fixture directory");
    let path = root.path().join("work-ledger-purge.redb");
    let db = Database::open(&path).expect("open work-ledger purge fixture");
    install_work_ledger_schema(&db).expect("install work-ledger purge fixture schema");
    FileFixture {
        _root: root,
        path,
        db,
    }
}

fn submit_blob_job(db: &Database, job_id: &str, input_refs: Vec<InputRef>) {
    let job = JobSpec::builder(job_id, "purge-contract", "input", "hub-node")
        .input_refs(input_refs)
        .submitted_at_ms(1)
        .build();
    submit_job(db, &job, &[] as &[&[u8]]).expect("submit work-ledger blob job");
}

fn work_job_ids(db: &Database) -> Vec<Vec<Value>> {
    db.execute("SELECT job_id FROM work_jobs ORDER BY job_id", &params())
        .expect("read work-ledger job ids")
        .rows
}

fn report_rows(mut pairs: Vec<(String, String)>) -> Vec<Vec<Value>> {
    pairs.sort();
    pairs
        .into_iter()
        .map(|(blob_hash, remaining_job_id)| {
            vec![Value::Text(blob_hash), Value::Text(remaining_job_id)]
        })
        .collect()
}

fn rows(db: &Database) -> Vec<Vec<Value>> {
    db.execute(
        "SELECT id, cohort FROM purge_contract_rows ORDER BY id",
        &params(),
    )
    .expect("read public purge fixture rows")
    .rows
}

fn observable_state(db: &Database) -> ObservablePurgeState {
    ObservablePurgeState {
        rows: rows(db),
        history: table_history(db),
        current_lsn: db.current_lsn(),
        selected_one: db.durable_deletion_state_for_test(TABLE, &Value::Int64(1)),
        selected_two: db.durable_deletion_state_for_test(TABLE, &Value::Int64(2)),
        survivor: db.durable_deletion_state_for_test(TABLE, &Value::Int64(3)),
    }
}

fn table_history(db: &Database) -> Vec<RowChange> {
    db.changes_since(Lsn(0))
        .expect("read the declared changes")
        .rows
        .into_iter()
        .filter(|row| row.table == TABLE)
        .collect()
}

fn history_for_id(history: &[RowChange], id: i64) -> Vec<RowChange> {
    let natural_key = NaturalKey::single("id".to_string(), Value::Int64(id));
    history
        .iter()
        .filter(|row| row.natural_key == natural_key)
        .cloned()
        .collect()
}

fn assert_selected_absent_and_survivor_intact(db: &Database) {
    assert_eq!(
        rows(db),
        vec![vec![Value::Int64(3), Value::Text("survivor".to_string())]]
    );
    let first = db.durable_deletion_state_for_test(TABLE, &Value::Int64(1));
    let second = db.durable_deletion_state_for_test(TABLE, &Value::Int64(2));
    let first_frontier = first
        .purge_frontier
        .expect("the first selected row has a permanent purge frontier");
    let second_frontier = second
        .purge_frontier
        .expect("the second selected row has a permanent purge frontier");
    assert_eq!(
        first_frontier, second_frontier,
        "one public batch assigns both selected rows the same permanent frontier"
    );
    assert_eq!(
        db.durable_deletion_state_for_test(TABLE, &Value::Int64(3))
            .purge_frontier,
        None,
        "the survivor must not receive a purge frontier"
    );
}

fn assert_successful_batch_history(db: &Database, survivor_history_before: &[RowChange]) {
    let history = table_history(db);
    assert!(
        history_for_id(&history, 1).is_empty(),
        "the first selected row leaves no table-scoped history"
    );
    assert!(
        history_for_id(&history, 2).is_empty(),
        "the second selected row leaves no table-scoped history"
    );
    assert_eq!(
        history_for_id(&history, 3),
        survivor_history_before,
        "the survivor keeps its exact ordered table-scoped history"
    );
}

#[test]
fn public_purge_batch_is_atomic_across_injected_persistence_failure_and_reopen() {
    // Authoritative erasure waits for proved orphan cleanup, while maintenance
    // completes outside queries (maintenance intent line 41; root policy 22).
    {
        use contextdb_vector::VectorCopyAdmission;
        use contextdb_vector::hnsw::paged_builder::PagedVectorPhase;
        use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
        let directory = tempfile::tempdir().unwrap();
        let db = Database::open(directory.path().join("orphan-purge.db")).unwrap();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.execute("CREATE TABLE orphan_items (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED HNSW (M = 2, EF_CONSTRUCTION = 4))", &HashMap::new()).unwrap();
        db.execute(
            "INSERT INTO orphan_items VALUES (1, '[1,0,0]'), (2, '[0,1,0]')",
            &HashMap::new(),
        )
        .unwrap();
        let route = VectorPartitionRef::new(
            VectorIndexRef::new("orphan_items", "v"),
            VectorPartitionKey::unpartitioned(),
        );
        let persistence = db.persistence.as_ref().unwrap().clone();
        vector_paged_build::install_paged_root_capture(&persistence, &db.tx_mgr);
        let mut owner = VectorCopyAdmission::new(db.accountant.vector_budget().unwrap());
        let context = db.maintenance_context();
        let record = context
            .start_paged_vector_build(&route, &persistence, &mut owner, true)
            .unwrap()
            .unwrap();
        let plan = vector_paged_build::paged_batch_plan(&record.declaration).unwrap();
        for _ in 0..4 {
            if db
                .vector_store
                .partition_paged_build_progress(&route)
                .unwrap()
                .phase
                == PagedVectorPhase::Complete
            {
                break;
            }
            db.vector_store
                .step_partition_paged_build_with_plan(&route, plan, &mut || Ok(()))
                .unwrap();
        }
        let complete = persistence
            .load_paged_build_record(&route)
            .unwrap()
            .unwrap()
            .progress()
            .unwrap();
        context
            .publish_paged_vector_build(
                &route,
                &persistence,
                &record,
                &complete,
                plan,
                db.declared_reader_memory().unwrap(),
            )
            .unwrap();
        db.execute(
            "CREATE TABLE audit_history (id INT PRIMARY KEY, v VECTOR(3))",
            &HashMap::new(),
        )
        .unwrap();
        db.execute(
            "INSERT INTO audit_history VALUES (1, '[1,0,0]')",
            &HashMap::new(),
        )
        .unwrap();
        // One more real journal key than the production audit's 64-key batch.
        for n in 0..65 {
            let sql = if n % 2 == 0 {
                "UPDATE audit_history SET v = '[0,1,0]'"
            } else {
                "UPDATE audit_history SET v = '[1,0,0]'"
            };
            db.execute(sql, &HashMap::new()).unwrap();
        }
        db.execute(
            "CREATE TABLE purge_target (id INT PRIMARY KEY)",
            &HashMap::new(),
        )
        .unwrap();
        db.execute("INSERT INTO purge_target VALUES (1)", &HashMap::new())
            .unwrap();
        let pages = TableDefinition::<&[u8], &[u8]>::new("vector_paged_generation_pages");
        let cleanup = TableDefinition::<u64, &[u8]>::new("vector_paged_cleanup");
        let footprint = || {
            persistence
                .with_db(|storage| {
                    let read = storage.begin_read().unwrap();
                    let p = read.open_table(pages).unwrap();
                    let c = read.open_table(cleanup).unwrap();
                    Ok((
                        p.len().unwrap(),
                        c.get(0).unwrap().is_some(),
                        c.len().unwrap(),
                    ))
                })
                .unwrap()
        };
        // The actual encoded native pages outlive their removed parent. A
        // malformed existing fence must also be treated as a fence, never as
        // permission to erase. This is a durable fault fixture, not a loader.
        db.tx_mgr
            .with_maintenance_publication(|publication| {
                publication.reserve()?;
                persistence.with_db(|storage| {
                    let write = storage.begin_write().unwrap();
                    for definition in [
                        TableDefinition::<&[u8], &[u8]>::new("vector_partition_generation_catalog"),
                        TableDefinition::<&[u8], &[u8]>::new("vector_partition_base_generations"),
                    ] {
                        let mut table = write.open_table(definition).unwrap();
                        let keys = table
                            .iter()
                            .unwrap()
                            .map(|r| r.unwrap().0.value().to_vec())
                            .collect::<Vec<_>>();
                        for key in keys {
                            table.remove(key.as_slice()).unwrap();
                        }
                    }
                    write
                        .open_table(cleanup)
                        .unwrap()
                        .insert(0, b"damaged-fence".as_slice())
                        .unwrap();
                    write.commit().unwrap();
                    Ok(())
                })?;
                publication.complete();
                Ok(())
            })
            .unwrap();
        let before = footprint();
        assert!(before.0 > 0 && before.1);
        let refused = db.execute("PURGE FROM purge_target WHERE id = 1", &HashMap::new());
        assert!(
            matches!(&refused, Err(Error::StoreCorrupted { path, .. }) if path == "vector_paged_generation_pages"),
            "the orphan ownership fence must return its existing typed refusal, never successful erasure: {refused:?}"
        );
        assert_eq!(
            db.execute("SELECT id FROM purge_target", &HashMap::new())
                .unwrap()
                .rows,
            vec![vec![Value::Int64(1)]]
        );
        assert!(footprint().1 && footprint().0 > 0);
        for _ in 0..32 {
            context.reclaim_paged_vector_storage().unwrap();
            if !footprint().1 && !persistence.has_paged_cleanup().unwrap() {
                break;
            }
        }
        assert_eq!(
            footprint(),
            (0, false, 0),
            "the last proved orphan is drained before erasure succeeds"
        );
        db.execute("PURGE FROM purge_target WHERE id = 1", &HashMap::new())
            .unwrap();
        assert!(
            db.execute("SELECT id FROM purge_target", &HashMap::new())
                .unwrap()
                .rows
                .is_empty()
        );
    }
    // The point-remove fault is thread-local and one-shot. Keep its run in an
    // owned thread so an early public-PURGE failure cannot arm the success
    // baseline below.
    let injected_error = std::thread::spawn(|| {
        let fixture = file_fixture();
        let before = observable_state(&fixture.db);
        fixture
            .db
            .arm_authoritative_purge_point_remove_persistence_failure_for_test();
        let error = fixture
            .db
            .execute(
                "PURGE FROM purge_contract_rows WHERE cohort = 'selected'",
                &params(),
            )
            .expect_err("the one public predicate purge reaches the injected persistence fault");
        fixture
            .db
            .close()
            .expect("close failed public purge fixture before reopen");
        let reopened = Database::open(&fixture.path).expect("reopen failed public purge fixture");
        assert_eq!(
            observable_state(&reopened),
            before,
            "the failed batch leaves both selected rows and every observable frontier/copy unchanged"
        );
        reopened
            .close()
            .expect("close failed public purge fixture after reopen");
        error
    })
    .join()
    .expect("injected public purge test thread did not panic");

    match injected_error {
        Error::Other(message) => assert_eq!(
            message,
            "authoritative purge point-remove persistence failure injected"
        ),
        other => panic!("expected the injected persistence failure, got {other:?}"),
    }

    let fixture = file_fixture();
    let history_before = table_history(&fixture.db);
    let selected_one_history_before = history_for_id(&history_before, 1);
    let selected_two_history_before = history_for_id(&history_before, 2);
    let survivor_history_before = history_for_id(&history_before, 3);
    assert!(
        !selected_one_history_before.is_empty(),
        "the first selected row has table-scoped history before PURGE"
    );
    assert!(
        !selected_two_history_before.is_empty(),
        "the second selected row has table-scoped history before PURGE"
    );
    assert!(
        !survivor_history_before.is_empty(),
        "the survivor has table-scoped history before PURGE"
    );
    let result = fixture
        .db
        .execute(
            "PURGE FROM purge_contract_rows WHERE cohort = 'selected'",
            &params(),
        )
        .expect("one public predicate purge succeeds after the isolated failure run");
    assert_eq!(result.rows_affected, 2);
    assert_selected_absent_and_survivor_intact(&fixture.db);
    assert_successful_batch_history(&fixture.db, &survivor_history_before);
    fixture
        .db
        .close()
        .expect("close successful public purge fixture before reopen");
    let reopened = Database::open(&fixture.path).expect("reopen successful public purge fixture");
    assert_selected_absent_and_survivor_intact(&reopened);
    assert_successful_batch_history(&reopened, &survivor_history_before);
}

#[test]
fn public_purge_reports_only_distinct_sorted_work_job_blob_survivors() {
    let fixture = ledger_file_fixture();
    let first_hash = BlobHash::of(b"first shared blob");
    let second_hash = BlobHash::of(b"second shared blob");
    let third_hash = BlobHash::of(b"third shared blob");
    let unrelated_hash = BlobHash::of(b"unrelated blob");

    submit_blob_job(
        &fixture.db,
        "selected-alpha",
        vec![
            InputRef::blob_ref(first_hash.clone()),
            InputRef::blob_ref(first_hash.clone()),
            InputRef::blob_ref(second_hash.clone()),
        ],
    );
    submit_blob_job(
        &fixture.db,
        "selected-beta",
        vec![
            InputRef::blob_ref(second_hash.clone()),
            InputRef::blob_ref(third_hash.clone()),
        ],
    );
    submit_blob_job(
        &fixture.db,
        "remaining-zeta",
        vec![
            InputRef::blob_ref(first_hash.clone()),
            InputRef::blob_ref(second_hash.clone()),
            InputRef::blob_ref(second_hash.clone()),
        ],
    );
    submit_blob_job(
        &fixture.db,
        "remaining-alpha",
        vec![
            InputRef::blob_ref(second_hash.clone()),
            InputRef::blob_ref(third_hash.clone()),
        ],
    );
    submit_blob_job(
        &fixture.db,
        "unrelated",
        vec![InputRef::blob_ref(unrelated_hash)],
    );

    let result = fixture
        .db
        .execute(
            "PURGE FROM work_jobs WHERE job_id = 'selected-alpha' OR job_id = 'selected-beta'",
            &params(),
        )
        .expect("purge the two selected work jobs");

    assert_eq!(result.rows_affected, 2, "the selected count remains exact");
    assert_eq!(
        result.columns,
        vec!["blob_hash".to_string(), "remaining_job_id".to_string()],
        "the public survivor-report schema stays explicit"
    );
    assert_eq!(
        result.rows,
        report_rows(vec![
            (first_hash.to_hex(), "remaining-zeta".to_string()),
            (second_hash.to_hex(), "remaining-alpha".to_string()),
            (second_hash.to_hex(), "remaining-zeta".to_string()),
            (third_hash.to_hex(), "remaining-alpha".to_string()),
        ]),
        "the report names each remaining referent once, sorted by lowercase hash then job id"
    );
    assert_eq!(
        work_job_ids(&fixture.db),
        vec![
            vec![Value::Text("remaining-alpha".to_string())],
            vec![Value::Text("remaining-zeta".to_string())],
            vec![Value::Text("unrelated".to_string())],
        ],
        "the report is tied to the committed work-job survivors"
    );

    let all_selected = fixture
        .db
        .execute(
            "PURGE FROM work_jobs WHERE job_id = 'remaining-alpha' OR job_id = 'remaining-zeta'",
            &params(),
        )
        .expect("purge every remaining referent for the selected blobs");
    assert_eq!(all_selected.rows_affected, 2);
    assert!(
        all_selected.columns.is_empty() && all_selected.rows.is_empty(),
        "when no remaining work job refers to a selected blob, PURGE returns no report rows"
    );
}

#[test]
fn public_purge_reports_blob_referent_from_a_superseded_selected_work_job_version() {
    let fixture = ledger_file_fixture();
    let historical_hash = BlobHash::of(b"selected historical blob");
    let current_hash = BlobHash::of(b"selected current blob");
    submit_blob_job(
        &fixture.db,
        "selected",
        vec![InputRef::blob_ref(historical_hash.clone())],
    );
    submit_blob_job(
        &fixture.db,
        "survivor",
        vec![InputRef::blob_ref(historical_hash.clone())],
    );
    fixture
        .db
        .execute(
            "UPDATE work_jobs SET input_refs = $input_refs WHERE job_id = 'selected'",
            &HashMap::from([(
                "input_refs".to_string(),
                Value::Json(serde_json::json!([{
                    "kind": "blob_ref",
                    "detail": { "hash": current_hash.to_hex() }
                }])),
            )]),
        )
        .expect("replace the selected work job's current blob reference");
    assert_eq!(
        fixture
            .db
            .execute(
                "SELECT input_refs FROM work_jobs WHERE job_id = 'selected'",
                &params(),
            )
            .expect("read selected work job's current input refs")
            .rows,
        vec![vec![Value::Json(serde_json::json!([{
            "kind": "blob_ref",
            "detail": { "hash": current_hash.to_hex() }
        }]))]],
        "the selected row's current version no longer names the historical blob"
    );

    let result = fixture
        .db
        .execute("PURGE FROM work_jobs WHERE job_id = 'selected'", &params())
        .expect("purge destroys every version of the selected work job");

    assert_eq!(result.rows_affected, 1);
    assert_eq!(
        result.columns,
        vec!["blob_hash".to_string(), "remaining_job_id".to_string()]
    );
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Text(historical_hash.to_hex()),
            Value::Text("survivor".to_string()),
        ]],
        "the historical selected version is destroyed too, so its surviving referent is reported"
    );
    assert_eq!(
        work_job_ids(&fixture.db),
        vec![vec![Value::Text("survivor".to_string())]],
        "only the unselected work job remains"
    );
}

#[test]
fn inbound_work_job_purge_ignores_malformed_unrelated_survivor_input_refs() {
    // Delivered erasure either removes the live selected copy or returns a
    // typed failure before its purge metadata and delivery frontier advance.
    {
        let fixture = file_fixture();
        let before = observable_state(&fixture.db);
        let reader = fixture.db.declared_reader_memory().unwrap();
        let selected = fixture
            .db
            .resolve_authoritative_purge_selection(
                TABLE,
                &NaturalKey::single("id".into(), Value::Int64(1)),
                reader.clone(),
            )
            .unwrap();
        let delivery = AuthoritativePurgeDeliveryItem {
            node_local_predicate: None,
            frontier: Lsn(fixture.db.current_lsn().0 + 1),
            ordinal: 0,
            table: TABLE.to_string(),
            table_generation: selected.table_generation,
            natural_key: selected.natural_key.clone(),
            purged_lineage_roots: vec![selected.lineage_root.clone()],
        };
        let key = Database::durable_lineage_table_generation_key(TABLE);
        let persistence = fixture.db.persistence.as_ref().unwrap();
        fixture
            .db
            .publish_direct_metadata(|| {
                persistence.flush_encoded_config_values(vec![(
                    key.as_str(),
                    RedbPersistence::encode_config_value(&0_u64)?,
                )])
            })
            .unwrap();
        let original = fixture
            .db
            .durable_lineage_table_generation(TABLE, reader.clone())
            .unwrap_err();
        let original_message = format!("{original:?}");
        assert!(matches!(original, Error::StoreCorrupted { ref path, .. } if path == &key));
        let mut copy = crate::vector_read_memory::admission(reader.clone());
        let outcome = fixture
            .db
            .apply_incoming_authoritative_purge_batch_while_authoritative(
                std::slice::from_ref(&delivery),
                &mut copy,
                reader.clone(),
            );
        let after_failed_delivery_lsn = fixture.db.current_lsn();
        // Restore only the damaged generation record before reading the row
        // and purge witnesses. A failed delivery has published none of them.
        fixture
            .db
            .publish_direct_metadata(|| {
                persistence.flush_encoded_config_values(vec![(
                    key.as_str(),
                    RedbPersistence::encode_config_value(&selected.table_generation)?,
                )])
            })
            .unwrap();
        assert_eq!(
            outcome.as_ref().err().map(|error| format!("{error:?}")),
            Some(original_message)
        );
        assert_eq!(after_failed_delivery_lsn, before.current_lsn);
        assert_eq!(observable_state(&fixture.db), before);
        fixture
            .db
            .apply_incoming_authoritative_purge_batch_while_authoritative(
                std::slice::from_ref(&delivery),
                &mut copy,
                reader.clone(),
            )
            .unwrap();
        assert_eq!(
            rows(&fixture.db),
            vec![
                vec![Value::Int64(2), Value::Text("selected".into())],
                vec![Value::Int64(3), Value::Text("survivor".into())]
            ]
        );
        let erased = fixture
            .db
            .durable_deletion_state_for_test(TABLE, &Value::Int64(1));
        assert!(erased.purge_frontier.is_some());
    }
    let fixture = ledger_file_fixture();
    let selected_hash = BlobHash::of(b"inbound selected blob");
    submit_blob_job(
        &fixture.db,
        "selected",
        vec![InputRef::blob_ref(selected_hash)],
    );
    submit_blob_job(
        &fixture.db,
        "malformed-survivor",
        vec![InputRef::ledger_input()],
    );
    let malformed_input_refs = Value::Json(serde_json::json!({ "not": "an input-ref array" }));
    fixture
        .db
        .execute(
            "UPDATE work_jobs SET input_refs = $input_refs WHERE job_id = 'malformed-survivor'",
            &HashMap::from([("input_refs".to_string(), malformed_input_refs.clone())]),
        )
        .expect("make the unrelated current work job malformed without weakening SQL JSON typing");

    let selected_key =
        NaturalKey::single("job_id".to_string(), Value::Text("selected".to_string()));
    let fixture_operation_reader_1 = fixture
        .db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let selected = fixture
        .db
        .resolve_authoritative_purge_selection(
            "work_jobs",
            &selected_key,
            fixture_operation_reader_1.clone(),
        )
        .expect("resolve local selected work-job lineage for inbound purge");
    let delivery = AuthoritativePurgeDeliveryItem {
        node_local_predicate: None,
        frontier: Lsn(fixture.db.current_lsn().0.saturating_add(1)),
        ordinal: 0,
        table: "work_jobs".to_string(),
        table_generation: selected.table_generation,
        natural_key: selected.natural_key.clone(),
        purged_lineage_roots: vec![selected.lineage_root.clone()],
    };
    let fixture_operation_reader_1 = fixture
        .db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_1 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_1.clone());

    fixture
        .db
        .apply_incoming_authoritative_purge_batch_while_authoritative(
            &[delivery],
            &mut fixture_operation_copy_1,
            fixture_operation_reader_1.clone(),
        )
        .expect("inbound purge must not parse an unrelated local survivor report");

    assert_eq!(
        work_job_ids(&fixture.db),
        vec![vec![Value::Text("malformed-survivor".to_string())]],
        "the inbound purge removes only the selected work job"
    );
    assert_eq!(
        fixture
            .db
            .execute(
                "SELECT input_refs FROM work_jobs WHERE job_id = 'malformed-survivor'",
                &params(),
            )
            .expect("read retained malformed survivor")
            .rows,
        vec![vec![malformed_input_refs]],
        "the malformed unrelated survivor remains untouched by inbound delivery"
    );
}

#[test]
fn public_purge_does_not_report_ordinary_lookalike_rows() {
    let fixture = ledger_file_fixture();
    fixture
        .db
        .execute(
            "CREATE TABLE ordinary_job_referents (job_id TEXT PRIMARY KEY, input_refs JSON NOT NULL)",
            &params(),
        )
        .expect("create ordinary work-job lookalike table");
    let shared_hash = BlobHash::of(b"lookalike shared blob");
    for job_id in ["selected", "remaining"] {
        fixture
            .db
            .execute(
                "INSERT INTO ordinary_job_referents (job_id, input_refs) VALUES ($job_id, $input_refs)",
                &HashMap::from([
                    ("job_id".to_string(), Value::Text(job_id.to_string())),
                    (
                        "input_refs".to_string(),
                        Value::Json(serde_json::json!([{
                            "kind": "blob_ref",
                            "detail": { "hash": shared_hash.to_hex() }
                        }])),
                    ),
                ]),
            )
            .expect("insert ordinary lookalike row");
    }

    let result = fixture
        .db
        .execute(
            "PURGE FROM ordinary_job_referents WHERE job_id = 'selected'",
            &params(),
        )
        .expect("purge ordinary lookalike row");
    assert_eq!(result.rows_affected, 1);
    assert!(
        result.columns.is_empty() && result.rows.is_empty(),
        "only the canonical work_jobs table receives a blob-survivor report"
    );
}

#[test]
fn work_job_purge_persistence_failure_returns_no_report_and_keeps_rows_after_reopen() {
    let injected_error = std::thread::spawn(|| {
        let fixture = ledger_file_fixture();
        let shared_hash = BlobHash::of(b"failure-is-atomic shared blob");
        submit_blob_job(
            &fixture.db,
            "selected",
            vec![InputRef::blob_ref(shared_hash.clone())],
        );
        submit_blob_job(
            &fixture.db,
            "remaining",
            vec![InputRef::blob_ref(shared_hash)],
        );
        let before = work_job_ids(&fixture.db);
        let before_lsn = fixture.db.current_lsn();

        fixture
            .db
            .arm_authoritative_purge_point_remove_persistence_failure_for_test();
        let error = fixture
            .db
            .execute("PURGE FROM work_jobs WHERE job_id = 'selected'", &params())
            .expect_err("the injected durable failure must produce no QueryResult");
        fixture
            .db
            .close()
            .expect("close failed work-job purge fixture before reopen");
        let reopened = Database::open(&fixture.path).expect("reopen failed work-job purge fixture");
        assert_eq!(
            work_job_ids(&reopened),
            before,
            "a failed purge neither removes a work job nor returns a survivor report"
        );
        assert_eq!(
            reopened.current_lsn(),
            before_lsn,
            "the failed purge does not reserve a visible durable mutation position"
        );
        reopened
            .close()
            .expect("close failed work-job purge fixture after reopen");
        error
    })
    .join()
    .expect("work-job purge failure test thread did not panic");

    match injected_error {
        Error::Other(message) => assert_eq!(
            message,
            "authoritative purge point-remove persistence failure injected"
        ),
        other => panic!("expected the injected persistence failure, got {other:?}"),
    }
}

#[test]
fn public_purge_requires_standalone_execution_without_invalidating_transactions() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE purge_contract_rows (id INTEGER PRIMARY KEY, cohort TEXT)",
        &params(),
    )
    .expect("create standalone-execution fixture table");
    db.execute(
        "INSERT INTO purge_contract_rows (id, cohort) VALUES (1, 'selected')",
        &params(),
    )
    .expect("insert standalone-execution fixture row");
    let before = rows(&db);

    db.execute("BEGIN", &params())
        .expect("open SQL transaction before PURGE");
    let sql_begin_error = db
        .execute(
            "PURGE FROM purge_contract_rows WHERE cohort = 'selected'",
            &params(),
        )
        .expect_err("PURGE inside SQL BEGIN is refused before mutation");
    assert!(matches!(
        sql_begin_error,
        Error::PurgeRequiresStandaloneExecution
    ));
    assert_eq!(rows(&db), before);
    db.execute("ROLLBACK", &params())
        .expect("SQL transaction remains valid for explicit rollback");

    let tx = db.begin().expect("open direct transaction before PURGE");
    let direct_tx_error = db
        .execute_in_tx(
            tx,
            "PURGE FROM purge_contract_rows WHERE cohort = 'selected'",
            &params(),
        )
        .expect_err("PURGE through execute_in_tx is refused before mutation");
    assert!(matches!(
        direct_tx_error,
        Error::PurgeRequiresStandaloneExecution
    ));
    assert_eq!(rows(&db), before);
    db.rollback(tx)
        .expect("direct transaction remains valid for explicit rollback");
}

#[test]
fn public_purge_unknown_predicate_column_fails_before_mutation() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE purge_contract_rows (id INTEGER PRIMARY KEY, cohort TEXT)",
        &params(),
    )
    .expect("create unknown-column fixture table");
    db.execute(
        "INSERT INTO purge_contract_rows (id, cohort) VALUES (1, 'selected')",
        &params(),
    )
    .expect("insert unknown-column fixture row");
    let before = rows(&db);

    let error = db
        .execute(
            "PURGE FROM purge_contract_rows WHERE missing_column = 'selected'",
            &params(),
        )
        .expect_err("unknown PURGE predicate column is rejected before mutation");
    assert!(matches!(
        error,
        Error::ColumnNotFound { table, column }
            if table == TABLE && column == "missing_column"
    ));
    assert_eq!(rows(&db), before);
}

#[test]
fn purge_statement_is_callback_forbidden_before_queue_drain() {
    let statement =
        contextdb_parser::parse("PURGE FROM purge_contract_rows WHERE cohort = 'selected'")
            .expect("parse public PURGE statement");
    assert!(matches!(statement, Statement::Purge(_)));
    assert!(
        Database::statement_forbidden_inside_cron_callback(&statement),
        "a callback must refuse PURGE before its queue drain can begin"
    );
}
