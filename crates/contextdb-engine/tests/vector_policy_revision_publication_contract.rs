#![cfg(feature = "test-seams")]
//! Online vector-policy revision publication through caller-driven maintenance
//! and indexed search. A preparation pause holds a real unpublished build while
//! queries and newer declarations proceed; its timeout only guards deadlock.

use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{
    Database, MaintenancePolicy, QueryResult, VectorMaintenancePreparationPhaseForTest,
};
use contextdb_vector::VectorPartitionRef;
use std::collections::HashMap;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

const SAFETY_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_MAINTENANCE_CYCLES: usize = 32;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn index() -> VectorIndexRef {
    VectorIndexRef::new("revision_docs", "embedding")
}

fn scope() -> Uuid {
    Uuid::from_u128(0x6101)
}

fn partition() -> VectorPartitionRef {
    VectorPartitionRef::new(
        index(),
        VectorPartitionKey::from_values(&[Value::Uuid(scope())])
            .expect("the fixture UUID has one canonical partition key"),
    )
}

fn axis(axis: usize) -> Vec<f32> {
    let mut vector = vec![0.0; 3];
    vector[axis] = 1.0;
    vector
}

fn ids(result: &QueryResult) -> Vec<Uuid> {
    let id_column = result
        .columns
        .iter()
        .position(|name| name == "id" || name.rsplit('.').next() == Some("id"))
        .expect("the vector projection has id");
    result
        .rows
        .iter()
        .map(|row| match row.get(id_column) {
            Some(Value::Uuid(id)) => *id,
            value => panic!("vector query returned a non-UUID id: {value:?}"),
        })
        .collect()
}

fn column(result: &QueryResult, name: &str) -> usize {
    result
        .columns
        .iter()
        .position(|column| column == name)
        .unwrap_or_else(|| panic!("inspection omits {name}: {:?}", result.columns))
}

fn value<'a>(result: &'a QueryResult, row: usize, name: &str) -> &'a Value {
    result
        .rows
        .get(row)
        .and_then(|row| row.get(column(result, name)))
        .unwrap_or_else(|| panic!("inspection omits row {row} field {name}: {result:?}"))
}

fn int(result: &QueryResult, row: usize, name: &str) -> i64 {
    match value(result, row, name) {
        Value::Int64(value) => *value,
        value => panic!("inspection {name} is an integer: {value:?}"),
    }
}

fn partition_row(db: &Database) -> QueryResult {
    db.execute(
        "SHOW VECTOR_PARTITIONS FOR revision_docs.embedding",
        &empty(),
    )
    .expect("inspect revision policy detail")
}

fn index_row(db: &Database) -> QueryResult {
    db.execute("SHOW VECTOR_INDEXES", &empty())
        .expect("inspect revision policy summary")
}

fn single_partition_row(result: &QueryResult) -> usize {
    assert_eq!(result.rows.len(), 1, "one declared partition detail row");
    0
}

fn indexed_query(db: &Database) -> Vec<Uuid> {
    ids(&db
        .execute(
            "SELECT id FROM revision_docs WHERE scope_id = $scope \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 3",
            &params([
                ("scope", Value::Uuid(scope())),
                ("query", Value::Vector(axis(0))),
            ]),
        )
        .expect("INDEXED query reaches the existing maintained route"))
}

fn create_current_control(db: &Database) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE revision_docs (\
         id UUID PRIMARY KEY, \
         scope_id UUID NOT NULL, \
         embedding VECTOR(3) PARTITION_KEY (scope_id) SEARCH_MODE INDEXED\
         )",
        &empty(),
    )
    .expect("create current maintained route control");
    for (id, embedding) in [
        (Uuid::from_u128(0x6111), axis(0)),
        (Uuid::from_u128(0x6112), vec![0.9, 0.1, 0.0]),
        (Uuid::from_u128(0x6113), axis(1)),
    ] {
        db.execute(
            "INSERT INTO revision_docs (id, scope_id, embedding) VALUES ($id, $scope, $embedding)",
            &params([
                ("id", Value::Uuid(id)),
                ("scope", Value::Uuid(scope())),
                ("embedding", Value::Vector(embedding)),
            ]),
        )
        .expect("seed current maintained route control");
    }
}

fn drive_until_indexed(db: &Database) -> u64 {
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        if let Some(serial) = db.__debug_vector_hnsw_build_serial_for_test(index()) {
            return serial;
        }
        db.run_maintenance_cycle()
            .expect("one finite caller-driven maintenance cycle returns");
    }
    panic!("caller-driven maintenance did not publish the current maintained route")
}

fn assert_current_control(db: &Database) -> u64 {
    let serial = drive_until_indexed(db);
    assert_eq!(
        indexed_query(db),
        vec![
            Uuid::from_u128(0x6111),
            Uuid::from_u128(0x6112),
            Uuid::from_u128(0x6113),
        ],
        "the existing complete graph remains an INDEXED serving route"
    );
    assert_eq!(
        db.__debug_vector_hnsw_build_serial_for_test(index()),
        Some(serial),
        "the current control query must not create a replacement graph"
    );
    serial
}

fn alter_auto_index_at(db: &Database, value: i64) {
    db.execute(
        &format!("ALTER TABLE revision_docs ALTER COLUMN embedding SET AUTO_INDEX_AT {value}"),
        &empty(),
    )
    .expect("AUTO_INDEX_AT applies to newly opened queries without topology rebuild");
}

fn alter_hnsw(db: &Database, members: &str) {
    db.execute(
        &format!("ALTER TABLE revision_docs ALTER COLUMN embedding SET HNSW ({members})"),
        &empty(),
    )
    .expect("declared HNSW policy updates atomically");
}

#[test]
fn online_vector_policy_revisions_keep_the_complete_graph_serving_and_reject_stale_publication() {
    let root = TempDir::new().expect("temporary durable revision store");
    let path = root.path().join("revision-policy.redb");
    let db = Arc::new(Database::open(&path).expect("open durable revision fixture"));
    create_current_control(&db);
    let serving_serial = assert_current_control(&db);
    let serving_topology = db
        .__debug_vector_hnsw_topology_digest_for_test(index())
        .expect("current complete graph has a topology digest");

    // The control above has already executed real caller-driven
    // maintenance and an INDEXED search before this new SQL.
    alter_auto_index_at(&db, 2);

    let summary_after_auto = index_row(&db);
    let summary_row = summary_after_auto
        .rows
        .iter()
        .position(|row| {
            row[column(&summary_after_auto, "table")] == Value::Text("revision_docs".into())
                && row[column(&summary_after_auto, "column")] == Value::Text("embedding".into())
        })
        .expect("one revision policy summary row");
    assert_eq!(
        value(&summary_after_auto, summary_row, "declared_auto_index_at"),
        &Value::Int64(2)
    );
    assert_eq!(
        value(&summary_after_auto, summary_row, "effective_auto_index_at"),
        &Value::Int64(2)
    );
    let after_auto = partition_row(&db);
    let row = single_partition_row(&after_auto);
    let initial_revision = int(&after_auto, row, "serving_policy_revision");
    assert_eq!(
        int(&after_auto, row, "desired_policy_revision"),
        initial_revision,
        "AUTO_INDEX_AT does not create a topology-policy revision"
    );
    assert_eq!(
        db.__debug_vector_hnsw_build_serial_for_test(index()),
        Some(serving_serial),
        "AUTO_INDEX_AT changes query routing, never HNSW topology"
    );
    assert_eq!(
        db.__debug_vector_hnsw_topology_digest_for_test(index()),
        Some(serving_topology),
        "AUTO_INDEX_AT changes query routing, never HNSW topology"
    );

    alter_hnsw(&db, "EF_SEARCH = 2");
    let summary_after_ef = index_row(&db);
    let summary_row = summary_after_ef
        .rows
        .iter()
        .position(|row| {
            row[column(&summary_after_ef, "table")] == Value::Text("revision_docs".into())
                && row[column(&summary_after_ef, "column")] == Value::Text("embedding".into())
        })
        .expect("one revision policy summary row after EF_SEARCH declaration");
    assert_eq!(
        value(&summary_after_ef, summary_row, "declared_hnsw_m"),
        &Value::Null
    );
    assert_eq!(
        value(
            &summary_after_ef,
            summary_row,
            "declared_hnsw_ef_construction"
        ),
        &Value::Null
    );
    assert_eq!(
        value(&summary_after_ef, summary_row, "declared_hnsw_ef_search"),
        &Value::Int64(2)
    );
    let after_ef = partition_row(&db);
    let row = single_partition_row(&after_ef);
    assert_eq!(
        int(&after_ef, row, "desired_policy_revision"),
        initial_revision,
        "EF_SEARCH does not create a topology-policy revision"
    );
    assert_eq!(
        int(&after_ef, row, "serving_policy_revision"),
        initial_revision,
        "EF_SEARCH does not replace the complete graph"
    );
    assert_eq!(
        value(&after_ef, row, "desired_hnsw_ef_search"),
        &Value::Int64(2)
    );
    assert_eq!(
        value(&after_ef, row, "serving_hnsw_ef_search"),
        &Value::Int64(2)
    );
    assert_eq!(
        db.__debug_vector_hnsw_build_serial_for_test(index()),
        Some(serving_serial),
        "EF_SEARCH applies to newly opened queries without a replacement build"
    );

    alter_hnsw(&db, "M = 24, EF_CONSTRUCTION = 400");
    let building = partition_row(&db);
    let row = single_partition_row(&building);
    assert_eq!(value(&building, row, "desired_hnsw_m"), &Value::Int64(24));
    assert_eq!(
        value(&building, row, "desired_hnsw_ef_construction"),
        &Value::Int64(400)
    );
    assert_eq!(value(&building, row, "serving_hnsw_m"), &Value::Int64(16));
    assert_eq!(
        value(&building, row, "serving_hnsw_ef_construction"),
        &Value::Int64(200)
    );
    assert_eq!(
        int(&building, row, "desired_policy_revision"),
        initial_revision + 1
    );
    assert_eq!(
        int(&building, row, "serving_policy_revision"),
        initial_revision
    );
    assert_eq!(
        indexed_query(&db),
        vec![
            Uuid::from_u128(0x6111),
            Uuid::from_u128(0x6112),
            Uuid::from_u128(0x6113),
        ],
        "the prior complete graph keeps serving while its replacement is pending"
    );

    let pause = db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(
        &partition(),
        VectorMaintenancePreparationPhaseForTest::EncodeGraph,
    );
    let worker_db = Arc::clone(&db);
    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        done_tx
            .send(worker_db.run_maintenance_cycle())
            .expect("maintenance result receiver remains live");
    });
    if let Err(error) = pause.wait_until_reached_timeout(SAFETY_TIMEOUT) {
        pause.release();
        let _ = done_rx.recv_timeout(SAFETY_TIMEOUT);
        worker
            .join()
            .expect("released maintenance worker exits after an unreached pause");
        panic!("replacement maintenance did not reach EncodeGraph: {error}");
    }
    let paused = partition_row(&db);
    let row = single_partition_row(&paused);
    assert_eq!(
        value(&paused, row, "maintenance_state"),
        &Value::Text("building".into())
    );
    assert_eq!(
        int(&paused, row, "desired_policy_revision"),
        initial_revision + 1
    );
    assert_eq!(
        int(&paused, row, "serving_policy_revision"),
        initial_revision
    );
    assert_eq!(
        indexed_query(&db),
        vec![
            Uuid::from_u128(0x6111),
            Uuid::from_u128(0x6112),
            Uuid::from_u128(0x6113),
        ],
        "the paused replacement cannot make the complete serving graph unavailable"
    );

    alter_hnsw(&db, "M = 12, EF_CONSTRUCTION = 64");
    let newer = partition_row(&db);
    let row = single_partition_row(&newer);
    assert_eq!(value(&newer, row, "desired_hnsw_m"), &Value::Int64(12));
    assert_eq!(
        value(&newer, row, "desired_hnsw_ef_construction"),
        &Value::Int64(64)
    );
    assert_eq!(
        int(&newer, row, "desired_policy_revision"),
        initial_revision + 2
    );
    assert_eq!(
        int(&newer, row, "serving_policy_revision"),
        initial_revision
    );
    pause.release();
    let stale_report = done_rx
        .recv_timeout(SAFETY_TIMEOUT)
        .expect("released finite maintenance returns")
        .expect("stale replacement abandons rather than publishes");
    worker.join().expect("released maintenance worker exits");
    assert_eq!(
        stale_report.vector.built_partitions, 0,
        "an obsolete durable candidate is not a successful partition publication"
    );
    assert_eq!(
        stale_report.vector.built_indexes, 0,
        "an obsolete durable candidate is not a successful index publication"
    );
    assert!(
        stale_report.vector.first_failure.is_none(),
        "rejecting stale work leaves the newest revision pending without fabricating a failure"
    );
    let stale_released = partition_row(&db);
    let row = single_partition_row(&stale_released);
    assert_eq!(
        int(&stale_released, row, "serving_policy_revision"),
        initial_revision,
        "the released older build cannot publish before a later maintenance cycle builds revision +2"
    );
    assert_eq!(
        db.__debug_vector_hnsw_build_serial_for_test(index()),
        Some(serving_serial),
        "the released older build cannot replace the complete graph after revision +2 is declared"
    );

    for _ in 0..MAX_MAINTENANCE_CYCLES {
        let observed = partition_row(&db);
        let row = single_partition_row(&observed);
        if int(&observed, row, "serving_policy_revision") == initial_revision + 2 {
            break;
        }
        db.run_maintenance_cycle()
            .expect("finite caller-driven maintenance advances the newest revision");
    }
    let published = partition_row(&db);
    let row = single_partition_row(&published);
    assert_eq!(
        int(&published, row, "desired_policy_revision"),
        initial_revision + 2
    );
    assert_eq!(
        int(&published, row, "serving_policy_revision"),
        initial_revision + 2
    );
    assert_eq!(value(&published, row, "desired_hnsw_m"), &Value::Int64(12));
    assert_eq!(value(&published, row, "serving_hnsw_m"), &Value::Int64(12));
    assert_eq!(
        value(&published, row, "desired_hnsw_ef_construction"),
        &Value::Int64(64)
    );
    assert_eq!(
        value(&published, row, "serving_hnsw_ef_construction"),
        &Value::Int64(64)
    );
    assert_ne!(
        db.__debug_vector_hnsw_build_serial_for_test(index()),
        Some(serving_serial),
        "only the newest desired policy may publish a replacement graph"
    );

    db.close().expect("close durable policy fixture");
    drop(db);
    let reopened = Database::open(&path).expect("reopen durable policy fixture");
    let reopened_detail = partition_row(&reopened);
    let row = single_partition_row(&reopened_detail);
    assert_eq!(
        int(&reopened_detail, row, "desired_policy_revision"),
        initial_revision + 2,
        "restart preserves newest desired revision"
    );
    assert_eq!(
        int(&reopened_detail, row, "serving_policy_revision"),
        initial_revision + 2,
        "restart preserves the published serving revision"
    );
    assert_eq!(
        value(&reopened_detail, row, "desired_hnsw_m"),
        &Value::Int64(12)
    );
    assert_eq!(
        value(&reopened_detail, row, "serving_hnsw_m"),
        &Value::Int64(12)
    );
}

#[test]
fn explicit_current_defaults_keep_their_identity_without_rebuilding_matching_graphs() {
    use contextdb_core::VectorQuantization;
    use contextdb_engine::cli_render::render_table_meta;
    use contextdb_vector::store::VectorIndexLayout;

    for durable in [false, true] {
        let root = TempDir::new().unwrap();
        let path = root.path().join("explicit-policy.redb");
        let mut db = if durable {
            Database::open(&path).unwrap()
        } else {
            Database::open_memory()
        };
        create_current_control(&db);
        let serial = assert_current_control(&db);
        let before = partition_row(&db);
        let initial_revision = int(&before, 0, "serving_policy_revision");
        let original_meta = db.table_meta("revision_docs").unwrap();
        alter_auto_index_at(&db, 1_000);
        alter_hnsw(&db, "M = 16, EF_CONSTRUCTION = 200, EF_SEARCH = 200");
        let declared = db.table_meta("revision_docs").unwrap();
        assert_ne!(
            declared, original_meta,
            "equal current settings do not erase authored identity"
        );
        let column = declared
            .columns
            .iter()
            .find(|column| column.name == "embedding")
            .unwrap();
        assert_eq!(
            (
                column.auto_index_at,
                column.hnsw_m,
                column.hnsw_ef_construction,
                column.hnsw_ef_search
            ),
            (Some(1_000), Some(16), Some(200), Some(200))
        );
        assert_eq!(column.vector_policy_revision, initial_revision as u64 + 1);
        assert!(
            render_table_meta("revision_docs", &declared).contains(
                "AUTO_INDEX_AT 1000 HNSW (M = 16, EF_CONSTRUCTION = 200, EF_SEARCH = 200)"
            )
        );
        let layout = db.vector_store_for_test().index_layout(&index()).unwrap();
        assert_eq!(layout.policy_revision, column.vector_policy_revision);
        let at_larger_population = layout.resolve_policy(5_001, 1);
        let adaptive =
            VectorIndexLayout::unpartitioned(3, VectorQuantization::F32).resolve_policy(5_001, 1);
        assert_eq!(
            (
                at_larger_population.hnsw_m,
                at_larger_population.hnsw_ef_construction
            ),
            (16, 200)
        );
        assert_eq!((adaptive.hnsw_m, adaptive.hnsw_ef_construction), (24, 400));
        assert_eq!(layout.resolve_policy(3, 1).auto_index_at_source, "declared");
        assert_eq!(layout.resolve_policy(3, 1).ef_search_source, "declared");
        for _ in 0..3 {
            db.run_maintenance_cycle().unwrap();
        }
        let observed = partition_row(&db);
        assert_eq!(
            int(&observed, 0, "desired_policy_revision"),
            initial_revision + 1
        );
        assert_eq!(
            int(&observed, 0, "serving_policy_revision"),
            initial_revision,
            "a reused graph keeps its actual build revision"
        );
        assert_eq!(
            value(&observed, 0, "maintenance_reason"),
            &Value::Text("none".into())
        );
        assert_eq!(
            db.__debug_vector_hnsw_build_serial_for_test(index()),
            Some(serial)
        );
        assert_eq!(indexed_query(&db).len(), 3);

        if durable {
            db.close().unwrap();
            drop(db);
            db = Database::open(&path).unwrap();
            db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            assert_eq!(db.table_meta("revision_docs").unwrap(), declared);
            assert_eq!(
                db.vector_store_for_test().index_layout(&index()).unwrap(),
                layout
            );
            assert_eq!(indexed_query(&db).len(), 3);
            db.run_maintenance_cycle().unwrap();
            let reopened = partition_row(&db);
            assert_eq!(
                int(&reopened, 0, "desired_policy_revision"),
                initial_revision + 1
            );
            assert_eq!(
                int(&reopened, 0, "serving_policy_revision"),
                initial_revision
            );
            assert_eq!(
                db.__debug_vector_hnsw_build_serial_for_test(index()),
                Some(serial)
            );
        }
        alter_hnsw(&db, "M = 16, EF_CONSTRUCTION = 200, EF_SEARCH = 200");
        assert_eq!(
            db.table_meta("revision_docs").unwrap(),
            declared,
            "repeating the same declaration is not a revision"
        );
        db.run_maintenance_cycle().unwrap();
        assert_eq!(
            db.__debug_vector_hnsw_build_serial_for_test(index()),
            Some(serial)
        );
        db.execute(
            "ALTER TABLE revision_docs ALTER COLUMN embedding SET HNSW DEFAULT",
            &empty(),
        )
        .unwrap();
        db.run_maintenance_cycle().unwrap();
        let cleared = partition_row(&db);
        assert_eq!(
            int(&cleared, 0, "desired_policy_revision"),
            initial_revision + 2
        );
        assert_eq!(
            int(&cleared, 0, "serving_policy_revision"),
            initial_revision
        );
        assert!(
            !render_table_meta("revision_docs", &db.table_meta("revision_docs").unwrap())
                .contains("HNSW")
        );
        assert_eq!(
            db.__debug_vector_hnsw_build_serial_for_test(index()),
            Some(serial)
        );

        alter_hnsw(&db, "M = 24, EF_CONSTRUCTION = 400");
        assert_eq!(
            indexed_query(&db).len(),
            3,
            "a real topology change keeps the old graph serving"
        );
        for _ in 0..MAX_MAINTENANCE_CYCLES {
            if int(&partition_row(&db), 0, "serving_policy_revision") == initial_revision + 3 {
                break;
            }
            db.run_maintenance_cycle().unwrap();
        }
        let published = partition_row(&db);
        assert_eq!(
            int(&published, 0, "serving_policy_revision"),
            initial_revision + 3
        );
        assert_eq!(value(&published, 0, "serving_hnsw_m"), &Value::Int64(24));
        assert_eq!(
            value(&published, 0, "serving_hnsw_ef_construction"),
            &Value::Int64(400)
        );
        assert_ne!(
            db.__debug_vector_hnsw_build_serial_for_test(index()),
            Some(serial)
        );
    }
}

#[test]
fn registered_layout_checks_every_authored_policy_member_and_revision_before_insert() {
    use contextdb_core::Error;

    let db = Database::open_memory();
    create_current_control(&db);
    let store = db.vector_store_for_test();
    let expected = store.index_layout(&index()).unwrap();
    let metadata = db.table_meta("revision_docs").unwrap();
    for field in [
        "auto_index_at",
        "hnsw_m",
        "hnsw_ef_construction",
        "hnsw_ef_search",
        "policy_revision",
    ] {
        let mut stale = expected.clone();
        match field {
            "auto_index_at" => stale.auto_index_at = Some(1_000),
            "hnsw_m" => stale.hnsw_m = Some(16),
            "hnsw_ef_construction" => stale.hnsw_ef_construction = Some(200),
            "hnsw_ef_search" => stale.hnsw_ef_search = Some(200),
            "policy_revision" => stale.policy_revision += 1,
            _ => unreachable!(),
        }
        store.register_index_with_layout(index(), stale).unwrap();
        let error = db
            .execute(
                "INSERT INTO revision_docs (id, scope_id, embedding) VALUES ($id, $scope, $vector)",
                &params([
                    ("id", Value::Uuid(Uuid::from_u128(0x61FF))),
                    ("scope", Value::Uuid(scope())),
                    ("vector", Value::Vector(axis(0))),
                ]),
            )
            .expect_err("a stale registered layout refuses before row/vector publication");
        assert!(
            matches!(&error, Error::Other(message) if message.starts_with("registered vector layout does not match the durable declaration")),
            "{field}: {error:?}"
        );
        store
            .register_index_with_layout(index(), expected.clone())
            .unwrap();
        assert_eq!(db.table_meta("revision_docs").unwrap(), metadata);
        assert_eq!(
            db.execute("SELECT id FROM revision_docs", &empty())
                .unwrap()
                .rows
                .len(),
            3
        );
        assert_eq!(
            store
                .partition_info(&index(), &partition().partition_key)
                .unwrap()
                .live_rows,
            3
        );
    }
    db.execute(
        "INSERT INTO revision_docs (id, scope_id, embedding) VALUES ($id, $scope, $vector)",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(0x61FF))),
            ("scope", Value::Uuid(scope())),
            ("vector", Value::Vector(axis(0))),
        ]),
    )
    .expect("matching declarations still admit the same row and vector");
    assert_eq!(
        db.execute("SELECT id FROM revision_docs", &empty())
            .unwrap()
            .rows
            .len(),
        4
    );
}

#[test]
fn restoring_explicit_defaults_abandons_an_inflight_revision_without_a_redundant_build() {
    let root = TempDir::new().unwrap();
    let db = Arc::new(Database::open(root.path().join("restored-policy.redb")).unwrap());
    create_current_control(&db);
    let serial = assert_current_control(&db);
    let initial_revision = int(&partition_row(&db), 0, "serving_policy_revision");
    alter_hnsw(&db, "M = 24, EF_CONSTRUCTION = 400");
    let pause = db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(
        &partition(),
        VectorMaintenancePreparationPhaseForTest::EncodeGraph,
    );
    let worker_db = Arc::clone(&db);
    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        done_tx.send(worker_db.run_maintenance_cycle()).unwrap();
    });
    if let Err(error) = pause.wait_until_reached_timeout(SAFETY_TIMEOUT) {
        pause.release();
        let _ = done_rx.recv_timeout(SAFETY_TIMEOUT);
        worker.join().unwrap();
        panic!("replacement did not reach graph preparation: {error}");
    }
    alter_hnsw(&db, "M = 16, EF_CONSTRUCTION = 200");
    let desired = db.vector_store_for_test().index_layout(&index()).unwrap();
    assert_eq!(
        (desired.hnsw_m, desired.hnsw_ef_construction),
        (Some(16), Some(200))
    );
    assert_eq!(desired.policy_revision, initial_revision as u64 + 2);
    pause.release();
    done_rx
        .recv_timeout(SAFETY_TIMEOUT)
        .expect("released maintenance returns")
        .expect("superseded revision is abandoned");
    worker.join().unwrap();
    let after_release = partition_row(&db);
    assert_eq!(
        int(&after_release, 0, "serving_policy_revision"),
        initial_revision
    );
    assert_eq!(
        db.__debug_vector_hnsw_build_serial_for_test(index()),
        Some(serial),
        "the superseded build cannot publish"
    );
    for _ in 0..3 {
        db.run_maintenance_cycle().unwrap();
    }
    let settled = partition_row(&db);
    assert_eq!(
        int(&settled, 0, "desired_policy_revision"),
        initial_revision + 2
    );
    assert_eq!(
        int(&settled, 0, "serving_policy_revision"),
        initial_revision
    );
    assert_eq!(
        value(&settled, 0, "maintenance_reason"),
        &Value::Text("none".into())
    );
    assert_eq!(
        db.__debug_vector_hnsw_build_serial_for_test(index()),
        Some(serial),
        "the compatible serving graph needs no replacement"
    );
    assert_eq!(indexed_query(&db).len(), 3);
}
