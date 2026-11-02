//! Endpoint retention retires graph bindings without deleting their SQL owners.
use super::*;
use crate::{Database, MaintenancePolicy, ReadSession};
use contextdb_core::read_contract::ReadRoute;
use contextdb_tx::OwnedAdjEntry;
use std::collections::{BTreeSet, HashMap};
use std::path::Path;

const CHILD_PATH: &str = "CLOSE1_RETIREMENT_GUARD_PATH";
const CHILD_CASE: &str = "CLOSE1_RETIREMENT_GUARD_CASE";
const CHILD_TEST: &str = "CLOSE1_RETIREMENT_GUARD_TEST";
const EXPIRED: u128 = 80_001;
const SURVIVOR: u128 = 80_002;
const ENDPOINT: u128 = 80_003;

fn bindings() -> HashMap<String, Value> {
    HashMap::from([(
        "anchor".into(),
        Value::Uuid(uuid::Uuid::from_u128(SURVIVOR)),
    )])
}

fn query(reverse: bool) -> &'static str {
    if reverse {
        "SELECT t FROM GRAPH_TABLE(edges MATCH (a)<-[:LINKS]-(b) WHERE a.id = $anchor COLUMNS (b.id AS t)) ORDER BY t"
    } else {
        "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $anchor COLUMNS (b.id AS t)) ORDER BY t"
    }
}

fn writer(path: &Path) -> Database {
    let db = Database::open(path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db
}

fn seed(path: &Path, reverse: bool, due: bool) -> Database {
    let db = writer(path);
    db.execute("CREATE TABLE retained (id UUID PRIMARY KEY, expires_at TIMESTAMP EXPIRES, embedding VECTOR(2)) RETAIN 1 HOURS", &HashMap::new()).unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    for id in [EXPIRED, SURVIVOR] {
        db.execute(
            "INSERT INTO retained VALUES ($id, $expires, '[1,0]')",
            &HashMap::from([
                ("id".into(), Value::Uuid(uuid::Uuid::from_u128(id))),
                (
                    "expires".into(),
                    Value::Timestamp(if due && id == EXPIRED { -1 } else { i64::MAX }),
                ),
            ]),
        )
        .unwrap();
    }
    for (id, other) in [(90_000, EXPIRED), (90_001, ENDPOINT)] {
        let (source, target) = if reverse {
            (other, SURVIVOR)
        } else {
            (SURVIVOR, other)
        };
        db.execute(
            "INSERT INTO edges VALUES ($id, $source, $target, 'LINKS')",
            &HashMap::from([
                ("id".into(), Value::Uuid(uuid::Uuid::from_u128(id))),
                ("source".into(), Value::Uuid(uuid::Uuid::from_u128(source))),
                ("target".into(), Value::Uuid(uuid::Uuid::from_u128(target))),
            ]),
        )
        .unwrap();
    }
    assert_eq!(
        db.execute(query(reverse), &bindings()).unwrap().rows(),
        both()
    );
    assert_eq!(
        db.execute("SELECT id FROM edges ORDER BY id", &HashMap::new())
            .unwrap()
            .rows(),
        sql_rows()
    );
    db
}

fn both() -> Vec<Vec<Value>> {
    [EXPIRED, ENDPOINT]
        .into_iter()
        .map(|id| vec![Value::Uuid(uuid::Uuid::from_u128(id))])
        .collect()
}

fn healthy() -> Vec<Vec<Value>> {
    vec![vec![Value::Uuid(uuid::Uuid::from_u128(ENDPOINT))]]
}

fn sql_rows() -> Vec<Vec<Value>> {
    [90_000, 90_001]
        .into_iter()
        .map(|id| vec![Value::Uuid(uuid::Uuid::from_u128(id))])
        .collect()
}

fn close(db: Database) {
    db.close().unwrap();
    drop(db);
}

fn assert_public(path: &Path, reverse: bool) {
    {
        let db = writer(path);
        assert_eq!(
            db.execute(query(reverse), &bindings()).unwrap().rows(),
            healthy(),
            "Legacy traversal must omit the expired endpoint's binding"
        );
        assert_eq!(
            db.execute("SELECT id FROM edges ORDER BY id", &HashMap::new())
                .unwrap()
                .rows(),
            sql_rows(),
            "retirement never cascades into SQL edge rows"
        );
        let owner = ReadSession::open_with_clock_for_test(
            path,
            Default::default(),
            Arc::new(crate::local_transport::ManualDeadlineClock::at(0)),
        )
        .unwrap();
        assert_eq!(owner.route(), ReadRoute::Owner);
        assert_eq!(
            owner.execute(query(reverse), &bindings()).unwrap().rows(),
            healthy()
        );
        assert_eq!(
            owner
                .execute("SELECT id FROM edges ORDER BY id", &HashMap::new())
                .unwrap()
                .rows(),
            sql_rows()
        );
        // A later statement must not reinterpret a retired witness as corruption.
        assert_eq!(
            owner.execute(query(reverse), &bindings()).unwrap().rows(),
            healthy()
        );
        drop(owner);
        close(db);
    }
    let file = ReadSession::open_with_clock_for_test(
        path,
        Default::default(),
        Arc::new(crate::local_transport::ManualDeadlineClock::at(0)),
    )
    .unwrap();
    assert_eq!(file.route(), ReadRoute::File);
    assert_eq!(
        file.execute(query(reverse), &bindings()).unwrap().rows(),
        healthy()
    );
    assert_eq!(
        file.execute("SELECT id FROM edges ORDER BY id", &HashMap::new())
            .unwrap()
            .rows(),
        sql_rows()
    );
    assert_eq!(
        file.execute(query(reverse), &bindings()).unwrap().rows(),
        healthy()
    );
}

fn physical(path: &Path, count: usize, deferred: bool) {
    let persistence = RedbPersistence::open(path).unwrap();
    let image = crate::read_image_memory::ImageMemory::new(None).unwrap();
    let _scope = image.enter();
    persistence
        .with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            for definition in [row_graph::FORWARD, row_graph::REVERSE] {
                let table = read
                    .open_table(definition)
                    .map_err(RedbPersistence::storage_error)?;
                assert_eq!(
                    table.len().unwrap(),
                    count as u64,
                    "both physical directions retire together"
                );
            }
            let meta = RedbPersistence::load_table_meta_in_read_txn(&read, "edges")?.unwrap();
            RedbPersistence::relational_indexes_ready_in_read(
                &read,
                &HashMap::from([("edges".into(), meta)]),
            )
            .expect("retirement preserves authenticated index readiness");
            Ok(())
        })
        .unwrap();
    let pending = persistence
        .load_config_value::<BTreeSet<NodeId>>(RETENTION_DEFERRED_EDGE_NODES)
        .unwrap();
    assert_eq!(
        pending,
        deferred.then(|| BTreeSet::from([uuid::Uuid::from_u128(EXPIRED)])),
        "reader-deferred work keeps its exact durable retry identity"
    );
    persistence.close();
}

#[test]
fn retention_omits_expired_bindings_in_both_routes_and_directions_without_sql_cascade() {
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    for reverse in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("retirement.db");
        let db = seed(&path, reverse, true);
        db.run_pruning_cycle_checked().unwrap();
        close(db);
        assert_public(&path, reverse);
        physical(&path, 1, false);
    }
}

#[test]
fn reader_held_binding_survives_until_release_then_retires_without_deleting_sql_rows() {
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    for reverse in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("held.db");
        let db = seed(&path, reverse, false);
        let snapshot = db.snapshot();
        let pin = db.pin_snapshot(snapshot).unwrap();
        db.execute(
            "UPDATE retained SET expires_at = $expires WHERE id = $id",
            &HashMap::from([
                ("expires".into(), Value::Timestamp(-1)),
                ("id".into(), Value::Uuid(uuid::Uuid::from_u128(EXPIRED))),
            ]),
        )
        .unwrap();
        db.run_pruning_cycle_checked().unwrap();
        assert_eq!(
            db.execute_at_snapshot(query(reverse), &bindings(), snapshot)
                .unwrap()
                .rows(),
            both(),
            "the held graph view keeps its original binding"
        );
        drop(pin);
        db.run_pruning_cycle_checked().unwrap();
        close(db);
        physical(&path, 1, false);
        assert_public(&path, reverse);
    }
}

#[test]
fn retired_bindings_stay_retired_through_snapshot_export_and_restore() {
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    for reverse in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.db");
        let exported = directory.path().join("snapshot.db");
        let db = seed(&path, reverse, true);
        db.run_pruning_cycle_checked().unwrap();
        db.export_snapshot(&exported)
            .expect("an authenticated retired row must be exportable");
        close(db);
        assert_public(&exported, reverse);
        physical(&exported, 1, false);
    }
}

#[test]
fn projection_and_index_rebuild_do_not_resurrect_retired_bindings() {
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    for reverse in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rebuild.db");
        let db = seed(&path, reverse, true);
        db.run_pruning_cycle_checked().unwrap();
        db.__rebuild_relational_indexes_from_durable_rows_for_test("edges")
            .unwrap();
        close(db);
        let persistence = RedbPersistence::open(&path).unwrap();
        persistence
            .with_db(|db| {
                let write = RedbPersistence::begin_history_write(db)?;
                RedbPersistence::rebuild_read_lineage_config_in_write(&write)?;
                RedbPersistence::apply_content_identity_mutations_in_write(
                    &write,
                    &[],
                    &mut persistence.proof_admission(),
                )?;
                RedbPersistence::write_writer_recovery_bounds_in_write(&write, None)?;
                write.commit().map_err(RedbPersistence::storage_error)
            })
            .unwrap();
        persistence.close();
        drop(persistence);
        assert_public(&path, reverse);
        physical(&path, 1, false);
    }
}

fn missing_binding_case(export: bool) {
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    for reverse in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("damaged.db");
        close(seed(&path, false, false));
        let persistence = RedbPersistence::open(&path).unwrap();
        let selected = persistence
            .with_db(|db| {
                let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
                let table = read
                    .open_table(row_graph::FORWARD)
                    .map_err(RedbPersistence::storage_error)?;
                for item in table.iter().map_err(RedbPersistence::storage_error)? {
                    let (_, value) = item.map_err(RedbPersistence::storage_error)?;
                    let (owned, _): (OwnedAdjEntry, usize) = bincode::serde::decode_from_slice(
                        value.value(),
                        bincode::config::standard(),
                    )
                    .unwrap();
                    if owned.entry.target == uuid::Uuid::from_u128(EXPIRED) {
                        return Ok(owned.owner.row_id);
                    }
                }
                panic!("healthy control must have the selected exact witness");
            })
            .unwrap();
        persistence.close();
        drop(persistence);
        // This seam recomputes producer roots, so this is genuine unexplained
        // reciprocal/owning-row damage rather than an unrelated stale checksum.
        Database::remove_row_graph_witness_for_test(&path, "edges", selected, reverse).unwrap();
        let db = writer(&path);
        let error = if export {
            db.export_snapshot(&directory.path().join("must-refuse.db"))
                .unwrap_err()
        } else {
            db.execute(
                "UPDATE edges SET edge_type = 'UPDATED' WHERE id = $id",
                &HashMap::from([("id".into(), Value::Uuid(uuid::Uuid::from_u128(90_000)))]),
            )
            .unwrap_err()
        };
        assert!(
            matches!(error, Error::StoreCorrupted { .. } | Error::SyncError(_)),
            "live missing witness must remain a typed refusal: {error:?}"
        );
        close(db);
    }
}

fn crash_case(case: &str, test: &str) {
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    if let Ok(path) = std::env::var(CHILD_PATH) {
        assert_eq!(std::env::var(CHILD_CASE).unwrap(), case);
        let db = writer(Path::new(&path));
        let snapshot = db.snapshot();
        let pin = (case == "deferred").then(|| db.pin_snapshot(snapshot).unwrap());
        if pin.is_some() {
            db.execute(
                "UPDATE retained SET expires_at = $expires WHERE id = $id",
                &HashMap::from([
                    ("expires".into(), Value::Timestamp(-1)),
                    ("id".into(), Value::Uuid(uuid::Uuid::from_u128(EXPIRED))),
                ]),
            )
            .unwrap();
            assert_eq!(
                db.execute_at_snapshot(query(false), &bindings(), snapshot)
                    .unwrap()
                    .rows(),
                both()
            );
        }
        db.__arm_retention_reclaim_crash_for_test(case != "before");
        db.run_pruning_cycle_checked().unwrap();
        panic!("the real reclaim crash boundary must abort this child");
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("crash.db");
    close(seed(&path, false, case != "deferred"));
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture"])
        .env(CHILD_PATH, &path)
        .env(CHILD_CASE, case)
        .env(CHILD_TEST, test)
        .output()
        .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            output.status.signal(),
            Some(libc::SIGABRT),
            "only the armed real crash hook may stop the child: {output:?}"
        );
    }
    physical(
        &path,
        if case == "after" { 1 } else { 2 },
        case == "deferred",
    );
    let db = writer(&path);
    assert_eq!(
        db.execute("SELECT id FROM retained", &HashMap::new())
            .unwrap()
            .rows(),
        vec![vec![Value::Uuid(uuid::Uuid::from_u128(SURVIVOR))]]
    );
    db.run_pruning_cycle_checked().unwrap();
    close(db);
    physical(&path, 1, false);
    assert_public(&path, false);
}

#[test]
fn crash_before_retirement_reopens_and_prunes_the_same_sql_rows() {
    crash_case(
        "before",
        "persistence::retention_retired_binding_guard_tests::crash_before_retirement_reopens_and_prunes_the_same_sql_rows",
    );
}

#[test]
fn crash_after_retirement_reopens_and_prunes_the_same_sql_rows() {
    crash_case(
        "after",
        "persistence::retention_retired_binding_guard_tests::crash_after_retirement_reopens_and_prunes_the_same_sql_rows",
    );
}

#[test]
fn crash_with_a_reader_preserves_durable_retry_identity_until_reopen_prunes() {
    crash_case(
        "deferred",
        "persistence::retention_retired_binding_guard_tests::crash_with_a_reader_preserves_durable_retry_identity_until_reopen_prunes",
    );
}

#[test]
fn unexplained_missing_live_binding_still_refuses_with_a_typed_error() {
    missing_binding_case(false);
}

#[test]
fn snapshot_export_refuses_an_unexplained_missing_live_binding() {
    missing_binding_case(true);
}

fn route_case(reverse: bool, owner: bool) {
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("route.db");
    let db = seed(&path, reverse, true);
    db.run_pruning_cycle_checked().unwrap();
    close(db);
    let live = owner.then(|| writer(&path));
    let reader = ReadSession::open(&path).unwrap();
    assert_eq!(
        reader.route(),
        if owner {
            ReadRoute::Owner
        } else {
            ReadRoute::File
        }
    );
    assert_eq!(
        reader
            .execute("SELECT id FROM edges ORDER BY id", &HashMap::new())
            .unwrap()
            .rows(),
        sql_rows(),
        "SQL rows remain after endpoint expiry"
    );
    assert_eq!(
        reader.execute(query(reverse), &bindings()).unwrap().rows(),
        healthy(),
        "this exact public route and direction must omit the expired endpoint"
    );
    assert_eq!(
        reader.execute(query(reverse), &bindings()).unwrap().rows(),
        healthy(),
        "a later statement accepts the authenticated retirement"
    );
    drop(reader);
    if let Some(db) = live {
        close(db);
    }
    physical(&path, 1, false);
}

#[test]
fn retention_file_forward_preserves_sql_rows_and_omits_expired_binding() {
    route_case(false, false);
}

#[test]
fn retention_file_reverse_preserves_sql_rows_and_omits_expired_binding() {
    route_case(true, false);
}

#[test]
fn retention_owner_forward_preserves_sql_rows_and_omits_expired_binding() {
    route_case(false, true);
}

#[test]
fn retention_owner_reverse_preserves_sql_rows_and_omits_expired_binding() {
    route_case(true, true);
}
