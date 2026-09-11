//! Delete-only durable journal progress for maintained vector routes.
//!
//! A delete does not add a mutable HNSW point, but it is durable route work.
//! Maintenance must therefore advance a covered generation and remove the
//! covered journal prefix even when no insert follows the prior base.

#![cfg(feature = "test-seams")]

use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use contextdb_vector::{VectorPartitionRef, store::VectorGraphGeneration};
use std::collections::{HashMap, HashSet};
use tempfile::TempDir;
use uuid::Uuid;

const MAX_MAINTENANCE_CYCLES: usize = 16;

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
    VectorIndexRef::new("delete_only_items", "embedding")
}

fn partition(scope: Uuid) -> VectorPartitionKey {
    VectorPartitionKey::from_values(&[Value::Uuid(scope)])
        .expect("one UUID partition component has one canonical key")
}

fn route(scope: Uuid) -> VectorPartitionRef {
    VectorPartitionRef::new(index(), partition(scope))
}

fn ids(result: &QueryResult) -> Vec<Uuid> {
    let id_column = result
        .columns
        .iter()
        .position(|column| column == "id" || column.rsplit('.').next() == Some("id"))
        .expect("vector query projects id");
    result
        .rows
        .iter()
        .map(|row| match row.get(id_column) {
            Some(Value::Uuid(id)) => *id,
            value => panic!("vector query returned a non-UUID id: {value:?}"),
        })
        .collect()
}

fn vector_search(db: &Database, scope: Uuid, mode: &str, limit: usize) -> Vec<Uuid> {
    ids(&db
        .execute(
            &format!(
                "SELECT id FROM delete_only_items WHERE scope_id = $scope \
                 ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT {limit}"
            ),
            &params([
                ("scope", Value::Uuid(scope)),
                ("query", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .unwrap_or_else(|error| panic!("{mode} vector query for partition {scope}: {error}")))
}

fn current_generation(db: &Database, scope: Uuid) -> VectorGraphGeneration {
    let status = db
        .vector_store_for_test()
        .partition_graph_generation_status(&route(scope))
        .unwrap_or_else(|| panic!("route {scope} has generation status"));
    status
        .change
        .or(status.dormant_change)
        .or(status.base)
        .or(status.dormant_base)
        .unwrap_or_else(|| panic!("route {scope} has a durable generation: {status:?}"))
}

fn graph_ready(db: &Database, scope: Uuid) -> bool {
    db.vector_store_for_test()
        .partition_info(&index(), &partition(scope))
        .is_some_and(|info| info.graph_available)
}

fn create_schema(db: &Database) {
    db.execute(
        "CREATE TABLE delete_only_items (\
         id UUID PRIMARY KEY, \
         scope_id UUID NOT NULL, \
         embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 8 SEARCH_MODE INDEXED\
         )",
        &empty(),
    )
    .expect("create the maintained partitioned vector table");
    db.execute(
        "CREATE TABLE ordinary_notes (id UUID PRIMARY KEY, body TEXT NOT NULL)",
        &empty(),
    )
    .expect("create the unrelated ordinary control table");
}

fn insert_row(db: &Database, id: Uuid, scope: Uuid, embedding: Vec<f32>) {
    db.execute(
        "INSERT INTO delete_only_items (id, scope_id, embedding) \
         VALUES ($id, $scope, $embedding)",
        &params([
            ("id", Value::Uuid(id)),
            ("scope", Value::Uuid(scope)),
            ("embedding", Value::Vector(embedding)),
        ]),
    )
    .expect("commit one deterministic vector row");
}

fn drive_maintenance_until_ready(db: &Database, scope: Uuid) {
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        if graph_ready(db, scope) {
            return;
        }
        db.run_maintenance_cycle()
            .expect("one finite caller-driven maintenance cycle returns");
    }
    panic!("maintenance did not publish the route within {MAX_MAINTENANCE_CYCLES} cycles");
}

fn drive_maintenance_until_covered_after(
    db: &Database,
    scope: Uuid,
    prior: VectorGraphGeneration,
) -> VectorGraphGeneration {
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        db.run_maintenance_cycle()
            .expect("one finite caller-driven maintenance cycle returns");
        let current = current_generation(db, scope);
        if current.generation_id > prior.generation_id
            && current.covered_tx > prior.covered_tx
            && current.covered_lsn > prior.covered_lsn
        {
            return current;
        }
    }
    panic!(
        "delete-only route work did not publish a later covered generation within \
         {MAX_MAINTENANCE_CYCLES} finite cycles"
    );
}

fn assert_covered_records_are_physically_absent(
    db: &Database,
    scope: Uuid,
    covered: VectorGraphGeneration,
    delete_record_lsns: &HashSet<contextdb_core::Lsn>,
    what: &str,
) {
    let journal = db.__debug_vector_partition_journal_file_for_test(&route(scope));
    assert_eq!(
        journal.truncated_through_lsn, covered.covered_lsn,
        "{what}: the physical journal reports the durable covered frontier"
    );
    assert!(
        journal
            .physical_record_lsns
            .iter()
            .all(|lsn| *lsn > covered.covered_lsn),
        "{what}: no journal record at or below the covered frontier remains physical: \
         {journal:?}"
    );
    assert!(
        delete_record_lsns
            .iter()
            .all(|lsn| !journal.physical_record_lsns.contains(lsn)),
        "{what}: every observed delete journal record was physically removed: {journal:?}"
    );
}

#[test]
fn delete_only_survivor_route_advances_its_durable_frontier_and_truncates_the_journal() {
    let root = TempDir::new().expect("temporary durable store directory");
    let path = root.path().join("delete-only-survivor.redb");
    let scope = Uuid::from_u128(0xD311);
    let deleted = Uuid::from_u128(0xD311_0001);
    let survivor = Uuid::from_u128(0xD311_0002);
    let other_survivor = Uuid::from_u128(0xD311_0003);
    let db = Database::open(&path).expect("open the durable survivor fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    create_schema(&db);
    insert_row(&db, deleted, scope, vec![1.0, 0.0, 0.0]);
    insert_row(&db, survivor, scope, vec![0.9, 0.1, 0.0]);
    insert_row(&db, other_survivor, scope, vec![0.0, 1.0, 0.0]);
    drive_maintenance_until_ready(&db, scope);

    let prior = current_generation(&db, scope);
    let journal_before_delete = db.__debug_vector_partition_journal_file_for_test(&route(scope));
    assert_eq!(
        journal_before_delete.truncated_through_lsn, prior.covered_lsn,
        "the ready durable route exposes its captured base frontier"
    );

    let deleted_rows = db
        .execute(
            "DELETE FROM delete_only_items WHERE id = $id",
            &params([("id", Value::Uuid(deleted))]),
        )
        .expect("commit one real delete after the captured base");
    assert_eq!(
        deleted_rows.rows_affected, 1,
        "the fixture deleted its real vector row"
    );
    assert_eq!(
        vector_search(&db, scope, "EXACT", 3),
        vec![survivor, other_survivor],
        "EXACT immediately excludes the deleted row and retains both survivors"
    );
    let journal_after_delete = db.__debug_vector_partition_journal_file_for_test(&route(scope));
    let before_lsns = journal_before_delete
        .physical_record_lsns
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let delete_record_lsns = journal_after_delete
        .physical_record_lsns
        .iter()
        .copied()
        .filter(|lsn| !before_lsns.contains(lsn))
        .collect::<HashSet<_>>();
    assert!(
        !delete_record_lsns.is_empty()
            && delete_record_lsns
                .iter()
                .all(|lsn| *lsn > prior.covered_lsn),
        "the delete appended durable route journal work beyond the captured base: \
         {journal_after_delete:?}"
    );

    let covered = drive_maintenance_until_covered_after(&db, scope, prior);
    assert_covered_records_are_physically_absent(
        &db,
        scope,
        covered,
        &delete_record_lsns,
        "the surviving route",
    );
    assert_eq!(
        vector_search(&db, scope, "INDEXED", 3),
        vec![survivor, other_survivor],
        "the current maintained route answers correctly after delete-only maintenance"
    );
    db.close()
        .expect("close the durable survivor fixture cleanly");

    let reopened = Database::open(&path).expect("clean reopen after delete-only maintenance");
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        vector_search(&reopened, scope, "EXACT", 3),
        vec![survivor, other_survivor],
        "clean reopen retains exactly the surviving rows"
    );
    assert_eq!(
        vector_search(&reopened, scope, "INDEXED", 3),
        vec![survivor, other_survivor],
        "clean reopen keeps the maintained survivor route correct"
    );
}

#[test]
fn delete_only_final_route_truncates_covered_history_before_snapshot_release_can_retire_it() {
    let root = TempDir::new().expect("temporary durable store directory");
    let path = root.path().join("delete-only-final.redb");
    let emptied_scope = Uuid::from_u128(0xD312);
    let control_scope = Uuid::from_u128(0xD313);
    let deleted = Uuid::from_u128(0xD312_0001);
    let control = Uuid::from_u128(0xD313_0001);
    let ordinary = Uuid::from_u128(0xD313_FFFF);
    let db = Database::open(&path).expect("open the durable final-delete fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    create_schema(&db);
    insert_row(&db, deleted, emptied_scope, vec![1.0, 0.0, 0.0]);
    insert_row(&db, control, control_scope, vec![1.0, 0.0, 0.0]);
    db.execute(
        "INSERT INTO ordinary_notes (id, body) VALUES ($id, $body)",
        &params([
            ("id", Value::Uuid(ordinary)),
            ("body", Value::Text("control".into())),
        ]),
    )
    .expect("commit the unrelated ordinary control row");
    drive_maintenance_until_ready(&db, emptied_scope);
    drive_maintenance_until_ready(&db, control_scope);

    let prior = current_generation(&db, emptied_scope);
    let journal_before_delete =
        db.__debug_vector_partition_journal_file_for_test(&route(emptied_scope));
    let pre_delete_snapshot = db.snapshot();
    let retirement_hold = db.pin_snapshot(pre_delete_snapshot);
    let deleted_rows = db
        .execute(
            "DELETE FROM delete_only_items WHERE id = $id",
            &params([("id", Value::Uuid(deleted))]),
        )
        .expect("commit the final real vector delete");
    assert_eq!(
        deleted_rows.rows_affected, 1,
        "the fixture deleted its final live row"
    );
    assert_eq!(
        vector_search(&db, emptied_scope, "EXACT", 1),
        Vec::<Uuid>::new(),
        "EXACT immediately observes the emptied partition"
    );
    let journal_after_delete =
        db.__debug_vector_partition_journal_file_for_test(&route(emptied_scope));
    let before_lsns = journal_before_delete
        .physical_record_lsns
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let delete_record_lsns = journal_after_delete
        .physical_record_lsns
        .iter()
        .copied()
        .filter(|lsn| !before_lsns.contains(lsn))
        .collect::<HashSet<_>>();
    assert!(
        !delete_record_lsns.is_empty()
            && delete_record_lsns
                .iter()
                .all(|lsn| *lsn > prior.covered_lsn),
        "the final delete appended durable route journal work beyond the ready base: \
         {journal_after_delete:?}"
    );

    let covered = drive_maintenance_until_covered_after(&db, emptied_scope, prior);
    assert_covered_records_are_physically_absent(
        &db,
        emptied_scope,
        covered,
        &delete_record_lsns,
        "the snapshot-held final route",
    );
    assert_eq!(
        vector_search(&db, control_scope, "INDEXED", 1),
        vec![control],
        "the unrelated maintained partition remains indexed while the empty route is cleaned"
    );
    drop(retirement_hold);
    db.run_maintenance_cycle()
        .expect("a post-release caller-driven maintenance cycle returns");
    db.close().expect("close after the empty route may retire");

    let reopened = Database::open(&path).expect("clean reopen after final delete");
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        vector_search(&reopened, emptied_scope, "EXACT", 1),
        Vec::<Uuid>::new(),
        "clean reopen remains exactly empty after the route can retire"
    );
    assert_eq!(
        vector_search(&reopened, control_scope, "INDEXED", 1),
        vec![control],
        "clean reopen keeps the unrelated maintained partition intact"
    );
    let ordinary_rows = reopened
        .execute(
            "SELECT id, body FROM ordinary_notes WHERE id = $id",
            &params([("id", Value::Uuid(ordinary))]),
        )
        .expect("read the unrelated ordinary control row after reopen");
    assert_eq!(
        ordinary_rows.rows.len(),
        1,
        "ordinary control remains intact after final deletion"
    );
}

/// With no pinned historical reader, maintenance is allowed to retire the
/// empty in-memory route before choosing work. Its durable journal still has
/// to be truncated first; the catalog-backed journal seam remains observable
/// after that in-memory retirement.
#[test]
fn delete_only_unpinned_final_route_physically_cleans_its_journal_before_retirement() {
    let root = TempDir::new().expect("temporary durable store directory");
    let path = root.path().join("delete-only-unpinned-final.redb");
    let emptied_scope = Uuid::from_u128(0xD314);
    let control_scope = Uuid::from_u128(0xD315);
    let deleted = Uuid::from_u128(0xD314_0001);
    let control = Uuid::from_u128(0xD315_0001);
    let ordinary = Uuid::from_u128(0xD315_FFFF);
    let db = Database::open(&path).expect("open the unpinned final-delete fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    create_schema(&db);
    insert_row(&db, deleted, emptied_scope, vec![1.0, 0.0, 0.0]);
    insert_row(&db, control, control_scope, vec![1.0, 0.0, 0.0]);
    db.execute(
        "INSERT INTO ordinary_notes (id, body) VALUES ($id, $body)",
        &params([
            ("id", Value::Uuid(ordinary)),
            ("body", Value::Text("control".into())),
        ]),
    )
    .expect("commit the unrelated ordinary control row");
    drive_maintenance_until_ready(&db, emptied_scope);
    drive_maintenance_until_ready(&db, control_scope);

    let prior = current_generation(&db, emptied_scope);
    let journal_before_delete =
        db.__debug_vector_partition_journal_file_for_test(&route(emptied_scope));
    let deleted_rows = db
        .execute(
            "DELETE FROM delete_only_items WHERE id = $id",
            &params([("id", Value::Uuid(deleted))]),
        )
        .expect("commit the unpinned final vector delete");
    assert_eq!(
        deleted_rows.rows_affected, 1,
        "the fixture deleted its final live row without pinning a snapshot"
    );
    assert_eq!(
        vector_search(&db, emptied_scope, "EXACT", 1),
        Vec::<Uuid>::new(),
        "EXACT immediately observes the unpinned emptied partition"
    );
    let journal_after_delete =
        db.__debug_vector_partition_journal_file_for_test(&route(emptied_scope));
    let before_lsns = journal_before_delete
        .physical_record_lsns
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let delete_record_lsns = journal_after_delete
        .physical_record_lsns
        .iter()
        .copied()
        .filter(|lsn| !before_lsns.contains(lsn))
        .collect::<HashSet<_>>();
    assert!(
        !delete_record_lsns.is_empty()
            && delete_record_lsns
                .iter()
                .all(|lsn| *lsn > prior.covered_lsn),
        "the unpinned final delete appended durable route journal work beyond \
         the ready base: {journal_after_delete:?}"
    );

    for _ in 0..MAX_MAINTENANCE_CYCLES {
        db.run_maintenance_cycle()
            .expect("one finite unpinned caller-driven maintenance cycle returns");
    }
    let journal_after_maintenance =
        db.__debug_vector_partition_journal_file_for_test(&route(emptied_scope));
    assert!(
        delete_record_lsns
            .iter()
            .all(|lsn| !journal_after_maintenance.physical_record_lsns.contains(lsn)),
        "the exact final-delete journal LSNs are physically absent after unpinned \
         maintenance: {journal_after_maintenance:?}"
    );
    assert!(
        journal_after_maintenance
            .physical_record_lsns
            .iter()
            .all(|lsn| *lsn > journal_after_maintenance.truncated_through_lsn),
        "the durable journal has no physical record at or below its reported frontier: \
         {journal_after_maintenance:?}"
    );
    assert_eq!(
        vector_search(&db, control_scope, "INDEXED", 1),
        vec![control],
        "the unrelated maintained partition remains indexed while the unpinned route retires"
    );
    db.close()
        .expect("close after unpinned final-delete maintenance");

    let reopened = Database::open(&path).expect("clean reopen after unpinned final delete");
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        vector_search(&reopened, emptied_scope, "EXACT", 1),
        Vec::<Uuid>::new(),
        "clean reopen remains exactly empty after unpinned route retirement"
    );
    assert_eq!(
        vector_search(&reopened, control_scope, "INDEXED", 1),
        vec![control],
        "clean reopen keeps the unrelated maintained partition intact"
    );
    let ordinary_rows = reopened
        .execute(
            "SELECT id, body FROM ordinary_notes WHERE id = $id",
            &params([("id", Value::Uuid(ordinary))]),
        )
        .expect("read the unrelated ordinary control row after unpinned reopen");
    assert_eq!(
        ordinary_rows.rows.len(),
        1,
        "ordinary control remains intact after unpinned final deletion"
    );
}

/// A maintained route emptied by ordinary deletes keeps an uncovered final
/// tombstone. After reopen, maintenance compacts that route into a durable
/// generation with no points; installing and loading it must yield an empty
/// route that keeps serving, not a panic in the maintenance cycle.
#[test]
fn a_route_emptied_by_deletes_is_maintained_after_reopen_and_accepts_new_rows() {
    let root = TempDir::new().expect("temporary durable store directory");
    let path = root.path().join("delete-only-emptied-reopen.redb");
    let emptied_scope = Uuid::from_u128(0xD316);
    let first = Uuid::from_u128(0xD316_0001);
    let last = Uuid::from_u128(0xD316_0002);
    let fresh = Uuid::from_u128(0xD316_0003);
    let db = Database::open(&path).expect("open the emptied-route fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    create_schema(&db);
    insert_row(&db, first, emptied_scope, vec![1.0, 0.0, 0.0]);
    insert_row(&db, last, emptied_scope, vec![0.9, 0.1, 0.0]);
    drive_maintenance_until_ready(&db, emptied_scope);

    let prior = current_generation(&db, emptied_scope);
    db.execute(
        "DELETE FROM delete_only_items WHERE id = $id",
        &params([("id", Value::Uuid(first))]),
    )
    .expect("delete all but one row of the maintained route");
    drive_maintenance_until_covered_after(&db, emptied_scope, prior);
    db.execute(
        "DELETE FROM delete_only_items WHERE id = $id",
        &params([("id", Value::Uuid(last))]),
    )
    .expect("delete the route's final row");
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        db.run_maintenance_cycle()
            .expect("maintenance of the emptied route returns before close");
    }
    db.close().expect("close the emptied route");

    let reopened = Database::open(&path).expect("reopen the emptied route");
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        let report = reopened
            .run_maintenance_cycle()
            .expect("maintenance of the reopened emptied route returns");
        assert_eq!(
            report.vector.first_failure, None,
            "maintaining an emptied route is not a failure"
        );
    }
    for mode in ["EXACT", "INDEXED"] {
        assert_eq!(
            vector_search(&reopened, emptied_scope, mode, 2),
            Vec::<Uuid>::new(),
            "{mode} serves no rows from the emptied route after reopen"
        );
    }

    insert_row(&reopened, fresh, emptied_scope, vec![1.0, 0.0, 0.0]);
    drive_maintenance_until_ready(&reopened, emptied_scope);
    for mode in ["EXACT", "INDEXED"] {
        assert_eq!(
            vector_search(&reopened, emptied_scope, mode, 2),
            vec![fresh],
            "{mode} serves a row written into the emptied route"
        );
    }
    reopened.close().expect("close the refilled route");
    let reopened = Database::open(&path).expect("reopen the refilled route");
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        vector_search(&reopened, emptied_scope, "INDEXED", 2),
        vec![fresh],
        "the refilled route survives reopen"
    );
}
