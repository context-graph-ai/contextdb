//! Compaction is invisible to vector search and maintenance. Native role and
//! qualification tables are proven by physical root fingerprints, and moving a
//! table's pages changes that fingerprint, so a sweep must carry the proof it
//! moves in the same commit, and must never mint a proof over damaged bytes.

use super::support::{
    calibration_receipt, empty, open_caller_driven, open_reader, params, plant_qualification,
    unit_vector,
};
use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::vector_ivf_observations::{
    NativeIvfFamilyRootsForTest, NativeIvfRootRelocationsForTest, NativeIvfRootRoleForTest,
    native_ivf_root_relocations_for_test,
};
use contextdb_engine::{Database, DatabaseOpenOptions};
use std::path::Path;

const D: usize = 32;

/// The caller drives every cycle, so what compacted and when is deterministic.
fn open(path: &Path) -> Database {
    open_caller_driven(path)
}
fn index() -> VectorIndexRef {
    VectorIndexRef::new("items", "embedding")
}
fn tuple() -> VectorPartitionKey {
    VectorPartitionKey::from_values(&[]).unwrap()
}
/// 500 x 32, unpartitioned, 32 maintenance cycles: the shape lane P's fixture failed on.
fn fixture(path: &Path) -> Database {
    let db = open(path);
    db.execute(
        &format!(
            "CREATE TABLE items (id INT PRIMARY KEY, scope_id INT NOT NULL, filter_key INT NOT NULL, body TEXT, embedding VECTOR({D})) HISTORY ALL SYNC OFF"
        ),
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX item_filter ON items (scope_id, filter_key)",
        &empty(),
    )
    .unwrap();
    for start in (0..500i64).step_by(64) {
        let tx = db.begin_or_panic();
        for id in start..500.min(start + 64) {
            db.execute_in_tx(
                tx,
                "INSERT INTO items VALUES ($id,$scope,$filter,$body,$v)",
                &params([
                    ("id", Value::Int64(id)),
                    ("scope", Value::Int64(0)),
                    ("filter", Value::Int64(id % 10)),
                    ("body", Value::Text(format!("Observation {id}"))),
                    ("v", Value::Vector(unit_vector(id as u64, D))),
                ]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
    }
    for _ in 0..32 {
        db.run_maintenance_cycle().unwrap();
    }
    db
}
fn ids(rows: &[Vec<Value>]) -> Vec<i64> {
    rows.iter()
        .map(|row| match row[0] {
            Value::Int64(id) => id,
            ref other => panic!("id column holds {other:?}"),
        })
        .collect()
}
fn search(db: &Database, mode: &str) -> Vec<i64> {
    let bound = params([("v", Value::Vector(unit_vector(5, D)))]);
    let sql = format!(
        "SELECT id FROM items WHERE scope_id = 0 ORDER BY embedding <=> $v USE VECTOR {mode} LIMIT 10"
    );
    ids(db
        .execute(&sql, &bound)
        .unwrap_or_else(|error| panic!("{mode} search: {error:?}"))
        .rows())
}

#[test]
fn compact_now_keeps_every_route_and_moves_native_roots() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let db = fixture(&path);
    let answers: Vec<_> = ["EXACT", "AUTO", "INDEXED"]
        .map(|mode| (mode, search(&db, mode)))
        .into_iter()
        .collect();
    let before = db
        .native_ivf_scope_roots_at_snapshot_for_test(&index(), &tuple(), db.snapshot())
        .unwrap();

    let report = db.compact_now().unwrap();
    assert!(
        report.file_shrank,
        "the fixture must really be compacted: {report:?}"
    );

    let after = db
        .native_ivf_scope_roots_at_snapshot_for_test(&index(), &tuple(), db.snapshot())
        .unwrap();
    assert_ne!(
        before.actual_roots, after.actual_roots,
        "compaction moved no native role, so this test proves nothing"
    );
    assert_eq!(
        after.expected_roots, after.actual_roots,
        "each stored root follows the table it proves"
    );
    for (mode, answer) in &answers {
        assert_eq!(
            &search(&db, mode),
            answer,
            "{mode} on the compacting handle"
        );
    }
    assert_owners_match(&db, "after compact_now");
    let seen = native_ivf_root_relocations_for_test();
    assert_eq!(
        seen.owner_entries_read, seen.scopes_read,
        "one owner, one scope, per moved table"
    );
    db.run_maintenance_cycle().unwrap();
    db.close().unwrap();

    let db = open(&path);
    for (mode, answer) in &answers {
        assert_eq!(&search(&db, mode), answer, "{mode} after a writable reopen");
    }
    for _ in 0..3 {
        db.run_maintenance_cycle().unwrap();
    }
    db.close().unwrap();

    let reader = open_reader(&path);
    let bound = params([("v", Value::Vector(unit_vector(5, D)))]);
    for (mode, answer) in &answers {
        let sql = format!(
            "SELECT id FROM items WHERE scope_id = 0 ORDER BY embedding <=> $v USE VECTOR {mode} LIMIT 10"
        );
        let result = reader
            .execute(&sql, &bound)
            .unwrap_or_else(|error| panic!("{mode} read-only: {error:?}"));
        assert_eq!(&ids(result.rows()), answer, "{mode} on the read-only route");
    }
}

/// Vectors with cluster structure, as embeddings have, so a cheaper setting can qualify.
fn clustered(id: u64) -> Vec<f32> {
    let centre = unit_vector(1_000 + id % 8, D);
    let noise = unit_vector(5_000_000 + id, D);
    centre
        .iter()
        .zip(&noise)
        .map(|(c, n)| c + 0.2 * n)
        .collect()
}

/// One open per file, each drained, as a host that opens briefly does: automatic
/// compaction reaches the permanent generation-zero tables during this journey.
#[test]
fn automatic_compaction_keeps_a_per_file_import_running() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let db = open(&path);
    db.execute(
        &format!(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR({D}) SEARCH_MODE AUTO) HISTORY CURRENT ONLY SYNC OFF"
        ),
        &empty(),
    )
    .unwrap();
    db.close().unwrap();
    let mut next = 0i64;
    let mut compactions = 0;
    for rows in [75i64, 88, 155, 588, 324] {
        let db = open(&path);
        let tx = db.begin_or_panic();
        for id in next..next + rows {
            db.execute_in_tx(
                tx,
                "INSERT INTO items (id, embedding) VALUES ($id, $embedding)",
                &params([
                    ("id", Value::Int64(id)),
                    ("embedding", Value::Vector(clustered(id as u64))),
                ]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
        next += rows;
        let mut settled = 0;
        for _ in 0..600 {
            let report = db.run_maintenance_cycle().unwrap();
            compactions += usize::from(report.compaction.ran);
            settled = if report.vector.remaining_partitions == 0 {
                settled + 1
            } else {
                0
            };
            if settled == 5 {
                break;
            }
        }
        assert_eq!(settled, 5, "file of {rows} rows drains");
        db.close().unwrap();
    }
    assert!(compactions > 0, "the journey must really compact");
    let moved = native_ivf_root_relocations_for_test();
    assert!(
        moved.refreshed_kinds[14] + moved.refreshed_kinds[15] > 0,
        "automatic compaction reached the permanent generation-zero tables: {moved:?}"
    );
    let db = open(&path);
    let bound = params([("q", Value::Vector(clustered(3)))]);
    let result = db
        .execute(
            "SELECT id FROM items ORDER BY embedding <=> $q USE VECTOR AUTO LIMIT 10",
            &bound,
        )
        .unwrap();
    let trace = result.trace().vector_search.clone().unwrap();
    assert_eq!(result.rows().len(), 10);
    assert_eq!(
        format!("{:?}", trace.resolved_mode),
        "Indexed",
        "a drained clustered import answers from its index"
    );
}

fn all_families(db: &Database) -> Vec<NativeIvfFamilyRootsForTest> {
    db.native_ivf_family_roots_for_test(&index(), &tuple())
        .unwrap()
}
fn assert_every_proof_holds(families: &[NativeIvfFamilyRootsForTest], why: &str) {
    for family in families {
        for tag in 0..16 {
            if family.present & (1 << tag) != 0 {
                assert_eq!(
                    family.expected_roots[tag],
                    family.actual_roots[tag],
                    "{why}: storage {} tag {}",
                    family.storage,
                    tag + 1
                );
            }
        }
        assert_eq!(
            family.expected_qualification, family.actual_qualification,
            "{why}: storage {} qualification",
            family.storage
        );
    }
}
fn assert_owners_match(db: &Database, why: &str) {
    let mut owned = db.native_ivf_owned_storage_for_test().unwrap();
    let mut live: Vec<u64> = all_families(db)
        .iter()
        .map(|family| family.storage)
        .collect();
    owned.sort_unstable();
    live.sort_unstable();
    assert_eq!(owned, live, "{why}: one owner entry per live storage id");
}
fn compact_and_count(db: &Database) -> NativeIvfRootRelocationsForTest {
    let before = native_ivf_root_relocations_for_test();
    db.compact_now().unwrap();
    let after = native_ivf_root_relocations_for_test();
    let mut delta = NativeIvfRootRelocationsForTest {
        refreshed_qualification: after.refreshed_qualification - before.refreshed_qualification,
        refreshed_retired: after.refreshed_retired - before.refreshed_retired,
        left_stale: after.left_stale - before.left_stale,
        owner_entries_read: after.owner_entries_read - before.owner_entries_read,
        scopes_read: after.scopes_read - before.scopes_read,
        retired_records_read: after.retired_records_read - before.retired_records_read,
        ..Default::default()
    };
    for tag in 0..16 {
        delta.refreshed_kinds[tag] = after.refreshed_kinds[tag] - before.refreshed_kinds[tag];
    }
    delta
}
fn import_clustered(db: &Database, ids: std::ops::Range<i64>) {
    let tx = db.begin_or_panic();
    for id in ids {
        db.execute_in_tx(
            tx,
            "INSERT INTO items (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                ("embedding", Value::Vector(clustered(id as u64))),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
}
fn drain(db: &Database) {
    for _ in 0..600 {
        if db
            .run_maintenance_cycle()
            .unwrap()
            .vector
            .remaining_partitions
            == 0
        {
            return;
        }
    }
    panic!("maintenance did not drain");
}
fn clustered_store(path: &Path, rows: i64) -> Database {
    let db = open(path);
    db.execute(
        &format!(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR({D}) SEARCH_MODE AUTO) HISTORY CURRENT ONLY SYNC OFF"
        ),
        &empty(),
    )
    .unwrap();
    import_clustered(&db, 0..rows);
    drain(&db);
    db
}

#[test]
fn compaction_carries_the_qualification_proof_and_its_receipt_stays() {
    let dir = tempfile::tempdir().unwrap();
    let db = clustered_store(&dir.path().join("store.db"), 906);
    let receipt = calibration_receipt(&db, "items", "embedding", &[]);
    assert!(receipt.is_some(), "the fixture must hold a real receipt");
    let serving = |db: &Database| {
        *all_families(db)
            .iter()
            .filter(|family| !family.retired && family.expected_qualification.is_some())
            .max_by_key(|family| family.generation)
            .expect("a family with a qualification table")
    };
    let before = serving(&db);
    let bound = params([("q", Value::Vector(clustered(3)))]);
    let sql = "SELECT id FROM items ORDER BY embedding <=> $q USE VECTOR AUTO LIMIT 10";
    let answer = ids(db.execute(sql, &bound).unwrap().rows());

    let seen = compact_and_count(&db);

    assert!(
        seen.refreshed_qualification > 0,
        "the qualification table really moved: {seen:?}"
    );
    let after = serving(&db);
    assert_eq!(
        before.generation, after.generation,
        "no newer receipt can hide a missed update"
    );
    assert_ne!(before.actual_qualification, after.actual_qualification);
    assert_every_proof_holds(&all_families(&db), "after compaction");
    assert_eq!(
        format!("{:?}", calibration_receipt(&db, "items", "embedding", &[])),
        format!("{receipt:?}"),
        "the same receipt, read through the moved table"
    );
    let result = db.execute(sql, &bound).unwrap();
    assert_eq!(ids(result.rows()), answer);
    assert_eq!(
        format!(
            "{:?}",
            result.trace().vector_search.clone().unwrap().resolved_mode
        ),
        "Indexed"
    );
}

#[test]
fn a_snapshot_held_across_compaction_keeps_its_answer_and_its_proof() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("store.db"));
    let held = db.snapshot();
    let _pin = db.pin_snapshot(held).unwrap();
    let bound = params([("v", Value::Vector(unit_vector(5, D)))]);
    let sql = "SELECT id FROM items WHERE scope_id = 0 ORDER BY embedding <=> $v USE VECTOR INDEXED LIMIT 10";
    let answer = ids(db.execute_at_snapshot(sql, &bound, held).unwrap().rows());
    let held_roots = db
        .native_ivf_family_roots_at_snapshot_for_test(&index(), &tuple(), held)
        .unwrap();
    assert_every_proof_holds(&held_roots, "the held snapshot, before");
    db.execute(
        "INSERT INTO items VALUES ($id,$scope,$filter,$body,$v)",
        &params([
            ("id", Value::Int64(9_000)),
            ("scope", Value::Int64(0)),
            ("filter", Value::Int64(0)),
            (
                "body",
                Value::Text("the row the held snapshot must not see".into()),
            ),
            ("v", Value::Vector(unit_vector(5, D))),
        ]),
    )
    .unwrap();

    let seen = compact_and_count(&db);

    assert!(
        seen.refreshed_kinds.iter().sum::<u64>() + seen.refreshed_qualification > 0,
        "native pages really moved while the snapshot was held: {seen:?}"
    );
    assert_eq!(
        ids(db.execute_at_snapshot(sql, &bound, held).unwrap().rows()),
        answer
    );
    assert!(!answer.contains(&9_000));
    let still = db
        .native_ivf_family_roots_at_snapshot_for_test(&index(), &tuple(), held)
        .unwrap();
    assert_eq!(
        still, held_roots,
        "the held snapshot keeps its own stored and actual pair"
    );
    let current = all_families(&db);
    assert_ne!(current, held_roots, "a new reader sees the moved tables");
    assert_every_proof_holds(&current, "a new reader");
    assert!(
        search(&db, "EXACT").contains(&9_000),
        "a new reader sees the newer row"
    );
}

fn partitioned(path: &Path) -> Database {
    let db = open(path);
    db.execute(
        &format!(
            "CREATE TABLE items (id INT PRIMARY KEY, scope_id INT NOT NULL, embedding VECTOR({D}) PARTITION_KEY (scope_id)) HISTORY ALL SYNC OFF"
        ),
        &empty(),
    )
    .unwrap();
    let tx = db.begin_or_panic();
    for id in 0..600i64 {
        db.execute_in_tx(
            tx,
            "INSERT INTO items VALUES ($id,$scope,$v)",
            &params([
                ("id", Value::Int64(id)),
                ("scope", Value::Int64(id % 2)),
                ("v", Value::Vector(unit_vector(id as u64, D))),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    for _ in 0..80 {
        db.run_maintenance_cycle().unwrap();
    }
    db
}
fn partition(scope: i64) -> VectorPartitionKey {
    VectorPartitionKey::from_values(&[Value::Int64(scope)]).unwrap()
}
fn partition_search(db: &Database, scope: i64) -> contextdb_core::Result<Vec<i64>> {
    let bound = params([("v", Value::Vector(unit_vector(5, D)))]);
    db.execute(
        &format!(
            "SELECT id FROM items WHERE scope_id = {scope} ORDER BY embedding <=> $v USE VECTOR INDEXED LIMIT 10"
        ),
        &bound,
    )
    .map(|result| ids(result.rows()))
}

#[test]
fn a_proof_already_stale_stays_stale_through_compaction() {
    for role in [
        NativeIvfRootRoleForTest::Kind(14),
        NativeIvfRootRoleForTest::Qualification,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let db = partitioned(&dir.path().join("store.db"));
        let healthy = partition_search(&db, 1).unwrap();
        if role == NativeIvfRootRoleForTest::Qualification {
            plant_qualification(
                &db,
                "items",
                "embedding",
                &[Value::Int64(0)],
                &[10],
                10,
                240,
            );
        }
        let serving = db
            .native_ivf_family_roots_for_test(&index(), &partition(0))
            .unwrap()
            .into_iter()
            .filter(|family| !family.retired)
            .max_by_key(|family| family.generation)
            .unwrap();
        let storage = serving.storage;
        let family = |db: &Database| {
            db.native_ivf_family_roots_for_test(&index(), &partition(0))
                .unwrap()
                .into_iter()
                .find(|family| family.storage == storage)
                .unwrap()
        };
        db.plant_native_ivf_stale_root_for_test(&index(), &partition(0), storage, role)
            .unwrap();
        let planted = family(&db);

        let first = compact_and_count(&db);
        let second = compact_and_count(&db);

        assert!(
            first.left_stale + second.left_stale > 0,
            "{role:?}: compaction reached the stale table: {first:?} {second:?}"
        );
        let after = family(&db);
        match role {
            NativeIvfRootRoleForTest::Kind(tag) => {
                let at = usize::from(tag - 1);
                assert_eq!(after.expected_roots[at], planted.expected_roots[at]);
                assert_ne!(after.expected_roots[at], after.actual_roots[at]);
                partition_search(&db, 0).expect_err("the stale proof still refuses");
            }
            NativeIvfRootRoleForTest::Qualification => {
                assert_eq!(after.expected_qualification, planted.expected_qualification);
                assert_ne!(after.expected_qualification, after.actual_qualification);
            }
        }
        assert_eq!(
            partition_search(&db, 1).unwrap(),
            healthy,
            "{role:?}: the healthy partition keeps serving"
        );
        for other in db
            .native_ivf_family_roots_for_test(&index(), &partition(1))
            .unwrap()
        {
            assert_every_proof_holds(&[other], "the healthy partition");
        }
    }
}

#[test]
fn retired_families_carry_their_proofs_and_their_owners_go_with_them() {
    let dir = tempfile::tempdir().unwrap();
    let db = clustered_store(&dir.path().join("store.db"), 400);
    assert_owners_match(&db, "after the first build");
    let held = db.snapshot();
    let pin = db.pin_snapshot(held).unwrap();
    let mut next = 400;
    let mut rounds = 0;
    while !all_families(&db).iter().any(|family| family.retired) {
        rounds += 1;
        assert!(rounds <= 8, "no family reached the retired inventory");
        import_clustered(&db, next..next + 400);
        next += 400;
        drain(&db);
    }
    assert_owners_match(&db, "an active family moved to the retired inventory");
    let before: Vec<_> = all_families(&db);

    // The reader lets go first: its pages become usable holes. No cycle runs
    // between, so the retired families are still there to be moved.
    drop(pin);
    let seen = compact_and_count(&db);

    assert!(
        seen.refreshed_retired > 0,
        "a retired table really moved: {seen:?}"
    );
    assert!(seen.retired_records_read > 0);
    assert_eq!(
        seen.owner_entries_read, seen.scopes_read,
        "one owner, one scope, per moved table"
    );
    let after = all_families(&db);
    assert_every_proof_holds(&after, "after compaction with retired families");
    let retired_moved = before
        .iter()
        .zip(&after)
        .any(|(old, new)| old.retired && new.retired && old.actual_roots != new.actual_roots);
    assert!(retired_moved, "the retired family's own tables moved");
    let retired_qualification_moved = before.iter().filter(|old| old.retired).any(|old| {
        after
            .iter()
            .find(|new| new.storage == old.storage)
            .is_some_and(|new| {
                new.retired
                    && old.actual_qualification.is_some()
                    && new.actual_qualification.is_some()
                    && new.actual_qualification != old.actual_qualification
                    && new.expected_qualification == new.actual_qualification
            })
    });
    assert!(
        retired_qualification_moved,
        "a retired family's own qualification table moved and its stored root follows it"
    );
    assert_owners_match(&db, "after compaction");

    let mut rounds = 0;
    while all_families(&db).iter().any(|family| family.retired) {
        rounds += 1;
        assert!(
            rounds <= 400,
            "retired families drain once their reader lets go"
        );
        db.run_maintenance_cycle().unwrap();
        assert_owners_match(&db, "through partial cleanup");
    }
    assert_every_proof_holds(&all_families(&db), "after reclamation");
}

#[test]
fn purging_a_row_erases_families_with_their_owner_entries() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir.path().join("store.db"));
    let before = db.native_ivf_owned_storage_for_test().unwrap();
    assert!(!before.is_empty());
    db.execute("PURGE FROM items WHERE id IN (7, 11)", &empty())
        .unwrap();
    assert_owners_match(&db, "after a purge erased unserved families");
    db.compact_now().unwrap();
    assert_every_proof_holds(&all_families(&db), "compaction after a purge");
}

#[test]
fn a_step_failing_after_it_carried_a_proof_rolls_table_and_proof_back_together() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let db = fixture(&path);
    let answer = search(&db, "INDEXED");
    db.arm_compaction_failure_after_next_native_carry_for_test();

    db.compact_now()
        .expect_err("the armed step fails before commit");

    // A compaction step that fails stops this handle's service; reopen recovers.
    drop(db);
    let db = open(&path);
    assert_every_proof_holds(&all_families(&db), "reopened after the failed step");
    assert_eq!(search(&db, "INDEXED"), answer);
    let seen = compact_and_count(&db);
    assert!(
        seen.refreshed_kinds.iter().sum::<u64>() > 0,
        "the next attempt moves them: {seen:?}"
    );
    assert_every_proof_holds(&all_families(&db), "after the next attempt");
    assert_eq!(search(&db, "INDEXED"), answer);
}

/// Damage under an unchanged directory header: the bytes differ, the proof does not.
fn with_damaged_root(
    table: impl Fn(&Database) -> String,
) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let db = fixture(&path);
    let name = table(&db);
    db.close().unwrap();
    redb::damage_table_root_for_test(&path, &name).unwrap();
    (dir, path)
}

#[test]
fn damaged_bytes_in_an_index_table_get_no_fresh_proof_and_the_sweep_goes_on() {
    let (_dir, path) = with_damaged_root(|db| {
        let serving = db
            .native_ivf_scope_roots_at_snapshot_for_test(&index(), &tuple(), db.snapshot())
            .unwrap();
        let storage = all_families(db)
            .iter()
            .find(|family| family.expected_roots == serving.expected_roots)
            .unwrap()
            .storage;
        format!("ivf_{storage:016x}_0e")
    });
    let db = open(&path);
    let serving = |db: &Database| {
        db.native_ivf_scope_roots_at_snapshot_for_test(&index(), &tuple(), db.snapshot())
            .unwrap()
    };
    let before = serving(&db);
    db.compact_now()
        .expect("one damaged index table does not stop the sweep");
    let after = serving(&db);
    assert_eq!(
        after.expected_roots[13], before.expected_roots[13],
        "the proof is untouched"
    );
    assert_eq!(
        after.actual_roots[13], before.actual_roots[13],
        "the table did not move"
    );
    assert_ne!(
        after.actual_roots, before.actual_roots,
        "the other tables did"
    );
}

#[test]
fn damaged_bytes_in_a_relational_table_refuse_and_name_it() {
    let (_dir, path) = with_damaged_root(|_| "rel_items".to_owned());
    let db = open(&path);
    let refusal = db
        .compact_now()
        .expect_err("no proof is minted over these bytes");
    let text = format!("{refusal:?}");
    assert!(
        text.contains("items") && text.contains("checksum"),
        "{text}"
    );
}

#[test]
fn damaged_authority_tables_are_not_treated_as_rebuildable_index_data() {
    for table in ["ivf_scopes"] {
        let (_dir, path) = with_damaged_root(|_| table.to_owned());
        match Database::open_with_options(&path, DatabaseOpenOptions::default()) {
            Err(error) => assert!(
                format!("{error:?}").contains("Corrupt"),
                "{table}: {error:?}"
            ),
            Ok(db) => {
                db.compact_now().expect_err("authority damage refuses");
            }
        }
    }
}

fn carried(seen: &NativeIvfRootRelocationsForTest) -> u64 {
    seen.refreshed_kinds.iter().sum::<u64>() + seen.refreshed_qualification + seen.refreshed_retired
}

#[test]
fn damaged_scope_directory_pages_refuse_before_a_proof_is_carried() {
    let (_dir, path) = with_damaged_root(|_| "ivf_scopes".to_owned());
    let db = open(&path);
    let before = native_ivf_root_relocations_for_test();
    let refusal = db
        .compact_now()
        .expect_err("damaged authority pages are never copied under a fresh checksum");
    let seen = native_ivf_root_relocations_for_test();
    assert_eq!(
        carried(&seen),
        carried(&before),
        "no proof was carried: {seen:?}"
    );
    assert!(format!("{refusal:?}").contains("Corrupt"), "{refusal:?}");
}

#[test]
fn a_scope_directory_disagreeing_with_configuration_refuses_before_a_proof_is_carried() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let db = fixture(&path);
    db.plant_native_ivf_unbound_scope_entry_for_test().unwrap();
    let before = native_ivf_root_relocations_for_test();
    let refusal = db
        .compact_now()
        .expect_err("a directory Configuration does not bind is not rebound by compaction");
    let seen = native_ivf_root_relocations_for_test();
    assert_eq!(
        carried(&seen),
        carried(&before),
        "no proof was carried: {seen:?}"
    );
    assert!(
        format!("{refusal:?}").contains("disagrees with Configuration"),
        "{refusal:?}"
    );
    drop(db);
    let db = open(&path);
    db.compact_now()
        .expect_err("the mismatch was not blessed by the failed attempt either");
}
