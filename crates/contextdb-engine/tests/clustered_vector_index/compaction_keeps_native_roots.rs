//! Compaction is invisible to vector search and maintenance. Native role and
//! qualification tables are proven by physical root fingerprints, and moving a
//! table's pages changes that fingerprint, so a sweep must carry the proof it
//! moves in the same commit, and must never mint a proof over damaged bytes.

use super::support::{empty, open_caller_driven, params, unit_vector};
use contextdb_core::Value;
use contextdb_engine::Database;
use contextdb_engine::vector_ivf_observations::{
    NativeIvfRootRelocationsForTest, native_ivf_root_relocations_for_test,
};
use std::path::Path;

const D: usize = 32;

/// The caller drives every cycle, so what compacted and when is deterministic.
fn open(path: &Path) -> Database {
    open_caller_driven(path)
}
/// 500 x 32, unpartitioned, 32 maintenance cycles: the compaction regression fixture.
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

fn carried(seen: &NativeIvfRootRelocationsForTest) -> u64 {
    seen.refreshed_kinds.iter().sum::<u64>() + seen.refreshed_qualification + seen.refreshed_retired
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
