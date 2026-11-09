//! Refuse damaged live RowCopies authority before obsolete-run cleanup.
use super::support::{
    drive_until_serving, empty, int_ids, ivf, open_caller_driven, params, serving, unit_vector,
};
use contextdb_core::{Error, Value, VectorIndexRef};
use contextdb_engine::Database;
use contextdb_vector::ivf::storage::{PageKey, PageKind};
use redb::ReadableTable;

fn rows(db: &Database) -> Vec<i64> {
    int_ids(
        &db.execute("SELECT id FROM items ORDER BY id", &empty())
            .unwrap(),
    )
}

fn fixture(path: &std::path::Path, base: i64) -> (Database, u64, PageKey) {
    let db = open_caller_driven(path);
    db.execute(&format!("CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(8) SEARCH_MODE INDEXED {} CONSOLIDATION (CHANGE_PERCENT = 10))", ivf(4,4,64)), &empty()).unwrap();
    for id in base..base + 40_i64 {
        db.execute(
            "INSERT INTO items VALUES ($id,$v)",
            &params([
                ("id", Value::Int64(id)),
                ("v", Value::Vector(unit_vector(id as u64, 8))),
            ]),
        )
        .unwrap();
    }
    drive_until_serving(&db, 40, 1, 10);
    let old = serving(&db, "items", "embedding", &[]).unwrap().generation;
    let pin = db.pin_snapshot(db.snapshot()).unwrap();
    for id in base..base + 7_i64 {
        db.execute(
            "UPDATE items SET embedding = $v WHERE id = $id",
            &params([
                ("id", Value::Int64(id)),
                ("v", Value::Vector(unit_vector(1000 + id as u64, 8))),
            ]),
        )
        .unwrap();
    }
    drive_until_serving(&db, 40, 1, 10);
    let current = serving(&db, "items", "embedding", &[]).unwrap().generation;
    assert_ne!(old, current);
    let mut copies = Vec::new();
    db.visit_native_ivf_physical_pages_for_test(
        &VectorIndexRef::new("items", "embedding"),
        &mut |page| {
            if page.key.kind == PageKind::RowCopies && page.key.run < current {
                copies.push((page.storage, page.key));
            }
        },
    )
    .unwrap();
    assert!(
        !copies.is_empty(),
        "an actual obsolete journal run must exist before pin release"
    );
    let target = copies[0];
    drop(pin);
    (db, target.0, target.1)
}

fn keys(db: &Database, storage: u64) -> Vec<Vec<u8>> {
    let name = format!("ivf_{storage:016x}_10");
    db.with_native_ivf_fixture_read_for_test(|read| {
        let table = read
            .open_table(redb::TableDefinition::<&[u8], &[u8]>::new(&name))
            .unwrap();
        Ok(table
            .iter()
            .unwrap()
            .map(|e| e.unwrap().0.value().to_vec())
            .collect())
    })
    .unwrap()
}

fn table_bytes(db: &Database, storage: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    let name = format!("ivf_{storage:016x}_10");
    db.with_native_ivf_fixture_read_for_test(|read| {
        let table = read
            .open_table(redb::TableDefinition::<&[u8], &[u8]>::new(&name))
            .unwrap();
        Ok(table
            .iter()
            .unwrap()
            .map(|e| {
                let (k, v) = e.unwrap();
                (k.value().to_vec(), v.value().to_vec())
            })
            .collect())
    })
    .unwrap()
}
fn swap_foreign_table(db: &Database, storage: u64, donor: &[(Vec<u8>, Vec<u8>)]) {
    let name = format!("ivf_{storage:016x}_10");
    db.with_native_ivf_fixture_write_for_test(|write| {
        let mut table = write
            .open_table(redb::TableDefinition::<&[u8], &[u8]>::new(&name))
            .unwrap();
        let existing: Vec<_> = table
            .iter()
            .unwrap()
            .map(|e| e.unwrap().0.value().to_vec())
            .collect();
        for key in existing {
            table.remove(key.as_slice()).unwrap();
        }
        for (key, value) in donor {
            table.insert(key.as_slice(), value.as_slice()).unwrap();
        }
        Ok(())
    })
    .unwrap();
}

fn assert_refusal(
    db: &Database,
    storage: u64,
    generation: u64,
    expected_rows: &[i64],
    damaged_keys: &[Vec<u8>],
) {
    let mut refused = None;
    for _ in 0..super::support::maintenance_cycles(40, 1, 10) {
        let outcome = db.run_maintenance_cycle();
        assert_eq!(
            keys(db, storage),
            damaged_keys,
            "no RowCopies keys may disappear before or during refusal"
        );
        if let Err(error) = outcome {
            refused = Some(error);
            break;
        }
    }
    assert!(
        matches!(refused, Some(Error::StoreCorrupted { .. })),
        "cleanup must return typed StoreCorrupted for the live journal: {refused:?}"
    );
    assert_eq!(
        serving(db, "items", "embedding", &[]).unwrap().generation,
        generation
    );
    assert_eq!(rows(db), expected_rows);
}

#[test]
#[ignore = "The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run."]
fn obsolete_copy_cleanup_refuses_foreign_live_journal_table() {
    let dir = tempfile::tempdir().unwrap();
    let (db, storage, _) = fixture(&dir.path().join("store.db"), 0);
    let (donor, donor_storage, _) = fixture(&dir.path().join("donor.db"), 100);
    let generation = serving(&db, "items", "embedding", &[]).unwrap().generation;
    let expected_rows = rows(&db);
    let original = table_bytes(&db, storage);
    let foreign = table_bytes(&donor, donor_storage);
    assert_ne!(
        original, foreign,
        "the donor is a different store with a different journal"
    );
    swap_foreign_table(&db, storage, &foreign);
    let damaged = keys(&db, storage);
    assert!(
        !damaged.is_empty(),
        "the foreign journal contains real runs"
    );
    assert_refusal(&db, storage, generation, &expected_rows, &damaged);
}
