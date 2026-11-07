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

#[test]
#[ignore = "the vector index route is switched off in this release"]
fn obsolete_copy_cleanup_refuses_tampered_live_journal() {
    let dir = tempfile::tempdir().unwrap();
    let (db, storage, key) = fixture(&dir.path().join("store.db"), 0);
    let generation = serving(&db, "items", "embedding", &[]).unwrap().generation;
    let expected_rows = rows(&db);
    let before = keys(&db, storage);
    db.corrupt_native_ivf_page_for_test(storage, key, true, 0, 1)
        .unwrap();
    let damaged = keys(&db, storage);
    assert_eq!(before, damaged);
    assert_refusal(&db, storage, generation, &expected_rows, &damaged);
}
fn swap_captured_kind_root(db: &Database, storage: u64) {
    let target = format!("ivf_{storage:016x}_10");
    db.with_native_ivf_fixture_write_for_test(|write| {
        let target_root = write
            .table_root_fingerprint(redb::TableDefinition::<&[u8], &[u8]>::new(&target))
            .unwrap()
            .unwrap();
        let alternative = write
            .list_tables()
            .unwrap()
            .filter_map(|handle| {
                let name = redb::TableHandle::name(&handle).to_owned();
                if !name.starts_with("ivf_") || name == target || name.len() != 23 {
                    return None;
                }
                let root = write
                    .table_root_fingerprint(redb::TableDefinition::<&[u8], &[u8]>::new(&name))
                    .unwrap()?;
                (root != target_root).then_some(root)
            })
            .next()
            .expect("another populated native kind table with a different valid root");
        let scopes = redb::TableDefinition::<&[u8], &[u8]>::new("ivf_scopes");
        let mut table = write.open_table(scopes).unwrap();
        let candidates: Vec<(Vec<u8>, Vec<u8>, usize)> = table
            .iter()
            .unwrap()
            .filter_map(|entry| {
                let (key, value) = entry.unwrap();
                let (key, value) = (key.value(), value.value());
                if key.len() != 64 || !value.starts_with(b"CDBIVFS1") {
                    return None;
                }
                let positions: Vec<usize> = value
                    .windows(32)
                    .enumerate()
                    .filter_map(|(i, w)| (w == target_root).then_some(i))
                    .collect();
                assert!(
                    positions.len() <= 1,
                    "journal fingerprint must have a unique location"
                );
                positions
                    .first()
                    .map(|i| (key.to_vec(), value.to_vec(), *i))
            })
            .collect();
        assert_eq!(
            candidates.len(),
            1,
            "exactly one captured scope contains the live journal root"
        );
        let (key, mut value, position) = candidates.into_iter().next().unwrap();
        value[position..position + 32].copy_from_slice(&alternative);
        let end = value.len() - 32;
        let mut digest = blake3::Hasher::new();
        digest.update(b"contextdb.clustered.scope.control.v1");
        digest.update(&key);
        digest.update(&value[..end]);
        value[end..].copy_from_slice(digest.finalize().as_bytes());
        table.insert(key.as_slice(), value.as_slice()).unwrap();
        Ok(())
    })
    .unwrap();
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
#[ignore = "the vector index route is switched off in this release"]
fn obsolete_copy_cleanup_refuses_swapped_captured_kind_root() {
    let dir = tempfile::tempdir().unwrap();
    let (db, storage, _) = fixture(&dir.path().join("store.db"), 0);
    let generation = serving(&db, "items", "embedding", &[]).unwrap().generation;
    let expected_rows = rows(&db);
    let before = keys(&db, storage);
    swap_captured_kind_root(&db, storage);
    assert_eq!(keys(&db, storage), before);
    assert_refusal(&db, storage, generation, &expected_rows, &before);
}

#[test]
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
