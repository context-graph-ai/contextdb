//! SQL mutation and file-reader outcomes across the first build and replacement.
//! The ordinary SQL controls do not certify native journal or descriptor binding.
use super::support::*;
use contextdb_core::{Value, Wallclock};
use contextdb_engine::{Database, ReadRoute, ReadSession};

const ROWS: i64 = 64;
const DIM: usize = 8;
fn bind(id: i64, seed: u64) -> std::collections::HashMap<String, Value> {
    params([
        ("id", Value::Int64(id)),
        ("v", Value::Vector(unit_vector(seed, DIM))),
    ])
}
fn seed(db: &Database) {
    db.execute(
        "CREATE TABLE items (id INT PRIMARY KEY, v VECTOR(8) SEARCH_MODE EXACT) HISTORY ALL",
        &empty(),
    )
    .unwrap();
    let tx = db.begin_or_panic();
    for id in 0..ROWS {
        db.execute_in_tx(
            tx,
            "INSERT INTO items VALUES ($id, $v)",
            &bind(id, id as u64),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
}
fn answer(db: &Database, mode: &str, seed: u64) -> Vec<i64> {
    int_ids(
        &db.execute(
            &format!("SELECT id FROM items ORDER BY v <=> $v USE VECTOR {mode} LIMIT 64"),
            &bind(0, seed),
        )
        .unwrap(),
    )
}
fn change(db: &Database) {
    db.execute("UPDATE items SET v = $v WHERE id = $id", &bind(7, 70_007))
        .unwrap();
    db.execute("DELETE FROM items WHERE id = 9", &empty())
        .unwrap();
    // Exercise the already-deleted path without turning it into a new insert.
    assert_eq!(
        db.execute("DELETE FROM items WHERE id = 9", &empty())
            .unwrap()
            .rows_affected(),
        0
    );
    let tx = db.begin_or_panic();
    db.execute_in_tx(
        tx,
        "UPDATE items SET v = $v WHERE id = $id",
        &bind(11, 70_011),
    )
    .unwrap();
    db.execute_in_tx(tx, "DELETE FROM items WHERE id = 13", &empty())
        .unwrap();
    db.rollback(tx).unwrap();
}
fn assert_current(db: &Database, mode: &str) {
    let ids = answer(db, mode, 70_007);
    assert_eq!(
        ids.first(),
        Some(&7),
        "the updated vector is the current version"
    );
    assert_eq!(ids.len(), (ROWS - 1) as usize);
    let mut unique = ids.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(
        unique,
        (0..ROWS).filter(|id| *id != 9).collect::<Vec<_>>(),
        "one result per surviving row; no resurrected deletion"
    );
    for id in [11, 13] {
        assert_eq!(
            answer(db, mode, id as u64).first(),
            Some(&id),
            "aborted mutations leave the old source and vector visible"
        );
    }
}
#[test]
fn canonical_mutations_and_rollback_survive_restart_control() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("canonical.db");
    let db = open_caller_driven(&path);
    seed(&db);
    change(&db);
    assert_current(&db, "EXACT");
    db.close().unwrap();
    drop(db);
    let db = open_caller_driven(&path);
    assert_current(&db, "EXACT");
    db.close().unwrap();
}
#[test]
fn canonical_file_reader_returns_updated_rows_and_excludes_deleted_rows_control() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("file-control.db");
    let db = open_caller_driven(&path);
    seed(&db);
    change(&db);
    let expected = answer(&db, "EXACT", 70_007);
    db.close().unwrap();
    drop(db);
    let reader = ReadSession::open(&path).unwrap();
    assert_eq!(reader.route(), ReadRoute::File);
    let found = reader
        .execute(
            "SELECT id FROM items ORDER BY v <=> $v USE VECTOR EXACT LIMIT 64",
            &bind(0, 70_007),
        )
        .unwrap();
    assert_eq!(int_ids(&found), expected);
    drop(reader);
}

fn enable_native(db: &Database) {
    db.execute("ALTER TABLE items ALTER COLUMN v SET SEARCH (TARGET_ACCURACY = 95, EXPECTED_VECTORS = DEFAULT)", &empty()).unwrap();
    db.execute(
        "ALTER TABLE items ALTER COLUMN v SET IVF (LISTS = 4, PROBES = 4, REFINE_FACTOR = 64.00)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "ALTER TABLE items ALTER COLUMN v SET SEARCH_MODE INDEXED",
        &empty(),
    )
    .unwrap();
}
fn index() -> contextdb_core::VectorIndexRef {
    contextdb_core::VectorIndexRef::new("items", "v")
}
fn tuple() -> contextdb_core::VectorPartitionKey {
    contextdb_core::VectorPartitionKey::unpartitioned()
}
fn partially_built(path: &std::path::Path) -> Database {
    let db = open_with_build_batch_rows(path, 16);
    seed(&db);
    enable_native(&db);
    let (_, ingestion) = observe_native_ivf_ingestion(&db, || db.run_maintenance_cycle().unwrap());
    assert!(ingestion.capture_complete && !ingestion.overflow);
    assert_eq!(
        ingestion.count, 16,
        "exactly the first bounded source batch was ingested"
    );
    assert!(build_progress(&db, "items", "v", &[]).in_flight);
    assert!(serving(&db, "items", "v", &[]).is_none());
    let cursor = native_ivf_build_progress_for_test(&db, &index(), &tuple()).unwrap();
    assert_eq!(cursor.ingested_vectors, 16);
    assert!(!cursor.canonical_complete);
    for id in [3, 5, 47, 49, 51, 53] {
        let identity = native_ivf_source_identity_for_test(&db, &index(), id).unwrap();
        assert_eq!(
            ingestion.identities[..ingestion.count].contains(&Some(identity)),
            id < 16,
            "the actual first batch, not SQL primary-key arithmetic, proves visited/unvisited status for {id}"
        );
        let journal = native_ivf_journal_identity_for_test(
            &db,
            &index(),
            &tuple(),
            contextdb_core::RowId(identity.row),
        )
        .unwrap();
        assert_eq!(journal.initialized, id < 16);
    }
    db
}
fn assert_partial_answers(ids: Vec<i64>, winner: i64) {
    assert_eq!(ids.first(), Some(&winner));
    assert_eq!(ids.len(), (ROWS - 2) as usize);
    let mut distinct = ids;
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(
        distinct,
        (0..ROWS)
            .filter(|id| ![5, 49].contains(id))
            .collect::<Vec<_>>()
    );
    assert!(distinct.contains(&51) && distinct.contains(&53));
}
#[test]
fn mutations_before_initial_maintenance_do_not_resurrect_rows_when_native_build_finishes() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("first-build.db");
    let db = partially_built(&path);
    for (update, delete) in [(3, 5), (47, 49)] {
        let old = native_ivf_source_identity_for_test(&db, &index(), update).unwrap();
        let removed = native_ivf_source_identity_for_test(&db, &index(), delete).unwrap();
        db.execute(
            "UPDATE items SET v = $v WHERE id = $id",
            &bind(update, 70_000 + update as u64),
        )
        .unwrap();
        db.execute(&format!("DELETE FROM items WHERE id = {delete}"), &empty())
            .unwrap();
        assert_eq!(
            db.execute(&format!("DELETE FROM items WHERE id = {delete}"), &empty())
                .unwrap()
                .rows_affected(),
            0
        );
        let updated = native_ivf_source_identity_for_test(&db, &index(), update).unwrap();
        assert_eq!(old.row, updated.row);
        assert!(updated.commit_lsn > old.commit_lsn);
        let live = native_ivf_journal_identity_for_test(
            &db,
            &index(),
            &tuple(),
            contextdb_core::RowId(old.row),
        )
        .unwrap();
        assert!(live.initialized && !live.deleted);
        assert_eq!(live.identity, Some(updated));
        let dead = native_ivf_journal_identity_for_test(
            &db,
            &index(),
            &tuple(),
            contextdb_core::RowId(removed.row),
        )
        .unwrap();
        assert!(dead.initialized && dead.deleted);
        assert_eq!(dead.copies, 0);
        assert!(dead.identity.unwrap().commit_lsn > removed.commit_lsn);
    }
    let rollback = [51, 53].map(|id| {
        let source = native_ivf_source_identity_for_test(&db, &index(), id).unwrap();
        let journal = native_ivf_journal_identity_for_test(
            &db,
            &index(),
            &tuple(),
            contextdb_core::RowId(source.row),
        )
        .unwrap();
        (source, journal)
    });
    let tx = db.begin_or_panic();
    db.execute_in_tx(
        tx,
        "UPDATE items SET v = $v WHERE id = $id",
        &bind(51, 70_051),
    )
    .unwrap();
    db.execute_in_tx(tx, "DELETE FROM items WHERE id = 53", &empty())
        .unwrap();
    db.rollback(tx).unwrap();
    for (id, (source, journal)) in [51, 53].into_iter().zip(rollback) {
        assert_eq!(
            native_ivf_source_identity_for_test(&db, &index(), id).unwrap(),
            source
        );
        assert_eq!(
            native_ivf_journal_identity_for_test(
                &db,
                &index(),
                &tuple(),
                contextdb_core::RowId(source.row)
            )
            .unwrap(),
            journal
        );
    }
    drive_until_serving(&db, ROWS as usize, 16, 10);
    for mode in ["INDEXED", "EXACT"] {
        for id in [3, 47] {
            assert_partial_answers(answer(&db, mode, 70_000 + id as u64), id);
        }
        for id in [51, 53] {
            assert_eq!(answer(&db, mode, id as u64).first(), Some(&id));
        }
    }
    db.close().unwrap();
    drop(db);
    let reader = ReadSession::open(&path).unwrap();
    assert_eq!(reader.route(), ReadRoute::File);
    for mode in ["INDEXED", "EXACT"] {
        for (id, query_seed) in [(3, 70_003), (47, 70_047), (51, 51), (53, 53)] {
            let (result, work) = observe_file_search(&reader, || {
                reader
                    .execute(
                        &format!(
                            "SELECT id FROM items ORDER BY v <=> $v USE VECTOR {mode} LIMIT 64"
                        ),
                        &bind(id, query_seed),
                    )
                    .unwrap()
            });
            assert_partial_answers(int_ids(&result), id);
            if mode == "INDEXED" {
                assert!(work.clustered_route && work.quantized_evaluations > 0);
            }
        }
    }
}
#[test]
fn initialized_live_and_preingest_tombstone_journals_refuse_stale_row_identity() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let db = partially_built(&root.path().join("stale.db"));
    for (id, delete) in [(3, false), (47, false), (5, true), (49, true)] {
        let old = native_ivf_source_identity_for_test(&db, &index(), id).unwrap();
        if delete {
            db.execute(&format!("DELETE FROM items WHERE id = {id}"), &empty())
                .unwrap();
        } else {
            db.execute(
                "UPDATE items SET v = $v WHERE id = $id",
                &bind(id, 70_000 + id as u64),
            )
            .unwrap();
        }
        let current = native_ivf_journal_identity_for_test(
            &db,
            &index(),
            &tuple(),
            contextdb_core::RowId(old.row),
        )
        .unwrap();
        assert!(current.initialized);
        assert_eq!(current.deleted, delete);
        assert!(current.identity.unwrap().commit_lsn > old.commit_lsn);
        let replacement = unit_vector(80_000 + id as u64, DIM);
        for mutation in [
            NativeConditionalMutation::Update(&replacement),
            NativeConditionalMutation::Delete,
        ] {
            let error = try_native_ivf_conditional_mutation_for_test(
                &db,
                &index(),
                &tuple(),
                old,
                mutation,
            )
            .unwrap_err();
            assert!(
                format!("{error:?}").starts_with("StaleRowVersion"),
                "the original typed stale-version refusal, got {error:?}"
            );
            assert_eq!(
                native_ivf_journal_identity_for_test(
                    &db,
                    &index(),
                    &tuple(),
                    contextdb_core::RowId(old.row)
                )
                .unwrap(),
                current
            );
        }
    }
    drive_until_serving(&db, ROWS as usize, 16, 10);
    for mode in ["INDEXED", "EXACT"] {
        assert_partial_answers(answer(&db, mode, 70_047), 47);
    }
}
#[test]
fn failure_after_source_and_preingest_journal_staging_rolls_back_both() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let db = partially_built(&root.path().join("staged-abort.db"));
    for (id, delete) in [(51, false), (53, true)] {
        let source = native_ivf_source_identity_for_test(&db, &index(), id).unwrap();
        let journal = native_ivf_journal_identity_for_test(
            &db,
            &index(),
            &tuple(),
            contextdb_core::RowId(source.row),
        )
        .unwrap();
        assert!(!journal.initialized);
        let (result, fired) = with_native_ivf_after_staged_failure_for_test(
            contextdb_core::RowId(source.row),
            || {
                if delete {
                    db.execute("DELETE FROM items WHERE id = 53", &empty())
                } else {
                    db.execute(
                        "UPDATE items SET v = $v WHERE id = $id",
                        &bind(id, 70_000 + id as u64),
                    )
                }
            },
        );
        assert!(fired, "failure is injected after both actual writes staged");
        assert!(result.is_err());
        assert_eq!(
            native_ivf_source_identity_for_test(&db, &index(), id).unwrap(),
            source
        );
        assert_eq!(
            native_ivf_journal_identity_for_test(
                &db,
                &index(),
                &tuple(),
                contextdb_core::RowId(source.row)
            )
            .unwrap(),
            journal
        );
    }
    drive_until_serving(&db, ROWS as usize, 16, 10);
    for mode in ["INDEXED", "EXACT"] {
        for id in [51, 53] {
            assert_eq!(answer(&db, mode, id as u64).first(), Some(&id));
        }
        assert_eq!(answer(&db, mode, 0).len(), ROWS as usize);
    }
}
#[test]
fn native_indexed_file_search_survives_restart_replacement_and_erasure() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("native-file.db");
    let db = open_caller_driven(&path);
    seed(&db);
    enable_native(&db);
    drive_until_serving(&db, ROWS as usize, 1, 10);
    let first = serving(&db, "items", "v", &[]).unwrap();
    db.execute(
        "ALTER TABLE items ALTER COLUMN v SET WITH (training_seed = 7)",
        &empty(),
    )
    .unwrap();
    drive_until_serving(&db, ROWS as usize, 1, 10);
    let replacement = serving(&db, "items", "v", &[]).unwrap();
    assert_ne!(first.generation, replacement.generation);
    change(&db);
    let victim = unit_vector(17, DIM);
    let needles = [
        little_endian_bytes(&victim),
        quantized_record(&db, "items", "v", &victim),
    ];
    let before = physical_pages(&db, "items", "v");
    for needle in &needles {
        assert!(!needle.is_empty());
        assert!(
            before
                .iter()
                .any(|page| contains_bytes(&page.bytes, needle)),
            "physical scan must positively see each victim representation before PURGE"
        );
    }
    assert_eq!(
        db.execute("PURGE FROM items WHERE id = 17", &empty())
            .unwrap()
            .rows_affected(),
        1
    );
    drive_until_serving(&db, ROWS as usize, 1, 10);
    assert_eq!(
        retained_generations(&db, "items", "v", &[]),
        vec![replacement.generation]
    );
    let pages = physical_pages(&db, "items", "v");
    assert!(!pages.is_empty());
    for needle in &needles {
        assert!(
            !pages.iter().any(|page| contains_bytes(&page.bytes, needle)),
            "PURGE erased canonical and quantized victim bytes from every physical copy"
        );
    }
    let expected = answer(&db, "EXACT", 70_007);
    assert_eq!(expected.len(), (ROWS - 2) as usize);
    assert!(!expected.contains(&9) && !expected.contains(&17));
    let (indexed, work) = observe_search(&db, || answer(&db, "INDEXED", 70_007));
    assert!(work.clustered_route);
    assert_eq!(indexed, expected);
    db.close().unwrap();
    drop(db);
    let reader = ReadSession::open(&path).unwrap();
    assert_eq!(reader.route(), ReadRoute::File);
    for mode in ["INDEXED", "EXACT"] {
        let (found, work) = observe_file_search(&reader, || {
            reader
                .execute(
                    &format!("SELECT id FROM items ORDER BY v <=> $v USE VECTOR {mode} LIMIT 64"),
                    &bind(0, 70_007),
                )
                .unwrap()
        });
        assert_eq!(int_ids(&found), expected);
        if mode == "INDEXED" {
            assert!(work.clustered_route);
            assert_eq!(work.served_generation, Some(replacement.generation));
            assert!(work.quantized_evaluations > 0);
        }
    }
    // Rows: the same lifecycle where a list record fills a page or spans pages.
    {
        use contextdb_core::Error;
        use contextdb_vector::ivf::storage::{PAGE_BYTES, PageKey, PageKind};
        use std::collections::BTreeMap;
        const LIVE: i64 = 8;
        // Each dimension with the fixed pages one of its list records spans.
        for (dimensions, record_pages) in [(3888_usize, 1_u64), (3889, 2), (4096, 2)] {
            let bound = |seed: u64| params([("q", Value::Vector(unit_vector(seed, dimensions)))]);
            let store = |path: &std::path::Path| {
                let db = open_caller_driven(path);
                db.execute(
                &format!(
                    "CREATE TABLE items (id INT PRIMARY KEY, v VECTOR({dimensions}) SEARCH_MODE INDEXED \
                     IVF (LISTS = 2, PROBES = 2, REFINE_FACTOR = 8.00) \
                     WITH (training_samples = 8, training_iterations = 1))"
                ),
                &empty(),
            )
            .unwrap();
                let tx = db.begin_or_panic();
                for id in 0..LIVE {
                    db.execute_in_tx(
                        tx,
                        "INSERT INTO items VALUES ($id, $v)",
                        &params([
                            ("id", Value::Int64(id)),
                            ("v", Value::Vector(unit_vector(id as u64, dimensions))),
                        ]),
                    )
                    .unwrap();
                }
                db.commit(tx).unwrap();
                drive_until_serving(&db, LIVE as usize, 1, 1);
                db
            };
            let search = |db: &Database, mode: &str, seed: u64| {
                db.execute(
                    &format!("SELECT id FROM items ORDER BY v <=> $q USE VECTOR {mode} LIMIT 16"),
                    &bound(seed),
                )
            };
            let ids =
                |db: &Database, mode: &str, seed: u64| int_ids(&search(db, mode, seed).unwrap());
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("wide-lifecycle.db");
            let db = store(&path);
            for id in 0..LIVE {
                for mode in ["INDEXED", "EXACT"] {
                    assert_eq!(
                        ids(&db, mode, id as u64).first(),
                        Some(&id),
                        "{dimensions} {mode}"
                    );
                }
            }
            let (found, work) = observe_search(&db, || ids(&db, "INDEXED", 0));
            assert_eq!(found, ids(&db, "EXACT", 0));
            assert!(work.clustered_route);
            assert_eq!(
                work.list_pages_read,
                LIVE as u64 * record_pages,
                "a full probe at {dimensions} dimensions reads every page of every record once"
            );

            let first = serving(&db, "items", "v", &[]).unwrap();
            db.execute(
                "ALTER TABLE items ALTER COLUMN v SET WITH (training_seed = 7)",
                &empty(),
            )
            .unwrap();
            drive_until_serving(&db, LIVE as usize, 1, 1);
            assert_ne!(
                first.generation,
                serving(&db, "items", "v", &[]).unwrap().generation
            );
            assert_eq!(ids(&db, "INDEXED", 0), ids(&db, "EXACT", 0));

            // A row committed after the build is served beside the replacement's lists.
            db.execute(
                "INSERT INTO items VALUES ($id, $v)",
                &params([
                    ("id", Value::Int64(LIVE)),
                    ("v", Value::Vector(unit_vector(LIVE as u64, dimensions))),
                ]),
            )
            .unwrap();
            assert_eq!(ids(&db, "INDEXED", LIVE as u64).first(), Some(&LIVE));
            drive_until_serving(&db, LIVE as usize + 1, 1, 1);

            // Whole-record needles, over payloads joined in page order across continuations.
            let victim = unit_vector(3, dimensions);
            let norm = victim
                .iter()
                .map(|value| f64::from(*value).powi(2))
                .sum::<f64>()
                .sqrt();
            let code: Vec<u8> = victim
                .iter()
                .map(|value| {
                    ((f64::from(*value) / norm * 127.0)
                        .round()
                        .clamp(-127.0, 127.0) as i8) as u8
                })
                .collect();
            let joined = |db: &Database| {
                let mut pages = BTreeMap::<(u64, PageKey), [u8; PAGE_BYTES]>::new();
                db.visit_native_ivf_physical_pages_for_test(&index(), &mut |page| {
                    pages.insert((page.storage, page.key), *page.payload);
                })
                .unwrap();
                pages.values().flatten().copied().collect::<Vec<u8>>()
            };
            let canonical = little_endian_bytes(&victim);
            let before = joined(&db);
            assert!(
                contains_bytes(&before, &canonical),
                "{dimensions} stored vector"
            );
            assert!(contains_bytes(&before, &code), "{dimensions} list record");
            assert_eq!(
                db.execute("PURGE FROM items WHERE id = 3", &empty())
                    .unwrap()
                    .rows_affected(),
                1
            );
            let after = joined(&db);
            assert!(
                !contains_bytes(&after, &canonical),
                "{dimensions} stored vector erased"
            );
            assert!(
                !contains_bytes(&after, &code),
                "{dimensions} list record erased"
            );
            let expected = ids(&db, "EXACT", 0);
            assert_eq!(expected.len(), LIVE as usize);
            assert!(!expected.contains(&3));
            assert_eq!(ids(&db, "INDEXED", 0), expected);

            db.close().unwrap();
            drop(db);
            let reader = ReadSession::open(&path).unwrap();
            assert_eq!(reader.route(), ReadRoute::File);
            for mode in ["INDEXED", "EXACT"] {
                let found = reader
                    .execute(
                        &format!(
                            "SELECT id FROM items ORDER BY v <=> $q USE VECTOR {mode} LIMIT 16"
                        ),
                        &bound(0),
                    )
                    .unwrap();
                assert_eq!(
                    int_ids(&found),
                    expected,
                    "{dimensions} {mode} from the file"
                );
            }
            drop(reader);
            let db = open_caller_driven(&path);
            for mode in ["INDEXED", "EXACT"] {
                assert_eq!(
                    ids(&db, mode, 0),
                    expected,
                    "{dimensions} {mode} after reopen"
                );
            }
            db.close().unwrap();
            drop(db);

            // Damage the last page of a real record on its own store, once per fault.
            for remove in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let db = store(&root.path().join("wide-damage.db"));
                let healthy = ids(&db, "EXACT", 0);
                let generation = serving(&db, "items", "v", &[]).unwrap().generation;
                let mut target = None;
                db.visit_native_ivf_physical_pages_for_test(&index(), &mut |page| {
                    if target.is_none()
                        && !page.building
                        && page.key.generation == generation
                        && page.key.kind == PageKind::Lists
                        && page.key.ordinal == record_pages - 1
                    {
                        target = Some((page.storage, page.key));
                    }
                })
                .unwrap();
                let (storage, key) = target.expect("the last page of a real record");
                if remove {
                    db.remove_native_ivf_page_for_test(storage, key).unwrap();
                } else {
                    db.corrupt_native_ivf_page_for_test(storage, key, true, 0, 1)
                        .unwrap();
                }
                assert!(
                    matches!(search(&db, "INDEXED", 0), Err(Error::StoreCorrupted { .. })),
                    "{dimensions} dimensions, removed {remove}: damage refuses INDEXED"
                );
                assert_eq!(ids(&db, "EXACT", 0), healthy, "EXACT stays usable");
                db.close().unwrap();
            }
        }
    }
}
#[test]
fn first_saved_search_loads_compact_serving_descriptor_as_real_manifest_source() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("descriptor.db");
    let db = open_caller_driven(&path);
    seed(&db);
    enable_native(&db);
    drive_until_serving(&db, ROWS as usize, 1, 10);
    let generation = serving(&db, "items", "v", &[]).unwrap().generation;
    let expected = answer(&db, "EXACT", 7);
    db.close().unwrap();
    drop(db);
    let db = open_caller_driven(&path);
    let stored = native_ivf_serving_descriptor_for_test(&db, &index(), &tuple())
        .unwrap()
        .unwrap();
    assert_eq!(stored.bytes.len(), 200);
    assert_eq!(stored.generation, generation);
    let (result, cold) = observe_native_ivf_descriptor(&db, || answer(&db, "INDEXED", 7));
    assert_eq!(result, expected);
    assert!(cold.capture_complete && !cold.overflow);
    assert_eq!(cold.descriptor_loads, 1);
    assert!(cold.descriptor_bytes > 0 && cold.descriptor_bytes <= 239);
    assert_eq!(cold.descriptor_bytes, stored.bytes.len() as u64);
    assert_eq!(cold.loaded, Some(stored.bytes));
    assert_eq!(cold.selected, Some(stored.bytes));
    assert_eq!(cold.selected_generation, Some(generation));
    assert_eq!(cold.saved_generation_loads, 1);
    assert_eq!(cold.saved_generation_bytes, cold.descriptor_bytes);
    assert!(cold.root_control_loads > 0);
    assert_eq!(cold.root_control_bytes, cold.root_control_loads * 3904);
    assert_eq!((cold.index_builds, cold.index_repairs), (0, 0));
    assert_eq!(cold.route, Some(SearchRoute::Indexed));
    let (result, warm) = observe_native_ivf_descriptor(&db, || answer(&db, "INDEXED", 7));
    assert_eq!(result, expected);
    assert!(warm.capture_complete && !warm.overflow);
    assert_eq!((warm.descriptor_loads, warm.descriptor_bytes), (0, 0));
    assert_eq!(
        (warm.saved_generation_loads, warm.saved_generation_bytes),
        (0, 0)
    );
    assert_eq!(warm.selected, Some(stored.bytes));
    assert_eq!(warm.selected_generation, Some(generation));
    assert_eq!(warm.route, Some(SearchRoute::Indexed));
    assert_eq!((warm.index_builds, warm.index_repairs), (0, 0));
    db.close().unwrap();
    drop(db);
    let db = open_caller_driven(&path);
    for (offset, mask) in [(200, 1), (0, 0)] {
        assert!(
            corrupt_native_ivf_serving_descriptor_for_test(&db, &index(), &tuple(), offset, mask)
                .is_err()
        );
        assert_eq!(
            native_ivf_serving_descriptor_for_test(&db, &index(), &tuple())
                .unwrap()
                .unwrap(),
            stored
        );
    }
    corrupt_native_ivf_serving_descriptor_for_test(&db, &index(), &tuple(), 32, 1).unwrap();
    db.close().unwrap();
    drop(db);
    let db = open_caller_driven(&path);
    let (result, indexed) = observe_native_ivf_descriptor(&db, || {
        db.execute(
            "SELECT id FROM items ORDER BY v <=> $v USE VECTOR INDEXED LIMIT 64",
            &bind(0, 7),
        )
    });
    let error = result.unwrap_err();
    assert!(
        format!("{error:?}").starts_with("IndexCorrupted"),
        "INDEXED returns its typed descriptor corruption: {error:?}"
    );
    assert!(indexed.capture_complete && !indexed.overflow && indexed.quarantined);
    let (auto, work) = observe_native_ivf_descriptor(&db, || answer(&db, "AUTO", 7));
    assert_eq!(auto, expected);
    assert_eq!(work.route, Some(SearchRoute::Exact));
    assert!(work.quarantined);
    assert!(work.capture_complete && !work.overflow);
    assert_eq!((work.index_builds, work.index_repairs), (0, 0));
    assert_eq!(answer(&db, "EXACT", 7), expected);
}
fn assert_codebook_layout(a: ProducerCodebookLayouts, b: ProducerCodebookLayouts) {
    for (left, right) in [
        (a.control, b.control),
        (a.values, b.values),
        (a.counts, b.counts),
    ] {
        assert_eq!((left.size(), left.align()), (right.size(), right.align()));
    }
    assert_eq!(a.charged_bytes, b.charged_bytes);
    assert_eq!(
        a.charged_bytes,
        a.control.size() + a.values.size() + a.counts.size()
    );
}
fn assert_workspace_layout(a: ProducerWorkspaceLayouts, b: ProducerWorkspaceLayouts) {
    for (left, right) in [
        (a.controls, b.controls),
        (a.buffer, b.buffer),
        (a.raw, b.raw),
        (a.values, b.values),
        (a.encoded, b.encoded),
        (a.order, b.order),
        (a.combined, b.combined),
        (a.page_scratch, b.page_scratch),
        (a.catalog, b.catalog),
    ] {
        assert_eq!((left.size(), left.align()), (right.size(), right.align()));
    }
    assert_eq!(a.charged_bytes, b.charged_bytes);
    assert_eq!(a.buffer_record_bytes, b.buffer_record_bytes);
    assert_eq!(
        a.charged_bytes,
        a.combined.size() + a.page_scratch.size() + a.catalog.size()
    );
    let mut combined = a.controls;
    for part in [a.buffer, a.raw, a.values, a.encoded, a.order] {
        combined = combined.extend(part).unwrap().0;
    }
    assert_eq!(combined.pad_to_align(), a.combined);
}
fn assert_constructor(attempt: ConstructorAttempt, bytes: usize, admitted: bool) {
    assert!(!attempt.overflow);
    assert!(attempt.event_count <= attempt.events.len());
    assert_eq!(attempt.retained_after_drop, 0);
    let events = &attempt.events[..attempt.event_count];
    if admitted {
        attempt.outcome.unwrap();
        assert!(matches!(events.first(),Some(Some(ConstructorEvent::Debited(n))) if *n==bytes));
        assert!(matches!(events.last(),Some(Some(ConstructorEvent::Released(n))) if *n==bytes));
        let mut allocations = 0;
        let mut frees = 0;
        for event in &events[1..events.len() - 1] {
            match event {
                Some(ConstructorEvent::Allocated) => allocations += 1,
                Some(ConstructorEvent::PhysicallyFreed) => frees += 1,
                _ => panic!("one actual constructor invoice around its allocations"),
            }
        }
        assert!(allocations > 0);
        assert_eq!(allocations, frees);
        let first_free = events
            .iter()
            .position(|e| matches!(e, Some(ConstructorEvent::PhysicallyFreed)))
            .unwrap();
        assert!(
            !events[first_free..]
                .iter()
                .any(|e| matches!(e, Some(ConstructorEvent::Allocated)))
        );
    } else {
        assert!(matches!(
            attempt.outcome,
            Err(contextdb_core::Error::MemoryBudgetExceeded { .. })
        ));
        assert_eq!(
            attempt.event_count, 0,
            "refusal precedes debit and physical allocation"
        );
    }
}
#[test]
fn default_policy_resolution_matches_producer_checked_layout_and_constructor_debits() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    for (dimensions, population, expected_vectors, memory_limit) in [
        (1, 0, 1, 8 * 1024 * 1024),
        (7, 17, 4096, 32 * 1024 * 1024),
        (129, 65, 1_000_000, 8 * 1024 * 1024),
    ] {
        let root = tempfile::tempdir().unwrap();
        let db = Database::open_with_options(
            root.path().join("layouts.db"),
            contextdb_engine::DatabaseOpenOptions {
                memory_limit: Some(memory_limit),
                maintenance_policy: contextdb_engine::MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap();
        db.execute(&format!("CREATE TABLE items (id INT PRIMARY KEY, v VECTOR({dimensions}) SEARCH_MODE INDEXED SEARCH (EXPECTED_VECTORS = {expected_vectors}) IVF (LISTS = DEFAULT) WITH (write_buffer_bytes = DEFAULT))"),&empty()).unwrap();
        for id in 0..population {
            db.execute(
                "INSERT INTO items VALUES ($id,$v)",
                &params([
                    ("id", Value::Int64(id)),
                    ("v", Value::Vector(unit_vector(id as u64, dimensions))),
                ]),
            )
            .unwrap();
        }
        let resolved = native_ivf_resolved_policy_for_test(&db, &index(), &tuple()).unwrap();
        assert_eq!(
            (
                resolved.inputs.dimensions,
                resolved.population,
                resolved.expected_vectors
            ),
            (dimensions, population as u64, expected_vectors)
        );
        assert_eq!(
            resolved.codebook_budget_bytes,
            resolved.build_budget_bytes / 16
        );
        let desired = 64_u64.max((resolved.population.max(expected_vectors)).div_ceil(1000));
        let mut low = 0_u32;
        let mut high = u32::try_from(desired).unwrap();
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            let fits = codebook_allocation_layout(dimensions, mid.min(population as u32), true)
                .is_ok_and(|layout| layout.charged_bytes as u64 <= resolved.codebook_budget_bytes);
            if fits {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        assert!(low > 0);
        assert_eq!(resolved.inputs.lists, low);
        assert_eq!(resolved.active_lists, low.min(population as u32));
        let producer_book =
            codebook_allocation_layout(dimensions, resolved.active_lists, true).unwrap();
        assert_codebook_layout(resolved.codebook, producer_book);
        let producer_workspace = workspace_allocation_layout(resolved.inputs).unwrap();
        assert_workspace_layout(resolved.workspace, producer_workspace);
        let record = producer_workspace.buffer_record_bytes;
        assert_eq!(record, dimensions * 4 + 40);
        assert_eq!(resolved.inputs.buffer_bytes % record, 0);
        assert!(producer_workspace.charged_bytes <= resolved.workspace_budget_bytes);
        let population_cap = resolved.population.max(expected_vectors).max(1) as usize;
        let records = resolved.inputs.buffer_bytes / record;
        assert!(records <= population_cap);
        if records < population_cap {
            let mut one_more = resolved.inputs;
            one_more.buffer_bytes = one_more.buffer_bytes.checked_add(record).unwrap();
            assert!(
                workspace_allocation_layout(one_more).map_or(true, |layout| layout.charged_bytes
                    > resolved.workspace_budget_bytes),
                "DEFAULT is the smaller of the population cap and the producer-checked complete-record capacity of the actual remaining workspace"
            );
        }
        for training_counts in [false, true] {
            let layout =
                codebook_allocation_layout(dimensions, resolved.active_lists, training_counts)
                    .unwrap();
            if resolved.active_lists == 0 {
                assert_eq!(population, 0);
                assert_eq!(layout.charged_bytes, 0);
                let attempt = native_ivf_constructor_attempt_for_test(
                    &db,
                    &index(),
                    &tuple(),
                    ConstructorAllocation::Codebook { training_counts },
                    0,
                );
                assert!(!attempt.overflow);
                attempt.outcome.unwrap();
                assert_eq!(attempt.event_count, 0, "empty codebook has no reservation");
                assert_eq!(attempt.retained_after_drop, 0);
                continue;
            }
            assert!(layout.charged_bytes > 0);
            if !training_counts {
                assert_eq!(layout.counts.size(), 0);
            }
            for admitted in [true, false] {
                assert_constructor(
                    native_ivf_constructor_attempt_for_test(
                        &db,
                        &index(),
                        &tuple(),
                        ConstructorAllocation::Codebook { training_counts },
                        layout.charged_bytes - usize::from(!admitted),
                    ),
                    layout.charged_bytes,
                    admitted,
                );
            }
        }
        for admitted in [true, false] {
            assert_constructor(
                native_ivf_constructor_attempt_for_test(
                    &db,
                    &index(),
                    &tuple(),
                    ConstructorAllocation::WriterWorkspace,
                    producer_workspace.charged_bytes - usize::from(!admitted),
                ),
                producer_workspace.charged_bytes,
                admitted,
            );
        }
        let error = db
            .execute(
                "ALTER TABLE items ALTER COLUMN v SET IVF (LISTS = 4294967295)",
                &empty(),
            )
            .unwrap_err();
        assert!(
            matches!(error, contextdb_core::Error::MemoryBudgetExceeded { .. }),
            "explicit over-budget LISTS refuses, never clamps: {error:?}"
        );
        assert_eq!(
            native_ivf_resolved_policy_for_test(&db, &index(), &tuple())
                .unwrap()
                .inputs
                .lists,
            resolved.inputs.lists
        );
    }
    assert!(matches!(
        codebook_allocation_layout(usize::MAX, u32::MAX, true),
        Err(contextdb_core::Error::MemoryBudgetExceeded { .. })
    ));
}
