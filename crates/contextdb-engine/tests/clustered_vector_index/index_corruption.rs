//! Damaged derived index state never stops writes. When a Context's serving
//! codebook fails its digest, or one of its sealed runs' directory pages is
//! missing, the generation is unserviceable: INDEXED is refused as store
//! corruption naming the page, AUTO takes EXACT with the exact answer,
//! writes keep committing and stay exact-visible, inspection shows the
//! rebuild recovery action, and the next maintenance call starts a
//! replacement from the committed rows, after which INDEXED serves every row
//! again and no recovery action is shown. A missing directory page is never
//! read as an empty list.

use super::support::{
    IndexDamage, IndexTestOptions, RecoveryAction, SearchRoute, build_progress,
    damage_serving_index, delta_runs, drive_until_serving, empty, int_ids, ivf, observe_search,
    open_with_index_test_options, params, plant_qualification, recovery_action, serving,
    unit_vector,
};
use contextdb_core::Value;
use contextdb_engine::Database;

const DIMENSIONS: usize = 16;
const OPTIONS: IndexTestOptions = IndexTestOptions {
    write_buffer_cap_bytes: Some(1024),
    build_batch_rows: None,
};
const BUILT: std::ops::Range<i64> = 0..200;
const SEALED: std::ops::Range<i64> = 200..240;
const AFTER: std::ops::Range<i64> = 1_000..1_060;

fn insert(db: &Database, id: i64) {
    db.execute(
        "INSERT INTO damaged_items (id, embedding) VALUES ($id, $embedding)",
        &params([
            ("id", Value::Int64(id)),
            (
                "embedding",
                Value::Vector(unit_vector(id as u64, DIMENSIONS)),
            ),
        ]),
    )
    .unwrap_or_else(|error| panic!("row {id} commits: {error:?}"));
}

fn query(db: &Database, id: i64, mode: &str) -> contextdb_core::Result<Vec<i64>> {
    db.execute(
        &format!(
            "SELECT id FROM damaged_items ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 10"
        ),
        &params([("query", Value::Vector(unit_vector(id as u64, DIMENSIONS)))]),
    )
    .map(|result| int_ids(&result))
}

fn assert_exact_serves(db: &Database, ids: impl IntoIterator<Item = i64>, why: &str) {
    for id in ids {
        assert_eq!(
            query(db, id, "EXACT").unwrap().first(),
            Some(&id),
            "{why}: EXACT finds row {id}"
        );
    }
}

#[test]
fn damaged_index_pages_refuse_indexed_and_never_stop_writes() {
    for damage in [
        IndexDamage::CodebookDigest,
        IndexDamage::MissingRunDirectory,
    ] {
        let root = tempfile::tempdir().expect("temporary store directory");
        let db = open_with_index_test_options(&root.path().join("damaged.db"), OPTIONS);
        db.execute(
            &format!(
                "CREATE TABLE damaged_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE AUTO {})",
                ivf(4, 4, 64)
            ),
            &empty(),
        )
        .expect("declare the clustered index");
        for id in BUILT {
            insert(&db, id);
        }
        drive_until_serving(&db, 200, 1, 10);
        for id in SEALED {
            insert(&db, id);
        }
        assert!(
            !delta_runs(&db, "damaged_items", "embedding", &[]).is_empty(),
            "{damage:?} fixture: a sealed delta run exists"
        );
        let generation = serving(&db, "damaged_items", "embedding", &[])
            .expect("a generation serves")
            .generation;
        plant_qualification(&db, "damaged_items", "embedding", &[], &[10], 10, 240);
        let (_, auto) = observe_search(&db, || query(&db, 7, "AUTO").unwrap());
        assert_eq!(
            auto.route,
            Some(SearchRoute::Indexed),
            "{damage:?} fixture: AUTO takes the index before the damage"
        );

        damage_serving_index(&db, "damaged_items", "embedding", &[], damage);
        let page = match damage {
            IndexDamage::CodebookDigest => "codebook",
            IndexDamage::MissingRunDirectory => "directory",
        };
        let error = query(&db, 7, "INDEXED").expect_err("INDEXED is refused");
        let debug = format!("{error:?}");
        assert!(
            debug.starts_with("StoreCorrupted") && debug.to_lowercase().contains(page),
            "{damage:?}: INDEXED is store corruption naming the {page} page, got {debug}"
        );
        let (auto_ids, auto) = observe_search(&db, || query(&db, 7, "AUTO").unwrap());
        assert_eq!(
            auto.route,
            Some(SearchRoute::Exact),
            "{damage:?}: AUTO takes EXACT"
        );
        assert_eq!(
            auto_ids,
            query(&db, 7, "EXACT").unwrap(),
            "{damage:?}: AUTO is exact"
        );
        assert_exact_serves(
            &db,
            BUILT.step_by(25).chain(SEALED),
            &format!("{damage:?} after the damage"),
        );

        for id in AFTER {
            insert(&db, id);
        }
        assert_exact_serves(
            &db,
            AFTER,
            &format!("{damage:?}: rows written after the damage"),
        );
        assert_eq!(
            recovery_action(&db, "damaged_items", "embedding", &[]),
            Some(RecoveryAction::Rebuild),
            "{damage:?}: inspection shows the rebuild recovery action"
        );

        db.run_maintenance_cycle().expect("one maintenance cycle");
        let progress = build_progress(&db, "damaged_items", "embedding", &[]);
        let replaced = serving(&db, "damaged_items", "embedding", &[])
            .is_some_and(|serving| serving.generation > generation);
        assert!(
            progress.in_flight || replaced,
            "{damage:?}: the next maintenance call starts a replacement: {progress:?}"
        );
        drive_until_serving(&db, 300, 1, 10);
        assert!(
            serving(&db, "damaged_items", "embedding", &[])
                .is_some_and(|serving| serving.generation > generation),
            "{damage:?}: a replacement generation serves"
        );
        for id in BUILT
            .step_by(25)
            .chain(SEALED.step_by(7))
            .chain(AFTER.step_by(7))
        {
            assert_eq!(
                query(&db, id, "INDEXED").unwrap().first(),
                Some(&id),
                "{damage:?}: after the rebuild INDEXED finds row {id}"
            );
        }
        assert_eq!(
            recovery_action(&db, "damaged_items", "embedding", &[]),
            None,
            "{damage:?}: no recovery action once the replacement serves"
        );
    }
}
