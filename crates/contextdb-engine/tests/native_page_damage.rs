//! Physical fault injection changes a real native page, preserving its expected authority.
use contextdb_core::{Error, Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use contextdb_vector::ivf::storage::PageKind;
use std::collections::HashMap;
const fn build_cycle_bound(rows: usize, batch: usize, training: usize) -> usize {
    rows.div_ceil(batch) * (training + 3) + rows + 3
}
#[test]
fn actual_page_header_and_payload_damage_refuse_indexed_and_leave_canonical_exact_usable() {
    for payload in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let db = Database::open_with_options(
            root.path().join("page.db"),
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap();
        db.execute("CREATE TABLE items (id INT PRIMARY KEY, v VECTOR(2) SEARCH_MODE INDEXED IVF (LISTS = 1, PROBES = 1, REFINE_FACTOR = 32.00) WITH (training_samples = 32, training_iterations = 1))",&HashMap::new()).unwrap();
        let tx = db.begin_or_panic();
        for id in 0..32 {
            db.execute_in_tx(
                tx,
                "INSERT INTO items VALUES ($id,$v)",
                &HashMap::from([
                    ("id".to_string(), Value::Int64(id)),
                    ("v".to_string(), Value::Vector(vec![1.0, id as f32 / 32.0])),
                ]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
        let mut done = false;
        for _ in 0..32 {
            if db
                .run_maintenance_cycle()
                .unwrap()
                .vector
                .remaining_partitions
                == 0
            {
                done = true;
                break;
            }
        }
        assert!(done);
        let index = VectorIndexRef::new("items", "v");
        let tuple = VectorPartitionKey::unpartitioned();
        let mut target = None;
        db.visit_native_ivf_physical_pages_for_test(&index, &mut |p| {
            if p.key.kind == PageKind::Codebook && p.key.ordinal == 1 {
                target = Some((p.storage, p.key));
            }
        })
        .unwrap();
        let before = db
            .native_ivf_scope_roots_at_snapshot_for_test(&index, &tuple, db.snapshot())
            .unwrap();
        let (storage, key) = target.expect("real codebook coordinate page");
        db.corrupt_native_ivf_page_for_test(storage, key, payload, 0, 1)
            .unwrap();
        let after = db
            .native_ivf_scope_roots_at_snapshot_for_test(&index, &tuple, db.snapshot())
            .unwrap();
        assert_eq!(
            before.expected_roots, after.expected_roots,
            "fault injection must preserve the expected family authority"
        );
        assert_ne!(
            before.actual_roots, after.actual_roots,
            "a real native kind root changed"
        );
        let params = HashMap::from([("q".to_string(), Value::Vector(vec![1.0, 0.0]))]);
        let query = |mode| {
            db.execute(
                &format!("SELECT id FROM items ORDER BY v <=> $q USE VECTOR {mode} LIMIT 3"),
                &params,
            )
        };
        assert!(
            matches!(query("INDEXED"), Err(Error::StoreCorrupted { .. })),
            "physical page damage refuses INDEXED"
        );
        assert_eq!(
            query("AUTO").unwrap().rows(),
            query("EXACT").unwrap().rows(),
            "canonical exact remains usable"
        );
        db.close().unwrap();
    }
}

#[test]
fn a_frozen_source_range_excludes_later_appends_and_preparation_is_counted_once_durably() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("replacement.db");
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    db.native_ivf_set_test_options(
        contextdb_engine::vector_ivf_observations::NativeIvfIndexTestOptions {
            build_batch_rows: Some(16),
            write_buffer_cap_bytes: Some(4096),
        },
    )
    .unwrap();
    db.execute("CREATE TABLE items (id INT PRIMARY KEY, v VECTOR(64) SEARCH_MODE INDEXED IVF (LISTS = 4, PROBES = 4, REFINE_FACTOR = 64.00) CONSOLIDATION (CHANGE_PERCENT = 10))",&HashMap::new()).unwrap();
    let insert = |ids: std::ops::Range<i64>| {
        let tx = db.begin_or_panic();
        for id in ids {
            let mut vector = vec![0.0; 64];
            vector[0] = 0.12345679;
            vector[1] = (id + 1) as f32;
            db.execute_in_tx(
                tx,
                "INSERT INTO items VALUES ($id,$v)",
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("v".into(), Value::Vector(vector)),
                ]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
    };
    insert(0..120);
    let mut ready = false;
    for _ in 0..build_cycle_bound(120, 16, 10) {
        if db
            .run_maintenance_cycle()
            .unwrap()
            .vector
            .remaining_partitions
            == 0
        {
            ready = true;
            break;
        }
    }
    assert!(ready, "the initial generation completes");
    insert(1000..1240);
    db.run_maintenance_cycle().unwrap();
    for id in 2000..2096 {
        insert(id..id + 1);
    }
    let index = VectorIndexRef::new("items", "v");
    let tuple = VectorPartitionKey::unpartitioned();
    let mut published = false;
    for _ in 0..build_cycle_bound(360, 16, 10) {
        let report = db.run_maintenance_cycle().unwrap();
        assert!(report.vector.first_failure.is_none(), "{report:?}");
        if report.vector.built_partitions != 0 {
            published = true;
            break;
        }
    }
    assert!(
        published,
        "future appends do not extend the frozen ingestion range: {:?}",
        db.native_ivf_build_progress_for_test(&index, &tuple)
            .unwrap()
            .build
    );
    let mut complete = false;
    for _ in 0..64 {
        let before = db
            .native_ivf_maintenance_state_for_test(&index, &tuple)
            .unwrap()
            .legacy_entries_prepared;
        let report = db.run_maintenance_cycle().unwrap();
        assert!(report.vector.first_failure.is_none(), "{report:?}");
        let after = db
            .native_ivf_maintenance_state_for_test(&index, &tuple)
            .unwrap()
            .legacy_entries_prepared;
        assert!(
            after >= before && after - before <= 16,
            "one actual preparation batch: {before}->{after}"
        );
        if db
            .native_ivf_catalog_for_test(&index, &tuple)
            .unwrap()
            .is_some_and(|c| c.legacy_runs == 0)
        {
            complete = true;
            break;
        }
    }
    assert!(complete, "the finite legacy set retires");
    assert_eq!(
        db.native_ivf_maintenance_state_for_test(&index, &tuple)
            .unwrap()
            .legacy_entries_prepared,
        96,
        "each post-frontier row prepared once"
    );
    db.close().unwrap();
    drop(db);
    let reopened = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        reopened
            .native_ivf_maintenance_state_for_test(&index, &tuple)
            .unwrap()
            .legacy_entries_prepared,
        96,
        "the actual preparation count survives reopen"
    );
    reopened.close().unwrap();
}

#[test]
fn a_retained_codebook_is_lazy_after_reopen_and_released_by_the_real_retirement_commit() {
    retained_codebook_lifecycle(false);
}

#[test]
fn dropping_a_state_b_table_releases_its_actual_retained_codebook_before_close() {
    retained_codebook_lifecycle(true);
}

fn retained_codebook_lifecycle(drop_at_b: bool) {
    use contextdb_vector::ivf::{
        IvfConstructorAllocationForTest, IvfConstructorObserverForTest,
        with_ivf_constructor_observer_for_test,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    #[derive(Default)]
    struct Seen {
        created: AtomicUsize,
        released: AtomicUsize,
    }
    impl IvfConstructorObserverForTest for Seen {
        fn admitted(&self, _: IvfConstructorAllocationForTest) {}
        fn debited(&self, _id: u64, owner: usize, bytes: usize, operation: &'static str) {
            if operation == "clustered_retained_codebook" {
                assert_ne!(owner, 0);
                assert!(bytes > 0);
                self.created.fetch_add(1, Ordering::SeqCst);
            }
        }
        fn released(&self, _id: u64, _owner: usize, _bytes: usize, operation: &'static str) {
            if operation == "clustered_retained_codebook" {
                self.released.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("retained.db");
    let open = || {
        Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap()
    };
    let seen = Arc::new(Seen::default());
    with_ivf_constructor_observer_for_test(seen.clone(), || {
        let db = open();
        db.execute("CREATE TABLE items (id INT PRIMARY KEY, v VECTOR(2) SEARCH_MODE INDEXED IVF (LISTS = 2, PROBES = 2, REFINE_FACTOR = 1.00) CONSOLIDATION (CHANGE_PERCENT = 10) WITH (write_buffer_bytes = 1024, build_batch_entries = 4, ingest_batch_keys = 4, remap_batch_entries = 4, training_iterations = 1))",&HashMap::new()).unwrap();
        let index = VectorIndexRef::new("items", "v");
        let tuple = VectorPartitionKey::unpartitioned();
        let insert = |db: &Database, id: i64| {
            db.execute(
                "INSERT INTO items VALUES ($id,$v)",
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("v".into(), Value::Vector(vec![1.0, id as f32 / 1000.0])),
                ]),
            )
            .unwrap()
        };
        for id in 0..24 {
            insert(&db, id);
        }
        for _ in 0..160 {
            if db
                .run_maintenance_cycle()
                .unwrap()
                .vector
                .remaining_partitions
                == 0
            {
                break;
            }
        }
        let old = db
            .native_ivf_maintenance_state_for_test(&index, &tuple)
            .unwrap()
            .manifest
            .unwrap();
        for id in 100..124 {
            insert(&db, id);
        }
        db.run_maintenance_cycle().unwrap();
        assert!(
            db.native_ivf_build_progress_for_test(&index, &tuple)
                .unwrap()
                .build
                .is_some_and(
                    |build| build.phase != contextdb_vector::ivf::storage::BuildPhase::Ready
                )
        );
        for id in 200..232 {
            insert(&db, id);
        }
        let mut found = false;
        for _ in 0..200 {
            db.run_maintenance_cycle().unwrap();
            if db
                .native_ivf_maintenance_state_for_test(&index, &tuple)
                .unwrap()
                .manifest
                .is_some_and(|manifest| manifest.generation != old.generation)
            {
                found = true;
                break;
            }
        }
        assert!(found);
        let held = db
            .native_ivf_retained_codebook_for_test(&index, &tuple)
            .unwrap();
        assert_eq!(held.manifest.unwrap().codebook, old.codebook);
        assert!(held.charged_bytes > 0);
        assert_eq!(
            held.original_owner,
            db.native_ivf_original_owner_for_test().unwrap()
        );
        assert_eq!(seen.created.load(Ordering::SeqCst), 1);
        if drop_at_b {
            db.execute("DROP TABLE items", &HashMap::new()).unwrap();
            assert_eq!(
                seen.released.load(Ordering::SeqCst),
                1,
                "the committed DROP releases the actual old codebook without waiting for database close"
            );
            db.close().unwrap();
            return;
        }
        db.close().unwrap();
        drop(db);
        assert_eq!(seen.released.load(Ordering::SeqCst), 1);
        let db = open();
        assert_eq!(
            seen.created.load(Ordering::SeqCst),
            1,
            "open performs no centroid rehydration"
        );
        let selected = db
            .execute(
                "SELECT id FROM items ORDER BY v <=> $v USE VECTOR INDEXED LIMIT 1",
                &HashMap::from([("v".into(), Value::Vector(vec![1.0, 0.25]))]),
            )
            .unwrap();
        assert_eq!(selected.rows().len(), 1);
        assert_eq!(
            seen.created.load(Ordering::SeqCst),
            2,
            "the first actual SQL search rehydrates the selected tuple once"
        );
        for _ in 0..3 {
            let held = db
                .native_ivf_retained_codebook_for_test(&index, &tuple)
                .unwrap();
            assert!(held.charged_bytes > 0);
            assert_eq!(
                held.original_owner,
                db.native_ivf_original_owner_for_test().unwrap()
            );
        }
        assert_eq!(
            seen.created.load(Ordering::SeqCst),
            2,
            "the first selected tuple rehydrates once"
        );
        let mut retired = false;
        for _ in 0..160 {
            db.run_maintenance_cycle().unwrap();
            if db
                .native_ivf_catalog_for_test(&index, &tuple)
                .unwrap()
                .unwrap()
                .retained_codebook
                .is_none()
            {
                retired = true;
                break;
            }
        }
        assert!(retired);
        let held = db
            .native_ivf_retained_codebook_for_test(&index, &tuple)
            .unwrap();
        assert!(held.manifest.is_none());
        assert_eq!(held.charged_bytes, 0);
        assert_eq!(seen.released.load(Ordering::SeqCst), 2);
        let result = db
            .execute(
                "SELECT id FROM items ORDER BY v <=> $v USE VECTOR INDEXED LIMIT 100",
                &HashMap::from([("v".into(), Value::Vector(vec![1.0, 0.25]))]),
            )
            .unwrap();
        let mut ids = result
            .rows()
            .iter()
            .map(|row| match row[0] {
                Value::Int64(id) => id,
                _ => panic!("integer key"),
            })
            .collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(
            ids,
            (0..24).chain(100..124).chain(200..232).collect::<Vec<_>>()
        );
        db.close().unwrap();
    });
}
