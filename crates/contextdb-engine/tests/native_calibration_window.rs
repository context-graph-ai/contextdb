//! One immutable private candidate verifies once while its ordinary serving view stays separate.
use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use contextdb_vector::ivf::storage::BuildPhase;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
#[test]
#[ignore = "the vector index route is switched off in this release"]
fn a_private_candidate_retains_its_real_verdict_across_calibration_queries() {
    let root = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(
        root.path().join("candidate.db"),
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, v VECTOR(2) SEARCH_MODE INDEXED IVF (LISTS = 1, PROBES = 1, REFINE_FACTOR = 1.00) WITH (training_samples = 32, training_iterations = 1, calibration_samples = 4, calibration_max_k = 1, calibration_depths = [1]))",&HashMap::new()).unwrap();
    let tx = db.begin_or_panic();
    for id in 0..32 {
        db.execute_in_tx(
            tx,
            "INSERT INTO items VALUES ($id,$v)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("v".into(), Value::Vector(vec![1.0, id as f32 / 32.0])),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    let index = VectorIndexRef::new("items", "v");
    let tuple = VectorPartitionKey::unpartitioned();
    let _point = db
        .plant_native_ivf_calibration_setting_for_test(&index, &tuple, 1, 100)
        .unwrap();
    let mut candidate = None;
    for _ in 0..64 {
        db.run_maintenance_cycle().unwrap();
        let progress = db
            .native_ivf_build_progress_for_test(&index, &tuple)
            .unwrap();
        if let Some(build) = progress
            .build
            .filter(|build| build.phase == BuildPhase::Publish)
        {
            candidate = Some(build.manifest.generation);
            break;
        }
    }
    let candidate = candidate.expect("a completed private candidate is prepared before F1");
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    contextdb_engine::vector_ivf_observations::set_native_ivf_verification_observer_for_test(
        Arc::new(move |event| {
            if event.index.table == "items" && event.generation == candidate {
                seen.fetch_add(1, Ordering::SeqCst);
            }
        }),
    );
    let mut complete = false;
    for _ in 0..64 {
        let report = db.run_maintenance_cycle().unwrap();
        assert!(report.vector.first_failure.is_none(), "{report:?}");
        if report.vector.remaining_partitions == 0 {
            complete = true;
            break;
        }
    }
    assert!(complete, "the finite one-depth calibration publishes");
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "the actual private root's verification runs once; each query consumes that recorded verdict"
    );
    db.close().unwrap();
}

struct Queries(AtomicUsize);
impl contextdb_vector::ivf::IvfConstructorObserverForTest for Queries {
    fn admitted(&self, _: contextdb_vector::ivf::IvfConstructorAllocationForTest) {}
    fn debited(&self, _: u64, _: usize, _: usize, operation: &'static str) {
        if operation == "clustered_query" {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}
#[test]
#[ignore = "the vector index route is switched off in this release"]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn a_rejected_point_reuses_its_actual_failing_sample_before_repeating_successes() {
    const SAMPLES: usize = 4;
    // The declared factors are 1,2,4; growth adds8, then reaches population8.
    const POINTS: usize = 4;
    let root = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(
        root.path().join("witness.db"),
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, v VECTOR(2) SEARCH_MODE INDEXED IVF (LISTS = 1, PROBES = 1) WITH (training_samples = 8, training_iterations = 1, calibration_samples = 4, calibration_max_k = 1, calibration_depths = [1], refine_factors = [1, 2, 4], refine_growth = 2))",&HashMap::new()).unwrap();
    let tx = db.begin_or_panic();
    for id in 0..8 {
        db.execute_in_tx(
            tx,
            "INSERT INTO items VALUES ($id,$v)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("v".into(), Value::Vector(vec![1.0, id as f32 / 8.0])),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    let index = VectorIndexRef::new("items", "v");
    let tuple = VectorPartitionKey::unpartitioned();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let _answers = db
        .rewrite_native_ivf_calibration_answers_for_test(
            &index,
            &tuple,
            Box::new(move |sample, _, exact| {
                observed.fetch_add(1, Ordering::SeqCst);
                if sample == SAMPLES - 1 {
                    Vec::new()
                } else {
                    exact.to_vec()
                }
            }),
        )
        .unwrap();
    let queries = Arc::new(Queries(AtomicUsize::new(0)));
    let mut complete = false;
    contextdb_vector::ivf::with_ivf_constructor_observer_for_test(queries.clone(), || {
        for _ in 0..64 {
            let report = db.run_maintenance_cycle().unwrap();
            assert!(report.vector.first_failure.is_none(), "{report:?}");
            if report.vector.remaining_partitions == 0 {
                complete = true;
                break;
            }
        }
    });
    assert!(complete);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        SAMPLES,
        "the same actual sample/depth answers are supplied once each"
    );
    let bound = SAMPLES + SAMPLES + SAMPLES + (POINTS - 1);
    assert!(
        queries.0.load(Ordering::SeqCst) <= bound,
        "the exact oracle and final run each use all {SAMPLES} samples; the first rejected point finds the late witness and each later point tests that same real witness first: actual={} bound={bound}",
        queries.0.load(Ordering::SeqCst)
    );
    let mut failed = None;
    db.with_native_ivf_read_for_test(&index,&tuple,|scope|scope.visit_qualification(&mut|record| {if let contextdb_engine::vector_ivf_observations::NativeIvfQualificationRecordForTest::Depth(depth)=record {failed=Some(depth);}})).unwrap();
    let depth = failed.unwrap();
    assert_eq!(depth.samples, SAMPLES as u32);
    assert_eq!(depth.queries_below, 1);
    assert!(!depth.qualified);
    db.close().unwrap();
}

type RootRecord = (redb::StorageSnapshotIdentity, u64, bool);

#[derive(Default)]
struct RootControls(std::sync::Mutex<(bool, Vec<RootRecord>)>);
impl contextdb_engine::vector_ivf_observations::NativeIvfSealObserver for RootControls {
    fn observe(&self, event: contextdb_engine::vector_ivf_observations::NativeIvfSealEvent<'_>) {
        use contextdb_engine::vector_ivf_observations::{
            NativeIvfSealEvent as Event, NativeIvfSourceInstance as Source,
        };
        let mut state = self.0.lock().unwrap();
        match event {
            Event::RootControlLoaded { scope, .. } if scope.index.table == "items" => {
                state.0 = true
            }
            Event::ScopeOpened {
                scope,
                source_instance: Source::Read { snapshot, .. },
                ..
            } if scope.index.table == "items" => {
                let loaded = std::mem::take(&mut state.0);
                if let Some(row) = state.1.iter_mut().find(|row| row.0 == snapshot) {
                    row.1 += u64::from(loaded);
                } else {
                    state.1.push((snapshot, u64::from(loaded), false));
                }
            }
            Event::PageReadAttempt {
                operation,
                source_instance: Source::Read { snapshot, .. },
                key,
                ..
            } if operation.scope.index.table == "items"
                && key.kind == contextdb_vector::ivf::storage::PageKind::Codebook
                && key.generation != 0 =>
            {
                let row = state
                    .1
                    .iter_mut()
                    .find(|row| row.0 == snapshot)
                    .expect("the actual read scope opened before loading its codebook");
                row.2 = true;
            }
            _ => {}
        }
    }
    fn before_boundary(
        &self,
        _: contextdb_engine::vector_ivf_observations::NativeIvfFaultBoundary<'_>,
    ) -> contextdb_core::Result<()> {
        Ok(())
    }
}
#[test]
#[ignore = "the vector index route is switched off in this release"]
fn an_immutable_private_query_root_loads_its_scope_control_once() {
    let controls = Arc::new(RootControls::default());
    contextdb_engine::vector_ivf_observations::with_native_ivf_seal_observer_for_test(
        controls.clone(),
        a_private_candidate_retains_its_real_verdict_across_calibration_queries,
    );
    let state = controls.0.lock().unwrap();
    let mut queries = 0;
    for (_, loads, used_codebook) in &state.1 {
        if *used_codebook {
            queries += 1;
            assert_eq!(
                *loads, 1,
                "the real immutable query root reuses its authenticated scope control"
            );
        }
    }
    assert!(
        queries > 0,
        "the production query actually loaded its codebook from the observed root"
    );
}

#[test]
fn default_training_seed_tracks_policy_revision_but_explicit_zero_stays_zero() {
    let root = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(
        root.path().join("seed-revision.db"),
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    db.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, v VECTOR(2) SEARCH_MODE AUTO)",
        &HashMap::new(),
    )
    .unwrap();
    let index = VectorIndexRef::new("items", "v");
    let tuple = VectorPartitionKey::unpartitioned();
    let resolve = || {
        db.native_ivf_resolved_policy_for_test(&index, &tuple)
            .unwrap()
            .training
            .seed
    };
    let first = resolve();
    assert_eq!(
        resolve(),
        first,
        "an unchanged partition/source/revision repeats its default seed"
    );
    db.execute(
        "ALTER TABLE items ALTER COLUMN v SET SEARCH (TARGET_ACCURACY = 99)",
        &HashMap::new(),
    )
    .unwrap();
    assert_ne!(
        resolve(),
        first,
        "the declared DEFAULT seed includes the selected partition's policy revision"
    );
    db.execute(
        "ALTER TABLE items ALTER COLUMN v SET WITH (training_seed = 0)",
        &HashMap::new(),
    )
    .unwrap();
    assert_eq!(resolve(), 0, "explicit zero is the consumer's exact seed");
    db.execute(
        "ALTER TABLE items ALTER COLUMN v SET SEARCH (TARGET_ACCURACY = 95)",
        &HashMap::new(),
    )
    .unwrap();
    assert_eq!(
        resolve(),
        0,
        "a policy revision never changes an explicit seed"
    );
    db.close().unwrap();
}
