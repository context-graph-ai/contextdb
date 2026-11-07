use super::*;
use redb::{ReadableDatabase, ReadableTable, TableDefinition, TableHandle};
use std::sync::atomic::AtomicUsize;

// The final publication remains the original public maintenance call in each
// fixture. Only its prerequisite advances through actual stored native phases.
pub(super) fn native_fixture_before_publication(
    db: &Database,
    routes: &[VectorPartitionRef],
    declared_source_rows: usize,
) {
    let bound = native_fixture_cycle_bound(db, routes, declared_source_rows);
    for _ in 0..bound {
        let pending = routes.iter().all(|route| {
            db.native_ivf_publication_eligible_for_test(&route.index, &route.partition_key)
                .unwrap()
        });
        if pending {
            return;
        }
        let report = db.run_maintenance_cycle().unwrap();
        assert!(
            report.vector.first_failure.is_none(),
            "native fixture preparation failed: {report:?}"
        );
    }
    panic!("native declared phases must converge before publication within {bound} cycles");
}

pub(super) fn native_fixture_cycle_bound(
    db: &Database,
    routes: &[VectorPartitionRef],
    declared_source_rows: usize,
) -> usize {
    routes
        .iter()
        .map(|route| {
            let resolved = db
                .native_ivf_resolved_policy_for_test(&route.index, &route.partition_key)
                .unwrap();
            let rows = declared_source_rows
                .max(resolved.population as usize)
                .max(1);
            let ingest = rows.div_ceil(resolved.training.ingest_batch_keys.max(1));
            let phase = rows.div_ceil(resolved.training.phase_batch_entries.max(1));
            // Ingest and its terminal cursor, Seed, each declared training pass,
            // Assign, Publish, Ready, plus a finite row-at-a-time remap allowance.
            ingest + (phase + 1) * (resolved.training.iterations as usize + 4) + rows + 6
        })
        .sum::<usize>()
        .max(1)
}

pub(super) fn native_fixture_ready(
    db: &Database,
    routes: &[VectorPartitionRef],
    declared_source_rows: usize,
) -> VectorMaintenanceReport {
    for _ in 0..native_fixture_cycle_bound(db, routes, declared_source_rows) {
        let report = db.run_maintenance_cycle().unwrap().vector;
        assert!(
            report.first_failure.is_none(),
            "native fixture readiness failed: {report:?}"
        );
        if report.ready_partitions >= routes.len() && report.remaining_partitions == 0 {
            return report;
        }
    }
    panic!("native fixture never reported ready with no remaining partitions");
}

pub(super) fn native_fixture_open(path: &std::path::Path, limit: Option<usize>) -> Database {
    let mut options = DatabaseOpenOptions {
        memory_limit: limit,
        maintenance_policy: MaintenancePolicy::CallerDriven,
        ..DatabaseOpenOptions::default()
    };
    // Opening the deliberately oversized fixture has its own declared reader;
    // the later measured maintenance still uses its original writer limit.
    options.owner_reads.limits.limits.memory = 64 * 1024 * 1024;
    Database::open_with_options(path, options).unwrap()
}

pub(super) fn native_fixture_cold_cache(db: &Database) {
    let persistence = db.persistence.as_ref().unwrap();
    persistence.settle_writer_cache_budget(0).unwrap();
    assert_eq!(
        persistence
            .read_cache_observation()
            .unwrap()
            .unwrap()
            .installed_capacity(),
        0
    );
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn native_fixture_control(
    db: &Database,
    route: &VectorPartitionRef,
) -> (Vec<u8>, Vec<u8>) {
    db.with_native_ivf_read_for_test(&route.index, &route.partition_key, |scope| {
        scope.with_scope_control_for_test(|_, stored, bytes| {
            Ok((stored.scope_key.to_vec(), bytes.to_vec()))
        })
    })
    .unwrap()
}

pub(super) fn native_fixture_serving(
    db: &Database,
    route: &VectorPartitionRef,
) -> Option<[u8; 200]> {
    db.native_ivf_serving_descriptor_for_test(&route.index, &route.partition_key)
        .unwrap()
        .map(|descriptor| descriptor.bytes)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn native_fixture_page_target(
    db: &Database,
    route: &VectorPartitionRef,
    kind: contextdb_vector::ivf::storage::PageKind,
) -> (String, Vec<u8>, Vec<u8>) {
    let mut saved = None;
    db.with_native_ivf_read_for_test(&route.index, &route.partition_key, |scope| {
        scope.visit_physical_pages(&mut |page| {
            if page.key.kind == kind && saved.is_none() {
                saved = Some((page.storage, page.bytes.to_vec()));
            }
        })
    })
    .unwrap();
    let (storage, bytes) = saved.expect("the real maintained native family owns the target page");
    let prefix = format!("ivf_{storage:016x}_");
    db.with_native_ivf_fixture_read_for_test(|read| {
        let mut found = None;
        for handle in read.list_tables().unwrap() {
            let name = handle.name();
            if !name.starts_with(&prefix) {
                continue;
            }
            let table = read
                .open_table(TableDefinition::<&[u8], &[u8]>::new(name))
                .unwrap();
            for entry in table.iter().unwrap() {
                let (key, value) = entry.unwrap();
                if value.value() == bytes.as_slice() {
                    assert!(
                        found.is_none(),
                        "the observed saved page has one physical table/key"
                    );
                    found = Some((
                        name.to_owned(),
                        key.value().to_vec(),
                        value.value().to_vec(),
                    ));
                }
            }
        }
        Ok(found.expect("the captured native saved page exists in its real storage family"))
    })
    .unwrap()
}

#[derive(Clone, Copy)]
enum ColdMetadata {
    Catalog,
    Journal,
    CoveredJournal,
}

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn public_cold_metadata_refusal(metadata: ColdMetadata) {
    const MALFORMED_METADATA_BYTES: usize = 9 * 1024 * 1024;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("metadata-owner.db");
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    let declaration = "CREATE TABLE metadata_items (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED SEARCH (TARGET_ACCURACY = 95, EXPECTED_VECTORS = 16) IVF (LISTS = 1, PROBES = 1, REFINE_FACTOR = 1.00) WITH (write_buffer_bytes = 4096, training_samples = 16, training_iterations = 1))";
    db.execute(declaration, &HashMap::new()).unwrap();
    for id in 0..16 {
        db.execute(
            "INSERT INTO metadata_items VALUES ($id, '[1,0,0]')",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    let native_routes = [VectorPartitionRef::new(
        VectorIndexRef::new("metadata_items", "v"),
        VectorPartitionKey::unpartitioned(),
    )];
    native_fixture_before_publication(&db, &native_routes, 16);
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        1
    );
    native_fixture_ready(&db, &native_routes, 16);
    let partition = VectorPartitionRef::new(
        VectorIndexRef::new("metadata_items", "v"),
        VectorPartitionKey::unpartitioned(),
    );
    if matches!(metadata, ColdMetadata::Journal) {
        db.execute(
            "UPDATE metadata_items SET v = '[0,1,0]' WHERE id = 0",
            &HashMap::new(),
        )
        .unwrap();
    }
    if matches!(
        metadata,
        ColdMetadata::Journal | ColdMetadata::CoveredJournal
    ) {
        // A real policy revision starts replacement ingestion on the next cycle;
        // it leaves the covered/uncovered row frontier itself unchanged.
        db.execute(
            "ALTER TABLE metadata_items ALTER COLUMN v SET WITH (training_seed = 1)",
            &HashMap::new(),
        )
        .unwrap();
    }
    let serving = native_fixture_serving(&db, &partition).unwrap();
    let original_catalog = native_fixture_control(&db, &partition);
    let (table_name, malformed_key, original_value) = match metadata {
        ColdMetadata::Catalog => (
            "ivf_scopes".to_owned(),
            original_catalog.0.clone(),
            original_catalog.1.clone(),
        ),
        ColdMetadata::Journal | ColdMetadata::CoveredJournal => {
            let (current, covered) = db
                .with_native_ivf_read_for_test(
                    &partition.index,
                    &partition.partition_key,
                    |scope| {
                        scope.with_scope_control_for_test(|_, stored, _| {
                            let generation = stored
                                .serving
                                .expect("the original serving manifest")
                                .generation;
                            let family = stored
                                .families
                                .iter()
                                .flatten()
                                .find(|family| family.generation == generation)
                                .unwrap();
                            Ok((stored.current.frontier, family.frozen_frontier))
                        })
                    },
                )
                .unwrap();
            if matches!(metadata, ColdMetadata::Journal) {
                assert!(
                    current > covered,
                    "the real committed update is outside the serving basis"
                );
            } else {
                assert!(
                    current <= covered,
                    "the native row-version source is covered by the serving basis"
                );
            }
            native_fixture_page_target(
                &db,
                &partition,
                contextdb_vector::ivf::storage::PageKind::RowVersions,
            )
        }
    };
    let definition = TableDefinition::<&[u8], &[u8]>::new(&table_name);
    native_fixture_cold_cache(&db);
    db.with_native_ivf_fixture_write_for_test(|write| {
        let malformed = vec![0xff; MALFORMED_METADATA_BYTES];
        write
            .open_table(definition)
            .unwrap()
            .insert(malformed_key.as_slice(), malformed.as_slice())
            .unwrap();
        Ok(())
    })
    .unwrap();
    let persistence = Arc::clone(db.persistence.as_ref().unwrap());
    let baseline = db.accountant.usage().used;
    let snapshot_holds = db.snapshot_registry.state.lock().active.len();
    let headroom = MALFORMED_METADATA_BYTES / 2;
    let limit = baseline + headroom;
    let largest_read = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&largest_read);
    db.clear_maintenance_cycle_stamp_for_test();
    let observation = redb::observe_backend_reads(Arc::new(move |bytes| {
        observed.fetch_max(bytes, Ordering::SeqCst);
    }));
    db.accountant.set_budget(Some(limit)).unwrap();
    let cycle = db.run_maintenance_cycle();
    if matches!(
        metadata,
        ColdMetadata::Journal | ColdMetadata::CoveredJournal
    ) {
        assert!(
            matches!(&cycle, Err(Error::StoreCorrupted { .. })),
            "persistent damage to the native journal must remain a typed corruption refusal: {cycle:?}"
        );
        drop(cycle);
    } else {
        let report = cycle.unwrap();
        let details = report
            .vector
            .first_failure_details
            .expect("the public cycle exposes the deferred physical owner");
        assert_eq!(details.failure, VectorMaintenanceFailure::MemoryLimit);
        assert_eq!(details.operation, "read_page");
        assert!(details.requested_bytes.unwrap() > MALFORMED_METADATA_BYTES as u64);
        assert!(details.available_bytes.unwrap() <= headroom as u64);
        assert_eq!(details.budget_limit_bytes, Some(limit as u64));
        assert_eq!(report.vector.built_partitions, 0);
    }
    assert!(db.maintenance_cycle_stamp_is_set_for_test());
    assert!(largest_read.load(Ordering::SeqCst) < MALFORMED_METADATA_BYTES);
    drop(observation);
    assert_eq!(db.accountant.usage().used, baseline);
    assert_eq!(
        db.snapshot_registry.state.lock().active.len(),
        snapshot_holds
    );
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_eq!(db.accountant.underflow_count_for_test(), 0);
    db.accountant.set_budget(None).unwrap();
    persistence
        .with_db(|source| {
            let read = source.begin_read().unwrap();
            let table = read.open_table(definition).unwrap();
            let value = table.get(malformed_key.as_slice()).unwrap().unwrap();
            assert_eq!(value.value().len(), MALFORMED_METADATA_BYTES);
            assert!(
                value.value().iter().all(|byte| *byte == 0xff),
                "a resource refusal does not erase or rewrite unknown metadata"
            );
            Ok(())
        })
        .unwrap();
    // Restore only the deliberately damaged private input, then reopen its
    // real writer so the final public control borrows the current real root.
    db.with_native_ivf_fixture_write_for_test(|write| {
        write
            .open_table(definition)
            .unwrap()
            .insert(malformed_key.as_slice(), original_value.as_slice())
            .unwrap();
        Ok(())
    })
    .unwrap();
    drop(persistence);
    db.close().unwrap();
    drop(db);
    let db = native_fixture_open(&path, None);
    assert_eq!(native_fixture_serving(&db, &partition), Some(serving));
    assert_eq!(native_fixture_control(&db, &partition), original_catalog);
    assert!(db.native_ivf_catalog_for_test(&partition.index, &partition.partition_key).unwrap().is_some_and(|catalog| catalog.unserviceable.is_none() && !catalog.legacy_unserviceable));
    assert_eq!(
        db.execute(
            "SELECT id FROM metadata_items ORDER BY v <=> [1,0,0] USE VECTOR INDEXED LIMIT 3",
            &HashMap::new()
        )
        .unwrap()
        .rows()
        .len(),
        3
    );
    db.close().unwrap();
}

#[test]
fn public_cycle_cold_catalog_refusal_keeps_serving_and_closes() {
    // The raw codec fixture owns this explicitly declared operation.
    let _fixture_decode_scope = crate::read_image_memory::ImageMemory::enter_writer_operation(
        crate::memory_accounting::AccountantHandle::standalone(None, true).unwrap(),
    )
    .unwrap();

    public_cold_metadata_refusal(ColdMetadata::Catalog);
}
#[test]
fn public_cycle_cold_journal_refusal_keeps_serving_and_closes() {
    public_cold_metadata_refusal(ColdMetadata::Journal);
}
#[test]
fn public_cycle_cold_covered_cleanup_refusal_keeps_serving_and_closes() {
    // The raw codec fixture owns this explicitly declared operation.
    let _fixture_decode_scope = crate::read_image_memory::ImageMemory::enter_writer_operation(
        crate::memory_accounting::AccountantHandle::standalone(None, true).unwrap(),
    )
    .unwrap();

    public_cold_metadata_refusal(ColdMetadata::CoveredJournal);
}

type RouteOutputHookForTest =
    Box<dyn FnMut(usize, usize, usize, &mut dyn FnMut() -> Result<()>) -> Result<()>>;
thread_local! {
    static ROUTE_OUTPUT_HOOK_FOR_TEST: std::cell::RefCell<Option<RouteOutputHookForTest>> =
        const { std::cell::RefCell::new(None) };
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn with_route_output_for_test(
    output_len: usize,
    output_pointer: usize,
    output_capacity: usize,
    action: impl FnOnce() -> Result<()>,
) -> Result<()> {
    let hook = ROUTE_OUTPUT_HOOK_FOR_TEST.with(|slot| slot.borrow_mut().take());
    let Some(mut hook) = hook else {
        return action();
    };
    let mut pending = Some(action);
    let result = hook(output_len, output_pointer, output_capacity, &mut || {
        pending
            .take()
            .expect("the real route-output action runs once")()
    });
    assert!(
        pending.is_none(),
        "the observation runs the real output action"
    );
    ROUTE_OUTPUT_HOOK_FOR_TEST.with(|slot| *slot.borrow_mut() = Some(hook));
    result
}

struct RouteOutputHookScopeForTest;
impl Drop for RouteOutputHookScopeForTest {
    fn drop(&mut self) {
        ROUTE_OUTPUT_HOOK_FOR_TEST.with(|slot| *slot.borrow_mut() = None);
        crate::persistence::content_identity::proof_allocation_oracle_tests::disarm_source_free_watch();
    }
}

#[test]
#[ignore = "the vector index route is switched off in this release"]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn native_route_output_refusal_releases_partial_output_preserves_serving_and_closes() {
    if crate::test_process::rerun_alone(
        module_path!(),
        "native_route_output_refusal_releases_partial_output_preserves_serving_and_closes",
        &[],
    ) {
        return;
    }
    use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
    use crate::vector_read_memory::{DeclaredReadMemory, statement_child_proof};
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let directory = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(
        directory.path().join("native-route-output.db"),
        DatabaseOpenOptions {
            memory_limit: Some(64 * 1024 * 1024),
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    let names = ["route_first", "route_second"];
    let routes = names.map(|table| {
        VectorPartitionRef::new(
            VectorIndexRef::new(table, "v"),
            VectorPartitionKey::unpartitioned(),
        )
    });
    for table in names {
        db.execute(
            &format!("CREATE TABLE {table} (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED SEARCH (TARGET_ACCURACY = 95, EXPECTED_VECTORS = 2) IVF (LISTS = 1, PROBES = 1, REFINE_FACTOR = 1.00) WITH (write_buffer_bytes = 4096, training_samples = 2, training_iterations = 1, calibration_samples = 1, calibration_depths = [1]))"),
            &HashMap::new(),
        ).unwrap();
        db.execute(
            &format!("INSERT INTO {table} VALUES (0, '[1,0,0]'), (1, '[0,1,0]')"),
            &HashMap::new(),
        )
        .unwrap();
    }
    let bound: usize = routes
        .iter()
        .map(|route| {
            let resolved = db
                .native_ivf_resolved_policy_for_test(&route.index, &route.partition_key)
                .unwrap();
            let rows = 2usize.max(resolved.population as usize);
            let ingest = rows.div_ceil(resolved.training.ingest_batch_keys.max(1));
            let phase = rows.div_ceil(resolved.training.phase_batch_entries.max(1));
            ingest + (phase + 1) * (resolved.training.iterations as usize + 4) + rows + 6
        })
        .sum();
    let mut ready = false;
    for _ in 0..bound {
        let report = db.run_maintenance_cycle().unwrap().vector;
        assert!(
            report.first_failure.is_none(),
            "native setup refused: {report:?}"
        );
        if report.ready_partitions == routes.len() && report.remaining_partitions == 0 {
            ready = true;
            break;
        }
    }
    assert!(
        ready,
        "declared native phases converge within their derived bound"
    );
    let descriptors = routes.each_ref().map(|route| {
        db.native_ivf_serving_descriptor_for_test(&route.index, &route.partition_key)
            .unwrap()
            .unwrap()
            .bytes
    });
    let assert_serving = || {
        for (index, table) in names.iter().enumerate() {
            let reader = db
                .read_session_with_clock_for_test(
                    contextdb_core::read_contract::ReadLimits::default(),
                    Arc::new(crate::local_transport::ManualDeadlineClock::at(0)),
                )
                .unwrap();
            let answer = reader
                .execute(
                    &format!(
                        "SELECT id FROM {table} ORDER BY v <=> [1,0,0] USE VECTOR INDEXED LIMIT 2"
                    ),
                    &HashMap::new(),
                )
                .unwrap();
            assert_eq!(answer.rows().len(), 2);
            assert!(answer.rows().contains(&vec![Value::Int64(0)]));
            assert!(answer.rows().contains(&vec![Value::Int64(1)]));
            assert_eq!(
                db.native_ivf_serving_descriptor_for_test(
                    &routes[index].index,
                    &routes[index].partition_key
                )
                .unwrap()
                .unwrap()
                .bytes,
                descriptors[index],
            );
        }
    };
    assert_serving();
    let parent = DeclaredReadMemory::from_reader_limit(64 * 1024 * 1024).unwrap();
    let memory = parent.child().unwrap();
    let baseline = parent.retained_bytes_for_test();
    let state = std::rc::Rc::new(std::cell::RefCell::new((0usize, false, false, 0usize)));
    let observed = std::rc::Rc::clone(&state);
    let narrowed = parent.clone();
    let hook: RouteOutputHookForTest = Box::new(move |len, pointer, capacity, action| {
        observed.borrow_mut().0 += 1;
        if len == 0 || observed.borrow().1 {
            return action();
        }
        assert!(
            capacity >= len && pointer != 0,
            "the prefix is a real live output Vec"
        );
        observed.borrow_mut().1 = true;
        observed.borrow_mut().3 = len;
        oracle::watch_exact_free_for_test(
            (
                pointer,
                capacity * std::mem::size_of::<VectorPartitionRef>(),
                std::mem::align_of::<VectorPartitionRef>(),
            ),
            0,
        );
        let current = narrowed.retained_bytes_for_test();
        let (result, _) = statement_child_proof::observe(&narrowed, current, action);
        observed.borrow_mut().2 = result.as_ref().err().is_some_and(|error| {
            VectorMaintenanceFailureDetails::from_error(error).failure
                == VectorMaintenanceFailure::MemoryLimit
        });
        result
    });
    ROUTE_OUTPUT_HOOK_FOR_TEST.with(|slot| {
        assert!(slot.borrow_mut().replace(hook).is_none());
    });
    let _hook_scope = RouteOutputHookScopeForTest;
    db.clear_maintenance_cycle_stamp_for_test();
    let outcome = db.run_maintenance_cycle_with_read_memory(memory.clone());
    assert!(
        state.borrow().0 >= 2 && state.borrow().1,
        "the real partial-output admission event is delivered"
    );
    assert!(
        state.borrow().3 > 0,
        "a prior route output stays live at the refused next output"
    );
    assert!(
        state.borrow().2,
        "route output must refuse on its real original reader counter before allocating"
    );
    let report = outcome.unwrap();
    assert_eq!(
        report.vector.first_failure,
        Some(VectorMaintenanceFailure::MemoryLimit)
    );
    assert!(
        db.maintenance_cycle_stamp_is_set_for_test(),
        "refusal still closes the maintenance cycle"
    );
    drop(report);
    assert_eq!(
        oracle::source_free_for_test(1)[0].2,
        1,
        "the actual partial output backing is physically freed once"
    );
    assert_eq!(
        parent.retained_bytes_for_test(),
        baseline,
        "partial output and its inputs release the original caller's credits"
    );
    drop(_hook_scope);
    assert_serving();
    db.close().unwrap();
}

fn native_isolation_fixture() -> (tempfile::TempDir, Database, [VectorPartitionRef; 2]) {
    let directory = tempfile::tempdir().unwrap();
    let db = native_fixture_open(
        &directory.path().join("native-isolation.db"),
        Some(64 * 1024 * 1024),
    );
    db.execute(
        "CREATE TABLE native_items (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 3 SEARCH_MODE INDEXED SEARCH (TARGET_ACCURACY = 95, EXPECTED_VECTORS = 32) IVF (LISTS = 1, PROBES = 1, REFINE_FACTOR = 1.00) WITH (write_buffer_bytes = 4096, training_samples = 16, training_iterations = 1, calibration_samples = 1, calibration_depths = [1]))",
        &HashMap::new(),
    ).unwrap();
    for scope in 0..2 {
        for row in 0..16 {
            db.execute(
                "INSERT INTO native_items VALUES ($id, $scope, '[1,0,0]')",
                &HashMap::from([
                    ("id".into(), Value::Int64(scope * 16 + row)),
                    ("scope".into(), Value::Int64(scope)),
                ]),
            )
            .unwrap();
        }
    }
    let routes = [0, 1].map(|scope| {
        VectorPartitionRef::new(
            VectorIndexRef::new("native_items", "v"),
            VectorPartitionKey::from_values(&[Value::Int64(scope)]).unwrap(),
        )
    });
    native_fixture_ready(&db, &routes, 16);
    (directory, db, routes)
}

struct NativeIsolationQueryObserver {
    tuple: VectorPartitionKey,
    counters: Arc<std::sync::Mutex<(usize, usize, usize)>>,
}

impl crate::vector_ivf_observations::NativeIvfSealObserver for NativeIsolationQueryObserver {
    fn observe(&self, event: crate::vector_ivf_observations::NativeIvfSealEvent<'_>) {
        use crate::vector_ivf_observations::NativeIvfSealEvent;
        match event {
            NativeIvfSealEvent::QueryExecuted {
                index,
                tuple,
                indexed,
                ..
            } => {
                let mut counters = self.counters.lock().unwrap();
                if indexed
                    && index.table == "native_items"
                    && index.column == "v"
                    && *tuple == self.tuple
                {
                    counters.0 += 1;
                } else {
                    counters.1 += 1;
                }
            }
            NativeIvfSealEvent::PayloadWritten { .. } | NativeIvfSealEvent::HeaderSealed { .. } => {
                self.counters.lock().unwrap().2 += 1;
            }
            _ => {}
        }
    }
    fn before_boundary(
        &self,
        _: crate::vector_ivf_observations::NativeIvfFaultBoundary<'_>,
    ) -> Result<()> {
        Ok(())
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn native_isolation_answer(db: &Database, scope: i64) -> Result<Vec<Vec<Value>>> {
    let reader = db.read_session_with_clock_for_test(
        contextdb_core::read_contract::ReadLimits::default(),
        Arc::new(crate::local_transport::ManualDeadlineClock::at(0)),
    )?;
    let counters = Arc::new(std::sync::Mutex::new((0, 0, 0)));
    let observer = Arc::new(NativeIsolationQueryObserver {
        tuple: VectorPartitionKey::from_values(&[Value::Int64(scope)]).unwrap(),
        counters: Arc::clone(&counters),
    });
    let result = crate::vector_ivf_observations::with_native_ivf_seal_observer_for_test(
        observer,
        || {
            reader.execute(
            "SELECT id FROM native_items WHERE scope = $scope ORDER BY v <=> [1,0,0] USE VECTOR INDEXED LIMIT 16",
            &HashMap::from([("scope".into(), Value::Int64(scope))]),
        )
        },
    )?;
    assert_eq!(
        *counters.lock().unwrap(),
        (1, 0, 0),
        "the public query executes the requested native indexed route with no query-time build writes"
    );
    let mut rows = result.rows().to_vec();
    rows.sort_by_key(|row| match row.first() {
        Some(Value::Int64(id)) => *id,
        other => panic!("the indexed public answer keeps its row id: {other:?}"),
    });
    Ok(rows)
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn native_non_route_values_are_not_read_while_healthy_routes_keep_serving() {
    const MALFORMED_METADATA_BYTES: usize = 9 * 1024 * 1024;
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let (directory, db, routes) = native_isolation_fixture();
    let before = routes
        .each_ref()
        .map(|route| native_fixture_serving(&db, route).unwrap());
    let expected = [0, 1].map(|scope| native_isolation_answer(&db, scope).unwrap());
    assert_eq!(
        expected[0],
        (0..16).map(|id| vec![Value::Int64(id)]).collect::<Vec<_>>()
    );
    assert_eq!(
        expected[1],
        (16..32)
            .map(|id| vec![Value::Int64(id)])
            .collect::<Vec<_>>()
    );
    db.with_native_ivf_fixture_write_for_test(|write| {
        let mut controls = write
            .open_table(TableDefinition::<&[u8], &[u8]>::new("ivf_scopes"))
            .unwrap();
        controls
            .insert(
                b"".as_slice(),
                vec![0xff; MALFORMED_METADATA_BYTES].as_slice(),
            )
            .unwrap();
        controls
            .insert(b"\xff".as_slice(), [0xff; 32].as_slice())
            .unwrap();
        Ok(())
    })
    .unwrap();
    db.close().unwrap();
    drop(db);
    let db = native_fixture_open(
        &directory.path().join("native-isolation.db"),
        Some(64 * 1024 * 1024),
    );
    native_fixture_cold_cache(&db);
    let extent_reads = Arc::new(AtomicUsize::new(0));
    let total = Arc::new(AtomicUsize::new(0));
    let observed_extents = Arc::clone(&extent_reads);
    let observed_total = Arc::clone(&total);
    let observation = redb::observe_backend_reads(Arc::new(move |bytes| {
        observed_total.fetch_add(bytes, Ordering::SeqCst);
        if bytes >= MALFORMED_METADATA_BYTES {
            observed_extents.fetch_add(1, Ordering::SeqCst);
        }
    }));
    let report = db.run_maintenance_cycle().unwrap().vector;
    assert!(
        report.first_failure.is_none(),
        "non-route values do not refuse a declared route: {report:?}"
    );
    assert_eq!(report.ready_partitions, 2);
    assert_eq!(report.remaining_partitions, 0);
    for scope in 0..2 {
        assert_eq!(
            native_isolation_answer(&db, scope as i64).unwrap(),
            expected[scope]
        );
        assert_eq!(
            native_fixture_serving(&db, &routes[scope]),
            Some(before[scope])
        );
    }
    drop(observation);
    assert_eq!(
        extent_reads.load(Ordering::SeqCst),
        0,
        "no cold backend read of the orphan value extent"
    );
    assert!(
        total.load(Ordering::SeqCst) < MALFORMED_METADATA_BYTES,
        "all successful backend reads together exclude even one full orphan body read"
    );
    db.close().unwrap();
}

#[test]
#[ignore = "the vector index route is switched off in this release"]
fn public_cycle_small_unknown_invalid_catalog_does_not_quarantine() {
    public_small_unknown_invalid_native();
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn public_small_unknown_invalid_native() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let directory = tempfile::tempdir().unwrap();
    let db = native_fixture_open(
        &directory.path().join("small-invalid-native.db"),
        Some(64 * 1024 * 1024),
    );
    db.execute(
        "CREATE TABLE native_items (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 3 SEARCH_MODE INDEXED SEARCH (TARGET_ACCURACY = 95, EXPECTED_VECTORS = 32) IVF (LISTS = 1, PROBES = 1, REFINE_FACTOR = 1.00) WITH (write_buffer_bytes = 4096, training_samples = 16, training_iterations = 1, calibration_samples = 1, calibration_depths = [1]))",
        &HashMap::new(),
    ).unwrap();
    let routes = [0, 1].map(|scope| {
        VectorPartitionRef::new(
            VectorIndexRef::new("native_items", "v"),
            VectorPartitionKey::from_values(&[Value::Int64(scope)]).unwrap(),
        )
    });
    for scope in 0..2 {
        for row in 0..16 {
            db.execute(
                "INSERT INTO native_items VALUES ($id, $scope, '[1,0,0]')",
                &HashMap::from([
                    ("id".into(), Value::Int64(scope * 16 + row)),
                    ("scope".into(), Value::Int64(scope)),
                ]),
            )
            .unwrap();
        }
        if scope == 0 {
            native_fixture_before_publication(&db, &routes[..1], 16);
            assert_eq!(
                db.run_maintenance_cycle().unwrap().vector.built_partitions,
                1
            );
            native_fixture_ready(&db, &routes[..1], 16);
        }
    }
    let serving = native_fixture_serving(&db, &routes[0]).unwrap();
    native_fixture_before_publication(&db, &routes[1..], 16);
    db.with_native_ivf_fixture_write_for_test(|write| {
        write
            .open_table(TableDefinition::<&[u8], &[u8]>::new("ivf_scopes"))
            .unwrap()
            .insert(b"".as_slice(), [0xff; 32].as_slice())
            .unwrap();
        Ok(())
    })
    .unwrap();
    db.clear_maintenance_cycle_stamp_for_test();
    let report = db.run_maintenance_cycle().unwrap();
    {
        assert!(
            report.vector.built_partitions >= 1,
            "the independent healthy route publishes; report={report:?}"
        );
    }
    assert!(db.maintenance_cycle_stamp_is_set_for_test());
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_eq!(db.accountant.underflow_count_for_test(), 0);
    assert!(report.vector.first_failure.is_none());
    assert_eq!(native_fixture_serving(&db, &routes[0]), Some(serving));
    let catalog = db
        .native_ivf_catalog_for_test(&routes[0].index, &routes[0].partition_key)
        .unwrap()
        .unwrap();
    assert!(catalog.unserviceable.is_none() && !catalog.legacy_unserviceable);
    assert_eq!(
        native_isolation_answer(&db, 0).unwrap(),
        (0..16).map(|id| vec![Value::Int64(id)]).collect::<Vec<_>>()
    );
    assert_eq!(
        native_isolation_answer(&db, 1).unwrap(),
        (16..32)
            .map(|id| vec![Value::Int64(id)])
            .collect::<Vec<_>>()
    );
    db.close().unwrap();
}
