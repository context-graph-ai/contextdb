use super::*;
use crate::direct_file_reader::test_seams::{
    ReadWorkCounters, ReadWorkObservationScope, ReadWorkPhase,
};
use crate::{Database, ReadSession, ReadSessionOptions};
use contextdb_core::{Value, read_contract::ReadRoute};
use std::collections::HashMap;

/// Open authenticates fixed history root controls and the ordered startup
/// digest without loading history proof bodies, rows or vectors.
fn assert_open_proves_bounded_history_and_startup_digest(
    open: ReadWorkCounters,
    case: &str,
    expected_hashed_bytes: u64,
) {
    assert_eq!(open.proof_nodes_read, 0, "{case}: {open:?}");
    assert_eq!(open.proof_nodes_hashed, 0, "{case}: {open:?}");
    assert_eq!(open.ordered_proof_nodes_read, 0, "{case}: {open:?}");
    assert_eq!(
        ReadWorkCounters {
            hashed_bytes: 0,
            proof_nodes_read: 0,
            proof_nodes_hashed: 0,
            ordered_proof_nodes_read: 0,
            ..open
        },
        ReadWorkCounters::default(),
        "{case}"
    );
    assert_eq!(open.hashed_bytes, expected_hashed_bytes, "{case}: {open:?}");
}

// These consumers use the graph sidecar only; legacy default oracles stay exact.
// The closed observation receipt supplies complete returned-key coverage.
// Population events denote whole-table walks; fixed seeks/probes stay separate.
fn assert_graph_no_population_walk(scope: &ReadWorkObservationScope) {
    let work = scope.graph_counters();
    let events = scope.graph_events();
    assert_eq!(
        work.graph_population_walks, 0,
        "open and addressed proof windows never walk a complete graph/history population"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.operation == "population_walk")
            .count(),
        0,
        "actual complete-walk events must agree with the graph sidecar"
    );
}

fn graph_retained_zero_touch_keys(before: &GraphPublicationState) -> Vec<(&'static str, Vec<u8>)> {
    // Decode and derive fixture identities before entering Mutation.
    let mut retained = Vec::new();
    for (key, bytes) in &before.1[0] {
        retained.push(("history", key.as_bytes().to_vec()));
        let position = RedbPersistence::numeric_change_key(key).unwrap();
        retained.push(("history", position.to_vec()));
        let entry = RedbPersistence::decode_stored_change_log(bytes)
            .unwrap()
            .entry;
        if let Some(selector) = RedbPersistence::log_table_selector(&entry, &position) {
            retained.push(("change_log_by_table", selector));
        }
    }
    for (key, _) in &before.0[2] {
        if key.get(17) == Some(&1) {
            retained.push(("graph_deletion", key.clone()));
        }
    }
    for (key, _) in &before.0[3] {
        retained.push(("graph_location", key.clone()));
    }
    retained
}

fn assert_graph_retained_zero_touch(
    scope: &ReadWorkObservationScope,
    retained: &[(&str, Vec<u8>)],
) {
    assert!(retained.iter().any(|(family, _)| *family == "history"));
    assert!(
        retained
            .iter()
            .any(|(family, _)| *family == "graph_location")
    );
    assert!(
        retained
            .iter()
            .any(|(family, _)| *family == "graph_deletion")
    );
    let events = scope.graph_events();
    assert!(
        events
            .iter()
            .filter(|event| matches!(
                event.operation.as_str(),
                "get" | "insert" | "remove" | "iteration_step"
            ))
            .all(|event| !retained
                .iter()
                .any(|(family, key)| event.family == *family && event.key == *key)),
        "pure predecessor retirement must never read, write or visit an exact retained history/T/deletion-C key"
    );
}

fn assert_graph_context(scope: &ReadWorkObservationScope, phase: ReadWorkPhase) {
    let events = scope.graph_events();
    assert!(
        !events.is_empty(),
        "an absent observation cannot prove zero work"
    );
    assert!(events.iter().all(|event| event.phase == phase));
    assert!(
        events
            .iter()
            .all(|event| event.operation_id.as_deref() == Some("graph-observer-row"))
    );
    assert!(
        events
            .iter()
            .any(|event| event.observation_status == "OBSERVED" && event.captured_root.is_some())
    );
    assert!(
        events
            .iter()
            .filter(|event| (event.family.starts_with("graph_")
                || matches!(event.family.as_str(), "commit_index" | "history"))
                && matches!(
                    event.operation.as_str(),
                    "get" | "remove" | "insert" | "maximum_seek"
                ))
            .all(|event| event.observation_status == "OBSERVED" && event.captured_root.is_some())
    );
}

fn assert_graph_open_work(
    scope: &ReadWorkObservationScope,
    expected_root: &str,
) -> crate::read_work_probe::GraphReadWorkCounters {
    assert_graph_context(scope, ReadWorkPhase::Open);
    assert_graph_no_population_walk(scope);
    let work = scope.graph_counters();
    assert_eq!(work.graph_payload_decodes, 0);
    assert_eq!(work.graph_private_history_decodes, 0);
    let events = scope.graph_events();
    let roots = events
        .iter()
        .filter(|event| {
            event.operation == "root"
                && matches!(
                    event.family.as_str(),
                    "graph_fwd"
                        | "graph_rev"
                        | "graph_contribution"
                        | "graph_location"
                        | "graph_lifecycle"
                )
        })
        .collect::<Vec<_>>();
    assert!(
        roots
            .iter()
            .all(|event| event.captured_root.as_deref() == Some(expected_root)),
        "the descriptors belong to the actual captured source transaction"
    );
    assert_eq!(
        roots.len(),
        5,
        "each logical validation acquires five graph-family descriptors"
    );
    assert_eq!(work.graph_root_reads, roots.len() as u64);
    for family in [
        "graph_fwd",
        "graph_rev",
        "graph_contribution",
        "graph_location",
        "graph_lifecycle",
    ] {
        let descriptors = roots
            .iter()
            .filter(|event| event.family == family)
            .collect::<Vec<_>>();
        assert_eq!(descriptors.len(), 1);
        assert!(
            descriptors[0]
                .acquired_root
                .as_ref()
                .is_some_and(|root| !root.is_empty())
        );
        assert_eq!(descriptors[0].captured_root, roots[0].captured_root);
    }
    work
}

fn assert_graph_addressed_work(
    scope: &ReadWorkObservationScope,
    bound: u64,
) -> crate::read_work_probe::GraphReadWorkCounters {
    assert_graph_context(scope, ReadWorkPhase::Query);
    assert_graph_no_population_walk(scope);
    let work = scope.graph_counters();
    assert!(work.graph_addressed_gets > 0);
    assert!(
        work.graph_addressed_gets <= bound,
        "additional addressed proof work: {work:?}"
    );
    assert_eq!(work.graph_maximum_seeks, 1);
    assert!(work.graph_commit_gets > 0);
    let events = scope.graph_events();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.operation == "maximum_seek")
            .count() as u64,
        work.graph_maximum_seeks
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.family == "commit_index" && event.operation == "get")
            .count() as u64,
        work.graph_commit_gets
    );
    assert!(events.iter().any(|event| event.family == "commit_index"
        && event.role.as_deref() == Some("requested_own")
        && event.queried_lsn.is_some()));
    assert!(events.iter().any(|event| event.family == "commit_index"
        && event.role.as_deref() == Some("maximum_source")
        && event.queried_lsn.is_some()));
    // Full-key iterator coverage is unfinished. No zero-population assertion.
    // The pinned final receipt closes that historical gap; the shared assertion above now applies.
    work
}

// The supplied history bytes are fetched before Query; full-history answers
// remain in the original helper after these isolated proof windows finish.
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn graph_addressed_observer_row(
    root: &graph_read::GraphReadRoot,
    predecessor: &AdjEntry,
    present: bool,
    physical: Lsn,
    ordinal: usize,
    expected: &ChangeLogEntry,
) -> Vec<crate::read_work_probe::GraphReadWorkCounters> {
    use std::ops::Bound;
    use writer_source::SourceWindowOwner;
    let memory = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
        16 * 1024 * 1024,
    ));
    let mut result = Vec::new();
    let expected_root = match root {
        graph_read::GraphReadRoot::Writer(root) => format!("{:?}", root.storage_identity()),
        graph_read::GraphReadRoot::Image(source) => format!("{:?}", source.storage_identity()),
    };
    if present {
        for reverse in [false, true] {
            let key = if reverse {
                RedbPersistence::graph_rev_key(predecessor)
            } else {
                RedbPersistence::graph_fwd_key(predecessor)
            };
            let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
            scope.set_operation_id("graph-observer-row");
            // This direct observation starts after capture. Bind the actual
            // pinned source before the cursor records its first range event.
            match root {
                graph_read::GraphReadRoot::Writer(source) => {
                    crate::read_work_probe::bind_graph_root(source.storage_identity())
                }
                graph_read::GraphReadRoot::Image(source) => {
                    crate::read_work_probe::bind_graph_root(source.storage_identity())
                }
            }
            let mut cursor = root
                .cursor(
                    reverse,
                    Bound::Included(&key),
                    Bound::Included(&key),
                    memory.clone(),
                )
                .unwrap();
            assert_eq!(
                cursor.next_edge_with(&|| Ok(())).unwrap().unwrap().edge(),
                predecessor
            );
            assert!(cursor.next_edge_with(&|| Ok(())).unwrap().is_none());
            result.push(assert_graph_addressed_work(&scope, 7));
            assert!(
                scope
                    .graph_events()
                    .iter()
                    .filter(|event| event.observation_status == "OBSERVED")
                    .all(|event| event.captured_root.as_deref() == Some(expected_root.as_str()))
            );
            assert!(scope.graph_counters().graph_payload_decodes > 0);
            drop(cursor);
            drop(scope);
        }
    }
    let mut key = String::new();
    RedbPersistence::write_change_log_entry_key(physical, ordinal, &mut key);
    let bytes = match root {
        graph_read::GraphReadRoot::Writer(root) => {
            let read = root.owner_read().unwrap();
            let table = read.open_table(CHANGE_LOG_TABLE).unwrap();
            let value = table.get(key.as_str()).unwrap().unwrap();
            value.value().to_vec()
        }
        graph_read::GraphReadRoot::Image(source) => source
            .with_read(|read| {
                let table = read.open_table(CHANGE_LOG_TABLE).unwrap();
                let value = table.get(key.as_str()).unwrap().unwrap();
                Ok(value.value().to_vec())
            })
            .unwrap(),
    };
    let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    scope.set_operation_id("graph-observer-row");
    match root {
        graph_read::GraphReadRoot::Writer(root) => root
            .verify_stored_graph_history_at_root(
                &key,
                &bytes,
                &SourceWindowOwner::Read(memory.clone().into()),
            )
            .unwrap(),
        graph_read::GraphReadRoot::Image(source) => source
            .verify_stored_graph_history(&key, &bytes, memory)
            .unwrap(),
    }
    let work = assert_graph_addressed_work(&scope, if present { 12 } else { 10 });
    assert!(
        scope
            .graph_events()
            .iter()
            .filter(|event| event.observation_status == "OBSERVED")
            .all(|event| event.captured_root.as_deref() == Some(expected_root.as_str()))
    );
    assert_eq!(work.graph_supplied_primary_reads, 1);
    assert!(work.graph_private_history_decodes > 0);
    let events = scope.graph_events();
    assert!(
        events
            .iter()
            .filter(|event| event.family == "commit_index")
            .all(|event| event.queried_lsn != Some(physical.0)),
        "physical storage coordinates never invent public commits"
    );
    assert!(events.iter().any(|event| event.family == "commit_index"
        && event.queried_lsn == Some(expected.lsn().0)
        && event.role.as_deref() == Some("requested_own")));
    result.push(work);
    result
}

fn graph_retirement_observer(before: &GraphPublicationState) -> ReadWorkObservationScope {
    // Decode fixture bytes and derive every exact storage key before Mutation.
    let mut retained = Vec::new();
    for (key, bytes) in &before.1[0] {
        retained.push(("history", key.as_bytes().to_vec()));
        let position = RedbPersistence::numeric_change_key(key).unwrap();
        retained.push(("history", position.to_vec()));
        let entry = RedbPersistence::decode_stored_change_log(bytes)
            .unwrap()
            .entry;
        if let Some(selector) = RedbPersistence::log_table_selector(&entry, &position) {
            retained.push(("change_log_by_table", selector));
        }
    }
    for (key, _) in &before.0[2] {
        retained.push((
            if key.get(17) == Some(&1) {
                "graph_deletion"
            } else {
                "graph_creation"
            },
            key.clone(),
        ));
    }
    for (key, _) in &before.0[3] {
        retained.push(("graph_location", key.clone()));
    }
    let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Mutation);
    scope.set_operation_id("graph-observer-row");
    for (family, key) in retained {
        scope.retain_graph_key(family, &key);
    }
    scope
}

fn assert_graph_retirement_work(
    scope: &ReadWorkObservationScope,
    predecessor: &AdjEntry,
    repack: bool,
) -> [u64; 4] {
    assert_graph_context(scope, ReadWorkPhase::Mutation);
    let work = scope.graph_counters();
    let events = scope.graph_events();
    let p = RedbPersistence::graph_fwd_key(predecessor);
    let reverse = RedbPersistence::graph_rev_key(predecessor);
    let creation = graph_history_record::creation_contribution_key(&p).unwrap();
    let lifecycle = graph_history_record::lifecycle_key(&p).unwrap();
    for (family, operation, key) in [
        ("graph_fwd", "remove", &p),
        ("graph_rev", "remove", &reverse),
        ("graph_creation", "remove", &creation),
    ] {
        assert_eq!(
            events
                .iter()
                .filter(|event| event.family == family
                    && event.operation == operation
                    && event.key == *key)
                .count(),
            1,
            "exact selected-key attempted operation"
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.family == family && event.operation == operation)
                .count(),
            1,
            "no additional same-family removals or receipt rewrites"
        );
    }
    let rewrites = events
        .iter()
        .filter(|event| event.family == "graph_lifecycle" && event.operation == "insert")
        .collect::<Vec<_>>();
    assert!(rewrites.iter().all(|event| event.key == lifecycle));
    if repack {
        assert!(
            !rewrites.is_empty(),
            "repack reinstalls witnesses before final retirement"
        );
    } else {
        assert_eq!(
            rewrites.len(),
            1,
            "retirement alone rewrites one lifecycle receipt"
        );
    }
    assert_eq!(work.graph_publication_commits, 1);
    assert_eq!(
        events
            .iter()
            .filter(|event| event.family == "publication" && event.operation == "committed")
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.family == "publication" && event.operation == "aborted")
            .count(),
        0
    );
    assert!(events.iter().any(|event| event.family == "publication"
        && event.operation == "begin"
        && event.captured_root.is_some()));
    // Retained keys are registered, but zero-touch counters are NOT asserted
    // until the source receipt pins complete per-key iterator coverage.
    // Historical gap above is closed by the pinned final source receipt.
    if !repack {
        assert_eq!(
            work.graph_survivor_history_touches, 0,
            "pure retirement never touches retained history keys"
        );
        assert_eq!(
            work.graph_survivor_location_touches, 0,
            "pure retirement never touches retained T keys"
        );
        assert_eq!(
            work.graph_survivor_deletion_contribution_touches, 0,
            "pure retirement never touches retained deletion C keys"
        );
    }
    [
        work.graph_forward_mutations,
        work.graph_reverse_mutations,
        work.graph_creation_contribution_mutations,
        work.graph_lifecycle_mutations,
    ]
}

// Samples the existing worker kernel before its first graph source cursor
// is opened. Keep capture receipts and worker installations as they were
// at this boundary, before the actual query is allowed to load edges.
#[cfg(any(test, feature = "test-seams"))]
#[derive(Default)]
struct OwnerGraphSnapshotBoundaryObserver {
    scope: std::sync::Mutex<Option<Arc<ReadWorkObservationScope>>>,
    snapshots: std::sync::Mutex<Vec<SnapshotId>>,
    first_edge: std::sync::Mutex<Option<crate::read_work_probe::GraphReadWorkCounters>>,
    capture_events: std::sync::Mutex<Vec<crate::read_work_probe::GraphWorkEvent>>,
    capture_workers: std::sync::Mutex<Vec<crate::read_work_probe::GraphWorkerInstallation>>,
    capture_pages: std::sync::Mutex<Option<(u64, u64)>>,
    capture_page_owners: std::sync::Mutex<Option<std::collections::BTreeMap<String, (u64, u64)>>>,
    first_source: std::sync::Mutex<Option<crate::read_session::ReadKernelSourceEvent>>,
}

#[cfg(any(test, feature = "test-seams"))]
impl crate::read_session::ReadKernelTestObserver for OwnerGraphSnapshotBoundaryObserver {
    fn registered_snapshot_for_test(
        &self,
        route: ReadRoute,
        operation: crate::read_session::ReadSessionOperation,
        snapshot: SnapshotId,
    ) {
        if self.scope.lock().unwrap().is_some() {
            assert_eq!(route, ReadRoute::Owner);
            assert_eq!(
                operation,
                crate::read_session::ReadSessionOperation::Execute
            );
            self.snapshots.lock().unwrap().push(snapshot);
        }
    }
    fn before_source_touch(
        &self,
        event: crate::read_session::ReadKernelSourceEvent,
        _: contextdb_core::read_contract::OwnerReadCancellation,
    ) {
        if event.source != crate::read_session::ReadKernelSource::GraphEdge {
            return;
        }
        let installed = self.scope.lock().unwrap();
        let Some(scope) = installed.as_ref() else {
            return;
        };
        let mut first = self.first_edge.lock().unwrap();
        if first.is_none() {
            assert_eq!(event.route, ReadRoute::Owner);
            assert_eq!(
                event.operation,
                crate::read_session::ReadSessionOperation::Execute
            );
            assert_eq!(scope.phase(), ReadWorkPhase::Query);
            assert_eq!(
                self.snapshots.lock().unwrap().len(),
                1,
                "the statement has registered its real snapshot before graph capture reaches its first source touch"
            );
            *self.first_source.lock().unwrap() = Some(event);
            let page_owners = scope
                .owner_page_admission_by_owner()
                .expect("the executing query must expose its actual per-capability admission map");
            let capture = scope
                .graph_events()
                .into_iter()
                .find(|event| {
                    event.family == "graph_snapshot" && event.operation == "SNAPSHOT_CAPTURE"
                })
                .expect("the first edge callback follows the real graph capture");
            let capture_owner = capture
                .funding_owner
                .as_deref()
                .expect("the actual capture names its funding capability");
            let named_pages = page_owners.get(capture_owner).expect("the cold first graph capture must admit pages on its named capability before opening the edge cursor");
            assert!(
                named_pages.0 > 0,
                "the first GraphEdge callback must already observe a page admitted on its named query owner"
            );
            assert!(
                named_pages.1 > 0,
                "nonzero bytes on the named query owner prove its actual page callback ran before the edge cursor"
            );
            *self.capture_page_owners.lock().unwrap() = Some(page_owners);
            *first = Some(
                scope
                    .owner_graph_counters()
                    .expect("the executing graph worker must install the owner binding"),
            );
            *self.capture_events.lock().unwrap() = scope.graph_events();
            *self.capture_workers.lock().unwrap() = scope.owner_worker_paths();
            *self.capture_pages.lock().unwrap() = Some(
                scope
                    .owner_page_admission()
                    .expect("the installed graph worker must expose its actual page admissions"),
            );
        }
    }
    fn cancellation_observed(
        &self,
        _: crate::read_session::ReadKernelCancellationEvent,
        _: contextdb_core::read_contract::OwnerReadCancellation,
    ) {
    }
}

#[cfg(any(test, feature = "test-seams"))]
fn assert_public_owner_open_through_graph_capture_row(directory: &Path) {
    let path = directory.join("graph-open-capture-owner.db");
    let observer = Arc::new(OwnerGraphSnapshotBoundaryObserver::default());
    // This block owns every handle used to author the fixture. No source,
    // graph reader, transaction, or writing Database survives its end.
    let (owner_reads, declared_reader_limit) = {
        let database = Database::open_with_options(
            &path,
            crate::DatabaseOpenOptions {
                maintenance_policy: crate::database::MaintenancePolicy::CallerDriven,
                test_kernel_observer: Some(observer.clone()),
                ..Default::default()
            },
        )
        .unwrap();
        database
            .execute(
                "CREATE TABLE nodes (id UUID PRIMARY KEY) SYNC OFF",
                &HashMap::new(),
            )
            .unwrap();
        database.execute("CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) SYNC OFF", &HashMap::new()).unwrap();
        for id in [1, 2] {
            database
                .execute(
                    "INSERT INTO nodes VALUES ($id)",
                    &HashMap::from([("id".into(), Value::Uuid(uuid::Uuid::from_u128(id)))]),
                )
                .unwrap();
        }
        let tx = database.begin_or_panic();
        database
            .insert_edge(
                tx,
                uuid::Uuid::from_u128(1),
                uuid::Uuid::from_u128(2),
                "LINKS".into(),
                HashMap::new(),
            )
            .unwrap();
        database.commit(tx).unwrap();
        // Preserve the exact reader declaration used to write this fixture.
        let owner_reads = database.owner_read_config().clone();
        let declared_reader_limit = owner_reads.limits.limits.memory;
        let written_edges = database.execute("SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $source COLUMNS (b.id AS t))", &HashMap::from([("source".into(), Value::Uuid(uuid::Uuid::from_u128(1)))])).unwrap();
        assert_eq!(
            written_edges.rows(),
            vec![vec![Value::Uuid(uuid::Uuid::from_u128(2))]],
            "the store has the authored edge before every writing handle is dropped"
        );
        database.close().unwrap();
        drop(database);
        (owner_reads, declared_reader_limit)
    };
    assert!(
        path.is_file(),
        "reopen the already-written task store, never an empty replacement"
    );
    let database = Database::open_with_options(
        &path,
        crate::DatabaseOpenOptions {
            owner_reads,
            maintenance_policy: crate::database::MaintenancePolicy::CallerDriven,
            test_kernel_observer: Some(observer.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        database.owner_read_config().limits.limits.memory,
        declared_reader_limit,
        "reopen preserves the fixture's existing declared reader limit"
    );
    // Tuple fields are read hits, read misses, evictions, resident bytes.
    let reopened_cache_bytes = database.__writer_cache_stats_for_test().unwrap().3;
    // The live writer read transaction pins one native root page and its control.
    assert!(
        reopened_cache_bytes <= 4096 + 112,
        "the reopened writer retains at most one native root page plus its control before public owner open: {reopened_cache_bytes}"
    );
    let owner_reads = database.owner_read_config().clone();
    database.close().unwrap();
    drop(database);
    // Double the committed node population, then release every writing handle.
    let growing = Database::open_with_options(
        &path,
        crate::DatabaseOpenOptions {
            owner_reads: owner_reads.clone(),
            maintenance_policy: crate::database::MaintenancePolicy::CallerDriven,
            test_kernel_observer: Some(observer.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    for id in [3, 4] {
        growing
            .execute(
                "INSERT INTO nodes VALUES ($id)",
                &HashMap::from([("id".into(), Value::Uuid(uuid::Uuid::from_u128(id)))]),
            )
            .unwrap();
    }
    growing.close().unwrap();
    drop(growing);
    let database = Database::open_with_options(
        &path,
        crate::DatabaseOpenOptions {
            owner_reads,
            maintenance_policy: crate::database::MaintenancePolicy::CallerDriven,
            test_kernel_observer: Some(observer.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        database.__writer_cache_stats_for_test().unwrap().3,
        reopened_cache_bytes,
        "the pinned writer root cache is identical after reopening two-node and four-node stores"
    );
    let scope = Arc::new(ReadWorkObservationScope::enter(ReadWorkPhase::Open));
    let binding = scope.bind_owner(&database, "owner-open-graph-capture-row");
    *observer.scope.lock().unwrap() = Some(scope.clone());
    let session = ReadSession::open_owner_only(&path, ReadSessionOptions::default()).unwrap();
    assert_eq!(session.route(), ReadRoute::Owner);
    let opened = scope
        .owner_graph_counters()
        .expect("public owner open must reach an installed executing worker");
    assert_eq!(opened.graph_payload_decodes, 0);
    assert_eq!(opened.graph_private_history_decodes, 0);
    assert_eq!(opened.graph_range_reads, 0);
    assert_eq!(opened.graph_iteration_steps, 0);
    assert!(opened.graph_root_reads <= 5);
    assert_eq!(opened.graph_population_walks, 0);
    assert_eq!(opened.graph_snapshots_constructed, 0);
    assert_eq!(
        scope
            .owner_page_admission()
            .expect("handshake and status workers must be observed"),
        (0, 0),
        "handshake and status paths admit no storage pages"
    );
    assert!(
        scope
            .owner_page_admission_by_owner()
            .expect("installed open workers expose an actual empty admission map")
            .is_empty()
    );
    let open_workers = scope.owner_worker_paths();
    let handshake = open_workers
        .iter()
        .find(|worker| worker.path == "handshake")
        .expect("owner-only open must observe the authenticated handshake separately");
    let status = open_workers
        .iter()
        .find(|worker| worker.path == "status")
        .expect("owner-only open must observe its status request separately");
    assert!(!handshake.worker_request_id.is_empty());
    assert!(!status.worker_request_id.is_empty());
    assert_ne!(handshake.worker_request_id, status.worker_request_id);
    assert!(
        !open_workers.iter().any(|worker| worker.path == "query"),
        "the first graph query has not run at public open completion"
    );
    let (connection, handshake_ordinal) = handshake
        .worker_request_id
        .rsplit_once(':')
        .expect("the actual handshake identity includes its connection and ordinal");
    assert_eq!(handshake_ordinal, "0");
    assert_eq!(
        status
            .worker_request_id
            .rsplit_once(':')
            .expect("the actual status identity includes its connection and ordinal")
            .0,
        connection
    );
    // Handshake/status have completed without adding cache warmth;
    // the first graph query retains the accepted fresh-writer pinned root.
    let cache_before_query = database.__writer_cache_stats_for_test().unwrap();
    assert_eq!(
        cache_before_query.3, reopened_cache_bytes,
        "public owner open adds no cache warmth: resident bytes equal the accepted fresh-writer baseline, with the pinned root preserved"
    );
    *observer.scope.lock().unwrap() = None;
    drop(binding);
    drop(scope);
    let scope = Arc::new(ReadWorkObservationScope::enter(ReadWorkPhase::Query));
    let binding = scope.bind_owner(&database, "owner-open-graph-capture-row");
    *observer.scope.lock().unwrap() = Some(scope.clone());
    assert!(
        scope.owner_graph_counters().is_none(),
        "a caller query scope alone is UNOBSERVED until its worker installs"
    );
    // The first real query samples inside the executing kernel before its
    // edge cursor opens, then continues to the actual target as its control.
    let actual_edges = session.execute("SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $source COLUMNS (b.id AS t))", &HashMap::from([("source".into(), Value::Uuid(uuid::Uuid::from_u128(1)))])).unwrap();
    let captured =
        observer.first_edge.lock().unwrap().expect(
            "the first real graph query must reach the source callback after graph capture",
        );
    assert_eq!(
        captured.graph_payload_decodes, 0,
        "open plus first graph capture has not decoded any edge"
    );
    assert_eq!(captured.graph_private_history_decodes, 0);
    assert_eq!(
        captured.graph_range_reads, 0,
        "the graph source cursor has not loaded an edge window"
    );
    assert_eq!(captured.graph_iteration_steps, 0);
    assert_eq!(captured.graph_population_walks, 0);
    assert!(captured.graph_root_reads <= 5);
    assert_eq!(
        captured.graph_snapshots_constructed, 1,
        "the first real query captured one graph storage root"
    );
    assert_eq!(
        captured.graph_descriptors_at_last_capture,
        captured.graph_root_reads
    );
    let capture_events = observer.capture_events.lock().unwrap();
    let captures = capture_events
        .iter()
        .filter(|event| event.family == "graph_snapshot" && event.operation == "SNAPSHOT_CAPTURE")
        .collect::<Vec<_>>();
    assert_eq!(
        captures.len(),
        1,
        "a counter alone cannot certify storage-root capture"
    );
    let capture = captures[0];
    let capture_root = capture
        .captured_root
        .as_deref()
        .expect("storage identity must be published at graph snapshot construction");
    assert!(!capture_root.is_empty());
    assert_eq!(capture.observation_status, "OBSERVED");
    assert_eq!(capture.phase, ReadWorkPhase::Query);
    assert_eq!(
        capture.operation_id.as_deref(),
        Some("owner-open-graph-capture-row")
    );
    let capture_workers = observer.capture_workers.lock().unwrap();
    let query = capture_workers
        .iter()
        .find(|worker| worker.path == "query")
        .expect("the executing first graph query must have a separate installation receipt");
    assert!(!query.worker_request_id.is_empty());
    assert_eq!(
        capture.worker_request_id.as_deref(),
        Some(query.worker_request_id.as_str())
    );
    assert_ne!(query.worker_request_id, handshake.worker_request_id);
    assert_ne!(query.worker_request_id, status.worker_request_id);
    assert_eq!(
        query
            .worker_request_id
            .rsplit_once(':')
            .expect("the actual query identity includes its connection and ordinal")
            .0,
        connection
    );
    assert!(
        !capture_workers
            .iter()
            .any(|worker| matches!(worker.path.as_str(), "handshake" | "status")),
        "open workers belong to the separate completed open scope"
    );
    assert!(
        !capture_events.iter().any(|event| matches!(
            event.operation.as_str(),
            "payload_decode"
                | "private_history_decode"
                | "range"
                | "iteration_step"
                | "population_walk"
        )),
        "edge loading cannot precede the first source cursor opening"
    );
    let source = observer
        .first_source
        .lock()
        .unwrap()
        .expect("sample the actual first GraphEdge callback");
    assert_eq!(
        source.completed_items, 0,
        "the first callback precedes the first edge cursor"
    );
    let source_owner = source
        .funding_owner
        .expect("the executing query must name its actual funding capability");
    assert_ne!(source_owner, 0);
    let capture_owner = capture
        .funding_owner
        .as_deref()
        .expect("adapter construction must name its actual funding capability");
    let (_, capture_address) = capture_owner
        .rsplit_once('@')
        .expect("the source observer names a capability type and Arc address");
    let capture_identity = usize::from_str_radix(
        capture_address
            .strip_prefix("0x")
            .expect("Arc addresses are hexadecimal"),
        16,
    )
    .unwrap();
    assert_eq!(
        capture_identity, source_owner,
        "adapter and query source use the same actual capability, independent of the caller operation label"
    );
    let control_bytes = capture
        .control_reservation_bytes
        .expect("snapshot capture records the exact retained adapter-control credit");
    assert!(control_bytes > 0);
    assert!(control_bytes as u64 <= declared_reader_limit);
    let request_bytes = source
        .control_reservation_bytes
        .expect("the source callback records current query request-reservation bytes");
    assert!(request_bytes > 0);
    assert!(request_bytes as u64 <= declared_reader_limit);
    // Adapter-control bytes and request bytes are different actual credits.
    // Successful page totals are cumulative admissions, not live credits.
    let capture_pages = observer
        .capture_pages
        .lock()
        .unwrap()
        .expect("the first graph callback must sample the executing worker's page accessor");
    assert_eq!(
        capture_pages,
        (
            captured.worker_admitted_pages,
            captured.worker_admitted_page_bytes
        )
    );
    assert!(capture_pages.1 <= declared_reader_limit);
    let capture_page_owners = observer.capture_page_owners.lock().unwrap();
    let capture_page_owners = capture_page_owners
        .as_ref()
        .expect("sample the real per-capability page map at the boundary");
    assert_eq!(
        capture_page_owners
            .values()
            .fold((0, 0), |sum, pages| (sum.0 + pages.0, sum.1 + pages.1)),
        capture_pages
    );
    assert!(
        capture_page_owners
            .keys()
            .all(|owner| owner == capture_owner),
        "every pre-edge page admission is funded by the captured query capability"
    );
    assert_eq!(capture_pages.0 == 0, capture_pages.1 == 0);
    // The observer has already required nonzero admission on this exact
    // named owner inside the first callback, before any edge cursor opens.
    let cache_after_query = database.__writer_cache_stats_for_test().unwrap();
    assert!(cache_after_query.0 >= cache_before_query.0);
    assert!(cache_after_query.1 >= cache_before_query.1);
    assert_eq!(
        actual_edges.rows(),
        vec![vec![Value::Uuid(uuid::Uuid::from_u128(2))]]
    );
    let loaded = scope
        .owner_graph_counters()
        .expect("the actual edge read remains observed on its worker");
    assert!(loaded.graph_payload_decodes > captured.graph_payload_decodes);
    let events = scope.graph_events();
    assert!(
        !events.is_empty(),
        "the actual edge control must emit graph events"
    );
    assert!(
        events
            .iter()
            .all(|event| event.phase == ReadWorkPhase::Query)
    );
    assert!(
        events
            .iter()
            .all(|event| event.operation_id.as_deref() == Some("owner-open-graph-capture-row"))
    );
    assert!(events.iter().all(|event| {
        event
            .worker_request_id
            .as_ref()
            .is_some_and(|id| !id.is_empty())
    }));
    assert!(
        events
            .iter()
            .any(|event| event.captured_root.is_some() && event.observation_status == "OBSERVED")
    );
    let loaded_root = events
        .iter()
        .find(|event| event.family == "graph_snapshot" && event.operation == "ROOT_BOUND")
        .expect("the actual edge control must bind its source root");
    assert_eq!(
        loaded_root
            .captured_root
            .as_deref()
            .expect("edge loading must observe its actual root"),
        capture_root
    );
    assert_eq!(
        loaded_root.worker_request_id.as_deref(),
        Some(query.worker_request_id.as_str())
    );
    let loaded_pages = scope
        .owner_page_admission()
        .expect("actual edge loading must preserve the executing page observer");
    assert_eq!(
        loaded_pages,
        (
            loaded.worker_admitted_pages,
            loaded.worker_admitted_page_bytes
        )
    );
    assert!(loaded_pages.0 >= capture_pages.0);
    assert!(loaded_pages.1 >= capture_pages.1);
    assert!(loaded_pages.1 <= declared_reader_limit);
    assert_eq!(loaded_pages.0 == 0, loaded_pages.1 == 0);
    let loaded_page_owners = scope
        .owner_page_admission_by_owner()
        .expect("the edge control remains observed per actual capability");
    assert_eq!(
        loaded_page_owners
            .values()
            .fold((0, 0), |sum, pages| (sum.0 + pages.0, sum.1 + pages.1)),
        loaded_pages
    );
    assert!(
        loaded_page_owners
            .keys()
            .all(|owner| owner == capture_owner)
    );
    for (owner, pages) in capture_page_owners {
        let loaded = loaded_page_owners
            .get(owner)
            .expect("successful admission totals retain the same capability identity");
        assert!(loaded.0 >= pages.0 && loaded.1 >= pages.1);
    }
    drop(capture_workers);
    drop(capture_events);
    *observer.scope.lock().unwrap() = None;
    drop(binding);
    drop(scope);
    drop(session);
    database.close().unwrap();
}

fn assert_public_owner_graph_observer_row(directory: &Path) {
    let path = directory.join("graph-observed-owner.db");
    let database = Database::open_with_options(
        &path,
        crate::DatabaseOpenOptions {
            maintenance_policy: crate::database::MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    database
        .execute(
            "CREATE TABLE nodes (id UUID PRIMARY KEY) SYNC OFF",
            &HashMap::new(),
        )
        .unwrap();
    database.execute("CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) SYNC OFF", &HashMap::new()).unwrap();
    for id in [1, 2] {
        database
            .execute(
                "INSERT INTO nodes VALUES ($id)",
                &HashMap::from([("id".into(), Value::Uuid(uuid::Uuid::from_u128(id)))]),
            )
            .unwrap();
    }
    let tx = database.begin_or_panic();
    database
        .insert_edge(
            tx,
            uuid::Uuid::from_u128(1),
            uuid::Uuid::from_u128(2),
            "LINKS".into(),
            HashMap::new(),
        )
        .unwrap();
    database.commit(tx).unwrap();
    let session = ReadSession::open_owner_only(&path, ReadSessionOptions::default()).unwrap();
    assert_eq!(session.route(), ReadRoute::Owner);
    let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let binding = scope.bind_owner(&database, "graph-observer-row");
    let result = session.execute("SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $source COLUMNS (b.id AS t))", &HashMap::from([("source".into(), Value::Uuid(uuid::Uuid::from_u128(1)))])).unwrap();
    assert_eq!(
        result.rows(),
        vec![vec![Value::Uuid(uuid::Uuid::from_u128(2))]]
    );
    let observed = scope
        .owner_graph_counters()
        .expect("executing owner worker must install the scope");
    assert!(observed.graph_addressed_gets > 0);
    assert!(observed.graph_payload_decodes > 0);
    assert_graph_context(&scope, ReadWorkPhase::Query);
    assert_graph_no_population_walk(&scope);
    assert!(scope.graph_events().iter().all(|event| {
        event
            .worker_request_id
            .as_ref()
            .is_some_and(|id| !id.is_empty())
    }));
    drop(binding);
    drop(scope);
    drop(session);
    database.close().unwrap();
}

// A real body and exact physical version join history repack and graph
// retirement through the existing scoped maintenance door.
fn graph_retention_row() -> (TableMeta, VersionedRow) {
    let meta = TableMeta {
        columns: vec![contextdb_core::ColumnDef {
            name: "body".into(),
            column_type: ColumnType::Text,
            nullable: false,
            primary_key: false,
            unique: false,
            default: None,
            references: None,
            expires: false,
            immutable: false,
            quantization: VectorQuantization::F32,
            rank_policy: None,
            context_id: false,
            scope_label: None,
            acl_ref: None,
            partition_key_columns: None,
            max_partitions: None,
            search_mode: contextdb_core::VectorSearchMode::Exact,
            auto_index_at: None,
            flat_below: None,
            auto_expansion_ceiling: None,
            hnsw_m: None,
            hnsw_ef_construction: None,
            hnsw_ef_search: None,
            hnsw_search_radius_millionths: None,
            vector_policy_revision: 0,
            consolidation_change_percent: None,
            consolidation_tombstone_percent: None,
            consolidation_disabled: false,
        }],
        ..TableMeta::default()
    };
    let row = VersionedRow {
        row_id: RowId(1),
        values: HashMap::from([(
            "body".into(),
            Value::Text("selected physical retention payload".into()),
        )]),
        created_tx: TxId(100),
        deleted_tx: None,
        lsn: Lsn(20),
        created_at: None,
    };
    (meta, row)
}

fn assert_graph_retention_row_source(root: &graph_read::GraphReadRoot, present: bool) {
    let (meta, expected) = graph_retention_row();
    let row = match root {
        graph_read::GraphReadRoot::Writer(root) => {
            let memory = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
                16 * 1024 * 1024,
            ));
            let key: [u8; 24] = RedbPersistence::rel_row_key(&expected).try_into().unwrap();
            let mut cursor = root
                .exact_row(
                    writer_source::SourceWindowOwner::Read(memory.into()),
                    "removed_rows",
                    &key,
                )
                .unwrap();
            let row = cursor
                .next_row()
                .unwrap()
                .map(|window| window.value.clone());
            assert!(cursor.next_row().unwrap().is_none());
            row
        }
        graph_read::GraphReadRoot::Image(source) => source
            .relational_row_by_id_lsn("removed_rows", &meta, expected.row_id, expected.lsn)
            .unwrap(),
    };
    assert_eq!(
        row,
        present.then_some(expected),
        "row payload presence follows this captured root, including pinned old roots"
    );
}

// Coordinates are fixture inputs, not reconstructed from an engine
// population. Each physical group holds one survivor with its own public J.
type GraphHistoryCoordinate = (Lsn, usize, ChangeLogEntry);
fn graph_distinct_history_fixture(
    path: &Path,
    versions: usize,
    references: usize,
) -> (RedbPersistence, AdjEntry, Vec<GraphHistoryCoordinate>) {
    let (store, predecessor, entries) = graph_history_fixture(path, versions, 1, false);
    let mut coordinates = vec![(Lsn(30), 0, entries[0].clone())];
    let commits = (1..references)
        .map(|offset| (Lsn(20 + offset as u64), TxId(100)))
        .collect::<BTreeMap<_, _>>();
    store.flush_commit_index_entries(&commits).unwrap();
    for offset in 1..references {
        let public_lsn = Lsn(20 + offset as u64);
        let physical_lsn = Lsn(30 + offset as u64 * 10);
        let entry = ChangeLogEntry::EdgeDelete {
            source: predecessor.source,
            target: predecessor.target,
            edge_type: predecessor.edge_type.clone(),
            lsn: public_lsn,
        };
        store
            .append_change_log(physical_lsn, std::slice::from_ref(&entry))
            .unwrap();
        coordinates.push((physical_lsn, 0, entry));
    }
    (store, predecessor, coordinates)
}

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn assert_graph_coordinate_witnesses(
    store: &RedbPersistence,
    predecessor: &AdjEntry,
    coordinates: &[GraphHistoryCoordinate],
    private_bytes: &[Vec<u8>],
    retired: bool,
) {
    use graph_history_record::*;
    assert_eq!(coordinates.len(), private_bytes.len());
    store
        .with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let history = read.open_table(CHANGE_LOG_TABLE).unwrap();
            let contributions = read.open_table(GRAPH_CONTRIBUTIONS).unwrap();
            let locations = read.open_table(GRAPH_OCCURRENCE_LOCATIONS).unwrap();
            let receipts = read.open_table(GRAPH_PREDECESSOR_LIFECYCLE).unwrap();
            let commits = read.open_table(COMMIT_INDEX_TABLE).unwrap();
            let p = RedbPersistence::graph_fwd_key(predecessor);
            let rk = lifecycle_key(&p)?;
            let receipt = receipts.get(rk.as_slice()).unwrap().unwrap();
            let mut expected_r = vec![1];
            expected_r.extend_from_slice(&100u64.to_be_bytes());
            expected_r.extend_from_slice(&(coordinates.len() as u64).to_be_bytes());
            expected_r.push(u8::from(retired));
            assert_eq!(receipt.value(), expected_r.as_slice());
            for ((physical, ordinal, entry), expected) in coordinates.iter().zip(private_bytes) {
                let mut key = String::new();
                RedbPersistence::write_change_log_entry_key(*physical, *ordinal, &mut key);
                let value = history.get(key.as_str()).unwrap().unwrap();
                assert_eq!(
                    value.value(),
                    expected.as_slice(),
                    "retained distinct-address private bytes remain exact"
                );
                let record = RedbPersistence::decode_stored_change_log(value.value())?;
                assert_eq!(record.tag, 2);
                assert_eq!(&record.entry, entry);
                let witness = record.witness.unwrap();
                assert_eq!(witness.predecessor, p);
                assert_eq!(witness.deletion_tx, TxId(100));
                let location_key = location_key(witness.id);
                let location = locations.get(location_key.as_slice()).unwrap().unwrap();
                let mut expected_t = vec![1];
                expected_t.extend_from_slice(&physical.0.to_be_bytes());
                expected_t.extend_from_slice(&(*ordinal as u64).to_be_bytes());
                assert_eq!(location.value(), expected_t.as_slice());
                let c_key = deletion_contribution_key(*physical, &witness);
                let contribution = contributions.get(c_key.as_slice()).unwrap().unwrap();
                let mut expected_c = vec![1];
                expected_c.extend_from_slice(&contribution_digest(
                    false,
                    &witness.id.to_be_bytes(),
                    expected,
                )?);
                assert_eq!(contribution.value(), expected_c.as_slice());
                assert_eq!(commits.get(entry.lsn().0).unwrap().unwrap().value(), 100);
                assert!(
                    commits.get(physical.0).unwrap().is_none(),
                    "a distinct physical history coordinate does not invent a commit"
                );
            }
            let fwd = read.open_table(GRAPH_FWD_TABLE).unwrap();
            let rev = read.open_table(GRAPH_REV_TABLE).unwrap();
            let reverse = RedbPersistence::graph_rev_key(predecessor);
            assert_eq!(fwd.get(p.as_slice()).unwrap().is_none(), retired);
            assert_eq!(rev.get(reverse.as_slice()).unwrap().is_none(), retired);
            let creation = creation_contribution_key(&p)?;
            assert_eq!(
                contributions.get(creation.as_slice()).unwrap().is_none(),
                retired
            );
            Ok(())
        })
        .unwrap();
}

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn assert_graph_coordinate_source_answers(
    root: graph_read::GraphReadRoot,
    predecessor: &AdjEntry,
    expected_edge: bool,
    coordinates: &[GraphHistoryCoordinate],
) {
    use std::ops::Bound;
    use writer_source::SourceWindowOwner;
    let memory = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
        16 * 1024 * 1024,
    ));
    for reverse in [false, true] {
        let key = if reverse {
            RedbPersistence::graph_rev_key(predecessor)
        } else {
            RedbPersistence::graph_fwd_key(predecessor)
        };
        let mut cursor = root
            .cursor(
                reverse,
                Bound::Included(&key),
                Bound::Included(&key),
                memory.clone(),
            )
            .unwrap();
        let window = cursor.next_edge_with(&|| Ok(())).unwrap();
        assert_eq!(window.is_some(), expected_edge);
        if let Some(window) = window {
            assert_eq!(window.edge(), predecessor);
        }
        assert!(cursor.next_edge_with(&|| Ok(())).unwrap().is_none());
    }
    let mut changes = Vec::new();
    match root {
        graph_read::GraphReadRoot::Writer(root) => {
            for (physical, ordinal, expected) in coordinates {
                let mut position = [0u8; 16];
                position[..8].copy_from_slice(&physical.0.to_be_bytes());
                position[8..].copy_from_slice(&(*ordinal as u64).to_be_bytes());
                let mut cursor = root
                    .cursor(
                        SourceWindowOwner::Read(memory.clone().into()),
                        writer_history::WRITER_CHANGE_LOG_TABLE.name(),
                        writer_source::WriterCursorFamily::Bytes,
                        Bound::Included(&position),
                        Bound::Included(&position),
                        false,
                    )
                    .unwrap();
                assert_eq!(cursor.next_change().unwrap().unwrap().value, *expected);
                assert!(cursor.next_change().unwrap().is_none());
            }
            let mut cursor = root
                .change_log_cursor(
                    SourceWindowOwner::Read(memory.into()),
                    coordinates.first().unwrap().0,
                    coordinates.last().unwrap().0,
                )
                .unwrap();
            while let Some(window) = cursor.next_change().unwrap() {
                changes.push(window.value.clone());
            }
        }
        graph_read::GraphReadRoot::Image(source) => {
            for (physical, ordinal, expected) in coordinates {
                let mut key = String::new();
                RedbPersistence::write_change_log_entry_key(*physical, *ordinal, &mut key);
                source
                    .with_read(|read| {
                        let history = read.open_table(CHANGE_LOG_TABLE).unwrap();
                        let value = history.get(key.as_str()).unwrap().unwrap();
                        source.verify_stored_graph_history_in_read(
                            read,
                            key.as_str(),
                            value.value(),
                            memory.clone(),
                        )?;
                        assert_eq!(
                            RedbPersistence::decode_stored_change_log(value.value())?.entry,
                            *expected
                        );
                        Ok(())
                    })
                    .unwrap();
            }
            source
                .visit_changes_since(Lsn(19), memory, &|| Ok(()), &mut |entry| {
                    changes.push(entry);
                    Ok(())
                })
                .unwrap();
        }
    }
    assert_eq!(
        changes,
        coordinates
            .iter()
            .map(|(_, _, entry)| entry.clone())
            .collect::<Vec<_>>()
    );
}

fn assert_graph_distinct_maintenance_rows(directory: &Path) {
    let mut mutation_control = None;
    let mut query_controls = HashMap::new();
    // The cross product varies fanout independently from unrelated graph
    // population. Repeated A=30/J=20 rows remain in the original helper.
    for references in [1, 4, 8] {
        for versions in [4, 8, 16] {
            let path = directory.join(format!(
                "graph-distinct-retirement-{references}-{versions}.db"
            ));
            let (store, predecessor, coordinates) =
                graph_distinct_history_fixture(&path, versions, references);
            let before = graph_publication_state(&store);
            let private_bytes = before.1[0]
                .iter()
                .map(|(_, value)| value.clone())
                .collect::<Vec<_>>();
            assert_graph_coordinate_witnesses(
                &store,
                &predecessor,
                &coordinates,
                &private_bytes,
                false,
            );
            drop(store);
            let (image, _receipt, old_source) =
                load_read_image_with_memory(&path, Some(16 * 1024 * 1024))
                    .unwrap()
                    .into_parts();
            drop(image);
            let fixture_open_accountant_41665 =
                Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
            let _fixture_decode_scope_41665 =
                crate::read_image_memory::ImageMemory::enter_writer_operation(
                    std::sync::Arc::clone(&fixture_open_accountant_41665).into(),
                )
                .unwrap();
            let store = RedbPersistence::open(&path).unwrap();
            store.attach_memory_accountant(
                std::sync::Arc::clone(&fixture_open_accountant_41665).into(),
            );
            let physical_end = coordinates.last().unwrap().0;
            let old_writer = store
                .initialize_writer_snapshot(SnapshotId(100), physical_end)
                .unwrap();
            let deferred = HashSet::new();
            let retained_zero_keys = graph_retained_zero_touch_keys(&before);
            let scope = graph_retirement_observer(&before);
            let stats = store
                .prune_retention_scoped(
                    &HashMap::new(),
                    &[],
                    &[],
                    &[],
                    Some((&[], std::slice::from_ref(&predecessor), &deferred)),
                    &[],
                    &Vec::new(),
                    &[("graph_fixture_progress".into(), vec![1])],
                )
                .unwrap();
            let counts = assert_graph_retirement_work(&scope, &predecessor, false);
            assert_graph_retained_zero_touch(&scope, &retained_zero_keys);
            if let Some(control) = mutation_control {
                assert_eq!(
                    counts, control,
                    "one predecessor retirement work stays fixed across independent fanout and population scales"
                );
            } else {
                mutation_control = Some(counts);
            }
            drop(scope);
            assert_eq!(stats.row_keys_removed, 0);
            assert_eq!(stats.change_log_keys_removed, 0);
            assert_eq!(stats.edge_keys_removed, 2);
            assert_graph_coordinate_witnesses(
                &store,
                &predecessor,
                &coordinates,
                &private_bytes,
                true,
            );
            let after = graph_publication_state(&store);
            assert_eq!(
                after.1[0], before.1[0],
                "every retained private history key and byte survives"
            );
            assert_eq!(after.0[3], before.0[3], "every retained T byte survives");
            let p = RedbPersistence::graph_fwd_key(&predecessor);
            let reverse = RedbPersistence::graph_rev_key(&predecessor);
            let creation = graph_history_record::creation_contribution_key(&p).unwrap();
            for (family, key) in [(0, &p), (1, &reverse), (2, &creation)] {
                assert_eq!(
                    after.0[family],
                    before.0[family]
                        .iter()
                        .filter(|(candidate, _)| candidate != key)
                        .cloned()
                        .collect::<Vec<_>>(),
                    "only the selected predecessor projection/creation C disappears"
                );
            }
            assert_eq!(before.0[4].len(), 1);
            let rk = graph_history_record::lifecycle_key(&p).unwrap();
            let mut expected_r = before.0[4].clone();
            assert_eq!(expected_r[0].0, rk);
            assert_eq!(expected_r[0].1[17], 0);
            expected_r[0].1[17] = 1;
            assert_eq!(
                after.0[4], expected_r,
                "one selected receipt changes Present to Retired without count or identity changes"
            );
            assert!(after.1[3].contains(&("graph_fixture_progress".to_owned(), vec![1])));
            assert!(
                !before.1[3]
                    .iter()
                    .any(|(key, _)| key == "graph_fixture_progress")
            );
            assert_ne!(
                after.1[4], before.1[4],
                "final binding joins the selected progress and graph publication"
            );
            assert_ne!(
                after.1[1], before.1[1],
                "recovery bounds join the same publication"
            );
            let query_work = graph_addressed_observer_row(
                &graph_read::GraphReadRoot::Writer(old_writer.clone()),
                &predecessor,
                true,
                coordinates[0].0,
                coordinates[0].1,
                &coordinates[0].2,
            );
            if let Some(control) = query_controls.get(&(true, true)) {
                assert_eq!(
                    &query_work, control,
                    "addressed work stays fixed across unrelated graph population and distinct public/physical fanout"
                );
            } else {
                query_controls.insert((true, true), query_work);
            }
            assert_graph_coordinate_source_answers(
                graph_read::GraphReadRoot::Writer(old_writer),
                &predecessor,
                true,
                &coordinates,
            );
            let query_work = graph_addressed_observer_row(
                &graph_read::GraphReadRoot::Image(old_source.clone()),
                &predecessor,
                true,
                coordinates[0].0,
                coordinates[0].1,
                &coordinates[0].2,
            );
            if let Some(control) = query_controls.get(&(false, true)) {
                assert_eq!(
                    &query_work, control,
                    "addressed work stays fixed across unrelated graph population and distinct public/physical fanout"
                );
            } else {
                query_controls.insert((false, true), query_work);
            }
            assert_graph_coordinate_source_answers(
                graph_read::GraphReadRoot::Image(old_source),
                &predecessor,
                true,
                &coordinates,
            );
            let new_writer = store
                .prepare_writer_snapshot(
                    store.reserve_writer_snapshot().unwrap(),
                    SnapshotId(100),
                    physical_end,
                )
                .unwrap();
            let query_work = graph_addressed_observer_row(
                &graph_read::GraphReadRoot::Writer(new_writer.clone()),
                &predecessor,
                false,
                coordinates[0].0,
                coordinates[0].1,
                &coordinates[0].2,
            );
            if let Some(control) = query_controls.get(&(true, false)) {
                assert_eq!(
                    &query_work, control,
                    "addressed work stays fixed across unrelated graph population and distinct public/physical fanout"
                );
            } else {
                query_controls.insert((true, false), query_work);
            }
            assert_graph_coordinate_source_answers(
                graph_read::GraphReadRoot::Writer(new_writer),
                &predecessor,
                false,
                &coordinates,
            );
            drop(store);
            drop(_fixture_decode_scope_41665);
            let (image, _receipt, new_source) =
                load_read_image_with_memory(&path, Some(16 * 1024 * 1024))
                    .unwrap()
                    .into_parts();
            drop(image);
            let query_work = graph_addressed_observer_row(
                &graph_read::GraphReadRoot::Image(new_source.clone()),
                &predecessor,
                false,
                coordinates[0].0,
                coordinates[0].1,
                &coordinates[0].2,
            );
            if let Some(control) = query_controls.get(&(false, false)) {
                assert_eq!(
                    &query_work, control,
                    "addressed work stays fixed across unrelated graph population and distinct public/physical fanout"
                );
            } else {
                query_controls.insert((false, false), query_work);
            }
            assert_graph_coordinate_source_answers(
                graph_read::GraphReadRoot::Image(new_source),
                &predecessor,
                false,
                &coordinates,
            );
            let fixture_open_accountant_41717 =
                std::sync::Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
            let _fixture_decode_scope_41717 =
                crate::read_image_memory::ImageMemory::enter_writer_operation(
                    std::sync::Arc::clone(&fixture_open_accountant_41717).into(),
                )
                .unwrap();
            let reopened = RedbPersistence::open(&path).unwrap();
            reopened.attach_memory_accountant(
                std::sync::Arc::clone(&fixture_open_accountant_41717).into(),
            );
            assert_eq!(
                graph_publication_state(&reopened),
                after,
                "restart exposes the complete new publication"
            );
        }
    }
}

// These rows use the two real captured source implementations. Graph work
// counts require the existing observer's graph extension; no missing
// observation is interpreted as zero here.
fn graph_history_fixture(
    path: &Path,
    versions: usize,
    references: usize,
    row_history: bool,
) -> (RedbPersistence, AdjEntry, Vec<ChangeLogEntry>) {
    let memory = Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
    let store =
        RedbPersistence::create_with_accountant(path, Some(memory.into()), REDB_CACHE_BYTES)
            .unwrap();
    let predecessor = AdjEntry {
        source: NodeId::from_u128(1),
        target: NodeId::from_u128(2),
        edge_type: "retained_link".into(),
        properties: HashMap::new(),
        created_tx: TxId(1),
        deleted_tx: Some(TxId(100)),
        lsn: Lsn(1),
    };
    let mut commits = BTreeMap::from([(Lsn(1), TxId(1)), (Lsn(20), TxId(100))]);
    let mut edges = vec![predecessor.clone()];
    for version in 0..versions {
        let tx = version as u64 + 2;
        commits.insert(Lsn(tx), TxId(tx));
        edges.push(AdjEntry {
            source: NodeId::from_u128(3),
            target: NodeId::from_u128(4),
            edge_type: "unrelated_link".into(),
            properties: HashMap::new(),
            created_tx: TxId(tx),
            deleted_tx: Some(TxId(100)),
            lsn: Lsn(tx),
        });
    }
    store.flush_commit_index_entries(&commits).unwrap();
    store.append_graph_edges_batch(&edges).unwrap();
    let deletion = ChangeLogEntry::EdgeDelete {
        source: predecessor.source,
        target: predecessor.target,
        edge_type: predecessor.edge_type.clone(),
        lsn: Lsn(20),
    };
    let mut entries = Vec::new();
    if row_history {
        let (meta, row) = graph_retention_row();
        store.flush_table_meta("removed_rows", &meta).unwrap();
        store
            .append_table_rows_batch("removed_rows", Some(&meta), std::slice::from_ref(&row))
            .unwrap();
        assert_eq!(
            store
                .load_relational_row_version(
                    "removed_rows",
                    &meta,
                    row.row_id,
                    row.created_tx,
                    row.lsn
                )
                .unwrap(),
            Some(row),
            "the repack fixture contains an actual physical row version"
        );
        entries.push(ChangeLogEntry::RowInsert {
            table: "removed_rows".into(),
            row_id: RowId(1),
            lsn: Lsn(20),
        });
    }
    entries.extend(std::iter::repeat_n(deletion, references));
    store.append_change_log(Lsn(30), &entries).unwrap();
    assert!(
        !store.load_commit_index().unwrap().contains_key(&Lsn(30)),
        "a history storage coordinate does not invent commit membership"
    );
    (store, predecessor, entries)
}

// Fixture inspection stays outside operation observation scopes. This is
// an assertion snapshot, never the engine's survivor classifier or index.
type GraphPublicationState = (Vec<Vec<(Vec<u8>, Vec<u8>)>>, Vec<Vec<(String, Vec<u8>)>>);
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn graph_publication_state(store: &RedbPersistence) -> GraphPublicationState {
    store
        .with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let mut byte_tables = Vec::new();
            for definition in [
                GRAPH_FWD_TABLE,
                GRAPH_REV_TABLE,
                graph_history_record::GRAPH_CONTRIBUTIONS,
                graph_history_record::GRAPH_OCCURRENCE_LOCATIONS,
                graph_history_record::GRAPH_PREDECESSOR_LIFECYCLE,
            ] {
                let table = read
                    .open_table(definition)
                    .map_err(RedbPersistence::storage_error)?;
                let mut rows = Vec::new();
                for next in table.iter().map_err(RedbPersistence::storage_error)? {
                    let (key, value) = next.map_err(RedbPersistence::storage_error)?;
                    rows.push((key.value().to_vec(), value.value().to_vec()));
                }
                byte_tables.push(rows);
            }
            let row_table_name = RedbPersistence::rel_table_name("removed_rows");
            let row_definition: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(row_table_name.as_str());
            let mut physical_rows = Vec::new();
            match read.open_table(row_definition) {
                Ok(table) => {
                    for next in table.iter().map_err(RedbPersistence::storage_error)? {
                        let (key, value) = next.map_err(RedbPersistence::storage_error)?;
                        physical_rows.push((key.value().to_vec(), value.value().to_vec()));
                    }
                }
                Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(error) => return Err(RedbPersistence::storage_error(error)),
            }
            byte_tables.push(physical_rows);
            let mut text_tables = Vec::new();
            for definition in [
                CHANGE_LOG_TABLE,
                writer_recovery::RECOVERY_TABLE,
                META_TABLE,
                CONFIG_TABLE,
                CONTENT_IDENTITY_METADATA_TABLE,
            ] {
                let mut rows = Vec::new();
                match read.open_table(definition) {
                    Ok(table) => {
                        for next in table.iter().map_err(RedbPersistence::storage_error)? {
                            let (key, value) = next.map_err(RedbPersistence::storage_error)?;
                            rows.push((key.value().to_owned(), value.value().to_vec()));
                        }
                    }
                    Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(error) => return Err(RedbPersistence::storage_error(error)),
                }
                text_tables.push(rows);
            }
            Ok((byte_tables, text_tables))
        })
        .unwrap()
}

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn assert_retained_graph_witnesses(
    store: &RedbPersistence,
    predecessor: &AdjEntry,
    private_bytes: &[Vec<u8>],
    first_ordinal: usize,
    retired: bool,
) {
    use graph_history_record::*;
    store
        .with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let history = read.open_table(CHANGE_LOG_TABLE).unwrap();
            let contributions = read.open_table(GRAPH_CONTRIBUTIONS).unwrap();
            let locations = read.open_table(GRAPH_OCCURRENCE_LOCATIONS).unwrap();
            let receipts = read.open_table(GRAPH_PREDECESSOR_LIFECYCLE).unwrap();
            let p = RedbPersistence::graph_fwd_key(predecessor);
            let r_key = lifecycle_key(&p)?;
            let receipt = receipts.get(r_key.as_slice()).unwrap().unwrap();
            assert_eq!(receipt.value().len(), 18);
            assert_eq!(receipt.value()[0], 1);
            assert_eq!(
                u64::from_be_bytes(receipt.value()[1..9].try_into().unwrap()),
                100
            );
            assert_eq!(
                u64::from_be_bytes(receipt.value()[9..17].try_into().unwrap()),
                private_bytes.len() as u64
            );
            assert_eq!(receipt.value()[17], u8::from(retired));
            for (offset, expected) in private_bytes.iter().enumerate() {
                let ordinal = first_ordinal + offset;
                let mut key = String::new();
                RedbPersistence::write_change_log_entry_key(Lsn(30), ordinal, &mut key);
                let stored = history.get(key.as_str()).unwrap().unwrap();
                assert_eq!(
                    stored.value(),
                    expected.as_slice(),
                    "retained private bytes are immutable"
                );
                let record = RedbPersistence::decode_stored_change_log(stored.value())?;
                assert_eq!(record.tag, 2);
                assert_eq!(record.entry.lsn(), Lsn(20));
                let witness = record.witness.unwrap();
                assert_eq!(witness.predecessor, p);
                assert_eq!(witness.deletion_tx, TxId(100));
                let t_key = location_key(witness.id);
                let location = locations.get(t_key.as_slice()).unwrap().unwrap();
                assert_eq!(location.value().len(), 17);
                assert_eq!(location.value()[0], 1);
                assert_eq!(
                    u64::from_be_bytes(location.value()[1..9].try_into().unwrap()),
                    30
                );
                assert_eq!(
                    u64::from_be_bytes(location.value()[9..17].try_into().unwrap()),
                    ordinal as u64
                );
                let c_key = deletion_contribution_key(Lsn(30), &witness);
                let contribution = contributions.get(c_key.as_slice()).unwrap().unwrap();
                assert_eq!(contribution.value()[0], 1);
                assert_eq!(
                    &contribution.value()[1..],
                    &contribution_digest(false, &witness.id.to_be_bytes(), expected)?
                );
            }
            let fwd = read.open_table(GRAPH_FWD_TABLE).unwrap();
            let rev = read.open_table(GRAPH_REV_TABLE).unwrap();
            let reverse = RedbPersistence::graph_rev_key(predecessor);
            assert_eq!(fwd.get(p.as_slice()).unwrap().is_none(), retired);
            assert_eq!(rev.get(reverse.as_slice()).unwrap().is_none(), retired);
            let creation = creation_contribution_key(&p)?;
            assert_eq!(
                contributions.get(creation.as_slice()).unwrap().is_none(),
                retired
            );
            let commits = read.open_table(COMMIT_INDEX_TABLE).unwrap();
            assert_eq!(commits.get(20).unwrap().unwrap().value(), 100);
            assert!(commits.get(30).unwrap().is_none());
            Ok(())
        })
        .unwrap();
}

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn assert_graph_source_answers(
    root: graph_read::GraphReadRoot,
    predecessor: &AdjEntry,
    expected_edge: bool,
    expected_history: &[ChangeLogEntry],
) {
    use std::ops::Bound;
    use writer_source::SourceWindowOwner;
    let memory = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
        16 * 1024 * 1024,
    ));
    for reverse in [false, true] {
        let key = if reverse {
            RedbPersistence::graph_rev_key(predecessor)
        } else {
            RedbPersistence::graph_fwd_key(predecessor)
        };
        let mut cursor = root
            .cursor(
                reverse,
                Bound::Included(&key),
                Bound::Included(&key),
                memory.clone(),
            )
            .unwrap();
        let window = cursor.next_edge_with(&|| Ok(())).unwrap();
        assert_eq!(window.is_some(), expected_edge);
        if let Some(window) = window {
            assert_eq!(window.edge(), predecessor);
        }
        assert!(cursor.next_edge_with(&|| Ok(())).unwrap().is_none());
    }
    let requested = expected_history
        .iter()
        .position(|entry| matches!(entry, ChangeLogEntry::EdgeDelete { .. }))
        .unwrap();
    let mut requested_key = String::new();
    RedbPersistence::write_change_log_entry_key(Lsn(30), requested, &mut requested_key);
    let mut changes = Vec::new();
    match root {
        graph_read::GraphReadRoot::Writer(root) => {
            let mut position = [0u8; 16];
            position[..8].copy_from_slice(&30u64.to_be_bytes());
            position[8..].copy_from_slice(&(requested as u64).to_be_bytes());
            let mut addressed = root
                .cursor(
                    SourceWindowOwner::Read(memory.clone().into()),
                    writer_history::WRITER_CHANGE_LOG_TABLE.name(),
                    writer_source::WriterCursorFamily::Bytes,
                    Bound::Included(&position),
                    Bound::Included(&position),
                    false,
                )
                .unwrap();
            assert_eq!(
                addressed.next_change().unwrap().unwrap().value,
                expected_history[requested]
            );
            assert!(addressed.next_change().unwrap().is_none());
            drop(addressed);
            let mut cursor = root
                .change_log_cursor(SourceWindowOwner::Read(memory.into()), Lsn(30), Lsn(30))
                .unwrap();
            while let Some(window) = cursor.next_change().unwrap() {
                changes.push(window.value.clone());
            }
        }
        graph_read::GraphReadRoot::Image(source) => {
            source
                .with_read(|read| {
                    let history = read.open_table(CHANGE_LOG_TABLE).unwrap();
                    let bytes = history.get(requested_key.as_str()).unwrap().unwrap();
                    source.verify_stored_graph_history_in_read(
                        read,
                        requested_key.as_str(),
                        bytes.value(),
                        memory.clone(),
                    )?;
                    assert_eq!(
                        RedbPersistence::decode_stored_change_log(bytes.value())?.entry,
                        expected_history[requested]
                    );
                    Ok(())
                })
                .unwrap();
            source
                .visit_changes_since(Lsn(19), memory, &|| Ok(()), &mut |entry| {
                    changes.push(entry);
                    Ok(())
                })
                .unwrap();
        }
    }
    assert_eq!(
        changes, expected_history,
        "both captured sources retain the complete history answer"
    );
}

// The callbacks use the installed execution target and its real declared
// operation child. Residency is a paired observation, never graph work.
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn assert_installed_commit_directory_rows(
    file: &ReadSession,
    expected: &[(Lsn, TxId)],
    path: &Path,
) -> ReadWorkObservationScope {
    use contextdb_core::read_contract::{OwnerReadCancellation, ReadLimits};
    use std::ops::Bound::{Excluded, Included, Unbounded};
    file.__with_resident_history_for_test(|database| {
        assert!(database.__has_captured_commit_directory_for_test());
        let child = database.declared_reader_memory()?;
        let request_identity = crate::read_work_probe::funding_owner_id(&child);
        let cancellation = OwnerReadCancellation::new();
        let open = crate::executor::bounded::ReadOpenContext::new(
            child, ReadLimits::default(), Arc::new(DirectoryProofClock), cancellation.clone(),
        )?;
        let checkpoint = || open.checkpoint();
        let context = contextdb_tx::ReadCommitContext::new(open.read_memory(), &checkpoint);
        let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
        assert_eq!(scope.commit_directory_residency(), None, "no sample means unobserved");
        assert_eq!(scope.commit_directory_funding(), None, "a later scope has no inferred funding owner");
        assert_eq!(database.__commit_directory_census_with_context_for_test(&context)?, (expected.len(), Lsn(1)));
        let funding_before = scope.commit_directory_funding().expect("installed manager binds actual native owners");
        let request_before = funding_before.request_child.expect("caller is a native declared child");
        assert_eq!(request_before.owner_identity, request_identity);
        assert_ne!(request_before.owner_identity, funding_before.open_child.owner_identity);
        assert_eq!(request_before.parent_identity, funding_before.open_child.parent_identity);
        assert!(funding_before.open_child.alive && request_before.alive && request_before.parent_alive);
        assert_eq!(funding_before.image_alive, Some(true));
        assert_eq!(funding_before.image_accountant_alive, Some(true));
        assert_eq!(scope.commit_directory_residency(), Some((0, expected.len() as u64)), "actual resident decoded slots and durable census remain separate");
        for pair in [expected[0], *expected.last().unwrap()] {
            assert_eq!(database.__commit_directory_get_with_context_for_test(pair.0, &context)?, Some(pair.1));
            assert!(database.__commit_directory_contains_with_context_for_test(pair.0, &context)?);
        }
        for absent in [Lsn(0), Lsn(18), Lsn(19), Lsn(30)] {
            assert_eq!(database.__commit_directory_get_with_context_for_test(absent, &context)?, None);
            assert!(!database.__commit_directory_contains_with_context_for_test(absent, &context)?);
        }
        let last_small = expected[expected.len()-2];
        for (bound, answer) in [
            (Included(Lsn(0)), None), (Excluded(Lsn(1)), None),
            (Included(Lsn(1)), Some(expected[0])),
            (Included(Lsn(19)), Some(last_small)), (Excluded(Lsn(20)), Some(last_small)),
            (Included(Lsn(20)), Some((Lsn(20), TxId(100)))),
            (Excluded(Lsn(21)), Some((Lsn(20), TxId(100)))),
            (Unbounded, Some((Lsn(20), TxId(100)))),
        ] {
            assert_eq!(database.__commit_directory_floor_with_context_for_test(bound, &context)?, answer);
        }
        let mut cursor = database.__commit_directory_cursor_with_context_for_test(Included(Lsn(2)), Excluded(Lsn(20)), &context)?;
        let funding_cursor = scope.commit_directory_funding().unwrap();
        let request_cursor = funding_cursor.request_child.unwrap();
        assert!(request_cursor.charged_bytes > request_before.charged_bytes, "real cursor controls charge the actual caller child");
        assert_eq!(request_cursor.parent_charged_bytes - request_before.parent_charged_bytes, request_cursor.charged_bytes - request_before.charged_bytes);
        assert_eq!(funding_cursor.open_child.charged_bytes, funding_before.open_child.charged_bytes);
        assert_eq!(funding_cursor.image_held_bytes, funding_before.image_held_bytes);
        assert_eq!(funding_cursor.image_accountant_used_bytes, funding_before.image_accountant_used_bytes);
        let mut actual = Vec::new();
        while let Some(pair) = cursor.next_entry(&context)? { actual.push(pair); }
        assert_eq!(actual, expected.iter().copied().filter(|(lsn, _)| lsn.0 >= 2 && lsn.0 < 20).collect::<Vec<_>>());
        assert!(cursor.next_entry(&context)?.is_none());
        drop(cursor);
        let funding_refunded = scope.commit_directory_funding().unwrap();
        assert_eq!(funding_refunded, funding_before, "real cursor drop refunds its child and parent to the quiescent baseline");
        for (lower, upper, wanted) in [
            (Included(Lsn(18)), Excluded(Lsn(20)), Vec::new()),
            (Included(Lsn(u64::MAX)), Unbounded, Vec::new()),
            (Excluded(Lsn(u64::MAX)), Unbounded, Vec::new()),
            (Included(Lsn(20)), Included(Lsn(u64::MAX)), vec![(Lsn(20), TxId(100))]),
        ] {
            let mut boundary_cursor = database.__commit_directory_cursor_with_context_for_test(lower, upper, &context)?;
            let mut found = Vec::new();
            while let Some(pair) = boundary_cursor.next_entry(&context)? { found.push(pair); }
            assert_eq!(found, wanted, "empty and maximum cursor bounds use the installed captured directory");
            assert_eq!(boundary_cursor.next_entry(&context)?, None);
        }
        assert_eq!(scope.commit_directory_residency(), Some((0, expected.len() as u64)), "cursor output does not become resident directory state");
        let refusal_limits = ReadLimits::default();
        let refusal_parent = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(refusal_limits.memory).expect("admit the declared fixture root");
        let refusal_open = crate::executor::bounded::ReadOpenContext::new(refusal_parent.child()?, refusal_limits, Arc::new(DirectoryProofClock), OwnerReadCancellation::new())?;
        let refusal_child = refusal_open.read_memory();
        let refusal_checkpoint = || refusal_open.checkpoint();
        let refusal_context = contextdb_tx::ReadCommitContext::new(refusal_child.clone(), &refusal_checkpoint);
        database.__commit_directory_census_with_context_for_test(&refusal_context)?;
        let available = (refusal_limits.memory - refusal_parent.retained_bytes_for_test()) as usize;
        refusal_child.try_reserve(available, "reader", "commit_directory_proof", "fixture admission")?;
        let saturated = scope.commit_directory_funding().unwrap();
        let refusal = match database.__commit_directory_cursor_with_context_for_test(Unbounded, Unbounded, &refusal_context) {
            Err(error) => error,
            Ok(_) => panic!("directory cursor must refuse when its actual declared child has no headroom"),
        };
        assert!(matches!(refusal, Error::MemoryBudgetExceeded { requested_bytes, available_bytes, budget_limit_bytes, .. } if budget_limit_bytes as u64 == refusal_limits.memory && requested_bytes > available_bytes));
        assert_eq!(scope.commit_directory_funding().unwrap(), saturated, "typed refusal neither charges ImageMemory nor leaks child or parent credit");
        refusal_child.release(available);
        drop(refusal_context); drop(refusal_child); drop(refusal_open);
        assert_eq!(refusal_parent.retained_bytes_for_test(), 0);
        drop(refusal_parent);
        database.__commit_directory_census_with_context_for_test(&context)?;
        // Captured Database is engine-private and has no public export door.
        // Collect through the public File session that installed this target,
        // then inspect a fresh public File target on the same durable fixture.
        let collected = file.metadata(crate::MetadataRequest::ImageState {
            kind: crate::DirectImageMetadataKind::ChangeLog,
        }, None)?;
        drop(collected);
        let exported = ReadSession::open(path)?;
        assert_eq!(directory_pinned_view(&exported), ((expected.len(),Lsn(1)), None, Some((Lsn(20),TxId(100))), Some((0,expected.len() as u64))));
        database.__sample_commit_directory_residency_with_context_for_test(&context)?;
        assert_eq!(scope.commit_directory_residency(), Some((0, expected.len() as u64)), "export collection leaves the installed captured directory unchanged");
        let before = scope.graph_counters();
        let error = database.__try_begin_for_test().unwrap_err();
        assert!(matches!(error, Error::InvalidStateTransition(ref reason) if reason == "captured commit history has no writable transaction capability"));
        assert_eq!(scope.graph_counters(), before, "captured mutation refusal precedes directory effects");
        let mut cancelled_cursor = database.__commit_directory_cursor_with_context_for_test(Unbounded, Unbounded, &context)?;
        assert_eq!(cancelled_cursor.next_entry(&context)?, Some(expected[0]));
        cancellation.cancel();
        assert!(matches!(cancelled_cursor.next_entry(&context), Err(Error::ReadCancelled)), "real cursor cancellation follows positive progress");
        assert!(matches!(database.__commit_directory_get_with_context_for_test(Lsn(20), &context), Err(Error::ReadCancelled)));
        drop(cancelled_cursor);
        drop(context);
        drop(open);
        let request_released = scope.commit_directory_funding().unwrap().request_child.unwrap();
        assert_eq!(request_released.owner_identity, request_identity);
        assert!(!request_released.alive);
        assert_eq!(request_released.charged_bytes, 0, "known request destruction is observed while the weak scope remains");
        Ok(scope)
    }).unwrap()
}

fn assert_commit_directory_final_drop(scope: &ReadWorkObservationScope) {
    let dead = scope
        .commit_directory_funding()
        .expect("previously bound funding remains observed after destruction");
    assert!(!dead.open_child.alive && !dead.open_child.parent_alive);
    assert_eq!(
        (
            dead.open_child.charged_bytes,
            dead.open_child.parent_charged_bytes
        ),
        (0, 0)
    );
    if let Some(request) = dead.request_child {
        assert!(!request.alive && !request.parent_alive);
        assert_eq!(
            (request.charged_bytes, request.parent_charged_bytes),
            (0, 0)
        );
    }
    assert_eq!(
        (dead.image_alive, dead.image_held_bytes),
        (Some(false), Some(0))
    );
    assert_eq!(
        (
            dead.image_accountant_alive,
            dead.image_accountant_used_bytes
        ),
        (Some(false), Some(0))
    );
    assert_eq!(
        dead.image_physical_free_returned,
        Some(true),
        "the original image requires its actual allocator free event"
    );
    assert_eq!(
        dead.image_accountant_physical_free_returned,
        Some(true),
        "the original accountant requires its actual allocator free event"
    );
}

struct DirectoryProofClock;
impl contextdb_core::read_contract::DeadlineClock for DirectoryProofClock {
    fn now_ms(&self) -> u64 {
        0
    }
    fn wait_until(&self, _: u64) -> contextdb_core::read_contract::DeadlineWait<'_> {
        Box::pin(std::future::pending())
    }
}

// The injected clock reads the existing real suffix-advance receipt. It
// never calls the diagnosis helper or manufactures work/progress counts.
struct SuffixProgressClock {
    scope: Arc<ReadWorkObservationScope>,
    cancellation: contextdb_core::read_contract::OwnerReadCancellation,
    stop: u8,
    deadline: u64,
    entry_reads: std::sync::atomic::AtomicU64,
}
impl contextdb_core::read_contract::DeadlineClock for SuffixProgressClock {
    fn now_ms(&self) -> u64 {
        if self.stop == 4
            && self
                .entry_reads
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                > 0
        {
            return self.deadline.saturating_add(1);
        }
        let progressed = self
            .scope
            .graph_events()
            .iter()
            .any(|event| event.family == "commit_index" && event.operation == "iteration_step");
        if progressed && self.stop == 1 {
            self.cancellation.cancel();
        }
        if progressed && self.stop == 2 {
            self.deadline.saturating_add(1)
        } else {
            0
        }
    }
    fn wait_until(&self, _: u64) -> contextdb_core::read_contract::DeadlineWait<'_> {
        Box::pin(std::future::pending())
    }
}

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn commit_suffix_fixture(path: &Path, frontier: Option<(Lsn, TxId)>, suffix: &[(u64, u64)]) {
    let store = RedbPersistence::create_with_accountant(
        path,
        Some(Arc::new(crate::memory_accounting::MemoryAccountant::no_limit()).into()),
        REDB_CACHE_BYTES,
    )
    .unwrap();
    if let Some(pair) = frontier {
        store
            .flush_commit_index_entries(&BTreeMap::from([pair]))
            .unwrap();
    }
    // These pairs are deliberately uncommitted: do not publish their
    // content-identity frontier. Open must diagnose them before mismatch.
    store
        .with_db(|database| {
            let write = database
                .begin_write()
                .map_err(RedbPersistence::storage_error)?;
            {
                let mut table = write.open_table(COMMIT_INDEX_TABLE).unwrap();
                for &(lsn, tx) in suffix {
                    table.insert(lsn, tx).unwrap();
                }
            }
            write.commit().map_err(RedbPersistence::storage_error)?;
            Ok(())
        })
        .unwrap();
}

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn assert_commit_suffix_open_rows(directory: &Path) {
    use contextdb_core::read_contract::{
        OwnerLimitExceededDetail, OwnerReadCancellation, ReadFailure, ReadFailureLimit, ReadLimits,
    };
    let frontier = Some((Lsn(20), TxId(100)));
    let cases = [
        ("healthy-nonempty-committed", frontier, vec![], None, 0),
        (
            "monotone-three",
            frontier,
            vec![(21, 101), (22, 102), (23, 103)],
            Some((
                ReadImageRequiresWriterCause::WritableSanitizerWouldChange,
                "writable recovery must truncate 3 uncommitted commit-index entries past Some((Lsn(20), TxId(100)))",
            )),
            3,
        ),
        (
            "interior-decrease",
            frontier,
            vec![(21, 101), (22, 103), (23, 102)],
            Some((
                ReadImageRequiresWriterCause::NonMonotonicCommitIndex,
                "commit-index transaction order needs writable repair",
            )),
            3,
        ),
        (
            "first-entry-decrease",
            frontier,
            vec![(21, 99), (22, 101)],
            Some((
                ReadImageRequiresWriterCause::NonMonotonicCommitIndex,
                "commit-index transaction order needs writable repair",
            )),
            1,
        ),
        (
            "empty-frontier-monotone",
            None,
            vec![(1, 1), (2, 2)],
            Some((
                ReadImageRequiresWriterCause::WritableSanitizerWouldChange,
                "writable recovery must truncate 2 uncommitted commit-index entries past None",
            )),
            2,
        ),
        ("empty-frontier-empty", None, vec![], None, 0),
        (
            "maximum-frontier",
            Some((Lsn(u64::MAX), TxId(100))),
            vec![],
            None,
            0,
        ),
    ];
    for (name, frontier, suffix, repair, diagnosed) in cases {
        let path = directory.join(format!("commit-suffix-{name}.db"));
        commit_suffix_fixture(&path, frontier, &suffix);
        let durable_before = std::fs::read(&path).unwrap();
        let parent =
            crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(16 * 1024 * 1024)
                .expect("admit the declared fixture root");
        let baseline = parent.retained_bytes_for_test();
        let scope = Arc::new(ReadWorkObservationScope::enter(ReadWorkPhase::Open));
        let open = crate::executor::bounded::ReadOpenContext::new(
            parent.child().unwrap(),
            ReadLimits::default(),
            Arc::new(DirectoryProofClock),
            OwnerReadCancellation::new(),
        )
        .unwrap();
        let (result, work) =
            read_persistence_test_scaffold::observe_open_history_work_for_test(|| {
                load_read_image_at_registration_with_context(&path, None, None, None, open)
            });
        assert_eq!(
            work.commit_index_scanned, diagnosed,
            "{name}: only anomalous suffix entries are diagnosed"
        );
        assert_eq!(
            read_persistence_test_scaffold::OpenHistoryWork {
                commit_index_scanned: 0,
                ..work
            },
            read_persistence_test_scaffold::OpenHistoryWork::default()
        );
        let advanced = scope
            .graph_events()
            .into_iter()
            .filter(|event| event.family == "commit_index" && event.operation == "iteration_step")
            .collect::<Vec<_>>();
        assert_eq!(advanced.len() as u64, diagnosed);
        assert_eq!(
            advanced
                .iter()
                .map(|event| event.key.clone())
                .collect::<Vec<_>>(),
            suffix
                .iter()
                .take(diagnosed as usize)
                .map(|(lsn, _)| lsn.to_be_bytes().to_vec())
                .collect::<Vec<_>>()
        );
        match repair {
            Some((cause, reason)) => match result {
                Err(LoadReadImageError::RequiresWriter {
                    cause: actual,
                    reason: actual_reason,
                }) => {
                    assert_eq!(actual, cause);
                    assert_eq!(actual_reason, reason);
                }
                _ => panic!("{name}: exact repair type must precede root/count disagreement"),
            },
            None => {
                let loaded = result.unwrap();
                assert_eq!(
                    work,
                    read_persistence_test_scaffold::OpenHistoryWork::default()
                );
                let (image, _receipt, source) = loaded.into_parts();
                assert!(image.commit_index.is_empty());
                drop(image);
                drop(source);
            }
        }
        drop(scope);
        assert_eq!(
            parent.retained_bytes_for_test(),
            baseline,
            "{name}: actual open child releases after physical image/source release"
        );
        assert_eq!(std::fs::read(&path).unwrap(), durable_before);
    }
    for stop in [1, 2, 3, 4] {
        let path = directory.join(format!("commit-suffix-stop-{stop}.db"));
        commit_suffix_fixture(&path, frontier, &[(21, 101), (22, 103), (23, 102)]);
        let before = std::fs::read(&path).unwrap();
        let parent =
            crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(16 * 1024 * 1024)
                .expect("admit the declared fixture root");
        let baseline = parent.retained_bytes_for_test();
        let scope = Arc::new(ReadWorkObservationScope::enter(ReadWorkPhase::Open));
        let cancellation = OwnerReadCancellation::new();
        if stop == 3 {
            cancellation.cancel();
        }
        let limits = ReadLimits::default();
        let clock = Arc::new(SuffixProgressClock {
            scope: scope.clone(),
            cancellation: cancellation.clone(),
            stop,
            deadline: limits.active_ms,
            entry_reads: std::sync::atomic::AtomicU64::new(0),
        });
        let source_events =
            read_persistence_test_scaffold::observe_read_image_source_events_for_test(&path);
        source_events.reset();
        let open = crate::executor::bounded::ReadOpenContext::new(
            parent.child().unwrap(),
            limits,
            clock,
            cancellation,
        );
        let (result, work) =
            read_persistence_test_scaffold::observe_open_history_work_for_test(|| match open {
                Ok(open) => {
                    load_read_image_at_registration_with_context(&path, None, None, None, open)
                }
                Err(error) => Err(LoadReadImageError::Memory(error)),
            });
        match result {
            Err(LoadReadImageError::Memory(Error::ReadCancelled)) if stop == 1 || stop == 3 => {}
            Err(LoadReadImageError::Memory(Error::ReadFailure(failure)))
                if stop == 2 || stop == 4 =>
            {
                assert_eq!(
                    failure,
                    ReadFailure::owner_limit_exceeded(OwnerLimitExceededDetail {
                        limit: ReadFailureLimit::ActiveMs,
                        value: limits.active_ms,
                        required: None,
                        statement: None
                    })
                );
            }
            _ => panic!("real open interruption must retain the existing typed refusal"),
        }
        let diagnosed = if stop == 1 {
            2
        } else if stop == 2 {
            1
        } else {
            0
        };
        assert_eq!(
            work.commit_index_scanned, diagnosed,
            "clock observes real progress; cancellation is read by the following checkpoint"
        );
        assert_eq!(
            read_persistence_test_scaffold::OpenHistoryWork {
                commit_index_scanned: 0,
                ..work
            },
            read_persistence_test_scaffold::OpenHistoryWork::default()
        );
        let advanced = scope
            .graph_events()
            .into_iter()
            .filter(|event| event.family == "commit_index" && event.operation == "iteration_step")
            .collect::<Vec<_>>();
        assert_eq!(
            advanced
                .iter()
                .map(|event| event.key.clone())
                .collect::<Vec<_>>(),
            [21u64, 22]
                .iter()
                .take(diagnosed as usize)
                .map(|lsn| lsn.to_be_bytes().to_vec())
                .collect::<Vec<_>>()
        );
        if stop == 1 || stop == 2 {
            let events = source_events.events();
            let dropped = events
                .iter()
                .position(|event| {
                    matches!(
                        event,
                        read_persistence_test_scaffold::ReadImageSourceEvent::SourceHandlesDropped
                    )
                })
                .unwrap();
            let released = events
                .iter()
                .position(|event| {
                    matches!(
                        event,
                        read_persistence_test_scaffold::ReadImageSourceEvent::Released { .. }
                    )
                })
                .unwrap();
            assert!(
                dropped < released,
                "physical source handles drop before release is certified"
            );
        }
        drop(scope);
        assert_eq!(parent.retained_bytes_for_test(), baseline);
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }
}

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn directory_pinned_view(
    file: &ReadSession,
) -> (
    (usize, Lsn),
    Option<TxId>,
    Option<(Lsn, TxId)>,
    Option<(u64, u64)>,
) {
    file.__with_resident_history_for_test(|database| {
        let open = crate::executor::bounded::ReadOpenContext::new(
            database.declared_reader_memory()?,
            contextdb_core::read_contract::ReadLimits::default(),
            Arc::new(DirectoryProofClock),
            contextdb_core::read_contract::OwnerReadCancellation::new(),
        )?;
        let checkpoint = || open.checkpoint();
        let context = contextdb_tx::ReadCommitContext::new(open.read_memory(), &checkpoint);
        let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
        let census = database.__commit_directory_census_with_context_for_test(&context)?;
        let get = database.__commit_directory_get_with_context_for_test(Lsn(40), &context)?;
        let floor = database.__commit_directory_floor_with_context_for_test(
            std::ops::Bound::Included(Lsn(40)),
            &context,
        )?;
        Ok((census, get, floor, scope.commit_directory_residency()))
    })
    .unwrap()
}

fn assert_directory_pinned_publication_row(directory: &Path) {
    let path = directory.join("commit-directory-pinned-publication.db");
    let (store, _, _) = graph_history_fixture(&path, 4, 4, false);
    drop(store);
    let old = ReadSession::open(&path).unwrap();
    let before = directory_pinned_view(&old);
    assert_eq!(
        before,
        ((6, Lsn(1)), None, Some((Lsn(20), TxId(100))), Some((0, 6)))
    );
    let identity = old.file_snapshot_for_test().unwrap();
    let fixture_open_accountant_42332 =
        Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
    let _fixture_decode_scope_42332 =
        crate::read_image_memory::ImageMemory::enter_writer_operation(
            std::sync::Arc::clone(&fixture_open_accountant_42332).into(),
        )
        .unwrap();
    let store = RedbPersistence::open(&path).unwrap();
    store.attach_memory_accountant(std::sync::Arc::clone(&fixture_open_accountant_42332).into());
    store
        .flush_commit_index_entries(&BTreeMap::from([(Lsn(40), TxId(101))]))
        .unwrap();
    drop(_fixture_decode_scope_42332);
    assert_eq!(
        directory_pinned_view(&old),
        before,
        "installed old directory retains the captured membership and floor after real publication"
    );
    assert_eq!(old.file_snapshot_for_test().unwrap(), identity);
    drop(store);
    let fresh = ReadSession::open(&path).unwrap();
    assert_eq!(
        directory_pinned_view(&fresh),
        (
            (7, Lsn(1)),
            Some(TxId(101)),
            Some((Lsn(40), TxId(101))),
            Some((0, 7))
        )
    );
}

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn assert_commit_directory_writable_controls(directory: &Path) {
    for versions in [4, 8, 16] {
        let path = directory.join(format!("commit-directory-writable-{versions}.db"));
        let (store, _, _) = graph_history_fixture(&path, versions, 4, false);
        let expected = store.load_commit_index().unwrap();
        drop(store);
        let database = Database::open(&path).unwrap();
        assert!(!database.__has_captured_commit_directory_for_test());
        let open = crate::executor::bounded::ReadOpenContext::new(
            database.declared_reader_memory().unwrap(),
            contextdb_core::read_contract::ReadLimits::default(),
            Arc::new(DirectoryProofClock),
            contextdb_core::read_contract::OwnerReadCancellation::new(),
        )
        .unwrap();
        let checkpoint = || open.checkpoint();
        let context = contextdb_tx::ReadCommitContext::new(open.read_memory(), &checkpoint);
        assert_eq!(
            database
                .__commit_directory_census_with_context_for_test(&context)
                .unwrap(),
            (expected.len(), Lsn(1))
        );
        assert_eq!(
            database
                .__commit_directory_floor_with_context_for_test(
                    std::ops::Bound::Included(Lsn(19)),
                    &context
                )
                .unwrap(),
            Some((Lsn(versions as u64 + 1), TxId(versions as u64 + 1)))
        );
        assert_eq!(
            database
                .__commit_directory_floor_with_context_for_test(
                    std::ops::Bound::Included(Lsn(20)),
                    &context
                )
                .unwrap(),
            Some((Lsn(20), TxId(100)))
        );
        assert_eq!(
            database.snapshot_at(Lsn(19)),
            SnapshotId(versions as u64 + 1)
        );
        assert_eq!(database.snapshot_at(Lsn(20)), SnapshotId(100));
    }
}

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
#[expect(
    clippy::needless_borrows_for_generic_args,
    reason = "The fixture explicitly borrows the path passed to the database opener."
)]
fn assert_graph_open_and_maintenance_rows(directory: &Path) {
    assert_commit_directory_writable_controls(directory);
    assert_directory_pinned_publication_row(directory);
    assert_commit_suffix_open_rows(directory);
    let mut control_open = None;
    let mut graph_open_control = None;
    let mut directory_constructor_control = None;
    let mut constructor_accountant_control = None;
    // Fixed stage registries, finalization and store wrappers are admitted
    // separately from the image-held graph source wrapper.
    // The private native accountant handle expands the retained
    // `AdmittedDynStore<CompositeStore>` allocation from 136 to 152
    // bytes. The existing allocator invoice records that real +16-byte
    // control; this fixture therefore observes the complete 848-byte
    // constructor total (the 8-byte wrapper plus these 840 bytes).
    let fixed_constructor_controls_bytes = 840;
    let mut writer_open_control = None;
    let mut query_controls = HashMap::new();
    let mut mutation_control = None;
    // N/2N/4N graph versions and tombstones. No graph work count is
    // asserted through the older row/vector-only observer.
    for versions in [4, 8, 16] {
        let path = directory.join(format!("graph-open-{versions}.db"));
        let (store, predecessor, entries) = graph_history_fixture(&path, versions, 4, false);
        // Durable fixture inspection precedes every operation scope.
        // These are support memberships, not resident directory entries.
        let expected_commit_count = match versions {
            4 => 6,
            8 => 10,
            16 => 18,
            _ => unreachable!(),
        };
        let expected_commits = (1..=versions as u64 + 1)
            .map(|lsn| (Lsn(lsn), TxId(lsn)))
            .chain(std::iter::once((Lsn(20), TxId(100))))
            .collect::<Vec<_>>();
        let physical_commits = store.load_commit_index().unwrap();
        assert_eq!(
            physical_commits.len(),
            expected_commit_count,
            "durable support grows at 4/8/16 versions"
        );
        assert_eq!(
            physical_commits.into_iter().collect::<Vec<_>>(),
            expected_commits,
            "every authored support commit is retained"
        );
        drop(store);
        // Match the public File opener's declared parent and operation
        // child. The image ceiling remains the existing fixture ceiling.
        let limits = ReadSessionOptions::default().limits;
        let declared_read_memory =
            crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(limits.memory)
                .expect("admit the declared fixture root");
        let raw_parent_baseline = declared_read_memory.retained_bytes_for_test();
        let raw_parent_identity = declared_read_memory.as_ref()
            as *const crate::vector_read_memory::DeclaredReadMemory
            as usize;
        let observation = ReadWorkObservationScope::enter(ReadWorkPhase::Open);
        assert_eq!(observation.commit_directory_funding(), None);
        let open_context = crate::executor::bounded::ReadOpenContext::new(
            declared_read_memory.child().unwrap(),
            limits,
            crate::read_session::read_clock(),
            OwnerReadCancellation::new(),
        )
        .unwrap();
        observation.set_operation_id("graph-observer-row");
        let raw_open_identity =
            crate::read_work_probe::funding_owner_id(&open_context.read_memory());
        let (loaded, history_work) =
            read_persistence_test_scaffold::observe_open_history_work_for_test(|| {
                load_read_image_at_registration_with_context(
                    &path,
                    Some(16 * 1024 * 1024),
                    None,
                    None,
                    open_context,
                )
                .unwrap()
            });
        let (image, _receipt, source) = loaded.into_parts();
        assert!(image.change_log.is_empty());
        assert!(image.ddl_log.is_empty());
        assert!(image.commit_index.is_empty());
        assert!(image.history_on_request.is_some());
        assert_eq!(
            history_work,
            read_persistence_test_scaffold::OpenHistoryWork::default()
        );
        // Complete the SAME real captured Database construction while
        // this Open scope is active. Keep the original empty-map and
        // legacy zero-work assertions above byte-identical.
        let raw_image_owner_identity =
            (&*image.memory) as *const crate::read_image_memory::ImageMemory as usize;
        let raw_image_accountant_identity = image.memory.accountant.as_ptr() as usize;
        let raw_image_held = image.memory.held();
        let raw_accountant_used = image.memory.accountant.usage().used;
        let constructor_context = image.open_context.clone();
        let funding_loaded = observation
            .commit_directory_funding()
            .expect("scope entered before native open observes actual child");
        assert_eq!(funding_loaded.open_child.owner_identity, raw_open_identity);
        assert_eq!(
            funding_loaded.open_child.parent_identity,
            raw_parent_identity
        );
        assert_eq!(
            funding_loaded.open_child.parent_charged_bytes,
            declared_read_memory.retained_bytes_for_test()
        );
        // Keep the actual pre-directory child sample as the baseline.
        // Zero is lawful: source cache/verifier/index use ImageMemory;
        // the installed directory must add charge to this same child.
        assert_eq!(
            funding_loaded.request_child, None,
            "no request child was observed during raw open"
        );
        assert_eq!(
            observation.commit_directory_source_registration_funding(),
            None,
            "raw open has not reserved the graph source wrapper"
        );
        let constructor_before = observation.graph_counters();
        let constructor_accountant = image.memory.accountant.clone();
        let (constructed, constructor_history_work) =
            read_persistence_test_scaffold::observe_open_history_work_for_test(|| {
                Database::open_committed_image(
                    image.into_runtime_parts(source.clone()),
                    Arc::new(crate::plugin::CorePlugin),
                    constructor_accountant,
                    declared_read_memory.clone(),
                )
                .unwrap()
            });
        assert_eq!(
            constructor_history_work,
            read_persistence_test_scaffold::OpenHistoryWork::default()
        );
        let constructor_after = observation.graph_counters();
        let constructor_gets =
            constructor_after.graph_commit_gets - constructor_before.graph_commit_gets;
        assert!(constructed.__has_captured_commit_directory_for_test());
        if let Some(control) = directory_constructor_control {
            assert_eq!(
                constructor_gets, control,
                "real directory construction adds the same fixed endpoint count at N/2N/4N"
            );
        } else {
            directory_constructor_control = Some(constructor_gets);
        }
        let constructor_keys = observation
            .graph_events()
            .into_iter()
            .filter(|event| event.family == "commit_index" && event.operation == "get")
            .map(|event| event.key)
            .collect::<Vec<_>>();
        assert_eq!(
            constructor_keys,
            [
                expected_commits.first().unwrap().0,
                expected_commits.last().unwrap().0
            ]
            .into_iter()
            .map(|lsn| lsn.0.to_be_bytes().to_vec())
            .collect::<Vec<_>>(),
            "both real constructor endpoint accesses remain visible"
        );
        assert_eq!(
            constructor_gets,
            [expected_commits.first(), expected_commits.last()]
                .into_iter()
                .flatten()
                .count() as u64,
            "real nonempty directory construction verifies exactly its two endpoints at every 4/8/16 fixture"
        );
        assert_eq!(
            observation.commit_directory_residency(),
            Some((0, expected_commit_count as u64)),
            "actual installed constructor pairs zero resident entries with growing durable support"
        );
        let constructor_checkpoint = || constructor_context.checkpoint();
        let constructor_request = contextdb_tx::ReadCommitContext::new(
            constructor_context.read_memory(),
            &constructor_checkpoint,
        );
        constructed
            .__commit_directory_census_with_context_for_test(&constructor_request)
            .unwrap();
        let funding_constructed = observation.commit_directory_funding().unwrap();
        let source_registration = observation.commit_directory_source_registration_funding().expect("real captured Database construction observes its successful graph source wrapper reservation");
        assert_eq!(
            source_registration.image_owner_identity,
            raw_image_owner_identity
        );
        assert_eq!(
            source_registration.image_accountant_identity,
            raw_image_accountant_identity
        );
        assert_eq!(
            source_registration.image_held_before_reservation,
            raw_image_held
        );
        assert_eq!(
            source_registration.image_accountant_used_before_reservation,
            raw_accountant_used
        );
        let wrapper_held_delta = source_registration
            .image_held_after_reservation
            .checked_sub(source_registration.image_held_before_reservation)
            .expect(
                "successful source wrapper reservation increases its actual image owner counter",
            );
        let wrapper_accountant_delta = source_registration
            .image_accountant_used_after_reservation
            .checked_sub(source_registration.image_accountant_used_before_reservation)
            .expect(
                "successful source wrapper reservation increases its actual accountant counter",
            );
        assert_eq!(
            wrapper_held_delta, 8,
            "the image-held graph source wrapper remains exactly eight bytes"
        );
        assert_eq!(
            wrapper_accountant_delta, wrapper_held_delta,
            "the same native reservation funds image ownership and its accountant"
        );
        assert_eq!(
            funding_constructed
                .image_held_bytes
                .and_then(|held| held.checked_sub(raw_image_held)),
            Some(wrapper_held_delta),
            "the old image observation window contains exactly the observed wrapper reservation"
        );
        let constructor_accountant_delta = funding_constructed
            .image_accountant_used_bytes
            .and_then(|used| used.checked_sub(raw_accountant_used))
            .expect("construction retains its wrapper and fixed controls on the actual accountant");
        assert_eq!(
            constructor_accountant_delta,
            wrapper_accountant_delta + fixed_constructor_controls_bytes,
            "the accountant holds the wrapper plus fixed stage registries, finalization and store wrappers"
        );
        if let Some(control) = constructor_accountant_control {
            assert_eq!(
                constructor_accountant_delta, control,
                "constructor accountant bytes are identical across 4/8/16 store sizes"
            );
        } else {
            constructor_accountant_control = Some(constructor_accountant_delta);
        }
        assert_eq!(
            funding_constructed.open_child.owner_identity,
            raw_open_identity
        );
        assert!(
            funding_constructed.open_child.charged_bytes > funding_loaded.open_child.charged_bytes,
            "installed directory controls use the declared open child"
        );
        assert_eq!(
            funding_constructed.open_child.parent_charged_bytes
                - funding_loaded.open_child.parent_charged_bytes,
            funding_constructed.open_child.charged_bytes - funding_loaded.open_child.charged_bytes
        );
        assert_eq!(
            funding_constructed.image_held_bytes,
            Some(source_registration.image_held_after_reservation),
            "directory controls do not charge ImageMemory instead"
        );
        assert_eq!(
            funding_constructed.image_accountant_used_bytes,
            Some(
                source_registration.image_accountant_used_after_reservation
                    + fixed_constructor_controls_bytes
            ),
            "the actual image accountant also holds the separately admitted fixed constructor controls"
        );
        drop(constructor_request);
        drop(constructor_context);
        drop(constructed);
        let observed = observation.counters();
        assert_eq!(observed.decoded_rows, 0);
        assert_eq!(observed.decoded_vectors, 0);
        assert_eq!(observed.hashed_rows, 0);
        assert_eq!(observed.hashed_vectors, 0);
        if let Some(control) = control_open {
            assert_eq!(observed, control);
        } else {
            control_open = Some(observed);
        }
        let graph_open =
            assert_graph_open_work(&observation, &format!("{:?}", source.storage_identity()));
        if let Some(control) = graph_open_control {
            assert_eq!(
                graph_open, control,
                "captured image open graph work stays fixed across 4/8/16 versions"
            );
        } else {
            graph_open_control = Some(graph_open);
        }
        let query_work = graph_addressed_observer_row(
            &graph_read::GraphReadRoot::Image(source.clone()),
            &predecessor,
            true,
            Lsn(30),
            0,
            &entries[0],
        );
        if let Some(control) = query_controls.get(&false) {
            assert_eq!(&query_work, control);
        } else {
            query_controls.insert(false, query_work);
        }
        assert_graph_source_answers(
            graph_read::GraphReadRoot::Image(source),
            &predecessor,
            true,
            &entries,
        );
        assert_eq!(
            declared_read_memory.retained_bytes_for_test(),
            raw_parent_baseline
        );
        drop(declared_read_memory);
        assert_commit_directory_final_drop(&observation);
        drop(observation);
        // Exercise the public File door on the same graph-only fallback
        // fixture, without publishing readiness or removing support.
        let public_open = ReadWorkObservationScope::enter(ReadWorkPhase::Open);
        public_open.set_operation_id("graph-observer-row");
        let (file, public_history_work) =
            read_persistence_test_scaffold::observe_open_history_work_for_test(|| {
                ReadSession::open(&path).unwrap()
            });
        assert_eq!(file.route(), ReadRoute::File);
        assert_eq!(
            public_history_work,
            read_persistence_test_scaffold::OpenHistoryWork::default()
        );
        assert_graph_context(&public_open, ReadWorkPhase::Open);
        assert_eq!(
            public_open.graph_counters(),
            graph_open,
            "public File open has the same fixed graph work as the captured source control"
        );
        let public_constructor_gets = public_open
            .graph_events()
            .iter()
            .filter(|event| event.family == "commit_index" && event.operation == "get")
            .count() as u64;
        assert_eq!(
            public_constructor_gets, constructor_gets,
            "RAW and public File complete the same real endpoint verification, fixed across every 4/8/16 fixture"
        );
        let public_constructor_keys = public_open
            .graph_events()
            .into_iter()
            .filter(|event| event.family == "commit_index" && event.operation == "get")
            .map(|event| event.key)
            .collect::<Vec<_>>();
        assert_eq!(
            public_constructor_keys, constructor_keys,
            "public File verifies the same first/last durable keys as the real RAW constructor at every 4/8/16 fixture"
        );
        assert_eq!(
            public_open.commit_directory_residency(),
            Some((0, expected_commit_count as u64))
        );
        drop(public_open);
        let funding_scope = assert_installed_commit_directory_rows(&file, &expected_commits, &path);
        let registration = file.file_storage_registration_for_test().unwrap();
        assert!(registration.descriptor_open);
        assert!(!registration.load_lock_held);
        assert_eq!(
            file.file_retained_charge_bytes_for_test(),
            Some(0),
            "the captured public reader never borrows the writer retention door"
        );
        let snapshot = file.file_snapshot_for_test().unwrap();
        let (_, _, _, file_memory) = file.file_memory_for_test().unwrap();
        let answer = file
            .metadata(
                crate::MetadataRequest::ImageState {
                    kind: crate::DirectImageMetadataKind::ChangeLog,
                },
                None,
            )
            .unwrap();
        let crate::direct_file_reader::DirectMetadataBody::ImageState {
            state: crate::direct_file_reader::DirectImageState::ChangeLog(changes),
        } = &answer.body
        else {
            panic!("the requested public history metadata returns its change state");
        };
        assert_eq!(
            changes.commit_index.len(),
            expected_commit_count,
            "public output preserves durable membership census"
        );
        assert_eq!(
            changes.commit_index, expected_commits,
            "the admitted public collector returns every authored support pair in order"
        );
        assert_eq!(
            file.file_snapshot_for_test().unwrap(),
            snapshot,
            "requesting history preserves the public session's committed identity"
        );
        drop(answer);
        drop(file);
        assert!(
            !file_memory.is_alive()
                && file_memory.is_physically_freed()
                && file_memory.accountant_is_physically_freed(),
            "the final public File handle releases its actual image-memory owner"
        );
        assert_commit_directory_final_drop(&funding_scope);
        drop(funding_scope);
        let fixture_open_accountant_42536 =
            Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
        let _fixture_decode_scope_42536 =
            crate::read_image_memory::ImageMemory::enter_writer_operation(
                std::sync::Arc::clone(&fixture_open_accountant_42536).into(),
            )
            .unwrap();
        let store = RedbPersistence::open(&path).unwrap();
        store
            .attach_memory_accountant(std::sync::Arc::clone(&fixture_open_accountant_42536).into());
        let root = store
            .initialize_writer_snapshot(SnapshotId(100), Lsn(30))
            .unwrap();
        let writer_observation = ReadWorkObservationScope::enter(ReadWorkPhase::Open);
        writer_observation.set_operation_id("graph-observer-row");
        let captured_read = root.owner_read().unwrap();
        RedbPersistence::read_authenticated_bounds_in_read(&captured_read).unwrap();
        let writer_open = assert_graph_open_work(
            &writer_observation,
            &format!("{:?}", root.storage_identity()),
        );
        if let Some(control) = writer_open_control {
            assert_eq!(
                writer_open, control,
                "captured writer root validation stays fixed across 4/8/16 versions"
            );
        } else {
            writer_open_control = Some(writer_open);
        }
        drop(captured_read);
        drop(writer_observation);
        let query_work = graph_addressed_observer_row(
            &graph_read::GraphReadRoot::Writer(root.clone()),
            &predecessor,
            true,
            Lsn(30),
            0,
            &entries[0],
        );
        if let Some(control) = query_controls.get(&true) {
            assert_eq!(&query_work, control);
        } else {
            query_controls.insert(true, query_work);
        }
        assert_graph_source_answers(
            graph_read::GraphReadRoot::Writer(root),
            &predecessor,
            true,
            &entries,
        );
    }

    // One predecessor with 1/N/2N retained references. Retirement must
    // preserve each witness, location and deletion contribution exactly.
    for references in [1, 4, 8] {
        for repack in [false, true] {
            let path = directory.join(format!("graph-retirement-{references}-{repack}.db"));
            let (store, predecessor, entries) = graph_history_fixture(&path, 4, references, repack);
            let before = graph_publication_state(&store);
            let first_ordinal = usize::from(repack);
            let private_bytes = before.1[0]
                .iter()
                .skip(first_ordinal)
                .map(|(_, bytes)| bytes.clone())
                .collect::<Vec<_>>();
            assert_retained_graph_witnesses(
                &store,
                &predecessor,
                &private_bytes,
                first_ordinal,
                false,
            );
            drop(store);
            let (image, _receipt, old_source) =
                load_read_image_with_memory(&path, Some(16 * 1024 * 1024))
                    .unwrap()
                    .into_parts();
            drop(image);
            let fixture_open_accountant_42588 =
                Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
            let _fixture_decode_scope_42588 =
                crate::read_image_memory::ImageMemory::enter_writer_operation(
                    std::sync::Arc::clone(&fixture_open_accountant_42588).into(),
                )
                .unwrap();
            let store = RedbPersistence::open(&path).unwrap();
            store.attach_memory_accountant(
                std::sync::Arc::clone(&fixture_open_accountant_42588).into(),
            );
            let old_writer = store
                .initialize_writer_snapshot(SnapshotId(100), Lsn(30))
                .unwrap();
            let removed = if repack {
                vec![("removed_rows".to_owned(), RowId(1), Lsn(30))]
            } else {
                Vec::new()
            };
            let (row_meta, physical_row) = graph_retention_row();
            let row_keys = if repack {
                vec![(
                    "removed_rows".to_owned(),
                    physical_row.row_id,
                    physical_row.created_tx,
                    physical_row.lsn,
                )]
            } else {
                Vec::new()
            };
            let table_meta = if repack {
                HashMap::from([("removed_rows".to_owned(), row_meta.clone())])
            } else {
                HashMap::new()
            };
            // Existing returned-error checkpoints exercise aborted history
            // repack. They do not cover the later graph retirement sites.
            for boundary in [1, 2].into_iter().filter(|_| repack) {
                let durable = std::cell::Cell::new(false);
                arm_currency_reclaim_fault_for_test(boundary, false);
                let error = store
                    .prune_currency_scoped(
                        &[],
                        &removed,
                        &[],
                        &[],
                        &Vec::new(),
                        &durable,
                        &[],
                        None,
                    )
                    .unwrap_err();
                assert!(
                    matches!(error, Error::Other(ref message) if message == "injected currency cleanup failure")
                );
                assert!(!durable.get());
                assert_eq!(
                    graph_publication_state(&store),
                    before,
                    "precommit repack refusal leaves all proof bytes and root bindings unchanged"
                );
            }
            let deferred = HashSet::new();
            let retained_zero_keys = if repack {
                Vec::new()
            } else {
                graph_retained_zero_touch_keys(&before)
            };
            let mutation_scope = graph_retirement_observer(&before);
            let stats = store
                .prune_retention_scoped(
                    &table_meta,
                    &row_keys,
                    &removed,
                    &[],
                    Some((&[], std::slice::from_ref(&predecessor), &deferred)),
                    &[],
                    &Vec::new(),
                    &[("graph_fixture_progress".into(), vec![1])],
                )
                .unwrap();
            let mutation_counts =
                assert_graph_retirement_work(&mutation_scope, &predecessor, repack);
            if !repack {
                assert_graph_retained_zero_touch(&mutation_scope, &retained_zero_keys);
            }
            if !repack {
                if let Some(control) = mutation_control {
                    assert_eq!(
                        mutation_counts, control,
                        "one retirement has constant selected-family work across 1/4/8 retained references"
                    );
                } else {
                    mutation_control = Some(mutation_counts);
                }
            }
            drop(mutation_scope);
            assert_eq!(stats.row_keys_removed, u64::from(repack));
            if repack {
                assert!(
                    store
                        .load_relational_row_version(
                            "removed_rows",
                            &row_meta,
                            physical_row.row_id,
                            physical_row.created_tx,
                            physical_row.lsn
                        )
                        .unwrap()
                        .is_none()
                );
            }
            assert_eq!(stats.change_log_keys_removed, u64::from(repack));
            assert_eq!(stats.edge_keys_removed, 2);
            assert_retained_graph_witnesses(&store, &predecessor, &private_bytes, 0, true);
            let after = graph_publication_state(&store);
            assert!(after.1[3].contains(&("graph_fixture_progress".to_owned(), vec![1])));
            assert!(
                !before.1[3]
                    .iter()
                    .any(|(key, _)| key == "graph_fixture_progress")
            );
            if repack {
                assert_eq!(before.0[5].len(), 1);
                assert!(
                    after.0[5].is_empty(),
                    "the selected physical row payload joins the same publication"
                );
                assert_graph_retention_row_source(
                    &graph_read::GraphReadRoot::Writer(old_writer.clone()),
                    true,
                );
                assert_graph_retention_row_source(
                    &graph_read::GraphReadRoot::Image(old_source.clone()),
                    true,
                );
            }
            assert_eq!(
                before.0[3],
                after.0[3]
                    .iter()
                    .map(|(key, value)| {
                        let mut old = value.clone();
                        let ordinal = u64::from_be_bytes(old[9..17].try_into().unwrap());
                        old[9..17].copy_from_slice(&(ordinal + first_ordinal as u64).to_be_bytes());
                        (key.clone(), old)
                    })
                    .collect::<Vec<_>>(),
                "only the retained locations' ordinals move"
            );
            for (key, value) in &before.0[2] {
                if key[17] == 1 {
                    assert!(
                        after.0[2].contains(&(key.clone(), value.clone())),
                        "repacking and predecessor retirement preserve deletion C bytes"
                    );
                }
            }
            let p_key = RedbPersistence::graph_fwd_key(&predecessor);
            let r_key = RedbPersistence::graph_rev_key(&predecessor);
            for (family, selected) in [(0, p_key.as_slice()), (1, r_key.as_slice())] {
                let expected = before.0[family]
                    .iter()
                    .filter(|(key, _)| key.as_slice() != selected)
                    .cloned()
                    .collect::<Vec<_>>();
                assert_eq!(
                    after.0[family], expected,
                    "unrelated graph versions remain exact"
                );
            }
            let creation = graph_history_record::creation_contribution_key(&p_key).unwrap();
            let expected_c = before.0[2]
                .iter()
                .filter(|(key, _)| key != &creation)
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(
                after.0[2], expected_c,
                "only the selected creation contribution is removed"
            );
            let mut expected_r = before.0[4].clone();
            for (_, value) in &mut expected_r {
                value[17] = 1;
            }
            assert_eq!(
                after.0[4], expected_r,
                "retirement changes only the exact receipt state"
            );
            if !repack {
                assert_eq!(
                    after.0[3], before.0[3],
                    "retirement preserves all survivor location bytes"
                );
                assert_eq!(
                    after.1[0], before.1[0],
                    "retirement preserves all survivor history bytes"
                );
            }
            assert_ne!(
                before.1[4], after.1[4],
                "final root binding changes with the publication"
            );
            assert_ne!(
                before.1[1], after.1[1],
                "recovery/root bindings change with the publication"
            );
            assert_graph_source_answers(
                graph_read::GraphReadRoot::Writer(old_writer),
                &predecessor,
                true,
                &entries,
            );
            assert_graph_source_answers(
                graph_read::GraphReadRoot::Image(old_source),
                &predecessor,
                true,
                &entries,
            );
            let new_writer = store
                .prepare_writer_snapshot(
                    store.reserve_writer_snapshot().unwrap(),
                    SnapshotId(100),
                    Lsn(30),
                )
                .unwrap();
            if repack {
                assert_graph_retention_row_source(
                    &graph_read::GraphReadRoot::Writer(new_writer.clone()),
                    false,
                );
            }
            assert_graph_source_answers(
                graph_read::GraphReadRoot::Writer(new_writer),
                &predecessor,
                false,
                &entries[first_ordinal..],
            );
            drop(store);
            drop(_fixture_decode_scope_42588);
            let (image, _receipt, new_source) =
                load_read_image_with_memory(&path, Some(16 * 1024 * 1024))
                    .unwrap()
                    .into_parts();
            drop(image);
            if repack {
                assert_graph_retention_row_source(
                    &graph_read::GraphReadRoot::Image(new_source.clone()),
                    false,
                );
            }
            assert_graph_source_answers(
                graph_read::GraphReadRoot::Image(new_source),
                &predecessor,
                false,
                &entries[first_ordinal..],
            );
            let fixture_open_accountant_42789 =
                std::sync::Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
            let _fixture_decode_scope_42789 =
                crate::read_image_memory::ImageMemory::enter_writer_operation(
                    std::sync::Arc::clone(&fixture_open_accountant_42789).into(),
                )
                .unwrap();
            let reopened = RedbPersistence::open(&path).unwrap();
            reopened.attach_memory_accountant(
                std::sync::Arc::clone(&fixture_open_accountant_42789).into(),
            );
            if repack {
                assert!(
                    reopened
                        .load_relational_row_version(
                            "removed_rows",
                            &row_meta,
                            physical_row.row_id,
                            physical_row.created_tx,
                            physical_row.lsn
                        )
                        .unwrap()
                        .is_none()
                );
            }
            assert_eq!(
                graph_publication_state(&reopened),
                after,
                "restart retains the complete new history/proof/root publication"
            );
        }
    }
}

// Exact durable bytes complement the existing public graph/history/root
// oracle with private history, selectors, read/clock bounds and commits.
// All census work is fixture inspection outside the mutation observer.
type GraphDurabilityState = (
    GraphPublicationState,
    Vec<Vec<(Vec<u8>, Vec<u8>)>>,
    Vec<Vec<(String, u64)>>,
    Vec<(u64, u64)>,
    BTreeMap<Lsn, TxId>,
    Vec<(String, Option<Vec<(Vec<u8>, Vec<u8>)>>)>,
);

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn graph_durability_state(store: &RedbPersistence) -> GraphDurabilityState {
    let supplement = store
        .with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let mut byte_tables = Vec::new();
            for definition in [
                writer_history::WRITER_CHANGE_LOG_TABLE,
                writer_history::CHANGE_LOG_BY_TABLE,
            ] {
                let table = read
                    .open_table(definition)
                    .map_err(RedbPersistence::storage_error)?;
                let mut rows = Vec::new();
                for next in table.iter().map_err(RedbPersistence::storage_error)? {
                    let (key, value) = next.map_err(RedbPersistence::storage_error)?;
                    rows.push((key.value().to_vec(), value.value().to_vec()));
                }
                byte_tables.push(rows);
            }
            let mut bounds = Vec::new();
            for name in ["retention_clock_bounds", "committed_read_lsn_bounds"] {
                let definition: TableDefinition<&str, u64> = TableDefinition::new(name);
                let table = read
                    .open_table(definition)
                    .map_err(RedbPersistence::storage_error)?;
                let mut rows = Vec::new();
                for next in table.iter().map_err(RedbPersistence::storage_error)? {
                    let (key, value) = next.map_err(RedbPersistence::storage_error)?;
                    rows.push((key.value().to_owned(), value.value()));
                }
                bounds.push(rows);
            }
            let table = read
                .open_table(writer_history::CHANGE_LOG_LSN_COUNT)
                .map_err(RedbPersistence::storage_error)?;
            let mut counts = Vec::new();
            for next in table.iter().map_err(RedbPersistence::storage_error)? {
                let (key, value) = next.map_err(RedbPersistence::storage_error)?;
                counts.push((key.value(), value.value()));
            }
            // Capture the actual related durable families, including table
            // absence. The fixture has no vector members; these bytes certify
            // their unchanged state, not populated-vector removal coverage.
            let mut publication_tables = Vec::new();
            for definition in [
                VECTORS_TABLE,
                VECTOR_PARTITION_MEMBERSHIP_TABLE,
                VECTOR_OPEN_DIRECTORY_TABLE,
                VECTOR_PARTITION_JOURNAL_TABLE,
                VECTOR_PARTITION_TOMBSTONE_TABLE,
                VECTOR_PARTITION_BASE_GENERATION_TABLE,
                VECTOR_PARTITION_CHANGE_GENERATION_TABLE,
                VECTOR_PARTITION_GRAPH_PAGE_TABLE,
                VECTOR_PARTITION_GENERATION_CATALOG_TABLE,
                vector_membership_pages::SOURCE,
                vector_membership_pages::REVERSE,
                vector_membership_pages::SUMMARY,
                TableDefinition::new("writer_row_heads"),
                CONTENT_IDENTITY_NODES_TABLE,
            ] {
                let rows = match read.open_table(definition) {
                    Ok(table) => {
                        let mut rows = Vec::new();
                        for next in table.iter().map_err(RedbPersistence::storage_error)? {
                            let (key, value) = next.map_err(RedbPersistence::storage_error)?;
                            rows.push((key.value().to_vec(), value.value().to_vec()));
                        }
                        Some(rows)
                    }
                    Err(redb::TableError::TableDoesNotExist(_)) => None,
                    Err(error) => return Err(RedbPersistence::storage_error(error)),
                };
                publication_tables.push((definition.name().to_owned(), rows));
            }
            let identity_roots = match read.open_table(CONTENT_IDENTITY_ROOTS_TABLE) {
                Ok(table) => {
                    let mut rows = Vec::new();
                    for next in table.iter().map_err(RedbPersistence::storage_error)? {
                        let (key, value) = next.map_err(RedbPersistence::storage_error)?;
                        rows.push((vec![key.value()], value.value().to_vec()));
                    }
                    Some(rows)
                }
                Err(redb::TableError::TableDoesNotExist(_)) => None,
                Err(error) => return Err(RedbPersistence::storage_error(error)),
            };
            publication_tables.push((
                CONTENT_IDENTITY_ROOTS_TABLE.name().to_owned(),
                identity_roots,
            ));
            Ok((byte_tables, bounds, counts, publication_tables))
        })
        .unwrap();
    (
        graph_publication_state(store),
        supplement.0,
        supplement.1,
        supplement.2,
        store.load_commit_index().unwrap(),
        supplement.3,
    )
}

// Same mixed row/history/predecessor transaction as the existing semantic
// row. This helper only removes its fixture's nominated physical version.
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn graph_durability_publication(
    store: &RedbPersistence,
    predecessor: &AdjEntry,
) -> Result<PruneScopedStats> {
    let (meta, row) = graph_retention_row();
    let deferred = HashSet::new();
    store.prune_retention_scoped(
        &HashMap::from([("removed_rows".to_owned(), meta)]),
        &[(
            "removed_rows".to_owned(),
            row.row_id,
            row.created_tx,
            row.lsn,
        )],
        &[("removed_rows".to_owned(), RowId(1), Lsn(30))],
        &[],
        Some((&[], std::slice::from_ref(predecessor), &deferred)),
        &[],
        &Vec::new(),
        &[("graph_fixture_progress".into(), vec![1])],
    )
}

// Publication attempts are linked to actual transaction fingerprints.
// A missing executing root is a failed observation, never a zero oracle.
fn assert_graph_durability_observation(
    scope: &ReadWorkObservationScope,
    committed: bool,
    boundary: Option<u8>,
) {
    assert_eq!(
        scope.graph_counters().graph_publication_commits,
        u64::from(committed)
    );
    let events = scope.graph_events();
    let begins = events
        .iter()
        .filter(|event| event.family == "publication" && event.operation == "begin")
        .collect::<Vec<_>>();
    assert_eq!(begins.len(), 1);
    let captured = begins[0]
        .captured_root
        .as_ref()
        .expect("the executing write has an observed root");
    assert_eq!(begins[0].observation_status, "OBSERVED");
    let outcomes = events
        .iter()
        .filter(|event| {
            event.family == "publication"
                && matches!(event.operation.as_str(), "committed" | "aborted")
        })
        .collect::<Vec<_>>();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(
        outcomes[0].operation,
        if committed { "committed" } else { "aborted" }
    );
    assert_eq!(outcomes[0].captured_root.as_ref(), Some(captured));
    assert_eq!(outcomes[0].observation_status, "OBSERVED");
    assert!(events.iter().any(|event| event.family == "root_binding"
        && event.operation == "root"
        && event.acquired_root.is_some()));
    if let Some(boundary) = boundary {
        assert!(
            events.iter().any(|event| event.family == "publication"
                && event.operation == "checkpoint"
                && event.key == vec![boundary]
                && event.role.as_deref() == Some("currency_reclaim_checkpoint_for_test")),
            "the selected returned-error checkpoint was actually reached"
        );
    }
    // A successful control must visit every distinct new stage. A fault
    // row must visit its own stage exactly once under the executing root.
    let stages = match boundary {
        None => (12..=26).collect::<Vec<u8>>(),
        Some(stage @ 12..=26) => vec![stage],
        Some(_) => Vec::new(),
    };
    for stage in stages {
        let checkpoints = events
            .iter()
            .filter(|event| {
                event.family == "publication"
                    && event.operation == "checkpoint"
                    && event.key == vec![stage]
                    && event.role.as_deref() == Some("currency_reclaim_checkpoint_for_test")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            checkpoints.len(),
            1,
            "publication stage {stage} must execute exactly once"
        );
        assert_eq!(
            checkpoints[0].observation_status, "OBSERVED",
            "publication stage {stage} must carry an executing root"
        );
        assert_eq!(
            checkpoints[0].captured_root.as_ref(),
            Some(captured),
            "publication stage {stage} belongs to this same transaction"
        );
    }
}

// The mapped test is its own isolated-process entry point; no additional
// test name, global fault cell or production injection hook is introduced.
fn graph_durability_interruption_child() -> bool {
    let Some(path) = std::env::var_os("CONTEXTDB_GRAPH_DURABILITY_PATH") else {
        return false;
    };
    let boundary: u8 = std::env::var("CONTEXTDB_GRAPH_DURABILITY_BOUNDARY")
        .expect("child boundary")
        .parse()
        .expect("numeric child boundary");
    if boundary >= 12 {
        assert!((12..=26).contains(&boundary));
    } else {
        assert!((4..=11).contains(&boundary));
    }
    let fixture_open_accountant_42956 =
        Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
    let _fixture_decode_scope_42956 =
        crate::read_image_memory::ImageMemory::enter_writer_operation(
            std::sync::Arc::clone(&fixture_open_accountant_42956).into(),
        )
        .unwrap();
    let store =
        RedbPersistence::open(Path::new(&path)).expect("open the parent's fresh temporary fixture");
    store.attach_memory_accountant(std::sync::Arc::clone(&fixture_open_accountant_42956).into());
    let candidates = store
        .load_graph_table(GRAPH_FWD_TABLE)
        .unwrap()
        .into_iter()
        .filter(|edge| {
            edge.source == NodeId::from_u128(1)
                && edge.target == NodeId::from_u128(2)
                && edge.edge_type == "retained_link"
        })
        .collect::<Vec<_>>();
    assert_eq!(candidates.len(), 1);
    arm_retention_reclaim_crash_for_test(boundary);
    let result = graph_durability_publication(&store, &candidates[0]);
    panic!("selected publication interruption {boundary} was not reached: {result:?}");
}

fn assert_graph_durability_rows(directory: &Path) {
    let fixture_path = directory.join("graph-durability-fixture.db");
    let (fixture, predecessor, entries) = graph_history_fixture(&fixture_path, 4, 4, true);
    let before = graph_durability_state(&fixture);
    let private_bytes = before.0.1[0]
        .iter()
        .skip(1)
        .map(|(_, bytes)| bytes.clone())
        .collect::<Vec<_>>();
    assert_retained_graph_witnesses(&fixture, &predecessor, &private_bytes, 1, false);
    assert_eq!(before.0.0[5].len(), 1);
    assert!(
        !before.0.1[3]
            .iter()
            .any(|(key, _)| key == "graph_fixture_progress")
    );
    drop(fixture);

    // Copy only the closed, test-owned database. A separate successful
    // transaction supplies the whole new state, including root bindings.
    let control_path = directory.join("graph-durability-control.db");
    std::fs::copy(&fixture_path, &control_path).unwrap();
    let fixture_open_accountant_42979 =
        Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
    let _fixture_decode_scope_42979 =
        crate::read_image_memory::ImageMemory::enter_writer_operation(
            std::sync::Arc::clone(&fixture_open_accountant_42979).into(),
        )
        .unwrap();
    let control = RedbPersistence::open(&control_path).unwrap();
    control.attach_memory_accountant(std::sync::Arc::clone(&fixture_open_accountant_42979).into());
    assert_eq!(graph_durability_state(&control), before);
    let observation = ReadWorkObservationScope::enter(ReadWorkPhase::Mutation);
    let stats = graph_durability_publication(&control, &predecessor).unwrap();
    assert_graph_durability_observation(&observation, true, None);
    drop(observation);
    assert_eq!(stats.row_keys_removed, 1);
    assert_eq!(stats.change_log_keys_removed, 1);
    assert_eq!(stats.edge_keys_removed, 2);
    let after = graph_durability_state(&control);
    assert_ne!(before, after);
    assert!(after.0.0[5].is_empty());
    assert!(after.0.1[3].contains(&("graph_fixture_progress".to_owned(), vec![1])));
    assert_ne!(before.0.1[1], after.0.1[1]);
    assert_ne!(before.0.1[4], after.0.1[4]);
    assert_retained_graph_witnesses(&control, &predecessor, &private_bytes, 0, true);
    drop(control);
    drop(_fixture_decode_scope_42979);

    for boundary in (4..=11).chain(12..=26) {
        let path = directory.join(format!("graph-durability-interruption-{boundary}.db"));
        std::fs::copy(&fixture_path, &path).unwrap();
        let executable = std::env::current_exe().unwrap();
        let module = module_path!().split_once("::").expect("crate and module").1;
        let exact_name = format!(
            "{module}::empty_declared_index_publishes_marker_and_postings_for_a_lazy_ordered_read"
        );
        let output = std::process::Command::new(executable)
            .args(["--exact", &exact_name, "--nocapture", "--test-threads=1"])
            .env("CONTEXTDB_GRAPH_DURABILITY_PATH", &path)
            .env("CONTEXTDB_GRAPH_DURABILITY_BOUNDARY", boundary.to_string())
            .current_dir(directory)
            .output()
            .expect("dispatch the exact mapped test in a separate process");
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let marker = format!("RETENTION_RECLAIM_CRASH={boundary}");
        assert_eq!(
            stderr.lines().filter(|line| *line == marker).count(),
            1,
            "the selected boundary marker is required: {stderr}"
        );
        assert_eq!(
            stderr
                .lines()
                .filter(|line| line.starts_with("RETENTION_RECLAIM_CRASH="))
                .count(),
            1
        );
        assert!(
            !output.status.success(),
            "the intended interruption cannot return success"
        );
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(
                output.status.signal(),
                Some(libc::SIGABRT),
                "a test failure is not the intended interruption: {stdout}\n{stderr}"
            );
        }
        assert!(
            !stdout.contains("FAILED"),
            "libtest failure is not a boundary interruption: {stdout}"
        );
        assert!(
            !stderr.contains("panicked at"),
            "a panic is not a boundary interruption: {stderr}"
        );
        let expected = if boundary == 11 { &after } else { &before };
        let fixture_open_accountant_43023 =
            Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
        let _fixture_decode_scope_43023 =
            crate::read_image_memory::ImageMemory::enter_writer_operation(
                std::sync::Arc::clone(&fixture_open_accountant_43023).into(),
            )
            .unwrap();
        let reopened = RedbPersistence::open(&path).unwrap();
        reopened
            .attach_memory_accountant(std::sync::Arc::clone(&fixture_open_accountant_43023).into());
        assert_eq!(
            &graph_durability_state(&reopened),
            expected,
            "boundary {boundary} must expose the complete old or complete new publication after reopen"
        );
        assert_retained_graph_witnesses(
            &reopened,
            &predecessor,
            &private_bytes,
            usize::from(boundary != 11),
            boundary == 11,
        );
        let root = reopened
            .initialize_writer_snapshot(SnapshotId(100), Lsn(30))
            .unwrap();
        assert_graph_retention_row_source(
            &graph_read::GraphReadRoot::Writer(root.clone()),
            boundary != 11,
        );
        assert_graph_source_answers(
            graph_read::GraphReadRoot::Writer(root),
            &predecessor,
            boundary != 11,
            if boundary == 11 {
                &entries[1..]
            } else {
                &entries
            },
        );
        drop(reopened);
        drop(_fixture_decode_scope_43023);

        // Consume the existing one-shot returned-error cell in the same
        // mixed transaction. It covers only the named instrumented sites.
        let error_path = directory.join(format!("graph-durability-error-{boundary}.db"));
        std::fs::copy(&fixture_path, &error_path).unwrap();
        let fixture_open_accountant_43036 =
            Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
        let _fixture_decode_scope_43036 =
            crate::read_image_memory::ImageMemory::enter_writer_operation(
                std::sync::Arc::clone(&fixture_open_accountant_43036).into(),
            )
            .unwrap();
        let store = RedbPersistence::open(&error_path).unwrap();
        store
            .attach_memory_accountant(std::sync::Arc::clone(&fixture_open_accountant_43036).into());
        assert_eq!(graph_durability_state(&store), before);
        let observation = ReadWorkObservationScope::enter(ReadWorkPhase::Mutation);
        arm_currency_reclaim_fault_for_test(boundary, false);
        let error = graph_durability_publication(&store, &predecessor).unwrap_err();
        assert!(
            matches!(error, Error::Other(ref message) if message == "injected currency cleanup failure")
        );
        assert_graph_durability_observation(&observation, boundary == 11, Some(boundary));
        drop(observation);
        assert_eq!(
            &graph_durability_state(&store),
            expected,
            "returned error {boundary} preserves the complete publication outcome"
        );
        drop(store);
        drop(_fixture_decode_scope_43036);
        let fixture_open_accountant_43047 =
            std::sync::Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
        let _fixture_decode_scope_43047 =
            crate::read_image_memory::ImageMemory::enter_writer_operation(
                std::sync::Arc::clone(&fixture_open_accountant_43047).into(),
            )
            .unwrap();
        let reopened = RedbPersistence::open(&error_path).unwrap();
        reopened
            .attach_memory_accountant(std::sync::Arc::clone(&fixture_open_accountant_43047).into());
        assert_eq!(
            &graph_durability_state(&reopened),
            expected,
            "returned error {boundary} has the same exact state after reopen"
        );
    }
}

// Physical relocation uses the real callback, separate from retention.
// Read only the persisted cursor; postcommit reporting failure occurs
// before compact_incremental updates its in-memory progress receipt.
type GraphCompactionState = (
    GraphDurabilityState,
    Option<Vec<u8>>,
    redb::StorageSnapshotIdentity,
);

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn graph_compaction_state(store: &RedbPersistence) -> GraphCompactionState {
    let cursor = store
        .with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let definition: TableDefinition<&str, &[u8]> =
                TableDefinition::new("writer_storage_compaction");
            match read.open_table(definition) {
                Ok(table) => Ok(table
                    .get("cursor")
                    .map_err(RedbPersistence::storage_error)?
                    .map(|value| value.value().to_vec())),
                Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
                Err(error) => Err(RedbPersistence::storage_error(error)),
            }
        })
        .unwrap();
    let root = store
        .initialize_writer_snapshot(SnapshotId(100), Lsn(30))
        .unwrap();
    (
        graph_durability_state(store),
        cursor,
        root.storage_identity(),
    )
}

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn graph_compaction_fixture(path: &Path) -> (AdjEntry, Vec<ChangeLogEntry>) {
    let (store, predecessor, entries) = graph_history_fixture(path, 4, 4, true);
    // The existing overlap fixture's real page population crosses the
    // 80-page relocation batch. It is temporary fixture data, not padding
    // counted as live relational/vector acceptance content.
    let payload: TableDefinition<u64, &[u8]> =
        TableDefinition::new("compaction_durability_payload");
    store
        .with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            {
                let mut table = write
                    .open_table(payload)
                    .map_err(RedbPersistence::storage_error)?;
                for id in 0..1024 {
                    table
                        .insert(id, &[91u8; 8192][..])
                        .map_err(RedbPersistence::storage_error)?;
                }
            }
            write.commit().map_err(RedbPersistence::storage_error)?;
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            {
                let mut table = write
                    .open_table(payload)
                    .map_err(RedbPersistence::storage_error)?;
                for id in 0..896 {
                    table.remove(id).map_err(RedbPersistence::storage_error)?;
                }
            }
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap();
    drop(store);
    (predecessor, entries)
}

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn graph_compaction_publication(
    store: &RedbPersistence,
) -> Result<(redb::CompactionProgress, Option<u64>)> {
    store.compact_incremental(
        Arc::new(crate::memory_accounting::MemoryAccountant::no_limit()).into(),
        Some(0.5),
    )
}

fn assert_graph_compaction_observation(scope: &ReadWorkObservationScope, boundary: Option<u8>) {
    let committed = boundary.is_none_or(|stage| stage == 11);
    assert_eq!(
        scope.graph_counters().graph_publication_commits,
        u64::from(committed)
    );
    let events = scope.graph_events();
    let begins = events
        .iter()
        .filter(|event| event.family == "publication" && event.operation == "begin")
        .collect::<Vec<_>>();
    assert_eq!(begins.len(), 1);
    let root = begins[0]
        .captured_root
        .as_ref()
        .expect("actual executing compaction root");
    let outcomes = events
        .iter()
        .filter(|event| {
            event.family == "publication"
                && matches!(event.operation.as_str(), "committed" | "aborted")
        })
        .collect::<Vec<_>>();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(
        outcomes[0].operation,
        if committed { "committed" } else { "aborted" }
    );
    assert_eq!(outcomes[0].captured_root.as_ref(), Some(root));
    assert_eq!(
        events
            .iter()
            .filter(|event| event.family == "publication" && event.operation == "compaction_hook")
            .count(),
        1
    );
    let stages = boundary.map_or_else(
        || {
            [27, 28, 29, 30, 9]
                .into_iter()
                .chain(19..=26)
                .chain([31, 10, 11])
                .collect::<Vec<_>>()
        },
        |stage| vec![stage],
    );
    let complete_order = [
        27, 28, 29, 30, 9, 19, 20, 21, 22, 23, 24, 25, 26, 31, 10, 11,
    ];
    let expected_order = boundary.map_or_else(
        || complete_order.to_vec(),
        |stage| {
            complete_order[..=complete_order
                .iter()
                .position(|selected| *selected == stage)
                .unwrap()]
                .to_vec()
        },
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.family == "publication"
                && event.operation == "checkpoint"
                && event.role.as_deref() == Some("currency_reclaim_checkpoint_for_test"))
            .map(|event| event.key.clone())
            .collect::<Vec<_>>(),
        expected_order
            .into_iter()
            .map(|stage| vec![stage])
            .collect::<Vec<_>>(),
        "the actual compaction callback reaches precisely the source-bound stage prefix"
    );
    for stage in stages {
        let selected = events
            .iter()
            .filter(|event| {
                event.family == "publication"
                    && event.operation == "checkpoint"
                    && event.key == vec![stage]
                    && event.role.as_deref() == Some("currency_reclaim_checkpoint_for_test")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            selected.len(),
            1,
            "specific compaction stage {stage} executed exactly once"
        );
        assert_eq!(selected[0].observation_status, "OBSERVED");
        assert_eq!(selected[0].captured_root.as_ref(), Some(root));
    }
}

fn graph_compaction_interruption_child() -> bool {
    let Some(path) = std::env::var_os("CONTEXTDB_GRAPH_COMPACTION_PATH") else {
        return false;
    };
    let boundary: u8 = std::env::var("CONTEXTDB_GRAPH_COMPACTION_BOUNDARY")
        .expect("child compaction stage")
        .parse()
        .expect("numeric child stage");
    // Selectors 27 through 31 are bound to the immutable revision 8a compaction stages.
    if (27..=31).contains(&boundary) {
        assert!((27..=31).contains(&boundary));
    } else {
        assert!([10, 11].contains(&boundary) || (19..=26).contains(&boundary));
    }
    let fixture_open_accountant_43134 =
        Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
    let _fixture_decode_scope_43134 =
        crate::read_image_memory::ImageMemory::enter_writer_operation(
            std::sync::Arc::clone(&fixture_open_accountant_43134).into(),
        )
        .unwrap();
    let store = RedbPersistence::open(Path::new(&path))
        .expect("open closed temporary compaction fixture beside registered old readers");
    store.attach_memory_accountant(std::sync::Arc::clone(&fixture_open_accountant_43134).into());
    arm_retention_reclaim_crash_for_test(boundary);
    let result = graph_compaction_publication(&store);
    panic!("selected compaction interruption {boundary} was not reached: {result:?}");
}

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn assert_graph_compaction_pinned_rows(directory: &Path, boundaries: &[u8]) {
    let fixture_path = directory.join("graph-compaction-fixture.db");
    let (predecessor, entries) = graph_compaction_fixture(&fixture_path);
    let initial_fixture_file_bytes = std::fs::metadata(&fixture_path).unwrap().len();
    let control_path = directory.join("graph-compaction-success.db");
    std::fs::copy(&fixture_path, &control_path).unwrap();
    // Begin the file readers before writer acquisition. Their registration
    // is real custody of the old storage root, held through publication.
    let control_session = ReadSession::open(&control_path).unwrap();
    assert_eq!(control_session.route(), ReadRoute::File);
    let control_registration = control_session
        .file_storage_registration_for_test()
        .unwrap();
    assert!(control_registration.descriptor_open);
    assert!(!control_registration.load_lock_held);
    let (image, _receipt, control_source) =
        load_read_image_with_memory(&control_path, Some(16 * 1024 * 1024))
            .unwrap()
            .into_parts();
    drop(image);
    let fixture_open_accountant_43155 =
        Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
    let _fixture_decode_scope_43155 =
        crate::read_image_memory::ImageMemory::enter_writer_operation(
            std::sync::Arc::clone(&fixture_open_accountant_43155).into(),
        )
        .unwrap();
    let control = RedbPersistence::open(&control_path).unwrap();
    control.attach_memory_accountant(std::sync::Arc::clone(&fixture_open_accountant_43155).into());
    let page_size = control
        .with_db(|db| {
            let snapshot = db
                .storage_statistics_snapshot()
                .map_err(RedbPersistence::storage_error)?;
            Ok(snapshot
                .stats()
                .map_err(RedbPersistence::storage_error)?
                .page_size())
        })
        .unwrap();
    let initial_fixture_file_pages = initial_fixture_file_bytes.div_ceil(page_size as u64);
    // Freeze the finite progress window before any compaction step.
    let maximum_progress_steps = initial_fixture_file_pages.div_ceil(80) + 1;
    let before = graph_compaction_state(&control);
    let control_writer = control
        .initialize_writer_snapshot(SnapshotId(100), Lsn(30))
        .unwrap();
    let writer_pin = control_writer.storage_identity();
    let source_pin = control_source.storage_identity();
    drop(_fixture_decode_scope_43155);
    let session_image = control_session.file_snapshot_for_test().unwrap();
    let row_answer = {
        let fixture_answer_101 = control_session
            .execute("SELECT body FROM removed_rows", &HashMap::new())
            .unwrap();
        fixture_answer_101.rows().to_vec()
    };
    assert_eq!(
        row_answer,
        vec![vec![Value::Text(
            "selected physical retention payload".into()
        )]]
    );
    let history_answer = control_session
        .metadata(
            crate::MetadataRequest::ImageState {
                kind: crate::DirectImageMetadataKind::ChangeLog,
            },
            None,
        )
        .unwrap()
        .body
        .clone();
    let directory_before = directory_pinned_view(&control_session);
    assert_eq!(
        directory_before,
        ((6, Lsn(1)), None, Some((Lsn(20), TxId(100))), Some((0, 6)))
    );
    let _fixture_decode_scope_43155 =
        crate::read_image_memory::ImageMemory::enter_writer_operation(
            std::sync::Arc::clone(&fixture_open_accountant_43155).into(),
        )
        .unwrap();
    let mut relocation_observed = false;
    let mut cumulative_pages_relocated = 0;
    let mut first_successful_step = None;
    for step in 0..maximum_progress_steps {
        let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Mutation);
        let (progress, _recycled) = graph_compaction_publication(&control).unwrap();
        assert_graph_compaction_observation(&scope, None);
        drop(scope);
        if step == 0 {
            first_successful_step = Some(graph_compaction_state(&control));
        }
        assert!(progress.pages_examined > 0);
        cumulative_pages_relocated += progress.pages_relocated;
        if progress.pages_relocated > 0 {
            relocation_observed = true;
            break;
        }
    }
    let first_successful_step =
        first_successful_step.expect("the control executes its first successful compaction step");
    assert!(
        relocation_observed,
        "the control must execute physical page relocation within {maximum_progress_steps} steps"
    );
    assert!(
        cumulative_pages_relocated > 0,
        "the control executes actual physical page relocation within the frozen progress window"
    );
    drop(_fixture_decode_scope_43155);
    assert_eq!(
        directory_pinned_view(&control_session),
        directory_before,
        "actual installed directory survives physical relocation at its original root"
    );
    let _fixture_decode_scope_43155 =
        crate::read_image_memory::ImageMemory::enter_writer_operation(
            std::sync::Arc::clone(&fixture_open_accountant_43155).into(),
        )
        .unwrap();
    let after = graph_compaction_state(&control);
    assert_ne!(after.1, before.1, "successor cursor is published durably");
    assert_ne!(
        after.2, before.2,
        "compaction publishes a new physical root"
    );
    assert_eq!(
        after.0.0.0, before.0.0.0,
        "forward/reverse/C/T/R and row bytes survive relocation"
    );
    assert_eq!(
        after.0.0.1[0], before.0.0.1[0],
        "private history bytes survive relocation"
    );
    assert_eq!(
        after.0.1, before.0.1,
        "numeric history and selectors survive relocation"
    );
    assert_eq!(
        after.0.2, before.0.2,
        "clock/read bounds survive relocation"
    );
    assert_eq!(
        after.0.3, before.0.3,
        "occurrence counts survive relocation"
    );
    assert_eq!(
        after.0.4, before.0.4,
        "commit directory members survive relocation"
    );
    {
        let observed_identity = control_writer.storage_identity();
        assert!(observed_identity.transaction_id >= writer_pin.transaction_id);
        assert_eq!(observed_identity.user_root, writer_pin.user_root);
    }
    {
        let observed_identity = control_source.storage_identity();
        assert!(observed_identity.transaction_id >= source_pin.transaction_id);
        assert_eq!(observed_identity.user_root, source_pin.user_root);
    }
    drop(_fixture_decode_scope_43155);
    {
        let mut observed_registration = control_session
            .file_storage_registration_for_test()
            .unwrap();
        assert!(
            observed_registration.storage_transaction_id
                >= control_registration.storage_transaction_id
        );
        observed_registration.storage_transaction_id = control_registration.storage_transaction_id;
        assert_eq!(observed_registration, control_registration);
    }
    assert_eq!(
        control_session.file_snapshot_for_test().unwrap(),
        session_image
    );
    assert_eq!(
        control_session
            .execute("SELECT body FROM removed_rows", &HashMap::new())
            .unwrap()
            .rows(),
        row_answer
    );
    assert_eq!(
        control_session
            .metadata(
                crate::MetadataRequest::ImageState {
                    kind: crate::DirectImageMetadataKind::ChangeLog
                },
                None
            )
            .unwrap()
            .body,
        history_answer
    );
    assert_graph_retention_row_source(
        &graph_read::GraphReadRoot::Writer(control_writer.clone()),
        true,
    );
    assert_graph_source_answers(
        graph_read::GraphReadRoot::Writer(control_writer.clone()),
        &predecessor,
        true,
        &entries,
    );
    assert_graph_retention_row_source(
        &graph_read::GraphReadRoot::Image(control_source.clone()),
        true,
    );
    assert_graph_source_answers(
        graph_read::GraphReadRoot::Image(control_source.clone()),
        &predecessor,
        true,
        &entries,
    );
    drop(control_writer);
    drop(control_source);
    drop(control_session);
    drop(control);

    // A normal successful call performs two later allocator housekeeping
    // commits. Obtain the exact first publication outcome independently
    // by stopping its reporting path at the already-bound postcommit stage.
    // This error occurs after the relocation transaction has committed;
    // it is never treated as an aborted publication or a success return.
    let published_path = directory.join("graph-compaction-published-control.db");
    std::fs::copy(&fixture_path, &published_path).unwrap();
    let published_session = ReadSession::open(&published_path).unwrap();
    let (image, _receipt, published_source) =
        load_read_image_with_memory(&published_path, Some(16 * 1024 * 1024))
            .unwrap()
            .into_parts();
    drop(image);
    let fixture_open_accountant_43208 =
        Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
    let _fixture_decode_scope_43208 =
        crate::read_image_memory::ImageMemory::enter_writer_operation(
            std::sync::Arc::clone(&fixture_open_accountant_43208).into(),
        )
        .unwrap();
    let published = RedbPersistence::open(&published_path).unwrap();
    published
        .attach_memory_accountant(std::sync::Arc::clone(&fixture_open_accountant_43208).into());
    let published_writer = published
        .initialize_writer_snapshot(SnapshotId(100), Lsn(30))
        .unwrap();
    let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Mutation);
    arm_currency_reclaim_fault_for_test(11, false);
    let error = graph_compaction_publication(&published).unwrap_err();
    assert!(
        matches!(error, Error::Other(ref message) if message == "injected currency cleanup failure")
    );
    assert_graph_compaction_observation(&scope, None);
    drop(scope);
    let first_publication = graph_compaction_state(&published);
    assert_eq!(
        first_publication.0, first_successful_step.0,
        "allocator housekeeping does not change published data/proof/binding bytes"
    );
    assert_eq!(
        first_publication.1, first_successful_step.1,
        "the published successor checkpoint is independent of later housekeeping"
    );
    assert_ne!(first_publication.2, before.2);
    assert_graph_source_answers(
        graph_read::GraphReadRoot::Writer(published_writer),
        &predecessor,
        true,
        &entries,
    );
    assert_graph_source_answers(
        graph_read::GraphReadRoot::Image(published_source),
        &predecessor,
        true,
        &entries,
    );
    drop(_fixture_decode_scope_43208);
    assert_eq!(
        published_session
            .execute("SELECT body FROM removed_rows", &HashMap::new())
            .unwrap()
            .rows(),
        row_answer
    );
    drop(published_session);
    drop(published);

    for &boundary in boundaries {
        if (27..=31).contains(&boundary) {
            assert!((27..=31).contains(&boundary));
        } else {
            assert!([10, 11].contains(&boundary) || (19..=26).contains(&boundary));
        }
        for crash in [false, true] {
            let path = directory.join(format!("graph-compaction-{boundary}-{crash}.db"));
            std::fs::copy(&fixture_path, &path).unwrap();
            let old_session = ReadSession::open(&path).unwrap();
            assert_eq!(old_session.route(), ReadRoute::File);
            let old_registration = old_session.file_storage_registration_for_test().unwrap();
            assert!(old_registration.descriptor_open);
            assert!(!old_registration.load_lock_held);
            let old_snapshot = old_session.file_snapshot_for_test().unwrap();
            let (image, _receipt, old_source) =
                load_read_image_with_memory(&path, Some(16 * 1024 * 1024))
                    .unwrap()
                    .into_parts();
            drop(image);
            let old_source_pin = old_source.storage_identity();
            assert_eq!(
                old_session
                    .execute("SELECT body FROM removed_rows", &HashMap::new())
                    .unwrap()
                    .rows(),
                row_answer
            );
            assert_eq!(
                old_session
                    .metadata(
                        crate::MetadataRequest::ImageState {
                            kind: crate::DirectImageMetadataKind::ChangeLog
                        },
                        None
                    )
                    .unwrap()
                    .body,
                history_answer
            );
            if crash {
                // The parent retains its registered reader handles across
                // child writer acquisition and abort. It holds no writer.
                let module = module_path!().split_once("::").unwrap().1;
                let exact_name = format!(
                    "{module}::empty_declared_index_publishes_marker_and_postings_for_a_lazy_ordered_read"
                );
                let output = std::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", &exact_name, "--nocapture", "--test-threads=1"])
                    .env("CONTEXTDB_GRAPH_COMPACTION_PATH", &path)
                    .env("CONTEXTDB_GRAPH_COMPACTION_BOUNDARY", boundary.to_string())
                    .current_dir(directory)
                    .output()
                    .unwrap();
                let stderr = String::from_utf8_lossy(&output.stderr);
                let stdout = String::from_utf8_lossy(&output.stdout);
                let marker = format!("RETENTION_RECLAIM_CRASH={boundary}");
                assert_eq!(
                    stderr.lines().filter(|line| *line == marker).count(),
                    1,
                    "specific compaction interruption marker required: {stderr}"
                );
                assert_eq!(
                    stderr
                        .lines()
                        .filter(|line| line.starts_with("RETENTION_RECLAIM_CRASH="))
                        .count(),
                    1
                );
                assert!(!output.status.success());
                #[cfg(unix)]
                {
                    use std::os::unix::process::ExitStatusExt;
                    assert_eq!(
                        output.status.signal(),
                        Some(libc::SIGABRT),
                        "generic child failure is not the selected compaction interruption: {stdout}\n{stderr}"
                    );
                }
                assert!(!stderr.contains("panicked at"));
                assert!(!stdout.contains("FAILED"));
            } else {
                let fixture_open_accountant_43266 =
                    Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
                let _fixture_decode_scope_43266 =
                    crate::read_image_memory::ImageMemory::enter_writer_operation(
                        std::sync::Arc::clone(&fixture_open_accountant_43266).into(),
                    )
                    .unwrap();
                let store = RedbPersistence::open(&path).unwrap();
                store.attach_memory_accountant(
                    std::sync::Arc::clone(&fixture_open_accountant_43266).into(),
                );
                let old_writer = store
                    .initialize_writer_snapshot(SnapshotId(100), Lsn(30))
                    .unwrap();
                let old_writer_pin = old_writer.storage_identity();
                {
                    let mut observed_state = graph_compaction_state(&store);
                    assert!(observed_state.2.transaction_id >= before.2.transaction_id);
                    observed_state.2.transaction_id = before.2.transaction_id;
                    assert_eq!(observed_state, before);
                }
                let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Mutation);
                arm_currency_reclaim_fault_for_test(boundary, false);
                let error = graph_compaction_publication(&store).unwrap_err();
                assert!(
                    matches!(error, Error::Other(ref message) if message == "injected currency cleanup failure")
                );
                assert_graph_compaction_observation(&scope, Some(boundary));
                drop(scope);
                {
                    let observed_identity = old_writer.storage_identity();
                    assert!(observed_identity.transaction_id >= old_writer_pin.transaction_id);
                    assert_eq!(observed_identity.user_root, old_writer_pin.user_root);
                }
                assert_graph_retention_row_source(
                    &graph_read::GraphReadRoot::Writer(old_writer.clone()),
                    true,
                );
                assert_graph_source_answers(
                    graph_read::GraphReadRoot::Writer(old_writer),
                    &predecessor,
                    true,
                    &entries,
                );
                drop(store);
                drop(_fixture_decode_scope_43266);
            }
            let expected = if boundary == 11 {
                &first_publication
            } else {
                &before
            };
            let fixture_open_accountant_43283 =
                Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
            let _fixture_decode_scope_43283 =
                crate::read_image_memory::ImageMemory::enter_writer_operation(
                    std::sync::Arc::clone(&fixture_open_accountant_43283).into(),
                )
                .unwrap();
            let reopened = RedbPersistence::open(&path).unwrap();
            reopened.attach_memory_accountant(
                std::sync::Arc::clone(&fixture_open_accountant_43283).into(),
            );
            let reopened_state = graph_compaction_state(&reopened);
            assert!(
                reopened_state.2.transaction_id >= before.2.transaction_id,
                "stage {boundary} recovery never moves the storage transaction counter backwards"
            );
            // Recovery may advance the counter; every committed content byte
            // and every other physical snapshot identity field must match.
            let reopened_content = (
                reopened_state.0,
                reopened_state.1,
                redb::StorageSnapshotIdentity {
                    transaction_id: expected.2.transaction_id,
                    ..reopened_state.2
                },
            );
            assert_eq!(
                &reopened_content, expected,
                "stage {boundary} publishes the exact old/new root, persisted progress and complete data"
            );
            let new_writer = reopened
                .initialize_writer_snapshot(SnapshotId(100), Lsn(30))
                .unwrap();
            assert_graph_retention_row_source(
                &graph_read::GraphReadRoot::Writer(new_writer.clone()),
                true,
            );
            assert_graph_source_answers(
                graph_read::GraphReadRoot::Writer(new_writer),
                &predecessor,
                true,
                &entries,
            );
            drop(_fixture_decode_scope_43283);
            {
                let mut observed_registration =
                    old_session.file_storage_registration_for_test().unwrap();
                assert!(
                    observed_registration.storage_transaction_id
                        >= old_registration.storage_transaction_id
                );
                observed_registration.storage_transaction_id =
                    old_registration.storage_transaction_id;
                assert_eq!(
                    observed_registration, old_registration,
                    "the same old descriptor/registration/root remains pinned"
                );
            }
            assert_eq!(old_session.file_snapshot_for_test().unwrap(), old_snapshot);
            {
                let observed_identity = old_source.storage_identity();
                assert!(observed_identity.transaction_id >= old_source_pin.transaction_id);
                assert_eq!(observed_identity.user_root, old_source_pin.user_root);
            }
            assert_eq!(
                old_session
                    .execute("SELECT body FROM removed_rows", &HashMap::new())
                    .unwrap()
                    .rows(),
                row_answer
            );
            assert_eq!(
                old_session
                    .metadata(
                        crate::MetadataRequest::ImageState {
                            kind: crate::DirectImageMetadataKind::ChangeLog
                        },
                        None
                    )
                    .unwrap()
                    .body,
                history_answer
            );
            assert_graph_retention_row_source(
                &graph_read::GraphReadRoot::Image(old_source.clone()),
                true,
            );
            assert_graph_source_answers(
                graph_read::GraphReadRoot::Image(old_source.clone()),
                &predecessor,
                true,
                &entries,
            );
            let _fixture_decode_scope_43283 =
                crate::read_image_memory::ImageMemory::enter_writer_operation(
                    std::sync::Arc::clone(&fixture_open_accountant_43283).into(),
                )
                .unwrap();
            let current_root = reopened
                .initialize_writer_snapshot(SnapshotId(100), Lsn(30))
                .unwrap()
                .storage_identity();
            drop(_fixture_decode_scope_43283);
            drop(reopened);
            let fresh_session = ReadSession::open(&path).unwrap();
            assert_eq!(fresh_session.route(), ReadRoute::File);
            let fresh_registration = fresh_session.file_storage_registration_for_test().unwrap();
            assert!(fresh_registration.storage_transaction_id >= current_root.transaction_id);
            assert_eq!(
                (
                    fresh_registration.storage_root_page,
                    fresh_registration.storage_root_checksum,
                    fresh_registration.storage_root_length
                ),
                (
                    current_root.user_root.map(|root| root.page),
                    current_root.user_root.map(|root| root.checksum),
                    current_root.user_root.map(|root| root.length)
                ),
                "fresh public session owns exactly the new current physical root"
            );
            assert_eq!(
                fresh_session
                    .execute("SELECT body FROM removed_rows", &HashMap::new())
                    .unwrap()
                    .rows(),
                row_answer
            );
            assert_eq!(
                fresh_session
                    .metadata(
                        crate::MetadataRequest::ImageState {
                            kind: crate::DirectImageMetadataKind::ChangeLog
                        },
                        None
                    )
                    .unwrap()
                    .body,
                history_answer
            );
        }
    }
}

fn indexed_fixture(path: &Path, table: &str, index: &str) {
    let database = Database::open(path).expect("create indexed readiness fixture");
    database
        .execute(
            &format!(
                "CREATE TABLE {table} (id INTEGER PRIMARY KEY, status TEXT NOT NULL, body TEXT NOT NULL, embedding VECTOR(2))"
            ),
            &HashMap::new(),
        )
        .expect("declare indexed readiness table");
    database
        .execute(
            &format!("CREATE INDEX {index} ON {table}(status)"),
            &HashMap::new(),
        )
        .expect("publish the readiness marker and posting table");
    database
        .execute(
            &format!("INSERT INTO {table} VALUES (1, 'ready', $body, $embedding)"),
            &HashMap::from([
                (
                    "body".to_owned(),
                    Value::Text("bounded-readiness-body-".repeat(4096)),
                ),
                ("embedding".to_owned(), Value::Vector(vec![1.0, 0.0])),
            ]),
        )
        .expect("persist one body that distinguishes fallback hydration");
    database.close().expect("close indexed readiness fixture");
}

/// An index dropped and declared again under the same name, over a column
/// of another type and in the other direction, reads back as a healthy
/// index -- immediately and after the store is reopened. The two
/// declarations share one physical posting table, so the drop has to take
/// the first declaration's postings out with it.
#[test]
fn an_index_redeclared_over_another_column_reads_back_clean_here_and_after_reopen() {
    let root = tempfile::tempdir().expect("task-scoped redeclared-index directory");
    let path = root.path().join("redeclared-index.db");
    let database = Database::open(&path).expect("create the redeclared-index fixture");
    database
        .execute(
            "CREATE TABLE shifting_rows (id INTEGER PRIMARY KEY, active BOOLEAN NOT NULL, revision INTEGER NOT NULL)",
            &HashMap::new(),
        )
        .expect("declare the redeclared-index table");
    database
        .execute(
            "CREATE INDEX shifting_index ON shifting_rows(active)",
            &HashMap::new(),
        )
        .expect("declare the index over the boolean column, ascending");
    for (id, active, revision) in [(1_i64, true, 30_i64), (2, false, 20), (3, true, 10)] {
        database
            .execute(
                "INSERT INTO shifting_rows VALUES ($id, $active, $revision)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    ("active".to_owned(), Value::Bool(active)),
                    ("revision".to_owned(), Value::Int64(revision)),
                ]),
            )
            .expect("stage rows the first declaration indexes");
    }
    database
        .execute(
            "DROP INDEX shifting_index ON shifting_rows",
            &HashMap::new(),
        )
        .expect("drop the index over the boolean column");
    database
        .execute(
            "CREATE INDEX shifting_index ON shifting_rows(revision DESC)",
            &HashMap::new(),
        )
        .expect("declare the same index name over an integer column, descending");

    let ordered = |database: &Database, label: &str| {
        let result = database
            .execute(
                "SELECT id FROM shifting_rows ORDER BY revision DESC",
                &HashMap::new(),
            )
            .unwrap_or_else(|error| {
                panic!("the redeclared index answers {label}: {error:?}");
            });
        result
            .rows()
            .iter()
            .map(|row| match row.first().expect("one projected column") {
                Value::Int64(id) => *id,
                other => panic!("the projection is the integer key: {other:?}"),
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        ordered(&database, "in the session that redeclared it"),
        vec![1, 2, 3],
        "the new declaration orders by revision descending"
    );
    database
        .close()
        .expect("close the redeclared-index fixture");

    let reopened = Database::open(&path).expect("reopen the redeclared-index fixture");
    assert_eq!(
        ordered(&reopened, "after the store is reopened"),
        vec![1, 2, 3],
        "the reopened committed image carries only the new declaration's postings"
    );
    reopened.close().expect("close the reopened fixture");
}

#[test]
fn ordered_iterator_is_untouched_until_boundary_and_charge_accept() {
    let touches = std::cell::Cell::new(0);
    let mut entries = std::iter::from_fn(|| {
        touches.set(touches.get() + 1);
        Some(7)
    });
    let charges = std::cell::Cell::new(0);
    let mut charge = || {
        charges.set(charges.get() + 1);
        Ok::<_, &str>(())
    };
    assert_eq!(
        charged_ordered_next(&mut entries, &mut || Err("deadline"), &mut charge),
        Err("deadline")
    );
    assert_eq!(charges.get(), 0);
    assert_eq!(touches.get(), 0);
    assert_eq!(
        charged_ordered_next(&mut entries, &mut || Ok(()), &mut || Err("work")),
        Err("work")
    );
    assert_eq!(touches.get(), 0);
    assert_eq!(
        charged_ordered_next(&mut entries, &mut || Ok(()), &mut charge),
        Ok(Some(7))
    );
    assert_eq!(charges.get(), 1);
    assert_eq!(touches.get(), 1);
}

#[test]
fn relational_and_vector_readiness_cross_matrix_refuses_malformed_before_rows() {
    let root = tempfile::tempdir().expect("task-scoped readiness-state directory");
    for directory_ready in [true, false] {
        for readiness in ["valid", "malformed", "unsupported", "absent"] {
            let case = format!(
                "{readiness}-{}",
                if directory_ready { "ready" } else { "unready" }
            );
            let path = root.path().join(format!("{case}.db"));
            let table = format!("rows_{}", case.replace('-', "_"));
            let index = format!("{table}_status_idx");
            indexed_fixture(&path, &table, &index);
            {
                let raw = redb::Database::open(&path).expect("open readiness matrix fixture");
                let write = raw.begin_write().expect("begin readiness matrix mutation");
                if !directory_ready {
                    write
                        .delete_table(VECTOR_PARTITION_MEMBERSHIP_TABLE)
                        .expect("remove the vector membership directory");
                }
                if readiness == "unsupported" {
                    write
                        .open_table(RELATIONAL_INDEX_READY_TABLE)
                        .unwrap()
                        .insert(table.as_str(), b"unsupported".as_slice())
                        .unwrap();
                }
                match readiness {
                    "valid" | "unsupported" => {}
                    "malformed" => {
                        let physical_name =
                            RedbPersistence::relational_index_table_name(&table, &index);
                        let physical: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                            TableDefinition::new(physical_name.as_str());
                        write.delete_table(physical).expect(
                            "remove the posting table while retaining its published marker",
                        );
                    }
                    "absent" => {
                        write
                            .delete_table(RELATIONAL_INDEX_READY_TABLE)
                            .expect("remove only the readiness table");
                    }
                    _ => unreachable!(),
                }
                write.commit().expect("commit readiness matrix mutation");
            }

            let work = ReadWorkObservationScope::enter(ReadWorkPhase::Open);
            if matches!(readiness, "malformed" | "unsupported") {
                let error = match load_read_image_with_memory(&path, Some(2 * 1024 * 1024)) {
                    Ok(_) => panic!("published malformed readiness must refuse in {case}"),
                    Err(error) => error,
                };
                assert_eq!(
                    error.kind(),
                    LoadReadImageErrorKind::Corrupt(ReadImageCorruptionCause::MalformedRecord),
                    "{case}"
                );
                assert_eq!(
                    work.counters(),
                    ReadWorkCounters::default(),
                    "{case} must refuse before whole-image row hydration"
                );
                assert!(matches!(
                    error.into_direct_reader_error(),
                    crate::direct_file_reader::DirectFileReaderError::CorruptStore(
                        crate::direct_file_reader::DirectStoreDiagnostic::MalformedRecord,
                        _
                    )
                ));
                drop(work);

                let public_work = ReadWorkObservationScope::enter(ReadWorkPhase::Open);
                let public_error = match ReadSession::open(&path) {
                    Ok(_) => panic!("the public read door must refuse malformed {case}"),
                    Err(error) => error,
                };
                assert!(
                    matches!(public_error, Error::StoreCorrupted { .. }),
                    "the public read door must retain its typed corruption refusal: {public_error:?}"
                );
                assert_eq!(
                    public_work.counters(),
                    ReadWorkCounters::default(),
                    "the public {case} refusal must precede row hydration"
                );
                drop(public_work);
            } else {
                let image = load_read_image_with_memory(&path, Some(2 * 1024 * 1024))
                    .unwrap_or_else(|error| panic!("{case} remains readable: {error}"));
                let expected_lazy = directory_ready && readiness == "valid";
                assert_eq!(image.image.lazy_relational, expected_lazy, "{case}");
                if expected_lazy {
                    assert_open_proves_bounded_history_and_startup_digest(
                        work.counters(),
                        &format!("{case} skips complete-image hydration"),
                        331,
                    );
                } else {
                    assert!(
                        work.counters().decoded_rows > 0,
                        "{case} retains complete-image hydration"
                    );
                }
                let first_open = work.counters();
                drop(image);
                drop(work);
                if expected_lazy {
                    // Keep the startup projection fixed while doubling retained
                    // row/vector bodies and adding a committed history entry.
                    let database =
                        Database::open(&path).expect("reopen the readiness matrix growth fixture");
                    database
                        .execute(
                            &format!("INSERT INTO {table} VALUES (2, 'ready', $body, $embedding)"),
                            &HashMap::from([
                                (
                                    "body".to_owned(),
                                    Value::Text("bounded-readiness-body-".repeat(4096)),
                                ),
                                ("embedding".to_owned(), Value::Vector(vec![0.0, 1.0])),
                            ]),
                        )
                        .expect("double retained readiness bodies and append history");
                    database
                        .close()
                        .expect("publish the larger readiness fixture");

                    let larger_work = ReadWorkObservationScope::enter(ReadWorkPhase::Open);
                    let larger_image = load_read_image_with_memory(&path, Some(2 * 1024 * 1024))
                        .expect("the larger readiness fixture remains readable");
                    assert!(larger_image.image.lazy_relational, "{case}");
                    assert_open_proves_bounded_history_and_startup_digest(
                        larger_work.counters(),
                        &format!("{case} doubled retained bodies skip complete-image hydration"),
                        331,
                    );
                    assert_eq!(
                        larger_work.counters(),
                        first_open,
                        "{case}: fixed 331 hash bytes and zero proof/row/vector work are identical at both store sizes"
                    );
                    drop(larger_image);
                    drop(larger_work);
                }
            }
        }
    }
}

// Admission failures retain the reader's typed refusal and return every
// short-lived credit. Limits come from the existing child declaration or
// the actual successful request, rather than a second policy constant.
#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn assert_graph_admission_rows(directory: &Path) {
    use std::ops::Bound;
    use writer_source::SourceWindowOwner;
    let path = directory.join("graph-admission-doors.db");
    let (store, predecessor, entries) = graph_history_fixture(&path, 0, 1, false);
    let before = graph_publication_state(&store);
    drop(store);
    // Every operation gets a released-file, zero-cache fixture. Both
    // native roots retain their actual registered descriptors; no page
    // from fixture publication or a prior direction can fund this read.
    let cold_roots = || {
        let image_memory = crate::read_image_memory::ImageMemory::new(None)
            .expect("unlimited graph admission fixture image owner");
        // Same non-cache source reservation as the production load door.
        // Zero is the existing fixture cache option, not a reader limit.
        let source_bytes =
            (64 * 1024) + content_identity::READ_VERIFIER_MEMORY_BYTES + std::mem::size_of::<Lsn>();
        let source_memory =
            SourceMemoryReservation::new(image_memory.clone(), source_bytes).unwrap();
        let limits = crate::read_session::ReadSessionOptions::default().limits;
        let declared =
            crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(limits.memory)
                .expect("admit the declared fixture root");
        let open_context = crate::executor::bounded::ReadOpenContext::new(
            declared.child().unwrap(),
            limits,
            crate::read_session::read_clock(),
            OwnerReadCancellation::new(),
        )
        .unwrap();
        let load = image_memory
            .scope(|| {
                let mut source =
                    ReadImageSource::start(&path, 0, 0, source_memory, None, None, None)?;
                let image = source.with_read_transaction(|read| {
                    decode_complete_read_image(
                        read,
                        &path,
                        image_memory.clone(),
                        open_context.clone(),
                    )
                })?;
                source.finish(image)
            })
            .unwrap();
        let (_, receipt, image) = load.into_parts();
        receipt.validate_released().unwrap();
        let fixture_open_accountant_43602 =
            Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
        let _fixture_decode_scope_43602 =
            crate::read_image_memory::ImageMemory::enter_writer_operation(
                std::sync::Arc::clone(&fixture_open_accountant_43602).into(),
            )
            .unwrap();
        let store = RedbPersistence::open_with_cache(&path, 0).unwrap();
        store
            .attach_memory_accountant(std::sync::Arc::clone(&fixture_open_accountant_43602).into());
        let writer = store
            .initialize_writer_snapshot(SnapshotId(100), Lsn(30))
            .unwrap();
        (
            store,
            [
                graph_read::GraphReadRoot::Writer(writer),
                graph_read::GraphReadRoot::Image(image),
            ],
        )
    };
    let declared_child = 16 * 1024 * 1024;
    let key = RedbPersistence::graph_fwd_key(&predecessor);
    let key_memory = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
        declared_child,
    ));
    let endpoint = SourceWindowOwner::Read(key_memory.clone().into())
        .endpoint(&[key.as_slice()], "graph_cursor")
        .unwrap();
    assert_eq!(
        key_memory.usage().used,
        key.len(),
        "the actual endpoint owns exactly its retained key"
    );
    drop(endpoint);
    assert_eq!(
        key_memory.usage().used,
        0,
        "the endpoint credit returns at its own lifetime boundary"
    );
    key_memory.set_budget(Some(key.len() - 1)).unwrap();
    let error = SourceWindowOwner::Read(key_memory.clone().into())
        .endpoint(&[key.as_slice()], "graph_cursor")
        .err()
        .expect("one byte below the actual endpoint request refuses");
    assert!(
        matches!(error, Error::MemoryBudgetExceeded { ref subsystem, ref operation, requested_bytes, budget_limit_bytes, .. }
        if subsystem == "writer_source" && operation == "graph_cursor" && requested_bytes == key.len() && budget_limit_bytes == key.len() - 1),
        "key admission keeps its original typed attribution: {error:?}"
    );
    assert_eq!(
        key_memory.usage().used,
        0,
        "failed endpoint admission retains no credit"
    );
    key_memory.set_budget(Some(declared_child)).unwrap();
    for route in 0..2 {
        for reverse in [false, true] {
            let (store, roots) = cold_roots();
            let root = &roots[route];
            assert_eq!(
                store.resident_cache_stats_for_test().unwrap().3,
                0,
                "the fresh reopened writer has zero resident cache bytes before this cold page read"
            );
            if let graph_read::GraphReadRoot::Image(image) = root {
                assert_eq!(
                    image.database.cache_stats().used_bytes(),
                    0,
                    "the independent image fixture has no resident page cache before this cold page read"
                );
            }
            let key = if reverse {
                RedbPersistence::graph_rev_key(&predecessor)
            } else {
                RedbPersistence::graph_fwd_key(&predecessor)
            };
            let page_memory = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
                key.len() - 1,
            ));
            let error = graph_read::with_pages(page_memory.clone(), || {
                let read_page = |read: &redb::ReadTransaction| -> Result<()> {
                    let table = read
                        .open_table(if reverse {
                            GRAPH_REV_TABLE
                        } else {
                            GRAPH_FWD_TABLE
                        })
                        .map_err(RedbPersistence::storage_error)?;
                    let value = table
                        .get_bounded(&key)
                        .map_err(RedbPersistence::storage_error)?;
                    assert!(
                        value.is_some(),
                        "the actual bounded page contains this primary/reverse edge"
                    );
                    Ok(())
                };
                match root {
                    graph_read::GraphReadRoot::Writer(writer) => {
                        read_page(writer.owner_read()?.as_ref())
                    }
                    graph_read::GraphReadRoot::Image(image) => image.with_read(read_page),
                }
            })
            .unwrap_err();
            assert!(
                matches!(error, Error::MemoryBudgetExceeded { budget_limit_bytes, requested_bytes, available_bytes, .. }
                if budget_limit_bytes == key.len() - 1 && requested_bytes > available_bytes),
                "real reject-only page admission restores the original memory error: {error:?}"
            );
            assert_eq!(
                page_memory.usage().used,
                0,
                "failed bounded primary/reverse page read releases partial credits"
            );
            let read_stored = |read: &redb::ReadTransaction| -> Result<Vec<u8>> {
                let table = read
                    .open_table(if reverse {
                        GRAPH_REV_TABLE
                    } else {
                        GRAPH_FWD_TABLE
                    })
                    .map_err(RedbPersistence::storage_error)?;
                Ok(table
                    .get(key.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .unwrap()
                    .value()
                    .to_vec())
            };
            let raw = match root {
                graph_read::GraphReadRoot::Writer(writer) => {
                    read_stored(&writer.owner_read().unwrap()).unwrap()
                }
                graph_read::GraphReadRoot::Image(image) => image.with_read(read_stored).unwrap(),
            };
            let decode_memory = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
                declared_child,
            ));
            let bridge = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
                declared_child,
            ));
            let decoder = crate::read_image_memory::ImageMemory::charging_reader_for(
                bridge.clone().into(),
                decode_memory.clone(),
                "graph_source",
                "edge_decode",
                "Raise the declared reader memory limit to read graph pages.",
            );
            let (decoded, consumed): (AdjEntry, usize) = decoder
                .scope(|| crate::read_image_memory::decode(&decoder.enter(), &raw))
                .unwrap();
            assert_eq!(
                (&decoded, consumed),
                (&predecessor, raw.len()),
                "the actual stored primary/reverse payload has a valid complete decode"
            );
            let required_decode = decode_memory.high_water_for_test();
            drop(decoded);
            drop(decoder);
            assert_eq!(
                decode_memory.usage().used,
                0,
                "the admitted stored decoder releases its scratch at the decoder lifetime boundary"
            );
            decode_memory.set_budget(Some(required_decode - 1)).unwrap();
            let decoder = crate::read_image_memory::ImageMemory::charging_reader_for(
                bridge.clone().into(),
                decode_memory.clone(),
                "graph_source",
                "edge_decode",
                "Raise the declared reader memory limit to read graph pages.",
            );
            let decoded: Result<(AdjEntry, usize)> =
                decoder.scope(|| crate::read_image_memory::decode(&decoder.enter(), &raw));
            let error = decoded.unwrap_err();
            assert!(
                matches!(error, Error::MemoryBudgetExceeded { ref subsystem, ref operation, budget_limit_bytes, .. }
                if subsystem == "graph_source" && operation == "edge_decode" && budget_limit_bytes == required_decode - 1),
                "stored decoder admission preserves its original typed error: {error:?}"
            );
            drop(decoder);
            assert_eq!(
                decode_memory.usage().used,
                0,
                "a refused stored decoder returns its partial credits"
            );
            assert_eq!(
                bridge.usage().used,
                0,
                "reader decoder scratch never borrows the source accountant"
            );
            let memory = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
                declared_child,
            ));
            let mut control = root
                .cursor(
                    reverse,
                    Bound::Included(&key),
                    Bound::Included(&key),
                    memory.clone(),
                )
                .unwrap();
            let window = control
                .next_edge_with(&|| Ok(()))
                .unwrap()
                .expect("the same exact bounded door has a valid source");
            assert_eq!(window.edge(), &predecessor);
            drop(window);
            drop(control);
            assert_eq!(
                memory.usage().used,
                0,
                "successful page, proof, decoder and cursor credits return"
            );
            memory.reset_high_water_for_test();
            let mut warm_control = root
                .cursor(
                    reverse,
                    Bound::Included(&key),
                    Bound::Included(&key),
                    memory.clone(),
                )
                .unwrap();
            assert_eq!(
                warm_control
                    .next_edge_with(&|| Ok(()))
                    .unwrap()
                    .unwrap()
                    .edge(),
                &predecessor
            );
            let successful_peak = memory.high_water_for_test();
            assert!(
                successful_peak > key.len(),
                "the warmed actual source operation retains its key/decoder/proof requirement"
            );
            drop(warm_control);
            assert_eq!(
                memory.usage().used,
                0,
                "the warm control returns its operation credits before the refusal"
            );
            // The measured control is fully released before the refusing
            // operation opens independent zero-cache captured roots. Its
            // proof state and forward/reverse pages cannot warm the retry.
            drop(roots);
            drop(store);
            let (store, roots) = cold_roots();
            let root = &roots[route];
            assert_eq!(
                store.resident_cache_stats_for_test().unwrap().3,
                0,
                "the independently reopened refusal writer has zero resident cache bytes"
            );
            if let graph_read::GraphReadRoot::Image(image) = root {
                assert_eq!(
                    image.database.cache_stats().used_bytes(),
                    0,
                    "the independently reopened refusal image has no resident page cache"
                );
            }
            memory.set_budget(Some(successful_peak - 1)).unwrap();
            let outcome = root
                .cursor(
                    reverse,
                    Bound::Included(&key),
                    Bound::Included(&key),
                    memory.clone(),
                )
                .and_then(|mut cursor| cursor.next_edge_with(&|| Ok(())).map(|_| ()));
            let error =
                outcome.expect_err("one byte below the observed complete operation peak refuses");
            assert!(
                matches!(error, Error::MemoryBudgetExceeded { budget_limit_bytes, .. } if budget_limit_bytes == successful_peak - 1),
                "page/decode admission preserves its memory refusal: {error:?}"
            );
            assert_eq!(
                memory.usage().used,
                0,
                "failed page/decode operation releases every partial credit"
            );
            assert_eq!(
                memory.underflow_count_for_test(),
                0,
                "credits return exactly once"
            );
            assert_eq!(
                graph_publication_state(&store),
                before,
                "read admission refusal changes no durable history, roots or progress"
            );
            memory.set_budget(Some(declared_child)).unwrap();
            let mut restored = root
                .cursor(
                    reverse,
                    Bound::Included(&key),
                    Bound::Included(&key),
                    memory.clone(),
                )
                .unwrap();
            assert_eq!(
                restored.next_edge_with(&|| Ok(())).unwrap().unwrap().edge(),
                &predecessor,
                "retry uses the same captured source after refusal"
            );
            drop(restored);
            assert_eq!(memory.usage().used, 0);
            if reverse {
                assert_graph_source_answers(root.clone(), &predecessor, true, &entries);
            }
            drop(roots);
            drop(store);
        }
    }
    let fixture_open_accountant_43720 =
        std::sync::Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
    let _fixture_decode_scope_43720 =
        crate::read_image_memory::ImageMemory::enter_writer_operation(
            std::sync::Arc::clone(&fixture_open_accountant_43720).into(),
        )
        .unwrap();
    let store = RedbPersistence::open_with_cache(&path, 0).unwrap();
    store.attach_memory_accountant(std::sync::Arc::clone(&fixture_open_accountant_43720).into());
    assert_eq!(
        graph_publication_state(&store),
        before,
        "all refusals and retries leave the complete fixture unchanged"
    );
}

#[cfg(any(test, feature = "test-seams"))]
#[derive(Default)]
struct GraphDoorCancellationObserver {
    armed: std::sync::atomic::AtomicBool,
    first: std::sync::Mutex<Option<crate::read_session::ReadKernelSourceEvent>>,
    caller_cancellation:
        std::sync::Mutex<Option<contextdb_core::read_contract::OwnerReadCancellation>>,
}
#[cfg(any(test, feature = "test-seams"))]
impl crate::read_session::ReadKernelTestObserver for GraphDoorCancellationObserver {
    fn before_source_touch(
        &self,
        event: crate::read_session::ReadKernelSourceEvent,
        cancellation: contextdb_core::read_contract::OwnerReadCancellation,
    ) {
        if event.source == crate::read_session::ReadKernelSource::GraphEdge
            && self.armed.swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            *self.first.lock().unwrap() = Some(event);
            let caller = self.caller_cancellation.lock().unwrap().clone();
            if let Some(caller) = caller {
                caller.cancel();
            } else {
                cancellation.cancel();
            }
        }
    }
    fn cancellation_observed(
        &self,
        _: crate::read_session::ReadKernelCancellationEvent,
        _: contextdb_core::read_contract::OwnerReadCancellation,
    ) {
    }
}

// Use stored ACL metadata and actual grant lookup. An allowed target and
// edge never confer the missing source entitlement on either public route.
#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn assert_graph_authorization_and_cancellation_rows(directory: &Path) {
    use crate::read_session::{ReadSession, ReadSessionOptions};
    use contextdb_core::Principal;
    use contextdb_core::read_contract::ReadRoute;
    let principal = "graph-door-reader";
    let source = uuid::Uuid::from_u128(1);
    let target = uuid::Uuid::from_u128(2);
    let source_acl = uuid::Uuid::from_u128(3);
    let allowed_acl = uuid::Uuid::from_u128(4);
    let query = "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $source COLUMNS (b.id AS t))";
    let anchor = uuid::Uuid::from_u128(6);
    let params = HashMap::from([
        ("source".into(), Value::Uuid(source)),
        ("anchor".into(), Value::Uuid(anchor)),
        ("target".into(), Value::Uuid(target)),
    ]);
    for allow_source in [false, true] {
        for malformed_projection in [None, Some(false), Some(true)] {
            let malformed_source = malformed_projection.is_some();
            let path = directory.join(format!(
                "graph-real-source-permission-{allow_source}-{malformed_projection:?}.db"
            ));
            let observer = Arc::new(GraphDoorCancellationObserver::default());
            let database = Database::open_with_options(
                &path,
                crate::DatabaseOpenOptions {
                    maintenance_policy: crate::database::MaintenancePolicy::CallerDriven,
                    test_kernel_observer: Some(observer.clone()),
                    ..Default::default()
                },
            )
            .unwrap();
            for sql in [
                "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID) SYNC OFF",
                // Non-unique id keeps the visibility controls list-shaped: a hidden
                // PRIMARY KEY identity correctly returns AclDenied, not an empty list.
                "CREATE TABLE nodes (id UUID, acl_id UUID ACL REFERENCES acl_grants(acl_id)) SYNC OFF",
                "CREATE INDEX graph_permission_nodes_id ON nodes(id)",
                "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT, acl_id UUID ACL REFERENCES acl_grants(acl_id)) SYNC OFF",
            ] {
                database.execute(sql, &HashMap::new()).unwrap();
            }
            database
                .execute(
                    "INSERT INTO acl_grants VALUES ($id, 'Agent', $principal, $acl)",
                    &HashMap::from([
                        ("id".into(), Value::Uuid(allowed_acl)),
                        ("principal".into(), Value::Text(principal.into())),
                        ("acl".into(), Value::Uuid(allowed_acl)),
                    ]),
                )
                .unwrap();
            if allow_source {
                database
                    .execute(
                        "INSERT INTO acl_grants VALUES ($id, 'Agent', $principal, $acl)",
                        &HashMap::from([
                            ("id".into(), Value::Uuid(source_acl)),
                            ("principal".into(), Value::Text(principal.into())),
                            ("acl".into(), Value::Uuid(source_acl)),
                        ]),
                    )
                    .unwrap();
            }
            for (id, acl) in [(source, source_acl), (target, allowed_acl)] {
                database
                    .execute(
                        "INSERT INTO nodes VALUES ($id, $acl)",
                        &HashMap::from([
                            ("id".into(), Value::Uuid(id)),
                            ("acl".into(), Value::Uuid(acl)),
                        ]),
                    )
                    .unwrap();
            }
            database
                .execute(
                    "INSERT INTO edges VALUES ($id, $source, $target, 'LINKS', $acl)",
                    &HashMap::from([
                        ("id".into(), Value::Uuid(uuid::Uuid::from_u128(5))),
                        ("source".into(), Value::Uuid(source)),
                        ("target".into(), Value::Uuid(target)),
                        ("acl".into(), Value::Uuid(allowed_acl)),
                    ]),
                )
                .unwrap();
            if !allow_source {
                database
                    .execute(
                        "INSERT INTO nodes VALUES ($id, $acl)",
                        &HashMap::from([
                            ("id".into(), Value::Uuid(anchor)),
                            ("acl".into(), Value::Uuid(allowed_acl)),
                        ]),
                    )
                    .unwrap();
                // A real graph edge enters the denied source from a visible node.
                // The original relational edge/grant controls remain unchanged.
                let tx = database.begin().unwrap();
                database
                    .insert_edge(tx, anchor, source, "LINKS".into(), HashMap::new())
                    .unwrap();
                database.commit(tx).unwrap();
            }
            for query in std::iter::once(query).chain((!allow_source).then_some(
            "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->{1,2}(b) WHERE a.id = $anchor AND b.id = $target COLUMNS (b.id AS t))"
        )) {
        assert_eq!(database.execute(query, &params).unwrap().rows(), vec![vec![Value::Uuid(target)]], "admin control confirms the actual stored graph edge");
        }
            database.close().unwrap();
            drop(database);
            if malformed_source {
                let fixture_open_accountant_43807 =
                    std::sync::Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
                let _fixture_decode_scope_43807 =
                    crate::read_image_memory::ImageMemory::enter_writer_operation(
                        std::sync::Arc::clone(&fixture_open_accountant_43807).into(),
                    )
                    .unwrap();
                let persistence = RedbPersistence::open(&path).unwrap();
                persistence.attach_memory_accountant(
                    std::sync::Arc::clone(&fixture_open_accountant_43807).into(),
                );
                persistence
                    .with_db(|db| {
                        let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
                        {
                            let mut table = write
                                .open_table(if malformed_projection == Some(true) {
                                    GRAPH_REV_TABLE
                                } else {
                                    GRAPH_FWD_TABLE
                                })
                                .map_err(RedbPersistence::storage_error)?;
                            let (key, mut raw) = {
                                let endpoints = if malformed_projection == Some(true) {
                                    [target.as_bytes().as_slice(), source.as_bytes().as_slice()]
                                        .concat()
                                } else {
                                    [source.as_bytes().as_slice(), target.as_bytes().as_slice()]
                                        .concat()
                                };
                                let (key, raw) = table
                                    .iter()
                                    .map_err(RedbPersistence::storage_error)?
                                    .map(|entry| entry.map_err(RedbPersistence::storage_error))
                                    .find(|entry| {
                                        entry.as_ref().is_ok_and(|(key, _)| {
                                            key.value().starts_with(&endpoints)
                                        })
                                    })
                                    .unwrap()?;
                                (key.value().to_vec(), raw.value().to_vec())
                            };
                            raw.push(0);
                            table
                                .insert(key.as_slice(), raw.as_slice())
                                .map_err(RedbPersistence::storage_error)?;
                        }
                        // Refresh the final physical descriptor bindings so the
                        // reader reaches the damaged accessed payload, rather
                        // than merely refusing an obsolete descriptor at open.
                        RedbPersistence::write_writer_recovery_bounds_in_write(&write, None)?;
                        write.commit().map_err(RedbPersistence::storage_error)?;
                        Ok(())
                    })
                    .unwrap();
                drop(persistence);
            }
            let database = Database::open_with_options(
                &path,
                crate::DatabaseOpenOptions {
                    maintenance_policy: crate::database::MaintenancePolicy::CallerDriven,
                    test_kernel_observer: Some(observer.clone()),
                    ..Default::default()
                },
            )
            .unwrap();
            let options = ReadSessionOptions {
                principal: Some(Principal::Agent(principal.into())),
                ..Default::default()
            };
            let owner = ReadSession::open_owner_only(&path, options.clone()).unwrap();
            assert_eq!(owner.route(), ReadRoute::Owner);
            assert_graph_permission_session(
                &owner,
                &observer,
                allow_source,
                malformed_source,
                query,
                &params,
                target,
            );
            drop(owner);
            database.close().unwrap();
            drop(database);
            let file =
                ReadSession::open_with_kernel_observer_for_test(&path, options, observer.clone())
                    .unwrap();
            assert_eq!(file.route(), ReadRoute::File);
            assert_graph_permission_session(
                &file,
                &observer,
                allow_source,
                malformed_source,
                query,
                &params,
                target,
            );
        }
    }
}

#[cfg(any(test, feature = "test-seams"))]
fn assert_graph_permission_session(
    session: &crate::read_session::ReadSession,
    observer: &GraphDoorCancellationObserver,
    allow_source: bool,
    malformed_source: bool,
    query: &str,
    params: &HashMap<String, Value>,
    target: uuid::Uuid,
) {
    let expected = if allow_source {
        vec![vec![Value::Uuid(target)]]
    } else {
        Vec::new()
    };
    assert_eq!(
        session
            .execute(
                "SELECT id FROM nodes WHERE id = $target",
                &HashMap::from([("target".into(), Value::Uuid(target))])
            )
            .unwrap()
            .rows(),
        vec![vec![Value::Uuid(target)]],
        "the principal genuinely holds the target grant"
    );
    assert_eq!(
        session
            .execute("SELECT edge_type FROM edges", &HashMap::new())
            .unwrap()
            .rows(),
        vec![vec![Value::Text("LINKS".into())]],
        "the principal genuinely holds the edge grant"
    );
    assert_eq!(
        session
            .execute("SELECT id FROM nodes WHERE id = $source", params)
            .unwrap()
            .rows(),
        if allow_source {
            vec![vec![params["source"].clone()]]
        } else {
            Vec::new()
        },
        "the source permission is decided by stored ACL metadata"
    );
    if malformed_source && allow_source {
        let before = session.route_resources_for_test().unwrap();
        let error = session.execute(query, params).unwrap_err();
        assert!(
            matches!(error, Error::StoreCorrupted { .. }),
            "an allowed source with a malformed accessed primary retains typed corruption: {error:?}"
        );
        let after = session.route_resources_for_test().unwrap();
        assert_eq!(
            after.active_cursors, before.active_cursors,
            "typed accessed-source failure closes its operation lifetime"
        );
        let error = session.execute(query, params).unwrap_err();
        assert!(
            matches!(error, Error::StoreCorrupted { .. }),
            "the unchanged malformed source keeps refusing on a fresh operation: {error:?}"
        );
        return;
    }
    if !allow_source {
        let error = session.execute(query, params).unwrap_err();
        assert!(
            matches!(error, Error::AclDenied { .. }),
            "a hidden pinned graph anchor keeps its existing typed denial: {error:?}"
        );
    }
    // Hidden nodes reached by a walk are filtered; hidden pinned starts are refused.
    let query = if allow_source {
        query
    } else {
        "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->{1,2}(b) WHERE a.id = $anchor AND b.id = $target COLUMNS (b.id AS t))"
    };
    assert_eq!(
        session.execute(query, params).unwrap().rows(),
        expected,
        "denied source plus allowed target and edge yields zero on this real route"
    );
    if allow_source {
        let before = session.route_resources_for_test().unwrap();
        let caller_cancellation = contextdb_core::read_contract::OwnerReadCancellation::new();
        *observer.caller_cancellation.lock().unwrap() = Some(caller_cancellation.clone());
        observer
            .armed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let error = session
            .execute_with_cancellation(query, params, &caller_cancellation)
            .unwrap_err();
        observer.caller_cancellation.lock().unwrap().take();
        assert!(
            matches!(error, Error::ReadCancelled),
            "source-boundary cancellation keeps its exact public type: {error:?}"
        );
        let event = observer
            .first
            .lock()
            .unwrap()
            .take()
            .expect("the real graph source callback fired");
        assert_eq!(
            event.completed_items, 0,
            "cancellation is driven before the first graph edge is fetched"
        );
        let after = session.route_resources_for_test().unwrap();
        assert_eq!(
            after.active_cursors, before.active_cursors,
            "cancelled statement releases its cursor lifetime"
        );
        assert_eq!(
            session.execute(query, params).unwrap().rows(),
            expected,
            "fresh operation survives cancellation without changing the pinned source"
        );
    }
    let cancelled = contextdb_core::read_contract::OwnerReadCancellation::new();
    cancelled.cancel();
    let error = session
        .execute_with_cancellation(query, params, &cancelled)
        .unwrap_err();
    assert!(
        matches!(error, Error::ReadCancelled),
        "pre-cancelled public read keeps cancellation distinct from denied permission: {error:?}"
    );
    assert_eq!(
        session.execute(query, params).unwrap().rows(),
        expected,
        "typed refusal and cancellation leave authorization and source answers unchanged"
    );
}

#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn empty_declared_index_publishes_marker_and_postings_for_a_lazy_ordered_read() {
    // The raw codec fixture owns this explicitly declared operation.
    let _fixture_decode_scope = crate::read_image_memory::ImageMemory::enter_writer_operation(
        crate::memory_accounting::AccountantHandle::standalone(None, true).unwrap(),
    )
    .unwrap();

    {
        use redb::ReadableTable;
        let mut failures = Vec::new();
        for damage in [
            "malformed_current",
            "malformed_historical",
            "native_page",
            "healthy_budget",
        ] {
            let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join("typed-relational-refusal.db");
                let database = Database::open(&path).unwrap();
                database
                    .execute(
                        "CREATE TABLE typed_refusal_rows (id INT PRIMARY KEY, body TEXT)",
                        &HashMap::new(),
                    )
                    .unwrap();
                let payload = "typed-relational-native-page-unique-marker-".repeat(8192);
                database
                    .execute(
                        "INSERT INTO typed_refusal_rows VALUES (1, $body)",
                        &HashMap::from([("body".into(), Value::Text(payload.clone()))]),
                    )
                    .unwrap();
                let historical = database.writer_read_root().unwrap().unwrap().snapshot;
                let selected = database
                    .scan("typed_refusal_rows", historical)
                    .unwrap()
                    .remove(0);
                if damage == "malformed_historical" {
                    database
                        .execute(
                            "UPDATE typed_refusal_rows SET body = 'new head' WHERE id = 1",
                            &HashMap::new(),
                        )
                        .unwrap();
                }
                let current = database.writer_read_root().unwrap().unwrap().snapshot;
                let snapshot = if damage == "malformed_historical" {
                    historical
                } else {
                    current
                };
                let statements = [
                    "SELECT id, body FROM typed_refusal_rows WHERE id = 1",
                    "SELECT id, body FROM typed_refusal_rows WHERE id IN (1, 2)",
                    "SELECT id, body FROM typed_refusal_rows",
                ];
                let controls = statements.map(|sql| {
                    database
                        .execute_at_snapshot(sql, &HashMap::new(), snapshot)
                        .unwrap()
                        .rows()
                        .to_vec()
                });
                assert!(
                    controls.iter().all(|rows| !rows.is_empty()),
                    "{damage}: every actual read door has a healthy answer"
                );
                database.close().unwrap();
                drop(database);
                let key = RedbPersistence::rel_row_key(&selected);
                let name = RedbPersistence::rel_table_name("typed_refusal_rows");
                let definition = TableDefinition::<&[u8], &[u8]>::new(&name);
                if damage.starts_with("malformed_") {
                    let raw = redb::Database::open(&path).unwrap();
                    let write = raw.begin_write().unwrap();
                    {
                        let mut table = write.open_table(definition).unwrap();
                        assert!(
                            table.get(key.as_slice()).unwrap().is_some(),
                            "the real version key exists before body damage"
                        );
                        table
                            .insert(key.as_slice(), b"{not a versioned row".as_slice())
                            .unwrap();
                        assert!(
                            table.get(key.as_slice()).unwrap().is_some(),
                            "malformed bytes preserve the valid physical key"
                        );
                    }
                    write.commit().unwrap();
                    drop(raw);
                } else if damage == "native_page" {
                    let raw = redb::Database::open(&path).unwrap();
                    let encoded = {
                        let read = raw.begin_read().unwrap();
                        let table = read.open_table(definition).unwrap();
                        let value = table.get(key.as_slice()).unwrap().unwrap();
                        value.value().to_vec()
                    };
                    drop(raw);
                    let mut file = std::fs::read(&path).unwrap();
                    let offsets: Vec<_> = file
                        .windows(encoded.len())
                        .enumerate()
                        .filter_map(|(offset, bytes)| {
                            (bytes == encoded.as_slice()).then_some(offset)
                        })
                        .collect();
                    assert!(
                        !offsets.is_empty(),
                        "locate the actual persisted relational body, without a guessed page offset"
                    );
                    for offset in offsets {
                        file[offset + encoded.len() - 2] ^= 1;
                    }
                    std::fs::write(&path, file).unwrap();
                }
                if damage == "healthy_budget" {
                    // A tight budget on healthy data must retain a typed memory refusal.
                    // The internal admission stage and exact byte figures are not the contract.
                    let mut options = ReadSessionOptions::default();
                    options.memory_limit = Some(payload.len());
                    options.limits.memory = payload.len().try_into().unwrap();
                    let reader = ReadSession::open_with_options(&path, options).expect("healthy metadata opens under the tight budget before the body is requested");
                    let error = reader.execute(statements[2], &HashMap::new()).unwrap_err();
                    // The read's own declared ceiling answers in the read
                    // vocabulary (dev contract); the store budget keeps its
                    // MemoryBudgetExceeded. Either is the typed memory refusal.
                    let typed_memory = match &error {
                        Error::MemoryBudgetExceeded {
                            requested_bytes,
                            available_bytes,
                            ..
                        } => requested_bytes > available_bytes,
                        Error::ReadFailure(failure) => matches!(failure.detail(),
                            contextdb_core::read_contract::ReadFailureDetail::OwnerLimitExceeded(detail)
                                if detail.limit == contextdb_core::read_contract::ReadFailureLimit::Memory),
                        _ => false,
                    };
                    assert!(
                        typed_memory,
                        "a healthy body that exceeds actual admission retains typed memory refusal: {error:?}"
                    );
                    assert!(
                        !matches!(&error, Error::StoreCorrupted { .. }),
                        "a tight budget on healthy data must never report store corruption: {error:?}"
                    );
                    drop(reader);
                    let reader =
                        ReadSession::open_with_options(&path, ReadSessionOptions::default())
                            .unwrap();
                    assert_eq!(
                        reader
                            .execute(statements[2], &HashMap::new())
                            .unwrap()
                            .rows(),
                        controls[2],
                        "tight-budget refusal preserves the healthy store and its complete answer"
                    );
                } else if damage == "native_page" {
                    let result =
                        ReadSession::open_with_options(&path, ReadSessionOptions::default())
                            .and_then(|reader| reader.execute(statements[2], &HashMap::new()));
                    match result {
                        Err(Error::StoreCorrupted { reason, .. }) => assert!(
                            !(reason == ""),
                            "native relational page corruption explains its refusal"
                        ),
                        other => panic!(
                            "native relational page corruption must refuse as StoreCorrupted: {other:?}"
                        ),
                    }
                } else {
                    let database = Database::open(&path).unwrap();
                    let mut failed_doors = Vec::new();
                    for sql in statements {
                        match if damage == "malformed_current" {
                            database.execute(sql, &HashMap::new())
                        } else {
                            database.execute_at_snapshot(sql, &HashMap::new(), snapshot)
                        } {
                            Err(Error::StoreCorrupted { reason, .. }) if !(reason == "") => {}
                            other => failed_doors.push((sql, format!("{other:?}"))),
                        }
                    }
                    assert!(
                        failed_doors.is_empty(),
                        "{damage}: every malformed valid-key point/list/scan door must refuse as StoreCorrupted with a reason: {failed_doors:?}"
                    );
                }
            }));
            if attempt.is_err() {
                failures.push(damage);
            }
        }
        assert_eq!(
            failures,
            Vec::<&str>::new(),
            "malformed bodies and native page corruption retain StoreCorrupted while healthy admission exhaustion retains MemoryBudgetExceeded"
        );
    }
    {
        use redb::ReadableTable;
        let mut failures = Vec::new();
        for damage in [
            "valid_key_historical_body",
            "missing_historical_version",
            "omitted_first_posting",
        ] {
            let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join("public-writer-historical-gap.db");
                let table_name = "public_history_rows";
                let sql = if damage == "omitted_first_posting" {
                    "SELECT id, body FROM public_history_rows ORDER BY bucket ASC LIMIT 1"
                } else {
                    "SELECT id, body FROM public_history_rows WHERE bucket IN (1, 3)"
                };
                let database = Database::open(&path).unwrap();
                for statement in [
                    "CREATE TABLE public_history_rows (id INT PRIMARY KEY, bucket INT UNIQUE, body TEXT)",
                    "CREATE INDEX public_history_bucket ON public_history_rows (bucket)",
                    "INSERT INTO public_history_rows (id, bucket, body) VALUES (1, 1, 'first')",
                    "INSERT INTO public_history_rows (id, bucket, body) VALUES (2, 2, 'unaffected')",
                    "UPDATE public_history_rows SET body = 'interior' WHERE id = 1",
                ] {
                    if statement.starts_with("CREATE INDEX") && damage != "omitted_first_posting" {
                        continue;
                    }
                    database.execute(statement, &HashMap::new()).unwrap();
                }
                let historical = database.writer_read_root().unwrap().unwrap().snapshot;
                let expected = database
                    .execute_at_snapshot(sql, &HashMap::new(), historical)
                    .unwrap();
                eprintln!(
                    "CDB137_PUBLIC_CONTROL {damage}: plan={:?}, index={:?}",
                    expected.trace().physical_plan,
                    expected.trace().index_used
                );
                let index_name = expected
                    .trace()
                    .index_used
                    .clone()
                    .expect("the public control selects a maintained index");
                if damage != "omitted_first_posting" {
                    let meta = database.table_meta(table_name).unwrap();
                    let selected = meta
                        .indexes
                        .iter()
                        .find(|declaration| declaration.name == index_name)
                        .unwrap();
                    assert_eq!(
                        selected.kind,
                        contextdb_core::IndexKind::Auto,
                        "the historical IN-list binds the actual automatic exact-index door"
                    );
                    assert_eq!(
                        selected.columns.len(),
                        1,
                        "the automatic exact door has one declared key column"
                    );
                }
                assert!(
                    !expected.rows().is_empty(),
                    "{damage}: a real historical answer is owed"
                );
                database
                    .execute(
                        "UPDATE public_history_rows SET body = 'head' WHERE id = 1",
                        &HashMap::new(),
                    )
                    .unwrap();
                drop(database);
                let reopened = Database::open(&path).unwrap();
                let complete = reopened
                    .execute_at_snapshot(sql, &HashMap::new(), historical)
                    .unwrap();
                assert_eq!(
                    complete.rows(),
                    expected.rows(),
                    "{damage}: cold public historical control equals the pre-close answer"
                );
                drop(reopened);
                let fixture_open_accountant_44039 =
                    std::sync::Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
                let _fixture_decode_scope_44039 =
                    crate::read_image_memory::ImageMemory::enter_writer_operation(
                        std::sync::Arc::clone(&fixture_open_accountant_44039).into(),
                    )
                    .unwrap();
                let store = RedbPersistence::open(&path).unwrap();
                store.attach_memory_accountant(
                    std::sync::Arc::clone(&fixture_open_accountant_44039).into(),
                );
                let metadata = store
                    .load_all_table_meta()
                    .unwrap()
                    .remove(table_name)
                    .unwrap();
                let versions = store.load_relational_table(table_name).unwrap();
                let interior = versions
                    .into_iter()
                    .find(|row| row.values.get("body") == Some(&Value::Text("interior".into())))
                    .unwrap();
                let key = RedbPersistence::rel_row_key(&interior);
                drop(store);
                drop(_fixture_decode_scope_44039);
                let raw = redb::Database::open(&path).unwrap();
                let write = raw.begin_write().unwrap();
                if damage == "omitted_first_posting" {
                    let name =
                        RedbPersistence::relational_index_table_name(table_name, &index_name);
                    let mut postings = write
                        .open_table(TableDefinition::<DurableRelationalIndexKey, &[u8]>::new(
                            &name,
                        ))
                        .unwrap();
                    let first = postings
                        .iter()
                        .unwrap()
                        .find_map(|item| {
                            let (key, value) = item.unwrap();
                            let position = key.value().decode_canonical().unwrap();
                            let deleted: Option<TxId> =
                                RedbPersistence::decode_exact(value.value()).unwrap();
                            (position.row_id == interior.row_id
                                && position.created_tx.0 <= historical.0
                                && deleted.is_none_or(|tx| tx.0 > historical.0))
                            .then(|| key.value())
                        })
                        .unwrap();
                    assert!(
                        postings.remove(&first).unwrap().is_some(),
                        "{damage}: the real first historical posting was omitted"
                    );
                } else {
                    let name = RedbPersistence::rel_table_name(table_name);
                    let mut rows = write
                        .open_table(TableDefinition::<&[u8], &[u8]>::new(&name))
                        .unwrap();
                    if damage == "valid_key_historical_body" {
                        let mut changed = interior.clone();
                        changed
                            .values
                            .insert("body".into(), Value::Text("wrong historical value".into()));
                        assert_eq!(
                            RedbPersistence::rel_row_key(&changed),
                            key,
                            "body tampering preserves the real physical version key"
                        );
                        let body = RedbPersistence::encode_versioned_row(&changed, Some(&metadata))
                            .unwrap();
                        rows.insert(key.as_slice(), body.as_slice()).unwrap();
                    } else {
                        assert!(
                            rows.remove(key.as_slice()).unwrap().is_some(),
                            "the owed historical version was physically removed"
                        );
                    }
                }
                write.commit().unwrap();
                drop(raw);
                let database = Database::open(&path).unwrap();
                match database.execute_at_snapshot(sql, &HashMap::new(), historical) {
                    Err(Error::StoreCorrupted {
                        path: logical,
                        reason,
                    }) => {
                        assert!(
                            !(logical == "") && !(reason == ""),
                            "{damage}: typed corruption identifies its logical source"
                        );
                    }
                    Ok(actual) => panic!(
                        "{damage}: required corruption refusal before wrong historical answer; actual={:?}; fresh_control={:?}",
                        actual.rows(),
                        expected.rows()
                    ),
                    Err(other) => panic!("{damage}: required typed corruption, got {other:?}"),
                }
                drop(database);
            }));
            if attempt.is_err() {
                failures.push(damage);
            }
        }
        assert_eq!(
            failures,
            Vec::<&str>::new(),
            "all three public historical writer gaps refuse before a wrong answer"
        );
    }
    if graph_compaction_interruption_child() {
        return;
    }
    if graph_durability_interruption_child() {
        return;
    }
    {
        // One differential row, multiple adversarial cells, against the real serializer.
        let window = 64_usize * 1024; // Exact native emitter window at emit_string_content.
        let mut adversarial_json_values = Vec::new();
        for character in ['é', '€', '🦀'] {
            for split in 1..character.len_utf8() {
                for output_prefix in [0_usize, 1, 2] {
                    let mut text = "a".repeat(window - split - output_prefix);
                    text.push(character);
                    text.push_str("\0\u{0001}\u{0008}\t\n\r\u{001f}\"\\\u{2028}\u{2029}");
                    text.push_str(&"z".repeat(window + character.len_utf8()));
                    adversarial_json_values.push(serde_json::Value::String(text));
                }
            }
        }
        adversarial_json_values.push(serde_json::json!({
        "quoted\"key\\with\ncontrols\u{2028}": [
            {"nested": "quoted\"text\\slashes\u{2029}", "empty": []},
            {"numbers": [i64::MIN, i64::MAX, u64::MAX, -0.0_f64, f64::MAX, f64::MIN_POSITIVE, 5e-324_f64, -123456789.125_f64]},
            [true, false, null, {"deeper": [{"escape": "\0\u{001f}\"\\"}]}]
        ],
        "empty-map": {}, "empty-string": ""
    }));
        adversarial_json_values.push(serde_json::json!([
            "\u{2028}\u{2029}", "\\\"\\\"", [], {},
            {"large-string": "🌍".repeat(window), "nested-json": {"quoted": "\"\\"}}
        ]));
        let mut values: Vec<contextdb_core::Value> = adversarial_json_values
            .into_iter()
            .flat_map(|json| match json {
                serde_json::Value::String(text) => vec![
                    contextdb_core::Value::Text(text.clone()),
                    contextdb_core::Value::Json(serde_json::Value::String(text)),
                ],
                other => vec![contextdb_core::Value::Json(other)],
            })
            .collect();
        values.extend([
            contextdb_core::Value::Int64(i64::MIN),
            contextdb_core::Value::Int64(i64::MAX),
            contextdb_core::Value::Float64(-123456789.125),
            contextdb_core::Value::Float64(-0.0),
            contextdb_core::Value::Float64(f64::MAX),
        ]);
        for value in values {
            let expected = serde_json::to_string(&value).expect("reference Value serialization");
            let mut actual = Vec::new();
            crate::persistence::content_identity::version_manifest::emit_value(
                &mut actual,
                &value,
                &mut || Ok(()),
            )
            .expect("bounded native Value emission");
            assert_eq!(
                actual,
                expected.as_bytes(),
                "bounded Value emission equals serde_json byte for byte"
            );
        }
    }
    let root = tempfile::tempdir().expect("task-scoped empty-index directory");
    // Addressed reads prove the complete version group before returning
    // its row, identity or absence, including the real authentication batch.
    let mut failed_addressed_rows = Vec::new();
    for damage in [
        "missing_interior_version",
        "injected_extra_version",
        "entirely_missing_deleted_group",
        "false_terminal_absence",
    ] {
        let fixture_outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let path = root.path().join(format!("addressed-{damage}.db"));
            let database = Database::open(&path).unwrap();
            database
                .execute(
                    "CREATE TABLE addressed_rows (id INT PRIMARY KEY, body TEXT)",
                    &HashMap::new(),
                )
                .unwrap();
            database
                .execute(
                    "CREATE TABLE addressed_control (id INT PRIMARY KEY, body TEXT)",
                    &HashMap::new(),
                )
                .unwrap();
            database
                .execute(
                    "INSERT INTO addressed_control VALUES (1, 'unaffected')",
                    &HashMap::new(),
                )
                .unwrap();
            database
                .execute(
                    "INSERT INTO addressed_rows VALUES (1, 'first')",
                    &HashMap::new(),
                )
                .unwrap();
            database
                .execute(
                    "UPDATE addressed_rows SET body = 'interior' WHERE id = 1",
                    &HashMap::new(),
                )
                .unwrap();
            let historical = database.writer_read_root().unwrap().unwrap().snapshot;
            let historical_row = database
                .scan("addressed_rows", historical)
                .unwrap()
                .into_iter()
                .next()
                .unwrap();
            database
                .execute(
                    "UPDATE addressed_rows SET body = 'head' WHERE id = 1",
                    &HashMap::new(),
                )
                .unwrap();
            if damage == "entirely_missing_deleted_group" {
                database
                    .execute("DELETE FROM addressed_rows WHERE id = 1", &HashMap::new())
                    .unwrap();
            }
            let current = database.writer_read_root().unwrap().unwrap().snapshot;
            let current_row = database
                .scan("addressed_rows", current)
                .unwrap()
                .into_iter()
                .next();
            let control = database
                .scan("addressed_control", current)
                .unwrap()
                .into_iter()
                .next()
                .unwrap();
            let meta = database.table_meta("addressed_rows").unwrap();
            let control_meta = database.table_meta("addressed_control").unwrap();
            let provider = database.declared_read_request_provider();
            let fixture_writer_accountant = database.bounded_read_accountant();
            database.close().unwrap();
            drop(database);

            // Identify and save the physical member through the production
            // row-key helper and decoder; no test codec or key layout.
            let fixture_open_accountant_44167 =
                std::sync::Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
            let _fixture_decode_scope_44167 =
                crate::read_image_memory::ImageMemory::enter_writer_operation(
                    std::sync::Arc::clone(&fixture_open_accountant_44167).into(),
                )
                .unwrap();
            let store = RedbPersistence::open(&path).unwrap();
            store.attach_memory_accountant(
                std::sync::Arc::clone(&fixture_open_accountant_44167).into(),
            );
            let versions = store.load_relational_table("addressed_rows").unwrap();
            drop(store);
            drop(_fixture_decode_scope_44167);
            let interior_key = RedbPersistence::rel_row_key(&historical_row);
            let group_keys = versions
                .iter()
                .filter(|row| row.row_id == historical_row.row_id)
                .map(RedbPersistence::rel_row_key)
                .collect::<Vec<_>>();
            let table_name = RedbPersistence::rel_table_name("addressed_rows");
            let definition = TableDefinition::<&[u8], &[u8]>::new(&table_name);
            let raw = redb::Database::create(&path).unwrap();
            let interior_bytes = {
                let read = raw.begin_read().unwrap();
                let table = read.open_table(definition).unwrap();
                table
                    .get(interior_key.as_slice())
                    .unwrap()
                    .unwrap()
                    .value()
                    .to_vec()
            };
            drop(raw);

            for phase in ["valid", "damaged"] {
                if phase == "damaged" {
                    if damage == "injected_extra_version" {
                        // Lawful replacement retires the exact member;
                        // restoring its saved bytes then makes it extra.
                        let fixture_open_accountant_44188 = fixture_writer_accountant.clone();
                        let _fixture_decode_scope_44188 =
                            crate::read_image_memory::ImageMemory::enter_writer_operation(
                                fixture_open_accountant_44188.clone(),
                            )
                            .unwrap();
                        let store = RedbPersistence::open(&path).unwrap();
                        store.attach_memory_accountant(fixture_open_accountant_44188.clone());
                        let retained = versions
                            .iter()
                            .filter(|row| RedbPersistence::rel_row_key(row) != interior_key)
                            .cloned()
                            .collect::<Vec<_>>();
                        store
                            .rewrite_table_rows("addressed_rows", &retained)
                            .unwrap();
                        drop(store);
                        drop(_fixture_decode_scope_44188);
                    }
                    let raw = redb::Database::create(&path).unwrap();
                    let write = raw.begin_write().unwrap();
                    {
                        let mut table = write.open_table(definition).unwrap();
                        match damage {
                            "missing_interior_version" => {
                                table.remove(interior_key.as_slice()).unwrap().unwrap();
                            }
                            "injected_extra_version" => {
                                assert!(
                                    table
                                        .insert(interior_key.as_slice(), interior_bytes.as_slice())
                                        .unwrap()
                                        .is_none(),
                                    "{damage}: the lawful replacement retired exactly the injected physical member"
                                );
                            }
                            "entirely_missing_deleted_group" | "false_terminal_absence" => {
                                for key in &group_keys {
                                    table.remove(key.as_slice()).unwrap().unwrap();
                                }
                            }
                            _ => unreachable!(),
                        }
                    }
                    write.commit().unwrap();
                    drop(raw);
                }
                let (image, receipt, source) = load_read_image(&path).unwrap().into_parts();
                receipt.validate_released().unwrap();
                for (snapshot_name, snapshot, expected) in [
                    ("historical", historical, Some(&historical_row)),
                    ("current", current, current_row.as_ref()),
                ] {
                    for door in ["point", "identity", "batch"] {
                        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                            || {
                                let limits = ReadSessionOptions::default().limits;
                                let context = crate::executor::bounded::ReadOpenContext::new(
                                    provider.request_budget().unwrap(),
                                    limits,
                                    crate::read_session::read_clock(),
                                    OwnerReadCancellation::new(),
                                )
                                .unwrap();
                                context.checkpoint().unwrap();
                                let memory = context.read_memory();
                                let decoded =
                                    crate::read_image_memory::ImageMemory::charging_reader(
                                        source._source_memory.memory.accountant.clone(),
                                        memory.clone(),
                                    );
                                let (control_result, result) =
                                    graph_read::with_pages(memory, || {
                                        decoded.scope(|| {
                                            source.with_read(|read| {
                                                context.checkpoint()?;
                                                let pair = match door {
                                                    "point" => (
                                                        source.row_by_id_in_read(
                                                            read,
                                                            "addressed_control",
                                                            &control_meta,
                                                            control.row_id,
                                                            snapshot,
                                                        ),
                                                        source.row_by_id_in_read(
                                                            read,
                                                            "addressed_rows",
                                                            &meta,
                                                            historical_row.row_id,
                                                            snapshot,
                                                        ),
                                                    ),
                                                    "identity" => (
                                                        source.row_identity_by_id_in_read(
                                                            read,
                                                            "addressed_control",
                                                            &control_meta,
                                                            control.row_id,
                                                            snapshot,
                                                            None,
                                                        ),
                                                        source.row_identity_by_id_in_read(
                                                            read,
                                                            "addressed_rows",
                                                            &meta,
                                                            historical_row.row_id,
                                                            snapshot,
                                                            None,
                                                        ),
                                                    ),
                                                    "batch" => {
                                                        // The established verifier owns proof funding. Its
                                                        // constructor caps reuse at the existing source cap.
                                                        let mut batch = source
                                                            .content_identity
                                                            .batch(RETAINED_CONTENT_PROOF_BYTES);
                                                        (
                                                            source.row_identity_by_id_in_read(
                                                                read,
                                                                "addressed_control",
                                                                &control_meta,
                                                                control.row_id,
                                                                snapshot,
                                                                Some(&mut batch),
                                                            ),
                                                            source.row_identity_by_id_in_read(
                                                                read,
                                                                "addressed_rows",
                                                                &meta,
                                                                historical_row.row_id,
                                                                snapshot,
                                                                Some(&mut batch),
                                                            ),
                                                        )
                                                    }
                                                    _ => unreachable!(),
                                                };
                                                context.checkpoint()?;
                                                Ok(pair)
                                            })
                                        })
                                    })
                                    .unwrap();
                                assert_eq!(
                                    control_result.unwrap().map(|(row, _memory)| (
                                        row.row_id,
                                        row.created_tx,
                                        row.lsn,
                                        row.values
                                    )),
                                    Some((
                                        control.row_id,
                                        control.created_tx,
                                        control.lsn,
                                        control.values.clone()
                                    )),
                                    "{damage}/{phase}/{snapshot_name}/{door}: the unaffected answer equals its fresh pre-close control"
                                );
                                if phase == "valid" {
                                    assert_eq!(
                                        result.unwrap().map(|(row, _memory)| (
                                            row.row_id,
                                            row.created_tx,
                                            row.lsn,
                                            row.values
                                        )),
                                        expected.map(|row| (
                                            row.row_id,
                                            row.created_tx,
                                            row.lsn,
                                            row.values.clone()
                                        )),
                                        "{damage}/{phase}/{snapshot_name}/{door}: reopened value, identity and lawful deletion absence equal the fresh DML control"
                                    );
                                } else {
                                    match result {
                                        Err(Error::StoreCorrupted {
                                            path: actual,
                                            reason,
                                        }) => {
                                            assert_eq!(
                                                actual,
                                                path.display().to_string().as_str(),
                                                "{damage}/{snapshot_name}/{door}: pre-exposure refusal names the damaged store"
                                            );
                                            assert!(
                                                !(reason == ""),
                                                "{damage}/{snapshot_name}/{door}: pre-exposure refusal explains the incomplete group"
                                            );
                                        }
                                        other => panic!(
                                            "{damage}/{snapshot_name}/{door}: refuse before returning any affected value, identity or absence, got {other:?}"
                                        ),
                                    }
                                }
                            },
                        ));
                        if outcome.is_err() {
                            failed_addressed_rows.push((damage, phase, snapshot_name, door));
                        }
                    }
                }
                drop((source, image));
            }
        }));
        if fixture_outcome.is_err() {
            failed_addressed_rows.push((damage, "setup", "setup", "setup"));
        }
    }
    assert_eq!(
        failed_addressed_rows,
        Vec::<(&str, &str, &str, &str)>::new(),
        "every named addressed-read row and clean control must run and satisfy its oracle before the inherited host"
    );
    #[cfg(any(test, feature = "test-seams"))]
    assert_graph_admission_rows(root.path());
    #[cfg(any(test, feature = "test-seams"))]
    assert_graph_authorization_and_cancellation_rows(root.path());
    assert_graph_open_and_maintenance_rows(root.path());
    assert_graph_distinct_maintenance_rows(root.path());
    assert_public_owner_graph_observer_row(root.path());
    #[cfg(any(test, feature = "test-seams"))]
    assert_public_owner_open_through_graph_capture_row(root.path());
    assert_graph_durability_rows(root.path());
    assert_graph_compaction_pinned_rows(
        root.path(),
        &[10, 11, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
    );
    let path = root.path().join("empty-declared-index.db");
    let table = "empty_status_rows";
    let index = "empty_status_rows_status_idx";
    let unrelated_payload = "unrelated-body-".repeat(64 * 1024);

    let database = Database::open(&path).expect("create the file-backed fixture");
    database
        .execute(
            "CREATE TABLE empty_status_rows (id UUID PRIMARY KEY, status TEXT NOT NULL)",
            &HashMap::new(),
        )
        .expect("declare the empty table");
    database
        .execute(
            "CREATE INDEX empty_status_rows_status_idx ON empty_status_rows(status)",
            &HashMap::new(),
        )
        .expect("declare an index with no postings yet");
    database
        .execute(
            "CREATE TABLE unrelated_large_body (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            &HashMap::new(),
        )
        .expect("declare an unrelated body table");
    database
        .execute(
            "INSERT INTO unrelated_large_body VALUES (1, $body)",
            &HashMap::from([("body".to_owned(), Value::Text(unrelated_payload.clone()))]),
        )
        .expect("persist an unrelated long body");
    let meta = database
        .table_meta(table)
        .expect("read the declared table metadata before closing");
    database
        .close()
        .expect("close before the raw Redb inspection");

    let raw = redb::Database::open(&path).expect("open the committed fixture through Redb");
    let read = raw.begin_read().expect("one Redb read transaction");
    let marker = read
        .open_table(RELATIONAL_INDEX_READY_TABLE)
        .expect("the readiness marker table exists")
        .get(table)
        .expect("read the table readiness marker")
        .expect("the empty table has a readiness marker");
    assert_eq!(
        marker.value(),
        RELATIONAL_INDEX_READY_VERSION,
        "the marker says the declared relational indexes are ready"
    );
    let physical_name = RedbPersistence::relational_index_table_name(table, index);
    let physical: TableDefinition<DurableRelationalIndexKey, &[u8]> =
        TableDefinition::new(physical_name.as_str());
    assert!(
        read.open_table(physical).is_ok(),
        "the ready marker and the empty index posting table are committed together"
    );
    assert!(
        RedbPersistence::relational_indexes_ready_in_read(
            &read,
            &HashMap::from([(table.to_owned(), meta)]),
        )
        .expect("inspect marker and every declared backing table in this same read transaction"),
        "the committed marker names a complete relational-index image"
    );
    drop(read);
    drop(raw);

    // Keep the same unrelated config key in both images while changing
    // its stored value by orders of magnitude. Open proof work should
    // depend on the copied startup settings, not this value's size.
    let persistence = RedbPersistence::open(&path).expect("open config writer");
    persistence
        .flush_config_value("unrelated_blob", &"x".repeat(4 * 1024))
        .expect("persist a small unrelated config value");
    drop(persistence);

    let mut options = ReadSessionOptions::default();
    options.memory_limit = Some(2 * 1024 * 1024);
    options.limits.memory = 2 * 1024 * 1024;
    let open_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Open);
    let reader = ReadSession::open_with_options(&path, options.clone())
        .expect("the public file reader opens the committed index image");
    assert_eq!(reader.route(), ReadRoute::File);
    let small_config_open = open_scope.counters();
    assert_open_proves_bounded_history_and_startup_digest(
        small_config_open,
        "opening an empty declared index does not hydrate unrelated row bodies",
        403,
    );
    drop(open_scope);

    let query_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let ordered = reader
        .execute(
            "SELECT id, status FROM empty_status_rows ORDER BY status",
            &HashMap::new(),
        )
        .expect("the public ordered read uses the committed empty posting table");
    assert!(
        ordered.rows().is_empty(),
        "an ordered read of the declared empty index has no phantom rows"
    );
    assert_eq!(
        query_scope.counters().decoded_rows,
        0,
        "the empty indexed read never decodes the unrelated long body"
    );
    drop(query_scope);
    let (usage, held, peak, _) = reader
        .file_memory_for_test()
        .expect("the public reader selected the direct-file route");
    assert!(held <= usage.used && usage.used <= peak);
    assert!(
        peak < unrelated_payload.len(),
        "the reader retained {peak} bytes while the unrelated body is {} bytes",
        unrelated_payload.len()
    );
    drop(reader);

    // Grow retained row/history bodies as well as the unrelated config.
    // The empty index and its startup projection remain the same.
    let database = Database::open(&path).expect("reopen the history writer");
    database
        .execute(
            "INSERT INTO unrelated_large_body VALUES (2, $body)",
            &HashMap::from([("body".to_owned(), Value::Text(unrelated_payload.clone()))]),
        )
        .expect("double the unrelated retained row bodies and append history");
    database
        .close()
        .expect("publish the larger history fixture");

    let persistence = RedbPersistence::open(&path).expect("reopen config writer");
    persistence
        .flush_config_value("unrelated_blob", &"x".repeat(512 * 1024))
        .expect("grow the same unrelated config value");
    drop(persistence);
    let large_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Open);
    let large_reader = ReadSession::open_with_options(&path, options)
        .expect("the larger unrelated config still opens");
    assert_eq!(large_reader.route(), ReadRoute::File);
    let large_config_open = large_scope.counters();
    assert_open_proves_bounded_history_and_startup_digest(
        large_config_open,
        "opening doubled retained bodies and longer history authenticates fixed controls",
        403,
    );
    assert_eq!(
        large_config_open, small_config_open,
        "fixed-control hash bytes and zero proof-node work are identical with longer history and doubled retained bodies"
    );
}
