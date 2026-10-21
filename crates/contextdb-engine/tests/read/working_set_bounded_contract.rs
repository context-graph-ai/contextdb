#![allow(clippy::assertions_on_constants)] // Existing compatibility and test seam shape.
//! The fixed declaration, successful answers and retained charge are decisive.
//! Decode-loop counters supplement those outcomes; they cannot grant PASS alone.
use super::reader_proof_support as fixture;
use contextdb_core::{Value, VectorSearchMode};
use contextdb_engine::direct_file_reader::test_seams::{ReadWorkObservationScope, ReadWorkPhase};
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    RetainedGraphPageReadObservation, observe_retained_graph_page_reads_for_test,
};
use contextdb_engine::{Database, ReadSession};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

fn params(context: usize) -> HashMap<String, Value> {
    HashMap::from([
        ("context".into(), Value::Uuid(fixture::context_id(context))),
        (
            "q".into(),
            Value::Vector(fixture::vector(context * fixture::ROWS_PER_CONTEXT)),
        ),
    ])
}
fn indexed(reader: &ReadSession, context: usize) -> Vec<Vec<Value>> {
    let answer = reader.execute(
        "SELECT id FROM reader_work WHERE context_id = $context ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 10", &params(context))
        .expect("a selected Context fits the unchanged session and query declarations");
    assert_eq!(answer.rows.len(), 10);
    assert_eq!(
        answer.trace.vector_search.as_ref().unwrap().resolved_mode,
        VectorSearchMode::Indexed
    );
    for row in &answer.rows {
        let Value::Uuid(id) = row[0] else {
            panic!("expected UUID id")
        };
        assert_eq!(fixture::ordinal(id) / fixture::ROWS_PER_CONTEXT, context);
    }
    answer.rows
}
fn charged(reader: &ReadSession) -> usize {
    let (usage, held, peak, _) = reader.file_memory_for_test().unwrap();
    assert_eq!(usage.limit, Some(fixture::SESSION_BYTES));
    assert!(held <= usage.used && usage.used <= peak);
    assert!(
        peak <= fixture::SESSION_BYTES,
        "even transient reservations fit: {usage:?}; peak={peak}"
    );
    peak
}

#[test]
fn retained_graph_residency_uses_the_session_ceiling() {
    const STATEMENT_SCRATCH_BYTES: u64 = 2 * 1024 * 1024;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("session-owned-graph.db");
    fixture::seed_working_set(&path);

    let mut options = fixture::options();
    options.limits.memory = STATEMENT_SCRATCH_BYTES;
    let reader = fixture::open_file(&path, options, Arc::new(fixture::Routes::default()));
    let before = reader.file_vector_passive_activity_for_test().unwrap();

    let rows = indexed(&reader, 0);

    assert_eq!(rows.len(), 10);
    let after = reader.file_vector_passive_activity_for_test().unwrap();
    assert!(
        after.sealed_generation_loads > before.sealed_generation_loads,
        "the query must exercise a cold retained-graph load"
    );
    let (usage, held, peak, _) = reader.file_memory_for_test().unwrap();
    assert_eq!(usage.limit, Some(fixture::SESSION_BYTES));
    assert!(held <= usage.used && usage.used <= peak);
    assert!(peak <= fixture::SESSION_BYTES);
}

fn complete_context(reader: &ReadSession, context: usize) {
    let rows = reader.execute(
        "SELECT id, payload, embedding FROM reader_work WHERE context_id = $context ORDER BY id",
        &params(context)).expect("one indexed Context's complete data fits");
    assert_eq!(rows.rows.len(), fixture::ROWS_PER_CONTEXT);
    assert!(
        rows.trace.index_used.is_some(),
        "real declared index serves the Context read"
    );
    for (offset, row) in rows.rows.iter().enumerate() {
        let id = context * fixture::ROWS_PER_CONTEXT + offset;
        assert_eq!(
            row,
            &vec![
                Value::Uuid(fixture::row_id(id)),
                Value::Text(fixture::payload(id)),
                Value::Vector(fixture::vector(id))
            ]
        );
    }
    drop(rows);
}

fn backend_reads<T>(operation: impl FnOnce() -> T) -> (T, u64) {
    let bytes = Arc::new(AtomicU64::new(0));
    let observed = bytes.clone();
    let observation = redb::observe_backend_reads(Arc::new(move |read| {
        observed.fetch_add(read as u64, Ordering::SeqCst);
    }));
    let answer = operation();
    drop(observation);
    (answer, bytes.load(Ordering::SeqCst))
}

#[derive(Debug, Default, Clone, Copy)]
struct GraphPageProfile {
    range_reads: u64,
    returned_bytes: u64,
    physical_page_lookups: u64,
    authenticated_body_bytes: u64,
    unaligned_full_page_reads: u64,
}

fn profiled_backend_reads<T>(operation: impl FnOnce() -> T) -> (T, u64, GraphPageProfile) {
    let profile = Arc::new(Mutex::new(GraphPageProfile::default()));
    let observed = Arc::clone(&profile);
    let (answer, backend_bytes) = observe_retained_graph_page_reads_for_test(
        Arc::new(move |event: RetainedGraphPageReadObservation| {
            let mut profile = observed.lock().unwrap();
            profile.range_reads = profile.range_reads.saturating_add(1);
            profile.returned_bytes = profile
                .returned_bytes
                .saturating_add(event.returned_bytes as u64);
            profile.physical_page_lookups = profile
                .physical_page_lookups
                .saturating_add(event.physical_page_lookups as u64);
            profile.authenticated_body_bytes = profile
                .authenticated_body_bytes
                .saturating_add(event.authenticated_body_bytes as u64);
            if event.returned_bytes == event.source_page_bytes
                && !event.offset.is_multiple_of(event.source_page_bytes)
            {
                profile.unaligned_full_page_reads =
                    profile.unaligned_full_page_reads.saturating_add(1);
            }
        }),
        || backend_reads(operation),
    );
    let profile = *profile.lock().unwrap();
    (answer, backend_bytes, profile)
}

#[test]
fn read_only_open_loads_only_the_working_set() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("working-set.db");
    let seed_started = Instant::now();
    fixture::seed_working_set(&path);
    let seed_elapsed = seed_started.elapsed();
    let routes = Arc::new(fixture::Routes::default());
    let open_started = Instant::now();
    let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Open);
    let reader = fixture::open_file(&path, fixture::options(), routes.clone());
    let open_elapsed = open_started.elapsed();
    let open = scope.counters();
    drop(scope);
    let snapshot = reader.file_snapshot_for_test().unwrap();
    let peer = fixture::open_file(
        &path,
        fixture::options(),
        Arc::new(fixture::Routes::default()),
    );
    assert_eq!(snapshot, peer.file_snapshot_for_test().unwrap());
    println!("open decode/hash diagnostics: {open:?}");
    charged(&reader);
    let cold_started = Instant::now();
    let first_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let (first, first_backend_bytes, first_page_profile) =
        profiled_backend_reads(|| indexed(&reader, 0));
    let cold_elapsed = cold_started.elapsed();
    let first_counts = first_scope.counters();
    drop(first_scope);
    let activity_after_cold = reader.file_vector_passive_activity_for_test().unwrap();
    assert!(
        first_backend_bytes > 0,
        "cold indexed query faults backend pages"
    );
    assert!(activity_after_cold.sealed_generation_loads > 0);
    let warm_started = Instant::now();
    let warm_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let (warm_answer, warm_backend_bytes, warm_page_profile) =
        profiled_backend_reads(|| indexed(&reader, 0));
    assert_eq!(warm_answer, first);
    let warm_elapsed = warm_started.elapsed();
    let warm = warm_scope.counters();
    drop(warm_scope);
    assert_eq!(
        reader
            .file_vector_passive_activity_for_test()
            .unwrap()
            .sealed_generation_loads,
        activity_after_cold.sealed_generation_loads,
        "negative control: an immediate repeat must not reload a resident generation"
    );
    // `RetainedGraphPageReadObservation` fires only on a backend page-range
    // read -- a retained-cache MISS that actually faults bytes -- never on a
    // hit served straight from the resident graph page. The cold query is
    // the first touch of this generation, so it must fault: every bound the
    // old single assertion applied to "the" profile still applies here,
    // unweakened, to the cold one.
    let profile = first_page_profile;
    assert!(
        profile.range_reads > 0,
        "the cold indexed query must fault at least one graph page range read: {profile:?}"
    );
    assert_eq!(
        profile.unaligned_full_page_reads, 0,
        "the graph cache must not turn a point miss into two full physical-page loads"
    );
    assert!(
        profile.physical_page_lookups
            <= profile
                .range_reads
                .saturating_add(profile.range_reads / 8)
                .saturating_add(1),
        "only boundary-straddling point values may require two authenticated pages: {profile:?}"
    );
    // The immediate repeat is the retention property this test also pins:
    // every page the cold query faulted is still resident, so the warm
    // repeat must fault NONE of them -- zero backend page-range reads, not
    // merely fewer. `warm_answer == first` (asserted above) already pins
    // that the zero-fault repeat still answers correctly; this pins that it
    // answers correctly BECAUSE nothing needed to be reloaded, not by
    // coincidence.
    assert_eq!(
        warm_page_profile.range_reads, 0,
        "an immediate repeat must be served entirely from the pages the cold query just retained \
         -- any backend page-range read here is a cache miss the retention property forbids: \
         {warm_page_profile:?}"
    );
    println!(
        "query diagnostics: cold={first_counts:?}/{first_backend_bytes} backend bytes/{cold_elapsed:?}/{first_page_profile:?}; warm={warm:?}/{warm_backend_bytes} backend bytes/{warm_elapsed:?}/{warm_page_profile:?}"
    );
    assert_eq!(indexed(&peer, 0), first, "independent readers agree");

    let all = reader.execute(
        "SELECT id FROM reader_work WHERE context_id IN ($c0,$c1,$c2,$c3,$c4,$c5,$c6,$c7) ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 10",
        &fixture::all_context_params()).expect("one indexed query spans all eight Contexts inside the same ceiling");
    assert_eq!(all.rows.len(), 10);
    assert_eq!(
        all.trace.vector_search.as_ref().unwrap().resolved_mode,
        VectorSearchMode::Indexed
    );
    let exact_all = peer.execute(
        "SELECT id FROM reader_work WHERE context_id IN ($c0,$c1,$c2,$c3,$c4,$c5,$c6,$c7) ORDER BY embedding <=> $q USE VECTOR EXACT LIMIT 10",
        &fixture::all_context_params()).unwrap();
    assert_eq!(all.rows, exact_all.rows);
    charged(&reader);
    drop(all);
    drop(exact_all);

    // Read every payload exactly and release each result. Their combined logical
    // size exceeds the fixed ceiling, forcing idle page eviction before reuse.
    // No explicit eviction helper can manufacture a cache success here.
    let activity_before_pressure = reader.file_vector_passive_activity_for_test().unwrap();
    let pressure_started = Instant::now();
    for context in 0..fixture::CONTEXTS {
        indexed(&reader, context);
        complete_context(&reader, context);
        complete_context(&reader, context);
        charged(&reader);
        assert_eq!(reader.file_snapshot_for_test(), Some(snapshot));
    }
    let pressure_elapsed = pressure_started.elapsed();
    assert!(fixture::ROWS * (fixture::TEXT + fixture::DIM * 4) > fixture::SESSION_BYTES);
    let activity_after_pressure = reader.file_vector_passive_activity_for_test().unwrap();
    assert!(
        activity_after_pressure.sealed_generation_evictions
            > activity_before_pressure.sealed_generation_evictions,
        "the unchanged ceiling must naturally evict at least one reloadable saved graph"
    );

    let writer = Database::open(&path).unwrap();
    writer
        .execute(
            "CREATE TABLE recycle_probe (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
            &HashMap::new(),
        )
        .unwrap();
    writer
        .execute(
            "INSERT INTO recycle_probe VALUES (1, 'unrelated-write')",
            &HashMap::new(),
        )
        .unwrap();
    let compacted = writer.compact_now().unwrap();
    assert!(compacted.ran && compacted.handle_recycled);
    writer.close().unwrap();

    let reload_started = Instant::now();
    let reload_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let mut reloaded_contexts = 0;
    let mut reload_backend_bytes = 0_u64;
    for context in 0..fixture::CONTEXTS {
        let before = reader.file_vector_passive_activity_for_test().unwrap();
        let (answer, bytes) = backend_reads(|| indexed(&reader, context));
        let after = reader.file_vector_passive_activity_for_test().unwrap();
        if after.sealed_generation_loads > before.sealed_generation_loads {
            reloaded_contexts += 1;
            reload_backend_bytes = reload_backend_bytes.saturating_add(bytes);
            let repeat_before = after.sealed_generation_loads;
            assert_eq!(indexed(&reader, context), answer);
            assert_eq!(
                reader
                    .file_vector_passive_activity_for_test()
                    .unwrap()
                    .sealed_generation_loads,
                repeat_before,
                "negative control: without a second eviction, a warm repeat cannot count as reload"
            );
        }
        assert_eq!(reader.file_snapshot_for_test(), Some(snapshot));
        charged(&reader);
    }
    let reload_elapsed = reload_started.elapsed();
    let reload = reload_scope.counters();
    drop(reload_scope);
    assert!(reloaded_contexts > 0, "an evicted saved graph must reload");
    assert!(
        reload_backend_bytes > 0,
        "reload must fault saved-graph bytes through the retained backend"
    );
    assert_eq!(indexed(&reader, 0), first);
    assert_eq!(reader.file_snapshot_for_test(), Some(snapshot));
    println!(
        "working-set phases: seed={seed_elapsed:?}, open={open_elapsed:?}, pressure={pressure_elapsed:?}, reload={reload_elapsed:?}; lifecycle before={activity_before_pressure:?}, after_pressure={activity_after_pressure:?}; reload={reload:?}, reloaded_contexts={reloaded_contexts}, reload_backend_bytes={reload_backend_bytes}"
    );

    let newer = fixture::open_file(
        &path,
        fixture::options(),
        Arc::new(fixture::Routes::default()),
    );
    assert_ne!(newer.file_snapshot_for_test(), Some(snapshot));
    assert_eq!(
        newer
            .execute(
                "SELECT payload FROM recycle_probe WHERE id = 1",
                &HashMap::new()
            )
            .unwrap()
            .rows,
        vec![vec![Value::Text("unrelated-write".into())]]
    );
    drop(newer);

    routes.file_only();
    let witness = reader.file_memory_for_test().unwrap().3;
    drop(reader);
    assert!(
        !witness.is_alive() && witness.is_physically_freed() && witness.accountant_is_physically_freed(),
        "closing releases the owned cache accountant"
    );
    drop(peer);

    let mut tiny = fixture::options();
    tiny.memory_limit = Some(1);
    assert!(
        matches!(
            ReadSession::open_with_options(&path, tiny),
            Err(contextdb_core::Error::MemoryBudgetExceeded { .. })
        ),
        "negative control: the same real opener must enforce the declaration"
    );
}
