//! Repeated indexed finds over the same saved partitions keep the pages they
//! need in the reader's charged serving memory.

use crate::a_saved_graph_load_checks_its_bytes_once::{
    ROOMY, Shape, fresh_reader_within, ids, seed_partition_rows, seed_shape, since, statement,
};
use crate::a_wide_find_admits_every_partition_when_serving_pages_give_way::{
    SHAPE as SHORT_SHAPE, calibrate, limit_for,
};
use contextdb_core::Error;
use contextdb_core::find_attribution::load_meters;
use contextdb_vector::hnsw::{
    HnswCandidateDistanceEvent, HnswCandidateObserver, PagedVectorObserver,
    with_hnsw_candidate_observer, with_paged_vector_observer,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const SHAPE: Shape = Shape {
    partitions: 5,
    rows_per_partition: 3_000,
    dimensions: 256,
};
const ROWS: [usize; SHAPE.partitions] = [3_000, 400, 400, 400, 400];

const SOURCE_PAGE_BYTES: usize = 64 * 1024;
// The saved spans of the large partition and its four smaller companions fit
// together, while the large partition needs more than an equal fifth share.
const WARM_SLACK_BYTES: usize = 200 * SOURCE_PAGE_BYTES;

#[derive(Default)]
struct Work {
    fills: AtomicU64,
    distances: AtomicU64,
}

impl PagedVectorObserver for Work {
    fn page_filled(&self) {
        self.fills.fetch_add(1, Ordering::Relaxed);
    }
}

impl HnswCandidateObserver for Work {
    fn before_candidate_distance(&self, _: HnswCandidateDistanceEvent) {
        self.distances.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(unix)]
#[test]
#[ignore = "encodes the all-resident serving-cache pricing model, which contradicts the intent's on-demand paging; superseded by the paged-pricing replacement rows"]
fn repeated_wide_finds_stop_filling_pages_within_the_declared_memory() {
    let fixture = seed_partition_rows(SHAPE, &ROWS);
    let working = calibrate(&fixture);
    let limit = limit_for(&fixture, &working, WARM_SLACK_BYTES as isize);
    let scopes: [&[usize]; 3] = [&[0], &[0, 1], &[0, 1, 2, 3, 4]];

    let spacious = fresh_reader_within(&fixture, ROOMY);
    let (wide_sql, wide_params) = statement(SHAPE, scopes[2], "USE VECTOR INDEXED");
    let roomy_first = Arc::new(Work::default());
    let before_loads = load_meters();
    let roomy_answer = with_paged_vector_observer(roomy_first.clone(), || {
        spacious
            .execute(&wide_sql, &wide_params)
            .expect("spacious reader answers the wide find")
    });
    let saved = since(before_loads, load_meters());
    assert_eq!(roomy_answer.rows.len(), 10);
    assert_eq!(saved.loads, SHAPE.partitions as u64);
    assert!(
        saved.saved_bytes <= WARM_SLACK_BYTES as u64,
        "the finite slack must cover every partition's complete saved graph span: saved={} slack={WARM_SLACK_BYTES}",
        saved.saved_bytes
    );
    assert!(roomy_first.fills.load(Ordering::Relaxed) > 0);
    let roomy_second = Arc::new(Work::default());
    with_paged_vector_observer(roomy_second.clone(), || {
        spacious
            .execute(&wide_sql, &wide_params)
            .expect("spacious reader retains its visited pages")
    });
    assert_eq!(roomy_second.fills.load(Ordering::Relaxed), 0);
    println!(
        "roomy wide find distinct pages={} saved bytes={} finite slack pages={}",
        roomy_first.fills.load(Ordering::Relaxed),
        saved.saved_bytes,
        WARM_SLACK_BYTES / SOURCE_PAGE_BYTES
    );
    assert!(
        roomy_first.fills.load(Ordering::Relaxed) < (WARM_SLACK_BYTES / SOURCE_PAGE_BYTES) as u64,
        "the finite declaration must fit the pages the wide walk visited"
    );

    for scope in scopes {
        let reader = fresh_reader_within(&fixture, limit);
        let (sql, params) = statement(SHAPE, scope, "USE VECTOR INDEXED");
        let mut previous = None;
        let mut first_work = None;
        let mut fills = Vec::new();
        for repetition in 0..4 {
            let before = load_meters();
            let work = Arc::new(Work::default());
            let answer = with_paged_vector_observer(work.clone(), || {
                with_hnsw_candidate_observer(work.clone(), || {
                    reader
                        .execute(&sql, &params)
                        .expect("the indexed find answers")
                })
            });
            let distances = work.distances.load(Ordering::Relaxed);
            let page_fills = work.fills.load(Ordering::Relaxed);
            assert!(distances > 0, "the query must walk a saved graph");
            assert_eq!(answer.rows.len(), 10, "the query answers a full page");
            if let Some(ids_before) = &previous {
                assert_eq!(ids(&answer), *ids_before, "warm answers stay identical");
                assert_eq!(
                    distances,
                    first_work.unwrap(),
                    "warm graph work stays fixed"
                );
                assert_eq!(
                    load_meters().loads,
                    before.loads,
                    "a warm find reloads no graph"
                );
            } else {
                previous = Some(ids(&answer));
                first_work = Some(distances);
            }
            assert!(
                reader.charged_bytes().expect("reader reports its charge") <= limit as u64,
                "retained pages must stay in the declared memory"
            );
            fills.push(page_fills);
            println!(
                "indexed scope={} repetition={repetition} fills={page_fills} distances={distances} charge={}",
                scope.len(),
                reader.charged_bytes().expect("reader reports its charge")
            );
        }
        assert!(fills[0] > 0, "the first find must fault serving pages");
        if scope.len() == SHAPE.partitions {
            assert_eq!(
                &fills[1..],
                &[0, 0, 0],
                "every find after the first must reuse the entry pages over every partition"
            );
        }
    }

    assert!(spacious.charged_bytes().unwrap() <= ROOMY as u64);
}

/// A finite declaration can hold the saved topology and a smaller serving
/// cache even when it cannot hold the directory for every page of the larger
/// cache selected from its remaining bytes.
#[cfg(unix)]
#[test]
#[ignore = "encodes the all-resident serving-cache pricing model, which contradicts the intent's on-demand paging; superseded by the paged-pricing replacement rows"]
fn a_graph_load_chooses_only_cache_slots_whose_directory_fits() {
    let shape = Shape {
        partitions: 1,
        rows_per_partition: 600,
        dimensions: 256,
    };
    let fixture = seed_shape(shape);
    let (sql, params) = statement(shape, &[0], "USE VECTOR INDEXED");
    let roomy = fresh_reader_within(&fixture, ROOMY);
    let before = load_meters();
    let oracle = roomy
        .execute(&sql, &params)
        .expect("the saved graph answers");
    let load = since(before, load_meters());
    assert_eq!(load.loads, shape.partitions as u64);
    assert!(
        load.cache_bytes > 3 * SOURCE_PAGE_BYTES as u64,
        "the graph must have more than three pages to make a smaller cache meaningful"
    );
    // The declaration covers the least load and several additional page
    // bodies. Extra directory slots must fit in those same bytes.
    // This query holds just over 10 KiB of candidates before the graph load.
    // The small residual beyond four page bodies cannot hold the fourth
    // slot's directory allocation.
    let beside = load.minimum_bytes as usize + 4 * SOURCE_PAGE_BYTES + 10_300;
    let mut limit = ROOMY;
    for _ in 0..12 {
        let reader = fresh_reader_within(&fixture, limit);
        let opened = reader
            .charged_bytes()
            .expect("reader reports its opening charge") as usize;
        let next = opened + beside;
        if next == limit {
            break;
        }
        limit = next;
    }
    let reader = fresh_reader_within(&fixture, limit);
    let opened = reader
        .charged_bytes()
        .expect("reader reports its opening charge") as usize;
    assert_eq!(limit - opened, beside, "the finite headroom is calibrated");
    let before = load_meters();
    let answer = reader
        .execute(&sql, &params)
        .expect("the topology and a smaller serving cache fit");
    let load = since(before, load_meters());
    assert_eq!(
        load.loads, shape.partitions as u64,
        "the indexed answer loads every saved graph"
    );
    println!(
        "finite graph load: opened={opened} limit={limit} minimum={} admitted={} cache={} saved={}",
        load.minimum_bytes, load.admitted_bytes, load.cache_bytes, load.saved_bytes
    );
    assert_eq!(ids(&answer), ids(&oracle));
    assert!(
        reader.charged_bytes().expect("reader reports its charge") <= limit as u64,
        "the loaded graph stays inside the declared memory"
    );
}

#[cfg(unix)]
#[test]
#[ignore = "encodes the all-resident serving-cache pricing model, which contradicts the intent's on-demand paging; superseded by the paged-pricing replacement rows"]
fn a_short_declaration_keeps_its_charge_and_refuses_missing_topologies() {
    let fixture = seed_shape(SHORT_SHAPE);
    let working = calibrate(&fixture);
    let short = (working.least_loads / 2 + working.workspace) as isize;
    let limit = limit_for(&fixture, &working, -short);
    let reader = fresh_reader_within(&fixture, limit);
    let (sql, params) = statement(SHORT_SHAPE, &[0, 1, 2, 3, 4], "USE VECTOR INDEXED");
    let refused = reader.execute(&sql, &params);
    assert!(
        matches!(
            &refused,
            Err(Error::MemoryBudgetExceeded { operation, .. })
                if operation == "load_retained_vector_generation"
        ),
        "a short declaration refuses a graph whose topologies cannot fit: {refused:?}"
    );
    assert!(
        reader.charged_bytes().expect("reader reports its charge") <= limit as u64,
        "a refused load must leave the reader inside its declaration"
    );
}
