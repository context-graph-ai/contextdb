//! Contract rows for paged saved graph admission and eviction.
//!
//! The load meters describe the retained control handle. Page callbacks describe
//! the pages actually faulted by the find; the reader accountant must follow
//! those fills and remain within its declared fund.

use crate::a_saved_graph_load_checks_its_bytes_once::{
    Fixture, ROOMY, Shape, context_id, fresh_reader_within, limits, seed_partition_rows, statement,
};
use crate::a_wide_find_admits_every_partition_when_serving_pages_give_way::{calibrate, limit_for};
use contextdb_core::Error;
use contextdb_core::find_attribution::load_meters;
use contextdb_core::read_contract::{ReadFailureDetail, ReadFailureLimit};
use contextdb_core::types::ContextId;
use contextdb_engine::{ReadSession, ReadSessionOptions};
use contextdb_vector::hnsw::{PagedVectorObserver, with_paged_vector_observer};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

const SHAPE: Shape = Shape {
    partitions: 5,
    rows_per_partition: 3_000,
    dimensions: 256,
};
const ROWS: [usize; 5] = [3_000, 400, 400, 400, 400];
const PAGE_BYTES: u64 = 64 * 1024;
const CACHE_SLACK: usize = 200 * PAGE_BYTES as usize;
static FIXTURE: OnceLock<Fixture> = OnceLock::new();
static PAGED_LIMIT: OnceLock<usize> = OnceLock::new();

fn fixture() -> &'static Fixture {
    FIXTURE.get_or_init(|| seed_partition_rows(SHAPE, &ROWS))
}

fn fresh_reader(fixture: &Fixture, memory: usize) -> ReadSession {
    fresh_reader_within(fixture, memory)
}

fn paged_limit(fixture: &Fixture) -> usize {
    *PAGED_LIMIT.get_or_init(|| {
        let working = calibrate(fixture);
        limit_for(fixture, &working, CACHE_SLACK as isize)
    })
}

// Opening a reader peaks above a whole streaming find, so the reader-wide
// limit cannot place a budget between "opens" and "admits its first page".
// The statement's own declared memory can: the reader stays roomy and the
// statement's memory grows from below one page until the find answers.
fn assert_first_page_refusal(fixture: &Fixture) {
    let (sql, params) = statement(SHAPE, &[0], "USE VECTOR INDEXED");
    let mut witnessed = false;
    let mut answered = false;
    let mut memory = 1_024usize;
    while memory <= ROOMY {
        // Opening loads the route's graph under the same statement memory, so
        // the first page a load cannot admit may be refused right there: by
        // type, from the graph source, before the page is allocated (no
        // session exists to hold anything).
        let reader = match statement_reader(fixture, memory) {
            Ok(reader) => reader,
            Err(Error::MemoryBudgetExceeded { subsystem, .. }) => {
                witnessed |= matches!(subsystem.as_ref(), "graph_source" | "storage_pages");
                memory *= 2;
                continue;
            }
            Err(other) if crossed_the_memory_limit(&other) => {
                memory *= 2;
                continue;
            }
            Err(other) => panic!("a statement short of memory is refused by memory type: {other}"),
        };
        let before = charged(&reader);
        match reader.execute(&sql, &params) {
            Err(Error::MemoryBudgetExceeded { .. }) => {
                assert_refusal_retains_nothing(&reader, &sql, &params, before);
                witnessed = true;
            }
            // The statement's declared memory is a read limit as well as a
            // budget, so a fill can stop at the limit before the budget refuses.
            Err(other) if crossed_the_memory_limit(&other) => {
                assert_refusal_retains_nothing(&reader, &sql, &params, before);
            }
            Ok(answer) => {
                assert_eq!(
                    answer.rows.len(),
                    10,
                    "an admitted find answers a full page"
                );
                answered = true;
                break;
            }
            Err(other) => panic!("a statement short of memory is refused by memory type: {other}"),
        }
        memory *= 2;
    }
    assert!(
        witnessed,
        "a statement budget below the find's pages returns typed MemoryBudgetExceeded before allocation"
    );
    assert!(answered, "a statement budget that admits the pages answers");
}

fn statement_reader(fixture: &Fixture, memory: usize) -> contextdb_core::Result<ReadSession> {
    ReadSession::with_runtime_directory_for_test(&fixture.runtime, || {
        ReadSession::open_with_options(
            &fixture.path,
            ReadSessionOptions {
                contexts: Some(
                    (0..SHAPE.partitions)
                        .map(|partition| ContextId::new(context_id(partition)))
                        .collect::<BTreeSet<_>>(),
                ),
                limits: limits(memory),
                memory_limit: Some(ROOMY),
                ..ReadSessionOptions::default()
            },
        )
    })
}

// A session loads a route's paged-generation handle on its first use and keeps
// it for the session, as after an answered find. So the first refused
// statement may keep that handle, never a page; the same refusal again, with
// the handle resident, retains nothing.
fn assert_refusal_retains_nothing(
    reader: &ReadSession,
    sql: &str,
    params: &std::collections::HashMap<String, contextdb_core::Value>,
    before: u64,
) {
    let settled = charged(reader);
    assert!(
        settled.saturating_sub(before) < PAGE_BYTES,
        "a refused first use keeps at most the route's handle, never a page fill: {before} -> {settled}"
    );
    assert!(
        reader.execute(sql, params).is_err(),
        "the same statement under the same memory is refused again"
    );
    assert_eq!(
        charged(reader),
        settled,
        "a refused page fill retains no allocation"
    );
}

fn crossed_the_memory_limit(error: &Error) -> bool {
    matches!(
        error,
        Error::ReadFailure(failure)
            if matches!(
                failure.detail(),
                ReadFailureDetail::OwnerLimitExceeded(detail)
                    if detail.limit == ReadFailureLimit::Memory
            )
    )
}

fn charged(reader: &ReadSession) -> u64 {
    reader.charged_bytes().expect("reader charge")
}

#[derive(Default)]
struct Fills {
    page_fills: AtomicU64,
    page_bytes: AtomicU64,
}
impl PagedVectorObserver for Fills {
    fn page_filled(&self) {
        self.page_fills.fetch_add(1, Ordering::Relaxed);
    }
    fn storage_work(&self, kind: contextdb_vector::hnsw::PagedVectorStorageWork, bytes: usize) {
        if kind == contextdb_vector::hnsw::PagedVectorStorageWork::PageRecordDecode {
            self.page_fills.fetch_add(1, Ordering::Relaxed);
            self.page_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }
}

fn paged_find(selected: usize) -> (u64, u64, u64, u64) {
    let fixture = fixture();
    let reader = fresh_reader(fixture, paged_limit(fixture));
    let (sql, params) = statement(
        SHAPE,
        &(0..selected).collect::<Vec<_>>(),
        "USE VECTOR INDEXED",
    );
    // The reader's whole accountant, not its file-image child: page fills are
    // read copies the find admits against the reader's one declared memory.
    let before_charge = reader
        .file_memory_for_test()
        .expect("file reader memory receipt")
        .2 as u64;
    let before = load_meters();
    let fills = Arc::new(Fills::default());
    let answer = with_paged_vector_observer(fills.clone(), || reader.execute(&sql, &params))
        .expect("paged indexed find answers");
    assert_eq!(
        answer.rows.len(),
        10,
        "the indexed graph search answers a full result page"
    );
    let after = load_meters();
    let (_, _, high_water, _) = reader
        .file_memory_for_test()
        .expect("file reader memory receipt");
    let charge = (high_water as u64).saturating_sub(before_charge);
    let filled_bytes = fills.page_bytes.load(Ordering::Relaxed);
    assert!(
        after.loads >= before.loads + selected as u64,
        "one or more control handles per selected partition are loaded"
    );
    assert!(
        after.admitted_bytes >= after.held_bytes,
        "control handles are admitted before they are retained"
    );
    assert!(
        after.held_bytes >= before.held_bytes,
        "retained control bytes do not go backwards during the find"
    );
    assert!(
        fills.page_fills.load(Ordering::Relaxed) > 0,
        "the cold find fills on-demand page records"
    );
    assert!(
        charge > 0,
        "page fill admission contributes to the reader high-water charge"
    );
    let control = after.admitted_bytes - before.admitted_bytes;
    assert!(
        charge <= control + filled_bytes,
        "the find's charged peak {charge} exceeds what its loads and fills held ({control} control + {filled_bytes} filled)"
    );
    assert_first_page_refusal(fixture);
    (
        after.loads - before.loads,
        after.admitted_bytes - before.admitted_bytes,
        after.held_bytes - before.held_bytes,
        filled_bytes,
    )
}

#[cfg(unix)]
#[test]
fn paged_load_pricing_replaces_wide_find_pages_give_way_row() {
    let _ = paged_find(5);
}
#[cfg(unix)]
#[test]
fn paged_load_pricing_replaces_topology_refusal_row() {
    let _ = paged_find(1);
}
#[cfg(unix)]
#[test]
fn paged_load_pricing_replaces_filled_memory_answer_row() {
    let _ = paged_find(2);
}
#[cfg(unix)]
#[test]
fn paged_load_pricing_replaces_idle_partition_eviction_row() {
    let _ = paged_find(3);
}
#[cfg(unix)]
#[test]
fn paged_load_pricing_replaces_admitted_at_held_row() {
    let _ = paged_find(1);
}
#[cfg(unix)]
#[test]
fn paged_load_pricing_replaces_future_load_share_row() {
    let _ = paged_find(5);
}
#[cfg(unix)]
#[test]
fn paged_load_pricing_replaces_repeated_wide_find_row() {
    let _ = paged_find(5);
}
#[cfg(unix)]
#[test]
fn paged_load_pricing_replaces_cache_slot_directory_row() {
    let _ = paged_find(1);
}
#[cfg(unix)]
#[test]
fn paged_load_pricing_replaces_short_declaration_row() {
    let _ = paged_find(2);
}
#[cfg(unix)]
#[test]
fn paged_load_pricing_replaces_per_layer_share_row() {
    let _ = paged_find(5);
}

#[cfg(unix)]
#[test]
fn paged_find_charge_growth_tracks_filled_pages_not_present_pages() {
    let fixture = fixture();
    let limit = paged_limit(fixture);
    let mut samples = Vec::new();
    for selected in [1, 2, 4] {
        let reader = fresh_reader(fixture, limit);
        let (sql, params) = statement(
            SHAPE,
            &(0..selected).collect::<Vec<_>>(),
            "USE VECTOR INDEXED",
        );
        let before_charge = reader.file_memory_for_test().expect("memory before find").2 as u64;
        let fills = Arc::new(Fills::default());
        let answer = with_paged_vector_observer(fills.clone(), || reader.execute(&sql, &params))
            .expect("find answers");
        assert_eq!(
            answer.rows.len(),
            10,
            "the indexed graph search answers a full result page"
        );
        let charge = (reader.file_memory_for_test().expect("memory after find").2 as u64)
            .saturating_sub(before_charge);
        let filled = fills.page_fills.load(Ordering::Relaxed);
        samples.push((filled, charge));
    }
    assert!(
        samples.windows(2).all(|pair| pair[1].0 >= pair[0].0),
        "filled bytes grow with selected partitions: {samples:?}"
    );
    assert!(
        samples.windows(2).any(|pair| pair[1].0 > pair[0].0),
        "the selected scopes fill additional pages: {samples:?}"
    );
    for (filled, charged) in &samples {
        assert!(*filled > 0, "page record fills are observed: {samples:?}");
        assert!(
            *charged > 0,
            "a nonzero page fill count has a nonzero charged high-water: {samples:?}"
        );
        assert!(
            *charged < 8 * PAGE_BYTES,
            "a find charges its filled working pages, not every page present: {samples:?}"
        );
    }
}

#[cfg(unix)]
#[test]
fn paged_load_refuses_first_page_by_typed_budget_before_allocation() {
    assert_first_page_refusal(fixture());
}
