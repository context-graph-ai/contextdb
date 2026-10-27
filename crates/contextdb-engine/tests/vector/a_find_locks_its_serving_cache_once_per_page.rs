//! A find locks its serving cache for the pages it reads, not for the vectors.
//!
//! The serving cache is shared, so every look into it takes a mutex. A page
//! holds about twenty-one 768-dimension vectors, so the number of times one
//! find needs that mutex is the number of PAGES it touches -- not the number
//! of vectors it scores. The lock used to be taken inside the per-point
//! decode, so a find paid it once per visited point: 1,221 acquisitions over
//! this 1,250-vector partition, every one of them a cache hit.
//!
//! Each search now keeps its own directory of the pages it has resolved, so
//! the same find takes the lock 85 times. The directory keeps no page
//! buffer alive -- a dead entry keeps only the page's small shared handle,
//! which its charge covers -- so a cache too small for its partition still
//! evicts and the search still goes back for what it lost. That half of the
//! contract is `a_search_locks_its_serving_cache_once_per_page_it_resolves`
//! in contextdb-vector, and the allocation count under a second search is
//! `concurrent_searches_stay_inside_their_serving_cache` there.
//!
//! Nothing about the answer changes: this is a locking-frequency repair, and
//! both arms assert the published rows are the ones the route published
//! before.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadRoute;
use contextdb_core::types::ContextId;
use contextdb_engine::{ReadSession, ReadSessionOptions};
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

const DIMENSIONS: usize = 768;
const LIMIT: usize = 10;
/// A 64 KiB serving page holds about twenty-one 768-dimension vectors, so a
/// 1,250-vector partition spans about sixty pages. Locking per page is bounded
/// by that; locking per visited point is bounded by the partition.
const PAGE_GRANULAR_LOCKS: u64 = 128;

fn rows() -> usize {
    std::env::var("CONTEXTDB_P1_EXACT_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1_250)
}

fn context_id() -> Uuid {
    Uuid::from_u128(0xC04C_0000_0000_0000_0000_0000_0000_0000)
}

fn generation_id() -> Uuid {
    Uuid::from_u128(0xC04C_1000_0000_0000_0000_0000_0000_0000)
}

fn embedding(ordinal: usize) -> Vec<f32> {
    let mut state = (ordinal as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15).max(1);
    (0..DIMENSIONS)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state >> 40) as f32 / 16_777_216.0) - 0.5
        })
        .collect()
}

#[derive(Default)]
struct CacheCounts {
    locks: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    filled: AtomicU64,
    decoded: AtomicU64,
}

impl contextdb_vector::hnsw::PagedVectorObserver for CacheCounts {
    fn page_cache_locked(&self) {
        self.locks.fetch_add(1, Ordering::Relaxed);
    }
    fn page_filled(&self) {
        self.filled.fetch_add(1, Ordering::Relaxed);
    }
    fn page_looked_up(&self, hit: bool) {
        if hit {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn vector_decoded(&self) {
        self.decoded.fetch_add(1, Ordering::Relaxed);
    }
}

fn params() -> HashMap<String, Value> {
    HashMap::from([
        ("is_current".into(), Value::Bool(true)),
        ("query".into(), Value::Vector(embedding(0))),
        ("context".into(), Value::Uuid(context_id())),
        ("generation".into(), Value::Uuid(generation_id())),
    ])
}

fn find_sql(mode: &str) -> String {
    format!(
        "SELECT id FROM passages WHERE context_id IN ($context) AND \
         generation_id IN ($generation) AND is_current = $is_current \
         ORDER BY embedding <=> $query {mode} LIMIT {LIMIT}"
    )
}

fn answer_ids(answer: &contextdb_engine::QueryResult) -> Vec<i64> {
    answer
        .rows()
        .iter()
        .map(|row| match row.first() {
            Some(Value::Int64(id)) => *id,
            other => panic!("the projection returns one integer id, got {other:?}"),
        })
        .collect()
}

#[cfg(unix)]
/// OWED product proof: on the seeded 1,250-row, 768-dimension fixture, a real
/// `ReadSession` exact search takes zero serving-cache locks and decodes, while
/// indexed search takes page-granular cache locks; both preserve the rows
/// returned by the warmed route. Keep this route-level proof until its fixture
/// can run routinely; vector unit guards do not exercise this read path.
#[test]
#[ignore = "requires the seeded 1,250-row, 768-dimension store built by an_exact_search_authenticates_pages_not_every_vector; fixture creation takes minutes and hundreds of megabytes"]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn a_find_locks_its_serving_cache_once_per_page() {
    use std::os::unix::fs::PermissionsExt;

    let root =
        std::path::PathBuf::from(std::env::var("CONTEXTDB_P1_EXACT_DIR").unwrap_or_else(|_| {
            // Keep this fixture in the current build's scratch directory.
            format!("{}/exact-auth", env!("CARGO_TARGET_TMPDIR"))
        }))
        .join(format!("rows-{}", rows()));
    let path = root.join("passages.db");
    let runtime = root.join("runtime");
    assert!(
        path.exists(),
        "seed {} with an_exact_search_authenticates_pages_not_every_vector first",
        path.display()
    );
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");

    let reader = ReadSession::with_runtime_directory_for_test(&runtime, || {
        ReadSession::open_with_options(
            &path,
            ReadSessionOptions {
                contexts: Some(BTreeSet::from([ContextId::new(context_id())])),
                ..ReadSessionOptions::default()
            },
        )
    })
    .expect("open the committed file image the consumer reads through");
    assert_eq!(reader.route(), ReadRoute::File);

    for mode in ["USE VECTOR EXACT", "USE VECTOR INDEXED"] {
        let sql = find_sql(mode);
        // Warm first, so this counts a find's own locking rather than the
        // first-touch page faults the fixture pays once.
        let warm = reader
            .execute(&sql, &params())
            .expect("the route answers warm");
        let expected = answer_ids(&warm);
        assert_eq!(expected.len(), LIMIT, "{mode} answers a full page");

        let counts = Arc::new(CacheCounts::default());
        let answer = contextdb_vector::hnsw::with_paged_vector_observer(counts.clone(), || {
            reader.execute(&sql, &params()).expect("the route answers")
        });
        let locks = counts.locks.load(Ordering::Relaxed);
        println!(
            "cache_locks rows={} mode={mode} locks={locks} hits={} misses={} filled={} \
             decoded={}",
            rows(),
            counts.hits.load(Ordering::Relaxed),
            counts.misses.load(Ordering::Relaxed),
            counts.filled.load(Ordering::Relaxed),
            counts.decoded.load(Ordering::Relaxed),
        );
        assert_eq!(
            answer_ids(&answer),
            expected,
            "{mode} publishes the same rows however often it locks its serving cache"
        );
        if mode.contains("EXACT") {
            // Since the exhaustive route was returned to reading each row's own
            // stored body, it has no business in the serving cache at all. This
            // arm would otherwise pass at zero without asserting anything, so it
            // pins the zero instead.
            assert_eq!(
                locks, 0,
                "an exhaustive search reads each row's own body and must not touch the \
                 maintained generation's serving cache at all"
            );
            assert_eq!(
                counts.decoded.load(Ordering::Relaxed),
                0,
                "an exhaustive search decodes no vector out of serving pages"
            );
            continue;
        }
        assert!(
            locks <= PAGE_GRANULAR_LOCKS,
            "{mode} over {} vectors took the serving-cache lock {locks} times; locking once per \
             page it reads is at most {PAGE_GRANULAR_LOCKS} for this partition",
            rows(),
        );
    }
}
