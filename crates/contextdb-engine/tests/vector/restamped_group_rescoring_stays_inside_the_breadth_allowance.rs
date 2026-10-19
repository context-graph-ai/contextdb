//! Rows that share a graph point and were restamped by scalar updates since
//! the graph was built answer at the score of the vector their snapshot sees:
//! each is scored from its own stored body before it competes. Those scores,
//! and final candidate scoring's reads of stored bodies, are distance
//! evaluations like the graph's, paid from a rescoring allowance declared the
//! same size as the graph's, `max(EF_SEARCH, k) x (2M + 4)`: one search
//! computes at most twice that. A group larger than its allowance is either
//! answered within it, as EXACT answers, or refused typed for its evaluation
//! limit before any evaluation past it, never answered in part; a search given
//! a wide enough allowance answers it.
//!
//! Every distance is counted where it is computed, not taken from the walk's
//! own receipt, and every search reconciles: the graph distances it computed
//! are the walk's admission and expansion evaluations, and the stored vectors
//! it read are the rescoring reads it reports in its receipt and its executed
//! route disclosure -- for current rows, for restamped ones, and for both.

use contextdb_core::{Error, Value};
use contextdb_engine::{Database, MaintenancePolicy};
use contextdb_vector::hnsw::{
    HnswAllowedSearchIncomplete, HnswCandidateDistanceEvent, HnswCandidateObserver,
    UnionSearchReceipt, with_hnsw_candidate_observer, with_union_search_receipts,
};
use contextdb_vector::mem::{
    NativeVectorScoreObserver, with_native_vector_score_observer_for_test,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::TempDir;
use uuid::Uuid;

const DIMENSION: usize = 8;
const M: usize = 4;
const NARROW: usize = 16;
const WIDE: usize = 1_000;
const LIMIT: usize = 10;
/// More group rows than the narrow allowance can score one by one.
const GROUP: usize = 400;
/// Rows of distinct vectors beside the group.
const DISTINCT: usize = 60;
const REPRESENTATIONS: [&str; 3] = [
    "",
    " WITH (quantization = 'SQ8')",
    " WITH (quantization = 'SQ4')",
];

/// The most distances one search computes: its graph allowance and its
/// rescoring allowance, each `max(EF_SEARCH, k) x (2M + 4)`.
fn allowance(breadth: usize) -> u64 {
    2 * (breadth.max(LIMIT) * (2 * M + contextdb_vector::hnsw::LAYER0_REACHABILITY_LINKS)) as u64
}

/// Graph distances a search computes and stored-vector scores, counted
/// apart. A reopened store builds a graph over the rows written since its
/// saved one before it searches; building it is not the search's breadth.
struct Counter(AtomicU64, AtomicU64);
impl HnswCandidateObserver for Counter {
    fn before_candidate_distance(&self, event: HnswCandidateDistanceEvent) {
        if !event.building() {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl NativeVectorScoreObserver for Counter {
    fn before_native_vector_score(&self) {
        self.1.fetch_add(1, Ordering::SeqCst);
    }
}

fn shared() -> Vec<f32> {
    vec![1.0; DIMENSION]
}
fn query() -> Vec<f32> {
    let mut query = shared();
    query[0] = 1.3;
    query
}
fn distinct(seed: usize) -> Vec<f32> {
    (0..DIMENSION)
        .map(|axis| (((seed * 31 + axis * 17) % 97) as f32 / 97.0) - 0.5)
        .collect()
}

fn declare(db: &Database, representation: &str) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE grouped (id UUID PRIMARY KEY, tag TEXT, \
             embedding VECTOR({DIMENSION}){representation} SEARCH_MODE INDEXED AUTO_INDEX_AT 1 \
             HNSW (M = {M}, EF_SEARCH = {NARROW}))"
        ),
        &HashMap::new(),
    )
    .expect("declare the table");
}

fn put(db: &Database, seed: usize, vector: Vec<f32>) {
    db.execute(
        "INSERT INTO grouped (id, tag, embedding) VALUES ($id, 'old', $embedding)",
        &HashMap::from([
            ("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128))),
            ("embedding".to_owned(), Value::Vector(vector)),
        ]),
    )
    .expect("insert one row");
}

fn settle(db: &Database) {
    for _ in 0..64 {
        if db
            .run_maintenance_cycle()
            .expect("one maintenance cycle")
            .vector
            .remaining_partitions
            == 0
        {
            return;
        }
    }
    panic!("maintenance did not settle");
}

fn breadth(db: &Database, breadth: usize) {
    db.execute(
        &format!("ALTER TABLE grouped ALTER COLUMN embedding SET HNSW (EF_SEARCH = {breadth})"),
        &HashMap::new(),
    )
    .expect("declare the breadth");
}

/// A maintained graph of distinct, current vectors and, when `group`, the
/// shared group beside them.
fn fill(db: &Database, representation: &str, group: bool) {
    declare(db, representation);
    if group {
        for seed in 1..=GROUP {
            put(db, seed, shared());
        }
    }
    for seed in 10_000..10_000 + DISTINCT {
        put(db, seed, distinct(seed));
    }
    settle(db);
}

/// Restamp every group row by a scalar update, and replace one late in row
/// order by a vector nearer the query than the group, none maintained.
fn restamp_group(db: &Database) {
    db.execute(
        "UPDATE grouped SET tag = 'new' WHERE tag = 'old'",
        &HashMap::new(),
    )
    .expect("restamp every row");
    let mut nearer = query();
    nearer[1] = 1.05;
    db.execute(
        "UPDATE grouped SET embedding = $embedding WHERE id = $id",
        &HashMap::from([
            (
                "id".to_owned(),
                Value::Uuid(Uuid::from_u128(GROUP as u128 - 3)),
            ),
            ("embedding".to_owned(), Value::Vector(nearer)),
        ]),
    )
    .expect("replace a late group row");
}

/// Restamp every other distinct row by a scalar update, unmaintained.
fn restamp_half(db: &Database) {
    for seed in (10_000..10_000 + DISTINCT).step_by(2) {
        db.execute(
            "UPDATE grouped SET tag = 'new' WHERE id = $id",
            &HashMap::from([("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128)))]),
        )
        .expect("restamp one row");
    }
}

/// One search's answer, the distances it computed counted where they were
/// computed, and what it reports about them.
struct Searched {
    ids: Result<Vec<Value>, Error>,
    graph: u64,
    stored: u64,
    disclosed: Option<usize>,
    receipts: Vec<UnionSearchReceipt>,
}

fn search(db: &Database, mode: &str) -> Searched {
    let counter = Arc::new(Counter(AtomicU64::new(0), AtomicU64::new(0)));
    let (result, receipts) = with_union_search_receipts(|| {
        with_hnsw_candidate_observer(counter.clone(), || {
            with_native_vector_score_observer_for_test(counter.clone(), || {
                db.execute(
                    &format!(
                        "SELECT id FROM grouped ORDER BY embedding <=> $query \
                         USE VECTOR {mode} LIMIT {LIMIT}"
                    ),
                    &HashMap::from([("query".to_owned(), Value::Vector(query()))]),
                )
            })
        })
    });
    let disclosed = result.as_ref().ok().and_then(|answer| {
        answer
            .trace
            .vector_search
            .as_ref()
            .and_then(|search| search.rescore_evaluations)
    });
    Searched {
        ids: result.map(|answer| answer.rows.into_iter().map(|row| row[0].clone()).collect()),
        graph: counter.0.load(Ordering::SeqCst),
        stored: counter.1.load(Ordering::SeqCst),
        disclosed,
        receipts,
    }
}

/// The search's one union receipt and its counted distances agree, and an
/// answer discloses the same rescoring reads; a search that did not answer
/// was refused typed for its evaluation limit, with nothing returned.
fn reconcile(label: &str, searched: &Searched) {
    let [receipt] = searched.receipts.as_slice() else {
        panic!("{label}: one union search ran, not {:?}", searched.receipts);
    };
    assert_eq!(
        searched.graph,
        (receipt.admission_evaluations + receipt.expansion_evaluations) as u64,
        "{label}: the graph distances computed are the walk's evaluations ({receipt:?})"
    );
    assert_eq!(
        searched.stored, receipt.rescore_evaluations as u64,
        "{label}: the stored vectors read are the rescoring reads reported ({receipt:?})"
    );
    match &searched.ids {
        Ok(_) => {
            assert_eq!(receipt.unresolved, None, "{label}: an answered search");
            assert_eq!(
                searched.disclosed,
                Some(receipt.rescore_evaluations),
                "{label}: the executed route discloses the rescoring reads it spent"
            );
        }
        Err(error) => {
            assert!(
                matches!(error, Error::VectorIndexedRouteUnavailable { .. }),
                "{label}: a search that does not answer is refused typed, not {error:?}"
            );
            assert_eq!(
                receipt.unresolved,
                Some(HnswAllowedSearchIncomplete::VectorEvaluationLimitReached),
                "{label}: refused for its evaluation limit"
            );
        }
    }
}

fn check_group(db: &Database, label: &str) {
    let exact = search(db, "EXACT").ids.expect("EXACT answers");
    let narrow = search(db, "INDEXED");
    assert!(
        narrow.graph + narrow.stored <= allowance(NARROW),
        "{label}: the narrow search computed {} graph + {} stored distances, past its \
         allowance of {} ({:?})",
        narrow.graph,
        narrow.stored,
        allowance(NARROW),
        narrow.receipts
    );
    reconcile(&format!("{label} narrow"), &narrow);
    if let Ok(answer) = &narrow.ids {
        assert_eq!(answer, &exact, "{label}: a narrow answer is EXACT's answer");
    }
    breadth(db, WIDE);
    let wide = search(db, "INDEXED");
    assert!(
        wide.graph + wide.stored <= allowance(WIDE),
        "{label}: the wide search computed {} distances, past {}",
        wide.graph + wide.stored,
        allowance(WIDE)
    );
    reconcile(&format!("{label} wide"), &wide);
    assert_eq!(
        wide.ids
            .unwrap_or_else(|error| panic!("{label}: the wide search answers: {error:?}")),
        exact,
        "{label}: the wide search answers as EXACT does"
    );
    breadth(db, NARROW);
}

/// Current rows are scored from their stored bodies only as final candidates;
/// those reads are reported like any other.
fn check_current(db: &Database, label: &str) {
    breadth(db, WIDE);
    let exact = search(db, "EXACT").ids.expect("EXACT answers");
    let indexed = search(db, "INDEXED");
    reconcile(label, &indexed);
    assert!(
        indexed.stored > 0,
        "{label}: final candidate scoring reads stored vectors"
    );
    assert_eq!(
        indexed
            .ids
            .unwrap_or_else(|error| panic!("{label}: the search answers: {error:?}")),
        exact,
        "{label}: INDEXED answers as EXACT does"
    );
    breadth(db, NARROW);
}

#[test]
fn restamped_group_rescoring_stays_inside_the_breadth_allowance() {
    for representation in REPRESENTATIONS {
        let current = Database::open_memory();
        fill(&current, representation, false);
        check_current(&current, &format!("current{representation}"));
        restamp_half(&current);
        check_current(&current, &format!("mixed{representation}"));

        let db = Database::open_memory();
        fill(&db, representation, true);
        restamp_group(&db);
        check_group(&db, &format!("resident{representation}"));

        let root = TempDir::new().expect("fixture directory");
        let path = root.path().join("grouped.redb");
        {
            let db = Database::open(&path).expect("open the durable fixture");
            fill(&db, representation, true);
            db.close().expect("close");
        }
        let reopened = Database::open(&path).expect("reopen the saved graph");
        reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        restamp_group(&reopened);
        check_group(&reopened, &format!("reopened{representation}"));
    }
}
