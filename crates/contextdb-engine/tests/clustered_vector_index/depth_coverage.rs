//! Coverage is decided per depth. Calibration persists, for every depth up to
//! its maximum k, how many exact identities each sample query's answer prefix
//! holds. A LIMIT k query is covered only when the summary at depth k itself
//! meets the declared recall on every sample query; a passing larger depth
//! does not cover it. Here one sample answer holds every exact top-50 identity
//! within its first 50 but misses two of the exact top 11 within its first
//! 11: LIMIT 11 is uncovered (AUTO takes EXACT; explicit INDEXED serves
//! unqualified), while LIMIT 5 and LIMIT 50, which pass on every sample
//! query, are covered.

use super::support::{
    ObjectiveStatus, SearchOutcome, SearchRoute, SearchWork, coverage_summary, drive_until_serving,
    empty, int_ids, observe_search, open_caller_driven, params, rewrite_next_calibration_answers,
    unit_vector,
};
use contextdb_core::Value;
use contextdb_engine::Database;

const DIMENSIONS: usize = 16;
const ROWS: i64 = 300;

/// The answer scored for the first sample query: the exact top 50, reordered
/// so the first 11 hold the exact top 9 then exact ranks 12 and 13, and the
/// exact ranks 10 and 11 move to positions 49 and 50.
fn reordered(exact: &[u64]) -> Vec<u64> {
    assert!(exact.len() >= 50, "calibration ranks at least 50 deep");
    let mut answer = exact[..9].to_vec();
    answer.extend_from_slice(&exact[11..50]);
    answer.extend_from_slice(&exact[9..11]);
    answer.extend_from_slice(&exact[50..]);
    answer
}

fn search(db: &Database, mode: &str, limit: usize) -> (Vec<i64>, SearchWork) {
    let (result, work) = observe_search(db, || {
        db.execute(
            &format!(
                "SELECT id FROM covered_items ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT {limit}"
            ),
            &params([("query", Value::Vector(unit_vector(9_999, DIMENSIONS)))]),
        )
        .expect("vector search")
    });
    (int_ids(&result), work)
}

#[test]
fn a_limit_is_covered_only_by_the_summary_at_its_own_depth() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_caller_driven(&root.path().join("coverage.db"));
    db.execute(
        &format!(
            "CREATE TABLE covered_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}))"
        ),
        &empty(),
    )
    .expect("declare a vector column with the derived search policy");
    let tx = db.begin_or_panic();
    for id in 0..ROWS {
        db.execute_in_tx(
            tx,
            "INSERT INTO covered_items (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                (
                    "embedding",
                    Value::Vector(unit_vector(id as u64, DIMENSIONS)),
                ),
            ]),
        )
        .expect("stage one row");
    }
    db.commit(tx).expect("commit the rows");
    {
        let _rewritten = rewrite_next_calibration_answers(
            &db,
            "covered_items",
            "embedding",
            &[],
            Box::new(|query, exact| {
                if query == 0 {
                    reordered(exact)
                } else {
                    exact.to_vec()
                }
            }),
        );
        drive_until_serving(&db);
    }

    let summary = coverage_summary(&db, "covered_items", "embedding", &[]);
    let at = |depth: u32| {
        *summary
            .iter()
            .find(|row| row.depth == depth)
            .unwrap_or_else(|| panic!("the summary has depth {depth}: {summary:?}"))
    };
    assert!(
        summary.len() >= 50
            && summary
                .iter()
                .enumerate()
                .all(|(i, row)| row.depth == i as u32 + 1),
        "the summary holds every depth from 1 upward: {summary:?}"
    );
    assert_eq!(
        (at(11).minimum_hits, at(11).queries_below),
        (9, 1),
        "at depth 11 one sample prefix holds only 9 of the exact top 11"
    );
    assert_eq!(at(5).queries_below, 0, "every sample prefix of 5 is exact");
    assert_eq!(
        (at(50).minimum_hits, at(50).queries_below),
        (50, 0),
        "every sample prefix of 50 holds the exact top 50"
    );

    for limit in [5_usize, 50] {
        let (_, auto) = search(&db, "AUTO", limit);
        assert_eq!(
            auto.route,
            Some(SearchRoute::Indexed),
            "AUTO serves covered LIMIT {limit} from the index: {auto:?}"
        );
        let (_, indexed) = search(&db, "INDEXED", limit);
        assert_eq!(indexed.outcome, Some(SearchOutcome::Complete));
        assert_eq!(
            indexed.objective_status,
            Some(ObjectiveStatus::Met),
            "a covered LIMIT {limit} meets its objective: {indexed:?}"
        );
    }

    let (auto_ids, auto) = search(&db, "AUTO", 11);
    assert_eq!(
        auto.route,
        Some(SearchRoute::Exact),
        "AUTO takes EXACT for the uncovered LIMIT 11, though depth 50 passes: {auto:?}"
    );
    let (exact_ids, _) = search(&db, "EXACT", 11);
    assert_eq!(auto_ids, exact_ids, "AUTO's LIMIT 11 is the exact answer");
    let (indexed_ids, indexed) = search(&db, "INDEXED", 11);
    assert_eq!(indexed_ids.len(), 11);
    assert_eq!(
        indexed.route,
        Some(SearchRoute::Indexed),
        "explicit INDEXED keeps its route"
    );
    assert_eq!(indexed.outcome, Some(SearchOutcome::Complete));
    assert_eq!(
        indexed.objective_status,
        Some(ObjectiveStatus::Unqualified),
        "explicit INDEXED serves the uncovered LIMIT 11 as unqualified: {indexed:?}"
    );
}
