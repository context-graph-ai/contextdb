//! The winner comparator must give candidates a total order. Three
//! non-unique candidates tied on the matched equality prefix -- an unrelated
//! composite, a sort-eliding composite, and a whole-key index -- can form a
//! preference cycle (B beats A by sort elision, A beats C by whole-key
//! coverage, C beats B by creation order) if selection uses pairwise exceptions.
//!
//! One scalar placement rank shared by every candidate cannot cycle. This file proves the winner is
//! independent of declaration order, and that EXPLAIN's rejected_reason for
//! every loser matches the actual ranking decision.

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

const UNRELATED: &str = "cycle_unrelated_idx"; // C = (a, c): matches neither the sort nor whole-key.
const SORT_SAVING: &str = "cycle_sort_saving_idx"; // B = (a, b): elides ORDER BY b.
const WHOLE_KEY: &str = "cycle_whole_key_idx"; // A = (a): whole-key, does not elide the sort.

/// One table per declaration order, so each permutation's own `creation_index`
/// tie-break is exercised for real rather than simulated.
fn seed_permutation(declare: [&str; 3]) -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE cycle_row (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL, c INTEGER NOT NULL)",
        &empty(),
    )
    .expect("create the cycle table");
    for index_name in declare {
        let ddl = match index_name {
            UNRELATED => format!("CREATE INDEX {UNRELATED} ON cycle_row (a, c)"),
            SORT_SAVING => format!("CREATE INDEX {SORT_SAVING} ON cycle_row (a, b)"),
            WHOLE_KEY => format!("CREATE INDEX {WHOLE_KEY} ON cycle_row (a)"),
            other => panic!("unknown index name {other}"),
        };
        db.execute(&ddl, &empty())
            .unwrap_or_else(|e| panic!("declare {index_name}: {e}"));
    }
    for (b, c) in [(2_i64, 9_i64), (0, 7), (1, 8)] {
        db.execute(
            "INSERT INTO cycle_row (id, a, b, c) VALUES ($id, 1, $b, $c)",
            &HashMap::from([
                ("id".into(), Value::Int64(b * 10 + c)),
                ("b".into(), Value::Int64(b)),
                ("c".into(), Value::Int64(c)),
            ]),
        )
        .expect("insert a cycle row");
    }
    db
}

fn all_permutations() -> Vec<[&'static str; 3]> {
    let names = [UNRELATED, SORT_SAVING, WHOLE_KEY];
    let mut out = Vec::new();
    for i in 0..3 {
        for j in 0..3 {
            if j == i {
                continue;
            }
            for k in 0..3 {
                if k == i || k == j {
                    continue;
                }
                out.push([names[i], names[j], names[k]]);
            }
        }
    }
    out
}

/// The sort-saving composite must win regardless of declaration order,
/// including the case the finding names explicitly: the unrelated composite
/// declared first.
#[test]
fn the_sort_saving_candidate_wins_under_every_declaration_order() {
    for declare in all_permutations() {
        let db = seed_permutation(declare);
        let result = db
            .execute(
                "SELECT id FROM cycle_row WHERE a = 1 ORDER BY b ASC",
                &empty(),
            )
            .unwrap_or_else(|e| panic!("query under declaration order {declare:?}: {e}"));
        assert_eq!(
            result.trace().index_used.as_deref(),
            Some(SORT_SAVING),
            "declaration order {declare:?} must not change the winner: {:?}",
            result.trace()
        );
        assert!(
            result.trace().sort_elided,
            "declaration order {declare:?}: the sort must be reported elided: {:?}",
            result.trace()
        );
        assert_eq!(
            result.rows(),
            vec![
                vec![Value::Int64(7)],
                vec![Value::Int64(18)],
                vec![Value::Int64(29)],
            ],
            "declaration order {declare:?}: rows must already arrive in b-order: {:?}",
            result.trace()
        );
    }
}

/// EXPLAIN's rejected_reason for every loser must match the actual ranking
/// decision: the unrelated composite lost the same way the whole-key index
/// did (the winner already answers the order), not to "whole-key coverage"
/// (which is now the loser's own excuse, not the winner's).
#[test]
fn explains_rejection_reasons_match_the_ranking_for_every_loser() {
    let db = seed_permutation([UNRELATED, SORT_SAVING, WHOLE_KEY]);
    let explained = db
        .explain_output("SELECT id FROM cycle_row WHERE a = 1 ORDER BY b ASC")
        .expect("explain must succeed");
    assert_eq!(explained.index_used.as_deref(), Some(SORT_SAVING));
    for loser in [WHOLE_KEY, UNRELATED] {
        let candidate = explained
            .indexes_considered
            .iter()
            .find(|c| c.name == loser)
            .unwrap_or_else(|| {
                panic!(
                    "explain omitted {loser}: {:?}",
                    explained.indexes_considered
                )
            });
        assert_eq!(
            candidate.rejected_reason,
            "chosen index already answers the requested order without a sort",
            "{loser} must be rejected for the real reason it lost -- the winner already answers \
             the order -- not a reason describing the winner's whole-key coverage: {:?}",
            explained.indexes_considered
        );
    }
}

/// Two different whole-key indexes on the SAME leading column both trivially
/// answer `ORDER BY` on that column (the equality-pinned value never varies),
/// so both are whole-key AND sort-eliding at once. The comparator must still
/// resolve this deterministically (by creation order), never returning
/// `Greater` in both directions.
#[test]
fn two_whole_key_candidates_that_both_supply_the_order_resolve_deterministically() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE cycle_twins (id INTEGER PRIMARY KEY, a INTEGER NOT NULL)",
        &empty(),
    )
    .expect("create the twins table");
    db.execute(
        "CREATE INDEX cycle_twin_first_idx ON cycle_twins (a)",
        &empty(),
    )
    .expect("declare the first whole-key index");
    db.execute(
        "CREATE INDEX cycle_twin_second_idx ON cycle_twins (a)",
        &empty(),
    )
    .expect("declare the second whole-key index");
    db.execute("INSERT INTO cycle_twins (id, a) VALUES (1, 1)", &empty())
        .expect("insert a twins row");
    let result = db
        .execute(
            "SELECT id FROM cycle_twins WHERE a = 1 ORDER BY a ASC",
            &empty(),
        )
        .expect("query must succeed");
    assert_eq!(
        result.trace().index_used.as_deref(),
        Some("cycle_twin_first_idx"),
        "two equally-ranked whole-key, sort-eliding candidates must resolve by creation order: {:?}",
        result.trace()
    );
    assert!(result.trace().sort_elided, "{:?}", result.trace());
}
