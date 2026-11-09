//! `candidate_elides_sort` must recognize a reverse-direction match, so a
//! descending `ORDER BY` can make a composite index rank above a
//! non-unique whole-key one, even though the bounded read kernel is able to
//! walk that composite backward and already asks the equivalent
//! either-direction question once an index is chosen
//! (`sort_keys_match_index_prefix_in_either_direction`). Candidate
//! SELECTION has to ask the same either-direction question, or a
//! reverse-only route is discarded before it is ever considered.
//!
//! This exercises the same non-unique whole-key vs. composite tie as
//! `composite_index_elides_a_still_needed_sort_over_whole_key.rs`, but with
//! `ORDER BY seq DESC` against a composite declared ASC, both with and
//! without a LIMIT, through `.execute` (the bounded read kernel).

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

const FK_INDEX: &str = "desc_widget_parent_fk_idx";
const COMPOSITE_INDEX: &str = "desc_widget_parent_seq_idx";

fn seed() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE desc_widget (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL, seq INTEGER NOT NULL)",
        &empty(),
    )
    .expect("create the desc_widget table");
    db.execute(
        &format!("CREATE INDEX {FK_INDEX} ON desc_widget (parent_id)"),
        &empty(),
    )
    .expect("create the whole-key, non-unique FK-shaped index");
    db.execute(
        &format!("CREATE INDEX {COMPOSITE_INDEX} ON desc_widget (parent_id, seq)"),
        &empty(),
    )
    .expect("create the composite index, declared ascending");
    for (id, parent, seq) in [(1_i64, 1_i64, 0_i64), (2, 1, 1), (3, 1, 2), (4, 2, 0)] {
        db.execute(
            "INSERT INTO desc_widget (id, parent_id, seq) VALUES ($id, $parent, $seq)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("parent".into(), Value::Int64(parent)),
                ("seq".into(), Value::Int64(seq)),
            ]),
        )
        .expect("insert a desc_widget row");
    }
    db
}

fn params() -> HashMap<String, Value> {
    HashMap::from([("parent".into(), Value::Int64(1))])
}

/// ASC still elides (the forward case `composite_index_elides_a_still_
/// needed_sort_over_whole_key.rs` already covers for a different fixture);
/// pinned again here so ASC and DESC are proven against the very same
/// table and index declarations.
#[test]
fn ascending_order_elides_through_the_composite_index() {
    let db = seed();
    let result = db
        .execute(
            "SELECT id FROM desc_widget WHERE parent_id = $parent ORDER BY seq ASC",
            &params(),
        )
        .expect("the ascending read must succeed");
    assert_eq!(
        result.rows(),
        vec![
            vec![Value::Int64(1)],
            vec![Value::Int64(2)],
            vec![Value::Int64(3)]
        ],
        "{:?}",
        result.trace()
    );
    assert_eq!(
        result.trace().index_used.as_deref(),
        Some(COMPOSITE_INDEX),
        "{:?}",
        result.trace()
    );
    assert!(result.trace().sort_elided, "{:?}", result.trace());
}

/// A descending `ORDER BY` must select the same composite index, walked
/// backward, instead of falling back to the whole-key index and a buffered
/// sort.
#[test]
fn descending_order_elides_through_the_same_composite_index_walked_backward() {
    let db = seed();
    let result = db
        .execute(
            "SELECT id FROM desc_widget WHERE parent_id = $parent ORDER BY seq DESC",
            &params(),
        )
        .expect("the descending read must succeed");
    assert_eq!(
        result.rows(),
        vec![
            vec![Value::Int64(3)],
            vec![Value::Int64(2)],
            vec![Value::Int64(1)]
        ],
        "rows must arrive in descending seq order: {:?}",
        result.trace()
    );
    assert_eq!(
        result.trace().index_used.as_deref(),
        Some(COMPOSITE_INDEX),
        "a descending ORDER BY must still select the composite index, walked backward, not the \
         whole-key index: {:?}",
        result.trace()
    );
    assert!(
        result.trace().sort_elided,
        "the backward walk already answers the order, so no separate sort should run: {:?}",
        result.trace()
    );
}

/// The same descending read under a LIMIT must take rows from the correct
/// end of the backward walk, not the first rows of a forward one.
#[test]
fn descending_order_with_limit_takes_the_correct_end_of_the_backward_walk() {
    let db = seed();
    let result = db
        .execute(
            "SELECT id FROM desc_widget WHERE parent_id = $parent ORDER BY seq DESC LIMIT 2",
            &params(),
        )
        .expect("the limited descending read must succeed");
    assert_eq!(
        result.rows(),
        vec![vec![Value::Int64(3)], vec![Value::Int64(2)]],
        "LIMIT 2 after ORDER BY seq DESC must be the two highest-seq rows: {:?}",
        result.trace()
    );
    assert_eq!(
        result.trace().index_used.as_deref(),
        Some(COMPOSITE_INDEX),
        "{:?}",
        result.trace()
    );
    assert!(result.trace().sort_elided, "{:?}", result.trace());
}
