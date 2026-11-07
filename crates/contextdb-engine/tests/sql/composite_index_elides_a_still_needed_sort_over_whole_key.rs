//! Winner comparator in `analyze_filter_for_index`: among candidates tied on
//! the matched equality prefix, a candidate that elides a still-needed
//! `ORDER BY` beats whole-key
//! coverage, UNLESS the whole-key candidate is a UNIQUE index (at most one
//! row, so the sort is moot) -- then whole-key still wins.
//!
//! The fixture mirrors the shape `receipt_fold_keeps_ordinal_hash_and_count_
//! across_small_cursor_pages` (context-graph) exercises against a real
//! foreign-key index: an equality-bound leading column shared by a
//! single-column (whole-key) index and a composite index whose second
//! column answers the requested order.

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

const FK_INDEX: &str = "widget_parent_fk_idx";
const COMPOSITE_INDEX: &str = "widget_parent_seq_idx";
const UNIQUE_FK_INDEX: &str = "__unique_parent_id";
const UNIQUE_COMPOSITE_INDEX: &str = "widget_u_parent_seq_idx";

/// A non-unique leading column with two tied candidate indexes: one covers
/// only that column (whole-key), the other adds `seq` (not whole-key, but
/// its declared order answers `ORDER BY seq`).
fn seed_non_unique_fixture() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE widget (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL, seq INTEGER NOT NULL)",
        &empty(),
    )
    .expect("create the non-unique-parent widget table");
    db.execute(
        &format!("CREATE INDEX {FK_INDEX} ON widget (parent_id)"),
        &empty(),
    )
    .expect("create the whole-key, non-unique FK-shaped index");
    db.execute(
        &format!("CREATE INDEX {COMPOSITE_INDEX} ON widget (parent_id, seq)"),
        &empty(),
    )
    .expect("create the sort-eliding composite index");
    for parent in 0..3_i64 {
        for seq in 0..3_i64 {
            db.execute(
                "INSERT INTO widget (id, parent_id, seq) VALUES ($id, $parent, $seq)",
                &HashMap::from([
                    ("id".into(), Value::Int64(parent * 100 + seq)),
                    ("parent".into(), Value::Int64(parent)),
                    ("seq".into(), Value::Int64(seq)),
                ]),
            )
            .expect("insert a widget row");
        }
    }
    db
}

/// The same shape, except the leading column is declared UNIQUE, so the
/// whole-key index reads at most one row and the sort is moot for it.
fn seed_unique_fixture() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE widget_u (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL UNIQUE, seq INTEGER NOT NULL)",
        &empty(),
    )
    .expect("create the unique-parent widget table");
    // The column-level UNIQUE above already declares the automatic
    // whole-key index `__unique_parent_id`; no explicit index is declared
    // for it.
    db.execute(
        &format!("CREATE INDEX {UNIQUE_COMPOSITE_INDEX} ON widget_u (parent_id, seq)"),
        &empty(),
    )
    .expect("create the sort-eliding composite index");
    for parent in 0..3_i64 {
        db.execute(
            "INSERT INTO widget_u (id, parent_id, seq) VALUES ($id, $parent, 0)",
            &HashMap::from([
                ("id".into(), Value::Int64(parent)),
                ("parent".into(), Value::Int64(parent)),
            ]),
        )
        .expect("insert a widget_u row");
    }
    db
}

fn params(parent: i64) -> HashMap<String, Value> {
    HashMap::from([("parent".into(), Value::Int64(parent))])
}

/// (a) The receipt_fold shape: equality on a non-unique FK-shaped column,
/// `ORDER BY` the composite index's next column. The composite index must
/// win and the sort must be reported elided.
#[test]
fn a_sort_eliding_composite_index_beats_a_non_unique_whole_key_index() {
    let db = seed_non_unique_fixture();
    let result = db
        .execute(
            "SELECT id FROM widget WHERE parent_id = $parent ORDER BY seq ASC",
            &params(1),
        )
        .expect("the ordered widget read must succeed");
    assert_eq!(
        result.rows(),
        vec![
            vec![Value::Int64(100)],
            vec![Value::Int64(101)],
            vec![Value::Int64(102)],
        ],
        "rows must already arrive in the requested order: {:?}",
        result.trace()
    );
    assert_eq!(
        result.trace().index_used.as_deref(),
        Some(COMPOSITE_INDEX),
        "a still-needed ORDER BY must be answered by the composite index, not the whole-key one: {:?}",
        result.trace()
    );
    assert!(
        result.trace().sort_elided,
        "the composite index's own order already answers ORDER BY seq ASC, so no sort should run: {:?}",
        result.trace()
    );
}

/// (b) The same tie, except the whole-key candidate is UNIQUE: it reads at
/// most one row, so the sort is moot and whole-key coverage wins as before.
#[test]
fn a_unique_whole_key_index_still_wins_over_a_sort_eliding_composite_candidate() {
    let db = seed_unique_fixture();
    let result = db
        .execute(
            "SELECT id FROM widget_u WHERE parent_id = $parent ORDER BY seq ASC",
            &params(1),
        )
        .expect("the ordered widget_u read must succeed");
    assert_eq!(
        result.trace().index_used.as_deref(),
        Some(UNIQUE_FK_INDEX),
        "a UNIQUE whole-key index must still win: at most one row means the sort is moot: {:?}",
        result.trace()
    );
}

/// (c) With no `ORDER BY` at all, whole-key preference is
/// unchanged: the whole-key index still wins the same non-unique tie (a),
/// and the composite index is still rejected for whole-key coverage, in
/// the ordinary whole-key refusal wording.
#[test]
fn with_no_order_by_the_original_whole_key_preference_is_unchanged() {
    let db = seed_non_unique_fixture();
    let result = db
        .execute(
            "SELECT id FROM widget WHERE parent_id = $parent",
            &params(1),
        )
        .expect("the unordered widget read must succeed");
    assert_eq!(
        result.trace().index_used.as_deref(),
        Some(FK_INDEX),
        "with no ORDER BY, whole-key coverage must still decide the tie exactly as before: {:?}",
        result.trace()
    );
    assert!(
        !result.trace().sort_elided,
        "there is no ORDER BY to elide: {:?}",
        result.trace()
    );
    let composite = result
        .trace()
        .indexes_considered
        .iter()
        .find(|candidate| candidate.name == COMPOSITE_INDEX)
        .unwrap_or_else(|| panic!("trace omitted the competing index: {:?}", result.trace()));
    assert_eq!(
        composite.rejected_reason,
        "chosen index has whole-key coverage",
        "the pre-existing rejection wording must survive untouched when no sort is in play: {:?}",
        result.trace()
    );
}

/// (d) When the amendment does flip the winner (case a), the loser's
/// `rejected_reason` in `.explain` must say so in plain words, not restate
/// the old code/enum-shaped reason and not dump a struct or variant name.
#[test]
fn the_displaced_whole_key_candidate_states_its_new_rejection_in_plain_words() {
    let db = seed_non_unique_fixture();
    // `.explain`, not `.execute`: the bounded read kernel's own ordered-scan
    // fast path (taken by `.execute` here) answers straight from the index,
    // the same as the eager kernel's `run_index_scan_with_order`, and does
    // not carry the full candidate list. `.explain` is the passive route
    // that always does.
    let explained = db
        .explain_output("SELECT id FROM widget WHERE parent_id = 1 ORDER BY seq ASC")
        .expect("explaining the ordered widget read must succeed");
    let displaced = explained
        .indexes_considered
        .iter()
        .find(|candidate| candidate.name == FK_INDEX)
        .unwrap_or_else(|| {
            panic!(
                "explain omitted the displaced whole-key index: {:?}",
                explained.indexes_considered
            )
        });
    assert_eq!(
        displaced.rejected_reason,
        "chosen index already answers the requested order without a sort",
        "the displaced whole-key candidate must name the new reason in plain words, not the old \
         whole-key-coverage wording (which is now backwards) or a code/enum dump: {:?}",
        explained.indexes_considered
    );
    for forbidden in ["::", "Some(", "None", "{", "}", "Ordering"] {
        assert!(
            !displaced.rejected_reason.contains(forbidden),
            "rejected_reason must read as plain words, not a code or enum dump: {:?}",
            displaced.rejected_reason
        );
    }
}
