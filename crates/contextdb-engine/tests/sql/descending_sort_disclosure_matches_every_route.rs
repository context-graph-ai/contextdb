//! Candidate selection can consider a reverse-direction match. Passive
//! EXPLAIN, bounded execution, and eager execution must use the same
//! either-direction check for a descending `ORDER BY`:
//!
//! - Passive EXPLAIN's Scan and Sort steps must agree when the selected
//!   composite index answers a descending order in reverse.
//! - The eager executor uses a forward-only index walk, so its candidate
//!   selection must not promise a reverse elision.
//!
//! This file proves passive EXPLAIN is internally consistent for a
//! descending order (with and without LIMIT), and that eager execution
//! never claims to elide a sort it still has to run.

use contextdb_core::Value;
use contextdb_engine::Database;
use contextdb_engine::executor::execute_eager_for_test;
use std::collections::HashMap;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

const FK_INDEX: &str = "desc_disclosure_fk_idx";
const COMPOSITE_INDEX: &str = "desc_disclosure_composite_idx";
const ONE_ROW_REASON: &str = "chosen index already answers the requested order without a sort";

fn seed() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE desc_disclosure (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL, seq INTEGER NOT NULL)",
        &empty(),
    )
    .expect("create the desc_disclosure table");
    db.execute(
        &format!("CREATE INDEX {FK_INDEX} ON desc_disclosure (parent_id)"),
        &empty(),
    )
    .expect("create the whole-key, non-unique FK-shaped index");
    db.execute(
        &format!("CREATE INDEX {COMPOSITE_INDEX} ON desc_disclosure (parent_id, seq)"),
        &empty(),
    )
    .expect("create the composite index, declared ascending");
    for (id, parent, seq) in [(1_i64, 1_i64, 0_i64), (2, 1, 1), (3, 1, 2)] {
        db.execute(
            "INSERT INTO desc_disclosure (id, parent_id, seq) VALUES ($id, $parent, $seq)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("parent".into(), Value::Int64(parent)),
                ("seq".into(), Value::Int64(seq)),
            ]),
        )
        .expect("insert a desc_disclosure row");
    }
    db
}

fn assert_passive_explain_is_consistent(sql: &str) {
    let db = seed();
    let explained = db.explain_output(sql).expect("explain must succeed");
    assert_eq!(
        explained.index_used.as_deref(),
        Some(COMPOSITE_INDEX),
        "{sql}: {:?}",
        explained.indexes_considered
    );
    let displaced = explained
        .indexes_considered
        .iter()
        .find(|c| c.name == FK_INDEX)
        .unwrap_or_else(|| {
            panic!(
                "{sql}: explain omitted {FK_INDEX}: {:?}",
                explained.indexes_considered
            )
        });
    assert_eq!(
        displaced.rejected_reason, ONE_ROW_REASON,
        "{sql}: the displaced whole-key index's own rejection already claims the winner answers \
         the order without a sort"
    );
    assert!(
        explained.sort_elided,
        "{sql}: explain's own sort_elided must agree with the reason it just gave for the \
         displaced candidate, not contradict it"
    );
}

/// Passive EXPLAIN must be internally consistent for a descending order: if
/// it says the winner already answers the order, its own `sort_elided` must
/// say so too.
#[test]
fn passive_explain_agrees_with_itself_on_a_descending_order() {
    assert_passive_explain_is_consistent(
        "SELECT id FROM desc_disclosure WHERE parent_id = 1 ORDER BY seq DESC",
    );
}

/// The same consistency requirement survives a LIMIT above the ORDER BY.
#[test]
fn passive_explain_agrees_with_itself_on_a_descending_order_with_limit() {
    assert_passive_explain_is_consistent(
        "SELECT id FROM desc_disclosure WHERE parent_id = 1 ORDER BY seq DESC LIMIT 2",
    );
}

/// The eager executor cannot walk an index backward. For a descending
/// order it must still return correctly ordered rows (via a real sort), and
/// it must not claim to have elided a sort it actually ran.
#[test]
fn eager_execution_never_claims_to_elide_a_descending_sort_it_still_runs() {
    let db = seed();
    let result = execute_eager_for_test(
        &db,
        "SELECT id FROM desc_disclosure WHERE parent_id = 1 ORDER BY seq DESC",
        &empty(),
        db.declared_reader_memory()
            .expect("admit the eager fixture reader"),
    )
    .expect("eager execution must succeed");
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int64(3)],
            vec![Value::Int64(2)],
            vec![Value::Int64(1)],
        ],
        "rows must still arrive in descending seq order: {:?}",
        result.trace
    );
    assert!(
        !result.trace.sort_elided,
        "the eager executor cannot walk an index backward, so it actually ran a sort here; it \
         must not claim otherwise: {:?}",
        result.trace
    );
    if let Some(fk) = result
        .trace
        .indexes_considered
        .iter()
        .find(|c| c.name == FK_INDEX)
    {
        assert_ne!(
            fk.rejected_reason, ONE_ROW_REASON,
            "the eager executor picked whichever index it did honestly, having never claimed a \
             reverse elision it cannot deliver; its own considered-candidate reasons must not \
             say otherwise: {:?}",
            result.trace
        );
    }
}

/// Control: eager execution's own ascending elision (forward direction,
/// unaffected by this fix) must still work.
#[test]
fn eager_execution_still_elides_an_ascending_sort() {
    let db = seed();
    let result = execute_eager_for_test(
        &db,
        "SELECT id FROM desc_disclosure WHERE parent_id = 1 ORDER BY seq ASC",
        &empty(),
        db.declared_reader_memory()
            .expect("admit the eager fixture reader"),
    )
    .expect("eager execution must succeed");
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int64(1)],
            vec![Value::Int64(2)],
            vec![Value::Int64(3)],
        ],
        "{:?}",
        result.trace
    );
    assert_eq!(
        result.trace.index_used.as_deref(),
        Some(COMPOSITE_INDEX),
        "{:?}",
        result.trace
    );
    assert!(result.trace.sort_elided, "{:?}", result.trace);
}
