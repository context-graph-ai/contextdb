//! Proof rows for reversed-operand equality in the statement-decides-pinned-ness planner fix.
//!
//! Promise (docs/query-language.md, "Explicit-anchor SELECT shapes"): whether a statement names one
//! row is decided by the statement and the table's declared keys. Equality is symmetric, so
//! `$v = membership_id` names exactly the row `membership_id = $v` names: a hidden named row is
//! refused, a visible one is returned, a residual mismatch is simply empty, and the plan is the
//! same on every route.
//!
//! Authored by writecost-opus-01 (the separate test author was closed; the source reviewer specified
//! the expectation).

use contextdb_core::read_contract::ReadLimits;
use contextdb_core::{ContextId, Error, Value};
use contextdb_engine::Database;
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

const CTX_A: u128 = 0xA;
const CTX_B: u128 = 0xB;
const GEN: u128 = 0x6;
const M_A: u128 = 0x1000;
const M_B: u128 = 0x2000;

fn none() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(name, value)| (name.to_owned(), value)).collect()
}

fn uuid(id: u128) -> Value {
    Value::Uuid(Uuid::from_u128(id))
}

fn roomy() -> ReadLimits {
    ReadLimits {
        result_rows: 1_000_000,
        result_bytes: 512 * 1024 * 1024,
        work: 1_000_000_000,
        active_ms: 600_000,
        memory: 512 * 1024 * 1024,
        cursor_page_rows: 100_000,
        cursor_page_bytes: 64 * 1024 * 1024,
        cursor_idle_ms: 600_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

fn contexts(ids: &[u128]) -> BTreeSet<ContextId> {
    ids.iter().map(|id| ContextId::new(Uuid::from_u128(*id))).collect()
}

/// `members`: membership_id UNIQUE, context-scoped, optionally a nonunique (context_id,
/// generation_id) index. One row in context A (M_A) and one in context B (M_B).
fn world(extra_index: bool) -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE members (id INTEGER PRIMARY KEY, membership_id UUID NOT NULL UNIQUE, context_id UUID CONTEXT_ID NOT NULL, generation_id UUID NOT NULL, ordinal INTEGER NOT NULL)",
        &none(),
    )
    .unwrap();
    if extra_index {
        db.execute("CREATE INDEX members_ctx_gen ON members (context_id, generation_id)", &none())
            .unwrap();
    }
    for (id, membership, context) in [(1, M_A, CTX_A), (2, M_B, CTX_B)] {
        db.execute(
            "INSERT INTO members (id, membership_id, context_id, generation_id, ordinal) VALUES ($id, $m, $c, $g, $o)",
            &params(vec![
                ("id", Value::Int64(id)),
                ("m", uuid(membership)),
                ("c", uuid(context)),
                ("g", uuid(GEN)),
                ("o", Value::Int64(id)),
            ]),
        )
        .unwrap();
    }
    db
}

#[derive(Debug, PartialEq, Eq)]
enum Answer {
    Rows(usize),
    ScopeViolation,
    Other(String),
}

fn classify(result: Result<usize, Error>) -> Answer {
    match result {
        Ok(rows) => Answer::Rows(rows),
        Err(Error::ContextScopeViolation { .. }) => Answer::ScopeViolation,
        Err(other) => Answer::Other(format!("{other:?}")),
    }
}

/// Live handle, bounded read session, retained cursor, and a transaction with a staged row.
fn every_route(db: &Database, sql: &str, binds: &HashMap<String, Value>) -> Vec<(&'static str, Answer)> {
    let narrowed = db.scoped_with_contexts(contexts(&[CTX_A]));
    let mut answers = vec![("eager", classify(narrowed.execute(sql, binds).map(|r| r.rows.len())))];
    let session = narrowed.read_session(roomy()).expect("open a bounded read view");
    answers.push(("bounded", classify(session.execute(sql, binds).map(|r| r.rows.len()))));
    answers.push((
        "retained cursor",
        classify(session.open_cursor(sql, binds).map(|cursor| cursor.first_page().rows.len())),
    ));
    let tx = narrowed.begin().expect("begin");
    narrowed
        .execute_in_tx(
            tx,
            "INSERT INTO members (id, membership_id, context_id, generation_id, ordinal) VALUES (99, $m, $c, $g, 99)",
            &params(vec![("m", uuid(0x9999)), ("c", uuid(CTX_A)), ("g", uuid(GEN))]),
        )
        .expect("stage a row of the reader's own");
    answers.push((
        "transaction with staged rows",
        classify(narrowed.execute_in_tx(tx, sql, binds).map(|r| r.rows.len())),
    ));
    narrowed.rollback(tx).expect("rollback");
    answers
}

fn all_routes_answer(db: &Database, sql: &str, binds: &HashMap<String, Value>, expected: &Answer, label: &str) {
    for (route, answer) in every_route(db, sql, binds) {
        assert_eq!(&answer, expected, "{label}: route `{route}` for `{sql}`");
    }
}

/// A hidden row named by its unique key with the operands reversed is refused on every route,
/// with and without a secondary index, alone and beside reversed residual conjuncts.
#[test]
fn a_hidden_row_named_with_reversed_operands_is_refused_on_every_route() {
    for extra_index in [false, true] {
        let db = world(extra_index);
        let label = if extra_index { "with secondary index" } else { "unique key only" };
        all_routes_answer(
            &db,
            "SELECT id FROM members WHERE $m = membership_id",
            &params(vec![("m", uuid(M_B))]),
            &Answer::ScopeViolation,
            label,
        );
        all_routes_answer(
            &db,
            "SELECT id FROM members WHERE $b = context_id AND $g = generation_id AND $m = membership_id",
            &params(vec![("b", uuid(CTX_B)), ("g", uuid(GEN)), ("m", uuid(M_B))]),
            &Answer::ScopeViolation,
            label,
        );
    }
}

/// The same reversed statement naming a visible row returns it on every route.
#[test]
fn a_visible_row_named_with_reversed_operands_is_returned_on_every_route() {
    let db = world(true);
    all_routes_answer(
        &db,
        "SELECT id FROM members WHERE $m = membership_id",
        &params(vec![("m", uuid(M_A))]),
        &Answer::Rows(1),
        "visible reversed",
    );
}

/// A reversed statement whose own context conjunct does not match the hidden row does not name it:
/// the complete predicate decides first, so the answer is empty, never a refusal.
#[test]
fn a_reversed_residual_mismatch_is_empty_on_every_route() {
    let db = world(true);
    all_routes_answer(
        &db,
        "SELECT id FROM members WHERE $a = context_id AND $g = generation_id AND $m = membership_id",
        &params(vec![("a", uuid(CTX_A)), ("g", uuid(GEN)), ("m", uuid(M_B))]),
        &Answer::Rows(0),
        "reversed residual mismatch",
    );
}

/// EXPLAIN plans the reversed form exactly as the column-first form: the same index, the same
/// reasons.
#[test]
fn explain_plans_reversed_equality_exactly_as_column_first() {
    for extra_index in [false, true] {
        let db = world(extra_index);
        let m = Uuid::from_u128(M_B);
        let g = Uuid::from_u128(GEN);
        let b = Uuid::from_u128(CTX_B);
        let pairs = [
            (
                format!("SELECT id FROM members WHERE membership_id = '{m}'"),
                format!("SELECT id FROM members WHERE '{m}' = membership_id"),
            ),
            (
                format!("SELECT id FROM members WHERE context_id = '{b}' AND generation_id = '{g}' AND membership_id = '{m}'"),
                format!("SELECT id FROM members WHERE '{b}' = context_id AND '{g}' = generation_id AND '{m}' = membership_id"),
            ),
        ];
        for (column_first, reversed) in pairs {
            assert_eq!(
                db.explain(&reversed).unwrap(),
                db.explain(&column_first).unwrap(),
                "explain differs for reversed operands (extra_index={extra_index})"
            );
        }
    }
}
