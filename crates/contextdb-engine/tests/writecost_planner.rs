//! Proof rows for the statement-decides-pinned-ness planner fix.
//!
//! Promise (docs/query-language.md, "Explicit-anchor SELECT shapes"): whether a statement names one
//! row is decided by the statement and the table's declared keys, never by the index the planner
//! happened to choose. A hidden row the statement names is refused (`ContextScopeViolation`); any
//! other hidden row is simply not there. The answer is the same on every index, every route and
//! every source.

use contextdb_core::read_contract::ReadLimits;
use contextdb_core::{ContextId, Error, Value};
use contextdb_engine::Database;
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

fn none() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
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

const CTX_A: u128 = 0xA;
const CTX_B: u128 = 0xB;
const GEN: u128 = 0x6;
const M_A: u128 = 0x1000;
const M_B: u128 = 0x2000;

fn contexts(ids: &[u128]) -> BTreeSet<ContextId> {
    ids.iter()
        .map(|id| ContextId::new(Uuid::from_u128(*id)))
        .collect()
}

struct World {
    db: Database,
}

/// `members`: membership_id UNIQUE, context-scoped, nonunique (context_id, generation_id) index.
/// One row in context A (membership M_A) and one in context B (membership M_B), both in
/// generation GEN. The reader is narrowed to A.
fn world(extra_indexes: bool) -> World {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE members (id INTEGER PRIMARY KEY, membership_id UUID NOT NULL UNIQUE, context_id UUID CONTEXT_ID NOT NULL, generation_id UUID NOT NULL, ordinal INTEGER NOT NULL)",
        &none(),
    )
    .unwrap();
    if extra_indexes {
        db.execute(
            "CREATE INDEX members_ctx_gen ON members (context_id, generation_id)",
            &none(),
        )
        .unwrap();
    }
    for (id, membership, context) in [(1, M_A, CTX_A), (2, M_B, CTX_B)] {
        db.execute(
            "INSERT INTO members (id, membership_id, context_id, generation_id, ordinal) VALUES ($id, $m, $c, $g, $o)",
            &params(vec![
                ("id", Value::Int64(id)),
                ("m", Value::Uuid(Uuid::from_u128(membership))),
                ("c", Value::Uuid(Uuid::from_u128(context))),
                ("g", Value::Uuid(Uuid::from_u128(GEN))),
                ("o", Value::Int64(id)),
            ]),
        )
        .unwrap();
    }
    World { db }
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

/// The same statement through every door: live writer handle, bounded read session, a retained
/// cursor, and a transaction that has staged a row of its own.
fn every_route(
    world: &World,
    sql: &str,
    binds: &HashMap<String, Value>,
) -> Vec<(&'static str, Answer)> {
    let narrowed = world.db.scoped_with_contexts(contexts(&[CTX_A]));
    let mut answers = Vec::new();
    answers.push((
        "eager",
        classify(narrowed.execute(sql, binds).map(|r| r.rows.len())),
    ));
    let session = narrowed
        .read_session(roomy())
        .expect("open a bounded read view");
    answers.push((
        "bounded",
        classify(session.execute(sql, binds).map(|r| r.rows.len())),
    ));
    answers.push((
        "retained cursor",
        classify(
            session
                .open_cursor(sql, binds)
                .map(|cursor| cursor.first_page().rows.len()),
        ),
    ));
    let tx = narrowed.begin().expect("begin");
    narrowed
        .execute_in_tx(
            tx,
            "INSERT INTO members (id, membership_id, context_id, generation_id, ordinal) VALUES (99, $m, $c, $g, 99)",
            &params(vec![
                ("m", Value::Uuid(Uuid::from_u128(0x9999))),
                ("c", Value::Uuid(Uuid::from_u128(CTX_A))),
                ("g", Value::Uuid(Uuid::from_u128(GEN))),
            ]),
        )
        .expect("stage a row of the reader's own");
    answers.push((
        "transaction with staged rows",
        classify(narrowed.execute_in_tx(tx, sql, binds).map(|r| r.rows.len())),
    ));
    narrowed.rollback(tx).expect("rollback");
    answers
}

fn all_routes_answer(
    world: &World,
    sql: &str,
    binds: &HashMap<String, Value>,
    expected: Answer,
    label: &str,
) {
    for (route, answer) in every_route(world, sql, binds) {
        assert_eq!(answer, expected, "{label}: route `{route}` for `{sql}`");
    }
}

fn uuid(id: u128) -> Value {
    Value::Uuid(Uuid::from_u128(id))
}

const RESIDUAL_MISMATCH: &str =
    "SELECT id FROM members WHERE context_id = $a AND generation_id = $g AND membership_id = $m";

/// Row 1. The membership is hidden (it lives in context B) and the statement's own context filter
/// says A: the statement does not name that row, so it is hidden, never refused.
#[test]
fn a_hidden_row_the_statement_does_not_match_is_hidden_on_every_route() {
    let w = world(true);
    let binds = params(vec![("a", uuid(CTX_A)), ("g", uuid(GEN)), ("m", uuid(M_B))]);
    all_routes_answer(
        &w,
        RESIDUAL_MISMATCH,
        &binds,
        Answer::Rows(0),
        "residual-mismatching hidden row",
    );
}

/// Row 2. The statement names the hidden row completely: refused, identically on every route and
/// whichever index the planner has, including after an index is dropped under it and on a table
/// with no index on any filtered column.
#[test]
fn a_hidden_row_the_statement_names_is_refused_on_every_route_and_every_index() {
    let sql = "SELECT id FROM members WHERE context_id = $b AND generation_id = $g AND membership_id = $m";
    let binds = params(vec![("b", uuid(CTX_B)), ("g", uuid(GEN)), ("m", uuid(M_B))]);
    // Both indexes present.
    let w = world(true);
    all_routes_answer(&w, sql, &binds, Answer::ScopeViolation, "both indexes");
    // The nonunique index dropped first: the plan falls back to the unique index or a scan.
    w.db.execute("DROP INDEX members_ctx_gen ON members", &none())
        .unwrap();
    all_routes_answer(
        &w,
        sql,
        &binds,
        Answer::ScopeViolation,
        "nonunique index dropped",
    );
    // A table whose filtered columns carry no index at all except the key: build it without the
    // UNIQUE flag's index being the only access path by filtering on a non-indexed shape too.
    let bare = {
        let db = Database::open_memory();
        db.execute(
            "CREATE TABLE members (id INTEGER PRIMARY KEY, membership_id UUID NOT NULL UNIQUE, context_id UUID CONTEXT_ID NOT NULL, generation_id UUID NOT NULL, ordinal INTEGER NOT NULL)",
            &none(),
        )
        .unwrap();
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
        World { db }
    };
    all_routes_answer(
        &bare,
        sql,
        &binds,
        Answer::ScopeViolation,
        "no secondary index",
    );
    // The no-pick route: the filter names the row only through non-indexed columns' conjunction
    // with its unique key bound by parameter on a non-leading expression.
    let no_pick = "SELECT id FROM members WHERE membership_id = $m AND ordinal = 2";
    all_routes_answer(
        &bare,
        no_pick,
        &params(vec![("m", uuid(M_B))]),
        Answer::ScopeViolation,
        "unique key with a residual",
    );
}

/// Row 3. A composite primary key: the full key naming a hidden row is refused; a partial key is a
/// set read and hides the row.
#[test]
fn a_composite_primary_key_pins_only_when_every_column_is_bound() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE pairs (a INTEGER NOT NULL, b INTEGER NOT NULL, context_id UUID CONTEXT_ID NOT NULL, note TEXT, PRIMARY KEY (a, b))",
        &none(),
    )
    .unwrap();
    for (a, b, context) in [(1, 1, CTX_A), (1, 2, CTX_B)] {
        db.execute(
            "INSERT INTO pairs (a, b, context_id, note) VALUES ($a, $b, $c, 'x')",
            &params(vec![
                ("a", Value::Int64(a)),
                ("b", Value::Int64(b)),
                ("c", uuid(context)),
            ]),
        )
        .unwrap();
    }
    let w = World { db };
    all_routes_answer_pairs(
        &w,
        "SELECT note FROM pairs WHERE a = 1 AND b = 2",
        Answer::ScopeViolation,
        "full key naming a hidden row",
    );
    all_routes_answer_pairs(
        &w,
        "SELECT note FROM pairs WHERE a = 1",
        Answer::Rows(1),
        "partial key is a set read",
    );
}

fn all_routes_answer_pairs(world: &World, sql: &str, expected: Answer, label: &str) {
    let narrowed = world.db.scoped_with_contexts(contexts(&[CTX_A]));
    let session = narrowed
        .read_session(roomy())
        .expect("open a bounded read view");
    assert_eq!(
        classify(narrowed.execute(sql, &none()).map(|r| r.rows.len())),
        expected,
        "{label}: eager"
    );
    assert_eq!(
        classify(session.execute(sql, &none()).map(|r| r.rows.len())),
        expected,
        "{label}: bounded"
    );
    assert_eq!(
        classify(
            session
                .open_cursor(sql, &none())
                .map(|c| c.first_page().rows.len())
        ),
        expected,
        "{label}: retained cursor"
    );
}

/// Row 4. A NULL right-hand side is never an equality that pins a row: no refusal, and the plan
/// carries no unique-point preference.
#[test]
fn a_null_right_hand_side_never_pins_a_row() {
    let w = world(true);
    let literal = "SELECT id FROM members WHERE membership_id = NULL";
    all_routes_answer(&w, literal, &none(), Answer::Rows(0), "literal NULL");
    let parameter = "SELECT id FROM members WHERE membership_id = $p";
    all_routes_answer(
        &w,
        parameter,
        &params(vec![("p", Value::Null)]),
        Answer::Rows(0),
        "NULL parameter",
    );
    for sql in [literal, parameter] {
        let plan = w.db.explain(sql).expect("explain");
        assert!(
            !plan.contains("UNIQUE and fully bound"),
            "a NULL comparison must not get the unique-point preference: {plan}"
        );
    }
}

/// Row 5. The statement that names a unique key explains the unique index, and the work it
/// examines is one key probe however many members the generation holds.
#[test]
fn the_unique_key_is_the_chosen_plan_and_costs_one_probe_at_any_size() {
    let sql = "SELECT id FROM members WHERE context_id = $a AND generation_id = $g AND membership_id = $m";
    let mut examined = Vec::new();
    for members in [20_u128, 40, 80] {
        let w = world(true);
        for n in 0..members {
            w.db.execute(
                "INSERT INTO members (id, membership_id, context_id, generation_id, ordinal) VALUES ($id, $m, $c, $g, $o)",
                &params(vec![
                    ("id", Value::Int64(1000 + n as i64)),
                    ("m", uuid(0x5000 + n)),
                    ("c", uuid(CTX_A)),
                    ("g", uuid(GEN)),
                    ("o", Value::Int64(1000 + n as i64)),
                ]),
            )
            .unwrap();
        }
        let binds = params(vec![
            ("a", uuid(CTX_A)),
            ("g", uuid(GEN)),
            ("m", uuid(0x5000 + members / 2)),
        ]);
        let plan = w.db.explain(sql).expect("explain");
        assert!(
            plan.contains("__unique_membership_id"),
            "the unique index is the plan at {members} members: {plan}"
        );
        let answered =
            w.db.scoped_with_contexts(contexts(&[CTX_A]))
                .execute(sql, &binds)
                .unwrap();
        assert_eq!(answered.rows.len(), 1);
        examined.push(answered.trace.rows_examined);
    }
    assert!(
        examined.iter().all(|n| *n <= 2),
        "one key probe at every size: {examined:?}"
    );
    assert!(
        examined.windows(2).all(|pair| pair[0] == pair[1]),
        "work does not grow with members: {examined:?}"
    );
}

/// Continued-growth ruling: fully bound predicate writes examine the current unique entry,
/// independent of retained versions. Existing planner rows only exercise SELECT, so this row
/// covers the write route, staged replacements, and the competing conditional commit.
#[test]
fn unique_predicate_writes_keep_equal_work_across_retained_history() {
    let mut work = Vec::new();
    for versions in [8, 32] {
        let root = tempfile::tempdir().unwrap();
        let db = Database::open(root.path().join("predicate-writes.db")).unwrap();
        db.execute("CREATE TABLE items (tenant INTEGER NOT NULL, id INTEGER NOT NULL, token INTEGER NOT NULL, body INTEGER NOT NULL, PRIMARY KEY (tenant, id), UNIQUE (tenant, token))", &none()).unwrap();
        db.execute("INSERT INTO items VALUES (1, 1, 10, 0)", &none())
            .unwrap();
        for body in 1..=versions {
            db.execute(
                "UPDATE items SET body = $body WHERE tenant = 1 AND id = 1",
                &params(vec![("body", Value::Int64(body))]),
            )
            .unwrap();
        }
        let point_work = db
            .execute(
                "SELECT body FROM items WHERE tenant = 1 AND id = 1",
                &none(),
            )
            .unwrap()
            .trace
            .rows_examined;
        let tx = db.begin().unwrap();
        let first = db
            .execute_in_tx(
                tx,
                "UPDATE items SET body = 100 WHERE tenant = 1 AND id = 1",
                &none(),
            )
            .unwrap();
        assert_eq!(first.rows_affected, 1);
        let staged = db
            .execute_in_tx(
                tx,
                "UPDATE items SET body = 101 WHERE tenant = 1 AND id = 1 AND body = 100",
                &none(),
            )
            .unwrap();
        assert_eq!(
            staged.rows_affected, 1,
            "the unique route sees this transaction's replacement"
        );
        let read = db
            .execute_in_tx(
                tx,
                "SELECT body FROM items WHERE tenant = 1 AND id = 1",
                &none(),
            )
            .unwrap();
        assert_eq!(read.rows, vec![vec![Value::Int64(101)]]);
        db.commit(tx).unwrap();
        let tx = db.begin().unwrap();
        assert!(
            db.guard_row_conditions_in_tx(
                tx,
                "items",
                "id",
                &Value::Int64(1),
                &[("body".into(), Value::Int64(101))]
            )
            .unwrap()
        );
        let changed = db
            .execute_in_tx(
                tx,
                "UPDATE items SET body = 102 WHERE tenant = 1 AND id = 1 AND body = 101",
                &none(),
            )
            .unwrap();
        assert_eq!(changed.rows_affected, 1);
        db.execute(
            "UPDATE items SET body = 103 WHERE tenant = 1 AND id = 1",
            &none(),
        )
        .unwrap();
        assert!(
            db.commit(tx).is_err(),
            "a competing publication still invalidates the conditional write"
        );
        let tx = db.begin().unwrap();
        let replacement = db
            .execute_in_tx(
                tx,
                "UPDATE items SET body = 104 WHERE tenant = 1 AND id = 1",
                &none(),
            )
            .unwrap();
        assert_eq!(replacement.rows_affected, 1);
        let deleted = db
            .execute_in_tx(
                tx,
                "DELETE FROM items WHERE tenant = 1 AND token = 10 AND body = 104",
                &none(),
            )
            .unwrap();
        assert_eq!(
            deleted.rows_affected, 1,
            "the secondary unique key sees the staged replacement"
        );
        db.commit(tx).unwrap();
        assert!(
            db.execute(
                "SELECT body FROM items WHERE tenant = 1 AND id = 1",
                &none()
            )
            .unwrap()
            .rows
            .is_empty()
        );
        let observed = (
            first.trace.rows_examined,
            staged.trace.rows_examined,
            deleted.trace.rows_examined,
        );
        assert_eq!(
            observed,
            (point_work, point_work, point_work),
            "fully bound writes use the same current-entry work as a unique read at {versions} retained versions"
        );
        work.push(observed);
    }
    assert_eq!(
        work[0], work[1],
        "predicate write work must not grow with retained history"
    );
}
