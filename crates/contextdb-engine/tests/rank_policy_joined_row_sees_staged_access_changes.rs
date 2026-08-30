#![cfg(feature = "test-seams")]
//! A ranked read decides the joined row's access under the reader's own open
//! transaction.
//!
//! A rank policy scores each candidate from a row in a joined table, and that
//! joined table can be access-controlled: a principal that holds no grant for
//! a joined row must not be scored from it. Grants are ordinary rows, so a
//! reader can stage one -- or stage its removal -- inside a transaction it has
//! open, and every other read that reader makes in that transaction already
//! sees what it staged.
//!
//! A ranked read that decides joined-row access against committed state alone
//! contradicts the reader's own transaction: a grant the reader just staged
//! does not admit the row, and a grant it just removed still does. The reader
//! is left unable to check the effect of an entitlement change before it
//! commits one -- which is the one moment checking is worth anything.
//!
//! So the joined row's access is decided under the same transaction the rest
//! of the read runs in.

use contextdb_core::read_contract::ReadLimits;
use contextdb_core::{Principal, Value};
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

/// The principal every read below is made as.
const READER: &str = "agent-a";

/// The two access lists the joined outcome rows carry.
fn favoured_acl() -> Uuid {
    Uuid::from_u128(0xAC1)
}
fn baseline_acl() -> Uuid {
    Uuid::from_u128(0xAC2)
}

/// The two ranked anchors. The favoured one outranks the baseline one only
/// while its joined outcome row can be reached.
fn favoured_decision() -> Uuid {
    Uuid::from_u128(0xDEC1)
}
fn baseline_decision() -> Uuid {
    Uuid::from_u128(0xDEC2)
}

const RANKED_SQL: &str = "SELECT id FROM ranked_decisions ORDER BY embedding <=> $query \
                          USE RANK outcome_weighted LIMIT 2";

/// Ceilings far above anything these reads need, so a ceiling can never be
/// what changes an answer.
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

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn unit_vector(seed: u64) -> Vec<f32> {
    let slope = 1.0 / (seed.saturating_add(1) as f32);
    let norm = (1.0 + slope * slope).sqrt();
    vec![1.0 / norm, slope / norm]
}

/// A ranked table whose joined outcome table is access-controlled, seeded so
/// that the favoured anchor outranks the baseline one exactly when its own
/// joined outcome row is reachable.
fn ranked_store_behind_a_grant() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, \
         principal_id TEXT, acl_id UUID)",
        &empty(),
    )
    .expect("create the grant table");
    db.execute(
        "CREATE TABLE ranked_outcomes (id UUID PRIMARY KEY, decision_id UUID, weight REAL, \
         acl_id UUID ACL REFERENCES acl_grants(acl_id))",
        &empty(),
    )
    .expect("create the access-controlled joined outcome table");
    db.execute(
        "CREATE INDEX ranked_outcomes_decision_idx ON ranked_outcomes(decision_id)",
        &empty(),
    )
    .expect("declare the joined index the rank policy relies on");
    db.execute(
        "CREATE TABLE ranked_decisions (
            id UUID PRIMARY KEY,
            embedding VECTOR(2) RANK_POLICY (
                JOIN ranked_outcomes ON decision_id,
                FORMULA 'coalesce({weight}, 0.0)',
                SORT_KEY outcome_weighted
            )
        )",
        &empty(),
    )
    .expect("create the ranked decision table");

    for (ordinal, (decision, acl, weight)) in [
        (favoured_decision(), favoured_acl(), 0.9f64),
        (baseline_decision(), baseline_acl(), 0.1f64),
    ]
    .into_iter()
    .enumerate()
    {
        db.execute(
            "INSERT INTO ranked_decisions (id, embedding) VALUES ($id, $embedding)",
            &params(vec![
                ("id", Value::Uuid(decision)),
                ("embedding", Value::Vector(unit_vector(ordinal as u64))),
            ]),
        )
        .expect("store a ranked decision row");
        db.execute(
            "INSERT INTO ranked_outcomes (id, decision_id, weight, acl_id) \
             VALUES ($id, $decision, $weight, $acl)",
            &params(vec![
                (
                    "id",
                    Value::Uuid(Uuid::from_u128(0xDEC3_0000 + ordinal as u128)),
                ),
                ("decision", Value::Uuid(decision)),
                ("weight", Value::Float64(weight)),
                ("acl", Value::Uuid(acl)),
            ]),
        )
        .expect("store the joined outcome row for a ranked decision");
    }
    db
}

/// Give the reading principal one access list, through the handle `db`.
fn grant(db: &Database, ordinal: u128, acl: Uuid) {
    db.execute(
        "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) \
         VALUES ($id, 'Agent', $principal, $acl)",
        &params(vec![
            ("id", Value::Uuid(Uuid::from_u128(0xAA00 + ordinal))),
            ("principal", Value::Text(READER.to_owned())),
            ("acl", Value::Uuid(acl)),
        ]),
    )
    .expect("grant the reading principal one access list");
}

/// How many grants for `acl` this handle can see right now.
fn visible_grants(db: &Database, acl: Uuid) -> usize {
    db.execute(
        "SELECT id FROM acl_grants WHERE acl_id = $acl",
        &params(vec![("acl", Value::Uuid(acl))]),
    )
    .expect("read the grants this handle can see")
    .rows
    .len()
}

fn ranked_query() -> HashMap<String, Value> {
    params(vec![("query", Value::Vector(unit_vector(0)))])
}

fn ids(result: &contextdb_engine::QueryResult) -> Vec<Uuid> {
    result
        .rows
        .iter()
        .map(|row| match row.first() {
            Some(Value::Uuid(id)) => *id,
            other => panic!("expected an id-leading ranked row, got {other:?}"),
        })
        .collect()
}

/// The ranked order a bounded read on `reader` answers with.
fn ranked_order(reader: &Database) -> Vec<Uuid> {
    ids(&reader
        .read_session(roomy())
        .expect("open a bounded read view")
        .execute(RANKED_SQL, &ranked_query())
        .expect("a ranked read must be served"))
}

/// The ranked order the executor answers the same statement with, on the same
/// handle and in whatever transaction it has open. It is reported alongside
/// every failure below so a disagreement between the two doors is visible in
/// the failure itself rather than needing a second run to find.
fn executor_order(reader: &Database) -> Vec<Uuid> {
    ids(&reader
        .execute(RANKED_SQL, &ranked_query())
        .expect("the executor answers the same ranked read"))
}

#[test]
fn a_grant_staged_in_the_readers_transaction_admits_the_joined_row_it_names() {
    let db = ranked_store_behind_a_grant();
    grant(&db, 2, baseline_acl());
    let reader = db.scoped_with_constraints(None, None, Some(Principal::Agent(READER.to_owned())));

    assert_eq!(
        ranked_order(&reader),
        vec![baseline_decision(), favoured_decision()],
        "before anything is staged, the favoured anchor scores nothing: the principal holds no \
         grant for its joined outcome row"
    );

    reader
        .execute("BEGIN", &empty())
        .expect("begin a transaction on the reading handle");
    grant(&reader, 1, favoured_acl());
    assert_eq!(
        visible_grants(&reader, favoured_acl()),
        1,
        "the fixture is only about a staged grant if this handle can see what it staged"
    );

    let staged = ranked_order(&reader);
    let executor = executor_order(&reader);
    reader
        .execute("ROLLBACK", &empty())
        .expect("discard the staged grant");

    assert_eq!(
        staged,
        vec![favoured_decision(), baseline_decision()],
        "a grant this reader staged admits the joined outcome row it names, so the favoured \
         anchor is scored from it and outranks the baseline one; the executor answered the \
         same statement on the same handle with {executor:?}"
    );
}

#[test]
fn a_grant_removed_in_the_readers_transaction_withholds_the_joined_row_it_named() {
    let db = ranked_store_behind_a_grant();
    grant(&db, 1, favoured_acl());
    grant(&db, 2, baseline_acl());
    let reader = db.scoped_with_constraints(None, None, Some(Principal::Agent(READER.to_owned())));

    assert_eq!(
        ranked_order(&reader),
        vec![favoured_decision(), baseline_decision()],
        "with both grants committed, the favoured anchor is scored from its joined outcome row"
    );

    reader
        .execute("BEGIN", &empty())
        .expect("begin a transaction on the reading handle");
    reader
        .execute(
            "DELETE FROM acl_grants WHERE acl_id = $acl",
            &params(vec![("acl", Value::Uuid(favoured_acl()))]),
        )
        .expect("stage the removal of the favoured grant");
    assert_eq!(
        visible_grants(&reader, favoured_acl()),
        0,
        "the fixture is only about a staged removal if this handle can see what it staged"
    );

    let staged = ranked_order(&reader);
    let executor = executor_order(&reader);
    reader
        .execute("ROLLBACK", &empty())
        .expect("restore the removed grant");

    assert_eq!(
        staged,
        vec![baseline_decision(), favoured_decision()],
        "a grant this reader removed no longer admits the joined outcome row it named, so the \
         favoured anchor is no longer scored from it; the executor answered the same statement \
         on the same handle with {executor:?}"
    );

    assert_eq!(
        ranked_order(&reader),
        vec![favoured_decision(), baseline_decision()],
        "the rolled-back removal leaves the committed grant standing"
    );
}
