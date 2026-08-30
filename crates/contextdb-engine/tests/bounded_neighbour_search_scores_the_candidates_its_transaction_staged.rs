//! A bounded neighbour search scores the documents its own transaction has
//! changed.
//!
//! A writing session that has moved a document's vector, added one, or
//! removed one is entitled to read its own work back before it commits. A
//! neighbour search is where getting that wrong is hardest to notice: the
//! answer is not "which rows match" but "which document is closest", so a
//! door reading the pre-transaction vectors returns a well-formed answer
//! naming the wrong document, and nothing about it looks wrong.
//!
//! The last arm is the other half of the same promise. When the document a
//! search is measuring FROM has had its vector cleared in this transaction,
//! there is no query vector to search with, and the store says so rather than
//! quietly falling back on the committed vector -- because falling back would
//! answer a question nobody asked, using a value the transaction has removed.
//!
//! The oracle throughout is the executor's own answer inside the same
//! transaction.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::{Database, QueryResult};
use serial_test::serial;
use std::collections::HashMap;
use uuid::Uuid;

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
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

fn doc(ordinal: u128) -> Uuid {
    Uuid::from_u128(0x008D_0000_0000_0000_0000_0000_0000_0000 + ordinal)
}

const SOURCE: u128 = 1;
const NEAR: u128 = 2;
const MID: u128 = 3;
const FAR: u128 = 4;
const NEWCOMER: u128 = 5;

fn vector_of(ordinal: u128) -> Vec<f32> {
    match ordinal {
        SOURCE => vec![1.0, 0.0, 0.0],
        NEAR => vec![0.95, 0.05, 0.0],
        MID => vec![0.5, 0.5, 0.0],
        FAR => vec![0.0, 0.0, 1.0],
        NEWCOMER => vec![0.99, 0.01, 0.0],
        other => panic!("no vector for document {other}"),
    }
}

fn label_of(ordinal: u128) -> &'static str {
    match ordinal {
        SOURCE => "source",
        NEAR => "near",
        MID => "mid",
        FAR => "far",
        NEWCOMER => "newcomer",
        other => panic!("no label for document {other}"),
    }
}

/// Documents whose distances from the source are clearly ordered, so a door
/// scoring the wrong candidates names a different document rather than
/// re-ordering ties.
fn seeded() -> Database {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE docs (id UUID PRIMARY KEY, label TEXT, embedding VECTOR(3))",
            &empty(),
        )
        .expect("create the document table");
    for ordinal in [SOURCE, NEAR, MID, FAR] {
        database
            .execute(
                "INSERT INTO docs (id, label, embedding) VALUES ($id, $label, $embedding)",
                &params(vec![
                    ("id", Value::Uuid(doc(ordinal))),
                    ("label", Value::Text(label_of(ordinal).to_owned())),
                    ("embedding", Value::Vector(vector_of(ordinal))),
                ]),
            )
            .expect("insert a document");
    }
    database
}

const NEAREST: &str = "SELECT label FROM docs WHERE id != $source \
                       ORDER BY embedding <=> ROW_VECTOR('docs','embedding',$source) LIMIT 1";

fn source_params() -> HashMap<String, Value> {
    params(vec![("source", Value::Uuid(doc(SOURCE)))])
}

fn answer(result: &QueryResult) -> String {
    format!("{:?}", result.rows)
}

fn eager(database: &Database) -> contextdb_core::Result<QueryResult> {
    database.execute(NEAREST, &source_params())
}

fn bounded(database: &Database) -> contextdb_core::Result<QueryResult> {
    database
        .read_session(roomy())
        .expect("open a bounded read view")
        .execute(NEAREST, &source_params())
}

/// Stage something, then hold the bounded answer to the executor's answer
/// inside the same transaction. `expect_change` guards the fixture: an arm
/// that does not move the answer proves nothing.
fn staged_arm(what: &str, stage: impl Fn(&Database), disagreements: &mut Vec<String>) {
    let database = seeded();
    let committed = answer(&eager(&database).expect("the executor answers before the transaction"));

    database
        .execute("BEGIN", &empty())
        .expect("open the session transaction");
    stage(&database);

    let eager_in_tx = eager(&database).unwrap_or_else(|error| {
        panic!("{what}: the executor answers inside the transaction: {error}")
    });
    let bounded_in_tx = bounded(&database);
    println!(
        "OBSERVED {what}: committed {committed} | executor {} | bounded {}",
        answer(&eager_in_tx),
        match &bounded_in_tx {
            Ok(bounded) => answer(bounded),
            Err(error) => format!("refused: {error}"),
        }
    );

    assert_ne!(
        answer(&eager_in_tx),
        committed,
        "{what}: the fixture is only adversarial if what it stages changes the answer"
    );
    match bounded_in_tx {
        Ok(bounded) => {
            if answer(&bounded) != answer(&eager_in_tx) {
                disagreements.push(format!(
                    "{what}: the executor names {} and a bounded read names {}",
                    answer(&eager_in_tx),
                    answer(&bounded)
                ));
            }
        }
        Err(error) => disagreements.push(format!(
            "{what}: the executor names {} and a bounded read refuses: {error}",
            answer(&eager_in_tx)
        )),
    }
    database
        .execute("ROLLBACK", &empty())
        .expect("abandon the transaction");
}

fn move_vector(database: &Database, which: u128, to: u128) {
    database
        .execute(
            "UPDATE docs SET embedding = $embedding WHERE id = $id",
            &params(vec![
                ("id", Value::Uuid(doc(which))),
                ("embedding", Value::Vector(vector_of(to))),
            ]),
        )
        .expect("stage a new vector for a document");
}

// Each arm builds its own store, and yet what this file measures came out
// differently when the harness ran these tests beside each other. Until that
// is understood, the file runs its arms one at a time so it measures the
// thing it is about rather than whatever else was in flight.
#[test]
#[serial]
fn a_bounded_neighbour_search_scores_what_this_transaction_staged() {
    let mut disagreements = Vec::new();

    staged_arm(
        "a candidate moved further away than the one behind it",
        |database| move_vector(database, NEAR, FAR),
        &mut disagreements,
    );
    staged_arm(
        "a candidate added closer than any that was there",
        |database| {
            database
                .execute(
                    "INSERT INTO docs (id, label, embedding) VALUES ($id, $label, $embedding)",
                    &params(vec![
                        ("id", Value::Uuid(doc(NEWCOMER))),
                        ("label", Value::Text(label_of(NEWCOMER).to_owned())),
                        ("embedding", Value::Vector(vector_of(NEWCOMER))),
                    ]),
                )
                .expect("stage a new document");
        },
        &mut disagreements,
    );
    staged_arm(
        "the nearest candidate removed",
        |database| {
            database
                .execute(
                    "DELETE FROM docs WHERE id = $id",
                    &params(vec![("id", Value::Uuid(doc(NEAR)))]),
                )
                .expect("stage the removal of the nearest document");
        },
        &mut disagreements,
    );
    staged_arm(
        "the document being measured from moved onto another",
        |database| move_vector(database, SOURCE, FAR),
        &mut disagreements,
    );

    assert!(
        disagreements.is_empty(),
        "a neighbour search that scores the wrong candidates does not return fewer rows, it \
         names a different document and looks entirely correct doing it:\n{}",
        disagreements.join("\n")
    );
}

#[test]
#[serial]
fn clearing_the_vector_a_search_measures_from_fails_the_same_way_through_both_doors() {
    let database = seeded();
    database
        .execute("BEGIN", &empty())
        .expect("open the session transaction");
    database
        .execute(
            "UPDATE docs SET embedding = NULL WHERE id = $id",
            &params(vec![("id", Value::Uuid(doc(SOURCE)))]),
        )
        .expect("stage the removal of the source document's vector");

    let eager_refusal = eager(&database)
        .expect_err("a search has no vector to measure from once this transaction cleared it");
    let bounded_outcome = bounded(&database);
    println!(
        "OBSERVED a cleared query vector: executor {eager_refusal:?} | bounded {bounded_outcome:?}"
    );

    assert!(
        matches!(
            eager_refusal,
            contextdb_core::Error::PersistedRowVectorCellNull { .. }
        ),
        "the executor says the cell it was told to read is empty rather than reading the vector \
         this transaction removed: {eager_refusal:?}"
    );
    match bounded_outcome {
        Ok(answered) => panic!(
            "a bounded read answered {:?} from a vector this transaction cleared -- falling back \
             on the committed vector answers a question nobody asked",
            answered.rows
        ),
        Err(bounded_refusal) => assert_eq!(
            format!("{bounded_refusal:?}"),
            format!("{eager_refusal:?}"),
            "both doors refuse the same way"
        ),
    }

    database
        .execute("ROLLBACK", &empty())
        .expect("abandon the transaction");
}

#[test]
#[serial]
fn an_abandoned_transaction_leaves_the_neighbour_search_where_it_started() {
    let database = seeded();
    let committed = answer(&eager(&database).expect("the executor answers before the transaction"));

    database
        .execute("BEGIN", &empty())
        .expect("open the session transaction");
    move_vector(&database, NEAR, FAR);
    database
        .execute(
            "DELETE FROM docs WHERE id = $id",
            &params(vec![("id", Value::Uuid(doc(MID)))]),
        )
        .expect("stage a removal too");
    database
        .execute("ROLLBACK", &empty())
        .expect("abandon the transaction");

    let eager_after = answer(&eager(&database).expect("the executor answers after the rollback"));
    let bounded_after =
        answer(&bounded(&database).expect("a bounded read answers after the rollback"));
    println!(
        "OBSERVED after the abandoned transaction: committed {committed} | executor \
         {eager_after} | bounded {bounded_after}"
    );

    assert_eq!(
        eager_after, committed,
        "abandoning the transaction puts the committed vectors back"
    );
    assert_eq!(
        bounded_after, committed,
        "a bounded neighbour search remembers none of an abandoned transaction"
    );
}
