//! A bounded neighbour search inside an open transaction scores the vectors
//! that transaction can see.
//!
//! A writing session that has changed a document's vector, or removed a
//! document, is entitled to read its own work back before it commits -- that
//! is what lets a multi-step write be reviewed while it is still
//! abandonable. A neighbour search is the sharpest case, because the answer
//! is not "which rows match" but "which row is closest", so a door reading
//! the pre-transaction vector does not return fewer rows or later rows, it
//! returns a DIFFERENT document and looks entirely correct doing it.
//!
//! The oracle is the executor's own answer inside the same transaction. The
//! query vector is read from a stored row, so the staged change reaches the
//! answer through the vector being scored as well as through the vectors
//! being scored against.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::{Database, QueryResult};
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
    Uuid::from_u128(0x007C_0000_0000_0000_0000_0000_0000_0000 + ordinal)
}

const SOURCE: u128 = 1;
const NEAR: u128 = 2;
const MID: u128 = 3;
const FAR: u128 = 4;

fn vector_of(ordinal: u128) -> Vec<f32> {
    match ordinal {
        SOURCE => vec![1.0, 0.0, 0.0],
        NEAR => vec![0.95, 0.05, 0.0],
        MID => vec![0.5, 0.5, 0.0],
        FAR => vec![0.0, 0.0, 1.0],
        other => panic!("no vector for document {other}"),
    }
}

/// Four documents whose distances from the source are clearly ordered, so a
/// door reading the wrong vector names a different document rather than
/// re-ordering ties.
fn seeded() -> Database {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE docs (id UUID PRIMARY KEY, label TEXT, embedding VECTOR(3))",
            &empty(),
        )
        .expect("create the document table");
    for (ordinal, label) in [
        (SOURCE, "source"),
        (NEAR, "near"),
        (MID, "mid"),
        (FAR, "far"),
    ] {
        database
            .execute(
                "INSERT INTO docs (id, label, embedding) VALUES ($id, $label, $embedding)",
                &params(vec![
                    ("id", Value::Uuid(doc(ordinal))),
                    ("label", Value::Text(label.to_owned())),
                    ("embedding", Value::Vector(vector_of(ordinal))),
                ]),
            )
            .expect("insert a document");
    }
    database
}

/// The nearest neighbour of the source, with the query vector read from the
/// source's own stored row.
const NEAREST: &str = "SELECT label FROM docs WHERE id != $source \
                       ORDER BY embedding <=> ROW_VECTOR('docs','embedding',$source) LIMIT 1";

fn source_params() -> HashMap<String, Value> {
    params(vec![("source", Value::Uuid(doc(SOURCE)))])
}

fn answer(result: &QueryResult) -> String {
    format!("{:?}", result.rows)
}

fn eager(database: &Database) -> QueryResult {
    database
        .execute(NEAREST, &source_params())
        .expect("the executor answers the neighbour search")
}

fn bounded(database: &Database) -> QueryResult {
    database
        .read_session(roomy())
        .expect("open a bounded read view")
        .execute(NEAREST, &source_params())
        .expect("a bounded read answers the neighbour search")
}

#[test]
fn a_bounded_neighbour_search_scores_the_vector_this_transaction_staged() {
    let database = seeded();
    let committed = answer(&eager(&database));

    database
        .execute("BEGIN", &empty())
        .expect("open the session transaction");
    // Move the SOURCE's own vector to where the far document sits, so the
    // nearest neighbour of the source is now the far one.
    database
        .execute(
            "UPDATE docs SET embedding = $embedding WHERE id = $id",
            &params(vec![
                ("id", Value::Uuid(doc(SOURCE))),
                ("embedding", Value::Vector(vector_of(FAR))),
            ]),
        )
        .expect("stage a new vector for the source document");

    let eager_in_tx = eager(&database);
    let bounded_in_tx = bounded(&database);
    println!(
        "OBSERVED a staged vector: committed {committed} | executor {} | bounded {}",
        answer(&eager_in_tx),
        answer(&bounded_in_tx)
    );

    assert_ne!(
        answer(&eager_in_tx),
        committed,
        "the fixture is only adversarial if the staged vector changes the nearest neighbour"
    );
    assert_eq!(
        answer(&bounded_in_tx),
        answer(&eager_in_tx),
        "a bounded neighbour search inside an open transaction scores the vector that \
         transaction staged: the executor names {} and the bounded read names {}",
        answer(&eager_in_tx),
        answer(&bounded_in_tx)
    );

    database
        .execute("ROLLBACK", &empty())
        .expect("abandon the transaction");
}

#[test]
fn a_bounded_neighbour_search_does_not_score_a_document_this_transaction_removed() {
    let database = seeded();
    let committed = answer(&eager(&database));

    database
        .execute("BEGIN", &empty())
        .expect("open the session transaction");
    database
        .execute(
            "DELETE FROM docs WHERE id = $id",
            &params(vec![("id", Value::Uuid(doc(NEAR)))]),
        )
        .expect("stage the removal of the nearest document");

    let eager_in_tx = eager(&database);
    let bounded_in_tx = bounded(&database);
    println!(
        "OBSERVED a staged removal: committed {committed} | executor {} | bounded {}",
        answer(&eager_in_tx),
        answer(&bounded_in_tx)
    );

    assert_ne!(
        answer(&eager_in_tx),
        committed,
        "the fixture removes the document that was nearest, so the answer has to move"
    );
    assert_eq!(
        answer(&bounded_in_tx),
        answer(&eager_in_tx),
        "a document this transaction removed is not scored by its own bounded read: the executor \
         names {} and the bounded read names {}",
        answer(&eager_in_tx),
        answer(&bounded_in_tx)
    );

    database
        .execute("ROLLBACK", &empty())
        .expect("abandon the transaction");
}

#[test]
fn an_abandoned_transaction_leaves_the_neighbour_search_where_it_started() {
    let database = seeded();
    let committed = answer(&eager(&database));

    database
        .execute("BEGIN", &empty())
        .expect("open the session transaction");
    database
        .execute(
            "UPDATE docs SET embedding = $embedding WHERE id = $id",
            &params(vec![
                ("id", Value::Uuid(doc(SOURCE))),
                ("embedding", Value::Vector(vector_of(FAR))),
            ]),
        )
        .expect("stage a new vector for the source document");
    database
        .execute(
            "DELETE FROM docs WHERE id = $id",
            &params(vec![("id", Value::Uuid(doc(MID)))]),
        )
        .expect("stage a removal too");
    database
        .execute("ROLLBACK", &empty())
        .expect("abandon the transaction");

    let eager_after = answer(&eager(&database));
    let bounded_after = answer(&bounded(&database));
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
        "a bounded neighbour search remembers none of an abandoned transaction: it names \
         {bounded_after} where the committed documents name {committed}"
    );
}
