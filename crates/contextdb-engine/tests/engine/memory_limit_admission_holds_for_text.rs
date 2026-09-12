//! An operator's memory limit means what it says about text.
//!
//! `SET MEMORY_LIMIT` is a promise: the store will not hold more than this.
//! The promise is kept by an estimate of what each stored value costs, so the
//! estimate is what these pin -- admission stops at the limit rather than
//! past it, an abandoned batch gives back exactly what it took, a store
//! reopened from disk accounts for what it loads the same way it accounted
//! for writing it, and graph edges and indexed text are held to the same
//! promise as plain rows.

use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use std::collections::HashMap;

const TEXT_BYTES: usize = 4_096;
const LIMIT_BYTES: u64 = 4 * 1024 * 1024;

fn payload() -> Value {
    Value::Text("x".repeat(TEXT_BYTES))
}

fn used(database: &Database) -> u64 {
    database.accountant().usage().used as u64
}

fn seeded(database: &Database) {
    database
        .execute(
            "CREATE TABLE held (id INTEGER PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create the fixture table");
}

fn insert(database: &Database, id: i64) -> contextdb_core::Result<()> {
    database
        .execute(
            "INSERT INTO held (id, payload) VALUES ($id, $payload)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(id)),
                ("payload".to_owned(), payload()),
            ]),
        )
        .map(|_| ())
}

fn is_memory_refusal(error: &Error) -> bool {
    let rendered = error.to_string().to_lowercase();
    rendered.contains("memory")
}

#[test]
fn admission_stops_at_the_limit_and_never_holds_more_than_it() {
    let database = Database::open_memory();
    seeded(&database);
    database
        .execute("SET MEMORY_LIMIT '4M'", &HashMap::new())
        .expect("set the memory limit");

    let mut admitted = 0_i64;
    let refusal = loop {
        match insert(&database, admitted) {
            Ok(()) => {
                admitted += 1;
                assert!(
                    used(&database) <= LIMIT_BYTES,
                    "the store never holds more than the limit it was given: used {} of {}",
                    used(&database),
                    LIMIT_BYTES
                );
                assert!(
                    admitted < 10_000,
                    "a four-mebibyte limit must stop admitting four-kibibyte rows long before this"
                );
            }
            Err(error) => break error,
        }
    };
    assert!(
        is_memory_refusal(&refusal),
        "what stops the store is the memory limit, said plainly: {refusal}"
    );
    assert!(
        admitted > 0,
        "the limit is roomy enough to admit real rows before it refuses"
    );
    assert!(
        used(&database) <= LIMIT_BYTES,
        "and the refusal leaves the store inside its limit: used {} of {LIMIT_BYTES}",
        used(&database)
    );
}

#[test]
fn an_abandoned_batch_gives_back_exactly_what_it_took() {
    let database = Database::open_memory();
    seeded(&database);
    let before = used(&database);

    database
        .execute("BEGIN", &HashMap::new())
        .expect("begin the batch");
    for id in 0..64 {
        insert(&database, id).expect("stage a row");
    }
    let staged = used(&database);
    assert!(
        staged > before,
        "staging rows costs the store something: {before} then {staged}"
    );

    database
        .execute("ROLLBACK", &HashMap::new())
        .expect("abandon the batch");
    assert_eq!(
        used(&database),
        before,
        "an abandoned batch returns the store to exactly what it held before it"
    );
}

#[test]
fn a_store_reopened_from_disk_accounts_for_what_it_loads_the_way_it_accounted_for_writing_it() {
    let directory = tempfile::tempdir().expect("task-scoped store directory");
    let path = directory.path().join("reopened.db");

    let written = {
        let database = Database::open(&path).expect("open the store for writing");
        seeded(&database);
        for id in 0..64 {
            insert(&database, id).expect("write a row");
        }
        let written = used(&database);
        database.close().expect("close the store");
        written
    };

    let reopened = Database::open(&path).expect("reopen the store");
    let loaded = used(&reopened);
    assert_eq!(
        loaded, written,
        "loading a store charges what writing it charged, so a limit means the same thing across \
         a restart: wrote {written}, loaded {loaded}"
    );
    reopened.close().expect("close the reopened store");
}

#[test]
fn graph_edges_carrying_text_are_held_to_the_same_limit() {
    let database = Database::open_memory();
    database
        .execute("SET MEMORY_LIMIT '4M'", &HashMap::new())
        .expect("set the memory limit");

    let source = uuid::Uuid::new_v4();
    let text = "x".repeat(TEXT_BYTES);
    let tx = database.begin().expect("begin the edge batch");
    let mut admitted = 0_u32;
    let refusal = loop {
        match database.insert_edge(
            tx,
            source,
            uuid::Uuid::new_v4(),
            "carries".to_owned(),
            HashMap::from([("note".to_owned(), Value::Text(text.clone()))]),
        ) {
            Ok(_) => {
                admitted += 1;
                assert!(
                    used(&database) <= LIMIT_BYTES,
                    "edges are held to the limit too: used {} of {LIMIT_BYTES}",
                    used(&database)
                );
                assert!(admitted < 10_000, "a four-mebibyte limit must stop this");
            }
            Err(error) => break error,
        }
    };
    assert!(
        is_memory_refusal(&refusal),
        "an edge that would cross the limit is refused by it: {refusal}"
    );
    assert!(
        admitted > 0,
        "real edges are admitted before the limit bites"
    );
    let _ = database.rollback(tx);
}

#[test]
fn indexed_text_is_held_to_the_same_limit() {
    let database = Database::open_memory();
    seeded(&database);
    database
        .execute(
            "CREATE INDEX held_payload ON held (payload)",
            &HashMap::new(),
        )
        .expect("create the text index");
    database
        .execute("SET MEMORY_LIMIT '4M'", &HashMap::new())
        .expect("set the memory limit");

    let mut admitted = 0_i64;
    let refusal = loop {
        match insert(&database, admitted) {
            Ok(()) => {
                admitted += 1;
                assert!(
                    used(&database) <= LIMIT_BYTES,
                    "indexed text is held to the limit: used {} of {LIMIT_BYTES}",
                    used(&database)
                );
                assert!(admitted < 10_000, "a four-mebibyte limit must stop this");
            }
            Err(error) => break error,
        }
    };
    assert!(
        is_memory_refusal(&refusal),
        "indexed rows are refused by the memory limit like any other: {refusal}"
    );
    assert!(admitted > 0);
}
