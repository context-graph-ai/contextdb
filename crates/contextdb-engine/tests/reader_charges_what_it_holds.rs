//! A reader is charged for what it holds, not for what its schema could make it
//! hold. Opening a store charges the reader for the schema it holds and for the
//! structures that actually exist; an index that has stored no posting costs
//! nothing beyond its declaration.
//!
//! Each test runs in its own process: the filter count it reads is a count of
//! the whole process.

#[path = "fixtures/isolated_process.rs"]
mod isolated_process;

use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use std::collections::HashMap;

fn options(memory: u64) -> ReadSessionOptions {
    ReadSessionOptions {
        limits: ReadLimits {
            memory,
            ..ReadLimits::default()
        },
        ..ReadSessionOptions::default()
    }
}

fn execute(database: &Database, sql: &str) {
    database
        .execute(sql, &HashMap::new())
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
}

/// A reader at 6 MiB opens a store whose schema declares 120 single-column
/// indexes. Every one of them is exact-only, so the old reader charge for their
/// probe filters alone was 120 x 64 KiB = 7.5 MiB, taken whether or not a single
/// filter existed.
#[test]
fn a_reader_is_charged_for_the_exact_filters_it_holds_not_for_every_declared_index() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_reader_is_charged_for_the_exact_filters_it_holds_not_for_every_declared_index",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("reader charge directory");
    let path = root.path().join("declared.db");
    let database = Database::open(&path).expect("create the declaration-heavy store");
    for table in 0..40 {
        execute(
            &database,
            &format!("CREATE TABLE t{table} (id INTEGER PRIMARY KEY, a TEXT, b TEXT)"),
        );
        execute(
            &database,
            &format!("CREATE INDEX t{table}_a ON t{table}(a)"),
        );
        execute(
            &database,
            &format!("CREATE INDEX t{table}_b ON t{table}(b)"),
        );
    }
    database.close().expect("close the writer");

    let filters_before = contextdb_relational::store::exact_filters_allocated_for_test();
    let reader = ReadSession::open_with_options(&path, options(6 * 1024 * 1024)).expect(
        "a reader at 6 MiB opens a store declaring 120 exact-only indexes: it holds none of their filters",
    );
    assert_eq!(
        contextdb_relational::store::exact_filters_allocated_for_test(),
        filters_before,
        "opening a reader allocates no exact-probe filter"
    );
    drop(reader);
}

/// A populated exact index answers a hit, a miss and a primary-key lookup from
/// the captured source, and the reader that answered still holds no filter. The
/// miss matters: a reader that returned no matches because its bitmap is absent
/// would pass an empty-declaration check and fail here.
#[test]
fn a_populated_exact_index_answers_hits_and_misses_while_the_reader_holds_no_filter() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_populated_exact_index_answers_hits_and_misses_while_the_reader_holds_no_filter",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("reader charge directory");
    let path = root.path().join("populated.db");
    let database = Database::open(&path).expect("create the populated store");
    execute(
        &database,
        "CREATE TABLE p (id INTEGER PRIMARY KEY, tag TEXT)",
    );
    execute(&database, "CREATE INDEX p_tag ON p(tag)");
    for id in 0..50 {
        execute(&database, &format!("INSERT INTO p VALUES ({id}, 't{id}')"));
    }
    database.close().expect("close the writer");

    let filters_before = contextdb_relational::store::exact_filters_allocated_for_test();
    let reader = ReadSession::open_with_options(&path, options(6 * 1024 * 1024))
        .expect("open the populated store at 6 MiB");

    let hit = reader
        .execute("SELECT id FROM p WHERE tag = 't7'", &HashMap::new())
        .expect("a probe for a stored key");
    assert_eq!(hit.rows().len(), 1, "the stored key is found");
    assert!(matches!(hit.rows()[0][0], Value::Int64(7)));

    let miss = reader
        .execute("SELECT id FROM p WHERE tag = 'absent'", &HashMap::new())
        .expect("a probe for an absent key");
    assert_eq!(miss.rows().len(), 0, "an absent key finds nothing");

    let by_key = reader
        .execute("SELECT tag FROM p WHERE id = 8", &HashMap::new())
        .expect("a primary-key lookup");
    assert_eq!(by_key.rows().len(), 1, "the primary key is found");

    assert_eq!(
        contextdb_relational::store::exact_filters_allocated_for_test(),
        filters_before,
        "answering hits and misses builds no filter in a reader"
    );
}

/// A reader refused for the partition directory of its vector indexes is told
/// to raise the reader's memory limit, not to stop committing rows: opening a
/// store holds every index's directory in memory, and no row is being
/// committed. The limit is walked down until the open is first refused, which is
/// the last step of the open that needs memory.
#[test]
fn a_reader_refused_for_its_vector_directories_is_not_told_to_stop_committing_rows() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_reader_refused_for_its_vector_directories_is_not_told_to_stop_committing_rows",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("reader charge directory");
    let path = root.path().join("directories.db");
    let database = Database::open(&path).expect("create the partitioned vector store");
    execute(
        &database,
        "CREATE TABLE vp (id INTEGER PRIMARY KEY, context_id UUID NOT NULL, embedding VECTOR(8) PARTITION_KEY (context_id) SEARCH_MODE INDEXED IVF (LISTS = 2, PROBES = 1, REFINE_FACTOR = 4.00))",
    );
    for context in 0..150_u128 {
        database
            .execute(
                "INSERT INTO vp (id, context_id, embedding) VALUES ($id, $context, $embedding)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(context as i64)),
                    (
                        "context".to_owned(),
                        Value::Uuid(uuid::Uuid::from_u128(1000 + context)),
                    ),
                    (
                        "embedding".to_owned(),
                        Value::Vector(vec![context as f32 + 1.0; 8]),
                    ),
                ]),
            )
            .expect("insert one row into its own partition");
    }
    database.close().expect("close the writer");

    let mut limit = 2 * 1024 * 1024_u64;
    let refusal = loop {
        match ReadSession::open_with_options(&path, options(limit)) {
            Ok(session) => {
                drop(session);
                limit -= 2048;
                assert!(
                    limit > 100_000,
                    "the open is refused before the limit reaches zero"
                );
            }
            Err(error) => break error,
        }
    };
    let contextdb_core::Error::MemoryBudgetExceeded {
        operation, hint, ..
    } = &refusal
    else {
        panic!("a reader below what its open needs is refused for memory: {refusal:?}");
    };
    assert_eq!(
        operation.to_string(),
        "prepare_vector_directory_growth",
        "the last memory step of the open is the vector directory, at limit {limit}"
    );
    let hint = hint.to_string();
    assert!(
        !hint.contains("committing"),
        "a read-only open is not told to stop committing rows: {hint}"
    );
    assert!(
        hint.contains("store is open"),
        "the hint says why the directory is held: {hint}"
    );
}
