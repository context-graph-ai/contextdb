//! A reading session runs every statement the engine classifies as a read,
//! and explains the ones it will not run.
//!
//! Two things used to be refused with the same words -- "bounded execution
//! accepts read-only SELECT plans" -- and neither refusal was right. The four
//! statements a store answers ABOUT ITSELF (`SHOW MEMORY_LIMIT`,
//! `SHOW DISK_LIMIT`, `SHOW SYNC_CONFLICT_POLICY`, `SHOW VECTOR_INDEXES`) are
//! classified reads, so a reading session has to be able to run them; they
//! simply are not SELECTs. And explaining a WRITE is not writing: planning
//! reads schema, chooses a strategy, and changes nothing, so `.explain DELETE`
//! must answer what the statement WOULD do rather than refuse.

#![cfg(all(unix, feature = "test-seams"))]

use contextdb_core::Value;
use contextdb_core::read_contract::{OwnerReadLimits, ReadLimits, ReadRoute};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MetadataBody, MetadataRequest, OwnerReadConfig, QueryResult,
    ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

/// Every statement `statement_effect` calls a read that is not a `SELECT`.
const READ_CLASSIFIED_NON_SELECT: [&str; 4] = [
    "SHOW MEMORY_LIMIT",
    "SHOW DISK_LIMIT",
    "SHOW SYNC_CONFLICT_POLICY",
    "SHOW VECTOR_INDEXES",
];

fn secure_runtime_root(directory: &Path, name: &str) -> PathBuf {
    let root = directory.join(name);
    std::fs::create_dir(&root).expect("create the task-scoped owner runtime root");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the task-scoped owner runtime root");
    root
}

fn seed(database: &Database) {
    for statement in [
        "CREATE TABLE documents (id INTEGER PRIMARY KEY, body TEXT)",
        "INSERT INTO documents (id, body) VALUES (1, 'kept')",
        "INSERT INTO documents (id, body) VALUES (2, 'also kept')",
    ] {
        database
            .execute(statement, &HashMap::new())
            .unwrap_or_else(|error| panic!("seed `{statement}`: {error}"));
    }
}

fn idle_store(directory: &Path) -> PathBuf {
    let path = directory.join("classified.db");
    let database = Database::open(&path).expect("open the fixture writer");
    seed(&database);
    database.close().expect("the fixture writer closes cleanly");
    path
}

fn served_store(directory: &Path, runtime_root: PathBuf) -> (Database, PathBuf) {
    let path = directory.join("served.db");
    let database = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime_root),
                limits: OwnerReadLimits {
                    limits: ReadLimits::default(),
                    concurrency: 4,
                },
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open a writer that serves owner reads");
    seed(&database);
    (database, path)
}

fn document_rows(answered: &QueryResult) -> usize {
    answered.rows.len()
}

fn explained(body: &MetadataBody) -> (String, Option<String>) {
    match body {
        MetadataBody::Explain {
            physical_plan,
            index,
            ..
        } => (physical_plan.clone(), index.clone()),
        other => panic!("asked for an explained statement and got {other:?}"),
    }
}

#[test]
fn the_idle_file_runs_every_read_classified_statement() {
    let directory = tempfile::TempDir::new().expect("task-scoped store directory");
    let path = idle_store(directory.path());
    let session = ReadSession::open(&path).expect("open the idle store");
    assert_eq!(session.route(), ReadRoute::File);

    for statement in READ_CLASSIFIED_NON_SELECT {
        let answered = session
            .execute(statement, &HashMap::new())
            .unwrap_or_else(|error| {
                panic!("`{statement}` is classified a read, so a reading session runs it: {error}")
            });
        assert!(
            !answered.columns.is_empty(),
            "`{statement}` answers with named columns: {answered:?}"
        );
    }
}

#[test]
fn a_live_owner_runs_every_read_classified_statement_over_its_channel() {
    let directory = tempfile::TempDir::new().expect("task-scoped store directory");
    let runtime_root = secure_runtime_root(directory.path(), "classified-runtime");
    let (database, path) = served_store(directory.path(), runtime_root.clone());

    let session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_options(&path, ReadSessionOptions::default())
    })
    .expect("a live owner is reachable");
    assert_eq!(session.route(), ReadRoute::Owner);

    for statement in READ_CLASSIFIED_NON_SELECT {
        let answered = session
            .execute(statement, &HashMap::new())
            .unwrap_or_else(|error| {
                panic!("`{statement}` runs over the owner channel too: {error}")
            });
        assert!(
            !answered.columns.is_empty(),
            "`{statement}` answers with named columns over the channel: {answered:?}"
        );
    }

    drop(session);
    database.close().expect("the writer closes cleanly");
}

#[test]
fn explaining_a_write_answers_its_plan_and_leaves_the_rows_alone() {
    let directory = tempfile::tempdir().expect("task-scoped store directory");
    let path = idle_store(directory.path());
    let session = ReadSession::open(&path).expect("open the idle store");

    for (statement, expected) in [
        (
            "DELETE FROM documents WHERE id = 1",
            "Delete(table=documents)",
        ),
        (
            "UPDATE documents SET body = 'changed' WHERE id = 1",
            "Update(table=documents)",
        ),
        (
            "INSERT INTO documents (id, body) VALUES (3, 'new')",
            "Insert(table=documents)",
        ),
    ] {
        let answer = session
            .metadata(
                MetadataRequest::Explain {
                    sql: statement.to_owned(),
                },
                None,
            )
            .unwrap_or_else(|error| {
                panic!("explaining `{statement}` answers its plan rather than refusing: {error}")
            });
        let (plan, index) = explained(&answer.body);
        assert!(
            plan.contains(expected),
            "explaining `{statement}` names the plan the engine chose: {plan}"
        );
        assert!(
            index.is_none(),
            "nothing ran, so no index was picked and none is reported: {index:?}"
        );
    }

    // The whole point: none of it happened.
    let still_there = session
        .execute("SELECT id FROM documents ORDER BY id", &HashMap::new())
        .expect("read the table back");
    assert_eq!(
        document_rows(&still_there),
        2,
        "explaining three writes changed nothing: {:?}",
        still_there.rows
    );
    assert_eq!(
        still_there.rows.first().and_then(|row| row.first()),
        Some(&Value::Int64(1)),
        "the row the explained DELETE named is still there: {:?}",
        still_there.rows
    );
}

#[test]
fn a_live_owner_explains_a_write_over_its_channel_and_leaves_the_rows_alone() {
    let directory = tempfile::TempDir::new().expect("task-scoped store directory");
    let runtime_root = secure_runtime_root(directory.path(), "explain-write-runtime");
    let (database, path) = served_store(directory.path(), runtime_root.clone());

    let session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_options(&path, ReadSessionOptions::default())
    })
    .expect("a live owner is reachable");
    assert_eq!(session.route(), ReadRoute::Owner);

    let answer = session
        .metadata(
            MetadataRequest::Explain {
                sql: "DELETE FROM documents WHERE id = 1".to_owned(),
            },
            None,
        )
        .expect("the owner explains a write rather than refusing it as one");
    let (plan, index) = explained(&answer.body);
    assert!(
        plan.contains("Delete(table=documents)"),
        "the owner names the plan the engine chose: {plan}"
    );
    assert!(index.is_none());

    let still_there = session
        .execute("SELECT id FROM documents ORDER BY id", &HashMap::new())
        .expect("read the table back over the channel");
    assert_eq!(
        document_rows(&still_there),
        2,
        "explaining a write over the channel changed nothing: {:?}",
        still_there.rows
    );

    drop(session);
    database.close().expect("the writer closes cleanly");
}
