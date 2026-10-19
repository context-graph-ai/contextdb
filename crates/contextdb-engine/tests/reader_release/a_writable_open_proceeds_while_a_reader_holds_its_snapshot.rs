//! A writable open proceeds while a direct reader holds its snapshot, on every
//! platform the engine supports.
//!
//! Promise: a reader that has its snapshot never makes a writer's open fail.
//! The writer reads the reader's registration to know what to keep, and must
//! be able to tell a live reader from a dead one. Where it cannot -- the
//! platform's check answering "not this reader" for every reader -- the
//! registration looks unsafe and the writer's open was refused with an I/O
//! error while a perfectly healthy reader was reading.

use crate::writer_acquisition_support::{SEEDED_ROWS, count_rows, seeded_store};
use contextdb_core::Value;
use contextdb_engine::{Database, ReadSession};
use std::collections::HashMap;

/// Test-estate reason (2026-10-16): a healthy direct reader holding its snapshot never makes a
/// writable open fail; the writer commits and the reader keeps reading what it opened. Before the
/// fix, a non-Linux platform could not confirm any reader's descriptor, so the writer judged the
/// registry indeterminate and refused its own open with an I/O error.
#[test]
fn a_writable_open_proceeds_while_a_healthy_reader_holds_its_snapshot() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "held-by-a-reader.db");
    let session = ReadSession::open(&path).expect("a direct reader takes its snapshot");
    assert_eq!(count_rows(&session), SEEDED_ROWS);

    let writer = Database::open(&path)
        .expect("a healthy reader holding its snapshot never makes a writable open fail");
    writer
        .execute(
            "INSERT INTO held (id, body) VALUES ($id, 'after')",
            &HashMap::from([("id".to_owned(), Value::Int64(SEEDED_ROWS))]),
        )
        .expect("the writer commits while the reader holds its snapshot");

    assert_eq!(
        count_rows(&session),
        SEEDED_ROWS,
        "the reader keeps the snapshot it opened"
    );
    drop(session);
    writer.close().expect("close");
}
