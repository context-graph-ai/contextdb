//! A read session spans writer reopens of the same store: a session opened
//! beside a writer keeps reading committed rows through every later close and
//! reopen, and the roots its cursors retain stay protected from every later
//! writer until the session lets them go.
//!
//! Each reopen is driven through the writer's startup capture of the reader
//! registry, which reports exactly once per writer start which reader roots
//! that writer must keep. Nothing here waits on time: every step is an
//! ordinary synchronous open, close, commit or fetch.

use contextdb_core::Value;
use contextdb_core::read_contract::{ReadLimits, ReadRoute};
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    WriterReaderSnapshotRegistryObservation, observe_writer_reader_snapshots_for_test,
};
use contextdb_engine::{Database, ReadCursor, ReadSession, ReadSessionOptions};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex};

const PAGE_ROWS: usize = 2;

/// Open the writer and return the reader registry its startup captured.
fn reopen_writer(path: &Path) -> (Database, WriterReaderSnapshotRegistryObservation) {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&observed);
    let writer = observe_writer_reader_snapshots_for_test(
        Arc::new(move |registry| sink.lock().expect("capture registry").push(registry)),
        || Database::open(path),
    )
    .expect("the writer reopens the store");
    let mut observed = observed.lock().expect("read captured registry").clone();
    assert_eq!(
        observed.len(),
        1,
        "one writer start captures its readers once"
    );
    (writer, observed.remove(0))
}

/// The roots a writer's startup was told to keep for readers.
fn retained_roots(registry: &WriterReaderSnapshotRegistryObservation) -> BTreeSet<(u64, [u8; 8])> {
    match registry {
        WriterReaderSnapshotRegistryObservation::Complete(snapshots) => snapshots
            .iter()
            .map(|snapshot| {
                (
                    snapshot.storage_transaction_id,
                    snapshot
                        .storage_root_page
                        .expect("a retained reader snapshot names its root"),
                )
            })
            .collect(),
        other => panic!("the writer must see the complete reader registry: {other:?}"),
    }
}

fn paged_session(path: &Path) -> ReadSession {
    let session = ReadSession::open_with_options(
        path,
        ReadSessionOptions {
            limits: ReadLimits {
                cursor_page_rows: PAGE_ROWS as u64,
                ..ReadLimits::default()
            },
            ..ReadSessionOptions::default()
        },
    )
    .expect("open a session beside the live writer");
    assert_eq!(session.route(), ReadRoute::Owner);
    session
}

fn read_every_row(session: &ReadSession, sql: &str) -> contextdb_core::Result<Vec<Vec<Value>>> {
    let mut cursor = session.open_cursor(sql, &HashMap::new())?;
    let mut rows = cursor.first_page().rows().to_vec();
    let mut more = cursor.first_page().has_more();
    while more {
        let page = cursor.fetch(None)?;
        rows.extend(page.rows().iter().cloned());
        more = page.has_more();
    }
    cursor.close()?;
    Ok(rows)
}

#[test]
fn a_session_opened_beside_a_writer_reads_through_every_later_writer_reopen() {
    const ROWS: i64 = 6;
    const REOPENS: usize = 5;
    let directory = tempfile::tempdir().expect("scratch directory");
    let path = directory.path().join("store.db");
    let mut writer = Database::open(&path).expect("open the first writer");
    writer
        .execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create table");
    for id in 0..ROWS {
        writer
            .execute(
                "INSERT INTO items (id) VALUES ($id)",
                &HashMap::from([("id".to_owned(), Value::Int64(id))]),
            )
            .expect("commit row");
    }
    let committed: Vec<_> = (0..ROWS).map(|id| vec![Value::Int64(id)]).collect();
    let session = paged_session(&path);
    assert_eq!(
        read_every_row(&session, "SELECT id FROM items ORDER BY id")
            .expect("the session reads beside its first writer"),
        committed
    );

    for reopen in 1..=REOPENS {
        writer.close().expect("close the writer");
        let (next, _startup) = reopen_writer(&path);
        writer = next;
        let rows = read_every_row(&session, "SELECT id FROM items ORDER BY id").unwrap_or_else(
            |refusal| {
                panic!(
                    "after writer reopen {reopen} the session must still read the committed \
                     rows, not be refused: {refusal}"
                )
            },
        );
        assert_eq!(rows, committed, "after writer reopen {reopen}");
    }
    drop(session);
    writer.close().expect("close the last writer");
}

struct LiveCursor {
    cursor: ReadCursor,
    snapshot: Vec<Vec<Value>>,
    seen: Vec<Vec<Value>>,
    more: bool,
}

impl LiveCursor {
    fn open(session: &ReadSession, snapshot: Vec<Vec<Value>>, opened_in: usize) -> Self {
        let cursor = session
            .open_cursor("SELECT id, payload FROM items ORDER BY id", &HashMap::new())
            .unwrap_or_else(|refusal| {
                panic!(
                    "the session must open a cursor beside writer {opened_in}, not be refused: \
                     {refusal}"
                )
            });
        let seen = cursor.first_page().rows().to_vec();
        let more = cursor.first_page().has_more();
        assert_eq!(
            seen,
            snapshot[..PAGE_ROWS],
            "first page beside writer {opened_in}"
        );
        assert!(
            more,
            "the cursor beside writer {opened_in} retains unread pages"
        );
        Self {
            cursor,
            snapshot,
            seen,
            more,
        }
    }

    fn fetch_one_page(&mut self, during: &str) {
        let page = self.cursor.fetch(None).unwrap_or_else(|refusal| {
            panic!("a retained cursor must keep reading {during}: {refusal}")
        });
        self.seen.extend(page.rows().iter().cloned());
        self.more = page.has_more();
        assert_eq!(
            self.seen,
            self.snapshot[..self.seen.len()],
            "a retained cursor answers its own snapshot {during}"
        );
    }
}

fn payload(generation: usize, id: i64) -> String {
    format!("generation-{generation}-row-{id}-{}", "p".repeat(256))
}

fn generation_rows(generation: usize, rows: i64) -> Vec<Vec<Value>> {
    (0..rows)
        .map(|id| vec![Value::Int64(id), Value::Text(payload(generation, id))])
        .collect()
}

/// Rewrite every row through several copy-on-write commits, so pages that no
/// registered reader protects are freed and reused.
fn rewrite_to_generation(writer: &Database, generation: usize, rows: i64) {
    for round in 0..3 {
        for id in 0..rows {
            let text = if round == 2 {
                payload(generation, id)
            } else {
                format!("churn-{generation}-{round}-{id}-{}", "c".repeat(256))
            };
            writer
                .execute(
                    "UPDATE items SET payload = $payload WHERE id = $id",
                    &HashMap::from([
                        ("id".to_owned(), Value::Int64(id)),
                        ("payload".to_owned(), Value::Text(text)),
                    ]),
                )
                .expect("rewrite a row");
        }
    }
}

#[test]
fn roots_a_session_retains_survive_every_later_writer_and_are_reclaimed_when_it_drops() {
    const ROWS: i64 = 16;
    const CYCLES: usize = 4;
    let directory = tempfile::tempdir().expect("scratch directory");
    let path = directory.path().join("store.db");
    let mut writer = Database::open(&path).expect("open the first writer");
    writer
        .execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create table");
    for id in 0..ROWS {
        writer
            .execute(
                "INSERT INTO items (id, payload) VALUES ($id, $payload)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    ("payload".to_owned(), Value::Text(payload(0, id))),
                ]),
            )
            .expect("commit row");
    }
    let session = paged_session(&path);
    let mut cursors = vec![LiveCursor::open(&session, generation_rows(0, ROWS), 1)];
    let mut retained = BTreeSet::new();

    for cycle in 1..=CYCLES {
        // Writer `cycle` publishes a new generation and frees the pages of the
        // previous ones, then hands the store to writer `cycle + 1`.
        rewrite_to_generation(&writer, cycle, ROWS);
        writer.close().expect("close the writer");
        let (next, startup) = reopen_writer(&path);
        writer = next;
        let roots = retained_roots(&startup);
        for root in &retained {
            assert!(
                roots.contains(root),
                "root {root:?} retained by the live session must survive the start of writer {}; \
                 it registered {roots:?}",
                cycle + 1
            );
        }
        assert!(
            roots.len() >= cursors.len(),
            "writer {} must keep a root for each of the session's {} live cursors; it registered \
             {roots:?}",
            cycle + 1,
            cursors.len()
        );
        retained.extend(roots);
        let during = format!("after writer {} starts", cycle + 1);
        for cursor in &mut cursors {
            cursor.fetch_one_page(&during);
        }
        if cycle < CYCLES {
            cursors.push(LiveCursor::open(
                &session,
                generation_rows(cycle, ROWS),
                cycle + 1,
            ));
        }
    }

    // One more writer start while every cursor is still live and unfinished.
    rewrite_to_generation(&writer, CYCLES + 1, ROWS);
    writer.close().expect("close the writer");
    let (next, startup) = reopen_writer(&path);
    writer = next;
    let roots = retained_roots(&startup);
    assert!(
        retained.iter().all(|root| roots.contains(root)),
        "every root the live session retains survives to the last writer start: retained \
         {retained:?}, registered {roots:?}"
    );

    for cursor in &mut cursors {
        while cursor.more {
            cursor.fetch_one_page("to the end of its snapshot");
        }
        assert_eq!(cursor.seen, cursor.snapshot);
    }
    drop(cursors);
    drop(session);
    writer.close().expect("close the writer");
    let (last, startup) = reopen_writer(&path);
    assert!(
        retained_roots(&startup).is_empty(),
        "once the session is gone no reader root is retained: {startup:?}"
    );
    last.close().expect("close the last writer");
}
