//! A checkpointed one-event commit (an event, its local-commit marker, the
//! context count, the source checkpoint, the revision, the pending-stream and
//! placement rows, and the session count, in one transaction) does a fixed
//! amount of publication work whatever history the store already holds, and a
//! process killed at any write of it leaves exactly the old or the new state.
//!
//! Every assertion is a counter or a state read from an actual durable commit:
//! syncs and writes handed to the store file, and the table writes the storage
//! layer reports (directory publications, witnesses, log entries, selectors).
//! No elapsed time is read.
use super::*;
use crate::persistence::read_persistence_test_scaffold::{
    WriterFileIoKindForTest, observe_writer_file_io_for_test, writer_writes_for_test,
};
use redb::commit_work::{RootWorkKindForTest, observe_root_work_for_test};
use redb::{ReadableDatabase, ReadableTable};
use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;
use std::sync::Mutex;

const CHILD_STORE: &str = "CONTEXTDB_CKPT_FLOOR_STORE";
const CHILD_WRITE_AT: &str = "CONTEXTDB_CKPT_FLOOR_WRITE_AT";
const CHILD_ROLE: &str = "CONTEXTDB_CKPT_FLOOR_ROLE";
const CHILD_CUT_PHASE: &str = "CONTEXTDB_CKPT_FLOOR_CUT_PHASE";
/// The exit code of a child whose armed write never came: the commit it armed
/// finished, so there is no later write position to try.
const COMMIT_FINISHED: i32 = 3;

fn context() -> uuid::Uuid {
    uuid::Uuid::from_u128(0xc0)
}

fn event(n: u64) -> HashMap<String, Value> {
    HashMap::from([
        (
            "id".into(),
            Value::Uuid(uuid::Uuid::from_u128(0xe0 << 64 | n as u128)),
        ),
        ("context_id".into(), Value::Uuid(context())),
        ("connector_id".into(), Value::Text("claude".into())),
        ("adapter_version".into(), Value::Text("1".into())),
        ("anchor_declaration_version".into(), Value::Text("1".into())),
        ("source_instance_ref".into(), Value::Text("src".into())),
        ("session_ref".into(), Value::Text(format!("s{}", n / 50))),
        ("source_event_key".into(), Value::Text(format!("k{n}"))),
        ("source_position".into(), Value::Text("{}".into())),
        ("seq".into(), Value::Int64(n as i64)),
        ("content_hash".into(), Value::Text("h".into())),
        ("anchor_count".into(), Value::Int64(0)),
        ("anchor_set_hash".into(), Value::Text("a".into())),
        ("author".into(), Value::Text("user".into())),
        ("kind".into(), Value::Text("message".into())),
        ("render_role".into(), Value::Text("user".into())),
        (
            "text".into(),
            Value::Text(format!("event {n} text body of a typical length")),
        ),
        ("payload".into(), Value::Text("{}".into())),
        ("anchors".into(), Value::Text("{}".into())),
        ("source_remainder".into(), Value::Text("{}".into())),
        ("coverage".into(), Value::Text("complete".into())),
    ])
}

fn params() -> HashMap<String, Value> {
    HashMap::from([("c".to_string(), Value::Uuid(context()))])
}

fn declare(db: &Database) {
    for ddl in [
        "CREATE TABLE contexts (id UUID PRIMARY KEY, name TEXT NOT NULL)",
        "CREATE TABLE events (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID NOT NULL REFERENCES contexts(id), connector_id TEXT NOT NULL, adapter_version TEXT NOT NULL, anchor_declaration_version TEXT NOT NULL, source_instance_ref TEXT NOT NULL, session_ref TEXT NOT NULL, source_event_key TEXT NOT NULL, source_position JSON NOT NULL, seq INTEGER NOT NULL, content_hash TEXT NOT NULL, anchor_count INTEGER NOT NULL, anchor_set_hash TEXT NOT NULL, occurred_at TIMESTAMP, author TEXT NOT NULL, kind TEXT NOT NULL, render_role TEXT NOT NULL, interaction_ref TEXT, text TEXT, payload JSON NOT NULL DEFAULT '{}', anchors JSON NOT NULL DEFAULT '{}', source_remainder JSON NOT NULL DEFAULT '{}', coverage TEXT NOT NULL, UNIQUE (context_id, id), UNIQUE (context_id, connector_id, session_ref, source_event_key), UNIQUE (context_id, connector_id, source_instance_ref, seq)) IMMUTABLE SYNC OFF MAINTAIN COUNT (context_id), (context_id, connector_id), (context_id, connector_id, source_instance_ref)",
        "CREATE INDEX idx_events_source_stream ON events (context_id, connector_id, source_instance_ref, seq)",
        "CREATE INDEX idx_events_session ON events (context_id, connector_id, session_ref, seq)",
        "CREATE INDEX idx_events_source_time ON events (context_id, connector_id, occurred_at, seq)",
        "CREATE INDEX idx_events_author_kind ON events (context_id, author, kind, seq)",
        "CREATE TABLE event_context_counts (context_id UUID CONTEXT_ID PRIMARY KEY REFERENCES contexts(id), captured_events INTEGER NOT NULL) SYNC OFF",
        "CREATE TABLE event_source_checkpoints (context_id UUID CONTEXT_ID NOT NULL REFERENCES contexts(id), connector_id TEXT NOT NULL, source_instance_ref TEXT NOT NULL, checkpoint JSON NOT NULL, committed_byte_offset INTEGER NOT NULL, prefix_sha256 TEXT NOT NULL, prefix_hash_state TEXT NOT NULL, latest_event_seq INTEGER, PRIMARY KEY (context_id, connector_id, source_instance_ref)) SYNC OFF",
        "CREATE TABLE event_local_commits (event_id UUID PRIMARY KEY REFERENCES events(id), context_id UUID CONTEXT_ID NOT NULL REFERENCES contexts(id), committed_at TXID NOT NULL, FOREIGN KEY (context_id, event_id) REFERENCES events(context_id, id)) IMMUTABLE SYNC OFF MAINTAIN COUNT NONE",
        "CREATE TABLE event_context_sessions (context_id UUID CONTEXT_ID NOT NULL REFERENCES contexts(id), connector_id TEXT NOT NULL, session_ref TEXT NOT NULL, event_count INTEGER NOT NULL, latest_occurred_at INTEGER, PRIMARY KEY (context_id, connector_id, session_ref)) SYNC OFF",
        "CREATE TABLE event_context_revisions (context_id UUID CONTEXT_ID PRIMARY KEY REFERENCES contexts(id), revision_tx INTEGER NOT NULL) SYNC OFF",
        "CREATE TABLE event_search_pending_streams (context_id UUID CONTEXT_ID NOT NULL REFERENCES contexts(id), generation_id UUID NOT NULL, connector_id TEXT NOT NULL, source_instance_ref TEXT NOT NULL, pending INTEGER NOT NULL, PRIMARY KEY (context_id, generation_id, connector_id, source_instance_ref)) SYNC OFF",
        "CREATE TABLE event_placement (id UUID PRIMARY KEY, expected_policy TEXT NOT NULL, first_event_tx INTEGER) SYNC OFF",
    ] {
        db.execute(ddl, &HashMap::new()).expect("declare");
    }
    db.execute("INSERT INTO contexts VALUES ($c, 'c')", &params())
        .unwrap();
    db.execute("INSERT INTO event_context_counts VALUES ($c, 0)", &params())
        .unwrap();
    db.execute(
        "INSERT INTO event_source_checkpoints VALUES ($c, 'claude', 'src', '{}', 0, 'h', 's', NULL)",
        &params(),
    )
    .unwrap();
    for seed in [
        "INSERT INTO event_context_revisions VALUES ($c, 0)",
        "INSERT INTO event_search_pending_streams VALUES ($c, $c, 'claude', 'src', 0)",
        "INSERT INTO event_placement VALUES ($c, 'p', NULL)",
    ] {
        db.execute(seed, &params()).unwrap();
    }
}

/// `count` events, one transaction, with the cg checkpointed shape when
/// `checkpointed` (a local-commit marker per event and the four bookkeeping
/// updates), otherwise the live shape (event plus context count only).
fn append(db: &Database, from: u64, count: u64, checkpointed: bool) {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let tx = db.begin().unwrap();
    let mut p = params();
    db.execute_in_tx(tx, "SELECT checkpoint, committed_byte_offset FROM event_source_checkpoints WHERE context_id = $c AND connector_id = 'claude' AND source_instance_ref = 'src'", &p).unwrap();
    db.execute_in_tx(tx, "SELECT seq FROM events WHERE context_id = $c AND connector_id = 'claude' AND source_instance_ref = 'src' ORDER BY seq DESC LIMIT 1", &p).unwrap();
    for n in from..from + count {
        db.insert_row(tx, "events", event(n)).unwrap();
        if checkpointed {
            db.insert_row(
                tx,
                "event_local_commits",
                HashMap::from([
                    (
                        "event_id".into(),
                        Value::Uuid(uuid::Uuid::from_u128(0xe0 << 64 | n as u128)),
                    ),
                    ("context_id".into(), Value::Uuid(context())),
                    ("committed_at".into(), Value::TxId(tx)),
                ]),
            )
            .unwrap();
        }
    }
    p.insert("n".into(), Value::Int64((from + count) as i64));
    db.execute_in_tx(
        tx,
        "UPDATE event_context_counts SET captured_events = $n WHERE context_id = $c",
        &p,
    )
    .unwrap();
    if checkpointed {
        for sql in [
            "UPDATE event_source_checkpoints SET checkpoint = '{\"record\": 1}', committed_byte_offset = $n, latest_event_seq = $n WHERE context_id = $c AND connector_id = 'claude' AND source_instance_ref = 'src'",
            "UPDATE event_context_revisions SET revision_tx = $n WHERE context_id = $c",
            "UPDATE event_search_pending_streams SET pending = $n WHERE context_id = $c AND generation_id = $c AND connector_id = 'claude' AND source_instance_ref = 'src'",
            "UPDATE event_placement SET first_event_tx = $n WHERE id = $c AND first_event_tx IS NULL",
        ] {
            db.execute_in_tx(tx, sql, &p).unwrap();
        }
        let seen = db
            .execute_in_tx(tx, "SELECT event_count FROM event_context_sessions WHERE context_id = $c AND connector_id = 'claude' AND session_ref = 'sess'", &p)
            .unwrap();
        let sql = if seen.rows.is_empty() {
            "INSERT INTO event_context_sessions VALUES ($c, 'claude', 'sess', $n, NULL)"
        } else {
            "UPDATE event_context_sessions SET event_count = $n WHERE context_id = $c AND connector_id = 'claude' AND session_ref = 'sess'"
        };
        db.execute_in_tx(tx, sql, &p).unwrap();
    }
    db.commit(tx).unwrap();
}

/// A store holding `stored` events and three warm checkpointed appends; the
/// next event number is returned with it.
fn store_with(path: &std::path::Path, stored: u64) -> (Database, u64) {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let db = Database::open(path).expect("open");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    declare(&db);
    let mut next = 0;
    while next < stored {
        let count = (stored - next).min(500);
        append(&db, next, count, true);
        next += count;
    }
    for _ in 0..3 {
        append(&db, next, 1, true);
        next += 1;
    }
    (db, next)
}

/// What one commit handed the store file and what the storage layer reported
/// writing, by table.
#[derive(Debug)]
struct Commit {
    syncs: u64,
    writes: u64,
    bytes: u64,
    by_table: BTreeMap<String, BTreeMap<String, u64>>,
    /// Keys written more than once in the commit, by (table, operation).
    repeated: BTreeMap<(String, String), u64>,
    /// Root work by kind, and finalizations / checksum pages by table.
    root_work: BTreeMap<String, u64>,
    finalized: BTreeMap<String, u64>,
    checksum_pages: BTreeMap<String, u64>,
    write_ranges: Vec<(u64, usize)>,
    checksum_work: Vec<(String, String, [u8; 8], usize, u8)>,
    capture_errors: Vec<String>,
}

impl Commit {
    fn count(&self, table: &str, operation: &str) -> u64 {
        self.by_table
            .get(table)
            .and_then(|operations| operations.get(operation))
            .copied()
            .unwrap_or(0)
    }
    /// Tables that received a row or index write (not the directory itself).
    fn written_tables(&self) -> BTreeSet<&str> {
        self.by_table
            .iter()
            .filter(|(_, operations)| {
                operations.keys().any(|operation| {
                    operation != "DirectoryInsert" && operation != "DirectoryRemove"
                })
            })
            .map(|(table, _)| table.as_str())
            .collect()
    }
    /// Every count except the content-identity family, whose node writes are
    /// path-deep data work that legitimately grows with the stored keys.
    fn fixed_work(&self) -> BTreeMap<String, BTreeMap<String, u64>> {
        self.by_table
            .iter()
            .filter(|(table, _)| !table.starts_with("__content_identity_nodes"))
            .map(|(table, operations)| (table.clone(), operations.clone()))
            .collect()
    }
}

fn observed_commit(db: &Database, run: impl FnOnce()) -> Commit {
    let events = Arc::new(Mutex::new(BTreeMap::<String, BTreeMap<String, u64>>::new()));
    let keys = Arc::new(Mutex::new(BTreeMap::<(String, String, Vec<u8>), u64>::new()));
    let guard = db
        .persistence
        .as_ref()
        .expect("file-backed")
        .with_db(|redb| {
            let events = events.clone();
            let keys = keys.clone();
            Ok(redb.observe_table_writes_for_test(Arc::new(move |event| {
                let operation = format!("{:?}", event.operation);
                *events
                    .lock()
                    .unwrap()
                    .entry(event.table.clone())
                    .or_default()
                    .entry(operation.clone())
                    .or_default() += 1;
                *keys
                    .lock()
                    .unwrap()
                    .entry((event.table.clone(), operation, event.key.clone()))
                    .or_default() += 1;
            })))
        })
        .expect("observe the actual table writes");
    let file = Arc::new(Mutex::new((0u64, 0u64, 0u64, Vec::new())));
    let root = Arc::new(Mutex::new((
        BTreeMap::<String, u64>::new(),
        BTreeMap::<String, u64>::new(),
        BTreeMap::<String, u64>::new(),
        None::<(String, u64)>,
        Vec::<String>::new(),
        Vec::<(String, String, [u8; 8], usize, u8)>::new(),
    )));
    let file_sink = file.clone();
    let root_sink = root.clone();
    observe_writer_file_io_for_test(
        Arc::new(move |event| {
            let mut file = file_sink.lock().unwrap();
            match (event.kind, event.success) {
                (WriterFileIoKindForTest::SyncEnd, Some(true)) => file.0 += 1,
                (WriterFileIoKindForTest::WriteEnd, Some(true)) => {
                    file.1 += 1;
                    file.2 += event.bytes as u64;
                    file.3.push((event.offset, event.bytes));
                }
                _ => {}
            }
        }),
        || {
            observe_root_work_for_test(
                Arc::new(move |work| {
                    let mut root = root_sink.lock().unwrap();
                    *root.0.entry(format!("{:?}", work.kind)).or_default() += 1;
                    match work.kind {
                        RootWorkKindForTest::CaptureBegin => {
                            if root.3.is_some() {
                                root.4.push("overlapping capture scopes".to_owned());
                            }
                            root.3 = Some((work.table.to_owned(), 0));
                        }
                        RootWorkKindForTest::CaptureEnd => {
                            let scope = root.3.take();
                            match scope {
                                Some((table, count)) if table == work.table && count <= 1 => (),
                                other => root
                                    .4
                                    .push(format!("capture end {}: {other:?}", work.table)),
                            }
                        }
                        RootWorkKindForTest::FinalizeBegin => {
                            *root.1.entry(work.table.to_string()).or_default() += 1;
                            let mut wrong_target = None;
                            if let Some((table, count)) = &mut root.3 {
                                *count += 1;
                                if table != work.table {
                                    wrong_target = Some(table.clone());
                                }
                            }
                            if let Some(table) = wrong_target {
                                root.4
                                    .push(format!("capture {table} finalized {}", work.table));
                            }
                        }
                        RootWorkKindForTest::ChecksumPage => {
                            *root.2.entry(work.table.to_string()).or_default() += 1;
                        }
                        _ => {}
                    }
                    if matches!(
                        work.kind,
                        RootWorkKindForTest::ChecksumPage | RootWorkKindForTest::ChecksumBytes
                    ) && let Some(page) = work.page
                    {
                        root.5.push((
                            work.table.to_owned(),
                            format!("{:?}", work.kind),
                            page,
                            work.bytes,
                            work.tag,
                        ));
                    }
                    Ok(())
                }),
                run,
            )
        },
    );
    drop(guard);
    let by_table = events.lock().unwrap().clone();
    let repeated = keys
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, count)| **count > 1)
        .fold(BTreeMap::new(), |mut acc, ((table, operation, _), _)| {
            *acc.entry((table.clone(), operation.clone())).or_default() += 1;
            acc
        });
    let (syncs, writes, bytes, write_ranges) = file.lock().unwrap().clone();
    let (root_work, finalized, checksum_pages, capture, mut capture_errors, checksum_work) =
        root.lock().unwrap().clone();
    if capture.is_some() {
        capture_errors.push("unterminated capture scope".to_owned());
    }
    Commit {
        syncs,
        writes,
        bytes,
        by_table,
        repeated,
        root_work,
        finalized,
        checksum_pages,
        write_ranges,
        checksum_work,
        capture_errors,
    }
}

fn checkpointed_commit_at(stored: u64) -> Commit {
    let root = tempfile::tempdir().expect("fixture directory");
    let (db, next) = store_with(&root.path().join("ckpt.db"), stored);
    let commit = observed_commit(&db, || append(&db, next, 1, true));
    db.close().expect("close");
    if let Some(path) = std::env::var_os("CONTEXTDB_COMMIT_COUNTER_RECEIPTS") {
        let path = std::path::PathBuf::from(path);
        std::fs::create_dir_all(&path).unwrap();
        let name = std::thread::current()
            .name()
            .unwrap_or("commit")
            .replace(':', "_");
        let receipt = serde_json::json!({
            "write_transaction_control_bytes": std::mem::size_of::<redb::WriteTransaction>(),
            "stored": stored, "syncs": commit.syncs, "writes": commit.writes,
            "bytes": commit.bytes, "by_table": commit.by_table,
            "repeated_keys": commit.repeated.iter().collect::<Vec<_>>(),
            "root_work": commit.root_work, "finalized": commit.finalized,
            "checksum_pages": commit.checksum_pages, "write_ranges": commit.write_ranges,
            "checksum_work": commit.checksum_work, "capture_errors": commit.capture_errors,
        });
        std::fs::write(
            path.join(format!("{name}-{stored}.json")),
            serde_json::to_vec_pretty(&receipt).unwrap(),
        )
        .unwrap();
    }
    commit
}

// Promise: one checkpointed append costs one durable sync and the same set of
// logical writes at 10 and at 20,000 stored events. Everything but the
// content-identity node path (which deepens with the stored keys) is flat,
// exactly: directory publications, witnesses, commitment states, log entries,
// their selectors and the rows and indexes of every table.
#[test]
fn a_checkpointed_commit_does_the_same_fixed_work_at_10_and_20000_stored_events() {
    let small = checkpointed_commit_at(10);
    let large = checkpointed_commit_at(20_000);
    assert_eq!(small.syncs, 1, "one durable sync at 10 events");
    assert_eq!(large.syncs, 1, "one durable sync at 20,000 events");
    assert_eq!(
        small.fixed_work(),
        large.fixed_work(),
        "per-table logical writes of one checkpointed commit at 10 and 20,000 events"
    );
}

// Promise: every table a commit changes is published to the directory once,
// at the end, and unrelated tables are not published at all. A commit that
// publishes a directory entry more often than it changes tables is re-flushing
// pending roots that it only needed to flush once.
#[test]
fn a_checkpointed_commit_publishes_each_changed_table_to_the_directory_once() {
    for stored in [10u64, 20_000] {
        let commit = checkpointed_commit_at(stored);
        let tables = commit.written_tables();
        let directory = commit.count("__redb_user_table_directory", "DirectoryInsert");
        assert!(
            directory <= tables.len() as u64,
            "stored={stored}: {directory} directory publications for {} changed tables: {tables:?}",
            tables.len()
        );
    }
}

// Promise: a history witness is written for a table whose root changed, once
// per mutation; capturing the same unchanged root again writes nothing. A
// replaced row has both a retired version and a successor, so legitimately
// changes the same physical tree twice. Idle reopen coverage lives beside the
// shared witness function in history_root_witness.rs.
#[test]
fn a_checkpointed_commit_writes_each_changed_tables_witness_once() {
    for stored in [10u64, 20_000] {
        let commit = checkpointed_commit_at(stored);
        let tables = commit.written_tables();
        let witnesses = commit.count("__history_root_witness_v1", "Insert");
        let protected_mutations: u64 = commit
            .by_table
            .iter()
            .filter(|(name, _)| name.starts_with("rel_") || name.starts_with("__rel_index_"))
            .map(|(_, operations)| {
                operations.get("Insert").copied().unwrap_or(0)
                    + operations.get("Remove").copied().unwrap_or(0)
            })
            .sum();
        assert!(
            witnesses <= protected_mutations,
            "stored={stored}: {witnesses} witnesses for {protected_mutations} protected mutations in {} changed tables: {tables:?}",
            tables.len()
        );
    }
}

// Promise: validating the graph-proof tables of a commit looks each one up by
// name; it never enumerates the catalog, so the work does not grow with the
// number of unrelated tables a store declares. One checkpointed commit
// enumerates zero catalog entries at 0 and at 64 extra unrelated tables.
#[test]
fn a_checkpointed_commit_enumerates_no_catalog_entries_at_any_table_count() {
    for extra_tables in [0usize, 64] {
        let root = tempfile::tempdir().expect("fixture directory");
        let (db, next) = store_with(&root.path().join("ckpt.db"), 10);
        for n in 0..extra_tables {
            db.execute(
                &format!("CREATE TABLE unrelated_{n:03} (id UUID PRIMARY KEY, body TEXT)"),
                &HashMap::new(),
            )
            .expect("an unrelated table");
        }
        let commit = observed_commit(&db, || append(&db, next, 1, true));
        let entries = commit.root_work.get("CatalogEntry").copied().unwrap_or(0);
        assert_eq!(
            entries, 0,
            "{extra_tables} unrelated tables: one checkpointed commit enumerated {entries} catalog entries"
        );
        db.close().expect("close");
    }
}

// Promise: each named capture finalizes at most its own pending root, once.
// A later mutation/restage can legitimately require another finalization.
// Final commit flushes all surviving pending roots outside capture scopes.
#[test]
fn a_checkpointed_commit_finalizes_each_pending_root_once() {
    for stored in [10u64, 20_000] {
        let commit = checkpointed_commit_at(stored);
        assert!(
            commit.root_work.get("CaptureBegin").copied().unwrap_or(0) > 0,
            "the observation must cover actual root captures"
        );
        assert!(
            commit.capture_errors.is_empty(),
            "stored={stored}: named captures touched unrelated roots or finalized twice: {:?}",
            commit.capture_errors
        );
    }
}

/// The state a recovered store must show, read through the ordinary surface.
fn fingerprint(db: &Database) -> Vec<String> {
    let mut lines = Vec::new();
    for sql in [
        "SELECT id, seq FROM events ORDER BY seq",
        "SELECT event_id FROM event_local_commits ORDER BY event_id",
        "SELECT captured_events FROM event_context_counts",
        "SELECT checkpoint, committed_byte_offset, latest_event_seq FROM event_source_checkpoints",
        "SELECT revision_tx FROM event_context_revisions",
        "SELECT pending FROM event_search_pending_streams",
        "SELECT first_event_tx FROM event_placement",
        "SELECT event_count FROM event_context_sessions",
        "SELECT id FROM events WHERE context_id = $c AND connector_id = 'claude' AND session_ref = 's0' ORDER BY seq",
    ] {
        let result = db.execute(sql, &params()).expect("read recovered state");
        lines.push(format!("{sql} => {:?}", result.rows));
    }
    lines.push(format!("lsn {}", db.current_lsn().0));
    let tables = [
        "contexts",
        "events",
        "event_local_commits",
        "event_context_counts",
        "event_source_checkpoints",
        "event_context_revisions",
        "event_search_pending_streams",
        "event_placement",
        "event_context_sessions",
    ];
    for table in tables {
        let mut proof = db.verify_historical_indexes(table).unwrap();
        proof.sort_by(|a, b| a.index.cmp(&b.index));
        lines.push(format!("historical {table} {proof:?}"));
    }
    let (storage, positions) = db
        .persistence
        .as_ref()
        .unwrap()
        .with_db(|store| {
            let read = store.begin_read().unwrap();
            let identity =
                crate::persistence::RedbPersistence::load_content_identity_from_read(&read)?;
            let mut storage = vec![format!("content identity {identity:?}")];
            for name in [
                "writer_change_log",
                "change_log_by_table",
                "__historical_index_commitments",
            ] {
                let table = read
                    .open_table(redb::TableDefinition::<&[u8], &[u8]>::new(name))
                    .unwrap();
                let mut hash = blake3::Hasher::new();
                for row in table.iter().unwrap() {
                    let (key, value) = row.unwrap();
                    hash.update(&(key.value().len() as u64).to_le_bytes());
                    hash.update(key.value());
                    hash.update(&(value.value().len() as u64).to_le_bytes());
                    hash.update(value.value());
                }
                storage.push(format!("{name} {}", hash.finalize()));
            }
            let primary = read
                .open_table(redb::TableDefinition::<&str, &[u8]>::new("change_log"))
                .unwrap();
            let mut hash = blake3::Hasher::new();
            for row in primary.iter().unwrap() {
                let (key, value) = row.unwrap();
                hash.update(&(key.value().len() as u64).to_le_bytes());
                hash.update(key.value().as_bytes());
                hash.update(&(value.value().len() as u64).to_le_bytes());
                hash.update(value.value());
            }
            storage.push(format!("change_log {}", hash.finalize()));
            let mut positions = Vec::new();
            for name in ["commit_index", "change_log_lsn_count"] {
                let table = read
                    .open_table(redb::TableDefinition::<u64, u64>::new(name))
                    .unwrap();
                let mut hash = blake3::Hasher::new();
                for row in table.iter().unwrap() {
                    let (key, value) = row.unwrap();
                    hash.update(&key.value().to_le_bytes());
                    hash.update(&value.value().to_le_bytes());
                    if name == "commit_index" {
                        positions.push((key.value(), value.value()));
                    }
                }
                storage.push(format!("{name} {}", hash.finalize()));
            }
            Ok((storage, positions))
        })
        .unwrap();
    lines.extend(storage);
    // Persisted row maps need not serialize their fields in the same order
    // in two independent writers. Compare every retained logical version at
    // its actual committed frontier through the production decoder instead.
    for (lsn, tx) in positions {
        for table in tables {
            let mut rows: Vec<_> = db
                .scan(table, SnapshotId(tx))
                .unwrap()
                .into_iter()
                .map(|row| {
                    (
                        row.row_id,
                        row.created_tx,
                        row.deleted_tx,
                        row.lsn,
                        row.created_at,
                        row.values.into_iter().collect::<BTreeMap<_, _>>(),
                    )
                })
                .collect();
            rows.sort_by_key(|row| (row.0, row.1));
            lines.push(format!("rows {table} at {lsn}/{tx}: {rows:?}"));
        }
    }
    lines
}

fn copy_store(from: &std::path::Path, to: &std::path::Path) {
    std::fs::copy(from, to).expect("copy the prepared store");
}

fn child_filter(test: &str) -> String {
    format!("database::checkpointed_commit_floor_tests::{test}")
}

fn stop_at_cut(phase: &str, nth: u64, seen: &std::sync::atomic::AtomicU64) {
    if seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 == nth {
        // Parsed by the parent to prove the exact intended boundary was reached.
        eprintln!("WRITER_COMMIT_CUT={phase}:{nth}");
        std::process::abort();
    }
}

/// The killed writer: open a copy, arm the nth write of the next commit, and
/// append one checkpointed event.
fn crash_child() -> bool {
    let Ok(role) = std::env::var(CHILD_ROLE) else {
        return false;
    };
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let store = std::path::PathBuf::from(std::env::var(CHILD_STORE).unwrap());
    let db = Database::open(&store).expect("open the copy");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let next: u64 = std::env::var("CONTEXTDB_CKPT_FLOOR_NEXT")
        .unwrap()
        .parse()
        .unwrap();
    match role.as_str() {
        "crash" => {
            let nth: u64 = std::env::var(CHILD_WRITE_AT).unwrap().parse().unwrap();
            let phase = std::env::var(CHILD_CUT_PHASE).unwrap();
            let seen = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let io_phase = phase.clone();
            let io_seen = seen.clone();
            observe_root_work_for_test(
                Arc::new(move |event| {
                    if (phase == "before_header" && event.kind == RootWorkKindForTest::HeaderBegin)
                        || (phase == "after_header" && event.kind == RootWorkKindForTest::HeaderEnd)
                    {
                        stop_at_cut(&phase, nth, &seen);
                    }
                    Ok(())
                }),
                || {
                    observe_writer_file_io_for_test(
                        Arc::new(move |event| {
                            let selected = match io_phase.as_str() {
                                "before_write" => event.kind == WriterFileIoKindForTest::WriteBegin,
                                "after_write" => {
                                    event.kind == WriterFileIoKindForTest::WriteEnd
                                        && event.success == Some(true)
                                }
                                "before_sync" => event.kind == WriterFileIoKindForTest::SyncBegin,
                                "after_sync" => {
                                    event.kind == WriterFileIoKindForTest::SyncEnd
                                        && event.success == Some(true)
                                }
                                "before_header" | "after_header" => false,
                                other => panic!("unknown cut phase {other}"),
                            };
                            if selected {
                                stop_at_cut(&io_phase, nth, &io_seen);
                            }
                        }),
                        || append(&db, next, 1, true),
                    )
                },
            );
            std::process::exit(COMMIT_FINISHED);
        }
        "measure" => {
            let before = writer_writes_for_test();
            let header = Arc::new(Mutex::new((0u64, Vec::new())));
            let observed = header.clone();
            let logical_headers = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let header_count = logical_headers.clone();
            observe_root_work_for_test(
                Arc::new(move |event| {
                    if event.kind == RootWorkKindForTest::HeaderBegin {
                        header_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                    Ok(())
                }),
                || {
                    observe_writer_file_io_for_test(
                        Arc::new(move |event| {
                            if event.kind == WriterFileIoKindForTest::WriteBegin {
                                let mut header = observed.lock().unwrap();
                                header.0 += 1;
                                if event.offset == 0 {
                                    let ordinal = header.0;
                                    header.1.push(ordinal);
                                }
                            }
                        }),
                        || append(&db, next, 1, true),
                    )
                },
            );
            println!("WRITER_COMMIT_WRITES={}", writer_writes_for_test() - before);
            println!(
                "WRITER_HEADER_WRITES={}",
                serde_json::to_string(&header.lock().unwrap().1).unwrap()
            );
            println!(
                "WRITER_LOGICAL_HEADERS={}",
                logical_headers.load(std::sync::atomic::Ordering::SeqCst)
            );
            db.close().expect("close the measured copy");
            true
        }
        "recover" => {
            println!(
                "RECOVERED_STATE={}",
                serde_json::to_string(&fingerprint(&db)).unwrap()
            );
            db.close().expect("close the fresh recovery process");
            true
        }
        other => panic!("unknown role {other}"),
    }
}

fn run_child(
    store: &std::path::Path,
    role: &str,
    next: u64,
    write_at: Option<u64>,
    cut_phase: Option<&str>,
) -> std::process::Output {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .arg("--exact")
        .arg(child_filter(
            "a_checkpointed_commit_killed_at_every_write_leaves_the_old_or_the_new_state",
        ))
        .arg("--nocapture")
        .env(CHILD_ROLE, role)
        .env(CHILD_STORE, store)
        .env("CONTEXTDB_CKPT_FLOOR_NEXT", next.to_string());
    command.env_remove(CHILD_CUT_PHASE);
    if let Some(nth) = write_at {
        command.env(CHILD_WRITE_AT, nth.to_string());
    }
    if let Some(phase) = cut_phase {
        command.env(CHILD_CUT_PHASE, phase);
    }
    command.output().unwrap()
}

// Promise: a process killed before any one write of a checkpointed commit
// leaves the store exactly as it was before the commit or exactly as the commit
// leaves it, never a mixture, in a fresh process, and the next append then
// behaves as it does on a fresh control. The pages-before-header order means
// the killed store is the old one at every position up to the last write, and
// the positions after the state turns new stay new.
#[test]
fn a_checkpointed_commit_killed_at_every_write_leaves_the_old_or_the_new_state() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    if crash_child() {
        return;
    }
    let root = tempfile::tempdir().expect("fixture directory");
    let base = root.path().join("base.db");
    let (db, next) = store_with(&base, 10);
    db.close().expect("close the prepared store");
    drop(db);

    // The fingerprints of the old state, and of the new state a clean commit
    // produces, each read in a fresh process-independent open.
    let old_copy = root.path().join("old.db");
    copy_store(&base, &old_copy);
    let old = {
        let db = Database::open(&old_copy).expect("open the old state");
        let state = fingerprint(&db);
        db.close().expect("close");
        state
    };
    let control = root.path().join("control.db");
    copy_store(&base, &control);
    let measured = run_child(&control, "measure", next, None, None);
    assert!(measured.status.success(), "the control commit succeeds");
    let writes: u64 = String::from_utf8_lossy(&measured.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("WRITER_COMMIT_WRITES="))
        .and_then(|count| count.trim().parse().ok())
        .expect("the control reports its writes");
    let headers: Vec<u64> = String::from_utf8_lossy(&measured.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("WRITER_HEADER_WRITES="))
        .map(|line| serde_json::from_str(line).unwrap())
        .unwrap();
    assert!(
        !headers.is_empty(),
        "the backend header write must be covered"
    );
    let logical_headers: u64 = String::from_utf8_lossy(&measured.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("WRITER_LOGICAL_HEADERS="))
        .map(|line| line.parse().unwrap())
        .unwrap();
    assert_eq!(
        logical_headers, 2,
        "both native header preparations must be covered even if buffered together"
    );
    assert!(
        writes > 1,
        "a checkpointed commit writes many times: {writes}"
    );
    let new = {
        let db = Database::open(&control).expect("open the new state");
        let state = fingerprint(&db);
        db.close().expect("close");
        state
    };
    assert_ne!(old, new, "the commit changes what the store holds");

    let mut turned_new_at = BTreeMap::new();
    let cuts = ["before_write", "after_write"]
        .into_iter()
        .flat_map(|phase| (1..=writes).map(move |nth| (phase, nth)))
        .chain(
            ["before_header", "after_header"]
                .into_iter()
                .flat_map(|phase| (1..=logical_headers).map(move |nth| (phase, nth))),
        )
        .chain([("before_sync", 1), ("after_sync", 1)]);
    for (phase, nth) in cuts {
        let copy = root.path().join(format!("crash-{phase}-{nth}.db"));
        copy_store(&base, &copy);
        let output = run_child(&copy, "crash", next, Some(nth), Some(phase));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(&format!("WRITER_COMMIT_CUT={phase}:{nth}")),
            "{phase} {nth}: the child must die at the exact armed boundary: {stderr}"
        );
        let recovery = run_child(&copy, "recover", next, None, None);
        assert!(
            recovery.status.success(),
            "fresh recovery failed: {}",
            String::from_utf8_lossy(&recovery.stderr)
        );
        let state: Vec<String> = String::from_utf8_lossy(&recovery.stdout)
            .lines()
            .find_map(|line| line.strip_prefix("RECOVERED_STATE="))
            .map(|line| serde_json::from_str(line).unwrap())
            .expect("fresh recovery state");
        let is_old = state == old;
        let is_new = state == new;
        assert!(
            is_old || is_new,
            "write {nth} of {writes}: the killed store is the old or the new state, never a mixture: {state:?}"
        );
        if phase.ends_with("write") && is_new {
            turned_new_at.entry(phase).or_insert(nth);
        }
        if let Some(turned) = turned_new_at.get(phase) {
            assert!(
                is_new,
                "write {nth}: once the commit is published (write {turned}) every later kill keeps it"
            );
        }
        if (phase == "before_write" && nth <= headers[0])
            || (phase == "after_write" && nth < headers[0])
        {
            assert!(
                is_old,
                "{phase} {nth}: before header publication must recover old state"
            );
        }
        if phase == "after_sync" {
            assert!(is_new, "acknowledged durable sync must recover new state");
        }
        // The next append from the recovered store converges with the control:
        // an old store takes the same commit again, a new store takes the next.
        let recovered = Database::open(&copy).unwrap();
        recovered.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        assert_eq!(
            fingerprint(&recovered),
            state,
            "another reopen keeps the same complete state"
        );
        if is_old {
            append(&recovered, next, 1, true);
            assert_eq!(
                fingerprint(&recovered),
                new,
                "write {nth}: re-appending on the recovered old store reaches the control's state"
            );
        }
        recovered.close().expect("close the recovered store");
    }
    // The exact first-header ordinal above, rather than a ratio of write
    // counts, is the publication boundary required by the existing oracle.
}
