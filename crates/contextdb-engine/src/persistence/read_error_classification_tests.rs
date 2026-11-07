//! One transient read error stays what it is. An EIO is reported as
//! the storage I/O class, never as a corrupt store with reset advice. A short
//! read or invalid bytes are still damage and still say so. Each case sweeps
//! every writer-file read of one operation, counted on a dry run of the same
//! operation, and fails exactly one read per run.
#![expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
use crate::Database;
use crate::executor::bounded_read_test_support as bounded;
use crate::persistence::read_persistence_test_scaffold::fail_nth_writer_read_with_for_test;
use contextdb_core::{Error, Value};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy)]
struct FrozenClock;
impl contextdb_core::read_contract::DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }
    fn wait_until(&self, _: u64) -> contextdb_core::read_contract::DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn params(id: i64, body: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_owned(), Value::Int64(id)),
        ("body".to_owned(), Value::Text(body.to_owned())),
    ])
}

/// A small closed store whose reads reach real pages: plain rows, rows with
/// several versions under full history, and vectors.
fn fixture(dir: &Path) -> PathBuf {
    let path = dir.join("base.db");
    let db = Database::open(&path).unwrap();
    let none = HashMap::new();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, body TEXT)", &none)
        .unwrap();
    db.execute(
        "CREATE TABLE h (id INTEGER PRIMARY KEY, body TEXT) HISTORY ALL",
        &none,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE vecs (id INTEGER PRIMARY KEY, embedding VECTOR(3))",
        &none,
    )
    .unwrap();
    let body = "w".repeat(1024);
    for start in (0..200i64).step_by(50) {
        let tx = db.begin().unwrap();
        for id in start..start + 50 {
            db.execute_in_tx(tx, "INSERT INTO t VALUES ($id, $body)", &params(id, &body))
                .unwrap();
        }
        db.commit(tx).unwrap();
    }
    for id in 0..40i64 {
        db.execute("INSERT INTO h VALUES ($id, $body)", &params(id, "first"))
            .unwrap();
        db.execute(
            &format!(
                "INSERT INTO vecs VALUES ({id}, [{}, 1, 0])",
                id as f64 / 40.0
            ),
            &none,
        )
        .unwrap();
    }
    for id in 0..40i64 {
        db.execute(
            "UPDATE h SET body = $body WHERE id = $id",
            &params(id, "second"),
        )
        .unwrap();
    }
    // A small graph, so graph traversals and their proof diagnosis
    // (graph_read GraphProofContext::diagnose) read real pages.
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &none,
    )
    .unwrap();
    for n in 0..20u128 {
        db.execute(
            "INSERT INTO edges VALUES ($id, $source, $target, 'LINKS')",
            &HashMap::from([
                (
                    "id".to_owned(),
                    Value::Uuid(uuid::Uuid::from_u128(1_000 + n)),
                ),
                ("source".to_owned(), Value::Uuid(uuid::Uuid::from_u128(n))),
                (
                    "target".to_owned(),
                    Value::Uuid(uuid::Uuid::from_u128(n + 1)),
                ),
            ]),
        )
        .unwrap();
    }
    db.close().unwrap();
    path
}

/// A traversal over the fixture's graph.
const GRAPH_TRAVERSAL: &str =
    "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) COLUMNS (b.id AS t))";

fn store_files(base: &Path) -> Vec<(PathBuf, String)> {
    let stem = base.file_name().unwrap().to_string_lossy().into_owned();
    std::fs::read_dir(base.parent().unwrap())
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            name.strip_prefix(&stem)
                .map(|rest| (entry.path(), rest.to_owned()))
        })
        .collect()
}

fn copy_store(base: &Path, to: &Path) {
    for (from, rest) in store_files(base) {
        std::fs::copy(from, format!("{}{rest}", to.display())).unwrap();
    }
}

fn remove_store(path: &Path) {
    for (file, _) in store_files(path) {
        std::fs::remove_file(file).unwrap();
    }
}

fn says_corrupt(error: &Error) -> bool {
    matches!(error, Error::StoreCorrupted { .. }) || format!("{error}").contains("reset")
}

fn text_says_corrupt(text: &str) -> bool {
    let text = text.to_lowercase();
    text.contains("corrupt") || text.contains("reset")
}

/// The store is intact afterwards: it opens, and every seeded row is there.
fn assert_intact(path: &Path, why: &str) {
    let db = Database::open(path).unwrap_or_else(|error| panic!("{why}: reopen: {error:?}"));
    let rows = db
        .execute("SELECT id FROM t WHERE id < 200", &HashMap::new())
        .unwrap_or_else(|error| panic!("{why}: read back: {error:?}"))
        .rows()
        .len();
    assert_eq!(rows, 200, "{why}: committed rows survive");
    db.close().unwrap();
}

/// Run `operation` on a fresh copy of `base` once clean, counting its reads,
/// then once per read with that read failing with `kind` (EIO when None).
/// `check` sees every run that failed after its fault fired. Returns how many
/// runs failed, so a sweep that never reached a failing read is visible. With
/// `stop_at`, the sweep ends at the read count that the dry run stored there.
fn sweep<T>(
    base: &Path,
    label: &str,
    kind: Option<ErrorKind>,
    stop_at: Option<&AtomicU64>,
    operation: impl Fn(&Path) -> Result<(), T>,
    mut check: impl FnMut(u64, u64, T, &Path),
) -> usize
where
    T: std::fmt::Debug,
{
    let dir = base.parent().unwrap();
    let dry = dir.join(format!("{label}-dry.db"));
    copy_store(base, &dry);
    let (outcome, total) = fail_nth_writer_read_with_for_test(0, None, || operation(&dry));
    outcome.unwrap_or_else(|error| panic!("{label}: the clean run succeeds: {error:?}"));
    remove_store(&dry);
    assert!(total > 0, "{label}: the operation reads the writer file");
    let last = stop_at.map_or(total, |stop| stop.load(Ordering::SeqCst).min(total));
    assert!(last > 0, "{label}: the sweep has reads before its stop");
    let mut failed = 0;
    for nth in 1..=last {
        let path = dir.join(format!("{label}-{nth}.db"));
        copy_store(base, &path);
        let (outcome, reads) = fail_nth_writer_read_with_for_test(nth, kind, || operation(&path));
        if reads >= nth
            && let Err(error) = outcome
        {
            failed += 1;
            check(nth, total, error, &path);
        }
        remove_store(&path);
    }
    failed
}

/// Open, add one row, close: the shape of a context-graph import's reopen.
fn open_and_commit(path: &Path) -> contextdb_core::Result<()> {
    let db = Database::open(path)?;
    db.execute("INSERT INTO t VALUES ($id, $body)", &params(10_000, "new"))?;
    db.close()
}

/// Every read of open (the open mappers, format validation, writable-open
/// history recovery, the companion) and of the first commit, up to that
/// commit's durable point. Reads past AfterRedb reach the root publication,
/// which still panics on a read error. Returning a typed refusal there extends
/// this sweep to the whole commit.
#[test]
fn one_read_error_while_opening_and_committing_is_an_io_error_not_corruption() {
    use crate::persistence::read_persistence_test_scaffold::writer_reads_seen_for_test;
    use crate::writer_commit_test_seams::{CommitBoundary, observe};
    let dir = tempfile::tempdir().unwrap();
    let base = fixture(dir.path());
    let durable = Arc::new(AtomicU64::new(u64::MAX));
    let failed = sweep(
        &base,
        "open",
        None,
        Some(&*durable),
        |path| {
            let durable = durable.clone();
            observe(
                Arc::new(move |boundary| {
                    if boundary == CommitBoundary::AfterRedb {
                        durable.fetch_min(writer_reads_seen_for_test(), Ordering::SeqCst);
                    }
                }),
                || open_and_commit(path),
            )
        },
        |nth, total, error, path| {
            assert!(
                !says_corrupt(&error),
                "read {nth} of {total}: one read error must not say corrupt or advise reset: {error:?}"
            );
            // The I/O class: the original EIO, or the handle's own refusal after
            // a failed commit (PreviousIo, which says to reopen).
            let text = format!("{error:?}");
            assert!(
                text.contains("os error 5") || text.contains("Previous I/O error"),
                "read {nth} of {total}: reported as the I/O failure it is: {error:?}"
            );
            assert_intact(path, &format!("after a failed read {nth}"));
        },
    );
    assert!(
        failed > 0,
        "some read of open or commit failed the operation"
    );
}

/// A short read or rejected bytes while opening are damage and say so.
#[test]
fn a_short_or_invalid_read_while_opening_is_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let base = fixture(dir.path());
    for kind in [ErrorKind::UnexpectedEof, ErrorKind::InvalidData] {
        let failed = sweep(
            &base,
            &format!("open-{kind:?}"),
            Some(kind),
            None,
            |path| Database::open(path).and_then(|db| db.close()),
            |nth, total, error, _| {
                assert!(
                    matches!(error, Error::StoreCorrupted { .. }),
                    "{kind:?} at read {nth} of {total} is damage: {error:?}"
                );
            },
        );
        assert!(failed > 0, "{kind:?}: some read of open failed it");
    }
}

fn bounded_read(db: &Database, sql: &str) -> Result<(), bounded::TestError> {
    let request = bounded::BoundedReadRequest::new(
        sql,
        HashMap::new(),
        contextdb_core::read_contract::ReadLimits::default(),
        Arc::new(FrozenClock),
    );
    bounded::execute(db, &request).map(|_| ())
}

/// As [`sweep`], but each run opens its copy cleanly first and only the reads
/// of `query` are counted and faulted. `check` sees the failure's Debug text.
fn sweep_query<T: std::fmt::Debug>(
    base: &Path,
    label: &str,
    kind: Option<ErrorKind>,
    query: impl Fn(&Database) -> Result<(), T>,
    mut check: impl FnMut(u64, u64, String),
) -> usize {
    let dir = base.parent().unwrap();
    let run = |path: &Path, nth: u64, kind: Option<ErrorKind>| {
        let db = Database::open(path).unwrap_or_else(|error| panic!("{label}: open: {error:?}"));
        let (outcome, reads) = fail_nth_writer_read_with_for_test(nth, kind, || query(&db));
        drop(db);
        (outcome.map_err(|error| format!("{error:?}")), reads)
    };
    let dry = dir.join(format!("{label}-dry.db"));
    copy_store(base, &dry);
    let (outcome, total) = run(&dry, 0, None);
    outcome.unwrap_or_else(|error| panic!("{label}: the clean run succeeds: {error}"));
    remove_store(&dry);
    assert!(total > 0, "{label}: the query reads the writer file");
    let mut failed = 0;
    for nth in 1..=total {
        let path = dir.join(format!("{label}-{nth}.db"));
        copy_store(base, &path);
        let (outcome, reads) = run(&path, nth, kind);
        if reads >= nth
            && let Err(error) = outcome
        {
            failed += 1;
            check(nth, total, error);
        }
        remove_store(&path);
    }
    failed
}

fn assert_io_sweep<T: std::fmt::Debug>(
    base: &Path,
    label: &str,
    query: impl Fn(&Database) -> Result<(), T>,
) {
    let failed = sweep_query(base, label, None, query, |nth, total, error| {
        assert!(
            !text_says_corrupt(&error),
            "{label}: read {nth} of {total}: one read error must not say corrupt: {error}"
        );
        assert!(
            error.contains("os error 5") || error.contains("Previous I/O error"),
            "{label}: read {nth} of {total}: reported as the I/O failure it is: {error}"
        );
    });
    assert!(failed > 0, "{label}: some read of the query failed it");
}

/// Content-size queries on the writer and on a captured root, a vector scan,
/// and a historical row read: receivers that used to turn I/O into corruption.
#[test]
fn one_read_error_in_a_content_size_query_or_vector_scan_is_io() {
    let dir = tempfile::tempdir().unwrap();
    let base = fixture(dir.path());
    assert_io_sweep(&base, "writer-content-size", |db| {
        db.execute("SHOW CONTENT_SIZE", &HashMap::new()).map(|_| ())
    });
    assert_io_sweep(&base, "captured-content-size", |db| {
        bounded_read(db, "SHOW CONTENT_SIZE")
    });
    assert_io_sweep(&base, "vector-scan", |db| {
        bounded_read(
            db,
            "SELECT id FROM vecs ORDER BY embedding <=> [1,0,0] USE VECTOR EXACT LIMIT 5",
        )
    });
}

/// The writer's historical row scan validates version bounds against the
/// per-table bounds table; a failed read there is I/O, not damaged history.
#[test]
fn one_read_error_in_a_writer_history_scan_is_io() {
    let dir = tempfile::tempdir().unwrap();
    let base = fixture(dir.path());
    assert_io_sweep(&base, "writer-history", |db| {
        db.execute("SELECT id, body FROM h", &HashMap::new())
            .map(|_| ())
    });
}

/// A cold authenticated read on a captured root and a checkpoint export: an
/// unreadable proof page is I/O. A proof that disagrees with stored content
/// stays corruption (engine_seams durable_reads_validate_the_whole_evidence_closure).
#[test]
fn one_read_error_in_a_proof_page_is_io() {
    let dir = tempfile::tempdir().unwrap();
    let base = fixture(dir.path());
    assert_io_sweep(&base, "captured-proof-read", |db| {
        bounded_read(db, "SELECT id, body FROM h")
    });
    let export = dir.path().join("export.db");
    assert_io_sweep(&base, "checkpoint-export", |db| {
        let outcome = db.export_snapshot(&export).map(|_| ());
        for (file, _) in store_files(&export) {
            std::fs::remove_file(file).unwrap();
        }
        outcome
    });
}

/// At runtime too, a short read or rejected bytes are damage; EIO is I/O.
#[test]
fn a_runtime_short_or_invalid_read_is_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let base = fixture(dir.path());
    for kind in [ErrorKind::UnexpectedEof, ErrorKind::InvalidData] {
        let failed = sweep_query(
            &base,
            &format!("runtime-{kind:?}"),
            Some(kind),
            |db: &Database| {
                db.execute("SELECT id, body FROM t", &HashMap::new())
                    .map(|_| ())
            },
            |nth, total, error| {
                assert!(
                    text_says_corrupt(&error),
                    "{kind:?} at runtime read {nth} of {total} is damage: {error}"
                );
            },
        );
        assert!(failed > 0, "{kind:?}: some runtime read failed the query");
    }
}

/// The advisory companion read during open: EIO is I/O. A short read at the
/// open-recovery read is a companion that cannot be recovered; the reader
/// coordination check keeps its own typed refusal for an unreadable companion.
#[test]
fn one_read_error_on_the_companion_is_io_and_a_short_read_is_damage() {
    use crate::persistence::read_persistence_test_scaffold::fail_nth_companion_read_for_test;
    let dir = tempfile::tempdir().unwrap();
    let base = fixture(dir.path());
    let dry = dir.path().join("companion-dry.db");
    copy_store(&base, &dry);
    let (opened, total) = fail_nth_companion_read_for_test(0, None, || Database::open(&dry));
    opened.unwrap().close().unwrap();
    assert!(total > 0, "open reads the companion");
    let mut io = 0;
    let mut damage = 0;
    for nth in 1..=total {
        for kind in [None, Some(ErrorKind::UnexpectedEof)] {
            let path = dir
                .path()
                .join(format!("companion-{nth}-{}.db", kind.is_some()));
            copy_store(&base, &path);
            let (opened, reads) =
                fail_nth_companion_read_for_test(nth, kind, || Database::open(&path));
            match opened {
                Err(error) if reads >= nth && kind.is_none() => {
                    io += 1;
                    assert!(
                        !says_corrupt(&error),
                        "companion read {nth} of {total}: EIO must not say corrupt: {error:?}"
                    );
                    assert_intact(&path, &format!("after companion read {nth} failed"));
                }
                Err(error @ Error::ReaderCoordinationUnavailable { .. }) if reads >= nth => {
                    assert!(!says_corrupt(&error), "companion read {nth}: {error:?}");
                }
                Err(error) if reads >= nth => {
                    damage += 1;
                    assert!(
                        matches!(error, Error::StoreCorrupted { .. }),
                        "companion read {nth} of {total}: a short read is damage: {error:?}"
                    );
                }
                Err(error) => panic!("companion read {nth}: failed before the fault: {error:?}"),
                Ok(db) => db.close().unwrap(),
            }
            remove_store(&path);
        }
    }
    assert!(
        io > 0 && damage > 0,
        "the companion fault reached open ({io} io, {damage} damage)"
    );
}

/// Received-schema application on a vector table reads the vector-pages
/// binding (vector_paged.rs validate_pages_root, through
/// received_changed_vector_incarnations); an unreadable binding is I/O.
#[test]
fn one_read_error_reading_the_vector_pages_binding_is_io() {
    let dir = tempfile::tempdir().unwrap();
    let base = fixture(dir.path());
    assert_io_sweep(&base, "received-vector-binding", |db| {
        let meta = db.table_meta("vecs").expect("the vector table's meta");
        db.replay_guard_persistence_for_test()
            .received_changed_vector_incarnations("vecs", &meta, &[])
            .map(|_| ())
    });
}

/// A refusal of the memory class, by category: a typed budget refusal or a
/// read failure for the memory limit. Never corruption.
fn says_memory(text: &str) -> bool {
    text.contains("MemoryBudgetExceeded")
        || text.contains("ProcessMemoryBudgetExceeded")
        || (text.contains("ReadFailure") && text.contains("Memory"))
        || text.contains("the read went past the memory it is allowed")
}

/// Step a memory budget up from `start` until `operation` fits. Every refusal on
/// the way is the memory class and never says corrupt. Returns the refusals.
fn memory_sweep(
    label: &str,
    start: u64,
    mut operation: impl FnMut(u64) -> Result<(), String>,
) -> usize {
    let mut limit = start;
    let mut refusals = 0;
    for _ in 0..400 {
        match operation(limit) {
            Ok(()) => return refusals,
            Err(text) => {
                assert!(
                    !text_says_corrupt(&text) && says_memory(&text),
                    "{label}: a refusal at {limit} bytes is the memory class, never corruption: {text}"
                );
                refusals += 1;
            }
        }
        limit += limit / 4 + 1024;
    }
    panic!("{label}: the operation never fitted");
}

fn bounded_read_within(db: &Database, sql: &str, memory: u64) -> Result<(), String> {
    let request = bounded::BoundedReadRequest::new(
        sql,
        HashMap::new(),
        contextdb_core::read_contract::ReadLimits {
            memory,
            ..contextdb_core::read_contract::ReadLimits::default()
        },
        Arc::new(FrozenClock),
    );
    bounded::execute(db, &request)
        .map(|_| ())
        .map_err(|error| format!("{error:?}"))
}

/// Captured-root reads under a stepped reader memory limit: content size, a
/// vector scan, a history read, an indexed lookup and a graph traversal. (No
/// SQL reaches the ordered-run index opens on this train; the ordered-run rows
/// below cover those.)
#[test]
fn a_captured_read_refused_for_memory_is_the_memory_class_never_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let base = fixture(dir.path());
    let db = Database::open(&base).unwrap();
    db.execute("CREATE INDEX h_body ON h (body)", &HashMap::new())
        .unwrap();
    for (label, sql) in [
        ("captured content size", "SHOW CONTENT_SIZE"),
        (
            "captured vector scan",
            "SELECT id FROM vecs ORDER BY embedding <=> [1,0,0] USE VECTOR EXACT LIMIT 5",
        ),
        ("captured history read", "SELECT id, body FROM h"),
        (
            "captured index lookup",
            "SELECT id FROM h WHERE body = 'second'",
        ),
        ("captured graph traversal", GRAPH_TRAVERSAL),
    ] {
        let refusals = memory_sweep(label, 4096, |memory| bounded_read_within(&db, sql, memory));
        assert!(refusals > 0, "{label}: some limit refused the read");
    }
    db.close().unwrap();
}

/// Writer reads under a stepped MEMORY_LIMIT: content size, the history scan,
/// the received vector-pages binding and a graph traversal.
#[test]
fn a_writer_read_refused_for_memory_is_the_memory_class_never_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let base = fixture(dir.path());
    let db = Database::open(&base).unwrap();
    let none = HashMap::new();
    let floor = db.accountant().usage().used as u64 + 64 * 1024;
    let set = |db: &Database, limit: u64| {
        db.execute(
            &format!("SET MEMORY_LIMIT '{}K'", limit.div_ceil(1024)),
            &HashMap::new(),
        )
        .unwrap();
    };
    let ops: [(&str, &dyn Fn(&Database) -> Result<(), String>); 4] = [
        ("writer content size", &|db| {
            db.execute("SHOW CONTENT_SIZE", &HashMap::new())
                .map(|_| ())
                .map_err(|e| format!("{e:?}"))
        }),
        ("writer history scan", &|db| {
            db.execute("SELECT id, body FROM h", &HashMap::new())
                .map(|_| ())
                .map_err(|e| format!("{e:?}"))
        }),
        ("writer graph traversal", &|db| {
            db.execute(GRAPH_TRAVERSAL, &HashMap::new())
                .map(|_| ())
                .map_err(|e| format!("{e:?}"))
        }),
        ("received vector binding", &|db| {
            let meta = db.table_meta("vecs").expect("the vector table's meta");
            db.replay_guard_persistence_for_test()
                .received_changed_vector_incarnations("vecs", &meta, &[])
                .map(|_| ())
                .map_err(|e| format!("{e:?}"))
        }),
    ];
    for (label, op) in ops {
        memory_sweep(label, floor, |limit| {
            set(&db, limit);
            op(&db)
        });
    }
    db.execute("SET MEMORY_LIMIT 'none'", &none).unwrap();
    db.close().unwrap();
}

/// Open under a stepped declared memory limit: the open mappers and format
/// validation report a refusal as the memory class, never corruption.
#[test]
fn an_open_refused_for_memory_is_the_memory_class_never_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let base = fixture(dir.path());
    let mut refusals = 0;
    let mut limit = 1u64;
    loop {
        let options = crate::DatabaseOpenOptions {
            memory_limit: Some(limit as usize),
            ..crate::DatabaseOpenOptions::default()
        };
        match Database::open_with_options(&base, options) {
            Ok(db) => {
                drop(db);
                break;
            }
            Err(error) => {
                let text = format!("{error:?}");
                assert!(
                    !text_says_corrupt(&text) && says_memory(&text),
                    "open: a refusal at {limit} bytes is the memory class, never corruption: {text}"
                );
                refusals += 1;
            }
        }
        assert!(limit < 1 << 30, "open never fitted");
        limit += if limit < 1 << 20 { 4096 } else { limit / 16 };
    }
    assert!(refusals > 0, "some declared limit refused the open");
}

/// A captured indexed lookup: an unreadable page is I/O. On this train no SQL
/// reaches the ordered-run index opens in retained_read_source (they are
/// covered by the ordered-run rows below), so this row covers the lookup only.
#[test]
fn one_read_error_in_a_captured_index_lookup_is_io() {
    let dir = tempfile::tempdir().unwrap();
    let base = fixture(dir.path());
    let db = Database::open(&base).unwrap();
    db.execute("CREATE INDEX h_body ON h (body)", &HashMap::new())
        .unwrap();
    db.close().unwrap();
    assert_io_sweep(&base, "captured-index-lookup", |db| {
        bounded_read(db, "SELECT id FROM h WHERE body = 'second'")
    });
}

/// A table with one secondary index, closed, for the ordered-run rows.
fn ordered_fixture(path: &Path) {
    let db = Database::open(path).unwrap();
    let none = HashMap::new();
    db.execute(
        "CREATE TABLE postings (id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL)",
        &none,
    )
    .unwrap();
    db.execute("CREATE INDEX by_bucket ON postings(bucket)", &none)
        .unwrap();
    for id in 0..40i64 {
        db.execute(
            "INSERT INTO postings VALUES ($id, 1)",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    db.close().unwrap();
}

/// Load a fresh read image of `path` under `memory` and pull one row of the
/// verified ordered run over `by_bucket`, which opens the retained physical and
/// current index tables (retained_read_source index opens).
fn ordered_pull(path: &Path, memory: Option<usize>) -> Result<(), String> {
    use contextdb_relational::mem::BoundedOrderedRowCursor;
    let load =
        super::load_read_image_with_memory(path, memory).map_err(|e| format!("load: {e:?}"))?;
    let meta = &load.image.table_meta["postings"];
    let declaration = super::RedbPersistence::relational_index_declarations(meta)
        .into_iter()
        .find(|d| d.name == "by_bucket")
        .unwrap();
    let snapshot = contextdb_core::SnapshotId(
        load.image
            .newest_committed_tx_for_test()
            .map(|tx| tx.0)
            .unwrap(),
    );
    let parent = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(
        load.source
            .verified_ordered_runs
            .memory
            .accountant
            .usage()
            .limit
            .map(|bytes| bytes as u64)
            .unwrap_or(u64::MAX),
    )
    .map_err(|e| format!("declare: {e:?}"))?;
    let child = parent.child().map_err(|e| format!("child: {e:?}"))?;
    let mut cursor = BoundedOrderedRowCursor::for_index("by_bucket".into(), false);
    cursor.select_identities_only();
    cursor.source_configure(1);
    load.source
        .bounded_relational_ordered_next(
            "postings",
            meta,
            &declaration,
            snapshot,
            &mut cursor,
            &mut || Ok(()),
            &mut || Ok(()),
            &mut |_| Ok(()),
            child,
        )
        .map(|_| ())
        .map_err(|e: Error| format!("pull: {e:?}"))
}

/// The ordered run's index opens under a stepped declared memory, each attempt
/// on a fresh read image so the index roots are cold. On 4e1f5c7b the current
/// index open answered a page-admission refusal with StoreCorrupted ("cannot
/// read current index by_bucket: native storage admission refused", at 286208
/// bytes), and the image load answered a native memory budget refusal with
/// Corrupt MalformedRecord. Every refusal, at load or at the pull, is now a
/// refusal and never corruption.
#[test]
fn an_ordered_index_run_refused_for_memory_is_the_memory_class_never_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ordered.db");
    ordered_fixture(&path);
    let mut refusals = 0;
    let mut limit = 0u64;
    loop {
        match ordered_pull(&path, Some(limit as usize)) {
            Ok(()) => break,
            Err(text) => {
                assert!(
                    !text_says_corrupt(&text) && says_memory(&text),
                    "ordered index run: a refusal at {limit} bytes is a refusal, never corruption: {text}"
                );
                refusals += 1;
            }
        }
        assert!(limit < 1 << 30, "the ordered index run never fitted");
        limit += if limit < 1 << 20 { 512 } else { limit / 64 };
    }
    assert!(refusals > 0, "some declared memory refused the ordered run");
}

/// Damage control for the same opens: an index table that is absent from the
/// committed store is still reported as corruption.
#[test]
fn an_absent_index_table_at_the_ordered_run_is_still_corruption() {
    for current in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ordered.db");
        ordered_fixture(&path);
        let name = if current {
            super::RedbPersistence::current_relational_index_table_name("postings", "by_bucket")
        } else {
            super::RedbPersistence::relational_index_table_name("postings", "by_bucket")
        };
        {
            let _scope = crate::read_image_memory::ImageMemory::enter_writer_operation(
                crate::memory_accounting::AccountantHandle::standalone(None, true).unwrap(),
            )
            .unwrap();
            let raw = redb::Database::create(&path).unwrap();
            let write = raw.begin_write().unwrap();
            let definition: redb::TableDefinition<super::DurableRelationalIndexKey, &[u8]> =
                redb::TableDefinition::new(name.as_str());
            assert!(write.delete_table(definition).unwrap(), "{name} existed");
            write.commit().unwrap();
        }
        let outcome = ordered_pull(&path, Some(256 * 1024 * 1024));
        let text = outcome.expect_err("an absent index table refuses the run");
        assert!(
            text_says_corrupt(&text),
            "absent {} index: still corruption: {text}",
            if current { "current" } else { "physical" }
        );
    }
}

/// Graph traversals on the writer and on a captured root, with the graph proof
/// diagnosis (graph_read GraphProofContext::diagnose): an unreadable graph page
/// is I/O, never corruption.
#[test]
fn one_read_error_in_a_graph_traversal_is_io() {
    let dir = tempfile::tempdir().unwrap();
    let base = fixture(dir.path());
    assert_io_sweep(&base, "writer-graph-traversal", |db| {
        db.execute(GRAPH_TRAVERSAL, &HashMap::new()).map(|_| ())
    });
    assert_io_sweep(&base, "captured-graph-traversal", |db| {
        bounded_read(db, GRAPH_TRAVERSAL)
    });
}
