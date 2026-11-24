//! A store is one folder: made with no configuration, the same after close and reopen, and held
//! for writing by one process at a time, for as long as that process has it open and no longer.

mod support;

use contextdb::{Database, DatabaseOpenOptions, Error, Value, params};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use support::{answer, int, printed, rows, run};

#[test]
fn a_fresh_store_is_one_folder_and_works_with_no_configuration() {
    let around = support::folder("fresh-store");
    let folder = around.join("notes");
    let db = support::open(&folder);

    // Nothing was declared, so no table is there: not the user's, and none the engine brought.
    assert!(
        support::names(&db).is_empty(),
        "a fresh store has the tables {:?}",
        support::names(&db)
    );
    match support::refusal(&db, "SELECT * FROM notes") {
        Error::TableNotFound(table) => assert_eq!(table, "notes"),
        other => panic!("a fresh store answered {other:?} when asked for a table"),
    }

    support::run(&db, support::NOTES);
    assert_eq!(support::names(&db), ["notes"]);
    db.execute(
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &params! { "id" => 1_i64, "body" => "buy milk" },
    )
    .expect("insert into a fresh store");
    assert_eq!(
        support::rows(&db, "SELECT id, body FROM notes ORDER BY id"),
        [[Value::Int64(1), Value::Text("buy milk".to_owned())]]
    );

    // The store is the folder it was opened at. Nothing was written beside it.
    assert_eq!(support::listing(&around), ["notes"]);
    assert_eq!(
        support::listing(&folder),
        ["data", "lock", "readers", "writer"],
        "the store is its data, the writer's lock, the readers' files and the writer's mark"
    );
    support::close(db);
}

#[test]
fn what_was_committed_is_there_after_close_and_reopen() {
    let folder = support::folder("close-and-reopen");
    let db = support::open(&folder);
    support::declare(&db);
    support::insert(&db, 1..=20);
    support::insert_together(&db, 21..=40);
    support::run(&db, "DELETE FROM t WHERE id > 35");
    support::close(db);

    let db = support::open(&folder);
    support::assert_rows_are(&db, 1..=35);

    // The reopened store goes on from there.
    support::insert(&db, 36..=36);
    support::close(db);
    support::assert_rows_are(&support::open(&folder), 1..=36);

    // A handle closed here refuses by name, a second close is allowed.
    let db = support::open(&folder);
    let tx = db.transaction().expect("begin");
    db.close().expect("close");
    db.close().expect("close a second time");
    let select = "SELECT id FROM t";
    assert!(matches!(
        support::refusal(&db, select),
        Error::DatabaseClosed
    ));
    assert!(matches!(
        support::refusal(&tx, select),
        Error::DatabaseClosed
    ));
    assert!(matches!(tx.commit(), Err(Error::DatabaseClosed)));
    assert!(matches!(db.transaction(), Err(Error::DatabaseClosed)));
    // Closes from many threads at once all succeed, and the folder then opens for writing.
    std::thread::scope(|s| {
        (0..4).for_each(|_| _ = s.spawn(|| db.close().expect("a close from any thread")));
    });
    support::assert_rows_are(&support::open(&folder), 1..=36);
}

#[test]
fn an_ivf_is_there_after_close_and_reopen() {
    let (folder, db) = support::built("ivf-after-reopen", 16, "INDEXING_THRESHOLD 1000", 2000);
    let query = support::gathered(99_000, 16);
    let sql = support::by_mode(None, "INDEXED", 10);
    // The partitions as inspection lists them, and an indexed search's rows and work.
    let state = |db: &contextdb::Database| {
        let result = support::answer(db, &sql, &query);
        (
            support::shown(db, "SHOW VECTOR_PARTITIONS"),
            result.rows().to_vec(),
            result.distance_evaluations(),
            support::watermark(db),
        )
    };
    let before = state(&db);
    assert_eq!(support::field(&before.0[0], "lists"), &Value::Int64(45));
    support::close(db);

    let db = support::open(&folder);
    assert!(state(&db) == before, "the IVF after reopen");
    // Nothing is left to build: a cycle makes no commit.
    assert_eq!(support::maintenance(&db), (1, 0));
    assert!(state(&db) == before, "after one more cycle");
}

// Extends the reopen journey above: no existing test copies a pinned view to a new store.
#[test]
fn an_exported_snapshot_opens_and_answers_as_its_source_did() {
    let (folder, db) = support::built("export-source", 16, "INDEXING_THRESHOLD 1000", 2000);
    support::close(db);
    let now = Arc::new(AtomicU64::new(100));
    let mut options = DatabaseOpenOptions::default();
    options.clock = Some(Arc::new(support::At(now.clone())));
    let db = support::open_with(&folder, options.clone()).unwrap();
    run(&db, "CREATE SCHEDULE timer EVERY '10 seconds' TX (advance)");
    run(&db, "CREATE INDEX docs_tag ON docs (ctx, tag)");
    run(&db, "ALTER TABLE docs SET MAINTAIN COUNT (ctx)");
    support::insert_doc(&db, 2001, "a", Some(support::gathered(2001, 16))).unwrap();
    run(
        &db,
        "CREATE TABLE edges (source_id INTEGER REFERENCES docs(id), \
        target_id INTEGER REFERENCES docs(id), edge_type TEXT, PRIMARY KEY (source_id, target_id))",
    );
    run(
        &db,
        "INSERT INTO edges VALUES (1, 2, 'CITES'), (2, 2001, 'CITES')",
    );
    for sql in [
        "CREATE TABLE links (id INTEGER PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL)",
        "INSERT INTO links VALUES (1, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'CITES')",
        "ALTER TABLE links RENAME COLUMN edge_type TO kind",
    ] {
        run(&db, sql);
    }
    run(
        &db,
        "CREATE TABLE payloads (id INTEGER PRIMARY KEY, body TEXT, doc JSON)",
    );
    let data = params! {
        "body" => "large text".repeat(4096),
        "doc" => contextdb::serde_json::json!({"text": "large JSON".repeat(4096)})
    };
    db.execute("INSERT INTO payloads VALUES (1, $body, $doc)", &data)
        .unwrap();
    std::fs::write(folder.join("identity.key"), b"source secret").unwrap();
    let around = support::folder("export-copies");
    let dest = around.join("copy");
    let before = support::watermark(&db);
    let checked = db.verify().unwrap();
    db.export_snapshot(&dest).expect("export every record kind");
    assert_eq!(support::watermark(&db), before);
    assert!(!dest.join("identity.key").exists());
    now.store(109, SeqCst);
    let copy = support::open_with(&dest, options).unwrap();
    assert_eq!(*copy.verify().unwrap(), *checked);
    assert_eq!(support::names(&copy), support::names(&db));
    for table in support::names(&db) {
        assert_eq!(printed(&copy, &table), printed(&db, &table));
    }
    for sql in [
        "SELECT * FROM docs ORDER BY id",
        "SELECT * FROM edges ORDER BY source_id, target_id",
        "SELECT * FROM payloads ORDER BY id",
        "SELECT id FROM docs WHERE ctx = 'a' AND tag = 7 ORDER BY id",
        "SELECT ctx, COUNT(*) FROM docs GROUP BY ctx",
        "SHOW VECTOR_INDEXES",
        "SHOW VECTOR_PARTITIONS",
    ] {
        assert_eq!(rows(&copy, sql), rows(&db, sql), "{sql}");
    }
    let graph = "SELECT 1 FROM GRAPH_TABLE(links MATCH (a)-[:CITES]->(b) COLUMNS (b.id AS i))";
    let refused =
        |on: &contextdb::Database| on.execute(graph, &params! {}).unwrap_err().to_string();
    assert_eq!(refused(&copy), refused(&db), "the same relationships: none");
    let sql = support::by_mode(None, "INDEXED", 10);
    let q = support::gathered(99_000, 16);
    assert_eq!(answer(&copy, &sql, &q).rows(), answer(&db, &sql, &q).rows());
    for (time, due) in [(109, false), (110, true)] {
        now.store(time, SeqCst);
        assert_eq!(copy.run_maintenance_cycle().is_err(), due);
    }
    support::insert_doc(&copy, 2002, "a", Some(q)).unwrap();
    assert!(db.export_snapshot(&dest).is_err(), "destination exists");
    support::close(copy);
    let copy = support::open(&dest);
    assert_eq!(int(&copy, "SELECT COUNT(*) FROM docs"), 2002);
    assert_eq!(int(&db, "SELECT COUNT(*) FROM docs"), 2001);
    support::close(copy);
    support::close(db);
    let db = support::open_limited(&folder, support::opening::opening_charge(&folder, None));
    match db.export_snapshot(around.join("no-room")).map(drop) {
        Err(error @ Error::MemoryBudgetExceeded { .. }) => {
            assert!(error.to_string().contains("MEMORY_LIMIT"), "{error}");
        }
        other => panic!("export with no spare memory gave {other:?}"),
    }
    support::close(db);
    let armed = Arc::new(AtomicBool::new(false));
    let storage = support::storage({
        let armed = armed.clone();
        move |call| {
            if let support::Call::Read(_, bytes) = call
                && armed.load(SeqCst)
                && let Some(byte) = bytes.get_mut(16)
            {
                *byte ^= 0xFF;
            }
        }
    });
    let mut options = DatabaseOpenOptions::default();
    options.storage = Some(storage);
    options.memory_limit = Some(64 << 20);
    let db = support::open_with(&folder, options).unwrap();
    armed.store(true, SeqCst);
    match db.export_snapshot(around.join("damaged")).map(drop) {
        Err(Error::StoreCorrupted { path, .. }) => assert_eq!(path, folder),
        other => panic!("damage met during export gave {other:?}"),
    }
    armed.store(false, SeqCst);
    support::close(db);
}

#[test]
fn the_storage_fork_keeps_its_own_manifest_and_a_path_named_memory_is_a_folder() {
    let v2 = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let read = |path: &str| std::fs::read_to_string(v2.join(path)).unwrap();
    assert!(read("Cargo.toml").contains("exclude = [\"redb\"]"));
    let fork = read("redb/Cargo.toml");
    assert!(fork.contains("name = \"contextdb-redb\"") && fork.contains("version = \"4.1.0\""));
    assert!(fork.contains("MIT OR Apache-2.0") && v2.join("redb/Cargo.lock").exists());
    let engine = read("contextdb/Cargo.toml");
    assert_eq!(engine.matches("package = ").count(), 1);
    assert!(engine.contains("\"contextdb-redb\", version = \"4.1.0\", path"));
    assert!(!read("contextdb-cli/Cargo.toml").contains("package = "));
    // A store works with no internet: no network crate is a dependency that is always built.
    for net in ["iroh", "quinn", "tokio", "hyper", "reqwest"] {
        let always = engine
            .lines()
            .filter(|l| l.starts_with(net) && !l.contains("optional"));
        assert_eq!(always.count(), 0, "{net} is a dependency of every build");
    }
    // There is no in-memory door: this name is a durable folder.
    let memory = support::folder("named-memory").join(":memory:");
    support::close(support::open(&memory));
    assert!(memory.is_dir());
}

#[test]
fn the_thread_count_follows_open_handles_only() {
    // The clock given at open is held by every thread the handle starts; once the handle closes
    // none holds it, so the engine's worker is gone with it.
    let clock = Arc::new(support::At(Arc::default()));
    let mut options = DatabaseOpenOptions::default();
    options.maintenance = contextdb::Maintenance::Engine;
    options.clock = Some(clock.clone());
    let db = Database::open_with_options(support::folder("threads-engine"), options).unwrap();
    assert!(
        Arc::strong_count(&clock) > 2,
        "the handle and its worker hold the clock"
    );
    // An import and an append leave the threads as they were (intent:55-59, design 8.3).
    support::declare(&db);
    let census = support::threads();
    support::insert_together(&db, 1..=1000);
    support::insert_together(&db, 1001..=1001);
    assert_eq!(support::threads(), census, "after the append");
    support::close(db);
    assert_eq!(
        Arc::strong_count(&clock),
        1,
        "no thread outlives the handle"
    );
}

#[test]
fn the_engine_never_replaces_the_hosts_panic_hook() {
    let seen = Arc::new(AtomicU64::new(0));
    let counted = seen.clone();
    std::panic::set_hook(Box::new(move |info| {
        let own = info.payload().downcast_ref::<&str>();
        if own.is_some_and(|text| text.starts_with("hook-probe")) {
            counted.fetch_add(1, SeqCst);
        }
    }));
    let probe = || std::panic::catch_unwind(|| panic!("hook-probe")).unwrap_err();
    let mut options = DatabaseOpenOptions::default();
    options.storage = Some(support::storage(|_| panic!("hook-probe in the door")));
    let door =
        std::panic::AssertUnwindSafe(|| support::open_with(&support::folder("hook-door"), options));
    let _ = std::panic::catch_unwind(door);
    let doors = seen.load(SeqCst);
    assert!(doors > 0, "the door's panic reached the hook");
    support::close(support::open(&support::folder("hook-plain")));
    probe();
    assert_eq!(seen.load(SeqCst), doors + 1, "the host's hook is installed");
}

#[test]
fn the_content_size_is_the_live_bytes_and_its_walk_reads_each_record_once() {
    let size = |db: &Database| rows(db, "SHOW CONTENT_SIZE");
    let twin = |name: &str, live: std::ops::RangeInclusive<i64>| {
        let db = support::open(&support::folder(name));
        support::declare_current_only(&db);
        support::insert_together(&db, live);
        size(&db)
    };
    let folder = support::folder("content-history");
    let db = support::open(&folder);
    support::declare_current_only(&db);
    support::insert_together(&db, 1..=300);
    let full = twin("content-300", 1..=300);
    assert_eq!(size(&db), full, "after the inserts");
    run(&db, "UPDATE t SET name = name WHERE id <= 100");
    assert_eq!(size(&db), full, "a replaced row is counted once");
    run(&db, "DELETE FROM t WHERE id > 250");
    run(&db, "PURGE FROM t WHERE id > 200");
    assert_eq!(size(&db), twin("content-200", 1..=200), "live rows only");
    // A table that declares no history keeps a deleted row's versions, there to read (entry 5).
    let kept = support::open(&support::folder("content-kept"));
    support::declare(&kept);
    support::insert_together(&kept, 1..=1);
    run(&kept, "DELETE FROM t WHERE id = 1");
    let versions = kept.row_versions("t", &[Value::Int64(1)], 1).unwrap();
    assert!(!versions.rows.rows().is_empty(), "history is kept");
    let Value::Int64(bytes) = size(&db)[0][7] else {
        panic!()
    };
    assert!(
        bytes as u64 <= support::data_file(&folder),
        "within the pages"
    );
    // The walk reads each record once: no more bytes than the store holds, at either size.
    for rows in [500, 5000] {
        let folder = support::folder(&format!("content-walk-{rows}"));
        let db = support::open(&folder);
        support::declare(&db);
        support::insert_together(&db, 1..=rows);
        support::close(db);
        let (door, read) = support::counting_bytes_read();
        let mut options = DatabaseOpenOptions::default();
        options.storage = Some(door);
        let db = support::open_with(&folder, options).unwrap();
        let opened = read.load(SeqCst);
        size(&db);
        let walked = read.load(SeqCst) - opened;
        assert!(
            walked <= support::data_file(&folder),
            "{rows} rows read once"
        );
    }
}

/// A plug-in whose open or close hook fails as set; it counts its closes and keeps every
/// statement it is shown.
#[derive(Default)]
struct Hooks {
    open: bool,
    close: bool,
    closes: AtomicU64,
    heard: std::sync::Mutex<Vec<String>>,
}
impl contextdb::DatabasePlugin for Hooks {
    fn on_open(&self) -> contextdb::Result<()> {
        self.open
            .then(|| Error::PlanError("open hook".into()))
            .map_or(Ok(()), Err)
    }
    fn on_close(&self) -> contextdb::Result<()> {
        self.closes.fetch_add(1, SeqCst);
        self.close
            .then(|| Error::PlanError("close hook".into()))
            .map_or(Ok(()), Err)
    }
    fn on_query(&self, sql: &str) -> contextdb::Result<()> {
        self.heard.lock().unwrap().push(sql.into());
        Ok(())
    }
}

/// Who is told of a failed close (section 7, C2): a holder that is not the last closes nothing
/// and succeeds; the last holder's close returns the drive's failure, else the close hook's, else
/// success; and the file is closed whatever failed before it.
#[test]
fn the_last_close_returns_the_first_failure_and_closes_the_file() {
    for case in ["", "sync", "sync hook", "hook", "close"] {
        let (fails, hook) = (case.trim_end_matches("hook").trim(), case.contains("hook"));
        let folder = support::folder(&format!("last-close-{fails}-{hook}"));
        let armed = Arc::new(AtomicBool::new(false));
        let closed = Arc::new(AtomicBool::new(false));
        let (refusing, seen) = (armed.clone(), closed.clone());
        let refused = format!("the drive refused the {fails}");
        let mut options = DatabaseOpenOptions::default();
        options.plugin = Some(Arc::new(Hooks {
            close: hook,
            ..Default::default()
        }));
        options.storage = Some(support::failing_storage(move |call| {
            let call = match call {
                support::Call::Sync => "sync",
                support::Call::Close => {
                    seen.store(true, SeqCst);
                    "close"
                }
                _ => "",
            };
            match refusing.load(SeqCst) && !call.is_empty() && call == fails {
                true => Err(std::io::Error::other(refused.clone())),
                false => Ok(()),
            }
        }));
        let db = support::open_with(&folder, options).unwrap();
        support::declare(&db);
        support::insert(&db, 1..=3);
        let last = db.scoped_with_constraints(None, None, None);
        armed.store(true, SeqCst);
        assert!(
            db.close().is_ok(),
            "a close that is not the last closes nothing"
        );
        assert_eq!(
            int(&last, "SELECT COUNT(*) FROM t"),
            3,
            "the store answers the last holder"
        );
        let result = last.close();
        match (fails, hook) {
            ("", false) => assert!(result.is_ok(), "a sound close succeeds: {result:?}"),
            ("", true) => assert!(
                matches!(&result, Err(Error::PlanError(m)) if m == "close hook"),
                "a hook that fails on a sound drive is what the last close returns: {result:?}"
            ),
            _ => assert!(
                matches!(&result, Err(Error::StoreIo { reason, .. }) if reason.contains(fails)),
                "the drive's failure, which outranks the hook, is what the last close returns: {result:?}"
            ),
        }
        assert!(
            closed.load(SeqCst),
            "{case}: the file is closed in any case"
        );
    }
}

/// The handle's rules at open (section 7, C3): a failed `on_open` runs no `on_close`, and
/// `on_query` is shown every submitted statement, one that fails to parse included.
#[test]
fn a_failed_open_hook_runs_no_close_hook_and_on_query_sees_every_statement() {
    let failing = Arc::new(Hooks {
        open: true,
        ..Default::default()
    });
    let mut options = DatabaseOpenOptions::default();
    options.plugin = Some(failing.clone());
    let opened = support::open_with(&support::folder("hook-open-fails"), options);
    assert!(opened.is_err(), "the open hook's failure refuses the open");
    assert_eq!(
        failing.closes.load(SeqCst),
        0,
        "no on_close after a failed on_open"
    );
    let hooks = Arc::new(Hooks::default());
    let mut options = DatabaseOpenOptions::default();
    options.plugin = Some(hooks.clone());
    let db = support::open_with(&support::folder("hook-hears-all"), options).unwrap();
    support::refusal(&db, "SELEC 1");
    let heard = hooks.heard.lock().unwrap().clone();
    assert!(
        heard.iter().any(|s| s.contains("SELEC 1")),
        "on_query sees bad SQL: {heard:?}"
    );
    support::close(db);
}

/// A growth happens whole or leaves nothing (section 7, C4). A lengthening that fails part way
/// through writing zeros is cut back to the file's old length, and the store stays usable; when
/// the cut-back fails too, the length is not known and the handle refuses every later call.
#[test]
fn a_growth_that_fails_part_way_is_cut_back_or_fails_the_handle() {
    for cut_back_fails in [false, true] {
        let folder = support::folder(&format!("growth-part-way-{cut_back_fails}"));
        let (armed, file) = (Arc::new(AtomicBool::new(false)), folder.join("data"));
        let growing = armed.clone();
        let mut options = DatabaseOpenOptions::default();
        options.storage = Some(support::failing_storage(move |call| match call {
            support::Call::Resize(len) if growing.load(SeqCst) => {
                let now = std::fs::metadata(&file)?.len();
                if len > now {
                    let zeros = std::fs::OpenOptions::new().write(true).open(&file)?;
                    zeros.set_len(now + (len - now) / 2)?; // the zeros reach half way
                    Err(std::io::Error::other("the drive gave out writing zeros"))
                } else if cut_back_fails {
                    Err(std::io::Error::other("the drive refused the cut-back"))
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        }));
        let db = support::open_with(&folder, options).unwrap();
        support::declare(&db);
        support::insert(&db, 1..=1);
        let before = support::data_file(&folder);
        let session = db.read_session(contextdb::ReadLimits::default()).unwrap();
        armed.store(true, SeqCst);
        let body = params! {"body" => support::letters(2, 8 << 20)};
        let grown = db.execute("INSERT INTO t VALUES(2,$body,0)", &body);
        armed.store(false, SeqCst);
        assert!(grown.is_err(), "the growth was refused");
        if cut_back_fails {
            let later = (
                db.execute("SELECT 1", &params! {}).is_err(),
                db.table_names().is_err(),
                session.execute("SELECT 1", &params! {}).is_err(),
            );
            let said = "a failed cut-back fails the handle and a read session opened before it";
            assert_eq!(later, (true, true, true), "{said}");
        } else {
            let after = support::data_file(&folder);
            assert_eq!(after, before, "a refused growth leaves the length");
            support::insert(&db, 3..=3);
            let rows = int(&db, "SELECT COUNT(*) FROM t");
            assert_eq!(rows, 2, "the store stays usable");
        }
        drop(db);
    }
}
