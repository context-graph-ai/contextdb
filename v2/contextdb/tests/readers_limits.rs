//! Read-session limits have no SQL-script door. Extends readers.rs and memory.rs.
mod support;
use contextdb::{Error, ReadLimits, ReadSession, Value, params};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use support::{Bytes, Observer};
#[test]
fn each_read_limit_refuses_by_name() {
    let folder = support::folder("session-limits");
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE h (id INTEGER PRIMARY KEY, v VECTOR(2), source_id UUID, target_id UUID, edge_type TEXT)",
    );
    support::run(
        &db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(2) RANK_POLICY \
         (JOIN h ON id, FORMULA '{vector_score}', SORT_KEY s))",
    );
    let tx = db.transaction().unwrap();
    for id in 1..=2048_i64 {
        tx.execute(
            "INSERT INTO t VALUES ($id, $v)",
            &params! { "id" => id, "v" => vec![id as f32, 1.0] },
        )
        .unwrap();
    }
    tx.commit().unwrap();
    let defaults = ReadLimits::default();
    assert_eq!(defaults.result_rows, 500);
    assert_eq!(defaults.result_bytes, 4 << 20);
    assert_eq!(defaults.work, u64::MAX, "no default work ceiling");
    assert_eq!(defaults.active_ms, 5000);
    assert_eq!(defaults.memory, 16 << 20);
    assert_eq!(defaults.cursor_page_rows, 100);
    assert_eq!(defaults.cursor_page_bytes, 1 << 20);
    assert_eq!(defaults.cursor_idle_ms, 300_000);
    assert_eq!(defaults.cursor_lifetime_ms, 1_800_000);
    let size = |mib: i64| Some(mib << 20);
    for (share, memory, shown) in [
        (None, 16, [None, Some(50), Some(50), size(8), size(16)]),
        (Some(0), 16, [Some(0), Some(50), Some(0), Some(0), size(16)]),
        (
            Some(25),
            64,
            [Some(25), Some(25), Some(25), size(16), size(64)],
        ),
    ] {
        let limits = ReadLimits {
            cache_share: share,
            memory: memory << 20,
            ..defaults.clone()
        };
        let session = db.read_session(limits).unwrap();
        let row = support::rows(&session, "SHOW READ_CACHE_SHARE").remove(0);
        let want = shown.map(|n| n.map_or(Value::Null, Value::Int64));
        assert_eq!(
            row, want,
            "share {share:?} under {memory} MiB: declared, default, effective percent, cache bytes, session memory"
        );
    }
    let mut wide = defaults.clone();
    wide.result_rows = 10_000;
    wide.work = 1_000_000;
    wide.memory = 64 << 20;
    let all = "SELECT id FROM t";
    assert_eq!(support::rows(&db, all).len(), 2048);
    let bytes = Arc::new(Bytes::default());
    let measured = db
        .read_session_with_progress(wide.clone(), bytes.clone())
        .unwrap();
    assert_eq!(support::rows(&measured, all).len(), 2048);
    let assembled = *bytes.0.lock().unwrap();
    assert!(assembled > 0);
    let search = "SELECT id FROM t WHERE id + 0 > 0 ORDER BY v <=> $q USE VECTOR EXACT LIMIT 4";
    let q = params! { "q" => vec![1.0_f32, 1.0] };
    let mut rules = vec![("result-rows", all, 2048), ("result-bytes", all, assembled)];
    let auto = search.replace("EXACT", "AUTO");
    for sql in [all, "SELECT SUM(id) FROM t", search, auto.as_str()] {
        let reference = db.execute(sql, &q).unwrap();
        let work = reference.rows_examined() + reference.distance_evaluations();
        assert!(work >= 2048, "{sql}");
        if sql.contains("<=>") {
            assert!(reference.distance_evaluations() > 0);
        }
        rules.push(("work", sql, work));
    }
    for sql in ["SHOW VECTOR_INDEXES", "SHOW VECTOR_PARTITIONS"] {
        rules.push(("result-rows", sql, support::rows(&db, sql).len() as u64));
    }
    for file in [false, true] {
        let open = |limits| support::read_session(&db, &folder, file, limits);
        let default = open(defaults.clone()).unwrap();
        assert_eq!(support::memory_shown(&default, "limit"), (16 << 20).into());
        let sql = "SELECT id FROM t WHERE id = 1";
        let explained = default.explain(sql).unwrap();
        let text: Vec<_> = explained.lines().map(|line| vec![line.into()]).collect();
        assert_eq!(support::rows(&default, &format!("EXPLAIN {sql}")), text);
        for &(field, sql, at) in &rules {
            let reference = db.execute(sql, &q).unwrap();
            for over in [false, true] {
                let mut limits = wide.clone();
                let cap = at - u64::from(over);
                match field {
                    "result-rows" => limits.result_rows = cap,
                    "result-bytes" => limits.result_bytes = cap,
                    _ => limits.work = cap,
                }
                let session = open(limits).unwrap();
                let result = session.execute(sql, &q);
                if over {
                    let error = result.unwrap_err();
                    assert!(matches!(error, Error::ReadLimitExceeded { .. }));
                    assert!(
                        error.to_string().replace('_', "-").contains(field),
                        "{error}"
                    );
                } else {
                    assert_eq!(result.unwrap().rows(), reference.rows());
                }
                assert_eq!(support::int(&session, "SELECT COUNT(*) FROM t"), 2048);
            }
        }
        let mut floor = wide.clone();
        floor.memory = 1 << 20;
        floor.memory = match open(floor.clone()) {
            Err(Error::MemoryBudgetExceeded {
                requested_bytes, ..
            }) => requested_bytes,
            _ => panic!("M1: 1 MiB is under a reader's floor, and its refusal names it"),
        };
        assert!(open(floor.clone()).is_ok());
        floor.memory -= 1;
        assert!(matches!(
            open(floor),
            Err(Error::MemoryBudgetExceeded { .. })
        ));
        let mut small = wide.clone();
        small.memory = 16 << 20;
        small.result_bytes = 64 << 20;
        let result = open(small)
            .unwrap()
            .execute("SELECT $body", &params! { "body" => "x".repeat(17 << 20) });
        assert!(matches!(result, Err(Error::MemoryBudgetExceeded { .. })));
        let mut tables = wide.clone();
        tables.tables = Some(["t".to_owned()].into());
        let only = open(tables).unwrap();
        assert_eq!(*only.table_names().unwrap(), ["t"]);
        assert!(only.declaration("h").is_err());
        for sql in [
            "SELECT id FROM h",
            "SELECT id FROM t WHERE id IN (SELECT id FROM h)",
            "SELECT id FROM t ORDER BY v <=> $q USE RANK s LIMIT 1",
            "WITH h AS (SELECT id FROM t) SELECT b_id FROM GRAPH_TABLE(h MATCH (a)-[e]->(b) COLUMNS (b.id AS b_id))",
            "DELETE FROM h WHERE id = 99",
        ] {
            db.execute(sql, &q).unwrap();
            assert!(only.execute(sql, &q).is_err(), "{sql}");
            assert!(only.explain(sql).is_err(), "{sql}");
        }
        let sql =
            "SELECT x FROM GRAPH_TABLE(t MATCH (a)-[e]->(b) COLUMNS ((SELECT id FROM h) AS x))";
        assert!(matches!(
            only.execute(sql, &q),
            Err(Error::ReadLimitExceeded { field, .. }) if field == "tables"
        ));
        assert_eq!(support::int(&only, "SELECT COUNT(*) FROM t"), 2048);
        for sql in ["SHOW VECTOR_INDEXES", "SHOW VECTOR_PARTITIONS LIMIT 1"] {
            let rows = support::rows(&only, sql);
            assert_eq!(rows.len(), 1, "{sql}: {rows:?}");
            assert_eq!(rows[0][0], "t".into(), "{sql}");
        }
    }
    let ddl = |sql| support::run(&db, sql);
    ddl("CREATE TABLE edges (source_id UUID, target_id UUID, edge_type TEXT)");
    ddl("CREATE TABLE secret (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID)");
    wide.tables = Some(["edges".to_owned()].into());
    let session = support::in_contexts(&db, &[1]).read_session(wide).unwrap();
    let node = "00000000-0000-0000-0000-000000000001";
    let sql = "SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[e]->(b) WHERE a.id = '00000000-0000-0000-0000-000000000001' COLUMNS (b.id AS b_id))";
    let probe = || match session.execute(sql, &params! {}) {
        Ok(r) => format!("{:?} {}", r.rows(), r.rows_examined()),
        Err(e) => format!("{e:?}: {e}"),
    };
    let before = probe();
    ddl(&format!("INSERT INTO secret VALUES ('{node}', '{node}')"));
    assert_eq!(before, probe());
    assert!(!probe().contains("secret"));
}

#[test]
fn a_file_sessions_declaration_hides_rows_counts_and_inspection() {
    use contextdb::{ReadSession, ReadSessionOptions};
    let folder = support::folder("file-declared-visibility");
    let db = support::open(&folder);
    let ddl = "CREATE TABLE scoped (id INT PRIMARY KEY, c UUID CONTEXT_ID, label TEXT SCOPE_LABEL ('public', 'secret'), v VECTOR(2))";
    support::run(&db, ddl);
    let q = params! {"a" => support::context(1), "b" => support::context(2)};
    db.execute("INSERT INTO scoped VALUES (1, $a, 'public', [1, 0]), (2, $a, 'secret', [0, 1]), (3, $b, 'public', [0, 1])", &q).unwrap();
    for contexts in [
        None,
        Some(support::context_set(&[1])),
        Some(Default::default()),
    ] {
        for labels in [
            None,
            Some(support::label_set(&["public"])),
            Some(Default::default()),
        ] {
            let reference = db.scoped_with_constraints(contexts.clone(), labels.clone(), None);
            let mut options = ReadSessionOptions::default();
            options.contexts = contexts.clone();
            options.scope_labels = labels.clone();
            options.limits.cursor_page_rows = 1;
            let session = ReadSession::open_with_options(&folder, options).unwrap();
            for sql in [
                "SELECT id FROM scoped",
                "SELECT COUNT(*) FROM scoped",
                "SELECT DISTINCT label FROM scoped",
            ] {
                let expected = support::rows(&reference, sql);
                assert_eq!(support::rows(&session, sql), expected);
                let mut cursor = session.open_cursor(sql, &q).unwrap();
                let mut rows = Vec::new();
                while cursor.has_more() {
                    rows.extend_from_slice(cursor.fetch(None).unwrap().rows());
                }
                assert_eq!(rows, expected, "{sql}");
            }
            if contexts.is_some() || labels.is_some() {
                let sql = "SHOW VECTOR_INDEXES";
                assert!(matches!(
                    session.execute(sql, &q),
                    Err(Error::AdminRequired { .. })
                ));
                assert!(matches!(
                    session.open_cursor(sql, &q),
                    Err(Error::AdminRequired { .. })
                ));
            }
        }
    }
}

/// A count through an index counts its entries as work, so a read session's work limit holds
/// it: ten entries answer under a limit of ten and are refused under nine, by name. Asked for
/// one more than the work left, a count of a thousand entries reads eleven before it is refused
/// (the sheet's section 7, point 2), and with LIMIT 0 none; the work the observer is last told
/// is the entries read.
#[test]
fn a_count_of_entries_is_work_the_limit_holds() {
    let db = support::open(&support::folder("count-work-limit"));
    support::run(
        &db,
        "CREATE TABLE c (id INTEGER PRIMARY KEY, tag INTEGER NOT NULL)",
    );
    support::run(&db, "CREATE INDEX c_tag ON c (tag)");
    let rows: Vec<String> = (1..=1000)
        .map(|id| format!("({id}, {})", if id <= 10 { 1 } else { 2 }))
        .collect();
    support::run(
        &db,
        &format!("INSERT INTO c (id, tag) VALUES {}", rows.join(", ")),
    );
    let sql = "SELECT COUNT(*) FROM c WHERE tag = 1";
    let reference = db.execute(sql, &params! {}).unwrap();
    assert_eq!(
        reference.rows_examined(),
        10,
        "the ten entries it counts are its work"
    );
    let run = |sql: &str, work: u64| {
        let observer = Observer::new(Arc::new(AtomicU64::new(0)), 0, false);
        let limits = ReadLimits {
            work,
            ..ReadLimits::default()
        };
        let session = db.read_session_with_progress(limits, observer.clone());
        let result = session.unwrap().execute(sql, &params! {});
        let told = observer.samples.lock().unwrap().last().map(|last| last[2]);
        (
            result,
            told.expect("the observer is told the work at the end"),
        )
    };
    assert_eq!(run(sql, 10).0.unwrap().rows(), reference.rows());
    let refused = run(sql, 9).0.unwrap_err();
    assert!(
        matches!(refused, Error::ReadLimitExceeded { .. }) && refused.to_string().contains("work"),
        "one entry over the limit is refused by the work limit: {refused}"
    );
    let (refused, read) = run("SELECT COUNT(*) FROM c WHERE tag >= 1", 10);
    assert!(
        refused.is_err(),
        "a thousand entries are over a work limit of ten"
    );
    assert!(
        read <= 11,
        "ten entries answer and the eleventh refuses: it read {read} of the thousand"
    );
    let (answer, read) = run("SELECT COUNT(*) FROM c WHERE tag >= 1 LIMIT 0", 50_000);
    assert!(answer.unwrap().rows().is_empty(), "LIMIT 0 answers no row");
    assert_eq!(read, 0, "a statement that returns no row reads no entry");
}

/// M1 and M2: a reader sets aside the same whatever the size of the store's file, inside its
/// door's floor (four terms, under 11 MiB), and opens at that floor; a default reader answers a
/// find with a 768-number vector in its text (M5). A limit below a door's floor is refused at
/// open naming it, and a writer's floor is 10 MiB and 768 KiB for each started 4 GiB of its file.
#[test]
fn a_default_reader_holds_no_page_maps_and_a_writer_holds_what_it_did() {
    let folder = support::folder("reader-page-maps");
    let db = support::open(&folder);
    let table = "CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(768))";
    support::run(&db, table);
    let v = support::vector(1, 768);
    db.execute("INSERT INTO t VALUES (1, $v)", &params! {"v" => v.clone()})
        .unwrap();
    support::close(db);
    let find = format!("SELECT id FROM t ORDER BY v <=> {v:?} LIMIT 1");
    let floor = |limit: Option<usize>, writer: bool| {
        let mut options = contextdb::DatabaseOpenOptions::default();
        options.memory_limit = limit;
        let opened = match writer {
            true => support::open_with(&folder, options).map(drop),
            false => {
                ReadSession::open_with_options(&folder, support::reader_options(limit)).map(drop)
            }
        };
        match opened {
            Err(Error::MemoryBudgetExceeded {
                requested_bytes, ..
            }) => requested_bytes as usize,
            other => panic!("a limit of {limit:?} below the floor gave {other:?}"),
        }
    };
    let mut used = Vec::new();
    for (len, maps) in [(None, 1), (Some(9 << 30), 3)] {
        if let Some(len) = len {
            let data = std::fs::OpenOptions::new()
                .write(true)
                .open(folder.join("data"));
            data.unwrap().set_len(len).unwrap();
        }
        let reader = ReadSession::open(&folder, ReadLimits::default()).unwrap();
        assert_eq!(support::ids(&reader, &find), [1], "a default reader's find");
        used.push(support::memory_used(&reader));
        let reader = floor(Some(1 << 20), false);
        assert!(reader < 11 << 20, "the reader's floor {reader}");
        assert_eq!(floor(Some(reader - 1), false), reader, "a reader's floor");
        support::open_reader(&folder, Some(reader));
        used.push(reader);
        let writer = (10 << 20) + maps * (768 << 10);
        assert_eq!(floor(Some(writer - 1), true), writer, "a writer's floor");
    }
    assert_eq!(used[0..2], used[2..], "a reader's use grew with the file");
    let held = support::opening::reader_charge(&folder, None)
        + support::form_charge(&support::form_of(&find));
    assert!(used[0] <= held, "a default reader holds past its account");
}

/// M8 rule 2: a read session's open leaves its mark before it reads the newest commit, so two
/// commits the writer makes while that open is stopped cannot change what it checks.
#[test]
fn a_read_session_open_stopped_after_its_mark_finishes_through_two_commits() {
    let folder = support::folder("open-beside-commits");
    let db = support::open(&folder);
    support::declare(&db);
    support::insert(&db, 1..=200);
    let gate = Arc::new(std::sync::Barrier::new(2));
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let (stop, once) = (gate.clone(), armed.clone());
    let mut options = support::reader_options(None);
    options.storage = Some(support::storage(move |call| {
        let past_header = matches!(call, support::Call::Read(offset, _) if offset >= 4096);
        if past_header && once.swap(false, std::sync::atomic::Ordering::SeqCst) {
            stop.wait();
            stop.wait();
        }
    }));
    std::thread::scope(|scope| {
        let opening = scope.spawn(|| ReadSession::open_with_options(&folder, options));
        gate.wait();
        support::run(&db, "UPDATE t SET name = 'second' WHERE id = 1");
        support::run(&db, "UPDATE t SET name = 'third' WHERE id = 2");
        gate.wait();
        let reader = opening
            .join()
            .unwrap()
            .expect("the open finishes with no error");
        assert_eq!(
            support::ids(&reader, "SELECT id FROM t WHERE id <= 2"),
            [1, 2]
        );
    });
}
