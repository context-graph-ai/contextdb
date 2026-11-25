mod support;
use contextdb::{DatabaseOpenOptions, Error, Value, params};
use support::Child;
#[test]
fn at_the_disk_limit_a_write_that_fits_is_not_refused() {
    let folder = support::folder("disk-limit");
    let db = support::open(&folder);
    support::declare(&db);
    support::insert(&db, 1..=3);
    support::declare_docs(&db, 2048, "");
    let bytes = std::fs::metadata(folder.join("data")).unwrap().len();
    let limit = bytes / 2;
    db.set_disk_limit(Some(limit)).unwrap();
    let bytes = std::fs::metadata(folder.join("data")).unwrap().len();
    assert_eq!(db.disk_limit().unwrap(), Some(limit));
    let shown = support::rows(&db, "SHOW DISK_LIMIT");
    assert_eq!(shown[0][0], Value::Int64(limit as i64));
    let body = support::letters(7, 2 << 20);
    let insert = "INSERT INTO t (id, name, tag) VALUES (4, $body, 0)";
    let args = params! {"body" => body};
    let error = db.execute(insert, &args).unwrap_err();
    assert!(matches!(error, Error::DiskLimitExceeded { .. }));
    let text = format!("{error:?}");
    assert!(
        text.contains(&limit.to_string()) && text.contains(&bytes.to_string()),
        "{text}"
    );
    assert_eq!(std::fs::metadata(folder.join("data")).unwrap().len(), bytes);
    support::assert_rows_are(&db, 1..=3);
    // Vectors count against the limit, and a refused write leaves the file as it was.
    let tx = db.transaction().unwrap();
    let put = |n| support::insert_doc(&tx, n, "a", Some(support::vector(n, 2048)));
    let refused = (1..=500).any(|n| matches!(put(n), Err(Error::DiskLimitExceeded { .. })));
    assert!(refused || matches!(tx.commit(), Err(Error::DiskLimitExceeded { .. })));
    assert_eq!(std::fs::metadata(folder.join("data")).unwrap().len(), bytes);
    support::run(&db, "UPDATE t SET tag = 9 WHERE id = 1");
    assert_eq!(support::int(&db, "SELECT tag FROM t WHERE id = 1"), 9);
    assert_eq!(std::fs::metadata(folder.join("data")).unwrap().len(), bytes);
    support::run(&db, "SET DISK_LIMIT 16M");
    db.execute(insert, &args).unwrap();
    assert!(std::fs::metadata(folder.join("data")).unwrap().len() <= 16 << 20);
    support::close(db);
    let db = contextdb::Database::open(&folder).unwrap();
    assert_eq!(db.disk_limit().unwrap(), Some(16 << 20));
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM t"), 4);
    // The statement's door persisted its limit; the method's door persists too.
    db.set_disk_limit(Some(48 << 20)).unwrap();
    let db = support::reopen(db, &folder);
    assert_eq!(db.disk_limit().unwrap(), Some(48 << 20));
    db.set_disk_limit(None).unwrap();
    db.set_memory_limit(Some(64 << 20)).unwrap();
    for mode in [
        contextdb::Maintenance::Caller,
        contextdb::Maintenance::Engine,
    ] {
        let stopped = support::folder(&format!("reserve-edge-{mode:?}"));
        std::fs::copy(folder.join("data"), stopped.join("data")).unwrap();
        let bytes = std::fs::metadata(stopped.join("data")).unwrap().len();
        let mut options = DatabaseOpenOptions::default();
        options.maintenance = mode;
        options.disk_limit = Some(bytes);
        let recovered = contextdb::Database::open_with_options(&stopped, options).unwrap();
        let sql = "UPDATE t SET tag = 27 WHERE id = 1";
        if mode == contextdb::Maintenance::Caller {
            let error = recovered.execute(sql, &params! {}).unwrap_err();
            assert!(matches!(error, Error::DiskLimitExceeded {
                operation: "write", current_bytes, budget_limit_bytes, hint,
            } if current_bytes == bytes && budget_limit_bytes == bytes
                && hint.contains("free space is being found") && hint.contains("run maintenance")));
            // The rebuild's own step says it advanced even when the count still to class does
            // not move, which is what a waiting writer reads (design 4, S2 rule 3).
            while {
                let report = recovered.run_maintenance_cycle().unwrap();
                assert_eq!(report.recovery_advanced, Some(true), "{report:?}");
                report.recovery_pages_remaining != Some(0)
            } {}
        }
        support::run(&recovered, sql);
        assert_eq!(
            support::int(&recovered, "SELECT tag FROM t WHERE id = 1"),
            27
        );
        assert_eq!(support::int(&recovered, "SELECT COUNT(*) FROM t"), 4);
        assert!(std::fs::metadata(stopped.join("data")).unwrap().len() <= bytes);
        support::close(recovered);
    }
    for id in 5..=8 {
        db.execute(
            "INSERT INTO t VALUES ($id, $body, 0)",
            &params! {"id" => id, "body" => args["body"].clone()},
        )
        .unwrap();
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = std::fs::metadata(folder.join("data")).unwrap();
        assert!(
            metadata.len() > 4 << 20,
            "exercise growth across several MiB"
        );
        eprintln!(
            "default open: length={}, allocated={}",
            metadata.len(),
            metadata.blocks() * 512
        );
        assert!(
            metadata.blocks() * 512 >= metadata.len(),
            "all bytes must have allocated blocks"
        );
    }
    support::close(db);
    let mut options = DatabaseOpenOptions::default();
    options.disk_limit = Some(32 << 20);
    let db = support::open_with(&folder, options).unwrap();
    assert_eq!(db.disk_limit().unwrap(), Some(32 << 20));
    let db = support::reopen(db, &folder);
    assert_eq!(db.disk_limit().unwrap(), None);
}

#[test]
fn a_refused_vector_write_changes_no_row_watermark_or_search() {
    let (folder, db) = support::built("disk-limit-vectors", 2, "", 50);
    let size = support::data_file(&folder);
    db.set_disk_limit(Some(size)).unwrap();
    let search = "SELECT id FROM docs ORDER BY v <=> [0.5, 0.5] USE VECTOR EXACT LIMIT 5";
    let before = (support::watermark(&db), support::rows(&db, search));
    let sql = "UPDATE docs SET ctx = $big, v = [9.0, 9.0]";
    let refused = db.execute(sql, &params! {"big" => support::letters(3, 2 << 20)});
    assert!(matches!(refused, Err(Error::DiskLimitExceeded { .. })));
    assert!(before == (support::watermark(&db), support::rows(&db, search)));
}

#[test]
fn an_index_build_under_the_disk_limit_is_refused_and_the_old_answers_serve_until_room_returns() {
    let folder = support::folder("disk-limit-build");
    let db = support::open(&folder);
    support::declare_docs(&db, 16, "INDEXING_THRESHOLD 1000");
    support::load_docs(&db, 1..=2000, "a", |n| support::gathered(n, 16));
    let query = support::gathered(60_000, 16);
    let sql = support::by_mode(None, "EXACT", 10);
    let wanted = support::stored(&db, support::DOCS).nearest(|_| true, &query, 10);
    let size = || std::fs::metadata(folder.join("data")).unwrap().len();
    let limit = size();
    db.set_disk_limit(Some(limit)).unwrap();
    for _ in 0..20 {
        let outcome = db.run_maintenance_cycle();
        assert!(outcome.is_ok() || matches!(outcome, Err(Error::DiskLimitExceeded { .. })));
        assert!(
            size() <= limit,
            "a build commit grew the file past the limit"
        );
        support::assert_nearest(&db, &sql, &query, &wanted, "the old state serves");
    }
    db.set_disk_limit(None).unwrap();
    support::maintenance(&db);
    support::assert_nearest(&db, &sql, &query, &wanted, "built once room returned");
}

#[test]
fn a_writer_at_the_disk_limit_waits_only_while_a_cycle_moves_space() {
    // Design 4, S2 rule 3: in Engine mode a writer at the limit waits while maintenance gives
    // space back, and is refused once a cycle moved nothing, with the hint naming the cause.
    let folder = support::folder("engine-wait");
    let mut options = DatabaseOpenOptions::default();
    options.maintenance = contextdb::Maintenance::Engine;
    // The clock stands still, so no tidy-up window ever ends: the worker's cycles move no page
    // into the free pages below, whenever they run.
    let still = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1_000_000));
    options.clock = Some(std::sync::Arc::new(support::At(still)));
    let db = contextdb::Database::open_with_options(&folder, options).unwrap();
    support::run(&db, "SET STORAGE_COMPACTION MIN_INTERVAL 1 HOURS");
    support::run(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)");
    let tx = db.transaction().unwrap();
    for id in 1..=400 {
        let row = params! { "id" => id, "body" => support::letters(id, 3000) };
        tx.execute("INSERT INTO t VALUES ($id, $body)", &row)
            .unwrap();
    }
    tx.commit().unwrap();
    // A reader on the older commit keeps every page the delete frees.
    let held = db.snapshot().unwrap();
    support::run(&db, "DELETE FROM t");
    // One row of two pages takes every free page left, and the file's length is then the
    // limit: the writer starts at zero free pages, so admission itself waits (review 13,
    // sixty-third request).
    let row = params! { "id" => 2, "body" => support::letters(2, 5000) };
    db.execute("INSERT INTO t VALUES ($id, $body)", &row)
        .expect("the free pages left hold one row");
    db.set_disk_limit(Some(std::fs::metadata(folder.join("data")).unwrap().len()))
        .unwrap();
    // A row of one page: the writer needs only the room the reader's end lets go.
    let body = params! { "body" => support::letters(1, 3000) };
    let insert = "INSERT INTO t VALUES (1, $body)";
    let error = db
        .execute(insert, &body)
        .expect_err("no space can be given back while held");
    let reader = "a reader on an older commit holds the freed space; end that reader or raise the \
                  disk limit";
    assert!(
        matches!(&error, Error::DiskLimitExceeded { operation: "write", hint, .. } if *hint == reader),
        "the writer is refused with the cause, not kept waiting: {error:?}"
    );
    drop(held);
    db.execute(insert, &body)
        .expect("the reader ended, so the cycles free the space it held");
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM t"), 2);
    support::close(db);
}

/// The second process's side: each partition of `docs.v` as `SHOW VECTOR_PARTITIONS` gives it
/// in a read session, one line a partition.
fn shown_partitions(role: &support::Role) {
    let reader = support::open_reader(&role.folder, None);
    for partition in support::shown(&reader, "SHOW VECTOR_PARTITIONS FOR docs.v") {
        let words = ["partition_key", "maintenance_state", "maintenance_reason"];
        let [key, state, reason] = words.map(|name| support::word(&partition, name));
        let action = support::word(&partition, "recovery_action");
        support::say(format!("partition {key}|{state}|{reason}|{action}"));
    }
}

#[test]
fn a_second_process_names_the_partition_a_full_disk_stalled_and_what_frees_it() {
    if let Some(role) = support::role() {
        return shown_partitions(&role);
    }
    let test = "a_second_process_names_the_partition_a_full_disk_stalled_and_what_frees_it";
    // Two copies with the same rows, their partitions made in the opposite order: a seal
    // refused for room is tried again for the first partition alone, so each copy stalls a
    // different one, and the file of each shows the same pending vectors (review 13, sixtieth
    // request, A.1: the operator is told the partition and its recovery without a write).
    for (first, second) in [("a", "b"), ("b", "a")] {
        let folder = support::folder(&format!("full-disk-names-{first}"));
        let db = support::open(&folder);
        // A column sealed before on the same partition is not the one named for docs' seal.
        let spare = "CREATE TABLE s (k INT PRIMARY KEY, ctx TEXT NOT NULL, v VECTOR(2) PARTITION_KEY (ctx))";
        support::run(&db, spare);
        support::run(&db, &format!("INSERT INTO s VALUES (1, '{first}', [1, 0])"));
        db.run_maintenance_cycle().unwrap();
        support::run(
            &db,
            "CREATE TABLE other (id INTEGER PRIMARY KEY, n INTEGER)",
        );
        support::run(&db, "INSERT INTO other VALUES (1, 0)");
        support::declare_docs(&db, 1024, "PARTITION_KEY (ctx) INDEXING_THRESHOLD 100000");
        let ids = |ctx| if ctx == "a" { 1..=40 } else { 41..=80 };
        for ctx in [first, second] {
            support::load_docs(&db, ids(ctx), ctx, |n| support::vector(n, 1024));
        }
        // At the file's length as the limit, rows of a few pages each fill the free pages until
        // one is refused: a few pages stay free, far fewer than one partition's seal writes.
        let limit = support::data_file(&folder);
        db.set_disk_limit(Some(limit)).unwrap();
        support::run(&db, "CREATE TABLE fill (id INTEGER PRIMARY KEY, body TEXT)");
        for id in 0.. {
            let row = params! { "id" => id, "body" => support::letters(id, 12_000) };
            match db.execute("INSERT INTO fill VALUES ($id, $body)", &row) {
                Ok(_) => assert!(id < 1024, "the free pages of a small file run out"),
                Err(Error::DiskLimitExceeded { .. }) => break,
                Err(other) => panic!("a fill row failed for another reason: {other:?}"),
            }
        }
        db.run_maintenance_cycle()
            .expect("a seal refused for room is recorded, and the cycle goes on");
        let told = |when: &str| {
            assert_eq!(
                support::data_file(&folder),
                limit,
                "the data file stays at its limit"
            );
            let lines = Child::start(test, "partitions", &folder).end();
            let line = |ctx: &str| {
                let key = format!("partition {{\"ctx\":\"{ctx}\"}}|");
                let line = lines.iter().find(|line| line.starts_with(&key));
                let line = line.unwrap_or_else(|| panic!("{ctx} is not shown {when}: {lines:?}"));
                line[key.len()..].to_owned()
            };
            assert_eq!(
                line(first),
                "stalled|disk_limit|raise the disk limit or free space",
                "{when}, the second process names the partition whose seal was refused for room"
            );
            assert!(
                !line(second).starts_with("stalled"),
                "{when}, the partition left pending is not named as stalled: {lines:?}"
            );
        };
        told("after the refusal");
        // A failed seal does not mean every smaller write fails, and an unrelated commit does
        // not clear what the second process is told.
        support::run(&db, "UPDATE other SET n = 1 WHERE id = 1");
        told("after an unrelated commit");
        // A PURGE of the stalled partition's rows leaves its key value nowhere in the space file.
        db.set_disk_limit(None).unwrap();
        support::run(&db, &format!("PURGE FROM docs WHERE ctx = '{first}'"));
        support::close(db);
        let space = std::fs::read(folder.join("space")).unwrap();
        let key = [4, first.as_bytes()[0], 0, 0];
        assert!(
            !space.windows(4).any(|w| w == key),
            "the space file keeps the key"
        );
    }
}

/// The retry the folder's `space` file keeps: after its version byte, the release mark it was
/// decided under (two u64s), the next candidate's ordinal (a u32) and whether nothing smaller
/// is owed (a byte); none while the file is missing.
fn retry(folder: &std::path::Path) -> Option<(Vec<u8>, u32, u8)> {
    let space = std::fs::read(folder.join("space")).ok()?;
    let k = u32::from_le_bytes(space.get(17..21)?.try_into().unwrap());
    Some((space.get(1..17)?.to_vec(), k, *space.get(21)?))
}

#[test]
fn a_disk_limit_set_at_a_packed_store_s_length_never_grows_the_file_past_it() {
    // A declared limit refuses growth past it, and the commit that records a lower limit is no
    // exception: setting it never leaves the file past it, open or once closed. With no page
    // free, that commit itself has no room and is refused as any other would be.
    let folder = support::folder("limit-at-packed-length");
    let db = support::open(&folder);
    support::run(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)");
    let put = |id: i64, n: usize| {
        let row = params! { "id" => id, "body" => support::letters(id + 1000, n) };
        db.execute("INSERT INTO t VALUES ($id, $body)", &row)
            .unwrap();
    };
    put(-1, 60_000);
    (1..=40).for_each(|id| put(id, 3000));
    put(0, 60_000);
    (41..=60).for_each(|id| put(id, 3000));
    for pass in 0.. {
        assert!(pass < 64, "a tidy-up pass of a small store ends");
        if db.compact_now().unwrap().work_remaining == Some(false) {
            break;
        }
    }
    let limit = support::data_file(&folder);
    let set = db.set_disk_limit(Some(limit));
    assert!(
        matches!(set, Ok(()) | Err(Error::DiskLimitExceeded { .. })),
        "setting a limit is recorded or refused for room: {set:?}"
    );
    assert_eq!(
        db.disk_limit().unwrap(),
        set.is_ok().then_some(limit),
        "a refused setting leaves the prior limit, none here, in force"
    );
    assert!(
        support::data_file(&folder) <= limit,
        "setting the limit grew the file past it"
    );
    support::close(db);
    assert!(
        support::data_file(&folder) <= limit,
        "the close after setting the limit grew the file past it"
    );
}

#[test]
fn opens_at_the_disk_limit_carry_the_retry_past_a_refused_step_to_a_move_that_commits() {
    // Design 4, S2: a tidy-up step refused for room at commit owes the next poll the next page
    // alone, kept in the `space` file under the release mark, so a store opened for one cycle at
    // a time still gets past the refusal instead of trying the same step forever.
    let folder = support::folder("retry-across-opens");
    let db = support::open(&folder);
    support::run(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)");
    let put = |id: i64, n: usize| {
        let row = params! { "id" => id, "body" => support::letters(id + 1000, n) };
        db.execute("INSERT INTO t VALUES ($id, $body)", &row)
            .unwrap();
    };
    // A large value low in the file and another above rows of one page each: once packed and
    // the low one deleted, its block is the only room below, and a whole step moving into it
    // needs more pages than the file has free.
    put(-1, 60_000);
    (1..=40).for_each(|id| put(id, 2000));
    put(0, 60_000);
    (41..=60).for_each(|id| put(id, 2000));
    for pass in 0.. {
        assert!(pass < 64, "a tidy-up pass of a small store ends");
        if db.compact_now().unwrap().work_remaining == Some(false) {
            break;
        }
    }
    support::run(&db, "DELETE FROM t WHERE id = -1");
    let limit = support::data_file(&folder);
    db.set_disk_limit(Some(limit)).unwrap();
    assert_eq!(
        support::data_file(&folder),
        limit,
        "the store starts at its limit"
    );
    support::close(db);
    // Each open runs one cycle: what its step did, and the retry and file length it left.
    let stale = retry(&folder);
    let mut opens = Vec::new();
    for _ in 0..2 {
        let db = support::open(&folder);
        let report = db.run_maintenance_cycle().unwrap();
        let did = (
            report.compaction_disk_refused == Some(true),
            report.compacted_pages,
        );
        support::close(db);
        let left = retry(&folder).expect("a refused step leaves its retry in the file");
        opens.push((did, left, support::data_file(&folder)));
    }
    let [
        (refused, (mark, owed, rest), len0),
        (moved, (_, spent, _), len1),
    ] = &opens[..]
    else {
        unreachable!()
    };
    // What the packing pass left was decided under an older mark, which the delete moved on,
    // so it is not believed: the first open starts the retry from nothing.
    assert!(
        stale.is_none_or(|(at, ..)| at != *mark),
        "the delete moved the release mark"
    );
    assert_eq!(
        (*refused, *owed, *rest),
        ((true, Some(0)), 1, 0),
        "the whole step is refused at commit, and the next page alone is owed: {opens:?}"
    );
    assert_eq!(
        (*moved, *spent),
        ((false, Some(1)), 0),
        "the next open moves that page alone, which commits, and the retry is spent: {opens:?}"
    );
    assert!(
        *len0 <= limit && *len1 <= limit,
        "no open lengthens the file past its limit"
    );
    let db = support::open(&folder);
    let bodies = support::rows(&db, "SELECT id, body FROM t ORDER BY id");
    let wanted = (0..=60).map(|id: i64| {
        let n = if id == 0 { 60_000 } else { 2000 };
        vec![
            Value::Int64(id),
            Value::Text(support::letters(id + 1000, n)),
        ]
    });
    assert!(
        bodies.into_iter().eq(wanted),
        "every row reads back as written after the moves"
    );
    support::close(db);
}
