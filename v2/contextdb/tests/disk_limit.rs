mod support;
use contextdb::{DatabaseOpenOptions, Error, Value, params};
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
            while recovered
                .run_maintenance_cycle()
                .unwrap()
                .recovery_pages_remaining
                != Some(0)
            {}
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
