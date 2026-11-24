//! Extends readers.rs's one-moment test; isolated until the snapshot API exists.
mod support;
use contextdb::{Error, params};
#[test]
fn a_pinned_moment_survives_changes_and_reopen_under_the_handles_memory_cap() {
    let folder = support::folder("pinned-moment-cap");
    let db = support::open(&folder);
    support::declare(&db);
    support::insert_together(&db, 1..=3);
    let idle = support::used(&db);
    let snapshot = db.snapshot().unwrap();
    assert!(
        support::used(&db) > idle,
        "a held snapshot's cache is charged to the handle"
    );
    let receipt = snapshot.id();
    assert_eq!(receipt, db.committed_watermark().unwrap());
    support::run(&db, "UPDATE t SET name = 'changed' WHERE id = 2");
    support::run(&db, "DELETE FROM t WHERE id = 1");
    let sql = "SELECT id, name, tag FROM t ORDER BY id";
    let before = support::used(&db);
    assert!(db.snapshot().unwrap().id() > receipt);
    assert_eq!(
        support::used(&db),
        before,
        "a dropped snapshot's charge is given back"
    );
    support::close(db);
    let db = support::open_limited(&folder, 16 << 20);
    support::run(&db, "DELETE FROM t WHERE id <= 3");
    support::insert(&db, 4..=4);
    support::cycle(&db);
    assert_eq!(snapshot.id(), receipt);
    let after = db.execute_at_snapshot(sql, &params! {}, &snapshot).unwrap();
    assert_eq!(after.rows(), (1..=3).map(support::row).collect::<Vec<_>>());
    let q = params! {"body" => "x".repeat(17 << 20)};
    let read = || db.execute_at_snapshot("SELECT $body FROM t WHERE id = 1", &q, &snapshot);
    let (_, before) = (read(), support::used(&db)); // the first run records the statement's form
    let refused = read();
    assert_eq!(
        support::used(&db),
        before,
        "a refused read allocates nothing"
    );
    assert!(matches!(refused, Err(Error::MemoryBudgetExceeded { .. })));
    assert_eq!(support::ids(&db, "SELECT id FROM t ORDER BY id"), [4]);
    // The snapshot names its store: a second store in another folder refuses it.
    let other = support::open(&support::folder("pinned-moment-other"));
    support::declare(&other);
    let error = other
        .execute_at_snapshot(sql, &params! {}, &snapshot)
        .unwrap_err();
    assert!(matches!(error, Error::SnapshotMismatch), "{error:?}");
    let words = "the snapshot belongs to another store; take one from this store";
    assert_eq!(error.to_string(), words);
    // One moment, by file or through the writer: a later commit is not seen.
    let seen = |s: &contextdb::ReadSession| {
        let key = [contextdb::Value::Int64(4)];
        let versions = s.row_versions("t", &key, 4).unwrap();
        let (rows, ddl) = (s.execute(sql, &params! {}).unwrap(), s.declaration("t"));
        format!("{:?}", (rows.rows(), ddl.unwrap(), versions.rows.rows()))
    };
    let file = contextdb::ReadSession::open(&folder, Default::default());
    for (n, one) in [file, db.read_session(Default::default())]
        .into_iter()
        .enumerate()
    {
        let one = one.unwrap().into_one_moment().unwrap();
        let before = seen(&db.read_session(Default::default()).unwrap());
        support::run(&db, &format!("ALTER TABLE t ADD COLUMN c{n} TEXT"));
        support::run(&db, &format!("UPDATE t SET name = 'moved{n}' WHERE id = 4"));
        assert_eq!(seen(&one), before, "row, declaration and versions");
    }
}

#[test]
fn a_snapshot_searches_the_vectors_of_its_own_moment() {
    let db = support::open(&support::folder("snapshot-vectors"));
    support::declare_docs(&db, 4, "PARTITION_KEY (ctx) INDEXING_THRESHOLD 4");
    support::load_docs(&db, 1..=10, "a", |n| support::vector(n, 4));
    let query = support::vector(3, 4);
    let kept = support::stored(&db, support::DOCS).nearest(|_| true, &query, 10);
    let snapshot = db.snapshot().unwrap();
    support::run(&db, "DELETE FROM docs WHERE id <= 5");
    support::insert_doc(&db, 11, "a", Some(query.clone())).unwrap();
    support::maintenance(&db);
    let at = |mode| {
        let sql = support::by_mode(None, mode, 10);
        let found = db.execute_at_snapshot(&sql, &params! {"q" => query.clone()}, &snapshot);
        (found, sql)
    };
    let [(exact, sql), (auto, _)] = ["EXACT", "AUTO"].map(at);
    let (exact, auto) = (exact.unwrap(), auto.unwrap());
    for (mode, found) in [("EXACT", &exact), ("AUTO", &auto)] {
        support::assert_same_rows(&support::found(found, &sql), &kept, mode);
    }
    assert_eq!(auto.distance_evaluations(), exact.distance_evaluations());
    support::assert_indexed_refused(
        at("INDEXED").0,
        "a snapshot taken while the index was behind",
    );
    support::search(&db, &support::by_mode(None, "INDEXED", 5), &query).expect("rebuilt");
}
