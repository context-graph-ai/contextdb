#[cfg(debug_assertions)]
fn recovery_marking(db: Database, path: &std::path::Path) -> Database {
    let snapshot = crate::create_tempfile();
    std::fs::copy(path, snapshot.path()).unwrap();
    drop(db);
    std::fs::copy(snapshot.path(), path).unwrap();
    let mut builder = Database::builder();
    let mut db = builder.set_incremental_recovery(true).open(path).unwrap();
    while db.recovery_step(3).unwrap() != 0 {}
    let mut expected: std::collections::BTreeSet<_> =
        db.mem.all_allocated_pages().into_iter().collect();
    db.mem
        .visit_commit_chain(db.mem.commit_pages_to_free(), |page, _| {
            expected.remove(&page);
            Ok(())
        })
        .unwrap();
    let roots = Database::do_repair(&mut db.mem, &|_| {}, false).unwrap();
    assert_eq!(expected, db.mem.all_allocated_pages().into_iter().collect());
    Database::commit_repair(&db.mem, roots).unwrap();
    db
}
