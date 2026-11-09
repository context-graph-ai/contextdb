use super::*;

fn by_id(id: uuid::Uuid) -> HashMap<String, Value> {
    HashMap::from([("id".to_owned(), Value::Uuid(id))])
}

/// A delete whose row another writer already removed commits as a no-op,
/// and a no-op is traceless: the second commit reports no affected row,
/// the history a push would carry gains no second deletion, and the
/// reopened store is the one the first delete left.
#[test]
fn a_stale_staged_delete_leaves_no_second_deletion_anywhere() {
    let directory = tempfile::TempDir::new().expect("task-scoped stale-delete directory");
    let path = directory.path().join("stale-delete.db");
    let id = uuid::Uuid::from_u128(0x5721);

    let db = Database::open(&path).expect("open the store");
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .expect("declare the table");
    db.execute("CREATE INDEX notes_body ON notes (body)", &HashMap::new())
        .expect("declare the index the delete has to maintain");
    db.execute(
        "INSERT INTO notes (id, body) VALUES ($id, 'first')",
        &by_id(id),
    )
    .expect("seed the row both writers will delete");

    let stale = db
        .begin()
        .expect("begin the transaction that will go stale");
    let staged = db
        .execute_in_tx(stale, "DELETE FROM notes WHERE id = $id", &by_id(id))
        .expect("stage the delete while the row is still there");
    assert_eq!(staged.rows_affected(), 1, "the delete staged one row");

    db.execute("DELETE FROM notes WHERE id = $id", &by_id(id))
        .expect("another writer removes the same row first");

    let after_the_real_delete = db.changes_since(Lsn(0)).expect("read the declared changes");
    let deletions_before = after_the_real_delete
        .rows
        .iter()
        .filter(|change| change.deleted)
        .count();
    assert_eq!(deletions_before, 1, "one writer deleted the row once");

    db.commit(stale).expect("the stale delete commits");

    let after_the_stale_commit = db.changes_since(Lsn(0)).expect("read the declared changes");
    assert_eq!(
        after_the_stale_commit
            .rows
            .iter()
            .filter(|change| change.deleted)
            .count(),
        deletions_before,
        "the no-op raises no second deletion for a peer to apply"
    );
    assert_eq!(
        after_the_stale_commit.rows.len(),
        after_the_real_delete.rows.len(),
        "the no-op adds nothing at all to the history a push would carry"
    );
    assert!(
        db.execute("SELECT id FROM notes", &HashMap::new())
            .expect("read the table back")
            .rows()
            .is_empty(),
        "the row is gone, deleted exactly once"
    );

    db.close().expect("close the store");
    let reopened = Database::open(&path).expect("reopen the store");
    assert!(
        reopened
            .execute("SELECT id FROM notes", &HashMap::new())
            .expect("read the reopened table")
            .rows()
            .is_empty(),
        "the reopened store holds what the one real delete left"
    );
    assert_eq!(
        reopened
            .changes_since(Lsn(0))
            .expect("read the declared changes")
            .rows
            .iter()
            .filter(|change| change.deleted)
            .count(),
        deletions_before,
        "the durable history carries one deletion, not two"
    );
    reopened.close().expect("close the reopened store");
}
