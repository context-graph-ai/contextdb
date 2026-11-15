//! What a handle and a transaction promise the compiler, and what a lookup does with a damaged
//! index.

use std::collections::HashMap;

use super::{Database, Transaction};
use crate::types::{Error, Value};

fn shared<T: Send + Sync>() {}
fn sent<T: Send>() {}

/// No test may start a thread, so what "calls may be made from many threads" rests on is
/// checked here by the compiler.
#[test]
fn a_handle_is_shared_between_threads_and_a_transaction_moves_to_one() {
    shared::<Database>();
    sent::<Transaction<'_>>();
}

/// An index entry whose row is not in the table is damage of the store, never an empty answer:
/// a lookup through the index reads the row it names, and a row that is not there means the
/// store disagrees with itself. No public call makes such an entry, so it is staged through
/// the store's own write, as a commit of a store that was damaged would have left it.
#[test]
fn an_index_entry_with_no_row_is_refused_as_damage_by_a_lookup_through_it() {
    use crate::memory::Budget;
    use crate::schema::Catalog;
    use crate::store::{ChangeSet, Key, Store};

    let folder =
        std::env::temp_dir().join(format!("contextdb-orphan-entry-{}", std::process::id()));
    let db = Database::open(&folder).expect("open a fresh store");
    for sql in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, tag INTEGER)",
        "CREATE INDEX t_tag ON t (tag)",
        "INSERT INTO t (id, tag) VALUES (1, 7)",
    ] {
        db.execute(sql, &HashMap::new()).expect(sql);
    }
    drop(db);

    {
        let budget = Budget::new(None).expect("a budget");
        let store = Store::open_for_writing(&folder, &budget, None).expect("open the store");
        let view = store.view().expect("a view");
        let catalog = Catalog::load(&view).expect("the tables");
        let table = catalog.table("t").expect("table t");
        // Place 0 is the index behind the primary key; `t_tag` is place 1, the one `tag = 9` walks.
        let entry = table
            .entry_shape(1)
            .entry(&[Value::Int64(9)])
            .expect("an entry for tag 9");
        let absent = Key::from_values(&[Value::Int64(99)], &[]).expect("a key");
        let mut changes = ChangeSet::new(view, &budget).expect("begin");
        changes
            .put_index(table.index_ids[1], entry, absent)
            .expect("stage the entry");
        store.commit(changes).expect("commit");
    }

    let db = Database::open(&folder).expect("open the store again");
    let found = db
        .execute("SELECT id FROM t WHERE tag = 7", &HashMap::new())
        .expect("a lookup through an entry with its row");
    assert_eq!(
        found.rows(),
        [vec![Value::Int64(1)]],
        "an entry with its row is found"
    );
    let found = db.execute("SELECT id FROM t WHERE tag = 9", &HashMap::new());
    assert!(
        matches!(found, Err(Error::StoreCorrupted { .. })),
        "an index entry naming a row that is not there must be refused as damage, not skipped \
         into an empty answer: {found:?}"
    );
    drop(db);
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}
