//! `COUNT(*)` answers the same number whether or not the engine keeps a stored
//! count for the key it is asked about.
//!
//! The engine maintains one authenticated live-row count per
//! `(table, column list, prefix values)` an index write touches, at every
//! prefix length of every index. Where a primary key or a unique constraint
//! already covers exactly that column set, and no column in it admits NULL, the
//! count can only ever be 0 or 1 — and the index postings already say which,
//! in at most one entry. Those counts are no longer maintained.
//!
//! The risk this file exists to close is that the answer changes. It does not:
//! every `COUNT(*)` below is asserted against the rows actually visible, across
//! insert, update, delete and re-insert, on both a bounded key and an unbounded
//! one, and on a unique column list that admits NULL — where the constraint
//! does NOT bound the count, because a row with NULL in a constrained column
//! takes no part in the uniqueness check (`docs/query-language.md`), so the
//! count is still maintained there.

use contextdb_core::Value;
use contextdb_engine::append_work;
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;

fn execute(db: &Database, sql: &str, params: HashMap<String, Value>) {
    db.execute(sql, &params)
        .unwrap_or_else(|error| panic!("fixture statement `{sql}`: {error}"));
}

fn count(db: &Database, sql: &str, params: HashMap<String, Value>) -> i64 {
    let answer = db
        .execute(sql, &params)
        .unwrap_or_else(|error| panic!("count `{sql}`: {error}"));
    assert_eq!(answer.rows().len(), 1, "a count answers one row: {answer:?}");
    assert_eq!(answer.rows()[0].len(), 1, "a count answers one column");
    match &answer.rows()[0][0] {
        Value::Int64(count) => *count,
        other => panic!("a count is an integer, got {other:?}"),
    }
}

fn uuid(n: u128) -> uuid::Uuid {
    uuid::Uuid::from_u128(0xbeef_0000_0000_0000_0000_0000_0000_0000 + n)
}

/// `bounded_rows` declares a primary key and a NOT NULL unique pair; `tag` is
/// an ordinary indexed column whose counts stay maintained. `nullable_rows`
/// declares a unique pair one of whose columns admits NULL.
fn fixture(path: &std::path::Path) -> Database {
    let db = Database::open(path).expect("open the writable fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    execute(
        &db,
        "CREATE TABLE bounded_rows (id UUID PRIMARY KEY, owner TEXT NOT NULL, \
         slot TEXT NOT NULL, tag TEXT NOT NULL, UNIQUE (owner, slot))",
        HashMap::new(),
    );
    execute(
        &db,
        "CREATE INDEX bounded_by_tag ON bounded_rows (tag)",
        HashMap::new(),
    );
    execute(
        &db,
        "CREATE TABLE nullable_rows (id UUID PRIMARY KEY, owner TEXT NOT NULL, \
         slot TEXT, UNIQUE (owner, slot))",
        HashMap::new(),
    );
    db
}

fn insert_bounded(db: &Database, n: u128, owner: &str, slot: &str, tag: &str) {
    execute(
        db,
        "INSERT INTO bounded_rows (id, owner, slot, tag) VALUES ($id, $owner, $slot, $tag)",
        HashMap::from([
            ("id".into(), Value::Uuid(uuid(n))),
            ("owner".into(), Value::Text(owner.into())),
            ("slot".into(), Value::Text(slot.into())),
            ("tag".into(), Value::Text(tag.into())),
        ]),
    );
}

const BY_ID: &str = "SELECT COUNT(*) FROM bounded_rows WHERE id = $id";
const BY_OWNER_SLOT: &str =
    "SELECT COUNT(*) FROM bounded_rows WHERE owner = $owner AND slot = $slot";
const BY_TAG: &str = "SELECT COUNT(*) FROM bounded_rows WHERE tag = $tag";

fn by_id(n: u128) -> HashMap<String, Value> {
    HashMap::from([("id".into(), Value::Uuid(uuid(n)))])
}

fn by_owner_slot(owner: &str, slot: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("owner".into(), Value::Text(owner.into())),
        ("slot".into(), Value::Text(slot.into())),
    ])
}

fn by_tag(tag: &str) -> HashMap<String, Value> {
    HashMap::from([("tag".into(), Value::Text(tag.into()))])
}

#[test]
fn a_bounded_key_counts_the_same_through_insert_update_delete_and_reinsert() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let db = fixture(&root.path().join("bounded-counts.db"));

    assert_eq!(count(&db, BY_ID, by_id(1)), 0, "absent primary key");
    assert_eq!(
        count(&db, BY_OWNER_SLOT, by_owner_slot("ada", "left")),
        0,
        "absent unique pair"
    );
    assert_eq!(count(&db, BY_TAG, by_tag("green")), 0, "absent tag");

    insert_bounded(&db, 1, "ada", "left", "green");
    insert_bounded(&db, 2, "ada", "right", "green");
    insert_bounded(&db, 3, "bob", "left", "blue");

    assert_eq!(count(&db, BY_ID, by_id(1)), 1);
    assert_eq!(count(&db, BY_ID, by_id(9)), 0);
    assert_eq!(count(&db, BY_OWNER_SLOT, by_owner_slot("ada", "left")), 1);
    assert_eq!(count(&db, BY_OWNER_SLOT, by_owner_slot("ada", "middle")), 0);
    // The unbounded column keeps its maintained count and must still be right.
    assert_eq!(count(&db, BY_TAG, by_tag("green")), 2);
    assert_eq!(count(&db, BY_TAG, by_tag("blue")), 1);

    // An update that moves a row between keys.
    execute(
        &db,
        "UPDATE bounded_rows SET slot = 'middle', tag = 'blue' WHERE id = $id",
        by_id(1),
    );
    assert_eq!(count(&db, BY_ID, by_id(1)), 1, "the row still exists");
    assert_eq!(
        count(&db, BY_OWNER_SLOT, by_owner_slot("ada", "left")),
        0,
        "the key it left is empty"
    );
    assert_eq!(
        count(&db, BY_OWNER_SLOT, by_owner_slot("ada", "middle")),
        1,
        "the key it moved to holds it"
    );
    assert_eq!(count(&db, BY_TAG, by_tag("green")), 1);
    assert_eq!(count(&db, BY_TAG, by_tag("blue")), 2);

    // A delete, then a re-insert of the very same keys.
    execute(&db, "DELETE FROM bounded_rows WHERE id = $id", by_id(1));
    assert_eq!(count(&db, BY_ID, by_id(1)), 0, "deleted by primary key");
    assert_eq!(count(&db, BY_OWNER_SLOT, by_owner_slot("ada", "middle")), 0);
    assert_eq!(count(&db, BY_TAG, by_tag("blue")), 1);

    insert_bounded(&db, 1, "ada", "middle", "blue");
    assert_eq!(count(&db, BY_ID, by_id(1)), 1, "re-inserted primary key");
    assert_eq!(count(&db, BY_OWNER_SLOT, by_owner_slot("ada", "middle")), 1);
    assert_eq!(count(&db, BY_TAG, by_tag("blue")), 2);

    db.close().expect("close the fixture");
}

/// A unique constraint over a column that admits NULL bounds nothing: rows with
/// NULL there do not take part in the uniqueness check, so several can share
/// the key. The count for such a column list is still maintained, and it must
/// be right.
#[test]
fn a_unique_column_list_that_admits_null_keeps_its_maintained_count() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let db = fixture(&root.path().join("nullable-counts.db"));

    let insert = |n: u128, owner: &str, slot: Option<&str>| {
        execute(
            &db,
            "INSERT INTO nullable_rows (id, owner, slot) VALUES ($id, $owner, $slot)",
            HashMap::from([
                ("id".into(), Value::Uuid(uuid(n))),
                ("owner".into(), Value::Text(owner.into())),
                (
                    "slot".into(),
                    slot.map(|slot| Value::Text(slot.into()))
                        .unwrap_or(Value::Null),
                ),
            ]),
        );
    };
    // The claim is that a unique list containing a NULLable column is NOT
    // constraint-bounded, so its count stays maintained. Declare it: silence
    // maintains none and there would be no maintained count to keep.
    execute(
        &db,
        "ALTER TABLE nullable_rows SET MAINTAIN COUNT (owner, slot)",
        HashMap::new(),
    );
    insert(1, "ada", None);
    insert(2, "ada", None);
    insert(3, "ada", Some("left"));

    let null_count = count(
        &db,
        "SELECT COUNT(*) FROM nullable_rows WHERE owner = $owner AND slot IS NULL",
        HashMap::from([("owner".into(), Value::Text("ada".into()))]),
    );
    assert_eq!(
        null_count, 2,
        "two rows share a unique key that contains NULL, so the constraint bounds nothing"
    );
    let present_count = count(
        &db,
        "SELECT COUNT(*) FROM nullable_rows WHERE owner = $owner AND slot = $slot",
        HashMap::from([
            ("owner".into(), Value::Text("ada".into())),
            ("slot".into(), Value::Text("left".into())),
        ]),
    );
    assert_eq!(present_count, 1);

    db.close().expect("close the fixture");
}

/// The work this removes, measured rather than argued: the same one-event
/// append into the pinned cg startup schema now stages nine fewer `Status`
/// members, and every one it drops was over a column list a declared
/// constraint bounds.
#[test]
fn the_cg_event_append_stages_nine_fewer_status_members() {
    use crate::one_event_append_work_by_family as cg_append;

    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let db = Database::open(root.path().join("cg-append-after-n2a.db"))
        .expect("open the writable fixture");
    cg_append::declare(&db);
    // The claim this test makes is about the constraint rule, so the three
    // append tables declare every prefix: without a declaration the engine
    // maintains none and there is no "nine fewer" to measure.
    cg_append::declare_all_prefixes_on_the_append_tables(&db);
    cg_append::append_event(&db, 0);

    append_work::reset();
    cg_append::append_event(&db, 1);
    let namespaces = append_work::take_status_namespaces();

    assert_eq!(
        namespaces,
        vec![("rel-index-count-v2".to_owned(), 26)],
        "an append stages thirty-five index-prefix counts less the nine a declared constraint \
         already bounds"
    );
}
