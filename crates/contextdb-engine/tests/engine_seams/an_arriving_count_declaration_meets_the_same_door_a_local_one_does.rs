//! A `MAINTAIN COUNT` declaration arriving over sync is judged by the SAME
//! door a locally typed one is.
//!
//! A schema author who writes `MAINTAIN COUNT` over a column list no index
//! leads with is refused by name, because nothing could ever read that count.
//! The two sync receive doors -- a received `AlterTable`, and a received
//! `CreateTable` adopting a table the receiver already has -- install the
//! declaration from the arriving constraint text, so without the same
//! question a peer could put into this store, and render back through
//! `.schema`, a clause this build refuses to its own operator, with no way
//! for that operator to see why it maintains nothing.
//!
//! The refusal is not a blanket one: the positive control at the end sends
//! the identical declaration to a receiver that HAS the index, and it is
//! adopted and maintained.

use contextdb_core::{Error, Lsn, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange};
use std::collections::HashMap;

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

fn schema_invalid_reason(error: Error) -> String {
    match error {
        Error::SchemaInvalid { reason } => reason,
        other => panic!("a refused declaration is a typed schema refusal, got {other:?}"),
    }
}

/// `owner` is a column of `placements` and nothing indexes it, so a count
/// over `(owner)` is exactly the list every local door refuses by name.
fn receiver_without_the_index() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE placements (id UUID PRIMARY KEY, owner TEXT NOT NULL, slot TEXT NOT NULL)",
        &p(),
    )
    .expect("the receiver already holds the table");
    db
}

fn columns() -> Vec<(String, String)> {
    vec![
        ("id".to_string(), "UUID PRIMARY KEY".to_string()),
        ("owner".to_string(), "TEXT NOT NULL".to_string()),
        ("slot".to_string(), "TEXT NOT NULL".to_string()),
    ]
}

fn arriving(ddl: DdlChange, db: &Database) -> ChangeSet {
    ChangeSet {
        rows: Vec::new(),
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: vec![ddl],
        ddl_lsn: vec![Lsn(db.current_lsn().0 + 1)],
    }
}

fn alter_declaring(list: &str) -> DdlChange {
    DdlChange::AlterTable {
        name: "placements".to_string(),
        columns: columns(),
        constraints: vec![format!("MAINTAIN COUNT {list}")],
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    }
}

fn create_declaring(list: &str) -> DdlChange {
    DdlChange::CreateTable {
        name: "placements".to_string(),
        columns: columns(),
        constraints: vec![format!("MAINTAIN COUNT {list}")],
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    }
}

fn apply(db: &Database, changes: ChangeSet) -> contextdb_core::Result<()> {
    db.apply_changes(
        changes,
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .map(|_| ())
}

/// The local door's own words, so the two refusals can be compared rather
/// than each asserted against its own hand-written expectation.
fn what_the_local_door_says() -> String {
    let db = receiver_without_the_index();
    let refused = db
        .execute("ALTER TABLE placements SET MAINTAIN COUNT (owner)", &p())
        .expect_err("a local declaration over an unindexed list is refused");
    schema_invalid_reason(refused)
}

#[test]
fn an_arriving_alter_table_is_refused_by_the_same_words_the_local_alter_is() {
    let local = what_the_local_door_says();
    assert!(
        local.contains("placements")
            && local.contains("(owner)")
            && local.contains("no index leads with"),
        "the local refusal names the table and the list: {local}"
    );

    let db = receiver_without_the_index();
    let before = db.table_meta("placements").expect("the table is here");
    let refused = apply(&db, arriving(alter_declaring("(owner)"), &db))
        .expect_err("an arriving declaration no index can carry is refused");
    assert_eq!(
        schema_invalid_reason(refused),
        local,
        "a peer meets the same door, word for word"
    );

    let after = db
        .table_meta("placements")
        .expect("the refused apply leaves the table alone");
    assert_eq!(
        after.maintained_counts, None,
        "the receiver stores no declaration it would refuse its own operator"
    );
    assert_eq!(
        after.columns.len(),
        before.columns.len(),
        "and the rest of the schema is untouched"
    );
    assert_eq!(after.indexes.len(), before.indexes.len());
}

#[test]
fn an_arriving_create_table_adopting_an_existing_table_meets_the_same_door() {
    let local = what_the_local_door_says();
    let db = receiver_without_the_index();
    let refused = apply(&db, arriving(create_declaring("(owner)"), &db))
        .expect_err("the adopt back door asks the identical question");
    assert_eq!(
        schema_invalid_reason(refused),
        local,
        "the CreateTable adopt door and the local door say the same thing"
    );
    assert_eq!(
        db.table_meta("placements")
            .expect("the table is still here")
            .maintained_counts,
        None,
        "nothing spelled as a create gets in where an alter is refused"
    );
}

/// The door refuses the DECLARATION, not the arrival. The identical wire
/// shape reaching a receiver that has the index is adopted, and the count it
/// declares is maintained there.
#[test]
fn the_same_declaration_is_adopted_where_an_index_carries_it() {
    let db = receiver_without_the_index();
    db.execute(
        "CREATE INDEX placements_by_owner ON placements (owner)",
        &p(),
    )
    .expect("the receiver builds the index the author has");
    for n in 0..6u128 {
        db.execute(
            "INSERT INTO placements (id, owner, slot) VALUES ($id, $owner, 'left')",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(uuid::Uuid::from_u128(n))),
                (
                    "owner".to_string(),
                    Value::Text(if n % 2 == 0 {
                        "ada".into()
                    } else {
                        "bob".into()
                    }),
                ),
            ]),
        )
        .expect("seed rows the count has to be right about");
    }
    apply(&db, arriving(alter_declaring("(owner)"), &db)).expect("the declaration is adopted");
    assert_eq!(
        db.table_meta("placements")
            .expect("the table is here")
            .maintained_counts,
        Some(contextdb_core::MaintainedCounts::Lists(vec![vec![(
            "owner".to_string(),
            contextdb_core::SortDirection::Asc
        )]])),
        "the receiver carries the declaration the author wrote"
    );
    let answer = db
        .execute(
            "SELECT COUNT(*) FROM placements WHERE owner = $owner",
            &HashMap::from([("owner".to_string(), Value::Text("ada".into()))]),
        )
        .expect("count over the adopted declaration");
    assert_eq!(answer.rows()[0][0], Value::Int64(3));
}
