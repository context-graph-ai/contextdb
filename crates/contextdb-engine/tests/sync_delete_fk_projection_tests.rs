use contextdb_core::{Lsn, SortDirection, Value};
use contextdb_engine::{
    Database,
    sync_types::{ChangeSet, DdlChange, NaturalKey, RowChange, SyncAdoption},
};
use std::collections::HashMap;

#[test]
fn rejected_keep_first_parent_delete_stays_present_during_mixed_ddl_fk_preflight() {
    let db = Database::open_memory();
    let p = HashMap::new();
    db.execute(
        "CREATE TABLE parents (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP FIRST",
        &p,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE children (id INTEGER PRIMARY KEY,parent_id INTEGER REFERENCES parents(id))",
        &p,
    )
    .unwrap();
    db.execute("INSERT INTO parents VALUES (1,'first')", &p)
        .unwrap();
    db.execute("INSERT INTO children VALUES (10,1)", &p)
        .unwrap();
    let lsn = Lsn(db.current_lsn().0 + 1);
    let change = ChangeSet {
        rows: vec![RowChange {
            table: "parents".into(),
            natural_key: NaturalKey::single("id".into(), Value::Int64(1)),
            values: HashMap::from([("__deleted".into(), Value::Bool(true))]),
            deleted: true,
            lsn,
            created_at: None,
        }],
        ddl: vec![DdlChange::CreateIndex {
            table: "children".into(),
            name: "idx_child_parent".into(),
            columns: vec![("parent_id".into(), SortDirection::Asc)],
        }],
        ddl_lsn: vec![lsn],
        ..Default::default()
    };
    db.apply_synced_changes(
        change,
        &db.conflict_policies(),
        &HashMap::from([(lsn, Some(lsn))]),
        SyncAdoption::Continuing,
    )
    .expect("rejected tombstone must not project parent absence or fail FK preflight");
    assert_eq!(
        db.execute("SELECT id FROM parents", &p).unwrap().rows.len(),
        1
    );
    assert_eq!(
        db.execute("SELECT id FROM children", &p)
            .unwrap()
            .rows
            .len(),
        1
    );
    assert!(
        db.table_meta("children")
            .unwrap()
            .indexes
            .iter()
            .any(|index| index.name == "idx_child_parent")
    );
}

#[test]
fn rejected_keep_first_composite_parent_delete_stays_present_during_mixed_ddl_fk_preflight() {
    let db = Database::open_memory();
    let p = HashMap::new();
    db.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY,a INTEGER,b INTEGER,UNIQUE(a,b)) SYNC CONFLICT KEEP FIRST",&p).unwrap();
    db.execute("CREATE TABLE children (id INTEGER PRIMARY KEY,pa INTEGER,pb INTEGER,FOREIGN KEY (pa,pb) REFERENCES parents(a,b))",&p).unwrap();
    db.execute("INSERT INTO parents VALUES (1,7,8)", &p)
        .unwrap();
    db.execute("INSERT INTO children VALUES (10,7,8)", &p)
        .unwrap();
    let lsn = Lsn(db.current_lsn().0 + 1);
    let change = ChangeSet {
        rows: vec![RowChange {
            table: "parents".into(),
            natural_key: NaturalKey::single("id".into(), Value::Int64(1)),
            values: HashMap::from([
                ("__deleted".into(), Value::Bool(true)),
                ("a".into(), Value::Int64(7)),
                ("b".into(), Value::Int64(8)),
            ]),
            deleted: true,
            lsn,
            created_at: None,
        }],
        ddl: vec![DdlChange::CreateIndex {
            table: "children".into(),
            name: "idx_child_pair".into(),
            columns: vec![
                ("pa".into(), SortDirection::Asc),
                ("pb".into(), SortDirection::Asc),
            ],
        }],
        ddl_lsn: vec![lsn],
        ..Default::default()
    };
    db.apply_synced_changes(
        change,
        &db.conflict_policies(),
        &HashMap::from([(lsn, Some(lsn))]),
        SyncAdoption::Continuing,
    )
    .expect("rejected composite tombstone must not project parent absence");
    assert_eq!(
        db.execute("SELECT id FROM parents", &p).unwrap().rows.len(),
        1
    );
    assert_eq!(
        db.execute("SELECT id FROM children", &p)
            .unwrap()
            .rows
            .len(),
        1
    );
    assert!(
        db.table_meta("children")
            .unwrap()
            .indexes
            .iter()
            .any(|index| index.name == "idx_child_pair")
    );
}
