//! The file-backed graph door must find and copy the row staged earlier in
//! this transaction without opening a mutating write-set borrow.
use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

#[test]
fn legacy_row_first_wider_key_alias_keeps_one_edge_and_properties_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("wide-row-first.db");
    let db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL, subtype_key TEXT NOT NULL DEFAULT '', UNIQUE (source_id, target_id, edge_type, subtype_key))", &HashMap::new()).unwrap();
    let source = Uuid::from_u128(101);
    let target = Uuid::from_u128(102);
    let id = Uuid::from_u128(103);
    let properties = HashMap::from([("weight".into(), Value::Float64(0.5))]);
    let tx = db.begin().unwrap();
    db.insert_row(
        tx,
        "edges",
        HashMap::from([
            ("id".into(), Value::Uuid(id)),
            ("source_id".into(), Value::Uuid(source)),
            ("target_id".into(), Value::Uuid(target)),
            ("edge_type".into(), Value::Text("link".into())),
        ]),
    )
    .unwrap();
    db.insert_edge(tx, source, target, "link".into(), properties.clone())
        .unwrap();
    db.commit(tx).unwrap();
    let check = |db: &Database| {
        assert_eq!(db.edge_count(source, "link", db.snapshot()).unwrap(), 1);
        assert_eq!(
            db.get_edge_properties(source, target, "link", db.snapshot())
                .unwrap(),
            Some(properties.clone())
        );
        let rows = db
            .execute(
                "SELECT id, source_id, target_id, edge_type, subtype_key FROM edges",
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(
            rows.rows(),
            &[vec![
                Value::Uuid(id),
                Value::Uuid(source),
                Value::Uuid(target),
                Value::Text("link".into()),
                Value::Text("".into())
            ]]
        );
        let row = db
            .point_lookup("edges", "id", &Value::Uuid(id), db.snapshot())
            .unwrap()
            .unwrap();
        assert_eq!(row.values.get("subtype_key"), Some(&Value::Text("".into())));
    };
    check(&db);
    db.close().unwrap();
    drop(db);
    let reopened = Database::open(&path).unwrap();
    check(&reopened);
    reopened.close().unwrap();
}
