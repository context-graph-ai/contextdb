use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use std::collections::HashMap;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn body(result: &QueryResult) -> &str {
    match &result.rows()[0][0] {
        Value::Text(value) => value.as_str(),
        other => panic!("expected text body, got {other:?}"),
    }
}

#[test]
fn disk_backed_rows_keep_writes_rollback_snapshots_and_reopen_coherent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("paged.contextdb");
    let old = "old-body:".to_owned() + &"a".repeat(32 * 1024);
    let new = "new-body:".to_owned() + &"b".repeat(32 * 1024);
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, unique_key TEXT UNIQUE, body TEXT NOT NULL, seq INTEGER NOT NULL)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO docs VALUES ($id, $key, $body, $seq)",
        &HashMap::from([
            ("id".into(), Value::Int64(1)),
            ("key".into(), Value::Text("one".into())),
            ("body".into(), Value::Text(old.clone())),
            ("seq".into(), Value::Int64(1)),
        ]),
    )
    .unwrap();
    let original_snapshot = db.snapshot();

    db.execute("BEGIN", &empty()).unwrap();
    db.execute(
        "UPDATE docs SET body = $body, seq = 2 WHERE id = 1",
        &HashMap::from([("body".into(), Value::Text(new.clone()))]),
    )
    .unwrap();
    db.execute("ROLLBACK", &empty()).unwrap();
    assert_eq!(
        body(
            &db.execute("SELECT body FROM docs WHERE id = 1", &empty())
                .unwrap()
        ),
        old
    );

    db.execute(
        "UPDATE docs SET body = $body, seq = 2 WHERE id = 1",
        &HashMap::from([("body".into(), Value::Text(new.clone()))]),
    )
    .unwrap();
    assert_eq!(
        db.scan("docs", original_snapshot).unwrap()[0].values["body"],
        Value::Text(old.clone())
    );
    assert_eq!(
        body(
            &db.execute("SELECT body FROM docs WHERE id = 1", &empty())
                .unwrap()
        ),
        new
    );
    db.execute(
        "INSERT INTO docs VALUES (2, 'one', 'duplicate', 1)",
        &empty(),
    )
    .unwrap();
    assert_eq!(
        db.execute("SELECT id FROM docs WHERE unique_key = 'one'", &empty())
            .unwrap()
            .rows(),
        vec![vec![Value::Int64(1)]]
    );

    db.execute("BEGIN", &empty()).unwrap();
    db.execute("DELETE FROM docs WHERE id = 1", &empty())
        .unwrap();
    db.execute("ROLLBACK", &empty()).unwrap();
    assert_eq!(
        db.execute("SELECT id FROM docs WHERE id = 1", &empty())
            .unwrap()
            .rows()
            .len(),
        1
    );
    db.close().unwrap();

    let reopened = Database::open(&path).unwrap();
    assert_eq!(
        body(
            &reopened
                .execute("SELECT body FROM docs WHERE id = 1", &empty())
                .unwrap()
        ),
        new
    );
    reopened
        .execute("CREATE INDEX docs_seq ON docs (seq)", &empty())
        .unwrap();
    reopened
        .execute("ALTER TABLE docs ADD COLUMN note TEXT", &empty())
        .unwrap();
    assert_eq!(
        body(
            &reopened
                .execute("SELECT body FROM docs WHERE seq = 2", &empty())
                .unwrap()
        ),
        new
    );
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    reopened.run_maintenance_cycle().unwrap();
    assert_eq!(
        body(
            &reopened
                .execute("SELECT body FROM docs WHERE id = 1", &empty())
                .unwrap()
        ),
        new
    );
    reopened
        .execute("DELETE FROM docs WHERE id = 1", &empty())
        .unwrap();
    assert!(
        reopened
            .execute("SELECT id FROM docs WHERE id = 1", &empty())
            .unwrap()
            .rows()
            .is_empty()
    );
    reopened.close().unwrap();
}
