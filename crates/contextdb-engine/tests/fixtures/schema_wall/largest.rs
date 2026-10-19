//! The 100-table FK chain's largest closure, with rows and vectors on every table.
//! The caller installs the unchanged 102-declaration replay fixture first.
//! Shared with the reader-overlap proof; unrelated resident content is separate.

use super::{empty, initialize_fixture_callbacks};
use contextdb_core::Value;
use contextdb_engine::{Database, sync_types::ChangeSet};
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

pub type Calls = Arc<Mutex<Vec<Value>>>;

pub fn affected_tables() -> BTreeSet<String> {
    (0..100).map(|i| format!("schema_{i:03}")).collect()
}

pub fn vector(ordinal: usize) -> Value {
    let mut values = vec![0.0; 768];
    values[0] = 1.0;
    values[1] = ordinal as f32;
    Value::Vector(values)
}

pub fn prepare_existing_rows_and_vectors(db: &Database) -> Calls {
    let calls = Arc::new(Mutex::new(Vec::new()));
    db.register_trigger_callback("schema_099_insert_trigger", {
        let calls = calls.clone();
        move |_db, context| {
            calls.lock().unwrap().push(context.row_values["id"].clone());
            Ok(())
        }
    })
    .expect("register real final-table callback before any writes");
    initialize_fixture_callbacks(db);
    for table in affected_tables() {
        db.execute(
            &format!("ALTER TABLE {table} ADD COLUMN embedding VECTOR(768)"),
            &empty(),
        )
        .expect("put vector state on an affected table, not only an unrelated fixture");
    }
    let tx = db.begin().unwrap();
    for ordinal in 0..100 {
        let params = HashMap::from([("v".into(), vector(ordinal))]);
        let sql = if ordinal == 0 {
            "INSERT INTO schema_000 (id, body, embedding) VALUES (1, 'kept', $v)".into()
        } else {
            format!(
                "INSERT INTO schema_{ordinal:03} (id, parent_id, body, embedding) VALUES (1, 1, 'kept', $v)"
            )
        };
        db.execute_in_tx(tx, &sql, &params)
            .expect("seed the dependency-complete FK chain");
    }
    db.commit(tx).unwrap();
    assert_eq!(*calls.lock().unwrap(), vec![Value::Int64(1)]);
    calls
}

pub fn author_migration(db: &Database) -> ChangeSet {
    let since = db.current_lsn();
    db.execute("ALTER TABLE schema_000 ADD COLUMN revised TEXT", &empty())
        .unwrap();
    // The root's transitive FK closure is all 100 existing tables. Listing 100
    // ADD COLUMN entries would hide a broken dependency-closure calculation.
    db.changes_since(since).expect("read the declared changes")
}

pub fn rows_and_vectors(db: &Database) -> Vec<Vec<Vec<Value>>> {
    (0..100)
        .map(|ordinal| {
            let table = format!("schema_{ordinal:03}");
            let result = db
                .execute(
                    &format!("SELECT id, body, embedding FROM {table} ORDER BY id"),
                    &empty(),
                )
                .unwrap()
                .rows;
            assert_eq!(
                result,
                vec![vec![
                    Value::Int64(1),
                    Value::Text("kept".into()),
                    vector(ordinal)
                ]]
            );
            let query = HashMap::from([("q".into(), vector(ordinal))]);
            assert_eq!(
                db.execute(
                    &format!(
                        "SELECT id FROM {table} ORDER BY embedding <=> $q USE VECTOR EXACT LIMIT 1"
                    ),
                    &query
                )
                .unwrap()
                .rows,
                vec![vec![Value::Int64(1)]],
                "the affected table's vector remains searchable"
            );
            result
        })
        .collect()
}

pub fn assert_constraints_and_callback(db: &Database, calls: &Calls) {
    db.execute(
        "CREATE EVENT TYPE schema_099_notice WHEN INSERT ON schema_099",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE SINK schema_099_delivery TYPE callback", &empty())
        .unwrap();
    db.execute(
        "CREATE ROUTE schema_099_route EVENT schema_099_notice TO schema_099_delivery",
        &empty(),
    )
    .unwrap();
    let events = db.subscribe();
    let calls_before = calls.lock().unwrap().len();
    assert!(
        db.execute(
            "INSERT INTO schema_099 (id, parent_id, body) VALUES (3, 999, 'invalid')",
            &empty()
        )
        .is_err(),
        "the last dependent still enforces its foreign key"
    );
    let invalid_callback_ran = calls.lock().unwrap().len() > calls_before;
    assert!(calls.lock().unwrap().len() <= calls_before + 1);
    assert!(
        db.execute("SELECT id FROM schema_099 WHERE id = 3", &empty())
            .unwrap()
            .rows
            .is_empty(),
        "refused observation leaves no durable record"
    );
    assert_eq!(db.sink_metrics("schema_099_delivery").queued, 0);
    assert!(
        events.try_recv().is_err(),
        "refused observation emits no event"
    );
    db.execute(
        "INSERT INTO schema_099 (id, parent_id, body) VALUES (2, 1, 'after-replay')",
        &empty(),
    )
    .unwrap();
    let observed = calls.lock().unwrap().clone();
    assert_eq!(observed.first(), Some(&Value::Int64(1)));
    assert_eq!(observed.last(), Some(&Value::Int64(2)));
    assert_eq!(observed.len(), 2 + usize::from(invalid_callback_ran));
    assert_eq!(
        db.execute("SELECT id FROM schema_099 WHERE id = 2", &empty())
            .unwrap()
            .rows,
        vec![vec![Value::Int64(2)]],
        "accepted observation is durable"
    );
    assert_eq!(db.sink_metrics("schema_099_delivery").queued, 1);
    assert!(
        events.try_recv().is_ok(),
        "accepted observation emits an event"
    );
    assert!(
        db.list_triggers()
            .iter()
            .any(|v| v.name == "schema_099_insert_trigger" && v.including_sync)
    );
}
