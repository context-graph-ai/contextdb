use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use std::collections::HashMap;

#[test]
fn the_first_cursor_after_turnover_keeps_the_sessions_deadline_driver_alive() {
    for replacement_serves in [false, true] {
        let directory = tempfile::tempdir().expect("scratch directory");
        let path = directory.path().join("store.db");
        let writer = Database::open(&path).expect("first writer");
        writer
            .execute(
                "CREATE TABLE items (id INTEGER PRIMARY KEY)",
                &HashMap::new(),
            )
            .unwrap();
        writer
            .execute("INSERT INTO items VALUES (1), (2)", &HashMap::new())
            .unwrap();
        let session = ReadSession::open_with_options(
            &path,
            ReadSessionOptions {
                limits: ReadLimits {
                    cursor_page_rows: 1,
                    ..ReadLimits::default()
                },
                ..ReadSessionOptions::default()
            },
        )
        .expect("select the first writer without opening a cursor");
        writer.close().unwrap();
        let replacement = replacement_serves.then(|| Database::open(&path).unwrap());
        for operation in 0..3 {
            let mut cursor = session
                .open_cursor("SELECT id FROM items ORDER BY id", &HashMap::new())
                .unwrap_or_else(|error| {
                    panic!(
                        "operation {operation}, replacement serves {replacement_serves}: {error}"
                    )
                });
            assert_eq!(cursor.first_page().rows(), &[vec![Value::Int64(1)]]);
            assert!(cursor.first_page().has_more());
            assert_eq!(cursor.fetch(None).unwrap().rows(), &[vec![Value::Int64(2)]]);
            cursor.close().unwrap();
        }
        drop(session);
        if let Some(replacement) = replacement {
            replacement.close().unwrap();
        }
    }
}
