//! Scratch stores report the same durable body sizes as explicit file stores.
use contextdb_core::Value;
use contextdb_engine::Database;
use redb::{ReadableDatabase as _, ReadableTable as _};
use std::collections::HashMap;

#[test]
fn memory_content_size_refuses_without_inventing_persisted_bytes() {
    let database = Database::open_memory();
    let params = HashMap::new();
    for populated in [false, true] {
        if populated {
            database
                .execute(
                    "CREATE TABLE memory_sizes (id INT PRIMARY KEY, payload TEXT, embedding VECTOR(3)) HISTORY ALL",
                    &params,
                )
                .expect("create memory-only content fixture");
            database
                .execute(
                    "INSERT INTO memory_sizes (id, payload, embedding) VALUES (1, $payload, $embedding)",
                    &HashMap::from([
                        ("payload".into(), Value::Text("persisted size is unavailable".into())),
                        ("embedding".into(), Value::Vector(vec![1.0, 2.0, 3.0])),
                    ]),
                )
                .expect("populate memory-only rows and vector bodies");
        }
        let answer = database
            .execute("SHOW CONTENT_SIZE", &params)
            .expect("task scratch has persisted committed bodies");
        let artifact = tempfile::tempdir().unwrap();
        let exported = artifact.path().join("scratch-copy.db");
        database
            .export_snapshot(&exported)
            .expect("capture actual committed scratch bodies");
        let raw = redb::Database::open(&exported).unwrap();
        let read = raw.begin_read().unwrap();
        let body_bytes = |name| {
            let definition: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new(name);
            match read.open_table(definition) {
                Ok(table) => table
                    .iter()
                    .unwrap()
                    .map(|entry| entry.unwrap().1.value().len() as i64)
                    .sum::<i64>(),
                Err(redb::TableError::TableDoesNotExist(_)) => 0,
                Err(error) => panic!("inspect exported physical bodies: {error}"),
            }
        };
        let rows = body_bytes("rel_memory_sizes");
        let vectors = body_bytes("vector_entries");
        assert_eq!(rows > 0, populated);
        assert_eq!(vectors > 0, populated);
        assert_eq!(
            answer.columns(),
            vec![
                "live_row_bytes",
                "live_vector_bytes",
                "live_content_bytes",
                "retained_row_bytes",
                "retained_vector_bytes"
            ]
        );
        assert_eq!(
            answer.rows(),
            vec![vec![
                Value::Int64(rows),
                Value::Int64(vectors),
                Value::Int64(rows + vectors),
                Value::Int64(0),
                Value::Int64(0)
            ]]
        );
    }
    let answer = database
        .execute("SELECT payload FROM memory_sizes WHERE id = 1", &params)
        .expect("durable size inspection leaves the store usable");
    assert_eq!(
        answer.rows(),
        vec![vec![Value::Text("persisted size is unavailable".into())]]
    );
}
