use super::*;

fn assert_published_count(database: &Database, count: usize) {
    let rows = database
        .execute("SELECT id FROM items", &HashMap::new())
        .unwrap();
    let vectors = database
        .execute(
            "SELECT id FROM items ORDER BY embedding <=> [1.0, 0.0] LIMIT 10",
            &HashMap::new(),
        )
        .unwrap();
    assert_eq!(rows.rows().len(), count);
    assert_eq!(vectors.rows().len(), count);
    let inspection = database
        .execute("SHOW VECTOR_INDEXES", &HashMap::new())
        .unwrap();
    let column = inspection
        .columns()
        .iter()
        .position(|name| name == "vector_count")
        .unwrap();
    assert_eq!(inspection.rows()[0][column], Value::Int64(count as i64));
}

#[test]
fn conflicting_real_writers_publish_atomic_rows_and_vectors_one_at_a_time() {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(2))",
            &HashMap::new(),
        )
        .unwrap();
    let drive =
        database.write_publication_drive_for_test(VectorIndexRef::new("items", "embedding"));
    std::thread::scope(|scope| {
        let drive = drive;
        for id in [1, 2] {
            let writer = database.scoped_with_constraints(None, None, None);
            scope.spawn(move || {
                writer
                    .execute(
                        &format!("INSERT INTO items VALUES ({id}, [1.0, 0.0])"),
                        &HashMap::new(),
                    )
                    .unwrap();
            });
        }
        let queued = drive.wait_for_queued_for_test(2);
        assert_eq!(queued.len(), 2);
        assert_ne!(queued[0], queued[1]);
        drive.grant_for_test(queued[0]).unwrap();
        let first = drive
            .wait_for_published_and_released_for_test(queued[0])
            .unwrap();
        assert_eq!(first.txid, queued[0]);
        assert_published_count(&database, 1);
        drive.grant_for_test(queued[1]).unwrap();
        let second = drive
            .wait_for_published_and_released_for_test(queued[1])
            .unwrap();
        assert!(second.committed_lsn > first.committed_lsn);
        assert_published_count(&database, 2);
    });
}

#[test]
fn dropping_a_publication_drive_releases_every_parked_transaction() {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(2))",
            &HashMap::new(),
        )
        .unwrap();
    let drive =
        database.write_publication_drive_for_test(VectorIndexRef::new("items", "embedding"));
    std::thread::scope(|scope| {
        let drive = drive;
        for id in [1, 2] {
            let writer = database.scoped_with_constraints(None, None, None);
            scope.spawn(move || {
                writer
                    .execute(
                        &format!("INSERT INTO items VALUES ({id}, [1.0, 0.0])"),
                        &HashMap::new(),
                    )
                    .unwrap();
            });
        }
        assert_eq!(drive.wait_for_queued_for_test(2).len(), 2);
        drop(drive);
    });
    assert_published_count(&database, 2);
}

#[test]
fn a_vector_update_uses_the_same_publication_admission() {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, body TEXT, embedding VECTOR(2))",
            &HashMap::new(),
        )
        .unwrap();
    database
        .execute(
            "INSERT INTO items VALUES (1, 'before', [1.0, 0.0])",
            &HashMap::new(),
        )
        .unwrap();
    let drive =
        database.write_publication_drive_for_test(VectorIndexRef::new("items", "embedding"));
    std::thread::scope(|scope| {
        let drive = drive;
        let writer = database.scoped_with_constraints(None, None, None);
        scope.spawn(move || {
            writer
                .execute(
                    "UPDATE items SET embedding = [0.0, 1.0], body = 'after' WHERE id = 1",
                    &HashMap::new(),
                )
                .unwrap();
        });
        let queued = drive.wait_for_queued_for_test(1);
        assert_eq!(queued.len(), 1);
        assert_eq!(
            database
                .execute("SELECT body FROM items", &HashMap::new())
                .unwrap()
                .rows(),
            vec![vec![Value::Text("before".to_owned())]]
        );
        drive.grant_for_test(queued[0]).unwrap();
        drive
            .wait_for_published_and_released_for_test(queued[0])
            .unwrap();
        assert_eq!(
            database
                .execute("SELECT body FROM items", &HashMap::new())
                .unwrap()
                .rows(),
            vec![vec![Value::Text("after".to_owned())]]
        );
        assert_eq!(
            database
                .execute(
                    "SELECT id FROM items ORDER BY embedding <=> [0.0, 1.0] LIMIT 10",
                    &HashMap::new()
                )
                .unwrap()
                .rows(),
            vec![vec![Value::Int64(1)]]
        );
    });
}
