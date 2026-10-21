use contextdb_core::read_contract::ReadRoute;
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use std::collections::HashMap;

fn exercise(reader: &ReadSession, route: ReadRoute) {
    assert_eq!(reader.route(), route);
    let baseline = reader.charged_bytes().expect("read accountant baseline");
    let query = "SELECT id, payload FROM charged_rows";
    let first = reader
        .open_cursor(query, &HashMap::new())
        .expect("first cursor");
    assert!(first.first_page().has_more(), "three rows exceed one page");
    let one = reader.charged_bytes().expect("first cursor charge");
    assert!(one > baseline, "hydrated rows retain a nonzero charge");
    let second = reader
        .open_cursor(query, &HashMap::new())
        .expect("second cursor");
    assert!(second.first_page().has_more());
    let two = reader.charged_bytes().expect("both cursor charges");
    assert!(
        two >= one,
        "retaining another cursor cannot reduce charged bytes"
    );
    second.close().expect("release second cursor");
    first.close().expect("release first cursor");
    assert_eq!(reader.charged_bytes().expect("released charge"), baseline);
}

#[test]
fn public_charged_bytes_reports_and_releases_cursor_hydration_on_both_routes() {
    let root = tempfile::tempdir().expect("task-scoped store directory");
    let path = root.path().join("charged-bytes.db");
    let writer = Database::open(&path).expect("open writer");
    writer
        .execute(
            "CREATE TABLE charged_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
            &HashMap::new(),
        )
        .expect("create rows");
    for id in 1..=3 {
        writer
            .execute(
                "INSERT INTO charged_rows VALUES ($id, $payload)",
                &HashMap::from([
                    ("id".into(), contextdb_core::Value::Int64(id)),
                    (
                        "payload".into(),
                        contextdb_core::Value::Text("x".repeat(16 * 1024)),
                    ),
                ]),
            )
            .expect("insert row");
    }
    let mut options = ReadSessionOptions::default();
    options.limits.cursor_page_rows = 1;
    let owner_reader =
        ReadSession::open_owner_only(&path, options.clone()).expect("open owner route");
    exercise(&owner_reader, ReadRoute::Owner);
    drop(owner_reader);
    writer.close().expect("close writer for file route");
    let file_reader =
        ReadSession::open_with_options(&path, options).expect("open committed file route");
    exercise(&file_reader, ReadRoute::File);
}
