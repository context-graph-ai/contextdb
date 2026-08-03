use contextdb_core::Value;
use contextdb_engine::Database;

#[cfg(not(feature = "durable-limits"))]
fn main() {
    let db = Database::open_memory();
    let _ = db.disk_limit();
    let _ = db.disk_file_size();
}

#[cfg(feature = "durable-limits")]
fn main() {
    let path = std::env::args().nth(1).expect("file-backed database path");
    let memory = 8 * 1024 * 1024;
    let disk = 16 * 1024 * 1024;
    let db = Database::open(&path).expect("open durable database");
    db.set_memory_limit(Some(memory))
        .expect("Database-owned memory setter");
    db.set_disk_limit(Some(disk))
        .expect("Database-owned disk setter");
    drop(db);

    let reopened = Database::open(&path).expect("reopen durable database");
    let shown = reopened
        .execute("SHOW MEMORY_LIMIT", &std::collections::HashMap::new())
        .expect("show durable memory limit");
    assert_eq!(
        shown.columns,
        vec![
            "limit".to_string(),
            "used".to_string(),
            "available".to_string(),
            "startup_ceiling".to_string(),
        ]
    );
    assert_eq!(shown.rows.len(), 1, "one memory-limit row");
    let row = &shown.rows[0];
    let [
        Value::Int64(limit),
        Value::Int64(used),
        Value::Int64(available),
        Value::Text(startup),
    ] = row.as_slice()
    else {
        panic!("SHOW MEMORY_LIMIT must return the durable limit and no startup ceiling: {row:?}");
    };
    assert_eq!(*limit, memory as i64);
    assert_eq!(startup, "none");
    assert_eq!(*available, (memory as i64 - *used).max(0));
    assert_eq!(reopened.disk_limit(), Some(disk));
}
