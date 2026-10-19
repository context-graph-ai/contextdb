//! A read-only open after the writer has closed does work and holds memory
//! for what the store currently is, not for how much history produced it:
//! the same live content opens the same way after a short or a long history
//! of unindexed vector writes or of deleted rows.
use contextdb_core::Value;
use contextdb_engine::vector_read_memory::{
    take_reader_budget_ledger_for_test, watch_reader_budget_ledger_for_test,
};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;

const DIM: usize = 64;

fn open_writer(path: &std::path::Path) -> Database {
    let mut options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        memory_limit: Some(512 * 1024 * 1024),
        ..DatabaseOpenOptions::default()
    };
    options.owner_reads.limits.limits.memory = 1 << 30;
    Database::open_with_options(path, options).unwrap()
}

fn vector(id: i64) -> Vec<f32> {
    let mut values: Vec<f32> = (0..DIM)
        .map(|lane| ((id as f32) * 0.618 + lane as f32 * 0.37).sin() * 0.25)
        .collect();
    values[(id as usize) % DIM] += 4.0;
    values
}

/// The reader's open peak and the number of reader charges the open took.
fn read_only_open(path: &std::path::Path) -> (usize, usize, ReadSession) {
    let mut options = ReadSessionOptions {
        memory_limit: Some(512 * 1024 * 1024),
        ..ReadSessionOptions::default()
    };
    options.limits.active_ms = 600_000;
    options.limits.memory = 1 << 30;
    watch_reader_budget_ledger_for_test();
    let session = ReadSession::open_with_options(path, options).unwrap();
    let ledger = take_reader_budget_ledger_for_test();
    let peak = ledger.iter().map(|e| e.used_after).max().unwrap_or(0);
    (peak, ledger.len(), session)
}

/// 16 indexed vectors, then `batches` of 256 vectors left in the unindexed
/// suffix when the writer closes.
fn vector_suffix_store(path: &std::path::Path, batches: i64) {
    let db = open_writer(path);
    db.execute(&format!("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR({DIM}) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32) CONSOLIDATION NONE)"), &HashMap::new()).unwrap();
    for id in 0..16i64 {
        db.execute(
            "INSERT INTO points VALUES ($id, 1, $v)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("v".into(), Value::Vector(vector(id))),
            ]),
        )
        .unwrap();
    }
    while db
        .run_maintenance_cycle()
        .unwrap()
        .vector
        .remaining_partitions
        != 0
    {}
    for batch in 0..batches {
        let first = 1_000_000 + batch * 256;
        let mut sql = String::from("INSERT INTO points VALUES ");
        let mut params = HashMap::new();
        for row in 0..256 {
            if row > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!("($i{row}, 1, $v{row})"));
            params.insert(format!("i{row}"), Value::Int64(first + row));
            params.insert(format!("v{row}"), Value::Vector(vector(first + row)));
        }
        db.execute(&sql, &params).unwrap();
    }
    db.close().unwrap();
}

/// `count` rows inserted and deleted one at a time; no row is live at close.
fn deleted_rows_store(path: &std::path::Path, count: i64) {
    let db = open_writer(path);
    db.execute(
        "CREATE TABLE gone (id INTEGER PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE kept (id INTEGER PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("INSERT INTO kept VALUES (1, 'live')", &HashMap::new())
        .unwrap();
    for id in 0..count {
        let params = HashMap::from([("id".to_string(), Value::Int64(id))]);
        db.execute(
            "INSERT INTO gone VALUES ($id, 'body of a row that will be deleted')",
            &params,
        )
        .unwrap();
        db.execute("DELETE FROM gone WHERE id = $id", &params)
            .unwrap();
    }
    db.close().unwrap();
}

fn assert_flat(kind: &str, short: (usize, usize), long: (usize, usize)) {
    eprintln!(
        "read_only_open kind={kind} short_peak={} short_charges={} long_peak={} long_charges={}",
        short.0, short.1, long.0, long.1
    );
    assert!(
        long.1 <= short.1 + 32,
        "{kind}: a longer history must not add reader charges to a read-only open \
         ({} charges after the short history, {} after the long one)",
        short.1,
        long.1
    );
    assert!(
        long.0 <= short.0 + 16 * 1024,
        "{kind}: a longer history must not raise the read-only open peak \
         ({} B after the short history, {} B after the long one)",
        short.0,
        long.0
    );
}

#[test]
fn a_read_only_open_does_the_same_work_after_a_short_or_long_unindexed_vector_history() {
    let root = tempfile::tempdir().unwrap();
    let short_path = root.path().join("suffix-2.db");
    let long_path = root.path().join("suffix-8.db");
    vector_suffix_store(&short_path, 2);
    vector_suffix_store(&long_path, 8);
    let (short_peak, short_charges, short) = read_only_open(&short_path);
    let (long_peak, long_charges, long) = read_only_open(&long_path);
    assert_flat(
        "vector_suffix",
        (short_peak, short_charges),
        (long_peak, long_charges),
    );
    // Every unindexed vector stays visible: the open's transaction bound
    // still covers the newest suffix write.
    for (session, rows) in [(short, 16 + 2 * 256), (long, 16 + 8 * 256)] {
        let cursor = session
            .open_cursor("SELECT COUNT(*) FROM points", &HashMap::new())
            .unwrap();
        assert_eq!(
            cursor.first_page().rows,
            vec![vec![Value::Int64(rows)]],
            "the read-only open sees every committed vector row"
        );
    }
}

#[test]
fn a_read_only_open_does_the_same_work_after_a_short_or_long_history_of_deleted_rows() {
    let root = tempfile::tempdir().unwrap();
    let short_path = root.path().join("deletes-256.db");
    let long_path = root.path().join("deletes-1024.db");
    deleted_rows_store(&short_path, 256);
    deleted_rows_store(&long_path, 1024);
    let (short_peak, short_charges, short) = read_only_open(&short_path);
    let (long_peak, long_charges, long) = read_only_open(&long_path);
    assert_flat(
        "deleted_rows",
        (short_peak, short_charges),
        (long_peak, long_charges),
    );
    for session in [short, long] {
        let cursor = session
            .open_cursor("SELECT COUNT(*) FROM gone", &HashMap::new())
            .unwrap();
        assert_eq!(cursor.first_page().rows, vec![vec![Value::Int64(0)]]);
        let cursor = session
            .open_cursor("SELECT body FROM kept WHERE id = 1", &HashMap::new())
            .unwrap();
        assert_eq!(
            cursor.first_page().rows,
            vec![vec![Value::Text("live".into())]]
        );
    }
}
