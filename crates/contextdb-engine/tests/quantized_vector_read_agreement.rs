//! A column declared with space-saving vector storage keeps an approximation,
//! not the caller's original. Every way of reading it must therefore answer
//! the same approximation: while the writer that inserted it is still open,
//! after that writer is gone and the store is reopened, and through a reader
//! of the closed file. A warm writer answering from a precision that survives
//! nowhere else is the defect this pins.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_engine::Database;
use contextdb_engine::direct_file_reader::test_seams::{DirectReaderConfig, open_for_test};

struct StoppedClock;

impl DeadlineClock for StoppedClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn generous_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 1_000,
        result_bytes: 4 * 1024 * 1024,
        work: 100_000,
        active_ms: 10_000,
        memory: 32 * 1024 * 1024,
        cursor_page_rows: 8,
        cursor_page_bytes: 512 * 1024,
        cursor_idle_ms: 10_000,
        cursor_lifetime_ms: 20_000,
    }
}

/// Values chosen so quantization has something to lose: they do not sit on
/// the grid an eight- or four-bit code can represent exactly.
fn awkward_vector(dimension: usize, seed: f32) -> Vec<f32> {
    (0..dimension)
        .map(|index| seed + (index as f32) * 0.137_913_5 - 0.501_237)
        .collect()
}

fn read_vectors(rows: &[Vec<Value>]) -> (Vec<f32>, Vec<f32>) {
    let row = rows.first().expect("the seeded row is present");
    let (Value::Vector(compact), Value::Vector(tiny)) = (&row[1], &row[2]) else {
        panic!("both vector columns read back as vectors: {row:?}")
    };
    (compact.clone(), tiny.clone())
}

fn select_sql(table: &str) -> String {
    format!("SELECT id, compact, tiny FROM {table} ORDER BY id")
}

#[test]
fn a_quantized_column_reads_the_same_warm_reopened_and_through_the_file() {
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("quantized.db");
    let runtime = root.path().join("runtime");
    std::fs::create_dir(&runtime).expect("runtime directory");
    let table = "quantized_agreement";

    let original_compact = awkward_vector(6, 0.317_43);
    let original_tiny = awkward_vector(6, -0.209_11);

    let database = Database::open(&path).expect("seed writer opens");
    database
        .execute(
            &format!(
                "CREATE TABLE {table} (id INTEGER PRIMARY KEY, \
                 compact VECTOR(6) WITH (quantization = 'SQ8'), \
                 tiny VECTOR(6) WITH (quantization = 'SQ4'))"
            ),
            &HashMap::new(),
        )
        .expect("declare quantized columns");
    database
        .execute(
            &format!("INSERT INTO {table} (id, compact, tiny) VALUES ($id, $compact, $tiny)"),
            &HashMap::from([
                ("id".to_owned(), Value::Int64(1)),
                (
                    "compact".to_owned(),
                    Value::Vector(original_compact.clone()),
                ),
                ("tiny".to_owned(), Value::Vector(original_tiny.clone())),
            ]),
        )
        .expect("insert quantized vectors");

    let warm = database
        .execute(&select_sql(table), &HashMap::new())
        .expect("warm writer reads its own row");
    let (warm_compact, warm_tiny) = read_vectors(&warm.rows);

    // The point of the pin: the warm answer is the STORED approximation, so
    // it is not the caller's original. Without this, the three-way agreement
    // below could be satisfied by never quantizing at all.
    assert_ne!(
        warm_compact, original_compact,
        "an SQ8 column answers with what it stores, not with the original"
    );
    assert_ne!(
        warm_tiny, original_tiny,
        "an SQ4 column answers with what it stores, not with the original"
    );
    assert_eq!(warm_compact.len(), original_compact.len());
    assert_eq!(warm_tiny.len(), original_tiny.len());

    database.close().expect("close the seed writer");

    let reopened = Database::open(&path).expect("reopen the store");
    let after_restart = reopened
        .execute(&select_sql(table), &HashMap::new())
        .expect("reopened writer reads the row");
    let (restart_compact, restart_tiny) = read_vectors(&after_restart.rows);
    assert_eq!(
        restart_compact, warm_compact,
        "an SQ8 column answers the same after a restart as it did warm"
    );
    assert_eq!(
        restart_tiny, warm_tiny,
        "an SQ4 column answers the same after a restart as it did warm"
    );
    reopened.close().expect("close the reopened writer");

    let reader = open_for_test(
        &path,
        DirectReaderConfig::new(generous_limits(), Arc::new(StoppedClock), runtime.clone()),
    )
    .unwrap_or_else(|error| panic!("direct reader hydrates the closed store: {error}"));
    let direct = reader
        .execute(&select_sql(table), &HashMap::new())
        .unwrap_or_else(|error| panic!("direct reader reads the row: {error}"));
    let (direct_compact, direct_tiny) = read_vectors(&direct.result.rows);
    assert_eq!(
        direct_compact, warm_compact,
        "an SQ8 column answers the same through the file as it did warm"
    );
    assert_eq!(
        direct_tiny, warm_tiny,
        "an SQ4 column answers the same through the file as it did warm"
    );

    assert!(Path::new(&path).exists(), "the store is left where it was");
}
