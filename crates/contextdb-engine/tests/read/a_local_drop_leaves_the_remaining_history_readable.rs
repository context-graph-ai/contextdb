//! A committed reader answers a store's history after one of its tables was
//! dropped: the surviving tables' changes in full, and nothing for the table
//! that no longer exists.
//!
//! A local drop leaves the dropped table's row changes in the change log. The
//! live store answers history by skipping changes whose table is gone; a
//! committed reader must answer the same way, not refuse the whole history
//! because one of its tables is missing, and not answer it empty.

use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_engine::Database;
use contextdb_engine::direct_file_reader::test_seams::{
    DirectImageMetadataKind, DirectImageState, DirectMetadataBody, DirectMetadataRequest,
    DirectReaderConfig, open_for_test,
};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

struct FixedClock;

impl DeadlineClock for FixedClock {
    fn now_ms(&self) -> u64 {
        1
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn config() -> DirectReaderConfig {
    DirectReaderConfig::new(
        ReadLimits {
            result_rows: 1_000,
            result_bytes: 64 * 1024 * 1024,
            work: 1_000_000,
            active_ms: 60_000,
            memory: 1024 * 1024 * 1024,
            cursor_page_rows: 100,
            cursor_page_bytes: 2 * 1024 * 1024,
            cursor_idle_ms: 10_000,
            cursor_lifetime_ms: 20_000,
        },
        Arc::new(FixedClock),
    )
}

#[test]
fn a_local_drop_leaves_the_other_tables_history_readable_and_whole() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    let directory = tempfile::TempDir::new().expect("task-scoped store directory");
    let path = directory.path().join("local-drop-history.db");
    let database = Database::open(&path).expect("open the store");
    for table in ["kept", "dropped"] {
        database
            .execute(
                &format!("CREATE TABLE {table} (id UUID PRIMARY KEY, body TEXT)"),
                &HashMap::new(),
            )
            .expect("declare the table");
    }
    for (table, id) in [
        ("kept", 0x01u128),
        ("dropped", 0x10),
        ("kept", 0x02),
        ("dropped", 0x11),
    ] {
        database
            .execute(
                &format!("INSERT INTO {table} (id, body) VALUES ($id, 'body')"),
                &HashMap::from([("id".to_owned(), Value::Uuid(Uuid::from_u128(id)))]),
            )
            .expect("insert a row");
    }
    database
        .execute("DROP TABLE dropped", &HashMap::new())
        .expect("drop one of the two tables");
    database.close().expect("release the store");

    let reader = open_for_test(&path, config()).expect("open the store as a committed reader");
    let changes = match reader
        .metadata(DirectMetadataRequest::ImageState {
            kind: DirectImageMetadataKind::ChangeLog,
        })
        .expect("the history of a store with a dropped table answers")
        .body.clone()
    {
        DirectMetadataBody::ImageState {
            state: DirectImageState::ChangeLog(changes),
        } => changes,
        other => panic!("change history answers as change-log state: {other:?}"),
    };
    let mut tables = changes
        .rows
        .iter()
        .map(|row| row.table.as_str())
        .collect::<Vec<_>>();
    tables.sort_unstable();
    assert_eq!(
        tables,
        ["kept", "kept"],
        "both of the surviving table's changes, and none of the dropped table's"
    );
}
