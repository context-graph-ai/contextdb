//! A reopened store stamps its next commit after every transaction its
//! committed history already names.
//!
//! The commit index maps each committed LSN to its transaction, and a reader
//! proves that mapping never goes backwards. When the rows that carried the
//! newest transactions are gone -- their table dropped -- the surviving rows,
//! links and vectors no longer name the newest committed transaction; only
//! the commit index does. A store that restarts its transaction counter from
//! the surviving data alone hands its next commit an older transaction than
//! one already committed, and from then on its own history reads as damage.

use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_engine::Database;
use contextdb_engine::direct_file_reader::test_seams::{
    DirectImageMetadataKind, DirectImageState, DirectMetadataBody, DirectMetadataRequest,
    DirectReaderConfig, open_for_test,
};
use contextdb_engine::read_content_test_adapter::committed_content_for_test;
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

fn insert(database: &Database, table: &str, id: u128) {
    database
        .execute(
            &format!("INSERT INTO {table} (id, body) VALUES ($id, 'body')"),
            &HashMap::from([("id".to_owned(), Value::Uuid(Uuid::from_u128(id)))]),
        )
        .unwrap_or_else(|error| panic!("insert into {table}: {error}"));
}

/// Ask a committed reader for the store's complete history; it proves every
/// commit-index entry in order, so a transaction stamped out of order is
/// reported here as damage.
fn read_committed_history(path: &std::path::Path) {
    let reader = open_for_test(path, config()).expect("open the store as a committed reader");
    match reader
        .metadata(DirectMetadataRequest::ImageState {
            kind: DirectImageMetadataKind::ChangeLog,
        })
        .expect("the store's own committed history must read back, not as damage")
        .body
    {
        DirectMetadataBody::ImageState {
            state: DirectImageState::ChangeLog(_),
        } => {}
        other => panic!("change history answers as change-log state: {other:?}"),
    }
}

fn kept_rows(path: &std::path::Path) -> usize {
    {
        let declared_read_memory =
            contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                contextdb_engine::OwnerReadConfig::default()
                    .limits
                    .limits
                    .memory,
            );
        let operation_read_memory = declared_read_memory
            .child()
            .expect("admit the committed-content observation owner");
        committed_content_for_test(path, operation_read_memory, declared_read_memory)
    }
    .expect("the committed store authenticates against its own root")
    .relational_rows
    .iter()
    .filter(|row| row.table == "kept")
    .count()
}

#[test]
fn a_commit_after_reopening_a_store_whose_newest_rows_were_dropped_keeps_history_readable() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    let directory = tempfile::TempDir::new().expect("task-scoped store directory");
    let path = directory.path().join("reopened-transactions.db");
    {
        let database = Database::open(&path).expect("open the store");
        for table in ["kept", "dropped"] {
            database
                .execute(
                    &format!("CREATE TABLE {table} (id UUID PRIMARY KEY, body TEXT)"),
                    &HashMap::new(),
                )
                .expect("declare the table");
        }
        insert(&database, "kept", 0x01);
        // The newest transactions are stamped on rows of the table dropped
        // next.
        for id in 0x10..0x14 {
            insert(&database, "dropped", id);
        }
        database.close().expect("release the store");
    }
    {
        let database = Database::open(&path).expect("reopen the store");
        database
            .execute("DROP TABLE dropped", &HashMap::new())
            .expect("drop the table holding the newest rows");
        database.close().expect("release the store");
    }
    read_committed_history(&path);
    assert_eq!(kept_rows(&path), 1);

    {
        let database = Database::open(&path).expect("reopen the store");
        insert(&database, "kept", 0x02);
        database.close().expect("release the reopened store");
    }
    read_committed_history(&path);
    assert_eq!(
        kept_rows(&path),
        2,
        "the commit made after reopening is stored and its history stays in order"
    );
}
