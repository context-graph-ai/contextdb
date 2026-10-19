//! A reader serves the change history on request as the complete committed
//! record in the order its author wrote it. A stored history record that is
//! missing, extra, or out of its authored position is reported as damage
//! through the same door that serves the history -- never answered as a
//! shorter or reordered history -- and the frontiers a reader exposes at open
//! come from the committed record, not from whatever the store's last entry
//! says.

use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadFailureKind, ReadLimits};
use contextdb_core::{Lsn, Value};
use contextdb_engine::direct_file_reader::test_seams::{
    DirectChangeState, DirectFileReader, DirectFileReaderError, DirectImageMetadataKind,
    DirectImageState, DirectMetadataBody, DirectMetadataRequest, DirectReaderConfig,
    DirectStoreDiagnostic, open_for_test,
};
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    HistoryDamage, prepare_history_damage_for_test,
};
use contextdb_engine::{Database, DdlChange};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

struct FixedClock;

impl DeadlineClock for FixedClock {
    fn now_ms(&self) -> u64 {
        1
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn reader_config() -> DirectReaderConfig {
    DirectReaderConfig::new(
        ReadLimits {
            result_rows: 1_000,
            result_bytes: 8 * 1024 * 1024,
            work: 1_000_000,
            active_ms: 10_000,
            memory: 128 * 1024 * 1024,
            cursor_page_rows: 100,
            cursor_page_bytes: 2 * 1024 * 1024,
            cursor_idle_ms: 10_000,
            cursor_lifetime_ms: 20_000,
        },
        Arc::new(FixedClock),
    )
}

const SINGLE_ROW_COMMITS: usize = 6;

fn insert(database: &Database, tx: Option<contextdb_core::TxId>, row: u128) {
    let params = HashMap::from([
        (
            "id".to_owned(),
            Value::Uuid(uuid::Uuid::from_u128(0x41_0000 + row)),
        ),
        (
            "body".to_owned(),
            Value::Text(format!("history body {row}")),
        ),
    ]);
    let sql = "INSERT INTO history_rows (id, body) VALUES ($id, $body)";
    match tx {
        Some(tx) => database.execute_in_tx(tx, sql, &params).map(|_| ()),
        None => database.execute(sql, &params).map(|_| ()),
    }
    .unwrap_or_else(|error| panic!("insert history row {row}: {error}"));
}

/// One table, one transaction that declares two tables in a written order,
/// single-row commits, and one commit that inserts two rows.
fn seed(path: &Path) {
    let database = Database::open(path).expect("open history fixture writer");
    database
        .execute(
            "CREATE TABLE history_rows (id UUID PRIMARY KEY, body TEXT NOT NULL)",
            &HashMap::new(),
        )
        .expect("declare the history table");
    let tx = database
        .begin()
        .expect("begin the authored schema transaction");
    for statement in [
        "CREATE TABLE authored_first (id UUID PRIMARY KEY)",
        "CREATE TABLE authored_second (id UUID PRIMARY KEY, note TEXT)",
    ] {
        database
            .execute_in_tx(tx, statement, &HashMap::new())
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    database.commit(tx).expect("commit the authored schema");
    for row in 0..SINGLE_ROW_COMMITS {
        insert(&database, None, row as u128);
    }
    let tx = database.begin().expect("begin the two-row commit");
    insert(&database, Some(tx), 100);
    insert(&database, Some(tx), 101);
    database
        .commit(tx)
        .expect("commit two rows at one position");
    database.close().expect("close history fixture writer");
}

fn change_history(reader: &DirectFileReader) -> Result<DirectChangeState, DirectFileReaderError> {
    reader
        .metadata(DirectMetadataRequest::ImageState {
            kind: DirectImageMetadataKind::ChangeLog,
        })
        .map(|response| match response.body {
            DirectMetadataBody::ImageState {
                state: DirectImageState::ChangeLog(changes),
            } => changes,
            other => panic!("change history answers as change-log state: {other:?}"),
        })
}

fn table_order(changes: &DirectChangeState) -> Vec<String> {
    changes
        .ddl
        .iter()
        .filter_map(|change| match change {
            DdlChange::CreateTable { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect()
}

pub(crate) fn assert_reported_as_damage(
    result: Result<DirectChangeState, DirectFileReaderError>,
    damage: &str,
) {
    match result {
        Err(DirectFileReaderError::CorruptStore(
            DirectStoreDiagnostic::MalformedRecord,
            reason,
        )) => {
            assert!(!reason.is_empty(), "{damage}: the damage carries a reason")
        }
        Ok(changes) => panic!(
            "{damage} must not be answered as a history: commits={} rows={} tables={:?}",
            changes.commit_index.len(),
            changes.rows.len(),
            table_order(&changes)
        ),
        Err(other) => panic!("{damage} has the wrong diagnosis: {other:?}"),
    }
}

pub(crate) fn damaged_history(
    damage: HistoryDamage,
) -> Result<DirectChangeState, DirectFileReaderError> {
    let root = tempfile::tempdir().expect("history damage directory");
    let path = root.path().join("history.db");
    seed(&path);
    prepare_history_damage_for_test(&path, damage)
        .unwrap_or_else(|error| panic!("plant {damage:?}: {error}"));
    let reader = open_for_test(&path, reader_config()).unwrap_or_else(|error| {
        panic!("{damage:?} is history damage and does not block open: {error}")
    });
    change_history(&reader)
}

#[test]
fn undamaged_history_is_complete_and_in_the_order_it_was_authored() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    let root = tempfile::tempdir().expect("history control directory");
    let path = root.path().join("history.db");
    seed(&path);
    let reader = open_for_test(&path, reader_config()).expect("open the undamaged store");
    let changes = change_history(&reader).expect("the undamaged history answers");
    assert_eq!(
        table_order(&changes),
        ["history_rows", "authored_first", "authored_second"],
        "schema history is served in the order its author wrote it"
    );
    assert_eq!(
        changes.ddl_lsn[1], changes.ddl_lsn[2],
        "both authored statements belong to one commit"
    );
    assert_eq!(changes.rows.len(), SINGLE_ROW_COMMITS + 2);
    assert!(
        changes.commit_index.len() > SINGLE_ROW_COMMITS,
        "every commit is in the history: {:?}",
        changes.commit_index
    );
    assert_eq!(changes.current_lsn, changes.commit_index.last().unwrap().0);
}

#[test]
fn a_removed_row_insert_history_record_is_reported_not_answered_short() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    assert_reported_as_damage(
        damaged_history(HistoryDamage::RemovedRowInsertRecord),
        "a removed row-insert history record",
    );
}

#[test]
fn a_missing_change_log_table_is_reported_not_answered_empty() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    assert_reported_as_damage(
        damaged_history(HistoryDamage::MissingChangeLogTable),
        "a missing change-log table",
    );
}

#[test]
fn a_history_record_the_committed_image_does_not_hold_is_reported() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    assert_reported_as_damage(
        damaged_history(HistoryDamage::ExtraChangeRecord),
        "an extra change record",
    );
}

#[test]
fn swapped_schema_statements_within_one_commit_are_reported_not_replayed_reordered() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    assert_reported_as_damage(
        damaged_history(HistoryDamage::SwappedSameCommitSchemaStatements),
        "swapped same-commit schema statements",
    );
}

#[test]
fn swapped_change_records_within_one_commit_are_reported() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    assert_reported_as_damage(
        damaged_history(HistoryDamage::SwappedSameCommitChangeRecords),
        "swapped same-commit change records",
    );
}

#[test]
fn a_missing_commit_index_member_is_reported_not_answered_short() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    assert_reported_as_damage(
        damaged_history(HistoryDamage::RemovedCommitIndexMember),
        "a missing commit-index member",
    );
}

fn assert_open_refuses(damage: HistoryDamage, what: &str) {
    let root = tempfile::tempdir().expect("frontier damage directory");
    let path = root.path().join("history.db");
    seed(&path);
    prepare_history_damage_for_test(&path, damage)
        .unwrap_or_else(|error| panic!("plant {damage:?}: {error}"));
    match open_for_test(&path, reader_config()) {
        Err(DirectFileReaderError::CorruptStore(
            DirectStoreDiagnostic::MalformedRecord,
            reason,
        )) => {
            assert!(!reason.is_empty(), "{what}: the refusal carries a reason")
        }
        Ok(reader) => {
            let frontiers =
                contextdb_engine::direct_file_reader::test_seams::owned_image_for_test(&reader)
                    .map(|image| image.changes);
            panic!("{what} must be refused at open, not exposed as {frontiers:?}")
        }
        Err(other) => panic!("{what} has the wrong diagnosis at open: {other:?}"),
    }
}

#[test]
fn a_tampered_last_commit_transaction_is_refused_at_open() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    assert_open_refuses(
        HistoryDamage::RaisedLastCommitTransaction,
        "a raised last commit transaction",
    );
}

#[test]
fn a_schema_statement_moved_past_the_committed_frontier_is_refused_at_open() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    assert_open_refuses(
        HistoryDamage::MovedLastSchemaStatementLater,
        "a schema statement moved past the committed frontier",
    );
}

/// A reader keeps serving the complete history of the snapshot it opened
/// while a writer commits more.
#[test]
fn history_stays_the_opened_snapshot_while_a_writer_commits() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    let root = tempfile::tempdir().expect("history overlap directory");
    let path = root.path().join("history.db");
    seed(&path);
    let reader = open_for_test(&path, reader_config()).expect("open the snapshot reader");
    let database = Database::open(&path).expect("writer reopens while the reader is open");
    for row in 200..203 {
        insert(&database, None, row);
    }
    database.close().expect("close the overlapping writer");
    let changes = change_history(&reader).expect("the snapshot history answers");
    assert_eq!(changes.rows.len(), SINGLE_ROW_COMMITS + 2);
    let later = open_for_test(&path, reader_config()).expect("open a later reader");
    let later_changes = change_history(&later).expect("the later history answers");
    assert_eq!(later_changes.rows.len(), SINGLE_ROW_COMMITS + 5);
    assert!(later_changes.current_lsn > changes.current_lsn);
    assert!(Lsn(0) < changes.current_lsn);
}

/// Rewriting the stored change log records each rewritten entry at the exact
/// position it is stored under, ordinal included: the rewritten history is
/// answered complete, and its same-commit order stays bound to the
/// committed image.
#[test]
fn a_rewritten_change_log_keeps_every_record_at_its_committed_position() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    let root = tempfile::tempdir().expect("rewrite directory");
    let path = root.path().join("history.db");
    seed(&path);
    let persistence = contextdb_engine::persistence::RedbPersistence::open(&path)
        .expect("open the store's persistence");
    let entries = persistence.load_change_log().expect("load the change log");
    assert!(
        entries
            .windows(2)
            .any(|pair| pair[0].lsn() == pair[1].lsn()),
        "the fixture holds two change records at one commit position"
    );
    persistence
        .rewrite_change_log(&entries)
        .expect("rewrite the change log");
    persistence.close();
    drop(persistence);

    let reader = open_for_test(&path, reader_config()).expect("open the rewritten store");
    let changes = change_history(&reader).expect("the rewritten history answers");
    assert_eq!(changes.rows.len(), SINGLE_ROW_COMMITS + 2);
    drop(reader);

    prepare_history_damage_for_test(&path, HistoryDamage::SwappedSameCommitChangeRecords)
        .expect("swap two same-commit records of the rewritten log");
    let reader = open_for_test(&path, reader_config()).expect("open the damaged store");
    assert_reported_as_damage(
        change_history(&reader),
        "swapped same-commit records of a rewritten change log",
    );
}

/// A clock that advances one millisecond each time it is read once armed, and
/// can cancel a request on a chosen read -- so a history walk meets its
/// deadline or its cancellation part-way through, deterministically.
struct SteppingClock {
    armed: std::sync::atomic::AtomicBool,
    reads: std::sync::atomic::AtomicU64,
    cancel_at: Option<(
        u64,
        contextdb_engine::direct_file_reader::test_seams::DirectCancellation,
    )>,
}

impl DeadlineClock for SteppingClock {
    fn now_ms(&self) -> u64 {
        use std::sync::atomic::Ordering;
        if !self.armed.load(Ordering::SeqCst) {
            return 1;
        }
        let read = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some((at, cancellation)) = &self.cancel_at
            && read == *at
        {
            cancellation.cancel();
        }
        1 + read
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn history_under(
    clock: Arc<SteppingClock>,
    active_ms: u64,
    cancellation: &contextdb_engine::direct_file_reader::test_seams::DirectCancellation,
) -> (
    Result<DirectChangeState, DirectFileReaderError>,
    usize,
    usize,
) {
    use contextdb_engine::direct_file_reader::test_seams::{
        image_memory_for_test, metadata_with_cancellation_for_test,
    };
    let root = tempfile::tempdir().expect("history checkpoint directory");
    let path = root.path().join("history.db");
    seed(&path);
    let mut config = reader_config();
    config.limits.active_ms = active_ms;
    config.clock = clock.clone();
    let reader = open_for_test(&path, config).expect("open the checkpoint reader");
    let (before, _, _) = image_memory_for_test(&reader);
    clock.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    let answer = metadata_with_cancellation_for_test(
        &reader,
        DirectMetadataRequest::ImageState {
            kind: DirectImageMetadataKind::ChangeLog,
        },
        cancellation,
    )
    .map(|response| match response.body {
        DirectMetadataBody::ImageState {
            state: DirectImageState::ChangeLog(changes),
        } => changes,
        other => panic!("change history answers as change-log state: {other:?}"),
    });
    clock
        .armed
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let (after, _, _) = image_memory_for_test(&reader);
    (answer, before.used, after.used)
}

/// A request cancelled while its history is being walked stops there: it is
/// answered as cancelled and the reader keeps none of the history it had
/// started to read.
#[test]
fn a_history_request_cancelled_mid_walk_stops_and_keeps_nothing() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    let cancellation = contextdb_engine::direct_file_reader::test_seams::DirectCancellation::new();
    let clock = Arc::new(SteppingClock {
        armed: false.into(),
        reads: 0.into(),
        cancel_at: Some((6, cancellation.clone())),
    });
    let (answer, before, after) = history_under(clock, 1_000_000, &cancellation);
    assert!(
        matches!(answer, Err(DirectFileReaderError::Cancelled)),
        "a cancelled history request is answered as cancelled: {answer:?}"
    );
    assert_eq!(
        after, before,
        "the cancelled walk keeps nothing loaded or charged"
    );
}

/// A request whose active-time budget runs out while its history is being
/// walked stops there with the active-time refusal, keeping nothing.
#[test]
fn a_history_request_past_its_active_time_stops_mid_walk_and_keeps_nothing() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    let cancellation = contextdb_engine::direct_file_reader::test_seams::DirectCancellation::new();
    let clock = Arc::new(SteppingClock {
        armed: false.into(),
        reads: 0.into(),
        cancel_at: None,
    });
    let (answer, before, after) = history_under(clock, 5, &cancellation);
    match &answer {
        Err(DirectFileReaderError::ReadFailure(failure)) => assert_eq!(
            failure.kind(),
            contextdb_core::read_contract::ReadFailureKind::OwnerLimitExceeded,
            "the refusal is the active-time limit: {failure:?}"
        ),
        other => panic!("a history walk past its active time is refused: {other:?}"),
    }
    assert_eq!(
        after, before,
        "the refused walk keeps nothing loaded or charged"
    );
}

/// Open no longer walks the commit index, so a transaction id stored out of
/// order in the middle of it is caught where the index is read: the history
/// door reports it before any answer uses it.
#[test]
fn a_commit_index_entry_out_of_transaction_order_is_reported_where_history_is_read() {
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    assert_reported_as_damage(
        damaged_history(HistoryDamage::LoweredMiddleCommitTransaction),
        "a middle commit-index entry out of transaction order",
    );
}

/// A commit-index entry past the authenticated frontier was never committed.
/// A direct reader asks for writable recovery, which removes just that tail.
#[test]
fn an_uncommitted_commit_index_tail_requires_writable_recovery_at_open() {
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    let root = tempfile::tempdir().expect("uncommitted tail directory");
    let path = root.path().join("history.db");
    seed(&path);
    prepare_history_damage_for_test(&path, HistoryDamage::AppendedUncommittedCommitEntry)
        .expect("append an uncommitted entry");
    match open_for_test(&path, reader_config()) {
        Err(DirectFileReaderError::DirectReadRequiresWriter { failure, .. }) => {
            assert_eq!(failure.kind(), ReadFailureKind::DirectReadRequiresWriter);
        }
        Ok(_) => panic!("direct read must request writable recovery"),
        Err(other) => panic!("direct read has the wrong diagnosis: {other:?}"),
    }
    let writer = Database::open(&path).expect("writable open truncates the tail");
    writer.close().expect("close recovered writer");
    let reader = open_for_test(&path, reader_config()).expect("read recovered file");
    assert_eq!(
        change_history(&reader)
            .expect("committed history remains complete")
            .rows
            .len(),
        SINGLE_ROW_COMMITS + 2
    );
}
