//! A reader tells a store whose committed history is corrupt from one whose
//! history only lacks an index entry a writer can rebuild, also when the
//! check runs at the first history request instead of at open.
//!
//! A deleted edge's tombstone lives in the change log, so a reader that
//! defers reading history also defers checking that tombstone against the
//! commit index. When the commit index names a different transaction for the
//! tombstone's commit, the committed history is corrupt; when it has no entry
//! for that commit at all, a writer has to reconstruct it. Each answer keeps
//! its own typed diagnosis -- the ones a reader gives for the same damage
//! found at open -- instead of collapsing into an untyped engine message.

use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadFailureKind, ReadLimits};
use contextdb_engine::Database;
use contextdb_engine::direct_file_reader::test_seams::{
    DirectFileReaderError, DirectImageMetadataKind, DirectImageState, DirectMetadataBody,
    DirectMetadataRequest, DirectReaderConfig, DirectRepairRequiredCause, DirectStoreDiagnostic,
    open_for_test,
};
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    TombstoneCommitDamage, damage_last_commit_for_edge_tombstone_for_test,
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

/// A store whose last commit deletes an edge.
fn store_ending_in_an_edge_delete() -> (tempfile::TempDir, std::path::PathBuf) {
    let root = tempfile::TempDir::new().expect("task-scoped store directory");
    let path = root.path().join("tombstone.db");
    let database = Database::open(&path).expect("open the store");
    database
        .execute(
            "CREATE TABLE marks (id UUID PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .expect("declare an ordinary table");
    let (source, target) = (Uuid::from_u128(0x7a_01), Uuid::from_u128(0x7a_02));
    let tx = database.begin().expect("begin the link");
    database
        .insert_edge(tx, source, target, "MARKS".to_owned(), HashMap::new())
        .expect("insert the link");
    database.commit(tx).expect("commit the link");
    let tx = database.begin().expect("begin the unlink");
    database
        .delete_edge(tx, source, target, "MARKS")
        .expect("delete the link");
    database.commit(tx).expect("commit the unlink");
    database.close().expect("release the store");
    (root, path)
}

fn history(
    path: &std::path::Path,
    kind: DirectImageMetadataKind,
) -> Result<(), DirectFileReaderError> {
    let reader = open_for_test(path, config())
        .expect("the reader opens: the tombstone is checked when history is read");
    reader
        .metadata(DirectMetadataRequest::ImageState { kind })
        .map(|response| match response.body.clone() {
            DirectMetadataBody::ImageState {
                state: DirectImageState::ChangeLog(_) | DirectImageState::Sync(_),
            } => {}
            other => panic!("history answers as history: {other:?}"),
        })
}

const KINDS: [DirectImageMetadataKind; 2] = [
    DirectImageMetadataKind::ChangeLog,
    DirectImageMetadataKind::Sync,
];

#[test]
fn a_healthy_edge_tombstone_reads_back() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    let (_root, path) = store_ending_in_an_edge_delete();
    for kind in KINDS {
        history(&path, kind).expect("a healthy tombstone's history answers");
    }
}

#[test]
fn a_tombstone_whose_commit_names_another_transaction_is_corrupt_committed_state() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    let (_root, path) = store_ending_in_an_edge_delete();
    damage_last_commit_for_edge_tombstone_for_test(
        &path,
        TombstoneCommitDamage::MismatchedTransaction,
    )
    .expect("plant the mismatched commit entry");
    for kind in KINDS {
        match history(&path, kind) {
            Err(DirectFileReaderError::CorruptStore(DirectStoreDiagnostic::MalformedRecord, _)) => {
            }
            other => panic!(
                "a commit index naming another transaction is corrupt committed state, \
                 not an untyped engine message: {other:?}"
            ),
        }
    }
}

#[test]
fn a_tombstone_whose_commit_entry_is_missing_needs_a_writer() {
    // Writers this proof opens are counted process-wide by the direct
    // reader contract, which runs under this lock.
    let _serial = crate::direct_file_reader_contract_tests::serialise_direct_file_test();
    let (_root, path) = store_ending_in_an_edge_delete();
    damage_last_commit_for_edge_tombstone_for_test(&path, TombstoneCommitDamage::MissingEntry)
        .expect("plant the missing commit entry");
    for kind in KINDS {
        match history(&path, kind) {
            Err(DirectFileReaderError::DirectReadRequiresWriter { failure, cause }) => {
                assert_eq!(failure.kind(), ReadFailureKind::DirectReadRequiresWriter);
                assert_eq!(cause, DirectRepairRequiredCause::MissingCommitIndex);
            }
            other => panic!(
                "a missing commit-index entry needs writable reconstruction, typed as such: \
                 {other:?}"
            ),
        }
    }
}
