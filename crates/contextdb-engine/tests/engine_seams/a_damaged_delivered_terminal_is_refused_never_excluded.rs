//! A delivered root leaves the pending set only through an edge terminal whose
//! signature and evidence verify at the store's own committed view. A stored
//! terminal altered after commit, or one whose diagnostic or object admission
//! is gone, never silently excludes its root: the pending read and the next
//! push refuse as corruption, after a reopen, after the custody index is
//! rebuilt; after a destination change the root is pending at the new
//! destination and offering it still refuses as corruption.
use crate::custody_fixture::*;
use contextdb_core::Error;
use contextdb_engine::Database;
use contextdb_server::{InProcessBroker, SyncClient};
use std::path::Path;
use std::sync::Arc;

fn corruption(error: &Error) -> bool {
    error
        .to_string()
        .contains("invalid canonical delivery metadata")
}

fn submission_of(key: &str) -> &str {
    key.rsplit('.').next().unwrap()
}

/// The stored terminal of the first delivered unit, its signature byte flipped.
fn flip_terminal_signature(edge: &Database) {
    let terminal = &edge
        .__custody_keys_for_test(None, "delivery_edge_outcome.v1.")
        .unwrap()[0];
    let mut bytes = edge
        .__custody_record_bytes_for_test(terminal)
        .unwrap()
        .unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x01;
    edge.__tamper_custody_record_for_test(terminal, Some(bytes))
        .unwrap();
}

async fn assert_refused(edge: &Database, client: &SyncClient, case: &str) {
    let pending = edge.__custody_pending_for_test();
    assert!(
        pending.as_ref().is_err_and(corruption),
        "{case}: the pending read refuses: {pending:?}"
    );
    let pushed = client.push().await;
    assert!(
        pushed.as_ref().is_err_and(corruption),
        "{case}: the push refuses, never skips the root: {pushed:?}"
    );
}

/// Reopen, refuse, rebuild the custody index, refuse again.
async fn reopened_and_rebuilt(
    edge: Arc<Database>,
    client: SyncClient,
    broker: &InProcessBroker,
    dir: &Path,
    case: &str,
) {
    assert_refused(&edge, &client, case).await;
    drop(client);
    Arc::into_inner(edge).unwrap().close().unwrap();
    let edge = Arc::new(open(&dir.join("edge.db"), None));
    let client = edge_client(&edge, broker, &dir.join("edge.key"));
    assert_refused(&edge, &client, &format!("{case}, reopened")).await;
    edge.__rebuild_custody_index_for_test().unwrap();
    assert_refused(&edge, &client, &format!("{case}, rebuilt")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_damaged_delivered_terminal_is_refused_never_excluded() {
    let broker = InProcessBroker::new();
    let first = start_hub(&broker, Arc::new(Database::open_memory())).await;

    // Stored signature tamper, and missing diagnostic and admission evidence.
    for case in [
        "terminal signature",
        "diagnostic deleted",
        "admission deleted",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (edge, client) = bound_edge(&broker, dir.path(), None).await;
        for unit in 0..4 {
            commit_unit(&edge, unit);
        }
        client.push().await.unwrap();
        let (pending, _) = edge.__custody_pending_for_test().unwrap();
        assert!(pending.is_empty(), "{case}: every unit was delivered");
        let terminal = edge
            .__custody_keys_for_test(None, "delivery_edge_outcome.v1.")
            .unwrap()[0]
            .clone();
        match case {
            "terminal signature" => flip_terminal_signature(&edge),
            "diagnostic deleted" => {
                let diagnostic = edge
                    .__custody_keys_for_test(None, "delivery_diagnostic.v1.")
                    .unwrap()
                    .into_iter()
                    .find(|key| submission_of(key) == submission_of(&terminal))
                    .unwrap();
                edge.__tamper_custody_record_for_test(&diagnostic, None)
                    .unwrap();
            }
            _ => {
                for admission in edge
                    .__custody_keys_for_test(None, "delivery_object_admission.v1.")
                    .unwrap()
                {
                    edge.__tamper_custody_record_for_test(&admission, None)
                        .unwrap();
                }
            }
        }
        reopened_and_rebuilt(edge, client, &broker, dir.path(), case).await;
    }

    // A destination change recomputes every root against the new destination:
    // the damaged terminal names a former destination, so its root is pending
    // there and is not excluded; offering it still reads its submission's
    // evidence, which refuses as corruption rather than sending the unit on.
    // (A store cannot rebind to a former hub, so that terminal never counts
    // again.)
    let dir = tempfile::tempdir().unwrap();
    let (edge, client) = bound_edge(&broker, dir.path(), None).await;
    for unit in 0..4 {
        commit_unit(&edge, unit);
    }
    client.push().await.unwrap();
    flip_terminal_signature(&edge);
    assert_refused(&edge, &client, "damaged at the current destination").await;
    let second = start_hub(&broker, Arc::new(Database::open_memory())).await;
    client.change_destination(&second.node).unwrap();
    client
        .bind_application_table_policy(expectation())
        .await
        .unwrap();
    let (moved, _) = edge.__custody_pending_for_test().unwrap();
    assert_eq!(
        moved.len(),
        4,
        "at the new destination every root is pending, the damaged one included"
    );
    let pushed = client.push().await;
    assert!(
        pushed.as_ref().is_err_and(corruption),
        "the damaged unit is refused, not sent on: {pushed:?}"
    );
    drop(client);
    second.stop().await;
    first.stop().await;
}
