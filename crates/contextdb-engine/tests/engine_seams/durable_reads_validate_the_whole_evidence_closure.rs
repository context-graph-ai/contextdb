//! A durable custody read validates the whole evidence closure of what it
//! answers at its captured root, not only each answered record's direct
//! companions. Selecting one delivered edge terminal alone, after a reopen,
//! refuses as corruption when the certificate behind its object admission is
//! deleted or altered, or when the admission itself is altered canonically.
use crate::custody_fixture::*;
use contextdb_core::Error;
use contextdb_engine::Database;
use contextdb_server::InProcessBroker;
use std::path::Path;
use std::sync::Arc;

const CORRUPTION: &str = "invalid canonical delivery metadata";

fn corruption(error: &Error) -> bool {
    error.to_string().contains(CORRUPTION)
}

/// A file-backed edge whose four units were delivered, damaged by `damage`
/// and reopened. Answers the reopened edge and the first delivered terminal.
async fn delivered_then_damaged(
    broker: &InProcessBroker,
    dir: &Path,
    damage: impl FnOnce(&Database),
) -> (Arc<Database>, String) {
    let (edge, client) = bound_edge(broker, dir, None).await;
    for unit in 0..4 {
        commit_unit(&edge, unit);
    }
    client.push().await.unwrap();
    let terminals = edge
        .__custody_keys_for_test(None, "delivery_edge_outcome.v1.")
        .unwrap();
    assert_eq!(terminals.len(), 4, "every unit was delivered");
    let terminal = terminals[0].clone();
    assert_eq!(
        edge.__custody_keys_for_test(None, &terminal).unwrap(),
        vec![terminal.clone()],
        "the healthy terminal is answered alone"
    );
    damage(&edge);
    drop(client);
    Arc::into_inner(edge).unwrap().close().unwrap();
    (Arc::new(open(&dir.join("edge.db"), None)), terminal)
}

fn all_keys(edge: &Database, prefix: &str) -> Vec<String> {
    let keys = edge.__custody_keys_for_test(None, prefix).unwrap();
    assert!(!keys.is_empty(), "the fixture holds {prefix} records");
    keys
}

#[tokio::test(flavor = "multi_thread")]
async fn durable_reads_validate_the_whole_evidence_closure() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, Arc::new(Database::open_memory())).await;

    let cases: Vec<(&str, Box<dyn FnOnce(&Database)>)> = vec![
        (
            "certificate deleted",
            Box::new(|edge: &Database| {
                for certificate in all_keys(edge, "delivery_peer_evidence.v1.") {
                    edge.__tamper_custody_record_for_test(&certificate, None)
                        .unwrap();
                }
            }),
        ),
        (
            "certificate signature altered",
            Box::new(|edge: &Database| {
                for certificate in all_keys(edge, "delivery_peer_evidence.v1.") {
                    let mut bytes = edge
                        .__custody_record_bytes_for_test(&certificate)
                        .unwrap()
                        .unwrap();
                    let last = bytes.len() - 1;
                    bytes[last] ^= 0x01;
                    edge.__tamper_custody_record_for_test(&certificate, Some(bytes))
                        .unwrap();
                }
            }),
        ),
        (
            "admission owning state altered",
            Box::new(|edge: &Database| {
                for admission in all_keys(edge, "delivery_object_admission.v1.") {
                    let bytes = edge
                        .__custody_mutated_record_for_test(&admission, "admission_owning_state")
                        .unwrap();
                    edge.__tamper_custody_record_for_test(&admission, Some(bytes))
                        .unwrap();
                }
            }),
        ),
    ];
    for (case, damage) in cases {
        let dir = tempfile::tempdir().unwrap();
        let (edge, terminal) = delivered_then_damaged(&broker, dir.path(), damage).await;
        let answer = edge.__custody_keys_for_test(None, &terminal);
        assert!(
            answer.as_ref().is_err_and(corruption),
            "{case}: the terminal alone refuses as corruption after reopen: {answer:?}"
        );
        drop(edge);
    }
    hub.stop().await;
}
