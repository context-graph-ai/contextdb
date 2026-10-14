//! A durable custody read remembers a record as checked only once that
//! record's whole evidence closure has validated at the captured root. After a
//! reopen, on one unchanged root, a delivered terminal whose object admission
//! or certificate is damaged refuses as corruption on every read: the first
//! read that reaches the damage, every repeated terminal-only read after it,
//! and every retry after a reader-budget refusal part way through the
//! closure. Nothing a failed or refused closure touched is ever skipped later.
use crate::custody_fixture::*;
use contextdb_core::Error;
use contextdb_engine::Database;
use contextdb_server::InProcessBroker;
use std::path::Path;
use std::sync::Arc;

const CORRUPTION: &str = "invalid canonical delivery metadata";
const AMPLE: usize = 256 * 1024 * 1024;

fn corruption(error: &Error) -> bool {
    error.to_string().contains(CORRUPTION)
}
fn budget(error: &Error) -> bool {
    matches!(error, Error::MemoryBudgetExceeded { .. })
}

/// Four delivered units, `damage` applied, then a fresh process view.
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
    let terminal = edge
        .__custody_keys_for_test(None, "delivery_edge_outcome.v1.")
        .unwrap()[0]
        .clone();
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

/// Every read of `terminal` at this root, repeated and after mid-closure
/// budget refusals, refuses as corruption.
fn always_refused(edge: &Database, terminal: &str, case: &str) {
    // An independent read at the damaged root warms the store's memo.
    let warm = edge.__custody_keys_for_test(None, "delivery_root.v1.");
    assert!(
        warm.as_ref().is_err_and(corruption),
        "{case}: the warming read reaches the damage: {warm:?}"
    );
    for attempt in 0..3 {
        let answer = edge.__custody_keys_for_test(None, terminal);
        assert!(
            answer.as_ref().is_err_and(corruption),
            "{case}: terminal-only read {attempt} refuses as corruption: {answer:?}"
        );
    }
    let mut refused_by_budget = 0;
    for step in 1..=48usize {
        let limit = step * 4096;
        match edge.__custody_keys_with_limit_for_test(terminal, limit) {
            Err(error) if budget(&error) => refused_by_budget += 1,
            Err(error) if corruption(&error) => {}
            other => panic!("{case}: limit {limit} answered a damaged terminal: {other:?}"),
        }
        let retry = edge.__custody_keys_with_limit_for_test(terminal, AMPLE);
        assert!(
            retry.as_ref().is_err_and(corruption),
            "{case}: the retry after limit {limit} still refuses as corruption: {retry:?}"
        );
    }
    assert!(
        refused_by_budget > 0,
        "{case}: some limit refuses part way through the closure"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_partial_closure_never_certifies_custody_evidence() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, Arc::new(Database::open_memory())).await;

    // The healthy control: the same reads answer exactly, with and without
    // a mid-closure refusal before them.
    let dir = tempfile::tempdir().unwrap();
    let (edge, terminal) = delivered_then_damaged(&broker, dir.path(), |_| {}).await;
    assert_eq!(
        edge.__custody_keys_for_test(None, "delivery_root.v1.")
            .unwrap()
            .len(),
        4
    );
    for step in 1..=48usize {
        let _ = edge.__custody_keys_with_limit_for_test(&terminal, step * 4096);
        assert_eq!(
            edge.__custody_keys_with_limit_for_test(&terminal, AMPLE)
                .unwrap(),
            vec![terminal.clone()],
            "the healthy terminal answers exactly after limit {}",
            step * 4096
        );
    }
    drop(edge);

    let cases: Vec<(&str, Box<dyn FnOnce(&Database)>)> = vec![
        (
            "certificate deleted",
            Box::new(|edge: &Database| {
                for key in all_keys(edge, "delivery_peer_evidence.v1.") {
                    edge.__tamper_custody_record_for_test(&key, None).unwrap();
                }
            }),
        ),
        (
            "admission deleted",
            Box::new(|edge: &Database| {
                for key in all_keys(edge, "delivery_object_admission.v1.") {
                    edge.__tamper_custody_record_for_test(&key, None).unwrap();
                }
            }),
        ),
        (
            "admission owning state altered",
            Box::new(|edge: &Database| {
                for key in all_keys(edge, "delivery_object_admission.v1.") {
                    let bytes = edge
                        .__custody_mutated_record_for_test(&key, "admission_owning_state")
                        .unwrap();
                    edge.__tamper_custody_record_for_test(&key, Some(bytes))
                        .unwrap();
                }
            }),
        ),
        (
            "admission certificate pointer altered",
            Box::new(|edge: &Database| {
                for key in all_keys(edge, "delivery_object_admission.v1.") {
                    let bytes = edge
                        .__custody_mutated_record_for_test(&key, "admission_certificate")
                        .unwrap();
                    edge.__tamper_custody_record_for_test(&key, Some(bytes))
                        .unwrap();
                }
            }),
        ),
    ];
    for (case, damage) in cases {
        let dir = tempfile::tempdir().unwrap();
        let (edge, terminal) = delivered_then_damaged(&broker, dir.path(), damage).await;
        always_refused(&edge, &terminal, case);
        drop(edge);
    }
    hub.stop().await;
}
