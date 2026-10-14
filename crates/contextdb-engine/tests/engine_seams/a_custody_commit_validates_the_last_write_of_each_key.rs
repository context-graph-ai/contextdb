//! A commit's custody validation checks the same final overlay persistence
//! stores: its deletes, then its writes in order, the last write of a key
//! winning. Writing a delivered object admission back after deleting it, or
//! after an earlier tampered write of it, commits and stores the valid body;
//! a tampered body written last refuses as corruption with nothing committed,
//! whatever was written before it.
use crate::custody_fixture::*;
use contextdb_core::Error;
use contextdb_engine::Database;
use contextdb_server::InProcessBroker;
use std::sync::Arc;

const CORRUPTION: &str = "invalid canonical delivery metadata";

fn corruption(result: &Result<(), Error>) -> bool {
    result
        .as_ref()
        .is_err_and(|error| error.to_string().contains(CORRUPTION))
}

/// The admission still stored as `good`, and its delivered terminal's whole
/// evidence closure still validating.
fn unchanged(edge: &Database, admission: &str, terminal: &str, good: &[u8], case: &str) {
    assert_eq!(
        edge.__custody_record_bytes_for_test(admission)
            .unwrap()
            .as_deref(),
        Some(good),
        "{case}: the stored admission is the valid body"
    );
    assert_eq!(
        edge.__custody_keys_for_test(None, terminal).unwrap(),
        vec![terminal.to_owned()],
        "{case}: the terminal's closure validates"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_custody_commit_validates_the_last_write_of_each_key() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, Arc::new(Database::open_memory())).await;
    let dir = tempfile::tempdir().unwrap();
    let (edge, client) = bound_edge(&broker, dir.path(), None).await;
    for unit in 0..2 {
        commit_unit(&edge, unit);
    }
    client.push().await.unwrap();
    let admission = edge
        .__custody_keys_for_test(None, "delivery_object_admission.v1.")
        .unwrap()[0]
        .clone();
    let terminal = edge
        .__custody_keys_for_test(None, "delivery_edge_outcome.v1.")
        .unwrap()[0]
        .clone();
    let good = edge
        .__custody_record_bytes_for_test(&admission)
        .unwrap()
        .unwrap();
    let bad = edge
        .__custody_mutated_record_for_test(&admission, "admission_owning_state")
        .unwrap();
    assert_ne!(good, bad);
    let key = admission.as_str();

    // The valid body written last commits, whatever came before it.
    let rewritten = edge.__commit_custody_records_for_test(&[key], &[(key, good.clone())]);
    assert!(
        rewritten.is_ok(),
        "delete then write of the valid body commits: {rewritten:?}"
    );
    unchanged(&edge, key, &terminal, &good, "delete+write");
    let replaced =
        edge.__commit_custody_records_for_test(&[], &[(key, bad.clone()), (key, good.clone())]);
    assert!(
        replaced.is_ok(),
        "a tampered write replaced by the valid body commits: {replaced:?}"
    );
    unchanged(&edge, key, &terminal, &good, "tamper then valid");

    // A tampered body written last refuses, nothing committed.
    let tampered =
        edge.__commit_custody_records_for_test(&[], &[(key, good.clone()), (key, bad.clone())]);
    assert!(
        corruption(&tampered),
        "valid then tampered refuses: {tampered:?}"
    );
    unchanged(&edge, key, &terminal, &good, "valid then tamper");
    let deleted_tampered = edge.__commit_custody_records_for_test(&[key], &[(key, bad.clone())]);
    assert!(
        corruption(&deleted_tampered),
        "delete then tampered refuses: {deleted_tampered:?}"
    );
    unchanged(&edge, key, &terminal, &good, "delete+tamper");
    let twice_tampered = edge.__commit_custody_records_for_test(
        &[],
        &[(key, bad.clone()), (key, good.clone()), (key, bad.clone())],
    );
    assert!(
        corruption(&twice_tampered),
        "tampered, valid, tampered refuses: {twice_tampered:?}"
    );
    unchanged(&edge, key, &terminal, &good, "tamper, valid, tamper");

    drop(client);
    drop(edge);
    hub.stop().await;
}
