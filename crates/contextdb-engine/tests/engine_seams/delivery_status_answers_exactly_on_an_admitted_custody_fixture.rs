//! On a fixture the reader limit admits, `delivery_status` read through the
//! durable custody journal answers exact counts: delivered units are accepted,
//! new units are pending, and a retired root is no longer eligible.
use crate::custody_fixture::*;
use contextdb_engine::Database;
use contextdb_server::InProcessBroker;
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread")]
async fn delivery_status_answers_exactly_on_an_admitted_custody_fixture() {
    let dir = tempfile::tempdir().unwrap();
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, Arc::new(Database::open_memory())).await;
    let (edge, client) = bound_edge(&broker, dir.path(), None).await;
    for unit in 0..12 {
        commit_unit(&edge, unit);
    }
    client.push().await.unwrap();
    for unit in 12..15 {
        commit_unit(&edge, unit);
    }
    let status = edge.delivery_status("records").unwrap();
    assert_eq!(
        (status.eligible, status.accepted, status.pending, status.refused),
        (15, 12, 3, 0)
    );
    update_root(&edge, 4);
    let status = edge.delivery_status("records").unwrap();
    assert_eq!(
        (status.eligible, status.accepted, status.pending),
        (14, 11, 3),
        "a retired root is no longer eligible"
    );
    let before = edge.__custody_writer_work_for_test();
    let again = edge.delivery_status("records").unwrap();
    assert_eq!((again.eligible, again.accepted, again.pending), (14, 11, 3));
    let after = edge.__custody_writer_work_for_test();
    assert_eq!(after.0, before.0, "status hydrates nothing");
    assert_eq!((after.2, after.3), (0, 0), "nothing is resident");
    // Reopen answers the same counts from disk.
    drop(client);
    Arc::into_inner(edge).unwrap().close().unwrap();
    let edge = open(&dir.path().join("edge.db"), None);
    let status = edge.delivery_status("records").unwrap();
    assert_eq!((status.eligible, status.accepted, status.pending), (14, 11, 3));
    hub.stop().await;
}
