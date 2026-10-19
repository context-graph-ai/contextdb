//! A file-backed store whose custody group index lost its completion marker
//! refuses custody work with a typed error. It never loads the delivery journal
//! whole in its place, and a refused commit publishes nothing. The repair
//! rebuild makes the index complete again.
use crate::custody_fixture::*;
use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use contextdb_server::InProcessBroker;
use std::sync::Arc;

const BUDGET: usize = 16 * 1024 * 1024;

fn incomplete(error: &Error) -> bool {
    matches!(error, Error::StoreCorrupted { path, .. } if path == "read_custody_groups")
}
fn bodies(db: &Database) -> Vec<Vec<Value>> {
    db.execute("SELECT id, body FROM records ORDER BY id", &p())
        .unwrap()
        .rows
}

#[tokio::test(flavor = "multi_thread")]
async fn an_incomplete_custody_index_refuses_without_loading_the_journal() {
    let dir = tempfile::tempdir().unwrap();
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, Arc::new(Database::open_memory())).await;
    let (edge, client) = bound_edge(&broker, dir.path(), Some(BUDGET)).await;
    for unit in 0..5 {
        commit_unit(&edge, unit);
    }
    edge.__invalidate_custody_groups_for_test().unwrap();
    assert!(!edge.__custody_groups_complete_for_test().unwrap());
    drop(client);
    Arc::into_inner(edge).unwrap().close().unwrap();

    // A fresh process view under a small fixed writer budget.
    let edge = Arc::new(open(&dir.path().join("edge.db"), Some(BUDGET)));
    let client = edge_client(&edge, &broker, &dir.path().join("edge.key"));
    let rows_before = bodies(&edge);
    assert_eq!(rows_before.len(), 5);
    let work_before = edge.__custody_writer_work_for_test();

    let status = edge.delivery_status("records");
    assert!(
        status.as_ref().is_err_and(incomplete),
        "status refuses with the typed incomplete-index error: {status:?}"
    );
    // A registration and a retiring UPDATE are refused whole.
    let tx = edge.begin().unwrap();
    edge.insert_row(
        tx,
        "records",
        std::collections::HashMap::from([
            ("id".into(), Value::Uuid(root_id(5))),
            ("body".into(), Value::Text("unit 5".into())),
        ]),
    )
    .unwrap();
    let registered = edge
        .register_delivery_manifest(
            tx,
            contextdb_engine::database::DeliveryManifest {
                root_table: "records",
                root_key: key(root_id(5)),
                members: Vec::new(),
            },
        )
        .and_then(|()| edge.commit(tx));
    assert!(
        registered.as_ref().is_err_and(incomplete),
        "registration refuses: {registered:?}"
    );
    let updated = edge.execute(
        "UPDATE records SET body = 'changed' WHERE id = $id",
        &std::collections::HashMap::from([("id".into(), Value::Uuid(root_id(1)))]),
    );
    assert!(
        updated.as_ref().is_err_and(incomplete),
        "a retiring update refuses: {updated:?}"
    );
    assert_eq!(
        bodies(&edge),
        rows_before,
        "no refused commit published a row"
    );
    let work_after = edge.__custody_writer_work_for_test();
    assert_eq!(
        work_after.0, work_before.0,
        "no delivery record was hydrated"
    );
    assert_eq!((work_after.2, work_after.3), (0, 0), "nothing is resident");
    assert_eq!(
        edge.__custody_durable_key_count_for_test("delivery_root.v1.")
            .unwrap(),
        5
    );

    // The repair rebuild makes the index complete; custody works again.
    edge.__rebuild_custody_index_for_test().unwrap();
    assert!(edge.__custody_groups_complete_for_test().unwrap());
    drop(client);
    Arc::into_inner(edge).unwrap().close().unwrap();
    let edge = Arc::new(open(&dir.path().join("edge.db"), Some(BUDGET)));
    let client = edge_client(&edge, &broker, &dir.path().join("edge.key"));
    let status = edge.delivery_status("records").unwrap();
    assert_eq!((status.eligible, status.pending), (5, 5));
    update_root(&edge, 1);
    assert_eq!(
        edge.__custody_durable_key_count_for_test("delivery_root.v1.")
            .unwrap(),
        4,
        "the update retired exactly one ownership"
    );
    let parity = edge
        .__custody_selection_parity_for_test(
            "records",
            &(0..5).map(|unit| key(root_id(unit))).collect::<Vec<_>>(),
        )
        .unwrap()
        .expect("the rebuilt index is complete");
    for (selector, resident, durable) in parity {
        assert_eq!(
            resident, durable,
            "rebuilt index answers {selector} exactly"
        );
    }
    drop(client);
    hub.stop().await;
}
