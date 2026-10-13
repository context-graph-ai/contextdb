//! Commit-time custody validation admits every key, decoded body, list and
//! verification workspace before allocating it, keeps the validated delta
//! charged until publication, and leaves nothing admitted after either a
//! refusal or a published commit. A refused commit publishes nothing.
use crate::custody_fixture::*;
use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use contextdb_engine::database::DeliveryManifest;
use contextdb_server::InProcessBroker;
use std::collections::HashMap;
use std::sync::Arc;

fn try_unit(db: &Database, unit: usize) -> contextdb_core::Result<()> {
    let tx = db.begin()?;
    db.insert_row(
        tx,
        "records",
        HashMap::from([
            ("id".into(), Value::Uuid(root_id(unit))),
            ("body".into(), Value::Text(format!("unit {unit}"))),
        ]),
    )?;
    db.register_delivery_manifest(
        tx,
        DeliveryManifest {
            root_table: "records",
            root_key: key(root_id(unit)),
            members: Vec::new(),
        },
    )?;
    db.commit(tx)
}
fn roots(db: &Database) -> u64 {
    db.__custody_durable_key_count_for_test("delivery_root.v1.")
        .unwrap()
}
fn rows(db: &Database) -> usize {
    db.execute("SELECT id FROM records", &p()).unwrap().rows.len()
}

#[tokio::test(flavor = "current_thread")]
async fn custody_commit_validation_admits_before_it_allocates() {
    let dir = tempfile::tempdir().unwrap();
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, Arc::new(Database::open_memory())).await;
    let (edge, client) = bound_edge(&broker, dir.path(), None).await;
    commit_unit(&edge, 0);
    let (roots_before, rows_before) = (roots(&edge), rows(&edge));

    // A zero budget refuses at the first admission: nothing was decoded.
    Database::__limit_custody_validation_for_test(Some(0));
    let decoded_before = edge.__custody_writer_work_for_test().1;
    let refused = try_unit(&edge, 1);
    assert!(
        matches!(refused, Err(Error::MemoryBudgetExceeded { .. })),
        "{refused:?}"
    );
    assert_eq!(
        edge.__custody_writer_work_for_test().1,
        decoded_before,
        "the refusal came before any custody record was decoded"
    );
    assert_eq!(
        Database::__last_custody_validation_for_test().0,
        Some(0),
        "the refused validation holds nothing"
    );
    assert_eq!((roots(&edge), rows(&edge)), (roots_before, rows_before));

    // A budget that runs out part way through still releases everything.
    Database::__limit_custody_validation_for_test(Some(4096));
    let refused = try_unit(&edge, 2);
    assert!(
        matches!(refused, Err(Error::MemoryBudgetExceeded { .. })),
        "{refused:?}"
    );
    assert_eq!(Database::__last_custody_validation_for_test().0, Some(0));
    assert_eq!((roots(&edge), rows(&edge)), (roots_before, rows_before));

    // Under an ample limit the delta stays admitted until publication, and the
    // published commit leaves nothing admitted.
    Database::__limit_custody_validation_for_test(Some(64 * 1024 * 1024));
    try_unit(&edge, 3).unwrap();
    let (held, retained) = Database::__last_custody_validation_for_test();
    assert!(retained > 0, "the validated delta was charged until publication");
    assert_eq!(held, Some(0), "publication released the delta's admission");
    assert_eq!(roots(&edge), roots_before + 1);
    Database::__limit_custody_validation_for_test(None);

    // The ordinary writer-accountant path also returns to zero.
    try_unit(&edge, 4).unwrap();
    assert_eq!(Database::__last_custody_validation_for_test().0, Some(0));
    assert_eq!(roots(&edge), roots_before + 2);
    drop(client);
    hub.stop().await;
}
