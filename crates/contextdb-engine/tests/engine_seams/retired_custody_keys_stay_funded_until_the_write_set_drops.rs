//! An UPDATE that retires many delivery roots moves each root's ownership key
//! into its commit's write set. Every key moved there, and the write set's
//! grown delete list, stay admitted to the writer until the write set is
//! dropped: the retirement's funding covers at least every key's bytes and
//! its list slot. A refusal while the destination grows, or at a late key
//! copy, refuses the statement typed with nothing durable changed, and the
//! ample retry retires exactly every root. A transaction that discards local
//! rows and also deletes a registered root prepares its custody commit twice;
//! it commits and retires exactly that root.
use crate::custody_fixture::*;
use crate::custody_long_key_fixture::*;
use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use contextdb_server::InProcessBroker;
use std::collections::HashMap;

const ROOTS: usize = 48;

fn budget(error: &Error) -> bool {
    matches!(error, Error::MemoryBudgetExceeded { .. })
}

fn long_root_keys(edge: &Database) -> Vec<String> {
    edge.__custody_keys_for_test(None, "delivery_root.v1.")
        .unwrap()
        .into_iter()
        .filter(|key| key.len() > 4096)
        .collect()
}

fn retire_all(edge: &Database, round: usize) -> Result<(), Error> {
    edge.execute(
        &format!("UPDATE {} SET body = $body", long_table()),
        &HashMap::from([("body".into(), Value::Text(format!("retired {round}")))]),
    )
    .map(drop)
}

#[tokio::test(flavor = "current_thread")]
async fn retired_custody_keys_stay_funded_until_the_write_set_drops() {
    let broker = InProcessBroker::new();
    let hub = start_long_hub(&broker).await;
    let dir = tempfile::tempdir().unwrap();
    let (edge, client) = bound_long_edge(&broker, dir.path()).await;
    for unit in 0..ROOTS {
        commit_long_unit(&edge, unit);
    }
    let roots = long_root_keys(&edge);
    assert_eq!(roots.len(), ROOTS, "every unit owns a long root key");
    // Refused while the write set's delete list grows, then at a late key
    // copy: typed, and nothing durable changes.
    for (label, occurrence) in [
        ("custody_retire_write_set", 1),
        ("custody_retire_root_keys", ROOTS - 2),
    ] {
        Database::__custody_refuse_at_for_test(Some((label, occurrence)));
        let refused = retire_all(&edge, 0);
        let left = Database::__custody_refuse_at_for_test(None);
        assert_eq!(left, None, "{label}#{occurrence} was reached");
        assert!(
            refused.as_ref().is_err_and(budget),
            "{label}#{occurrence}: the statement refuses typed: {refused:?}"
        );
        assert_eq!(
            long_root_keys(&edge),
            roots,
            "{label}#{occurrence}: no root was retired"
        );
    }

    // A discard of one delivered root and an ordinary delete of another, in
    // one transaction: the discard drain makes custody commit preparation run
    // twice, each run with its own funding; the commit succeeds and retires
    // exactly the deleted root.
    client.push().await.unwrap();
    let discarded = root_id(ROOTS - 1);
    let deleted = root_id(ROOTS - 2);
    let mixed = edge.begin().unwrap();
    edge.execute_in_tx(
        mixed,
        &format!("DISCARD FROM {} WHERE id = $id", long_table()),
        &HashMap::from([("id".into(), Value::Uuid(discarded))]),
    )
    .unwrap();
    edge.execute_in_tx(
        mixed,
        &format!("DELETE FROM {} WHERE id = $id", long_table()),
        &HashMap::from([("id".into(), Value::Uuid(deleted))]),
    )
    .unwrap();
    let committed = edge.commit(mixed);
    assert!(
        committed.is_ok(),
        "a discard with an ordinary root delete commits: {committed:?}"
    );
    let remaining = long_root_keys(&edge);
    assert!(
        remaining.len() < ROOTS && remaining.iter().all(|key| roots.contains(key)),
        "the deleted root is retired and no other key appears ({} remain)",
        remaining.len()
    );
    assert!(
        remaining.len() >= ROOTS - 2,
        "only the deleted and the discarded roots leave ({} remain)",
        remaining.len()
    );
    let roots = remaining;
    let key_bytes: usize = roots.iter().map(String::len).sum();

    // The ample statement retires every root, its keys funded throughout.
    retire_all(&edge, 1).unwrap();
    let funded = Database::__last_retired_custody_funding_for_test();
    let slots = roots.len() * std::mem::size_of::<String>();
    assert!(
        funded >= key_bytes + slots,
        "the write set's {} retired keys ({key_bytes} bytes) and their slots \
         ({slots} bytes) were funded by only {funded} bytes",
        roots.len()
    );
    assert!(
        long_root_keys(&edge).is_empty(),
        "every long root is retired exactly once"
    );
    println!(
        "S3_RETIRE roots={} key_bytes={key_bytes} funded={funded}",
        roots.len()
    );

    drop(client);
    drop(edge);
    hub.stop().await;
}
