//! An UPDATE that retires many delivery roots moves each root's ownership key
//! into its commit's write set. Every key moved there, and the write set's
//! grown delete list, stay admitted to the writer until the write set is
//! dropped: the retirement's funding covers at least every key's bytes and
//! its list slot. A refusal while the destination grows, or at a late key
//! copy, refuses the statement typed with nothing durable changed, and the
//! ample retry retires exactly every root.
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
    let key_bytes: usize = roots.iter().map(String::len).sum();

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

    // The ample statement retires every root, its keys funded throughout.
    retire_all(&edge, 1).unwrap();
    let funded = Database::__last_retired_custody_funding_for_test();
    let slots = ROOTS * std::mem::size_of::<String>();
    assert!(
        funded >= key_bytes + slots,
        "the write set's {ROOTS} retired keys ({key_bytes} bytes) and their slots \
         ({slots} bytes) were funded by only {funded} bytes"
    );
    assert!(
        long_root_keys(&edge).is_empty(),
        "every long root is retired exactly once"
    );
    println!("S3_RETIRE roots={ROOTS} key_bytes={key_bytes} funded={funded}");

    drop(client);
    drop(edge);
    hub.stop().await;
}
