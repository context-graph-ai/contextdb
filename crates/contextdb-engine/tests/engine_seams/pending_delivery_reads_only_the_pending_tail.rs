//! Pending delivery is read from a durable pending index through a bounded,
//! resumable cursor: the work is the pending tail, not the delivered history.
//! A root leaves pending only through a committed, validated edge terminal; a
//! corrupt or mismatched outcome is refused at commit and the root is still
//! offered on the next push. A destination change and a reopen keep the set.
use crate::custody_fixture::*;
use contextdb_engine::Database;
use contextdb_server::InProcessBroker;
use std::sync::Arc;

const TAIL: usize = 3;

fn submission_of(manifest_key: &str) -> &str {
    manifest_key.rsplit('.').next().unwrap()
}

/// `(examined entries for the pending tail, examined after a destination change)`.
async fn arm(delivered: usize) -> (u64, u64) {
    let dir = tempfile::tempdir().unwrap();
    let broker = InProcessBroker::new();
    let first = start_hub(&broker, Arc::new(Database::open_memory())).await;
    let (edge, client) = bound_edge(&broker, dir.path(), None).await;
    for unit in 0..delivered {
        commit_unit(&edge, unit);
    }
    client.push().await.unwrap();
    let (keys, _) = edge.__custody_pending_for_test().unwrap();
    assert!(keys.is_empty(), "every registered unit was delivered: {keys:?}");

    for unit in delivered..delivered + TAIL {
        commit_unit(&edge, unit);
    }
    let (tail, examined) = edge.__custody_pending_for_test().unwrap();
    assert_eq!(tail.len(), TAIL);
    assert_eq!(
        examined, TAIL as u64,
        "the pending read examined only the pending tail, at {delivered} delivered roots"
    );

    // A corrupt or mismatched outcome for a pending root is refused at commit.
    let outcomes = edge
        .__custody_keys_for_test(None, "delivery_edge_outcome.v1.")
        .unwrap();
    assert_eq!(outcomes.len(), delivered);
    let delivered_outcome = &outcomes[0];
    let delivered_bytes = edge
        .__custody_record_bytes_for_test(delivered_outcome)
        .unwrap()
        .unwrap();
    let namespace_prefix = &delivered_outcome[..delivered_outcome.rfind('.').unwrap()];
    let pending_outcome = format!("{namespace_prefix}.{}", submission_of(&tail[0]));
    for (shape, bytes) in [
        ("another unit's outcome", delivered_bytes.clone()),
        ("garbage", vec![0x93, 0x01, 0x02, 0x03]),
    ] {
        let refused = edge.__commit_custody_record_for_test(&pending_outcome, Some(bytes));
        assert!(refused.is_err(), "{shape}: refused at commit");
    }
    let (still, _) = edge.__custody_pending_for_test().unwrap();
    assert_eq!(still, tail, "a refused outcome excluded no root");
    let mut tampered = delivered_bytes.clone();
    let last = tampered.len() - 1;
    tampered[last] ^= 0x01;
    assert!(
        edge.__commit_custody_record_for_test(delivered_outcome, Some(tampered))
            .is_err(),
        "a re-signed-invalid outcome is refused at commit"
    );

    // The next push offers exactly the tail; the hub's outcomes clear it.
    client.push().await.unwrap();
    let (cleared, _) = edge.__custody_pending_for_test().unwrap();
    assert!(cleared.is_empty(), "the pushed tail was delivered: {cleared:?}");

    // A destination change makes every current root pending again.
    first.stop().await;
    let second = start_hub(&broker, Arc::new(Database::open_memory())).await;
    client.change_destination(&second.node).unwrap();
    client
        .bind_application_table_policy(expectation())
        .await
        .unwrap();
    let (moved, moved_examined) = edge.__custody_pending_for_test().unwrap();
    assert_eq!(moved.len(), delivered + TAIL);

    // Reopen keeps the same pending set.
    drop(client);
    Arc::into_inner(edge).unwrap().close().unwrap();
    let edge = Arc::new(open(&dir.path().join("edge.db"), None));
    let client = edge_client(&edge, &broker, &dir.path().join("edge.key"));
    let (reopened, _) = edge.__custody_pending_for_test().unwrap();
    assert_eq!(reopened, moved);
    let parity = edge
        .__custody_selection_parity_for_test(
            "records",
            &(0..delivered + TAIL)
                .map(|unit| key(root_id(unit)))
                .collect::<Vec<_>>(),
        )
        .unwrap()
        .unwrap();
    let (_, resident, durable) = parity
        .iter()
        .find(|(selector, _, _)| selector == "pending")
        .unwrap();
    assert_eq!(resident, durable, "the pending index agrees with the resident journal");

    // Delivery to the new destination clears it.
    client.push().await.unwrap();
    let (after, _) = edge.__custody_pending_for_test().unwrap();
    assert!(after.is_empty(), "{after:?}");
    drop(client);
    second.stop().await;
    (examined, moved_examined)
}

#[tokio::test(flavor = "multi_thread")]
async fn pending_delivery_reads_only_the_pending_tail() {
    let (small, small_moved) = arm(8).await;
    let (large, large_moved) = arm(16).await;
    assert_eq!(
        small, large,
        "doubling delivered history leaves the pending read's work unchanged"
    );
    assert_eq!((small_moved, large_moved), (8 + TAIL as u64, 16 + TAIL as u64));
}
