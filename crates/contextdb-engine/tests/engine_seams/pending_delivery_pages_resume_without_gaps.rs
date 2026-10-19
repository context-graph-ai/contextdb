//! The pending-delivery read walks the durable pending index in pages of 64
//! entries and resumes each page strictly after the last entry of the one
//! before. At 0, 64, 65, 128 and 129 pending roots it answers every pending
//! manifest exactly once, in submission order, having examined exactly the
//! pending entries; a reopen and a destination change keep that answer. The
//! visit is bounded per page; the answer it materializes is admitted to the
//! reader, so a limit that runs out on a later page refuses and leaves
//! nothing admitted.
use crate::custody_fixture::*;
use contextdb_core::Error;
use contextdb_engine::Database;
use contextdb_server::InProcessBroker;
use std::sync::Arc;

fn exactly(edge: &Database, expected: &[String], case: &str) {
    let (keys, examined) = edge.__custody_pending_for_test().unwrap();
    assert_eq!(
        keys, expected,
        "{case}: every pending manifest once, in order"
    );
    assert_eq!(
        examined,
        expected.len() as u64,
        "{case}: examined exactly the pending entries"
    );
}

async fn arm(broker: &InProcessBroker, second: &Hub, units: usize) {
    let dir = tempfile::tempdir().unwrap();
    let (edge, client) = bound_edge(broker, dir.path(), None).await;
    for unit in 0..units {
        commit_unit(&edge, unit);
    }
    // Manifest keys sort by submission, the pending index's order.
    let expected = edge
        .__custody_keys_for_test(None, "delivery_manifest.v1.")
        .unwrap();
    assert_eq!(expected.len(), units);
    exactly(&edge, &expected, &format!("{units} pending"));

    drop(client);
    Arc::into_inner(edge).unwrap().close().unwrap();
    let edge = Arc::new(open(&dir.path().join("edge.db"), None));
    let client = edge_client(&edge, broker, &dir.path().join("edge.key"));
    exactly(&edge, &expected, &format!("{units} pending, reopened"));

    client.change_destination(&second.node).unwrap();
    client
        .bind_application_table_policy(expectation())
        .await
        .unwrap();
    exactly(
        &edge,
        &expected,
        &format!("{units} pending, destination changed"),
    );

    if units > 128 {
        // A limit that runs out on a later page refuses and releases. Every
        // custody read first admits its error reserve (design A4,
        // 2 * (16 KiB + 64) bytes), so the sweep starts above it.
        const ERROR_RESERVE: usize = 2 * (16 * 1024 + 64);
        let mut later_page = 0;
        for step in 1..=128usize {
            let limit = ERROR_RESERVE + step * 256;
            let (answer, examined, held) = edge.__custody_pending_with_limit_for_test(limit);
            assert_eq!(held, 0, "limit {limit}: nothing stays admitted");
            match answer {
                Ok(keys) => assert_eq!(keys, expected),
                Err(error) => {
                    assert!(
                        matches!(error, Error::MemoryBudgetExceeded { .. }),
                        "{error:?}"
                    );
                    if examined > 64 && examined < units as u64 {
                        later_page += 1;
                    }
                }
            }
        }
        assert!(later_page > 0, "some limit refuses on a later page");
    }
    drop(client);
}

#[tokio::test(flavor = "multi_thread")]
async fn pending_delivery_pages_resume_without_gaps() {
    let broker = InProcessBroker::new();
    let first = start_hub(&broker, Arc::new(Database::open_memory())).await;
    let second = start_hub(&broker, Arc::new(Database::open_memory())).await;
    for units in [0, 64, 65, 128, 129] {
        arm(&broker, &second, units).await;
    }
    second.stop().await;
    first.stop().await;
}
