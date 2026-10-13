//! A durable custody read validates each selected record's required companions
//! at the same captured root, as the resident journal did when it loaded. A
//! missing or tampered companion still produces the custody corruption refusal
//! after reopen; it never yields an answer or a status credit.
use crate::custody_fixture::*;
use contextdb_core::Error;
use contextdb_engine::Database;
use contextdb_server::InProcessBroker;
use std::path::Path;
use std::sync::Arc;

fn corruption(error: &Error) -> bool {
    error
        .to_string()
        .contains("invalid canonical delivery metadata")
}

async fn damaged_edge(
    broker: &InProcessBroker,
    dir: &Path,
    damage: impl FnOnce(&Database),
) -> Arc<Database> {
    let (edge, client) = bound_edge(broker, dir, None).await;
    for unit in 0..4 {
        commit_unit(&edge, unit);
    }
    let healthy = edge
        .__custody_keys_for_test(None, "delivery_root.v1.")
        .unwrap();
    assert_eq!(healthy.len(), 4, "the undamaged journal answers");
    damage(&edge);
    drop(client);
    Arc::into_inner(edge).unwrap().close().unwrap();
    Arc::new(open(&dir.join("edge.db"), None))
}

fn assert_refused(edge: &Database, case: &str) {
    for selector in ["delivery_root.v1.", "delivery_"] {
        let answer = edge.__custody_keys_for_test(None, selector);
        assert!(
            answer.as_ref().is_err_and(corruption),
            "{case}: selecting {selector} refuses as corruption: {answer:?}"
        );
    }
    let status = edge.delivery_status("records");
    assert!(
        status.as_ref().is_err_and(corruption),
        "{case}: status refuses as corruption, never counts the unit: {status:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn durable_custody_reads_refuse_missing_or_tampered_companions() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, Arc::new(Database::open_memory())).await;

    // A root whose source-history companion is missing.
    let missing = tempfile::tempdir().unwrap();
    let edge = damaged_edge(&broker, missing.path(), |edge| {
        let history = edge
            .__custody_keys_for_test(None, "delivery_source_history.v1.")
            .unwrap();
        assert_eq!(history.len(), 4);
        edge.__tamper_custody_record_for_test(&history[1], None)
            .unwrap();
    })
    .await;
    assert_eq!(
        edge.__custody_durable_key_count_for_test("delivery_source_history.v1.")
            .unwrap(),
        3,
        "the companion is really gone from the committed journal"
    );
    assert_refused(&edge, "missing source history");
    drop(edge);

    // A root whose manifest companion was altered after signing.
    let tampered = tempfile::tempdir().unwrap();
    let edge = damaged_edge(&broker, tampered.path(), |edge| {
        let manifest = edge
            .__custody_keys_for_test(None, "delivery_manifest.v1.")
            .unwrap()
            .remove(2);
        let mut bytes = edge
            .__custody_record_bytes_for_test(&manifest)
            .unwrap()
            .unwrap();
        let at = bytes
            .windows(b"records".len())
            .position(|w| w == b"records")
            .expect("the manifest names its root table");
        bytes[at] ^= 0x20;
        edge.__tamper_custody_record_for_test(&manifest, Some(bytes))
            .unwrap();
    })
    .await;
    assert_refused(&edge, "tampered manifest");
    drop(edge);
    hub.stop().await;
}
