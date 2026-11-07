//! A malformed stored custody record refuses with the same custody codec
//! diagnostic whether it is read durably or loaded into a resident journal,
//! and a memory refusal raised while decoding stays a memory refusal.
use crate::custody_fixture::*;
use contextdb_core::Error;
use contextdb_engine::Database;
use contextdb_server::InProcessBroker;
use std::sync::Arc;

fn text(error: &Option<Error>) -> String {
    error.as_ref().map(ToString::to_string).unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_custody_record_refuses_with_the_custody_codec_diagnostic() {
    let dir = tempfile::tempdir().unwrap();
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, Arc::new(Database::open_memory())).await;
    let (edge, client) = bound_edge(&broker, dir.path(), None).await;
    commit_unit(&edge, 0);
    let key = edge
        .__custody_keys_for_test(None, "delivery_manifest.v1.")
        .unwrap()
        .pop()
        .unwrap();
    let good = edge.__custody_record_bytes_for_test(&key).unwrap().unwrap();

    // A healthy record decodes on both paths.
    let (durable, resident) = Database::__custody_decode_refusals_for_test(&key, &good, None);
    assert!(
        durable.is_none() && resident.is_none(),
        "{durable:?} {resident:?}"
    );

    let mut trailing = good.clone();
    trailing.push(0xc0);
    let malformed = [
        ("truncated", good[..good.len() / 2].to_vec()),
        ("trailing", trailing),
        ("garbage", vec![0xc1, 0xff, 0x00, 0x13]),
        ("empty", Vec::new()),
    ];
    for (shape, bytes) in malformed {
        let (durable, resident) = Database::__custody_decode_refusals_for_test(&key, &bytes, None);
        assert!(resident.is_some(), "{shape}: the resident load refuses");
        assert_eq!(
            text(&durable),
            text(&resident),
            "{shape}: the durable path keeps the custody codec diagnostic"
        );
        assert!(
            !text(&durable).contains("MessagePack decode error"),
            "{shape}: {}",
            text(&durable)
        );
    }

    // A refusal by the decode's own memory owner passes through unchanged.
    let (durable, _) = Database::__custody_decode_refusals_for_test(
        &key,
        &good,
        Some(good.len() + key.len() + 64),
    );
    assert!(
        matches!(durable, Some(Error::MemoryBudgetExceeded { .. })),
        "a decode-time admission refusal is not rewritten: {durable:?}"
    );
    drop(client);
    hub.stop().await;
}
