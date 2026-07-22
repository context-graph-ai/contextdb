//! Consumer-side fetch-retention coverage.
//!
//! A fetched blob is protected from the store's GC while its transfer is in
//! flight (and between an abort and a resume) by a `fetch/` tag. This proves
//! the tag is RELEASED once the blob is fully delivered to the caller's sink —
//! without release, every blob a node ever downloaded would pin disk forever,
//! breaking the bounded-disk invariant. It guards the retention lifecycle to
//! ensure fetch tags are released and disk is not pinned.

use contextdb_engine::Database;
use contextdb_engine::work_ledger::{BlobHash, MovementPolicy, install_work_ledger_schema};
use contextdb_server::blob_resolver::BlobService;
use contextdb_server::transport::iroh::IrohServer;
use std::sync::Arc;

#[path = "media_support/mod.rs"]
mod media_support;
use media_support::*;

#[tokio::test]
async fn a_fully_delivered_fetch_releases_its_protection_tag() {
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let consumer_dir = tempfile::tempdir().expect("consumer dir");
    let holder_key = identity_file(&holder_dir);
    let consumer_key = identity_file(&consumer_dir);
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);

    let mut content = b"FETCH-RETENTION-".to_vec();
    content.extend((0..300_000u32).map(|n| n as u8));
    let h = BlobHash::of(&content);

    let holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&holder_db).expect("holder schema");
    seed_entitlement(&holder_db, "job-1", &holder_node, &consumer_node, &h);
    let holder = BlobService::new(
        holder_db,
        MovementPolicy {
            auto_propagate: true,
        },
        holder_key.clone(),
    );
    holder.set_test_clock(T0);
    assert_eq!(holder.ingest_bytes(&content).expect("ingest"), h);
    let endpoint = within(IrohServer::bind(&bind_spec(&holder_key)))
        .await
        .expect("bind holder");
    holder.serve_on(&endpoint);
    let ticket = endpoint.ticket();

    let consumer_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&consumer_db).expect("consumer schema");
    seed_entitlement(&consumer_db, "job-1", &holder_node, &consumer_node, &h);
    let consumer = BlobService::new(
        consumer_db,
        MovementPolicy {
            auto_propagate: true,
        },
        consumer_key,
    );
    consumer.set_test_clock(T0);

    let mut sink: Vec<u8> = Vec::new();
    within(consumer.resolve_blob_ref(&h, &ticket, &mut sink))
        .await
        .expect("entitled resolve must succeed");
    assert_eq!(sink, content, "the consumer received the full blob");

    // The load-bearing assertion: the fetch delivered the whole blob, so the
    // consumer's fetch-protection tag must be gone — otherwise every download
    // would pin disk forever.
    assert_eq!(
        consumer.fetch_tag_count_for_test(),
        0,
        "a fully-delivered fetch must release its protection tag; a lingering tag is a permanent per-fetch disk leak"
    );
}
