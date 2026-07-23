//! Media-transfer acceptance tests for the blob_ref resolver's observable
//! contract over real localhost Iroh endpoints with an in-process work ledger.

use contextdb_core::Wallclock;
use contextdb_engine::Database;
use contextdb_engine::work_ledger::{
    BlobHash, ClaimInsert, InputRef, JobSpec, MovementPolicy, cancel_job, insert_claim,
    install_work_ledger_schema, record_failure, submit_job,
};
use contextdb_server::blob_resolver::{BlobStore, ResolveError};
use contextdb_server::transport::iroh::IrohServer;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[path = "media_support/mod.rs"]
mod media_support;
use media_support::*;

const GRACE: i64 = 60_000;
const MB: usize = 1024 * 1024;

fn seed_mixed_entitlement(
    db: &Database,
    job_id: &str,
    submitter: &str,
    claimant: &str,
    hash: &BlobHash,
) {
    let job = JobSpec::builder(job_id, "media.demo", "batch", submitter)
        .input_refs(vec![
            InputRef::blob_ref(hash.clone()),
            InputRef::local_path("/holder/private/clip.bin"),
        ])
        .submitted_at_ms(T0)
        .build();
    submit_job(db, &job, &[] as &[&[u8]]).expect("submit mixed-input job");
    match insert_claim(db, job_id, 1, claimant, T0 + LEASE, T0).expect("insert mixed claim") {
        ClaimInsert::Inserted => {}
        other => panic!("mixed claim seed must insert, got {other:?}"),
    }
}

fn marked(marker: &str, size: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(size);
    bytes.extend_from_slice(marker.as_bytes());
    let mut n: u8 = 0;
    while bytes.len() < size {
        bytes.push(n);
        n = n.wrapping_add(1);
    }
    bytes.truncate(size);
    bytes
}

struct FailingSink {
    fail_after: u64,
    accepted: u64,
}

impl FailingSink {
    fn new(fail_after: u64) -> Self {
        Self {
            fail_after,
            accepted: 0,
        }
    }
}

impl std::io::Write for FailingSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.accepted >= self.fail_after {
            return Err(std::io::Error::other("sink is full (ENOSPC)"));
        }
        let room = (self.fail_after - self.accepted) as usize;
        let take = room.min(buf.len());
        self.accepted += take as u64;
        Ok(take)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn ingest_round_trip_matches_blake3_identity() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&db).expect("schema");
    let holder = BlobStore::new(
        db,
        MovementPolicy {
            auto_propagate: true,
        },
        identity_file(&dir),
    );

    let content = marked("INGEST-MARKER-a1b2", 40 * 1024);
    let h = holder.ingest_bytes(&content).expect("ingest bytes");
    assert_eq!(h, BlobHash::of(&content));

    let file = dir.path().join("clip.bin");
    std::fs::write(&file, &content).expect("write file");
    let hf = holder.ingest_file(&file).expect("ingest file");
    assert_eq!(hf, h);

    let h_again = holder.ingest_bytes(&content).expect("re-ingest");
    assert_eq!(h_again, h);
}

#[tokio::test]
async fn entitled_resolve_streams_bit_identical_bytes() {
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let consumer_dir = tempfile::tempdir().expect("consumer dir");
    let holder_key = identity_file(&holder_dir);
    let consumer_key = identity_file(&consumer_dir);
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);
    let content = marked("BLOB-MARKER-2c2c", 512 * 1024);
    let h = BlobHash::of(&content);

    let holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&holder_db).expect("holder schema");
    seed_entitlement(&holder_db, "job-1", &holder_node, &consumer_node, &h);
    let holder = BlobStore::new(
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
    let consumer = BlobStore::new(
        consumer_db,
        MovementPolicy {
            auto_propagate: true,
        },
        consumer_key,
    );
    consumer.set_test_clock(T0);

    let mut sink: Vec<u8> = Vec::new();
    let written = within(consumer.resolve_blob_ref(&h, &ticket, &mut sink))
        .await
        .expect("entitled resolve must succeed");
    assert_eq!(written as usize, content.len());
    assert_eq!(BlobHash::of(&sink), h);
    assert_eq!(sink, content);
    assert_eq!(
        holder.payload_bytes_emitted_for_test(),
        content.len() as u64,
        "the provider payload counter must be load-bearing on an authorized transfer"
    );
    assert_eq!(holder.fetch_requests_received_for_test(), 1);
}

#[tokio::test]
async fn holder_refuses_unentitled_fetcher_by_authenticated_node_id() {
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let consumer_dir = tempfile::tempdir().expect("consumer dir");
    let rogue_dir = tempfile::tempdir().expect("rogue dir");
    let failed_dir = tempfile::tempdir().expect("failed claimant dir");
    let wrong_hash_dir = tempfile::tempdir().expect("wrong-hash claimant dir");
    let impersonator_dir = tempfile::tempdir().expect("identity impersonator dir");
    let bypass_dir = tempfile::tempdir().expect("authorization bypass dir");
    let holder_key = identity_file(&holder_dir);
    let consumer_key = identity_file(&consumer_dir);
    let rogue_key = identity_file(&rogue_dir);
    let failed_key = identity_file(&failed_dir);
    let wrong_hash_key = identity_file(&wrong_hash_dir);
    let impersonator_key = identity_file(&impersonator_dir);
    let bypass_key = identity_file(&bypass_dir);
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);
    let rogue_node = node_id_of(&rogue_key);
    let failed_node = node_id_of(&failed_key);
    let wrong_hash_node = node_id_of(&wrong_hash_key);
    let content = marked("ROGUE-DENIED-9e9e", 256 * 1024);
    let h = BlobHash::of(&content);
    let other_hash = BlobHash::of(b"a different claimed blob");

    let holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&holder_db).expect("holder schema");
    seed_entitlement(&holder_db, "job-1", &holder_node, &consumer_node, &h);
    seed_entitlement(&holder_db, "job-failed", &holder_node, &failed_node, &h);
    record_failure(
        &holder_db,
        "job-failed",
        1,
        &failed_node,
        "attempt failed",
        T0,
    )
    .expect("record rogue failure");
    seed_entitlement(
        &holder_db,
        "job-other-hash",
        &holder_node,
        &wrong_hash_node,
        &other_hash,
    );
    let holder = BlobStore::new(
        holder_db,
        MovementPolicy {
            auto_propagate: true,
        },
        holder_key.clone(),
    );
    holder.set_test_clock(T0);
    holder.ingest_bytes(&content).expect("ingest");
    let endpoint = within(IrohServer::bind(&bind_spec(&holder_key)))
        .await
        .expect("bind holder");
    holder.serve_on(&endpoint);
    let ticket = endpoint.ticket();

    let rogue_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&rogue_db).expect("rogue schema");
    seed_entitlement(&rogue_db, "job-1", &holder_node, &rogue_node, &h);
    let rogue = BlobStore::new(
        rogue_db,
        MovementPolicy {
            auto_propagate: true,
        },
        rogue_key,
    );
    rogue.set_test_clock(T0);

    let mut sink = Vec::new();
    let result = within(rogue.resolve_blob_ref(&h, &ticket, &mut sink)).await;
    assert!(
        matches!(result, Err(ResolveError::Unentitled)),
        "got {result:?}"
    );
    assert!(sink.is_empty());
    assert_eq!(holder.fetch_requests_received_for_test(), 1);

    let failed_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&failed_db).expect("failed claimant schema");
    seed_entitlement(&failed_db, "job-failed", &holder_node, &failed_node, &h);
    let failed = BlobStore::new(
        failed_db,
        MovementPolicy {
            auto_propagate: true,
        },
        failed_key,
    );
    failed.set_test_clock(T0);
    let mut failed_sink = Vec::new();
    let failed_result = within(failed.resolve_blob_ref(&h, &ticket, &mut failed_sink)).await;
    assert!(
        matches!(failed_result, Err(ResolveError::Unentitled)),
        "failed claim attempt got {failed_result:?}"
    );
    assert!(failed_sink.is_empty());
    assert_eq!(holder.fetch_requests_received_for_test(), 2);

    let wrong_hash_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&wrong_hash_db).expect("wrong-hash claimant schema");
    seed_entitlement(
        &wrong_hash_db,
        "job-forged-requested-hash",
        &holder_node,
        &wrong_hash_node,
        &h,
    );
    let wrong_hash = BlobStore::new(
        wrong_hash_db,
        MovementPolicy {
            auto_propagate: true,
        },
        wrong_hash_key,
    );
    wrong_hash.set_test_clock(T0);
    let mut wrong_hash_sink = Vec::new();
    let wrong_hash_result =
        within(wrong_hash.resolve_blob_ref(&h, &ticket, &mut wrong_hash_sink)).await;
    assert!(
        matches!(wrong_hash_result, Err(ResolveError::Unentitled)),
        "a live claim for another hash must not authorize this request, got {wrong_hash_result:?}"
    );
    assert!(wrong_hash_sink.is_empty());
    assert_eq!(holder.fetch_requests_received_for_test(), 3);

    let impersonator_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&impersonator_db).expect("identity impersonator schema");
    seed_entitlement(&impersonator_db, "job-1", &holder_node, &consumer_node, &h);
    let impersonator = BlobStore::new(
        impersonator_db,
        MovementPolicy {
            auto_propagate: true,
        },
        impersonator_key,
    );
    impersonator.set_test_clock(T0);
    let mut impersonator_sink = Vec::new();
    let impersonator_result =
        within(impersonator.resolve_blob_ref(&h, &ticket, &mut impersonator_sink)).await;
    assert!(
        matches!(impersonator_result, Err(ResolveError::Unentitled)),
        "holder must use the authenticated peer identity, not the caller's forged ledger identity: {impersonator_result:?}"
    );
    assert!(impersonator_sink.is_empty());

    let bypass_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&bypass_db).expect("authorization bypass schema");
    let bypass = BlobStore::new(
        bypass_db,
        MovementPolicy {
            auto_propagate: true,
        },
        bypass_key,
    );
    bypass.set_test_clock(T0);
    let mut bypass_sink = Vec::new();
    let bypass_result =
        within(bypass.attempt_authorization_bypass_for_test(&h, &ticket, &mut bypass_sink)).await;
    assert!(
        matches!(bypass_result, Err(ResolveError::Unentitled)),
        "the stock provider path must not bypass holder authorization: {bypass_result:?}"
    );
    assert!(bypass_sink.is_empty());
    assert_eq!(
        holder.payload_bytes_emitted_for_test(),
        0,
        "all unauthorized authenticated callers must be refused before payload bytes"
    );
    assert_eq!(holder.fetch_requests_received_for_test(), 5);
}

#[tokio::test]
async fn holder_refuses_a_fetcher_whose_lease_has_expired_at_resolve() {
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let consumer_dir = tempfile::tempdir().expect("consumer dir");
    let holder_key = identity_file(&holder_dir);
    let consumer_key = identity_file(&consumer_dir);
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);
    let content = marked("EXPIRED-3B3B", 256 * 1024);
    let h = BlobHash::of(&content);

    let holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&holder_db).expect("holder schema");
    seed_entitlement(&holder_db, "job-1", &holder_node, &consumer_node, &h);
    let holder = BlobStore::new(
        holder_db,
        MovementPolicy {
            auto_propagate: true,
        },
        holder_key.clone(),
    );
    holder.set_test_clock(T0 + LEASE + 1);
    holder.ingest_bytes(&content).expect("ingest");
    let endpoint = within(IrohServer::bind(&bind_spec(&holder_key)))
        .await
        .expect("bind holder");
    holder.serve_on(&endpoint);
    let ticket = endpoint.ticket();

    let consumer_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&consumer_db).expect("consumer schema");
    seed_entitlement(&consumer_db, "job-1", &holder_node, &consumer_node, &h);
    let consumer = BlobStore::new(
        consumer_db,
        MovementPolicy {
            auto_propagate: true,
        },
        consumer_key,
    );
    consumer.set_test_clock(T0);

    let mut sink = Vec::new();
    let result = within(consumer.resolve_blob_ref(&h, &ticket, &mut sink)).await;
    assert!(
        matches!(result, Err(ResolveError::Unentitled)),
        "got {result:?}"
    );
    assert!(sink.is_empty());
    assert_eq!(
        holder.payload_bytes_emitted_for_test(),
        0,
        "entitlement refusal must happen before the holder emits payload bytes"
    );
    assert_eq!(holder.fetch_requests_received_for_test(), 1);
}

#[tokio::test]
async fn unset_holder_clock_falls_back_to_fabric_wallclock_and_refuses_expired_lease() {
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let consumer_dir = tempfile::tempdir().expect("consumer dir");
    let holder_key = identity_file(&holder_dir);
    let consumer_key = identity_file(&consumer_dir);
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);
    let content = marked("UNSET-CLOCK-2727", 256 * 1024);
    let h = BlobHash::of(&content);

    let holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&holder_db).expect("holder schema");
    seed_entitlement(&holder_db, "job-1", &holder_node, &consumer_node, &h);
    let holder = BlobStore::new(
        holder_db,
        MovementPolicy {
            auto_propagate: true,
        },
        holder_key.clone(),
    );
    holder.ingest_bytes(&content).expect("ingest");
    let endpoint = within(IrohServer::bind(&bind_spec(&holder_key)))
        .await
        .expect("bind holder");
    holder.serve_on(&endpoint);
    let ticket = endpoint.ticket();

    let consumer_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&consumer_db).expect("consumer schema");
    seed_entitlement(&consumer_db, "job-1", &holder_node, &consumer_node, &h);
    let consumer = BlobStore::new(
        consumer_db,
        MovementPolicy {
            auto_propagate: true,
        },
        consumer_key,
    );
    consumer.set_test_clock(T0);

    // RAII mock clock (restores the previous clock even on panic) — replaces
    // a hand-rolled reset-on-drop struct from before the seam grew its guard.
    let _clock = Wallclock::test_clock_guard(|| (T0 + LEASE + 1) as u64);

    let mut sink = Vec::new();
    let result = within(consumer.resolve_blob_ref(&h, &ticket, &mut sink)).await;
    assert!(
        matches!(result, Err(ResolveError::Unentitled)),
        "got {result:?}"
    );
    assert!(sink.is_empty());
    assert_eq!(
        holder.payload_bytes_emitted_for_test(),
        0,
        "entitlement refusal must happen before the holder emits payload bytes"
    );
    assert_eq!(holder.fetch_requests_received_for_test(), 1);
}

#[tokio::test]
async fn resolve_time_policy_recheck_forbids_entitled_node() {
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let consumer_dir = tempfile::tempdir().expect("consumer dir");
    let holder_key = identity_file(&holder_dir);
    let consumer_key = identity_file(&consumer_dir);
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);
    let content = marked("POLICY-FORBID-4d4d", 128 * 1024);
    let h = BlobHash::of(&content);

    let holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&holder_db).expect("holder schema");
    seed_entitlement(&holder_db, "job-1", &holder_node, &consumer_node, &h);
    let holder = BlobStore::new(
        holder_db,
        MovementPolicy {
            auto_propagate: false,
        },
        holder_key.clone(),
    );
    holder.set_test_clock(T0);
    holder.ingest_bytes(&content).expect("ingest");
    let endpoint = within(IrohServer::bind(&bind_spec(&holder_key)))
        .await
        .expect("bind holder");
    holder.serve_on(&endpoint);
    let ticket = endpoint.ticket();

    let consumer_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&consumer_db).expect("consumer schema");
    seed_entitlement(&consumer_db, "job-1", &holder_node, &consumer_node, &h);
    let consumer = BlobStore::new(
        consumer_db,
        MovementPolicy {
            auto_propagate: true,
        },
        consumer_key,
    );
    consumer.set_test_clock(T0);

    let mut sink = Vec::new();
    let result = within(consumer.resolve_blob_ref(&h, &ticket, &mut sink)).await;
    assert!(
        matches!(result, Err(ResolveError::PolicyForbidden)),
        "got {result:?}"
    );
    assert!(sink.is_empty());
    assert_eq!(
        holder.payload_bytes_emitted_for_test(),
        0,
        "movement-policy refusal must happen before the holder emits payload bytes"
    );
    assert_eq!(holder.fetch_requests_received_for_test(), 1);

    let mixed_holder_dir = tempfile::tempdir().expect("mixed holder dir");
    let mixed_consumer_dir = tempfile::tempdir().expect("mixed consumer dir");
    let mixed_holder_key = identity_file(&mixed_holder_dir);
    let mixed_consumer_key = identity_file(&mixed_consumer_dir);
    let mixed_holder_node = node_id_of(&mixed_holder_key);
    let mixed_consumer_node = node_id_of(&mixed_consumer_key);

    let mixed_holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&mixed_holder_db).expect("mixed holder schema");
    seed_mixed_entitlement(
        &mixed_holder_db,
        "job-mixed",
        &mixed_holder_node,
        &mixed_consumer_node,
        &h,
    );
    let mixed_holder = BlobStore::new(
        mixed_holder_db,
        MovementPolicy {
            auto_propagate: true,
        },
        mixed_holder_key.clone(),
    );
    mixed_holder.set_test_clock(T0);
    mixed_holder.ingest_bytes(&content).expect("mixed ingest");
    let mixed_endpoint = within(IrohServer::bind(&bind_spec(&mixed_holder_key)))
        .await
        .expect("bind mixed holder");
    mixed_holder.serve_on(&mixed_endpoint);
    let mixed_ticket = mixed_endpoint.ticket();

    let mixed_consumer_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&mixed_consumer_db).expect("mixed consumer schema");
    seed_mixed_entitlement(
        &mixed_consumer_db,
        "job-mixed",
        &mixed_holder_node,
        &mixed_consumer_node,
        &h,
    );
    let mixed_consumer = BlobStore::new(
        mixed_consumer_db,
        MovementPolicy {
            auto_propagate: true,
        },
        mixed_consumer_key,
    );
    mixed_consumer.set_test_clock(T0);

    let mut mixed_sink = Vec::new();
    let mixed_result =
        within(mixed_consumer.resolve_blob_ref(&h, &mixed_ticket, &mut mixed_sink)).await;
    assert!(
        matches!(mixed_result, Err(ResolveError::PolicyForbidden)),
        "a mixed blob_ref/local_path job must be refused by movement policy, got {mixed_result:?}"
    );
    assert!(mixed_sink.is_empty());
    assert_eq!(mixed_holder.payload_bytes_emitted_for_test(), 0);
    assert_eq!(mixed_holder.fetch_requests_received_for_test(), 1);
    mixed_endpoint.close().await;
}

#[tokio::test]
async fn tampered_bytes_are_caught_by_hash_verify() {
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let consumer_dir = tempfile::tempdir().expect("consumer dir");
    let holder_key = identity_file(&holder_dir);
    let consumer_key = identity_file(&consumer_dir);
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);
    let content = marked("GENUINE-5e5e", 200 * 1024);
    let h = BlobHash::of(&content);
    let tampered = marked("TAMPERED-5f5f", 200 * 1024);
    assert_ne!(BlobHash::of(&tampered), h);

    let holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&holder_db).expect("holder schema");
    seed_entitlement(&holder_db, "job-1", &holder_node, &consumer_node, &h);
    let holder = BlobStore::new(
        holder_db,
        MovementPolicy {
            auto_propagate: true,
        },
        holder_key.clone(),
    );
    holder.set_test_clock(T0);
    holder
        .serve_wrong_bytes_for_test(&h, &tampered)
        .expect("install tamper");
    let endpoint = within(IrohServer::bind(&bind_spec(&holder_key)))
        .await
        .expect("bind holder");
    holder.serve_on(&endpoint);
    let ticket = endpoint.ticket();

    let consumer_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&consumer_db).expect("consumer schema");
    seed_entitlement(&consumer_db, "job-1", &holder_node, &consumer_node, &h);
    let consumer = BlobStore::new(
        consumer_db,
        MovementPolicy {
            auto_propagate: true,
        },
        consumer_key,
    );
    consumer.set_test_clock(T0);

    let mut sink = Vec::new();
    let result = within(consumer.resolve_blob_ref(&h, &ticket, &mut sink)).await;
    assert!(
        matches!(&result, Err(ResolveError::HashMismatch { expected, .. }) if expected == &h),
        "got {result:?}"
    );
    assert_ne!(BlobHash::of(&sink), h);
    assert!(
        sink.is_empty(),
        "unverified bytes must never reach the caller-facing worker sink"
    );
    assert_eq!(holder.fetch_requests_received_for_test(), 1);
    assert_eq!(
        holder.payload_bytes_emitted_for_test(),
        tampered.len() as u64,
        "tampered provider bytes must traverse verified transport without reaching the worker sink"
    );
}

#[tokio::test]
async fn resolve_against_a_down_holder_is_holder_unreachable() {
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let consumer_dir = tempfile::tempdir().expect("consumer dir");
    let holder_key = identity_file(&holder_dir);
    let consumer_key = identity_file(&consumer_dir);
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);
    let content = marked("UNREACHABLE-6a6a", 64 * 1024);
    let h = BlobHash::of(&content);

    let endpoint = within(IrohServer::bind(&bind_spec(&holder_key)))
        .await
        .expect("bind holder");
    let ticket = endpoint.ticket();
    endpoint.close().await;

    let consumer_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&consumer_db).expect("consumer schema");
    seed_entitlement(&consumer_db, "job-1", &holder_node, &consumer_node, &h);
    let consumer = BlobStore::new(
        consumer_db,
        MovementPolicy {
            auto_propagate: true,
        },
        consumer_key,
    );
    consumer.set_test_clock(T0);

    let mut sink = Vec::new();
    let result = within(consumer.resolve_blob_ref(&h, &ticket, &mut sink)).await;
    assert!(
        matches!(result, Err(ResolveError::HolderUnreachable)),
        "got {result:?}"
    );
    assert!(sink.is_empty());
}

/// A consumer that ALREADY holds the referenced content in its own local
/// content-addressed store — e.g. because it ingested it itself while
/// serving as the job's submitter/hub — must materialize the blob from that
/// local copy rather than dialing the holder ticket at all. Today
/// `resolve_blob_ref` has no local-content check: it runs the client-side
/// entitlement pre-check, then unconditionally dials `holder_ticket` via
/// `adapter::fetch_into_sink`. Against an undialable (closed) ticket that
/// dial fails with `HolderUnreachable` even though the consumer's own store
/// already has the exact bytes on disk — the live defect this test pins.
#[tokio::test]
async fn locally_held_content_materializes_without_reaching_the_holder() {
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let consumer_dir = tempfile::tempdir().expect("consumer dir");
    let holder_key = identity_file(&holder_dir);
    let consumer_key = identity_file(&consumer_dir);
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);
    let content = marked("LOCAL-ALREADY-HELD-5d5d", 64 * 1024);
    let h = BlobHash::of(&content);

    // Mint a well-formed but undialable holder ticket — same "bind then
    // close" idiom as `resolve_against_a_down_holder_is_holder_unreachable`.
    let endpoint = within(IrohServer::bind(&bind_spec(&holder_key)))
        .await
        .expect("bind holder");
    let dead_ticket = endpoint.ticket();
    endpoint.close().await;

    let consumer_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&consumer_db).expect("consumer schema");
    seed_entitlement(&consumer_db, "job-1", &holder_node, &consumer_node, &h);
    let consumer = BlobStore::new(
        consumer_db,
        MovementPolicy {
            auto_propagate: true,
        },
        consumer_key,
    );
    consumer.set_test_clock(T0);

    // The consumer's OWN store already holds this content — ingested through
    // the service's normal ingest path, exactly as production would if this
    // node had already produced or previously fetched the bytes.
    assert_eq!(consumer.ingest_bytes(&content).expect("local ingest"), h);

    let mut sink = Vec::new();
    let written = within(consumer.resolve_blob_ref(&h, &dead_ticket, &mut sink))
        .await
        .expect("locally held content must materialize without reaching the (dead) holder");
    assert_eq!(written as usize, content.len());
    assert_eq!(sink, content);
}

#[tokio::test]
async fn authorized_holder_without_the_blob_returns_blob_not_found() {
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let consumer_dir = tempfile::tempdir().expect("consumer dir");
    let holder_key = identity_file(&holder_dir);
    let consumer_key = identity_file(&consumer_dir);
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);
    let content = marked("NEVER-INGESTED-7a7a", 96 * 1024);
    let h = BlobHash::of(&content);

    let holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&holder_db).expect("holder schema");
    seed_entitlement(&holder_db, "job-1", &holder_node, &consumer_node, &h);
    let holder = BlobStore::new(
        holder_db,
        MovementPolicy {
            auto_propagate: true,
        },
        holder_key.clone(),
    );
    holder.set_test_clock(T0);
    let endpoint = within(IrohServer::bind(&bind_spec(&holder_key)))
        .await
        .expect("bind holder");
    holder.serve_on(&endpoint);
    let ticket = endpoint.ticket();

    let consumer_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&consumer_db).expect("consumer schema");
    seed_entitlement(&consumer_db, "job-1", &holder_node, &consumer_node, &h);
    let consumer = BlobStore::new(
        consumer_db,
        MovementPolicy {
            auto_propagate: true,
        },
        consumer_key,
    );
    consumer.set_test_clock(T0);

    let mut sink = Vec::new();
    let result = within(consumer.resolve_blob_ref(&h, &ticket, &mut sink)).await;
    assert!(
        matches!(result, Err(ResolveError::BlobNotFound)),
        "got {result:?}"
    );
    assert!(sink.is_empty());
    assert_eq!(holder.fetch_requests_received_for_test(), 1);
}

#[tokio::test]
async fn holder_dropping_mid_transfer_yields_typed_abort_no_hang() {
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let consumer_dir = tempfile::tempdir().expect("consumer dir");
    let holder_key = identity_file(&holder_dir);
    let consumer_key = identity_file(&consumer_dir);
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);
    let content = marked("BIG-DROP-8a8a", 8 * MB);
    let h = BlobHash::of(&content);

    let holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&holder_db).expect("holder schema");
    seed_entitlement(&holder_db, "job-1", &holder_node, &consumer_node, &h);
    let holder = BlobStore::new(
        holder_db,
        MovementPolicy {
            auto_propagate: true,
        },
        holder_key.clone(),
    );
    holder.set_test_clock(T0);
    holder.ingest_bytes(&content).expect("ingest");
    holder.drop_after_bytes_for_test(64 * 1024);
    let endpoint = within(IrohServer::bind(&bind_spec(&holder_key)))
        .await
        .expect("bind holder");
    holder.serve_on(&endpoint);
    let ticket = endpoint.ticket();

    let consumer_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&consumer_db).expect("consumer schema");
    seed_entitlement(&consumer_db, "job-1", &holder_node, &consumer_node, &h);
    let consumer = BlobStore::new(
        consumer_db,
        MovementPolicy {
            auto_propagate: true,
        },
        consumer_key,
    );
    consumer.set_test_clock(T0);

    let mut sink = Vec::new();
    let result = within(consumer.resolve_blob_ref(&h, &ticket, &mut sink)).await;
    assert!(
        matches!(result, Err(ResolveError::TransferAborted)),
        "got {result:?}"
    );
    assert_ne!(BlobHash::of(&sink), h);
    assert_eq!(holder.fetch_requests_received_for_test(), 1);
}

#[tokio::test]
async fn blob_exceeding_the_frame_ceiling_transfers_over_the_streaming_surface() {
    let size = 64 * MB + 4096;
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let consumer_dir = tempfile::tempdir().expect("consumer dir");
    let holder_key = identity_file(&holder_dir);
    let consumer_key = identity_file(&consumer_dir);
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);
    let content = marked("OVER-64MB-9a9a", size);
    let h = BlobHash::of(&content);

    let holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&holder_db).expect("holder schema");
    seed_entitlement(&holder_db, "job-1", &holder_node, &consumer_node, &h);
    let holder = BlobStore::new(
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
    let consumer = BlobStore::new(
        consumer_db,
        MovementPolicy {
            auto_propagate: true,
        },
        consumer_key,
    );
    consumer.set_test_clock(T0);

    let mut sink = Vec::with_capacity(size);
    let written = within(consumer.resolve_blob_ref(&h, &ticket, &mut sink))
        .await
        .expect("a >64MB blob must transfer");
    assert_eq!(written as usize, size);
    assert_eq!(BlobHash::of(&sink), h);
    assert_eq!(holder.fetch_requests_received_for_test(), 1);
    assert_eq!(
        holder.payload_bytes_emitted_for_test(),
        content.len() as u64
    );
}

#[tokio::test]
async fn hub_holds_only_the_reference_never_the_blob_bytes() {
    use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
    use contextdb_engine::work_ledger::apply_work_ledger_policy_overrides;
    use contextdb_server::{InProcessBroker, SyncClient, SyncServer};
    use std::sync::atomic::{AtomicBool, Ordering};

    let broker = InProcessBroker::new();
    let tenant = "hub-bypass";
    let hub_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&hub_db).expect("hub schema");
    let mut hub_policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);
    apply_work_ledger_policy_overrides(&mut hub_policies);
    let hub = Arc::new(SyncServer::with_transport(
        hub_db.clone(),
        broker.server(),
        contextdb_core::TenantId::from(tenant),
        hub_policies,
    ));
    let hub_shutdown = Arc::new(AtomicBool::new(false));
    let hub_task = tokio::spawn({
        let hub = hub.clone();
        let s = hub_shutdown.clone();
        async move { hub.run_until(s).await }
    });

    let holder_dir = tempfile::tempdir().expect("holder dir");
    let consumer_dir = tempfile::tempdir().expect("consumer dir");
    let holder_key = identity_file(&holder_dir);
    let consumer_key = identity_file(&consumer_dir);
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);
    let marker = "BLOBPAYLOAD7F3A";
    let content = marked(marker, 300 * 1024);
    let h = BlobHash::of(&content);
    let marker_hex: String = marker.bytes().map(|b| format!("{b:02x}")).collect();
    let marker_b64 = base64_standard(marker.as_bytes());

    let holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&holder_db).expect("holder schema");
    submit_job(
        &holder_db,
        &blob_job("job-1", &holder_node, &h),
        &[] as &[&[u8]],
    )
    .expect("submit");
    let holder_client = SyncClient::with_transport(
        holder_db.clone(),
        broker.client(),
        contextdb_core::TenantId::from(tenant),
    );
    within(holder_client.push())
        .await
        .expect("holder push job row");
    let holder = BlobStore::new(
        holder_db.clone(),
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
    let consumer_client = SyncClient::with_transport(
        consumer_db.clone(),
        broker.client(),
        contextdb_core::TenantId::from(tenant),
    );
    within(consumer_client.pull_default())
        .await
        .expect("consumer pull job row");
    match insert_claim(&consumer_db, "job-1", 1, &consumer_node, T0 + LEASE, T0).expect("claim") {
        ClaimInsert::Inserted => {}
        other => panic!("claim must insert, got {other:?}"),
    }
    within(consumer_client.push())
        .await
        .expect("consumer push claim");
    within(holder_client.pull_default())
        .await
        .expect("holder pull consumer claim");
    let consumer = BlobStore::new(
        consumer_db,
        MovementPolicy {
            auto_propagate: true,
        },
        consumer_key,
    );
    consumer.set_test_clock(T0);

    let mut sink = Vec::new();
    within(consumer.resolve_blob_ref(&h, &ticket, &mut sink))
        .await
        .expect("entitled resolve");
    assert_eq!(BlobHash::of(&sink), h);
    assert!(row_count(&hub_db, "work_jobs") >= 1);
    assert!(row_count(&hub_db, "work_claims") >= 1);
    assert!(
        !any_table_contains(&hub_db, marker.as_bytes())
            && !any_table_contains(&hub_db, marker_hex.as_bytes())
            && !any_table_contains(&hub_db, marker_b64.as_bytes())
    );
    assert!(sink.windows(marker.len()).any(|w| w == marker.as_bytes()));
    assert_eq!(holder.fetch_requests_received_for_test(), 1);
    assert_eq!(
        holder.payload_bytes_emitted_for_test(),
        content.len() as u64
    );

    hub_shutdown.store(true, Ordering::SeqCst);
    let _ = hub_task.await;
}

fn row_count(db: &Database, table: &str) -> usize {
    match db.execute(&format!("SELECT * FROM {table}"), &HashMap::new()) {
        Ok(r) => r.rows.len(),
        Err(_) => 0,
    }
}

fn any_table_contains(db: &Database, needle: &[u8]) -> bool {
    for table in db.table_names() {
        let Ok(result) = db.execute(&format!("SELECT * FROM {table}"), &HashMap::new()) else {
            continue;
        };
        for row in &result.rows {
            for cell in row {
                let rendered = format!("{cell:?}");
                if rendered
                    .as_bytes()
                    .windows(needle.len())
                    .any(|w| w == needle)
                {
                    return true;
                }
            }
        }
    }
    false
}

fn base64_standard(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[((n >> 18) & 0x3f) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[((n >> 6) & 0x3f) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[(n & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

#[tokio::test]
async fn sink_write_failure_is_a_distinct_typed_error_no_hang() {
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let consumer_dir = tempfile::tempdir().expect("consumer dir");
    let holder_key = identity_file(&holder_dir);
    let consumer_key = identity_file(&consumer_dir);
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);
    let content = marked("SINK-FAIL-1818", 4 * MB);
    let h = BlobHash::of(&content);

    let holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&holder_db).expect("holder schema");
    seed_entitlement(&holder_db, "job-1", &holder_node, &consumer_node, &h);
    let holder = BlobStore::new(
        holder_db,
        MovementPolicy {
            auto_propagate: true,
        },
        holder_key.clone(),
    );
    holder.set_test_clock(T0);
    holder.ingest_bytes(&content).expect("ingest");
    let endpoint = within(IrohServer::bind(&bind_spec(&holder_key)))
        .await
        .expect("bind holder");
    holder.serve_on(&endpoint);
    let ticket = endpoint.ticket();

    let consumer_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&consumer_db).expect("consumer schema");
    seed_entitlement(&consumer_db, "job-1", &holder_node, &consumer_node, &h);
    let consumer = BlobStore::new(
        consumer_db,
        MovementPolicy {
            auto_propagate: true,
        },
        consumer_key,
    );
    consumer.set_test_clock(T0);

    let mut sink = FailingSink::new(64 * 1024);
    let result = within(consumer.resolve_blob_ref(&h, &ticket, &mut sink)).await;
    assert!(
        matches!(result, Err(ResolveError::SinkWrite(_))),
        "got {result:?}"
    );
    assert_eq!(holder.fetch_requests_received_for_test(), 1);
}

#[tokio::test]
async fn reclaim_spares_a_blob_a_live_job_still_references() {
    let dir = tempfile::tempdir().expect("dir");
    let holder_key = identity_file(&dir);
    let holder_node = node_id_of(&holder_key);
    let content = marked("SHARED-1919", 128 * 1024);
    let h = BlobHash::of(&content);

    let db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&db).expect("schema");
    submit_job(
        &db,
        &blob_job("job-terminal", &holder_node, &h),
        &[] as &[&[u8]],
    )
    .expect("submit terminal");
    submit_job(
        &db,
        &blob_job("job-live", &holder_node, &h),
        &[] as &[&[u8]],
    )
    .expect("submit live");
    cancel_job(&db, "job-terminal", "operator", None, T0).expect("cancel terminal");

    let holder = BlobStore::new(
        db.clone(),
        MovementPolicy {
            auto_propagate: true,
        },
        holder_key,
    );
    holder.ingest_bytes(&content).expect("ingest");
    let reclaimed = holder
        .reclaim_unreferenced(T0 + GRACE + 1, GRACE)
        .expect("reclaim");
    assert_eq!(reclaimed, 0);

    let solo = marked("WITHIN-GRACE-19b", 96 * 1024);
    let h_solo = BlobHash::of(&solo);
    submit_job(
        &db,
        &blob_job("job-solo", &holder_node, &h_solo),
        &[] as &[&[u8]],
    )
    .expect("submit solo");
    cancel_job(&db, "job-solo", "operator", None, T0).expect("cancel solo");
    holder.ingest_bytes(&solo).expect("ingest solo");
    let spared = holder
        .reclaim_unreferenced(T0 + GRACE - 1, GRACE)
        .expect("reclaim within grace");
    assert_eq!(spared, 0);
}

#[tokio::test]
async fn reclaim_deletes_an_unreferenced_blob_then_resolve_is_blob_not_found() {
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let consumer_dir = tempfile::tempdir().expect("consumer dir");
    let holder_key = identity_file(&holder_dir);
    let consumer_key = identity_file(&consumer_dir);
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);
    let content = marked("RECLAIMED-2020", 128 * 1024);
    let h = BlobHash::of(&content);

    let holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&holder_db).expect("holder schema");
    seed_entitlement(&holder_db, "job-1", &holder_node, &consumer_node, &h);
    cancel_job(&holder_db, "job-1", "operator", None, T0).expect("cancel");
    let holder = BlobStore::new(
        holder_db,
        MovementPolicy {
            auto_propagate: true,
        },
        holder_key.clone(),
    );
    let resolve_now = T0 + GRACE + 1;
    holder.set_test_clock(resolve_now);
    holder.ingest_bytes(&content).expect("ingest");

    let reclaimed = holder
        .reclaim_unreferenced(resolve_now, GRACE)
        .expect("reclaim");
    assert_eq!(reclaimed, 1);

    let endpoint = within(IrohServer::bind(&bind_spec(&holder_key)))
        .await
        .expect("bind holder");
    holder.serve_on(&endpoint);
    let ticket = endpoint.ticket();

    let consumer_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&consumer_db).expect("consumer schema");
    seed_entitlement(&consumer_db, "job-1", &holder_node, &consumer_node, &h);
    let consumer = BlobStore::new(
        consumer_db,
        MovementPolicy {
            auto_propagate: true,
        },
        consumer_key,
    );
    consumer.set_test_clock(resolve_now);

    let mut sink = Vec::new();
    let result = within(consumer.resolve_blob_ref(&h, &ticket, &mut sink)).await;
    assert!(
        matches!(result, Err(ResolveError::BlobNotFound)),
        "got {result:?}"
    );
    assert!(sink.is_empty());
    assert_eq!(holder.fetch_requests_received_for_test(), 1);
}

#[tokio::test]
async fn two_entitled_consumers_resolve_the_same_blob_concurrently() {
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let c1_dir = tempfile::tempdir().expect("c1 dir");
    let c2_dir = tempfile::tempdir().expect("c2 dir");
    let holder_key = identity_file(&holder_dir);
    let c1_key = identity_file(&c1_dir);
    let c2_key = identity_file(&c2_dir);
    let holder_node = node_id_of(&holder_key);
    let c1_node = node_id_of(&c1_key);
    let c2_node = node_id_of(&c2_key);
    let content = marked("CONCURRENT-2424", 256 * 1024);
    let h = BlobHash::of(&content);

    let holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&holder_db).expect("holder schema");
    seed_entitlement(&holder_db, "job-c1", &holder_node, &c1_node, &h);
    seed_entitlement(&holder_db, "job-c2", &holder_node, &c2_node, &h);
    let holder = BlobStore::new(
        holder_db,
        MovementPolicy {
            auto_propagate: true,
        },
        holder_key.clone(),
    );
    holder.set_test_clock(T0);
    holder.ingest_bytes(&content).expect("ingest");
    let endpoint = within(IrohServer::bind(&bind_spec(&holder_key)))
        .await
        .expect("bind holder");
    holder.serve_on(&endpoint);
    let ticket = endpoint.ticket();

    let c1_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&c1_db).expect("c1 schema");
    seed_entitlement(&c1_db, "job-c1", &holder_node, &c1_node, &h);
    let c1 = BlobStore::new(
        c1_db,
        MovementPolicy {
            auto_propagate: true,
        },
        c1_key,
    );
    c1.set_test_clock(T0);

    let c2_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&c2_db).expect("c2 schema");
    seed_entitlement(&c2_db, "job-c2", &holder_node, &c2_node, &h);
    let c2 = BlobStore::new(
        c2_db,
        MovementPolicy {
            auto_propagate: true,
        },
        c2_key,
    );
    c2.set_test_clock(T0);

    let mut sink1 = Vec::new();
    let mut sink2 = Vec::new();
    let (r1, r2) = within(async {
        tokio::join!(
            c1.resolve_blob_ref(&h, &ticket, &mut sink1),
            c2.resolve_blob_ref(&h, &ticket, &mut sink2),
        )
    })
    .await;
    r1.expect("consumer 1 resolve must succeed");
    r2.expect("consumer 2 resolve must succeed");
    assert_eq!(sink1, content);
    assert_eq!(sink2, content);
    assert_eq!(holder.fetch_requests_received_for_test(), 2);
    assert_eq!(
        holder.payload_bytes_emitted_for_test(),
        (2 * content.len()) as u64
    );
}

#[tokio::test]
async fn interrupted_large_transfer_resumes_from_offset_not_from_zero() {
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let consumer_dir = tempfile::tempdir().expect("consumer dir");
    let rogue_dir = tempfile::tempdir().expect("resume rogue dir");
    let holder_key = identity_file(&holder_dir);
    let consumer_key = identity_file(&consumer_dir);
    let rogue_key = identity_file(&rogue_dir);
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);

    let size = 8 * MB;
    let interrupt_at: u64 = 5 * MB as u64;
    let content = marked("RESUME-2222", size);
    let h = BlobHash::of(&content);

    let holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&holder_db).expect("holder schema");
    seed_entitlement(&holder_db, "job-1", &holder_node, &consumer_node, &h);
    let holder = BlobStore::new(
        holder_db,
        MovementPolicy {
            auto_propagate: true,
        },
        holder_key.clone(),
    );
    holder.set_test_clock(T0);
    holder.ingest_bytes(&content).expect("ingest");
    holder.drop_after_bytes_for_test(interrupt_at);
    let endpoint = within(IrohServer::bind(&bind_spec(&holder_key)))
        .await
        .expect("bind holder");
    holder.serve_on(&endpoint);
    let ticket = endpoint.ticket();

    let consumer_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&consumer_db).expect("consumer schema");
    seed_entitlement(&consumer_db, "job-1", &holder_node, &consumer_node, &h);
    let consumer = BlobStore::new(
        consumer_db,
        MovementPolicy {
            auto_propagate: true,
        },
        consumer_key,
    );
    consumer.set_test_clock(T0);

    let mut sink = Vec::new();
    let first = within(consumer.resolve_blob_ref(&h, &ticket, &mut sink)).await;
    assert!(
        matches!(first, Err(ResolveError::TransferAborted)),
        "first leg: {first:?}"
    );
    assert_eq!(holder.fetch_requests_received_for_test(), 1);
    let after_first = sink.len() as u64;
    assert!(
        after_first > 0 && after_first < size as u64,
        "first leg must retain a meaningful verified prefix, got {after_first} bytes"
    );
    let valid_prefix = sink.clone();
    assert!(
        sink.len() > 256 * 1024,
        "resume fixture must retain more than one transfer chunk"
    );
    let corrupt_at = sink.len() / 2;
    sink[corrupt_at] ^= 0xff;
    endpoint.close().await;
    let corrupt_before = sink.clone();
    let emitted_before_corrupt = holder.payload_bytes_emitted_for_test();
    let requests_before_corrupt = holder.fetch_requests_received_for_test();
    let corrupt = within(consumer.resume_bytes_moved_for_test(&h, &ticket, &mut sink)).await;
    assert!(
        matches!(corrupt, Err(ResolveError::HashMismatch { .. })),
        "a corrupt existing prefix must fail verification before dialing the unavailable holder, got {corrupt:?}"
    );
    assert_eq!(sink, corrupt_before);
    assert_eq!(
        holder.payload_bytes_emitted_for_test(),
        emitted_before_corrupt,
        "corrupt-prefix refusal must happen before provider payload emission"
    );
    assert_eq!(
        holder.fetch_requests_received_for_test(),
        requests_before_corrupt,
        "corrupt prefix must fail before contacting the unavailable holder"
    );

    sink = valid_prefix;
    let resumed_endpoint = within(IrohServer::bind(&bind_spec(&holder_key)))
        .await
        .expect("rebind holder after prefix check");
    holder.serve_on(&resumed_endpoint);
    let resumed_ticket = resumed_endpoint.ticket();

    let rogue_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&rogue_db).expect("resume rogue schema");
    let rogue_node = node_id_of(&rogue_key);
    seed_entitlement(
        &rogue_db,
        "job-forged-resume",
        &holder_node,
        &rogue_node,
        &h,
    );
    let rogue = BlobStore::new(
        rogue_db,
        MovementPolicy {
            auto_propagate: true,
        },
        rogue_key,
    );
    rogue.set_test_clock(T0);
    let mut rogue_sink = sink.clone();
    let rogue_before = rogue_sink.clone();
    let emitted_before_rogue = holder.payload_bytes_emitted_for_test();
    let rogue_resume =
        within(rogue.resume_bytes_moved_for_test(&h, &resumed_ticket, &mut rogue_sink)).await;
    assert!(
        matches!(rogue_resume, Err(ResolveError::Unentitled)),
        "every resume request must reauthorize its authenticated caller, got {rogue_resume:?}"
    );
    assert_eq!(rogue_sink, rogue_before);
    assert_eq!(
        holder.payload_bytes_emitted_for_test(),
        emitted_before_rogue,
        "unauthorized resume must emit no payload bytes"
    );
    assert_eq!(holder.fetch_requests_received_for_test(), 2);

    let payload_before_retry = holder.payload_bytes_emitted_for_test();
    let bytes_on_retry =
        within(consumer.resume_bytes_moved_for_test(&h, &resumed_ticket, &mut sink))
            .await
            .expect("resume leg must succeed");
    assert_eq!(BlobHash::of(&sink), h);
    assert!(
        bytes_on_retry <= (size as u64 - after_first) + (256 * 1024),
        "retry must resume from offset {after_first}, moved {bytes_on_retry}"
    );
    assert_eq!(
        holder
            .payload_bytes_emitted_for_test()
            .saturating_sub(payload_before_retry),
        bytes_on_retry,
        "reported resume movement must equal the holder's measured tail payload"
    );
    assert_eq!(holder.fetch_requests_received_for_test(), 3);
    resumed_endpoint.close().await;
}

/// A fetch whose holder silently BLACK-HOLES the dial (a ticket address that
/// drops packets rather than answering or refusing) must still return a typed
/// `HolderUnreachable` in bounded time — never an indefinite hang. This is the
/// "silent drop" cousin of the connection-refused case that
/// `resolve_against_a_down_holder_is_holder_unreachable` covers: a closed
/// endpoint is refused (RST) quickly, but a routed-yet-unanswering address is
/// exactly where an unbounded dial would hang forever.
///
/// The bypass seam skips the client-side entitlement pre-check so the DIAL
/// itself is exercised (not the fast-failing pre-check). The outer guard is
/// comfortably longer than the internal dial bound: if the inner future ever
/// hung, the guard would fire and this assert would catch it. Today the inner
/// future completes because `peer_connect` dials through the transport's
/// `CONNECT_TIMEOUT`, which maps a dial that gives up to `HolderUnreachable`.
#[tokio::test]
async fn blackhole_holder_dial_returns_bounded_holder_unreachable() {
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let consumer_dir = tempfile::tempdir().expect("consumer dir");
    let holder_key = identity_file(&holder_dir);
    let consumer_key = identity_file(&consumer_dir);
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);
    let content = marked("BLACKHOLE-9f9f", 64 * 1024);
    let h = BlobHash::of(&content);

    // Bind a real holder ONLY to mint a well-formed ticket carrying a valid
    // endpoint id, then close it and rewrite the ticket to point at a
    // black-hole address (240.0.0.0/4 is reserved and silently dropped, not
    // connection-refused) so the dial has an address it will try and never
    // reach.
    let endpoint = within(IrohServer::bind(&bind_spec(&holder_key)))
        .await
        .expect("bind holder");
    let real_ticket = endpoint.ticket();
    endpoint.close().await;
    let full: iroh_tickets::endpoint::EndpointTicket =
        real_ticket.parse().expect("holder ticket parses");
    let blackhole_addr = iroh::EndpointAddr::new(full.endpoint_addr().id)
        .with_ip_addr("240.0.0.1:38321".parse().expect("black-hole socket addr"));
    let blackhole_ticket = iroh_tickets::endpoint::EndpointTicket::new(blackhole_addr).to_string();

    let consumer_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&consumer_db).expect("consumer schema");
    seed_entitlement(&consumer_db, "job-1", &holder_node, &consumer_node, &h);
    let consumer = BlobStore::new(
        consumer_db,
        MovementPolicy {
            auto_propagate: true,
        },
        consumer_key,
    );
    consumer.set_test_clock(T0);

    let mut sink = Vec::new();
    // Self-guarding: the outer guard (45s) is well above the internal dial
    // bound. A hang would trip the guard (the `Err(_)` arm); a bounded dial
    // completes the inner future (the `Ok(...)` arm) with a typed error.
    let guarded = tokio::time::timeout(
        Duration::from_secs(45),
        consumer.attempt_authorization_bypass_for_test(&h, &blackhole_ticket, &mut sink),
    )
    .await;
    let inner = guarded.expect(
        "the dial must return a typed result within the guard — it must never hang indefinitely",
    );
    assert!(
        matches!(inner, Err(ResolveError::HolderUnreachable)),
        "a black-holed dial must surface as HolderUnreachable, got {inner:?}"
    );
    assert!(sink.is_empty(), "a failed dial writes nothing to the sink");
}

/// Faithful to the p1-fresh-embed-probe layout: the holder and consumer
/// identity files live in the SAME parent directory (distinct filenames), so
/// their `store_root()` collides on one `<dir>/blob-store`. The holder opens
/// (and keeps open) that content store when it ingests + serves; the consumer
/// then opens the SAME store inside `resolve_blob_ref`, BEFORE the entitlement
/// pre-check or any dial. That second open contends for the redb file lock.
///
/// The bug this guards: the store open is awaited SYNCHRONOUSLY (a spawned
/// thread joined in `BlobStoreHandle::open`), so an unbounded wait there
/// wedges the calling worker in a way no caller-side `tokio::time::timeout`
/// can reclaim — exactly the transcript's ">30s, needed an OS kill." The
/// promise (USR-2/INT-17): a fetch must ALWAYS return a bounded, typed
/// `ResolveError`, never an un-cancellable hang.
///
/// Because a `tokio::time::timeout` cannot reclaim a synchronous wedge, this
/// test drives the resolve on a dedicated OS thread and bounds it with an
/// OS-level `recv_timeout`. Pre-fix the thread wedges and the guard fires
/// (the test fails without hanging CI); post-fix the resolve returns a typed
/// `LocalStoreUnavailable` well within the guard.
#[test]
fn shared_store_root_resolve_returns_bounded_typed_error_not_uncancellable_hang() {
    let dir = tempfile::tempdir().expect("shared dir");
    // Distinct identity FILENAMES, one shared PARENT dir → colliding store root.
    let holder_key = dir.path().join("probe-blob-holder-identity.key");
    let consumer_key = dir.path().join("probe-blob-consumer-identity.key");
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);
    let content = marked("SHARED-ROOT-1234", 64 * 1024);
    let h = BlobHash::of(&content);

    // A small runtime for the setup (bind/serve); the holder keeps its store
    // handle — and therefore the redb lock — open for the whole test.
    let setup_rt = tokio::runtime::Runtime::new().expect("setup runtime");
    let holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&holder_db).expect("holder schema");
    seed_entitlement(&holder_db, "job-1", &holder_node, &consumer_node, &h);
    let holder = BlobStore::new(
        holder_db,
        MovementPolicy {
            auto_propagate: true,
        },
        holder_key.clone(),
    );
    holder.set_test_clock(T0);
    assert_eq!(holder.ingest_bytes(&content).expect("ingest"), h);
    let endpoint = setup_rt
        .block_on(IrohServer::bind(&bind_spec(&holder_key)))
        .expect("bind holder");
    holder.serve_on(&endpoint);
    let ticket = endpoint.ticket();

    let consumer_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&consumer_db).expect("consumer schema");
    let consumer = BlobStore::new(
        consumer_db,
        MovementPolicy {
            auto_propagate: true,
        },
        consumer_key,
    );
    consumer.set_test_clock(T0);
    // Trim the fail-fast bound so the (now-bounded) contended open returns
    // quickly; production keeps its full default. This only trims the wait a
    // test observes — the production guarantee is unchanged.
    consumer.set_store_open_timeout_ms_for_test(2_000);

    // Drive the resolve on its OWN OS thread with its OWN runtime and bound it
    // at the OS level: a synchronous store-open wedge cannot be reclaimed by a
    // tokio timeout, so the guard must sit beneath the runtime.
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("resolve runtime");
        let mut sink = Vec::new();
        let outcome = rt.block_on(consumer.resolve_blob_ref(&h, &ticket, &mut sink));
        let _ = tx.send((outcome, sink.len()));
    });

    let result = rx.recv_timeout(Duration::from_secs(20));
    match result {
        Ok((outcome, sink_len)) => {
            let _ = worker.join();
            assert!(
                matches!(outcome, Err(ResolveError::LocalStoreUnavailable)),
                "a colliding store root must surface as a typed LocalStoreUnavailable, got {outcome:?}"
            );
            assert_eq!(sink_len, 0, "a failed local store-open writes nothing");
        }
        Err(_) => panic!(
            "resolve_blob_ref did not return within the guard: the local store-open wedged \
             (un-cancellable synchronous hang) instead of returning a bounded, typed error"
        ),
    }

    // Restore the process-global open bound (the production default) so a later
    // parallel test in this binary is not held to the trimmed 2s window.
    holder.set_store_open_timeout_ms_for_test(10_000);

    setup_rt.block_on(endpoint.close());
}
