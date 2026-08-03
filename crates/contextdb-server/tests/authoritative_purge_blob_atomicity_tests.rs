//! RED contracts for authoritative removal of engine-held transfer media.
//!
//! These tests deliberately name only narrow test seams where the current
//! external blob adapter has no ContextDB-owned equivalent yet. They exercise
//! the public PURGE and node-to-node transfer surfaces otherwise.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::work_ledger::{
    BlobHash, InputRef, JobSpec, MovementPolicy, install_work_ledger_schema, submit_job,
};
use contextdb_server::blob_resolver::{BlobStore, ResolveError};
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{FabricIdentity, SyncClient, SyncServer, peer_dial_spec};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[path = "media_support/mod.rs"]
mod media_support;
use media_support::*;

const MB: usize = 1024 * 1024;

fn policy() -> MovementPolicy {
    MovementPolicy {
        auto_propagate: true,
    }
}

fn submit_blob_job(db: &Database, job_id: &str, submitter: &str, hash: &BlobHash) {
    let job = JobSpec::builder(job_id, "media.purge", "input", submitter)
        .input_refs(vec![InputRef::blob_ref(hash.clone())])
        .submitted_at_ms(T0)
        .build();
    submit_job(db, &job, &[] as &[&[u8]]).expect("submit blob-referencing work job");
}

fn exact_hashes(store: &BlobStore) -> BTreeSet<[u8; 32]> {
    store.exact_hash_state_for_test()
}

fn marked(marker: &str, size: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(size);
    bytes.extend_from_slice(marker.as_bytes());
    bytes.extend((0..size.saturating_sub(bytes.len())).map(|index| index as u8));
    bytes
}

struct Hub {
    db: Arc<Database>,
    ticket: String,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

async fn start_hub(root: &Path, tenant: &str) -> Hub {
    let identity = root.join("hub.fabric-identity.key");
    let endpoint = IrohServer::bind(&bind_spec(&identity))
        .await
        .expect("bind authoritative hub");
    let ticket = endpoint.ticket();
    let db = Arc::new(Database::open(root.join("hub.redb")).expect("open authoritative hub"));
    install_work_ledger_schema(&db).expect("install hub work-ledger schema");
    let server = Arc::new(SyncServer::new(
        db.clone(),
        &endpoint,
        TenantId::from(tenant),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    Hub {
        db,
        ticket,
        stop,
        task,
    }
}

impl Hub {
    async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        within(self.task)
            .await
            .expect("authoritative hub stops cleanly");
    }
}

/// Contract 2: delivery of an inbound authoritative removal deletes a last
/// reference, while another job's reference keeps a shared hash servable.
#[tokio::test]
async fn inbound_purge_removes_last_ref_blob_and_preserves_shared_blob() {
    let root = tempfile::tempdir().expect("temporary hub and edge directory");
    let tenant = "authoritative-purge-blob-inbound";
    let hub = start_hub(root.path(), tenant).await;

    let edge_path = root.path().join("edge.redb");
    let edge_identity = root.path().join("edge.fabric-identity.key");
    let edge_node = FabricIdentity::load_or_generate(&edge_identity)
        .expect("persist edge identity")
        .node_id();
    let edge_db = Arc::new(Database::open(&edge_path).expect("open edge database"));
    install_work_ledger_schema(&edge_db).expect("install edge work-ledger schema");
    let edge_store = BlobStore::new(edge_db.clone(), policy(), edge_identity.clone());

    let last_ref_bytes = b"last reference engine-held media";
    let shared_bytes = b"shared engine-held media";
    let last_ref = edge_store
        .ingest_bytes(last_ref_bytes)
        .expect("ingest last reference");
    let shared = edge_store
        .ingest_bytes(shared_bytes)
        .expect("ingest shared reference");
    submit_blob_job(&edge_db, "last-ref", &edge_node, &last_ref);
    submit_blob_job(&edge_db, "shared-selected", &edge_node, &shared);
    submit_blob_job(&edge_db, "shared-survivor", &edge_node, &shared);

    let dial = peer_dial_spec(&hub.ticket, &edge_identity);
    let client = SyncClient::new(edge_db.clone(), &dial, TenantId::from(tenant));
    within(client.push())
        .await
        .expect("edge-originated blob references reach the authoritative hub");
    hub.db
        .execute(
            "PURGE FROM work_jobs WHERE job_id IN ('last-ref', 'shared-selected')",
            &HashMap::new(),
        )
        .expect("public PURGE accepts both selected inbound work jobs");

    within(client.pull_default())
        .await
        .expect("edge receives the authoritative removal delivery");
    assert_eq!(
        exact_hashes(&edge_store),
        BTreeSet::from([shared.as_bytes()]),
        "the last reference must no longer be served, while a live shared reference remains"
    );
    let jobs = edge_db
        .execute(
            "SELECT job_id FROM work_jobs ORDER BY job_id",
            &HashMap::new(),
        )
        .expect("read edge work rows after inbound removal")
        .rows;
    assert_eq!(jobs, vec![vec![Value::Text("shared-survivor".to_string())]]);

    within(client.shutdown()).await;
    hub.stop().await;
}

/// Contract 3: PURGE waits for already-authorized readers and writers of its
/// selected hash, then makes their retained generations permanently stale.
#[test]
fn purge_waits_for_selected_hash_guards_and_rejects_stale_provider_and_writer() {
    let root = tempfile::tempdir().expect("temporary in-flight fence directory");
    let identity = identity_file(&root);
    let db = Arc::new(Database::open(root.path().join("edge.redb")).expect("open edge database"));
    install_work_ledger_schema(&db).expect("install work-ledger schema");
    let node = node_id_of(&identity);
    let store = BlobStore::new(db.clone(), policy(), identity);
    let selected_bytes = b"write racing selected purge";
    let survivor_bytes = b"unrelated live blob";
    let later_unrelated_bytes = b"unrelated progress after selected purge";
    let selected = BlobHash::of(selected_bytes);
    assert_eq!(
        store
            .ingest_bytes(selected_bytes)
            .expect("ingest the selected blob before pausing its authorized provider"),
        selected
    );
    let survivor = store
        .ingest_bytes(survivor_bytes)
        .expect("ingest live survivor");
    submit_blob_job(&db, "selected", &node, &selected);
    submit_blob_job(&db, "survivor", &node, &survivor);

    let provider = store
        .pause_authorized_provider_stream_after_shared_hash_guard_for_test(&selected)
        .expect("start an authorized selected-hash provider stream");
    let partial_writer = store
        .pause_partial_blob_writer_after_shared_hash_guard_for_test(&selected, selected_bytes)
        .expect("start a selected-hash partial writer");
    assert!(
        provider.wait_until_shared_hash_guarded(std::time::Duration::from_secs(5)),
        "the provider must hold the selected hash's shared guard before PURGE starts"
    );
    assert!(
        partial_writer.wait_until_shared_hash_guarded(std::time::Duration::from_secs(5)),
        "the partial writer must hold the selected hash's shared guard before PURGE starts"
    );

    let purge_db = db.clone();
    let purge = std::thread::spawn(move || {
        purge_db.execute(
            "PURGE FROM work_jobs WHERE job_id = 'selected'",
            &HashMap::new(),
        )
    });
    assert!(
        db.wait_until_authoritative_purge_waits_for_blob_hash_for_test(
            &selected.as_bytes(),
            std::time::Duration::from_secs(5),
        ),
        "PURGE must register as waiting for the selected hash rather than race its active guards"
    );
    assert!(
        !purge.is_finished(),
        "without a sleep, the registered selected-hash wait proves PURGE has not returned early"
    );

    provider.abort_retaining_stale_generation_for_test();
    partial_writer.abort_retaining_stale_generation_for_test();
    purge
        .join()
        .expect("purge thread did not panic")
        .expect("PURGE returns after both selected-hash guards release");
    let payload_at_purge_return = provider.payload_bytes_emitted_for_test();

    let provider_error = provider
        .resume_from_stale_generation_for_test()
        .expect_err("the provider's retained generation cannot serve after PURGE");
    let writer_error = partial_writer
        .complete_from_stale_generation_for_test()
        .expect_err("the partial writer's retained generation cannot finalize after PURGE");
    assert!(
        provider_error.to_string().contains("purge")
            || provider_error.to_string().contains("generation"),
        "the stale provider must report the selected hash's durable removal fence: {provider_error}"
    );
    assert!(
        writer_error.to_string().contains("purge")
            || writer_error.to_string().contains("generation"),
        "the stale writer must report the selected hash's durable removal fence: {writer_error}"
    );
    assert!(
        store
            .authoritative_engine_blob_roles_for_test(&selected)
            .is_empty(),
        "no manifest, payload chunk, outboard, partial bitfield, tag, protection, or cache may resurrect"
    );
    assert_eq!(
        provider.payload_bytes_emitted_for_test(),
        payload_at_purge_return,
        "the released provider cannot emit selected payload after PURGE returns"
    );
    let later_unrelated = store
        .ingest_bytes(later_unrelated_bytes)
        .expect("an unrelated hash continues to make progress after the selected purge");
    assert_eq!(
        exact_hashes(&store),
        BTreeSet::from([survivor.as_bytes(), later_unrelated.as_bytes()]),
        "the selected hash stays absent while unrelated hashes remain live and progress normally"
    );
}

/// Contract 5: after a file-backed restart the consumer resumes its durable
/// verified prefix and does not hydrate the whole object merely to reopen it.
#[tokio::test]
async fn restart_resumes_durable_partial_without_whole_object_hydration() {
    let root = tempfile::tempdir().expect("temporary restart-resume directory");
    let holder_dir = tempfile::tempdir().expect("holder identity directory");
    let consumer_dir = tempfile::tempdir().expect("consumer identity directory");
    let holder_key = identity_file(&holder_dir);
    let consumer_key = identity_file(&consumer_dir);
    let holder_node = node_id_of(&holder_key);
    let consumer_node = node_id_of(&consumer_key);
    let content = marked("RESTART-RESUME-ENGINE-HELD", 4 * MB);
    let hash = BlobHash::of(&content);

    let holder_db = Arc::new(Database::open(root.path().join("holder.redb")).expect("open holder"));
    install_work_ledger_schema(&holder_db).expect("install holder schema");
    seed_entitlement(&holder_db, "resume", &holder_node, &consumer_node, &hash);
    let holder = BlobStore::new(holder_db, policy(), holder_key.clone());
    holder.set_test_clock(T0);
    assert_eq!(
        holder.ingest_bytes(&content).expect("ingest held object"),
        hash
    );
    holder.drop_after_bytes_for_test(3 * MB as u64);
    let endpoint = within(IrohServer::bind(&bind_spec(&holder_key)))
        .await
        .expect("bind direct holder");
    holder.serve_on(&endpoint);
    let ticket = endpoint.ticket();

    let consumer_path = root.path().join("consumer.redb");
    let consumer_db = Arc::new(Database::open(&consumer_path).expect("open consumer"));
    install_work_ledger_schema(&consumer_db).expect("install consumer schema");
    seed_entitlement(&consumer_db, "resume", &holder_node, &consumer_node, &hash);
    let mut sink = Vec::new();
    {
        let consumer = BlobStore::new(consumer_db.clone(), policy(), consumer_key.clone());
        consumer.set_test_clock(T0);
        let first = within(consumer.resolve_blob_ref(&hash, &ticket, &mut sink)).await;
        assert!(
            matches!(first, Err(ResolveError::TransferAborted)),
            "first leg: {first:?}"
        );
        assert!(
            sink.len() > 256 * 1024,
            "interruption must leave a substantial verified prefix"
        );
        assert_eq!(
            consumer.verified_partial_bytes_for_test(&hash),
            sink.len() as u64,
            "the interrupted prefix is durable engine-held resume state"
        );
    }
    let verified_prefix_len = sink.len();
    consumer_db
        .close()
        .expect("flush interrupted consumer database");
    drop(consumer_db);

    let consumer_db = Arc::new(Database::open(&consumer_path).expect("reopen consumer database"));
    let consumer = BlobStore::new(consumer_db, policy(), consumer_key);
    consumer.set_test_clock(T0);
    assert_eq!(consumer.bytes_hydrated_while_opening_for_test(), 0);
    assert_eq!(
        consumer.verified_partial_bytes_for_test(&hash),
        sink.len() as u64
    );
    let moved = within(consumer.resume_bytes_moved_for_test(&hash, &ticket, &mut sink))
        .await
        .expect("restart resumes the verified tail");
    assert_eq!(BlobHash::of(&sink), hash);
    assert!(
        moved <= (content.len() - verified_prefix_len) as u64 + 256 * 1024,
        "the restart retry moves only the missing tail, not the whole object"
    );
    endpoint.close().await;
}

/// Contract 4's control half: an unpurged backup restores a servable complete
/// object and an interrupted prefix that can still resume over the direct
/// holder-to-worker path. The engine test owns the corresponding publication
/// refusal and post-purge absence checks.
#[tokio::test]
async fn control_backup_restores_complete_serve_and_interrupted_resume_state() {
    let root = tempfile::tempdir().expect("temporary backup control directory");
    let holder_dir = tempfile::tempdir().expect("remote holder identity directory");
    let archive_dir = tempfile::tempdir().expect("backup source identity directory");
    let restored_dir = tempfile::tempdir().expect("restored server identity directory");
    let verifier_dir = tempfile::tempdir().expect("restored receiver identity directory");
    let holder_key = identity_file(&holder_dir);
    let archive_key = identity_file(&archive_dir);
    let restored_key = identity_file(&restored_dir);
    let verifier_key = identity_file(&verifier_dir);
    let holder_node = node_id_of(&holder_key);
    let archive_node = node_id_of(&archive_key);
    let restored_node = node_id_of(&restored_key);
    let verifier_node = node_id_of(&verifier_key);
    let complete_bytes = marked("BACKUP-COMPLETE-SERVE", MB);
    let partial_bytes = marked("BACKUP-PARTIAL-RESUME", 2 * MB);
    let complete = BlobHash::of(&complete_bytes);
    let partial = BlobHash::of(&partial_bytes);

    let holder_db = Arc::new(Database::open(root.path().join("holder.redb")).expect("open holder"));
    install_work_ledger_schema(&holder_db).expect("install holder schema");
    seed_entitlement(
        &holder_db,
        "archive-fetch",
        &holder_node,
        &archive_node,
        &partial,
    );
    seed_entitlement(
        &holder_db,
        "restored-resume",
        &holder_node,
        &restored_node,
        &partial,
    );
    let holder = BlobStore::new(holder_db, policy(), holder_key.clone());
    holder.set_test_clock(T0);
    holder
        .ingest_bytes(&partial_bytes)
        .expect("ingest partial source");
    holder.drop_after_bytes_for_test(3 * MB as u64 / 2);
    let holder_endpoint = within(IrohServer::bind(&bind_spec(&holder_key)))
        .await
        .expect("bind holder");
    holder.serve_on(&holder_endpoint);
    let holder_ticket = holder_endpoint.ticket();

    let archive_path = root.path().join("archive.redb");
    let backup_path = root.path().join("control.redb");
    let partial_prefix = {
        let archive_db = Arc::new(Database::open(&archive_path).expect("open archive"));
        install_work_ledger_schema(&archive_db).expect("install archive schema");
        seed_entitlement(
            &archive_db,
            "archive-fetch",
            &holder_node,
            &archive_node,
            &partial,
        );
        seed_entitlement(
            &archive_db,
            "restored-resume",
            &holder_node,
            &restored_node,
            &partial,
        );
        seed_entitlement(
            &archive_db,
            "complete-serve",
            &restored_node,
            &verifier_node,
            &complete,
        );
        let archive = BlobStore::new(archive_db.clone(), policy(), archive_key);
        archive.set_test_clock(T0);
        archive
            .ingest_bytes(&complete_bytes)
            .expect("ingest complete control object");
        let mut prefix = Vec::new();
        let interrupted =
            within(archive.resolve_blob_ref(&partial, &holder_ticket, &mut prefix)).await;
        assert!(
            matches!(interrupted, Err(ResolveError::TransferAborted)),
            "control partial: {interrupted:?}"
        );
        assert!(!prefix.is_empty() && prefix.len() < partial_bytes.len());
        archive_db
            .export_snapshot(&backup_path)
            .expect("publish unpurged control backup");
        drop(archive);
        archive_db.close().expect("flush control backup source");
        prefix
    };

    let restored_db = Arc::new(Database::open(&backup_path).expect("open restored backup"));
    let restored = BlobStore::new(restored_db.clone(), policy(), restored_key.clone());
    restored.set_test_clock(T0);
    assert_eq!(restored.bytes_hydrated_while_opening_for_test(), 0);
    assert_eq!(
        restored.bounded_raw_blob_bytes_for_test(&complete, complete_bytes.len()),
        Some(complete_bytes.clone()),
        "the control restore exposes bounded raw complete bytes to its serving surface"
    );
    assert_eq!(
        restored.verified_partial_bytes_for_test(&partial),
        partial_prefix.len() as u64
    );
    assert_eq!(
        restored.durable_next_missing_range_for_test(&partial),
        Some(partial_prefix.len() as u64..partial_bytes.len() as u64),
        "the control restore identifies the next durable missing range without hydrating the object"
    );

    let restored_endpoint = within(IrohServer::bind(&bind_spec(&restored_key)))
        .await
        .expect("bind restored complete-object server");
    restored.serve_on(&restored_endpoint);
    let verifier_db =
        Arc::new(Database::open(root.path().join("verifier.redb")).expect("open verifier"));
    install_work_ledger_schema(&verifier_db).expect("install verifier schema");
    seed_entitlement(
        &verifier_db,
        "complete-serve",
        &restored_node,
        &verifier_node,
        &complete,
    );
    let verifier = BlobStore::new(verifier_db, policy(), verifier_key);
    verifier.set_test_clock(T0);
    let mut served = Vec::new();
    within(verifier.resolve_blob_ref(&complete, &restored_endpoint.ticket(), &mut served))
        .await
        .expect("restored complete object is still servable to an entitled worker");
    assert_eq!(served, complete_bytes);

    let mut resumed = partial_prefix;
    let moved =
        within(restored.resume_bytes_moved_for_test(&partial, &holder_ticket, &mut resumed))
            .await
            .expect("restored interrupted state resumes directly from its holder");
    assert_eq!(resumed, partial_bytes);
    assert!(moved <= partial_bytes.len() as u64);
    restored_endpoint.close().await;
    holder_endpoint.close().await;
}
