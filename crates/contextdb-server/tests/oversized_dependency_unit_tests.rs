//! A dependency-complete memory larger than one Iroh frame still arrives whole.
//!
//! The production ticketed-Iroh path is intentional here.  This test does not
//! use the blob plane, an in-process broker, or a transport double: the encoded
//! source batch must exceed the real 64-MiB framed-request ceiling before it is
//! offered to the hub.

use contextdb_core::{Lsn, TenantId, Value, VersionedRow};
use contextdb_engine::Database;
use contextdb_server::protocol::{
    MessageType, PushRequest, encode, wire_changeset_with_arrivals_lineages_and_ddl_provenance,
};
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{
    FabricIdentity, SyncClient, SyncServer, TransferCounters, TransferDirection, TransferPlane,
    TransferReceipt, peer_dial_spec,
};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

const TENANT: &str = "oversized-dependency-unit";
const IROH_FRAME_CEILING_BYTES: usize = 64 * 1024 * 1024;

fn bind_spec(identity_path: &Path) -> String {
    format!("iroh:?identity={}", identity_path.display())
}

fn declare_tables(db: &Database) {
    db.execute(
        "CREATE TABLE parents (id UUID PRIMARY KEY, body TEXT) \
         SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("declare parent table");
    db.execute(
        "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id), body TEXT) \
         SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("declare child table");
}

fn insert_parent(db: &Database, id: Uuid, body: String) {
    db.execute(
        "INSERT INTO parents (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body)),
        ]),
    )
    .expect("insert parent");
}

fn update_parent(db: &Database, id: Uuid, body: String) {
    db.execute(
        "UPDATE parents SET body = $body WHERE id = $id",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body)),
        ]),
    )
    .expect("update parent");
}

fn insert_child(db: &Database, id: Uuid, parent_id: Uuid) {
    db.execute(
        "INSERT INTO children (id, parent_id, body) VALUES ($id, $parent_id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("parent_id".to_string(), Value::Uuid(parent_id)),
            ("body".to_string(), Value::Text("outcome".to_string())),
        ]),
    )
    .expect("insert child");
}

/// Sum the outbound sync-plane counters across every peer receipt. There is
/// exactly one authenticated peer (the hub) in this test, but summing rather
/// than indexing by peer id keeps the assertion robust to that detail.
fn sync_sent_counters(receipts: &[TransferReceipt]) -> TransferCounters {
    receipts
        .iter()
        .filter(|receipt| {
            receipt.plane == TransferPlane::Sync && receipt.direction == TransferDirection::Sent
        })
        .fold(TransferCounters::default(), |mut acc, receipt| {
            acc.items += receipt.counters.items;
            acc.payload_bytes += receipt.counters.payload_bytes;
            acc
        })
}

fn row(db: &Database, table: &str, id: Uuid) -> Option<VersionedRow> {
    db.point_lookup(table, "id", &Value::Uuid(id), db.snapshot())
        .expect("point lookup")
}

struct Hub {
    db: Arc<Database>,
    endpoint: IrohServer,
    ticket: String,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

async fn start_hub(root: &Path) -> Hub {
    let identity_path = root.join("hub.db.fabric-identity.key");
    let endpoint = IrohServer::bind(&bind_spec(&identity_path))
        .await
        .expect("bind authenticated Iroh hub");
    let ticket = endpoint.ticket();
    let db = Arc::new(Database::open(root.join("hub.db")).expect("open hub database"));
    declare_tables(&db);
    let server = Arc::new(SyncServer::new(
        db.clone(),
        &endpoint,
        TenantId::from(TENANT),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    Hub {
        db,
        endpoint,
        ticket,
        stop,
        task,
    }
}

impl Hub {
    async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        // No test-authored stopwatch: the hub's own run loop reacts to the
        // `stop` flag and exits its task; a task that never exits is a real
        // hang, and nextest's own harness slow-timeout is the liveness bound
        // for that, not an assertion this test makes.
        self.task
            .await
            .expect("authenticated Iroh hub run task must not panic");
        self.endpoint.close().await;
    }
}

/// The source unit contains a parent revision whose real encoded PushRequest
/// exceeds Iroh's 64-MiB ceiling, plus its outcome child. The source first
/// bootstraps its DDL, then updates its pending parent before the first data
/// sync, so the child-before-parent ordering defect cannot mask the real
/// frame-boundary transport path.
#[tokio::test]
async fn oversized_dependency_unit_uses_the_authenticated_sync_fallback_and_commits_once() {
    let root = tempfile::tempdir().expect("tempdir");
    let hub = start_hub(root.path()).await;
    let edge_path = root.path().join("edge.db");
    let identity_path = root.path().join("edge.db.fabric-identity.key");
    let edge_node_id = FabricIdentity::load_or_generate(&identity_path)
        .expect("persist edge identity")
        .node_id();
    let dial_spec = peer_dial_spec(&hub.ticket, &identity_path);
    let edge = Arc::new(Database::open(&edge_path).expect("open edge database"));
    declare_tables(&edge);
    let client = SyncClient::new(edge.clone(), &dial_spec, TenantId::from(TENANT));

    client
        .push()
        .await
        .expect("bootstrap DDL reaches the authenticated hub before the oversized unit");
    let parent_id = Uuid::new_v4();
    insert_parent(&edge, parent_id, "decision-before-evidence".to_string());
    let source_before = client.push_watermark();
    let hub_before = hub.db.current_lsn();
    let incarnation = edge
        .sync_incarnation(&TenantId::from(TENANT))
        .expect("read durable edge incarnation");
    let hub_edge_watermark_before = hub
        .db
        .persisted_sync_applied_push_watermark_for_node_incarnation(
            &TenantId::from(TENANT),
            &edge_node_id,
            incarnation,
        )
        .expect("read hub per-edge watermark");

    let child_id = Uuid::new_v4();
    insert_child(&edge, child_id, parent_id);
    update_parent(&edge, parent_id, "x".repeat(IROH_FRAME_CEILING_BYTES));
    let source_final_lsn = edge.current_lsn();

    // Captured before the push: `changes`/`arrivals` reflect exactly the
    // state the client's own pre-send computation would see. A row's arrival
    // annotation can itself change once the push it is part of has been
    // acknowledged (the row's own sync-source position advances), so reading
    // this after the push -- unlike the lineage/DDL provenance below, which
    // this same push durably and immutably binds -- would silently rebuild
    // the reference from a changed fact, not the one the client actually
    // encoded.
    let (changes, arrivals) = edge.changes_since_with_arrivals(source_before);

    // Production observation seams (test-seam builds only), read before the
    // push so the verdict below is a delta, not an absolute reading that
    // could already be nonzero from the bootstrap push above.
    let controller = hub.endpoint.large_request_test_controller();
    let client_sync_sent_before = sync_sent_counters(&client.transfer_receipts());

    // No test-authored stopwatch: production already bounds this exchange
    // with its own typed deadlines (the sync client's PUSH_REQUEST_TIMEOUT
    // and the transport's per-fragment idle/receipt ceilings), so a genuinely
    // stuck transport surfaces as `pushed.is_err()` below or as nextest's own
    // harness slow-timeout -- never as a test-authored `tokio::time::timeout`.
    let pushed = client.push().await;
    assert!(
        pushed.is_ok(),
        "an oversized dependency unit must use authenticated ordinary-sync fallback instead of failing at Iroh's frame ceiling: {pushed:?}"
    );

    // Build the exact reference encoding the client just sent, using the
    // real production encoder over the real database-computed row-author
    // lineages and DDL provenance -- the same inputs
    // `wire_changeset_with_arrivals_lineages_and_ddl_provenance` combines in
    // `SyncClient::push` (sync_client.rs). This MUST run only after the push
    // above has completed: computing a row's creation lineage durably binds
    // and signs it the first time it is computed, and that binding is
    // immutable thereafter (`bind_unbound_creation_lineage_with_state`,
    // database.rs). Computing it here first reads back the exact signature
    // the client's own send already bound and persisted; the dummy signer
    // below exists only to satisfy the function signature and is never
    // actually invoked, since by now every row's lineage is already signed.
    //
    // The parent update and the child insert are two separate local commits
    // (two separate LSNs), so the ordinary acceptance-stamped splitter would
    // cut them into two requests -- but this fixture's whole point is that
    // the child's dependency on its parent makes the two into one
    // dependency-complete unit, and `SyncClient::push` sends a
    // dependency-complete unit as a single combined request tagged
    // `MessageType::DependencyCompletePushRequest` (a distinct wire message
    // type from an ordinary `PushRequest`, encoded by its own variant name),
    // bypassing the acceptance-stamped splitter entirely
    // (`if unit.dependency_complete { vec![(unit.changes, true)] }` and the
    // message-type selection below it, sync_client.rs:~1330-1448). The
    // reference below matches both: one request over the whole changeset,
    // tagged the same dependency-complete message type.
    let dummy_lineage_signer: &dyn Fn(&[u8]) -> contextdb_core::Result<Vec<u8>> =
        &|_bytes: &[u8]| {
            panic!(
                "unreachable: every row's creation lineage is already durably signed by the \
                 preceding client.push()"
            )
        };
    let (lineages, ddl_provenance) = edge
        .outbound_row_lineages_and_ddl_provenance_for_test(
            &changes,
            &TenantId::from(TENANT),
            &edge_node_id,
            incarnation,
            dummy_lineage_signer,
        )
        .expect(
            "read back the same outbound row lineages and DDL provenance the sync client just attached",
        );
    // The client's transfer-receipt payload counter is a documented floor
    // that counts only each row's own serialized `values` -- table names,
    // natural keys, arrival/lineage/DDL provenance, and all protocol/message
    // framing are deliberately excluded (`row_payload_bytes`,
    // transfer_receipts.rs:10-13 and protocol.rs:1216-1226). Rather than
    // reproduce that private msgpack formula here -- which would silently
    // drift from production the moment it changes -- this bounds the
    // counter's growth using only values the test already has on hand: the
    // fixture wrote the parent's `body` to exactly `IROH_FRAME_CEILING_BYTES`
    // bytes (asserted below against the hub's retained parent revision), and
    // a msgpack encoding of that body cannot be smaller than its raw byte
    // length, so the counter must grow by at least that many bytes; and
    // because the counter excludes lineage, DDL and protocol overhead that
    // the full encoded push includes, it must grow strictly less than
    // `largest_request_len` (computed just below).
    let largest_request = encode(
        MessageType::DependencyCompletePushRequest,
        &PushRequest {
            changeset: wire_changeset_with_arrivals_lineages_and_ddl_provenance(
                changes,
                &arrivals,
                &lineages,
                ddl_provenance,
            ),
            incarnation,
        },
    )
    .expect("encode the exact production PushRequest shape");
    assert!(
        largest_request.len() > IROH_FRAME_CEILING_BYTES,
        "the test must drive an actual encoded dependency batch above Iroh's frame ceiling; got {} bytes",
        largest_request.len()
    );
    let largest_request_len = largest_request.len() as u64;
    drop(largest_request);

    // The fallback path moved the oversized unit as one set of fragments and
    // dispatched it to the sync handler exactly once ("commits once" at the
    // transport boundary) -- state and counters, not elapsed time.
    let observations = controller.observations_for_test();
    let unit_digests: BTreeSet<[u8; 32]> = observations
        .accepted_fragment_sequences
        .iter()
        .map(|fragment| fragment.unit_digest)
        .collect();
    assert_eq!(
        unit_digests.len(),
        1,
        "the oversized push must be staged as exactly one dependency-unit digest, got {unit_digests:?}"
    );
    let total_fragments: BTreeSet<u32> = observations
        .accepted_fragment_sequences
        .iter()
        .map(|fragment| fragment.total_fragments)
        .collect();
    assert_eq!(
        total_fragments.len(),
        1,
        "every accepted fragment must agree on the unit's total fragment count, got {total_fragments:?}"
    );
    let expected_total_fragments = *total_fragments
        .iter()
        .next()
        .expect("at least one fragment was accepted for the oversized push");
    let accepted_sequences: BTreeSet<u32> = observations
        .accepted_fragment_sequences
        .iter()
        .map(|fragment| fragment.sequence)
        .collect();
    assert_eq!(
        accepted_sequences,
        (0..expected_total_fragments).collect::<BTreeSet<u32>>(),
        "every fragment 0..total_fragments must have arrived exactly once"
    );
    assert_eq!(
        observations.accepted_fragment_sequences.len(),
        expected_total_fragments as usize,
        "no fragment sequence must have been accepted more than once"
    );
    for fragment in &observations.accepted_fragment_sequences {
        assert_eq!(
            fragment.total_bytes, largest_request_len,
            "every accepted fragment must report the same total unit byte count as the encoded push"
        );
    }
    assert_eq!(
        observations.completed_handler_dispatches, 1,
        "the completed oversized request must be dispatched to the sync handler exactly once"
    );
    let stage_snapshots_after = controller
        .stage_snapshots_for_test()
        .expect("read the hub's durable large-request stage");
    assert!(
        stage_snapshots_after.is_empty(),
        "the durable oversized-request stage must be consumed and reclaimed once the push returns, got {stage_snapshots_after:?}"
    );
    let client_sync_sent_after = sync_sent_counters(&client.transfer_receipts());
    assert!(
        client_sync_sent_after.payload_bytes
            >= client_sync_sent_before.payload_bytes + IROH_FRAME_CEILING_BYTES as u64,
        "the client's outbound sync payload counter must grow by at least the fixture's own \
         oversized parent body length (IROH_FRAME_CEILING_BYTES bytes): before={client_sync_sent_before:?}, \
         after={client_sync_sent_after:?}"
    );
    assert!(
        client_sync_sent_after.payload_bytes
            < client_sync_sent_before.payload_bytes + largest_request_len,
        "the client's outbound sync payload counter is a documented floor that excludes lineage, \
         DDL and protocol overhead (transfer_receipts.rs:10-13), so it must grow strictly less \
         than the full encoded push length: before={client_sync_sent_before:?}, after={client_sync_sent_after:?}, \
         largest_request_len={largest_request_len}"
    );
    assert!(
        client_sync_sent_after.items > client_sync_sent_before.items,
        "the client's outbound sync item counter must grow: before={client_sync_sent_before:?}, after={client_sync_sent_after:?}"
    );

    let hub_parent = row(&hub.db, "parents", parent_id)
        .expect("the hub receives the final parent with its outcome child");
    let hub_child = row(&hub.db, "children", child_id)
        .expect("the hub never exposes the outcome child without its final parent");
    assert!(
        matches!(
            hub_parent.values.get("body"),
            Some(Value::Text(body)) if body.len() == IROH_FRAME_CEILING_BYTES
        ),
        "the hub retains the final oversized parent revision"
    );
    assert_eq!(
        hub_child.values.get("parent_id"),
        Some(&Value::Uuid(parent_id)),
        "the outcome retains its declared parent reference"
    );

    let dependency_lsns = hub
        .db
        .changes_since(hub_before)
        .rows
        .iter()
        .filter(|change| {
            (change.table == "parents"
                && change
                    .natural_key
                    .key_values()
                    .contains(&Value::Uuid(parent_id)))
                || (change.table == "children"
                    && change
                        .natural_key
                        .key_values()
                        .contains(&Value::Uuid(child_id)))
        })
        .map(|change| change.lsn)
        .collect::<BTreeSet<Lsn>>();
    assert_eq!(
        dependency_lsns.len(),
        1,
        "the complete dependency unit receives one hub acceptance position"
    );
    assert_eq!(
        client.push_watermark(),
        source_final_lsn,
        "source progress advances only after the complete oversized unit is accepted"
    );
    assert_eq!(
        hub.db
            .persisted_sync_applied_push_watermark_for_node_incarnation(
                &TenantId::from(TENANT),
                &edge_node_id,
                incarnation,
            )
            .expect("read final hub per-edge watermark"),
        Some(source_final_lsn),
        "the hub advances the per-edge receipt only for the completed unit"
    );
    assert_ne!(
        hub_edge_watermark_before,
        Some(source_final_lsn),
        "fixture: the source final LSN was not already acknowledged before the oversized unit"
    );

    client.shutdown().await;
    hub.stop().await;
}
