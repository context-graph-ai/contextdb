//! A pull cursor is a bare `Lsn` with nothing binding it to the store that
//! issued it (`SyncClient::build` loads watermarks keyed by TENANT alone,
//! `sync_client.rs`). Pointing an edge at a different store for the same
//! tenant — an operator handing it a new endpoint ticket, or a hub that was
//! wiped and rebuilt under the same transport identity — silently skips
//! that store's history below the old cursor. The existing stale-restore
//! guard only fires when the new store's clock is BEHIND the cursor; it
//! misses the case where the new store is ahead in its own numbering but
//! still holds real history below the old number.
//!
//! Contract once fixed: a pull cursor is only ever compared against the
//! history of the store that issued it. A page from a store other than the
//! one the cursor addresses is discarded unapplied, never partially
//! trusted. The cursor and the store identity it addresses persist and
//! reload TOGETHER, so this holds across a process restart too — of either
//! side. Re-adoption after a source change is not held hostage by
//! whatever ordering position the OLD source recorded: the new source's
//! served content is authoritative, even for a key whose old-source
//! position was numerically higher than anything the new source has
//! produced yet.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_server::protocol::{MessageType, PullRequest, PullResponse, decode, encode};
use contextdb_server::subjects::pull_subject;
use contextdb_server::transport::{ClientTransport, TransportFuture};
use contextdb_server::{InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

const DDL: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

/// A bound on a hung exchange, not a timing assertion: every assertion below
/// reads state after the exchange returns.
async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), fut)
        .await
        .expect("bounded sync exchange exceeded 60s")
}

/// Pull, retrying purely on cooperative yields (no sleep) while the hub side
/// has not yet registered its handlers on the broker — the transport-level
/// "no responder" miss a freshly `tokio::spawn`ed server generation produces
/// for the brief window before its first poll runs. Bounded so a genuine
/// failure still panics loudly instead of hanging.
async fn pull_when_hub_is_live(
    client: &SyncClient,
) -> Result<contextdb_engine::sync_types::ApplyResult, contextdb_core::Error> {
    for _ in 0..10_000 {
        match client.pull_default().await {
            Err(err) if err.to_string().contains("no responder") => {
                tokio::task::yield_now().await;
                continue;
            }
            other => return other,
        }
    }
    panic!("hub transport never became reachable")
}

/// Push, retrying purely on cooperative yields (no sleep) while the hub side
/// has not yet registered its handlers on the broker — the same tolerance
/// `pull_when_hub_is_live` gives a pull.
async fn push_when_hub_is_live(
    client: &SyncClient,
) -> Result<contextdb_engine::sync_types::ApplyResult, contextdb_core::Error> {
    for _ in 0..10_000 {
        match client.push().await {
            Err(err) if err.to_string().contains("no responder") => {
                tokio::task::yield_now().await;
                continue;
            }
            other => return other,
        }
    }
    panic!("hub transport never became reachable")
}

/// Issue a raw pull request straight at the transport, bypassing
/// `SyncClient` entirely, so the served RESPONSE (including its cursor) can
/// be inspected directly instead of only the applied result. Retries
/// purely on cooperative yields (no sleep) while the hub side has not yet
/// registered its handlers, the same tolerance `pull_when_hub_is_live` gives
/// a `SyncClient`-mediated pull.
async fn raw_pull_response(
    transport: &Arc<dyn ClientTransport>,
    subject: &str,
    request: &PullRequest,
) -> PullResponse {
    let request_bytes = encode(MessageType::PullRequest, request).expect("encode pull request");
    for _ in 0..10_000 {
        match transport
            .request(subject, request_bytes.clone(), Duration::from_secs(20))
            .await
        {
            Err(err) if err.to_string().contains("no responder") => {
                tokio::task::yield_now().await;
                continue;
            }
            Err(err) => panic!("pull request failed: {err}"),
            Ok(response_bytes) => {
                let envelope = decode(&response_bytes).expect("decode response envelope");
                return rmp_serde::from_slice(&envelope.payload).expect("decode pull response");
            }
        }
    }
    panic!("hub transport never became reachable")
}

fn insert_row(db: &Database, id: i64, body: &str) {
    let mut row = p();
    row.insert("id".to_string(), Value::Int64(id));
    row.insert("body".to_string(), Value::Text(body.to_string()));
    db.execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &row)
        .unwrap_or_else(|err| panic!("insert row {id}: {err}"));
}

fn only_body(db: &Database, id: i64) -> Option<String> {
    let mut params = p();
    params.insert("id".to_string(), Value::Int64(id));
    let result = db
        .execute("SELECT body FROM notes WHERE id = $id", &params)
        .expect("notes scan");
    result.rows.into_iter().next().map(|row| match &row[0] {
        Value::Text(body) => body.clone(),
        other => panic!("expected a text body, got {other:?}"),
    })
}

fn row_exists(db: &Database, id: i64) -> bool {
    let mut params = p();
    params.insert("id".to_string(), Value::Int64(id));
    let result = db
        .execute("SELECT id FROM notes WHERE id = $id", &params)
        .expect("notes scan");
    !result.rows.is_empty()
}

struct HubGen {
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl HubGen {
    async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        self.task.await.expect("hub generation task");
    }
}

fn start_hub_as(
    broker: &InProcessBroker,
    identity: &str,
    tenant: &str,
    hub_db: Arc<Database>,
) -> HubGen {
    let server = Arc::new(SyncServer::with_transport(
        hub_db,
        broker.server_as(identity),
        TenantId::from(tenant),
        ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    HubGen { stop, task }
}

// ---------------------------------------------------------------------------
// A pull cursor names the store it addresses.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replaced_serving_store_delivers_its_history_below_the_old_cursor() {
    let broker = InProcessBroker::new();
    let tenant = "source-swap";
    let identity = "stable-transport-identity";

    let hub1_db = Arc::new(Database::open_memory());
    hub1_db.execute(DDL, &p()).expect("hub1 ddl");
    for id in 1..=30 {
        insert_row(&hub1_db, id, &format!("hub1-{id}"));
    }
    let hub1 = start_hub_as(&broker, identity, tenant, hub1_db);

    let edge_db = Arc::new(Database::open_memory());
    edge_db.execute(DDL, &p()).expect("edge ddl");
    let edge_client =
        SyncClient::with_transport(edge_db.clone(), broker.client(), TenantId::from(tenant));
    pull_when_hub_is_live(&edge_client)
        .await
        .expect("pull hub1");
    let old_cursor = edge_client.pull_watermark();
    assert!(old_cursor >= contextdb_core::Lsn(30));

    hub1.stop().await;

    // The hub is rebuilt: a brand-new store under the SAME transport
    // identity, holding a row below the old cursor.
    let hub2_db = Arc::new(Database::open_memory());
    hub2_db.execute(DDL, &p()).expect("hub2 ddl");
    insert_row(&hub2_db, 999, "only-on-hub2-below-old-cursor");
    for id in 1000..1040 {
        insert_row(&hub2_db, id, &format!("hub2-{id}"));
    }
    assert!(hub2_db.current_lsn() >= old_cursor);
    let hub2 = start_hub_as(&broker, identity, tenant, hub2_db.clone());

    pull_when_hub_is_live(&edge_client)
        .await
        .expect("pull hub2");

    assert!(
        row_exists(&edge_db, 999),
        "the replaced store's history below the old cursor must be \
         delivered, not silently skipped just because the old cursor's raw \
         number happens to be lower than the new store's current clock"
    );

    hub2.stop().await;
}

/// A single paged pull whose source changes BETWEEN two pages. Two
/// independent hubs run concurrently the whole time; a custom transport
/// redirects only the SECOND page request of the pull subject to the other
/// hub, so there is no race to win — the mid-pull swap is deterministic.
struct RedirectSecondPullPage {
    primary: Arc<dyn ClientTransport>,
    secondary: Arc<dyn ClientTransport>,
    pull_subject: String,
    pull_calls: AtomicUsize,
}

impl ClientTransport for RedirectSecondPullPage {
    fn peer_node_id(&self) -> Option<String> {
        self.primary.peer_node_id()
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        if subject == self.pull_subject {
            let n = self.pull_calls.fetch_add(1, Ordering::SeqCst);
            if n >= 1 {
                return self.secondary.request(subject, request_bytes, timeout);
            }
        }
        self.primary.request(subject, request_bytes, timeout)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_page_from_an_unexpected_store_is_discarded_not_applied() {
    let broker1 = InProcessBroker::new();
    let broker2 = InProcessBroker::new();
    let tenant = "mid-pull-source-swap";

    let hub1_db = Arc::new(Database::open_memory());
    hub1_db.execute(DDL, &p()).expect("hub1 ddl");
    for id in 1..=520 {
        insert_row(&hub1_db, id, &format!("hub1-{id}"));
    }
    let hub1 = start_hub_as(&broker1, "hub1", tenant, hub1_db);

    let hub2_db = Arc::new(Database::open_memory());
    hub2_db.execute(DDL, &p()).expect("hub2 ddl");
    for id in 10_000..10_520 {
        insert_row(&hub2_db, id, &format!("hub2-filler-{id}"));
    }
    insert_row(&hub2_db, 99_999, "only-on-hub2-page2-must-not-apply");
    let hub2 = start_hub_as(&broker2, "hub2", tenant, hub2_db);

    // Warm hub2 up on a THROWAWAY client, off a plain (non-redirecting)
    // transport, before wiring the redirect below — so the mid-pull swap's
    // page-1/page-2 call counting can never be perturbed by a retry racing
    // hub2's own registration.
    let warmup_db = Arc::new(Database::open_memory());
    warmup_db.execute(DDL, &p()).expect("warmup ddl");
    let warmup_client =
        SyncClient::with_transport(warmup_db, broker2.client(), TenantId::from(tenant));
    pull_when_hub_is_live(&warmup_client)
        .await
        .expect("warm up hub2");

    let edge_db = Arc::new(Database::open_memory());
    edge_db.execute(DDL, &p()).expect("edge ddl");
    let transport = Arc::new(RedirectSecondPullPage {
        primary: broker1.client(),
        secondary: broker2.client(),
        pull_subject: pull_subject(tenant),
        pull_calls: AtomicUsize::new(0),
    });
    let edge_client =
        SyncClient::with_transport(edge_db.clone(), transport, TenantId::from(tenant));

    let pull_result = within(edge_client.pull_default()).await;

    // Page one — served by hub1, the store the pull's cursor actually
    // addresses — must be applied regardless of what happens to a later
    // page in the same pull. An implementation that discards the WHOLE pull
    // rather than just the mismatched page must fail here. id=1 and id=400
    // sit safely inside page one regardless of exactly where the page-size
    // budget (shared with the initial DDL entry) draws the boundary.
    assert!(
        row_exists(&edge_db, 1) && row_exists(&edge_db, 400),
        "page one, served by the expected store, must land even though a \
         later page in the same pull is discarded: pull_result={pull_result:?}"
    );
    // Hub1's own tail rows were never fetched — the redirect diverted that
    // request to hub2 before hub1 could serve it — so the cursor must not
    // have been advanced past page one's boundary onto hub1's own remainder
    // (id=520, hub1's last row, sits safely beyond any plausible page-one
    // boundary near the page-size budget).
    assert!(
        !row_exists(&edge_db, 520),
        "the cursor must not be advanced past page one's source-consistent \
         position: hub1's own remaining rows, never fetched because the \
         redirect intercepted that request, must not appear as if they had \
         landed: pull_result={pull_result:?}"
    );
    assert!(
        !row_exists(&edge_db, 99_999),
        "a page served by a store other than the one the pull's cursor \
         addresses must be discarded, not applied — a row unique to the \
         unexpected second store's page must never land locally: \
         pull_result={pull_result:?}"
    );
    assert!(
        pull_result.is_ok(),
        "a source-mismatched page is handled by discarding it, not by \
         surfacing an error that aborts the whole pull: {pull_result:?}"
    );
    assert!(
        edge_client.pages_discarded_for_source_mismatch() >= 1,
        "the pull report must surface that a served page was discarded for \
         a source mismatch, not silently drop it"
    );

    hub1.stop().await;
    hub2.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cursor_and_its_store_survive_a_restart_together() {
    let broker = InProcessBroker::new();
    let tenant = "cursor-source-restart";
    let identity = "restart-hub-identity";

    let hub1_db = Arc::new(Database::open_memory());
    hub1_db.execute(DDL, &p()).expect("hub1 ddl");
    for id in 1..=30 {
        insert_row(&hub1_db, id, &format!("hub1-{id}"));
    }
    let hub1 = start_hub_as(&broker, identity, tenant, hub1_db);

    let dir = tempfile::TempDir::new().expect("tempdir");
    let edge_path = dir.path().join("edge.db");
    let old_cursor = {
        let edge_db = Arc::new(Database::open(&edge_path).expect("open edge"));
        edge_db.execute(DDL, &p()).expect("edge ddl");
        let edge_client =
            SyncClient::with_transport(edge_db.clone(), broker.client(), TenantId::from(tenant));
        pull_when_hub_is_live(&edge_client)
            .await
            .expect("pull hub1");
        edge_client.pull_watermark()
    }; // the edge handle closes here

    hub1.stop().await;

    // The hub is rebuilt: a brand-new store under the SAME transport
    // identity, holding a row below the old cursor.
    let hub2_db = Arc::new(Database::open_memory());
    hub2_db.execute(DDL, &p()).expect("hub2 ddl");
    insert_row(&hub2_db, 999, "only-on-hub2-below-old-cursor");
    for id in 1000..1040 {
        insert_row(&hub2_db, id, &format!("hub2-{id}"));
    }
    let hub2 = start_hub_as(&broker, identity, tenant, hub2_db);

    // The EDGE restarts too, reopening the SAME file-backed database — its
    // persisted cursor must reload bound to the store it addresses, not as
    // a bare number.
    let edge_db2 = Arc::new(Database::open(&edge_path).expect("reopen edge"));
    let edge_client2 =
        SyncClient::with_transport(edge_db2.clone(), broker.client(), TenantId::from(tenant));
    assert_eq!(
        edge_client2.pull_watermark(),
        old_cursor,
        "fixture: the reopened edge must reload its persisted cursor"
    );

    pull_when_hub_is_live(&edge_client2)
        .await
        .expect("pull hub2 after restart");

    assert!(
        row_exists(&edge_db2, 999),
        "the reopened edge must still detect the store swap and deliver \
         the new store's history below its old cursor — the property must \
         survive a restart of the edge itself, not just a live process"
    );

    hub2.stop().await;
}

/// An edge repointed at a rebuilt hub must converge to the rebuilt hub's
/// state, INCLUDING a key whose OLD-source recorded position was numerically
/// higher than anything the new source has produced yet. Re-adoption after a
/// source change must not let the old source's provenance arbitrate against
/// the new source's served content.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_edge_repointed_at_a_rebuilt_hub_converges_including_numerically_lower_arrivals() {
    let broker = InProcessBroker::new();
    let tenant = "source-readoption";
    let identity = "readoption-hub-identity";

    let hub1_db = Arc::new(Database::open_memory());
    hub1_db.execute(DDL, &p()).expect("hub1 ddl");
    // Run store1's clock up before authoring the shared key, so the edge's
    // recorded provenance for it is numerically high.
    for id in 0..300 {
        insert_row(&hub1_db, 2_000 + id, "hub1-filler");
    }
    insert_row(&hub1_db, 42, "store1-value");
    let hub1 = start_hub_as(&broker, identity, tenant, hub1_db);

    let edge_db = Arc::new(Database::open_memory());
    edge_db.execute(DDL, &p()).expect("edge ddl");
    let edge_client =
        SyncClient::with_transport(edge_db.clone(), broker.client(), TenantId::from(tenant));
    // A client's own default policy is ServerWins (EdgeWins after the pull
    // remap), which always applies the incoming row unconditionally — that
    // would pass this test even with no arrival comparison at all. Set
    // LatestWins explicitly so this test actually exercises the arrival-based
    // arbitration its own name and doc comment claim to cover.
    edge_client.set_default_conflict_policy(ConflictPolicy::LatestWins);
    pull_when_hub_is_live(&edge_client)
        .await
        .expect("pull store1");
    assert_eq!(
        only_body(&edge_db, 42).as_deref(),
        Some("store1-value"),
        "fixture: the edge must hold store1's value before the swap"
    );
    let old_cursor = edge_client.pull_watermark();

    hub1.stop().await;

    // The hub is rebuilt: a brand-new store under the SAME transport
    // identity. Key 42 is authored EARLY in store2's own history, so its LSN
    // there is numerically LOWER than the edge's recorded provenance from
    // store1 — but enough filler follows it that store2's overall clock
    // still ends up AHEAD of the edge's old cursor, so the existing
    // clock-behind stale-restore guard does not fire and this is a genuine
    // test of source-identity re-adoption, not of that guard.
    let hub2_db = Arc::new(Database::open_memory());
    hub2_db.execute(DDL, &p()).expect("hub2 ddl");
    insert_row(&hub2_db, 42, "store2-value");
    for id in 0..400 {
        insert_row(&hub2_db, 3_000 + id, "hub2-filler");
    }
    assert!(
        hub2_db.current_lsn() > old_cursor,
        "fixture: store2's overall clock must be ahead of the edge's old \
         cursor, so only the numerically-lower per-key arrival is in play"
    );
    let hub2 = start_hub_as(&broker, identity, tenant, hub2_db);

    pull_when_hub_is_live(&edge_client)
        .await
        .expect("pull store2");

    assert_eq!(
        only_body(&edge_db, 42).as_deref(),
        Some("store2-value"),
        "an edge repointed at a rebuilt hub must converge to the rebuilt \
         hub's state, including a key whose old-source recorded arrival was \
         numerically HIGHER than anything the new source has produced yet"
    );

    hub2.stop().await;
}

// ---------------------------------------------------------------------------
// Standing guard for what is NOT broken.
// ---------------------------------------------------------------------------

/// A served page that carries schema but no data LSN takes its RESPONSE
/// cursor from the DDL LSN, never from a `current_lsn()` read taken AFTER the
/// changeset snapshot — every producer keeps `ddl_lsn` at the same
/// cardinality as `ddl`, so the post-snapshot fallback should never fire.
/// Driven through a real in-process hub (`handle_pull`, not a bare
/// `Database::changes_since` call) so a regression that reintroduces the
/// post-snapshot-read window flips the actual served response red, not just
/// an engine-level computation nothing ever serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_schema_carrying_page_takes_its_cursor_from_the_schema_position() {
    let broker = InProcessBroker::new();
    let tenant = "ddl-only-serve-cursor";

    let hub_db = Arc::new(Database::open_memory());
    hub_db
        .execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
            &p(),
        )
        .expect("ddl");
    let expected_cursor = hub_db.changes_since(contextdb_core::Lsn(0)).max_lsn();
    assert!(
        expected_cursor.is_some(),
        "fixture: a DDL-only changeset must carry a max_lsn derived from the \
         DDL entry itself"
    );

    let hub = start_hub_as(&broker, "ddl-cursor-hub", tenant, hub_db.clone());
    let transport = broker.client();
    let request = PullRequest {
        since_lsn: contextdb_core::Lsn(0),
        max_entries: None,
    };
    let response = raw_pull_response(&transport, &pull_subject(tenant), &request).await;

    assert_eq!(
        response.cursor, expected_cursor,
        "a served DDL-only page's RESPONSE cursor must come from the schema \
         position, not a post-snapshot current_lsn() read"
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// Re-adoption must cover rows that reach the rebuilt hub BY SYNC, not just
// ones the hub authored directly.
// ---------------------------------------------------------------------------

/// The standard hub-rebuild recovery flow: after a hub is wiped and rebuilt,
/// every edge re-pushes its still-current state to the fresh hub — the key
/// under test never touches the hub's own local writer at all. The row
/// therefore reaches the rebuilt hub BY SYNC (from edge X), so the rebuilt
/// hub stamps it with ITS OWN freshly-reset (small) arrival counter, never
/// `None`. A repointed edge (Y) holding an old-source sidecar recorded under
/// the extinct hub's (numerically much higher) provenance must still
/// converge to it: the numerically lower carried arrival must not be
/// mistaken for a stale echo of the old source's higher position — the two
/// numbers come from unrelated, independently-reset counters and are never
/// comparable across a source change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_edge_repointed_at_a_rebuilt_hub_converges_to_a_value_that_reached_it_by_sync() {
    let broker = InProcessBroker::new();
    let tenant = "source-readoption-via-sync";
    let identity = "readoption-sync-hub-identity";

    let hub1_db = Arc::new(Database::open_memory());
    hub1_db.execute(DDL, &p()).expect("hub1 ddl");
    // Run store1's clock up before authoring the shared key, so edge Y's
    // recorded sidecar for it is numerically high.
    for id in 0..300 {
        insert_row(&hub1_db, 2_000 + id, "hub1-filler");
    }
    insert_row(&hub1_db, 42, "hub1-value");
    let hub1 = start_hub_as(&broker, identity, tenant, hub1_db);

    let edge_y_db = Arc::new(Database::open_memory());
    edge_y_db.execute(DDL, &p()).expect("edge y ddl");
    let edge_y =
        SyncClient::with_transport(edge_y_db.clone(), broker.client(), TenantId::from(tenant));
    // A client's own default policy is ServerWins (EdgeWins after the pull
    // remap), which always applies the incoming row unconditionally — that
    // would pass or fail this test for the wrong reason. Set LatestWins
    // explicitly so this exercises real arrival-based arbitration.
    edge_y.set_default_conflict_policy(ConflictPolicy::LatestWins);
    pull_when_hub_is_live(&edge_y).await.expect("pull hub1");
    assert_eq!(
        only_body(&edge_y_db, 42).as_deref(),
        Some("hub1-value"),
        "fixture: edge Y must hold hub1's value before the rebuild"
    );
    let old_cursor = edge_y.pull_watermark();

    hub1.stop().await;

    // The hub is rebuilt: a fresh store under the SAME transport identity,
    // starting from nothing.
    let hub2_db = Arc::new(Database::open_memory());
    hub2_db.execute(DDL, &p()).expect("hub2 ddl");
    let hub2 = start_hub_as(&broker, identity, tenant, hub2_db.clone());

    // A DIFFERENT edge (X) re-pushes its still-current value for the SAME
    // key — the row reaches hub2 BY SYNC, so hub2 stamps it with its own
    // fresh arrival rather than serving it with `None`.
    let edge_x_db = Arc::new(Database::open_memory());
    edge_x_db.execute(DDL, &p()).expect("edge x ddl");
    insert_row(&edge_x_db, 42, "edgeX-newer-value");
    let edge_x =
        SyncClient::with_transport(edge_x_db.clone(), broker.client(), TenantId::from(tenant));
    push_when_hub_is_live(&edge_x)
        .await
        .expect("edge x push to hub2");
    assert_eq!(
        only_body(&hub2_db, 42).as_deref(),
        Some("edgeX-newer-value"),
        "fixture: hub2 must hold edge X's pushed value before edge Y re-pulls"
    );
    assert!(
        hub2_db.current_lsn() < old_cursor,
        "fixture: hub2's own arrival counter must be numerically LOWER than \
         edge Y's old-source sidecar, or this test does not exercise the \
         numerically-lower-carried-arrival case"
    );

    pull_when_hub_is_live(&edge_y).await.expect("pull hub2");

    assert_eq!(
        only_body(&edge_y_db, 42).as_deref(),
        Some("edgeX-newer-value"),
        "an edge repointed at a rebuilt hub must converge to a value that \
         reached the hub BY SYNC from another edge, not just one the hub \
         authored directly — the served row's numerically lower arrival \
         (stamped by the rebuilt hub's own fresh counter) must not be \
         mistaken for a stale echo of the old source's higher provenance"
    );

    hub2.stop().await;
}
/// HARD CONSTRAINT on the fix above: a value an edge authored locally and has
/// never pushed anywhere must survive a source re-adoption pull, even when
/// the rebuilt hub serves a DIFFERENT value for the same key (reached by sync
/// from another edge, the same shape as the scenario above). A local write
/// clears the row's sync-provenance sidecar — nothing outside
/// `apply_synced_changes` ever populates it — so re-adoption's served-wins
/// rule, which acts only on rows whose sidecar shows they are unmodified
/// since their last sync, must never reach this row at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_locally_authored_never_pushed_value_survives_source_readoption() {
    let broker = InProcessBroker::new();
    let tenant = "readoption-preserves-local-edit";
    let identity = "readoption-local-edit-hub-identity";

    let hub1_db = Arc::new(Database::open_memory());
    hub1_db.execute(DDL, &p()).expect("hub1 ddl");
    for id in 0..300 {
        insert_row(&hub1_db, 2_000 + id, "hub1-filler");
    }
    insert_row(&hub1_db, 42, "hub1-value");
    let hub1 = start_hub_as(&broker, identity, tenant, hub1_db);

    let edge_y_db = Arc::new(Database::open_memory());
    edge_y_db.execute(DDL, &p()).expect("edge y ddl");
    let edge_y =
        SyncClient::with_transport(edge_y_db.clone(), broker.client(), TenantId::from(tenant));
    // A client's own default policy is ServerWins (EdgeWins after the pull
    // remap), which always applies the incoming row unconditionally — that
    // would pass or fail this test for the wrong reason. Set LatestWins
    // explicitly so this exercises real arrival-based arbitration.
    edge_y.set_default_conflict_policy(ConflictPolicy::LatestWins);
    pull_when_hub_is_live(&edge_y).await.expect("pull hub1");
    assert_eq!(
        only_body(&edge_y_db, 42).as_deref(),
        Some("hub1-value"),
        "fixture: edge Y must hold hub1's value before the rebuild"
    );

    hub1.stop().await;

    let hub2_db = Arc::new(Database::open_memory());
    hub2_db.execute(DDL, &p()).expect("hub2 ddl");
    let hub2 = start_hub_as(&broker, identity, tenant, hub2_db.clone());

    // hub2 also holds a value for the same key, reached BY SYNC from another
    // edge — the value edge Y's local edit below must still not lose to.
    let edge_x_db = Arc::new(Database::open_memory());
    edge_x_db.execute(DDL, &p()).expect("edge x ddl");
    insert_row(&edge_x_db, 42, "hub2-inherited-value");
    let edge_x =
        SyncClient::with_transport(edge_x_db.clone(), broker.client(), TenantId::from(tenant));
    push_when_hub_is_live(&edge_x)
        .await
        .expect("edge x push to hub2");

    // Edge Y makes a LOCAL edit — never pushed anywhere — after its earlier
    // sync from hub1. This is a plain local write, not a sync apply, so it
    // clears edge Y's own sidecar for row 42.
    let mut edit = p();
    edit.insert("id".to_string(), Value::Int64(42));
    edit.insert(
        "body".to_string(),
        Value::Text("edgeY-local-never-pushed-value".to_string()),
    );
    edge_y_db
        .execute("UPDATE notes SET body = $body WHERE id = $id", &edit)
        .expect("edge y local edit");

    pull_when_hub_is_live(&edge_y).await.expect("pull hub2");

    assert_eq!(
        only_body(&edge_y_db, 42).as_deref(),
        Some("edgeY-local-never-pushed-value"),
        "a locally-authored value never pushed anywhere must survive a \
         source re-adoption pull, even though the rebuilt hub serves a \
         different value for the same key, reached by sync from another edge"
    );

    hub2.stop().await;
}

// ---------------------------------------------------------------------------
// Fail-closed on missing source identity when cursor is bound
// ---------------------------------------------------------------------------

/// A custom transport that blanks the source field in a pull response to
/// simulate a misbehaving or outdated hub that omits the source identity.
struct BlankSourceInPullResponse {
    inner: Arc<dyn ClientTransport>,
    pull_subject: String,
}

impl ClientTransport for BlankSourceInPullResponse {
    fn peer_node_id(&self) -> Option<String> {
        self.inner.peer_node_id()
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        if subject != self.pull_subject {
            return self.inner.request(subject, request_bytes, timeout);
        }
        let inner = self.inner.clone();
        let subject = subject.to_string();
        Box::pin(async move {
            let response_bytes = inner.request(&subject, request_bytes, timeout).await?;
            let envelope = decode(&response_bytes).expect("decode response envelope");
            let mut pull_response: PullResponse =
                rmp_serde::from_slice(&envelope.payload).expect("decode pull response");
            // Blank the source field to simulate a hub that omits source identity
            pull_response.source = None;
            // Re-encode with the blanked source
            let blanked_bytes =
                encode(MessageType::PullResponse, &pull_response).expect("re-encode pull response");
            Ok(blanked_bytes)
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bound_cursor_pull_refuses_when_response_carries_no_source_identity() {
    let broker = InProcessBroker::new();
    let tenant = "bound-cursor-missing-source";
    let identity = "stable-identity";

    let hub_db = Arc::new(Database::open_memory());
    hub_db.execute(DDL, &p()).expect("hub ddl");
    for id in 1..=30 {
        insert_row(&hub_db, id, &format!("hub-{id}"));
    }
    let hub = start_hub_as(&broker, identity, tenant, hub_db);

    let dir = tempfile::TempDir::new().expect("tempdir");
    let edge_path = dir.path().join("edge.db");
    let initial_cursor = {
        let edge_db = Arc::new(Database::open(&edge_path).expect("open edge"));
        edge_db.execute(DDL, &p()).expect("edge ddl");
        let edge_client =
            SyncClient::with_transport(edge_db.clone(), broker.client(), TenantId::from(tenant));

        // First pull establishes a source-bound cursor
        pull_when_hub_is_live(&edge_client)
            .await
            .expect("initial pull succeeds");
        edge_client.pull_watermark()
    }; // the edge handle closes here

    assert!(
        initial_cursor > contextdb_core::Lsn(0),
        "cursor should be bound"
    );

    // Reopen the database to reload the persisted cursor, then use a transport
    // that blanks the source in responses. The new client will load the cursor
    // from the database, making it bound to the original hub's source.
    let blanking_transport = Arc::new(BlankSourceInPullResponse {
        inner: broker.client(),
        pull_subject: pull_subject(tenant),
    });
    let edge_db2 = Arc::new(Database::open(&edge_path).expect("reopen edge"));
    let edge_client_blanked =
        SyncClient::with_transport(edge_db2.clone(), blanking_transport, TenantId::from(tenant));

    // Verify the new client loaded the same cursor
    assert_eq!(
        edge_client_blanked.pull_watermark(),
        initial_cursor,
        "new client should reload cursor from database"
    );

    // Pull with blanked source should fail with a SyncError naming the missing source
    let pull_result = within(edge_client_blanked.pull_default()).await;

    assert!(
        pull_result.is_err(),
        "pull with bound cursor but missing source identity must fail: {pull_result:?}"
    );
    let err_msg = pull_result.unwrap_err().to_string();
    assert!(
        err_msg.contains("missing source identity") || err_msg.contains("source"),
        "error must name the missing source issue: {err_msg}"
    );

    hub.stop().await;
}
