//! An offline descendant is refused before authoritative purge delivery removes it.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::database::AuthoritativePurgeRootClassification;
use contextdb_engine::sync_types::{ConflictPolicy, NaturalKey};
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{FabricIdentity, SyncClient, SyncServer, peer_dial_spec};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

const TABLE: &str = "notes";
const DDL: &str =
    "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("bounded authenticated Iroh operation")
}

fn bind_spec(identity_path: &Path) -> String {
    format!("iroh:?identity={}", identity_path.display())
}

fn insert(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert exact hub-authored row");
}

fn body(db: &Database, id: Uuid) -> Option<String> {
    db.execute(
        "SELECT body FROM notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("read exact row")
    .rows
    .first()
    .map(|row| match &row[0] {
        Value::Text(body) => body.clone(),
        value => panic!("notes.body must be TEXT, got {value:?}"),
    })
}

struct Hub {
    db: Arc<Database>,
    ticket: String,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

async fn start_hub(root: &Path, tenant: &str) -> Hub {
    let identity_path = root.join("hub.db.fabric-identity.key");
    let endpoint = IrohServer::bind(&bind_spec(&identity_path))
        .await
        .expect("bind file-backed authoritative hub");
    let ticket = endpoint.ticket();
    let db = Arc::new(Database::open(root.join("hub.db")).expect("open file-backed hub"));
    db.execute(DDL, &HashMap::new())
        .expect("hub solely declares the explicit two-way table");
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
        within(self.task).await.expect("hub stops cleanly");
    }
}

#[tokio::test]
async fn pre_purge_descendant_is_visibly_refused_then_removed_by_authoritative_pull() {
    let root = tempfile::tempdir().expect("temporary test directory");
    let tenant = "authoritative-purge-descendant-refusal";
    let selected_id = Uuid::from_u128(0x1ce9_739e_9a30_4ea0_9a74_0b87_7f66_1001);
    let survivor_id = Uuid::from_u128(0x4d0a_a70c_c908_48e8_801f_5d2b_b17e_1002);
    let hub = start_hub(root.path(), tenant).await;
    insert(&hub.db, selected_id, "selected hub row");
    insert(&hub.db, survivor_id, "unrelated hub survivor");

    let edge_path = root.path().join("edge.db");
    let edge_identity = root.path().join("edge.db.fabric-identity.key");
    FabricIdentity::load_or_generate(&edge_identity)
        .expect("persist the stable authenticated edge identity");
    let edge = Arc::new(Database::open(&edge_path).expect("open file-backed edge"));
    let client = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&hub.ticket, &edge_identity),
        TenantId::from(tenant),
    );
    assert!(
        edge.table_meta(TABLE).is_none(),
        "blank edge receives the authoritative table through the baseline pull"
    );
    within(client.pull_default())
        .await
        .expect("baseline pull installs the hub table and its rows");
    assert_eq!(
        body(&edge, selected_id).as_deref(),
        Some("selected hub row"),
        "baseline pull gives the edge the exact selected row"
    );
    assert_eq!(
        body(&edge, survivor_id).as_deref(),
        Some("unrelated hub survivor"),
        "baseline pull gives the edge the exact unrelated survivor"
    );
    assert_eq!(
        edge.durable_deletion_state_for_test(TABLE, &Value::Uuid(selected_id))
            .purge_frontier,
        None,
        "baseline edge has no selected purge frontier"
    );

    hub.db
        .execute(
            "PURGE FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(selected_id))]),
        )
        .expect("public standalone PURGE removes the selected lineage at the hub");
    assert_eq!(
        body(&hub.db, selected_id),
        None,
        "the hub no longer serves the selected row"
    );
    assert_eq!(
        body(&hub.db, survivor_id).as_deref(),
        Some("unrelated hub survivor"),
        "the hub retains the unrelated survivor"
    );
    let hub_state = hub
        .db
        .durable_deletion_state_for_test(TABLE, &Value::Uuid(selected_id));
    let hub_root = hub_state
        .lineage_root
        .filter(|root| !root.is_empty())
        .expect("the hub exposes the selected lineage root");
    assert!(
        hub_state
            .purge_frontier
            .is_some_and(|frontier| !frontier.is_empty()),
        "the hub exposes a nonempty selected purge frontier"
    );

    edge.execute(
        "UPDATE notes SET body = $body WHERE id = $id",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(selected_id)),
            (
                "body".to_string(),
                Value::Text("offline descendant edit".to_string()),
            ),
        ]),
    )
    .expect("edge edits the still-live selected row before authoritative delivery");
    assert_eq!(
        body(&edge, selected_id).as_deref(),
        Some("offline descendant edit"),
        "the edge retains its offline descendant before pull"
    );
    assert_eq!(
        body(&edge, survivor_id).as_deref(),
        Some("unrelated hub survivor"),
        "the edge retains the unrelated survivor before pull"
    );
    assert_eq!(
        edge.durable_deletion_state_for_test(TABLE, &Value::Uuid(selected_id))
            .purge_frontier,
        None,
        "the offline descendant has no local purge frontier before pull"
    );

    let refused = within(client.push())
        .await
        .expect("hub visibly refuses the offline descendant edit");
    let rendered = serde_json::to_value(&refused).expect("typed refusal serializes");
    let conflicts = rendered["conflicts"]
        .as_array()
        .expect("offline descendant refusal has typed conflicts");
    assert_eq!(
        conflicts.len(),
        1,
        "one offline descendant produces one visible refusal"
    );
    let conflict = &conflicts[0];
    assert_eq!(conflict["table"], TABLE, "refusal names the exact table");
    assert_eq!(
        conflict["natural_key"],
        serde_json::to_value(NaturalKey::single(
            "id".to_string(),
            Value::Uuid(selected_id)
        ))
        .expect("serialize exact selected natural key"),
        "refusal names the exact selected key"
    );
    assert_eq!(
        conflict["mutation_kind"], "edit",
        "refusal identifies the descendant edit"
    );
    let diagnostic = conflict.to_string().to_ascii_lowercase();
    assert!(
        diagnostic.contains("purge") && diagnostic.contains("lineage"),
        "typed refusal identifies the purged lineage boundary: {conflict}"
    );
    assert_eq!(
        body(&hub.db, selected_id),
        None,
        "the refused descendant never reappears at the hub"
    );
    assert_eq!(
        body(&hub.db, survivor_id).as_deref(),
        Some("unrelated hub survivor"),
        "the refusal leaves the hub survivor intact"
    );

    within(client.pull_default())
        .await
        .expect("authoritative pull removes the refused descendant");
    assert_eq!(
        body(&edge, selected_id),
        None,
        "the authoritative pull removes the selected lineage from the edge"
    );
    let edge_state = edge.durable_deletion_state_for_test(TABLE, &Value::Uuid(selected_id));
    assert_eq!(
        edge_state.lineage_root.as_deref(),
        Some(hub_root.as_str()),
        "the edge records the exact authoritative lineage root"
    );
    assert!(
        edge_state
            .purge_frontier
            .is_some_and(|frontier| !frontier.is_empty()),
        "the edge stores a nonempty local permanent purge frontier"
    );
    assert_eq!(
        body(&edge, survivor_id).as_deref(),
        Some("unrelated hub survivor"),
        "the authoritative pull retains the unrelated survivor"
    );

    within(client.shutdown()).await;
    hub.stop().await;
}

#[tokio::test]
async fn fresh_same_key_hub_lineage_survives_stale_descendant_group_and_stale_edge_converges() {
    let root = tempfile::tempdir().expect("temporary test directory");
    let tenant = "fresh-same-key-with-stale-descendant-group";
    let selected_id = Uuid::from_u128(0x584a_1ec0_4c3f_4878_92a1_c77d_2001);
    let sibling_id = Uuid::from_u128(0x2fce_4195_c73c_4e32_8314_c77d_2002);
    let selected_key = NaturalKey::single("id".to_string(), Value::Uuid(selected_id));
    let sibling_key = NaturalKey::single("id".to_string(), Value::Uuid(sibling_id));
    let hub = start_hub(root.path(), tenant).await;
    insert(&hub.db, selected_id, "old hub row");

    let stale_path = root.path().join("stale.db");
    let stale_identity = root.path().join("stale.db.fabric-identity.key");
    FabricIdentity::load_or_generate(&stale_identity).expect("persist stale edge identity");
    let stale = Arc::new(Database::open(&stale_path).expect("open stale edge"));
    let stale_client = SyncClient::new(
        stale.clone(),
        &peer_dial_spec(&hub.ticket, &stale_identity),
        TenantId::from(tenant),
    );

    let fresh_path = root.path().join("fresh.db");
    let fresh_identity = root.path().join("fresh.db.fabric-identity.key");
    FabricIdentity::load_or_generate(&fresh_identity).expect("persist fresh edge identity");
    let fresh = Arc::new(Database::open(&fresh_path).expect("open fresh edge"));
    let fresh_client = SyncClient::new(
        fresh.clone(),
        &peer_dial_spec(&hub.ticket, &fresh_identity),
        TenantId::from(tenant),
    );

    within(stale_client.pull_default())
        .await
        .expect("stale baseline pull");
    within(fresh_client.pull_default())
        .await
        .expect("fresh baseline pull");
    let stale_old_sidecar = stale
        .authoritative_purge_current_live_row_sidecar_for_test(TABLE, &selected_key)
        .expect("stale edge records the baseline live lineage");
    let fresh_old_sidecar = fresh
        .authoritative_purge_current_live_row_sidecar_for_test(TABLE, &selected_key)
        .expect("fresh edge records the baseline live lineage");
    let old_root = stale_old_sidecar.lineage_root.clone();
    assert!(!old_root.is_empty(), "baseline lineage root is nonempty");
    assert_eq!(fresh_old_sidecar.lineage_root, old_root);
    assert_eq!(
        fresh_old_sidecar.author_node_id, stale_old_sidecar.author_node_id,
        "both edges begin from the exact hub-created lineage"
    );
    within(stale_client.shutdown()).await;
    drop(stale_client);

    hub.db
        .execute(
            "PURGE FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(selected_id))]),
        )
        .expect("hub permanently purges the old selected lineage");
    within(fresh_client.pull_default())
        .await
        .expect("fresh edge receives the authoritative purge");
    assert_eq!(body(&fresh, selected_id), None);
    assert!(matches!(
        fresh.classify_authoritative_purge_root_for_test(
            TABLE,
            fresh_old_sidecar.table_generation,
            &selected_key,
            &old_root,
        ),
        AuthoritativePurgeRootClassification::Purged { .. }
    ));

    insert(&fresh, selected_id, "fresh same-key body");
    let fresh_push = within(fresh_client.push())
        .await
        .expect("fresh same-key root reaches the hub");
    assert!(fresh_push.conflicts.is_empty());
    let fresh_sidecar = fresh
        .authoritative_purge_current_live_row_sidecar_for_test(TABLE, &selected_key)
        .expect("fresh edge exposes its accepted fresh live lineage");
    let fresh_root = fresh_sidecar.lineage_root.clone();
    assert!(!fresh_root.is_empty());
    assert_ne!(fresh_root, old_root);
    let frozen_hub_sidecar = hub
        .db
        .authoritative_purge_current_live_row_sidecar_for_test(TABLE, &selected_key)
        .expect("hub records the accepted fresh lineage")
        .clone();
    assert_eq!(frozen_hub_sidecar.lineage_root, fresh_root);
    assert!(matches!(
        hub.db.classify_authoritative_purge_root_for_test(
            TABLE,
            stale_old_sidecar.table_generation,
            &selected_key,
            &old_root,
        ),
        AuthoritativePurgeRootClassification::Purged { .. }
    ));
    assert_eq!(
        hub.db.classify_authoritative_purge_root_for_test(
            TABLE,
            frozen_hub_sidecar.table_generation,
            &selected_key,
            &fresh_root,
        ),
        AuthoritativePurgeRootClassification::NotPurged
    );

    let stale_before_group = stale.current_lsn();
    let stale_tx = stale.begin().expect("begin stale descendant group");
    stale
        .execute_in_tx(
            stale_tx,
            "UPDATE notes SET body = $body WHERE id = $id",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(selected_id)),
                (
                    "body".to_string(),
                    Value::Text("stale descendant body".to_string()),
                ),
            ]),
        )
        .expect("stage stale descendant edit");
    stale
        .execute_in_tx(
            stale_tx,
            "INSERT INTO notes (id, body) VALUES ($id, $body)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(sibling_id)),
                (
                    "body".to_string(),
                    Value::Text("stale sibling body".to_string()),
                ),
            ]),
        )
        .expect("stage stale sibling insert");
    stale.commit(stale_tx).expect("commit stale two-row group");
    let stale_group = stale.changes_since(stale_before_group);
    assert_eq!(stale_group.rows.len(), 2, "stale group carries two rows");
    assert!(
        stale_group
            .rows
            .iter()
            .all(|row| row.lsn == stale_group.rows[0].lsn),
        "the stale edit and sibling insert share one source LSN"
    );
    assert!(
        stale_group
            .rows
            .iter()
            .any(|row| row.natural_key == selected_key),
        "the stale group includes the selected old-lineage edit"
    );
    assert!(
        stale_group
            .rows
            .iter()
            .any(|row| row.natural_key == sibling_key),
        "the stale group includes the independently syncable sibling"
    );

    let stale_client = SyncClient::new(
        stale.clone(),
        &peer_dial_spec(&hub.ticket, &stale_identity),
        TenantId::from(tenant),
    );
    let stale_push = within(stale_client.push())
        .await
        .expect("hub adjudicates the stale descendant group");
    assert_eq!(stale_push.applied_rows, 1);
    assert_eq!(stale_push.skipped_rows, 1);
    assert_eq!(stale_push.conflicts.len(), 1);
    let stale_conflict = &stale_push.conflicts[0];
    assert_eq!(stale_conflict.natural_key, selected_key);
    assert_eq!(stale_conflict.reason.as_deref(), Some("purged_lineage"));
    assert_eq!(stale_conflict.resolution, ConflictPolicy::LatestWins);
    assert_eq!(stale_conflict.mutation_kind.as_deref(), Some("edit"));
    assert_eq!(stale_conflict.table.as_deref(), Some(TABLE));
    assert!(stale_conflict.winning_author_node_id.is_none());
    assert!(stale_conflict.hub_acceptance_position.is_none());
    assert!(matches!(
        hub.db.classify_authoritative_purge_root_for_test(
            TABLE,
            stale_old_sidecar.table_generation,
            &selected_key,
            &old_root,
        ),
        AuthoritativePurgeRootClassification::Purged { .. }
    ));
    assert_eq!(
        hub.db.classify_authoritative_purge_root_for_test(
            TABLE,
            frozen_hub_sidecar.table_generation,
            &selected_key,
            &fresh_root,
        ),
        AuthoritativePurgeRootClassification::NotPurged
    );
    assert_eq!(
        body(&hub.db, selected_id).as_deref(),
        Some("fresh same-key body")
    );
    assert_eq!(
        body(&hub.db, sibling_id).as_deref(),
        Some("stale sibling body")
    );
    assert_eq!(
        hub.db
            .execute(
                "SELECT id FROM notes WHERE id = $id",
                &HashMap::from([("id".to_string(), Value::Uuid(sibling_id))]),
            )
            .expect("count hub sibling")
            .rows
            .len(),
        1,
        "the accepted sibling appears at the hub exactly once"
    );
    assert_eq!(
        hub.db
            .authoritative_purge_current_live_row_sidecar_for_test(TABLE, &selected_key)
            .expect("hub retains a fresh live sidecar")
            .clone(),
        frozen_hub_sidecar,
        "stale-group handling never replaces the fresh hub lineage"
    );
    assert_eq!(
        body(&stale, selected_id).as_deref(),
        Some("stale descendant body")
    );
    assert_eq!(
        body(&stale, sibling_id).as_deref(),
        Some("stale sibling body")
    );
    assert_eq!(
        stale
            .authoritative_purge_current_live_row_sidecar_for_test(TABLE, &selected_key)
            .expect("stale edge still carries its old live sidecar before pull")
            .lineage_root,
        old_root
    );

    within(stale_client.pull_default())
        .await
        .expect("one ordinary pull converges the stale edge");
    let converged_sidecar = stale
        .authoritative_purge_current_live_row_sidecar_for_test(TABLE, &selected_key)
        .expect("stale edge receives the fresh live sidecar");
    assert_eq!(
        body(&stale, selected_id).as_deref(),
        Some("fresh same-key body")
    );
    assert_eq!(
        converged_sidecar.author_node_id,
        frozen_hub_sidecar.author_node_id
    );
    assert_eq!(
        converged_sidecar.author_database_incarnation,
        frozen_hub_sidecar.author_database_incarnation
    );
    assert_eq!(
        converged_sidecar.author_local_mutation_position,
        frozen_hub_sidecar.author_local_mutation_position
    );
    assert_eq!(
        converged_sidecar.table_generation,
        frozen_hub_sidecar.table_generation
    );
    assert_eq!(converged_sidecar.lineage_root, fresh_root);
    assert_eq!(
        converged_sidecar.lineage_attestation,
        frozen_hub_sidecar.lineage_attestation
    );
    assert_eq!(
        converged_sidecar.author_node_id,
        fresh_sidecar.author_node_id
    );
    assert!(matches!(
        stale.classify_authoritative_purge_root_for_test(
            TABLE,
            stale_old_sidecar.table_generation,
            &selected_key,
            &old_root,
        ),
        AuthoritativePurgeRootClassification::Purged { .. }
    ));
    assert_eq!(
        stale.classify_authoritative_purge_root_for_test(
            TABLE,
            converged_sidecar.table_generation,
            &selected_key,
            &fresh_root,
        ),
        AuthoritativePurgeRootClassification::NotPurged
    );
    assert_eq!(
        body(&stale, sibling_id).as_deref(),
        Some("stale sibling body")
    );
    assert_eq!(
        hub.db
            .authoritative_purge_current_live_row_sidecar_for_test(TABLE, &selected_key)
            .expect("hub fresh sidecar remains available")
            .clone(),
        frozen_hub_sidecar,
        "stale convergence leaves the hub fresh lineage unchanged"
    );
    assert_eq!(
        body(&hub.db, selected_id).as_deref(),
        Some("fresh same-key body")
    );
    assert_eq!(
        hub.db
            .authoritative_purge_current_live_row_sidecar_for_test(TABLE, &selected_key)
            .expect("hub retains the frozen fresh live sidecar")
            .clone(),
        frozen_hub_sidecar
    );
    assert_eq!(
        body(&hub.db, sibling_id).as_deref(),
        Some("stale sibling body")
    );

    within(stale_client.shutdown()).await;
    within(fresh_client.shutdown()).await;
    hub.stop().await;
}
