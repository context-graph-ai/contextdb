//! Authoritative purge and local discard erase every selected owned copy in one boundary.

use contextdb_core::{EdgeDiscardMode, Error, TenantId, Value, Wallclock};
use contextdb_engine::database::{
    DeliveryManifest, ReadExecutionConvergenceEvent, ReadExecutionConvergenceObserver,
};
use contextdb_engine::read_session::ReadKernelSource;
use contextdb_engine::sync_types::NaturalKey;
use contextdb_engine::{Database, DeliveryOutcomeKind, QueryResult};
use contextdb_server::sync_client::ApplicationTablePolicyExpectation;
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{SyncClient, SyncServer, peer_dial_spec};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use uuid::Uuid;

const T0: u64 = 1_700_000_000_000;

/// The root table's declared clauses. The discard mode is written explicitly
/// where a case depends on it, and left silent where the case is about the
/// effective default.
const ROOT_CLAUSES_AFTER_OUTCOME: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE \
     DELIVERY MANIFEST OVER record_parts EDGE DISCARD AFTER OUTCOME";
const ROOT_CLAUSES_SILENT_DISCARD: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE \
     DELIVERY MANIFEST OVER record_parts";
const ROOT_CLAUSES_NEVER: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE \
     DELIVERY MANIFEST OVER record_parts EDGE DISCARD NEVER";
const ROOT_CLAUSES_ALWAYS: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE \
     DELIVERY MANIFEST OVER record_parts EDGE DISCARD ALWAYS";
const MEMBER_CLAUSES: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE";
const MEMBER_CLAUSES_ALWAYS: &str =
    "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE EDGE DISCARD ALWAYS";

/// A node-local table the owner can rebuild. It never leaves the node, so it
/// has no hub side and no mode of its own to satisfy.
const DERIVED_TABLE_DDL: &str = "CREATE TABLE record_extracts (id UUID PRIMARY KEY, records_id UUID, batch_ref TEXT, body TEXT) \
     SYNC OFF";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

#[derive(Default)]
struct SelectionWitness(AtomicU64);

impl ReadExecutionConvergenceObserver for SelectionWitness {
    fn observe(&self, event: ReadExecutionConvergenceEvent) {
        if matches!(
            event,
            ReadExecutionConvergenceEvent::EagerRowSourceTouch
                | ReadExecutionConvergenceEvent::PullKernelSourceTouch {
                    source: ReadKernelSource::TableRow | ReadKernelSource::IndexEntry,
                    ..
                }
        ) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// Observe only the statement under test, excluding preparation and readback.
/// The callbacks originate inside the shared eager scan or bounded row source.
fn observe_selection<T>(db: &Database, operation: impl FnOnce() -> T) -> (T, u64) {
    let witness = Arc::new(SelectionWitness::default());
    let result = db.with_read_execution_convergence_observer_for_test(witness.clone(), operation);
    (result, witness.0.load(Ordering::SeqCst))
}

fn text(value: &str) -> Value {
    Value::Text(value.to_string())
}

#[derive(Debug, PartialEq)]
struct ErasureTableResult {
    affected: u64,
    pending_units: Option<u64>,
    survivors: serde_json::Value,
}

/// Presentation is replaceable. This adapter recognizes
/// today's labels only to recover the per-table erasure facts the contract
/// owns; a future representation can replace this decoder without changing
/// the assertions below.
fn normalize_erasure(result: &QueryResult) -> BTreeMap<String, ErasureTableResult> {
    let column = |name: &str| {
        result
            .columns
            .iter()
            .position(|column| column == name)
            .unwrap_or_else(|| panic!("erasure representation supplies its {name} field"))
    };
    let table = column("table");
    let affected = column("rows_affected");
    let survivors = column("survivors");
    let pending_units = result
        .columns
        .iter()
        .position(|column| column == "pending_units");
    result
        .rows
        .iter()
        .map(|row| {
            let table = match &row[table] {
                Value::Text(table) => table.clone(),
                other => panic!("erasure table identity is text, got {other:?}"),
            };
            let affected = match row[affected] {
                Value::Int64(value) if value >= 0 => value as u64,
                ref other => panic!("erasure affected count is non-negative, got {other:?}"),
            };
            let pending_units = pending_units.map(|index| match row[index] {
                Value::Int64(value) if value >= 0 => value as u64,
                ref other => panic!("erasure pending count is non-negative, got {other:?}"),
            });
            let survivors = match &row[survivors] {
                Value::Json(survivors) => survivors.clone(),
                other => panic!("erasure survivors are structured values, got {other:?}"),
            };
            (
                table,
                ErasureTableResult {
                    affected,
                    pending_units,
                    survivors,
                },
            )
        })
        .collect()
}

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .expect("a sync exchange must complete within 60s")
}

fn bind_spec(identity_path: &Path) -> String {
    format!("iroh:?identity={}", identity_path.display())
}

struct Hub {
    db: Arc<Database>,
    ticket: String,
    node_id: String,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl Hub {
    async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        within(self.task).await.expect("hub stops cleanly");
    }
}

async fn start_hub(root: &Path, tenant: &str, root_clauses: &str) -> Hub {
    start_hub_with_member_clauses(root, tenant, root_clauses, MEMBER_CLAUSES).await
}

async fn start_hub_with_member_clauses(
    root: &Path,
    tenant: &str,
    root_clauses: &str,
    member_clauses: &str,
) -> Hub {
    let identity_path = root.join("hub.db.fabric-identity.key");
    let endpoint = IrohServer::bind(&bind_spec(&identity_path))
        .await
        .expect("bind the authoritative hub endpoint");
    let ticket = endpoint.ticket();
    let node_id = endpoint.node_id();
    let db = Arc::new(Database::open(root.join("hub.db")).expect("open the hub store"));
    if member_clauses == MEMBER_CLAUSES {
        create_tables(&db, root_clauses);
    } else {
        create_tables_with_member_clauses(&db, root_clauses, member_clauses);
    }
    db.__seed_tenant_table_policies_for_test(
        TenantId::from(tenant),
        ApplicationTablePolicyExpectation::new()
            .expect_table("records", root_clauses)
            .expect("the root fixture uses canonical declaration clauses")
            .expect_table("record_parts", member_clauses)
            .expect("the member fixture uses canonical declaration clauses"),
    )
    .expect("install durable declaration prerequisites for erasure tests");
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
        node_id,
        stop,
        task,
    }
}

fn create_tables(db: &Database, root_clauses: &str) {
    create_tables_with_member_clauses(db, root_clauses, MEMBER_CLAUSES);
}

fn create_tables_with_member_clauses(db: &Database, root_clauses: &str, member_clauses: &str) {
    db.execute(
        &format!(
            "CREATE TABLE records (id UUID PRIMARY KEY, batch_ref TEXT, body TEXT) {root_clauses}"
        ),
        &p(),
    )
    .expect("root schema matches policy");
    let members = format!(
        "CREATE TABLE record_parts (id UUID PRIMARY KEY, records_id UUID REFERENCES records(id), batch_ref TEXT, body TEXT) {member_clauses}"
    );
    for ddl in [members.as_str(), DERIVED_TABLE_DDL] {
        db.execute(ddl, &p()).expect("related table schema");
    }
}

fn open_edge(root: &Path, name: &str) -> Arc<Database> {
    Arc::new(Database::open(root.join(format!("{name}.db"))).expect("open the edge store"))
}

async fn bind_edge(db: &Database, client: &SyncClient, root_clauses: &str) {
    bind_edge_with_member_clauses(db, client, root_clauses, MEMBER_CLAUSES).await;
}

async fn bind_edge_with_member_clauses(
    db: &Database,
    client: &SyncClient,
    root_clauses: &str,
    member_clauses: &str,
) {
    within(
        client.__seed_application_table_policy_binding_for_test(
            ApplicationTablePolicyExpectation::new()
                .expect_table("records", root_clauses)
                .expect("the root expectation is written in the declaration's clause words")
                .expect_table("record_parts", member_clauses)
                .expect("the member expectation is written in the declaration's clause words"),
        ),
    )
    .await
    .expect("the edge binds both declared tables");
    if db.table_meta("records").is_none() {
        create_tables_with_member_clauses(db, root_clauses, member_clauses);
    }
}

fn key_of(id: Uuid) -> NaturalKey {
    NaturalKey::single("id".to_string(), Value::Uuid(id))
}

/// Writes one ordinary local unit — a root row, `members` member rows and a
/// node-local derived row. Delivery membership is deliberately not staged here:
/// callers that offer this unit must prepare its manifest in the same source
/// transaction with `stage_manifested_unit`.
fn write_unit(db: &Database, batch: &str, root: Uuid, members: &[Uuid]) {
    let tx = db.begin().expect("open the writing transaction");
    db.execute_in_tx(
        tx,
        "INSERT INTO records (id, batch_ref, body) VALUES ($id, $batch, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(root)),
            ("batch".to_string(), text(batch)),
            ("body".to_string(), text("root row")),
        ]),
    )
    .expect("write the root row");
    for member in members {
        db.execute_in_tx(
            tx,
            "INSERT INTO record_parts (id, records_id, batch_ref, body) \
             VALUES ($id, $root, $batch, $body)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(*member)),
                ("root".to_string(), Value::Uuid(root)),
                ("batch".to_string(), text(batch)),
                ("body".to_string(), text("member row")),
            ]),
        )
        .expect("write the member row");
    }
    db.commit(tx).expect("commit the unit");

    write_derived_row(db, batch, root);
}

fn write_derived_row(db: &Database, batch: &str, root: Uuid) {
    // The node-local derived row is not part of the delivery unit; it exists so
    // a multi-table erasure has a third selected application table to carry.
    db.execute(
        "INSERT INTO record_extracts (id, records_id, batch_ref, body) \
         VALUES ($id, $root, $batch, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("root".to_string(), Value::Uuid(root)),
            ("batch".to_string(), text(batch)),
            ("body".to_string(), text("derived row")),
        ]),
    )
    .expect("write the node-local derived row");
}

fn stage_unit(db: &Database, batch: &str, root: Uuid, members: &[Uuid]) -> contextdb_core::TxId {
    let tx = db.begin().expect("open the writing transaction");
    db.execute_in_tx(
        tx,
        "INSERT INTO records (id, batch_ref, body) VALUES ($id, $batch, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(root)),
            ("batch".to_string(), text(batch)),
            ("body".to_string(), text("root row")),
        ]),
    )
    .expect("write the root row");
    for member in members {
        db.execute_in_tx(
            tx,
            "INSERT INTO record_parts (id, records_id, batch_ref, body) \
             VALUES ($id, $root, $batch, $body)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(*member)),
                ("root".to_string(), Value::Uuid(root)),
                ("batch".to_string(), text(batch)),
                ("body".to_string(), text("member row")),
            ]),
        )
        .expect("write the member row");
    }

    // The node-local derived row is not part of the unit; it exists so a
    // multi-table erasure has a third table to carry.
    db.execute_in_tx(
        tx,
        "INSERT INTO record_extracts (id, records_id, batch_ref, body) \
         VALUES ($id, $root, $batch, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("root".to_string(), Value::Uuid(root)),
            ("batch".to_string(), text(batch)),
            ("body".to_string(), text("derived row")),
        ]),
    )
    .expect("write the node-local derived row");
    tx
}

/// Commit source rows and their canonical signed manifest together, so an
/// ordinary push offers a real delivery unit rather than fixture-only rows.
fn stage_manifested_unit(
    db: &Database,
    client: &SyncClient,
    batch: &str,
    root: Uuid,
    members: &[Uuid],
) {
    let tx = stage_unit(db, batch, root, members);
    client
        .__stage_delivery_manifest_for_test(
            tx,
            DeliveryManifest {
                root_table: "records",
                root_key: key_of(root),
                members: members
                    .iter()
                    .map(|member| ("record_parts", key_of(*member)))
                    .collect(),
            },
        )
        .expect("canonical manifest registers the source rows before their commit");
    db.commit(tx)
        .expect("commit source rows and delivery metadata atomically");
}

fn row_count(db: &Database, table: &str, batch: &str) -> usize {
    db.execute(
        &format!("SELECT id FROM {table} WHERE batch_ref = $batch"),
        &HashMap::from([("batch".to_string(), text(batch))]),
    )
    .unwrap_or_else(|err| panic!("{table} must answer: {err}"))
    .rows
    .len()
}

fn total_rows(db: &Database, table: &str) -> usize {
    db.execute(&format!("SELECT id FROM {table}"), &p())
        .unwrap_or_else(|err| panic!("{table} must answer: {err}"))
        .rows
        .len()
}

/// Every listed table's whole content, as text, so "unchanged" is proven
/// against content rather than against a count.
fn whole_store(db: &Database) -> Vec<String> {
    ["records", "record_parts", "record_extracts"]
        .into_iter()
        .map(|table| {
            let result = db
                .execute(&format!("SELECT * FROM {table} ORDER BY id"), &p())
                .unwrap_or_else(|err| panic!("{table} must answer: {err}"));
            format!("{table}:{:?}", result.rows)
        })
        .collect()
}

fn outcome_words(db: &Database) -> Vec<String> {
    let result = db
        .execute("SHOW DELIVERY OUTCOMES FOR records", &p())
        .expect("outcomes render");
    let outcome_column = result
        .columns
        .iter()
        .position(|column| column == "outcome")
        .expect("outcomes name their verdict column");
    result
        .rows
        .iter()
        .map(|row| format!("{:?}", row[outcome_column]))
        .collect()
}

// ---------------------------------------------------------------------------

// Purge erases manifests and outcomes with their lineage, while fresh same-key creation succeeds.
#[tokio::test]
async fn purging_a_root_erases_its_manifest_and_outcomes_on_hub_and_edge_and_a_fresh_same_key_row_starts_a_new_unit()
 {
    let _clock = Wallclock::test_clock_guard(|| T0);
    let root = tempfile::tempdir().expect("temporary test directory");
    let tenant = "purge-takes-the-custody-trail";
    let hub = start_hub(root.path(), tenant, ROOT_CLAUSES_AFTER_OUTCOME).await;

    let edge = open_edge(root.path(), "edge");
    let edge_identity = root.path().join("edge.db.fabric-identity.key");
    let client = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&hub.ticket, &edge_identity),
        TenantId::from(tenant),
    );
    bind_edge(&edge, &client, ROOT_CLAUSES_AFTER_OUTCOME).await;

    let selected = Uuid::new_v4();
    let survivor = Uuid::new_v4();
    let selected_members = [Uuid::new_v4(), Uuid::new_v4()];
    let survivor_member = Uuid::new_v4();
    let tx = stage_unit(&edge, "selected", selected, &selected_members);
    client
        .__stage_delivery_manifest_for_test(
            tx,
            DeliveryManifest {
                root_table: "records",
                root_key: key_of(selected),
                members: selected_members
                    .iter()
                    .map(|m| ("record_parts", key_of(*m)))
                    .collect(),
            },
        )
        .expect("canonical selected manifest");
    edge.commit(tx)
        .expect("commit rows and delivery metadata atomically");

    let tx = stage_unit(&edge, "survivor", survivor, &[survivor_member]);
    client
        .__stage_delivery_manifest_for_test(
            tx,
            DeliveryManifest {
                root_table: "records",
                root_key: key_of(survivor),
                members: vec![("record_parts", key_of(survivor_member))],
            },
        )
        .expect("canonical survivor manifest");
    edge.commit(tx)
        .expect("commit rows and delivery metadata atomically");

    // Purge preparation uses a complete ordinary push: the backup must
    // contain a real batch bookmark, not a fixture's premature per-unit receipt.
    let delivered = within(client.push())
        .await
        .expect("deliver both committed units through the ordinary lane");
    assert_eq!((delivered.applied_rows, delivered.skipped_rows), (5, 0));
    assert_eq!(
        edge.delivery_outcome("records", &key_of(selected))
            .unwrap()
            .unwrap()
            .kind(),
        DeliveryOutcomeKind::Accepted
    );
    assert_eq!(
        edge.delivery_outcome("records", &key_of(survivor))
            .unwrap()
            .unwrap()
            .kind(),
        DeliveryOutcomeKind::Accepted
    );

    assert_eq!(
        total_rows(&hub.db, "records"),
        2,
        "canonical acceptance materialized both real units"
    );
    // A distinct equivalent writer owns a different immutable
    // creator lineage for the same root and must receive the same erasure.
    let equivalent_edge = open_edge(root.path(), "equivalent-edge");
    let equivalent_identity = root.path().join("equivalent-edge.db.fabric-identity.key");
    let equivalent_client = SyncClient::new(
        equivalent_edge.clone(),
        &peer_dial_spec(&hub.ticket, &equivalent_identity),
        TenantId::from(tenant),
    );
    bind_edge(
        &equivalent_edge,
        &equivalent_client,
        ROOT_CLAUSES_AFTER_OUTCOME,
    )
    .await;
    stage_manifested_unit(
        &equivalent_edge,
        &equivalent_client,
        "selected",
        selected,
        &selected_members,
    );
    within(equivalent_client.push())
        .await
        .expect("offer the distinct equivalent source through the ordinary lane");
    assert_eq!(
        equivalent_edge
            .delivery_outcome("records", &key_of(selected))
            .unwrap()
            .unwrap()
            .kind(),
        DeliveryOutcomeKind::Equivalent
    );
    let stale_snapshot = root.path().join("stale-edge.db");
    edge.export_snapshot(&stale_snapshot).unwrap();
    let stale_identity = root.path().join("stale-edge.db.fabric-identity.key");
    std::fs::copy(&edge_identity, &stale_identity).unwrap();
    let stale_edge = Arc::new(Database::open(&stale_snapshot).unwrap());
    assert_eq!(
        whole_store(&stale_edge),
        whole_store(&edge),
        "the backup preserves the actual source rows and their original lineage"
    );

    // The hub forgets the root.
    hub.db
        .execute(
            "PURGE FROM records WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(selected))]),
        )
        .expect("the hub erases the selected root");

    assert!(
        hub.db
            .delivery_outcome("records", &key_of(selected))
            .expect("read purged hub outcome")
            .is_none(),
        "the hub holds no answer for a root it has forgotten — an erased record \
         cannot be reported as delivered"
    );
    let hub_totals = hub.db.delivery_status("records").expect("hub totals");
    assert_eq!(
        (hub_totals.eligible, hub_totals.accepted, hub_totals.pending),
        (0, 0, 0),
        "received hub materializations are never locally eligible roots"
    );
    assert_eq!(
        outcome_words(&hub.db),
        vec!["Text(\"accepted\")".to_string()],
        "the hub renders exactly one answer, the survivor's"
    );
    assert!(
        hub.db
            .delivery_outcome("records", &key_of(survivor))
            .expect("read surviving hub outcome")
            .is_some(),
        "the survivor's answer is untouched"
    );

    // The edge that held the unit learns of the erasure on its next exchange.
    within(equivalent_client.pull_default())
        .await
        .expect("the distinct equivalent lineage receives the authoritative erasure");
    assert_eq!(row_count(&equivalent_edge, "records", "selected"), 0);
    assert_eq!(
        equivalent_edge.delivery_status("records").unwrap().eligible,
        0
    );
    assert!(
        equivalent_edge
            .delivery_outcome("records", &key_of(selected))
            .unwrap()
            .is_none()
    );
    within(equivalent_client.pull_default())
        .await
        .expect("equivalent purge delivery is idempotent");
    within(client.pull_default())
        .await
        .expect("the edge receives the erasure");
    assert!(
        edge.delivery_outcome("records", &key_of(selected))
            .expect("read purged edge outcome")
            .is_none(),
        "the edge's own record of the forgotten unit is gone with the rows"
    );
    let edge_totals = edge.delivery_status("records").expect("edge totals");
    assert_eq!(
        (
            edge_totals.eligible,
            edge_totals.accepted,
            edge_totals.pending
        ),
        (1, 1, 0),
        "the edge no longer counts the forgotten unit at all — not as delivered, \
         and not as still owed"
    );
    assert_eq!(
        row_count(&edge, "records", "selected"),
        0,
        "the forgotten root is gone from the edge"
    );
    assert_eq!(
        row_count(&edge, "records", "survivor"),
        1,
        "the survivor is untouched on the edge"
    );

    // This older copy has the purged source lineage and stale receipts.
    // Ordinary reconnect must erase the selected unit before it can regain credit.
    let stale_identity = root.path().join("stale-edge.db.fabric-identity.key");
    let stale_client = SyncClient::new(
        stale_edge.clone(),
        &peer_dial_spec(&hub.ticket, &stale_identity),
        TenantId::from(tenant),
    );
    within(stale_client.pull_default())
        .await
        .expect("the second edge receives the erasure");
    within(stale_client.push())
        .await
        .expect("the second edge's offer is answered rather than wedging it");
    assert_eq!(
        row_count(&stale_edge, "records", "selected"),
        0,
        "a unit still owed to the hub, whose root belongs to the erased lineage, is \
         removed locally instead of being offered forever"
    );
    assert!(
        stale_edge
            .delivery_outcome("records", &key_of(selected))
            .expect("read stale edge outcome")
            .is_none(),
        "its records go with it"
    );
    assert_eq!(
        stale_edge
            .delivery_status("records")
            .expect("second edge totals")
            .eligible,
        1,
        "nothing of that unit is left to count"
    );
    assert_eq!(
        total_rows(&hub.db, "records"),
        1,
        "nothing of the erased lineage was re-created at the hub"
    );

    // A genuinely new creation under the same key after the erasure is a new
    // unit, not a resurrection of the old one.
    let fresh_member = Uuid::new_v4();
    let before_fresh = edge
        .delivery_status("records")
        .expect("edge totals before fresh unit");
    stage_manifested_unit(&edge, &client, "fresh", selected, &[fresh_member]);
    let fresh = edge
        .delivery_status("records")
        .expect("edge totals after fresh unit");
    assert_eq!(
        (fresh.eligible, fresh.accepted, fresh.pending),
        (
            before_fresh.eligible + 1,
            before_fresh.accepted,
            before_fresh.pending + 1,
        ),
        "the post-purge root is a new submitted source unit, not a reused terminal"
    );
    assert_eq!(
        (
            row_count(&edge, "records", "fresh"),
            row_count(&edge, "record_parts", "fresh"),
        ),
        (1, 1),
        "the new submission owns fresh root and member row lives before delivery"
    );
    within(client.push())
        .await
        .expect("the newly manifested unit reaches the hub");
    assert_eq!(
        hub.db
            .delivery_outcome("records", &key_of(selected))
            .unwrap()
            .unwrap()
            .kind(),
        DeliveryOutcomeKind::Accepted,
        "the hub answers the new unit under the key the erasure freed"
    );
    assert_eq!(
        edge.delivery_outcome("records", &key_of(selected))
            .unwrap()
            .unwrap()
            .kind(),
        DeliveryOutcomeKind::Accepted,
        "the edge reads the new answer back"
    );
    assert_eq!(
        outcome_words(&hub.db).len(),
        2,
        "the hub now holds two answers: the survivor's and the new unit's"
    );
    assert_eq!(
        row_count(&hub.db, "records", "fresh"),
        1,
        "the new root landed at the hub under the key the erasure freed"
    );

    within(client.shutdown()).await;
    within(stale_client.shutdown()).await;
    within(equivalent_client.shutdown()).await;
    hub.stop().await;
}

// Multi-table purge selects and erases as one durable boundary at both hub and edge.
#[tokio::test]
async fn a_multi_table_purge_is_one_erasure_boundary_with_per_table_results_and_is_refused_before_selection_when_illegal()
 {
    let _clock = Wallclock::test_clock_guard(|| T0);
    let root = tempfile::tempdir().expect("temporary test directory");
    let tenant = "one-erasure-boundary";
    let hub = start_hub(root.path(), tenant, ROOT_CLAUSES_AFTER_OUTCOME).await;

    let edge = open_edge(root.path(), "edge");
    let edge_identity = root.path().join("edge.db.fabric-identity.key");
    let client = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&hub.ticket, &edge_identity),
        TenantId::from(tenant),
    );
    bind_edge(&edge, &client, ROOT_CLAUSES_AFTER_OUTCOME).await;

    let selected = Uuid::new_v4();
    let selected_member = Uuid::new_v4();
    let survivor = Uuid::new_v4();
    stage_manifested_unit(&edge, &client, "selected", selected, &[selected_member]);
    within(client.__seed_delivery_outcome_for_test(
        "records",
        &key_of(selected),
        DeliveryOutcomeKind::Accepted,
        None,
    ))
    .await
    .unwrap();
    write_derived_row(&hub.db, "selected", selected);
    write_unit(&hub.db, "survivor", survivor, &[Uuid::new_v4()]);
    assert_eq!(
        (
            row_count(&hub.db, "records", "selected"),
            row_count(&hub.db, "record_parts", "selected"),
            row_count(&hub.db, "record_extracts", "selected"),
        ),
        (1, 1, 1),
        "the hub has one selected row in every listed table"
    );
    assert_eq!(
        (
            row_count(&edge, "records", "selected"),
            row_count(&edge, "record_parts", "selected"),
            row_count(&edge, "record_extracts", "selected"),
        ),
        (1, 1, 1),
        "the edge retains the exact root and member row lives it offered, plus its local derived row"
    );

    let selected_clause = "batch_ref = $batch";
    let list = format!(
        "PURGE FROM records WHERE {selected_clause}, \
         record_parts WHERE {selected_clause}, \
         record_extracts WHERE {selected_clause}"
    );
    let selected_params = HashMap::from([("batch".to_string(), text("selected"))]);

    // ---- refusals, every one of them before a row is selected --------------
    let before_refusals = whole_store(&hub.db);

    let tx = hub.db.begin().expect("open a transaction");
    let (result, selections) = observe_selection(&hub.db, || {
        hub.db.execute_in_tx(tx, &list, &selected_params)
    });
    assert_eq!(
        selections, 0,
        "transactional PURGE refuses before row selection"
    );
    match result {
        Err(Error::PurgeRequiresStandaloneExecution) => {}
        other => panic!("the erasure stays a standalone statement, got {other:?}"),
    }
    hub.db.rollback(tx).expect("close the transaction");

    let (result, selections) = observe_selection(&hub.db, || {
        hub.db.execute(
            &format!("PURGE FROM records WHERE {selected_clause}, records WHERE {selected_clause}"),
            &selected_params,
        )
    });
    assert_eq!(
        selections, 0,
        "duplicate tables refuse before row selection"
    );
    match result {
        Err(Error::SchemaInvalid { reason }) => assert!(
            reason.contains("records") && reason.contains("twice"),
            "the refusal names the table the caller listed twice: {reason}"
        ),
        other => panic!("a table listed twice is refused before selection, got {other:?}"),
    }

    hub.db
        .execute("CREATE TABLE loose_notes (body TEXT) SYNC OFF", &p())
        .expect("a table with no key of its own");
    let (result, selections) = observe_selection(&hub.db, || {
        hub.db.execute(
            &format!("PURGE FROM records WHERE {selected_clause}, loose_notes WHERE body = $batch"),
            &selected_params,
        )
    });
    assert_eq!(
        selections, 0,
        "ineligible tables refuse before any listed selection"
    );
    match result {
        Err(Error::SchemaInvalid { reason }) => assert!(
            reason.contains("loose_notes"),
            "the refusal names the table that cannot be erased this way: {reason}"
        ),
        other => panic!("a table that is not erasable is refused before selection, got {other:?}"),
    }

    // Refuse a node-local subquery before the first table scans.
    let (result, selections) = observe_selection(&hub.db, || {
        hub.db.execute(
        "PURGE FROM records WHERE batch_ref = $batch, record_extracts WHERE records_id IN (SELECT id FROM records)",
        &selected_params,
    )
    });
    assert!(
        matches!(result, Err(Error::SubqueryNotSupported)),
        "node-local purge needs a self-contained predicate: {result:?}"
    );
    assert_eq!(
        selections, 0,
        "a later node-local subquery refuses before every selection"
    );

    let edge_before_refusal = whole_store(&edge);
    let (result, selections) = observe_selection(&edge, || edge.execute(&list, &selected_params));
    assert_eq!(selections, 0, "an edge PURGE refuses before row selection");
    match result {
        Err(Error::PurgeRequiresAuthoritativeHub { hub_node_id }) => assert_eq!(
            hub_node_id, hub.node_id,
            "an edge is told which node holds the authority to forget"
        ),
        other => panic!("an enrolled edge may never erase for the fleet, got {other:?}"),
    }
    assert_eq!(whole_store(&edge), edge_before_refusal);

    assert_eq!(
        whole_store(&hub.db),
        before_refusals,
        "not one refusal destroyed a row"
    );

    assert!(hub.db.execute("PURGE FROM records WHERE batch_ref=$batch, record_parts WHERE absent_column=$batch", &selected_params).is_err());
    assert_eq!(
        whole_store(&hub.db),
        before_refusals,
        "all selections finish before destructive work"
    );

    // ---- the boundary fails part way, and nothing changed -------------------
    let before_failure = whole_store(&hub.db);
    hub.db.__arm_erasure_boundary_persist_fault_for_test();
    let failed = hub.db.execute(&list, &selected_params);
    assert!(
        hub.db.__erasure_boundary_persist_fault_reached_for_test(),
        "actual first-table destructive work was reached inside the uncommitted Redb transaction"
    );
    assert!(
        matches!(&failed,Err(Error::Other(message)) if message=="storage error: erasure first table staged fault injected"),
        "the injected persistence failure, not a preflight refusal, must be returned: {failed:?}"
    );
    assert!(
        failed.is_err(),
        "a boundary that could not complete must report so, got {failed:?}"
    );
    let hub_path = root.path().join("hub.db");
    let hub_node_id = hub.node_id.clone();
    hub.stop().await;
    let reopened = Arc::new(Database::open(&hub_path).expect("reopen the hub store"));
    assert_eq!(
        whole_store(&reopened),
        before_failure,
        "a boundary that failed part way destroyed nothing: every listed table holds \
         exactly what it held before"
    );

    // PURGE reports affected rows and survivors per table.
    let listed = reopened
        .execute(&list, &selected_params)
        .expect("the boundary completes");
    assert_eq!(
        normalize_erasure(&listed),
        BTreeMap::from([
            (
                "records".to_string(),
                ErasureTableResult {
                    affected: 1,
                    pending_units: None,
                    survivors: serde_json::json!([]),
                },
            ),
            (
                "record_parts".to_string(),
                ErasureTableResult {
                    affected: 1,
                    pending_units: None,
                    survivors: serde_json::json!([]),
                },
            ),
            (
                "record_extracts".to_string(),
                ErasureTableResult {
                    affected: 1,
                    pending_units: None,
                    survivors: serde_json::json!([]),
                },
            ),
        ]),
        "every selected table reports its affected count and survivors"
    );
    assert_eq!(
        listed.rows_affected, 3,
        "the statement reports the total it destroyed"
    );
    assert_eq!(
        row_count(&reopened, "records", "survivor"),
        1,
        "the unlisted selection is untouched"
    );

    // A listed table that matches nothing contributes zero and does not fail.
    write_unit(&reopened, "second", Uuid::new_v4(), &[]);
    let mixed = reopened
        .execute(
            &format!(
                "PURGE FROM records WHERE {selected_clause}, \
                 record_parts WHERE {selected_clause}"
            ),
            &HashMap::from([("batch".to_string(), text("second"))]),
        )
        .expect("a table matching nothing does not fail the statement");
    let mixed_rows_affected = mixed.rows_affected;
    let mixed = normalize_erasure(&mixed);
    assert_eq!(mixed["records"].affected, 1);
    assert_eq!(
        mixed["record_parts"].affected, 0,
        "the listed zero-match table remains present with zero affected rows"
    );
    assert_eq!(mixed_rows_affected, 1);

    // ---- delivery lands on a bound edge as one unit -------------------------
    let restarted_identity = root.path().join("hub.db.fabric-identity.key");
    let endpoint = IrohServer::bind(&bind_spec(&restarted_identity))
        .await
        .expect("rebind the hub endpoint");
    assert_eq!(
        endpoint.node_id(),
        hub_node_id,
        "the restarted hub keeps its identity, so the edge is still bound to it"
    );
    let server = Arc::new(SyncServer::new(
        reopened.clone(),
        &endpoint,
        TenantId::from(tenant),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let ticket = endpoint.ticket();
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    let client = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&ticket, &edge_identity),
        TenantId::from(tenant),
    );
    let before_edge_apply = whole_store(&edge);
    edge.__arm_erasure_boundary_persist_fault_for_test();
    let failed_delivery = within(client.pull_default()).await;
    assert!(
        failed_delivery.is_err(),
        "a failed delivered erasure must report failure"
    );
    assert!(
        edge.__erasure_boundary_persist_fault_reached_for_test(),
        "the delivered unit reached destructive persistence"
    );
    assert_eq!(
        whole_store(&edge),
        before_edge_apply,
        "no prefix of the delivered list becomes visible"
    );
    within(client.pull_default())
        .await
        .expect("the edge receives the erasure");
    assert_eq!(
        (
            row_count(&edge, "records", "selected"),
            row_count(&edge, "record_parts", "selected"),
            row_count(&edge, "record_extracts", "selected"),
        ),
        (0, 0, 0),
        "delivery erases every listed selection, including the SYNC OFF derived row"
    );
    let after_delivery = whole_store(&edge);
    within(client.pull_default())
        .await
        .expect("a repeat exchange is idempotent");
    assert_eq!(
        whole_store(&edge),
        after_delivery,
        "asking again restores nothing and erases nothing further"
    );

    // Zero hub matches must still deliver the explicit local selection.
    write_derived_row(&edge, "edge-only", Uuid::new_v4());
    let zero = reopened
        .execute(
            "PURGE FROM record_extracts WHERE batch_ref = $batch",
            &HashMap::from([("batch".into(), text("edge-only"))]),
        )
        .unwrap();
    assert_eq!(zero.rows_affected, 0);
    within(client.pull_default())
        .await
        .expect("zero-match hub instruction reaches the edge");
    assert_eq!(row_count(&edge, "record_extracts", "edge-only"), 0);

    // Deliberately replay the actual ordinary purge plane after fresh creation.
    write_derived_row(&edge, "selected", selected);
    within(client.shutdown()).await;
    edge.persist_sync_pull_watermark(&TenantId::from(tenant), contextdb_core::Lsn(0))
        .unwrap();
    let replay = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&ticket, &edge_identity),
        TenantId::from(tenant),
    );
    within(replay.pull_default())
        .await
        .expect("re-delivered instruction remains applied");
    assert_eq!(
        row_count(&edge, "record_extracts", "selected"),
        1,
        "fresh post-application local lineage survives the actual replay"
    );
    within(replay.shutdown()).await;

    within(client.shutdown()).await;
    stop.store(true, Ordering::SeqCst);
    within(task).await.expect("the restarted hub stops cleanly");
}

// Declared discard modes govern one local transaction, with no fleet tombstone.
#[tokio::test]
async fn edge_discard_follows_declared_modes_and_one_local_transaction_boundary() {
    discard_never().await;
    discard_after_outcome().await;
    discard_always().await;
    discard_persistence_failure().await;
    discard_transaction_writes();
}

async fn discard_never() {
    let _clock = Wallclock::test_clock_guard(|| T0);
    let list = "DISCARD FROM records WHERE batch_ref = $batch, \
         record_parts WHERE batch_ref = $batch, \
         record_extracts WHERE batch_ref = $batch";

    // ---- the tenant says never ---------------------------------------------
    {
        let root = tempfile::tempdir().expect("temporary test directory");
        let tenant = "edge-discard-never";
        let hub = start_hub(root.path(), tenant, ROOT_CLAUSES_NEVER).await;
        let edge = open_edge(root.path(), "edge");
        let identity = root.path().join("edge.db.fabric-identity.key");
        let client = SyncClient::new(
            edge.clone(),
            &peer_dial_spec(&hub.ticket, &identity),
            TenantId::from(tenant),
        );
        bind_edge(&edge, &client, ROOT_CLAUSES_NEVER).await;
        // An unbound push-only name is eligible even on this enrolled edge.
        edge.execute(
            "CREATE TABLE unbound_notes (id INTEGER PRIMARY KEY, body TEXT) SYNC PUSH ONLY",
            &p(),
        )
        .unwrap();
        edge.execute("INSERT INTO unbound_notes VALUES (1, 'local')", &p())
            .unwrap();
        let discarded = edge
            .execute("DISCARD FROM unbound_notes WHERE id = 1", &p())
            .unwrap();
        assert_eq!(discarded.rows_affected, 1);
        assert!(
            edge.execute("SELECT * FROM unbound_notes", &p())
                .unwrap()
                .rows
                .is_empty()
        );

        let kept_root = Uuid::new_v4();
        let kept_member = Uuid::new_v4();
        let tx = stage_unit(&edge, "kept", kept_root, &[kept_member]);
        client
            .__stage_delivery_manifest_for_test(
                tx,
                DeliveryManifest {
                    root_table: "records",
                    root_key: key_of(kept_root),
                    members: vec![("record_parts", key_of(kept_member))],
                },
            )
            .expect("install the never-discard unit's signed manifest prerequisite");
        edge.commit(tx)
            .expect("commit rows and delivery metadata atomically");

        within(client.__seed_delivery_outcome_for_test(
            "records",
            &key_of(kept_root),
            DeliveryOutcomeKind::Accepted,
            None,
        ))
        .await
        .expect("canonical terminal before mode refusal");
        let before = whole_store(&edge);
        let (result, selections) = observe_selection(&edge, || {
            edge.execute(list, &HashMap::from([("batch".to_string(), text("kept"))]))
        });
        assert_eq!(
            selections, 0,
            "NEVER refuses before any row source is entered"
        );
        match result {
            Err(Error::EdgeDiscardDenied {
                hub_node_id,
                table,
                mode,
                pending_count,
            }) => {
                assert_eq!(hub_node_id, hub.node_id, "the refusal names the hub");
                assert_eq!(
                    table, "records",
                    "the refusal names the table that forbids it"
                );
                assert_eq!(
                    mode,
                    EdgeDiscardMode::Never,
                    "the refusal names the declared mode"
                );
                assert_eq!(pending_count, 0, "nothing is owed; the mode alone refuses");
            }
            other => {
                panic!("a table declared never-discard refuses the whole statement, got {other:?}")
            }
        }
        let edge_path = root.path().join("edge.db");
        within(client.shutdown()).await;
        drop(client);
        drop(edge);
        let reopened = Database::open(&edge_path).expect("reopen the edge store");
        assert_eq!(
            whole_store(&reopened),
            before,
            "every listed table is unchanged on reopen"
        );
        hub.stop().await;
    }
}

async fn discard_after_outcome() {
    let _clock = Wallclock::test_clock_guard(|| T0);
    let list = "DISCARD FROM records WHERE batch_ref = $batch, \
         record_parts WHERE batch_ref = $batch, \
         record_extracts WHERE batch_ref = $batch";
    // ---- the tenant is silent, so the effective mode governs ----------------
    {
        let root = tempfile::tempdir().expect("temporary test directory");
        let tenant = "edge-discard-silent";
        let hub = start_hub(root.path(), tenant, ROOT_CLAUSES_SILENT_DISCARD).await;
        let edge = open_edge(root.path(), "edge");
        let identity = root.path().join("edge.db.fabric-identity.key");
        let client = SyncClient::new(
            edge.clone(),
            &peer_dial_spec(&hub.ticket, &identity),
            TenantId::from(tenant),
        );
        // Establish actual durable hub authority through the public binding.
        within(
            client.bind_application_table_policy(
                ApplicationTablePolicyExpectation::new()
                    .expect_table("records", ROOT_CLAUSES_SILENT_DISCARD)
                    .unwrap()
                    .expect_table("record_parts", MEMBER_CLAUSES)
                    .unwrap(),
            ),
        )
        .await
        .unwrap();
        create_tables(&edge, ROOT_CLAUSES_SILENT_DISCARD);
        let policy = hub
            .db
            .execute("SHOW TENANT TABLE POLICY FOR records", &p())
            .unwrap();
        let mode = policy
            .columns
            .iter()
            .position(|column| column == "edge_discard")
            .unwrap();
        assert_eq!(policy.rows[0][mode], text("after_outcome"));
        within(client.shutdown()).await; // Enforcement below reads the persisted binding offline.

        // Written but not yet offered: the hub has said nothing about it.
        let waiting_root = Uuid::new_v4();
        let waiting_member = Uuid::new_v4();
        let tx = stage_unit(&edge, "waiting", waiting_root, &[waiting_member]);
        client
            .__stage_delivery_manifest_for_test(
                tx,
                DeliveryManifest {
                    root_table: "records",
                    root_key: key_of(waiting_root),
                    members: vec![("record_parts", key_of(waiting_member))],
                },
            )
            .expect("canonical pending registration");
        edge.commit(tx)
            .expect("commit rows and delivery metadata atomically");

        let params = HashMap::from([("batch".to_string(), text("waiting"))]);
        let before = whole_store(&edge);
        let (result, selections) = observe_selection(&edge, || edge.execute(list, &params));
        assert!(
            selections > 0,
            "AFTER OUTCOME selects the actual owning units before checking pending"
        );
        match result {
            Err(Error::EdgeDiscardDenied {
                hub_node_id,
                table,
                mode,
                pending_count,
            }) => {
                assert_eq!(hub_node_id, hub.node_id);
                assert_eq!(table, "records");
                assert_eq!(
                    mode,
                    EdgeDiscardMode::AfterOutcome,
                    "silence in the declaration enforces as the effective mode, and \
                     the refusal says which mode it enforced"
                );
                assert_eq!(
                    pending_count, 1,
                    "the refusal names how many units are still owed"
                );
            }
            other => panic!("an unanswered unit holds the erasure back, got {other:?}"),
        }
        assert_eq!(
            whole_store(&edge),
            before,
            "the refusal changed nothing, so the caller can wait and retry"
        );

        let client = SyncClient::new(
            edge.clone(),
            &peer_dial_spec(&hub.ticket, &identity),
            TenantId::from(tenant),
        );
        // Once the hub has answered, the same statement goes through.
        within(client.__seed_delivery_outcome_for_test(
            "records",
            &key_of(waiting_root),
            DeliveryOutcomeKind::Accepted,
            None,
        ))
        .await
        .expect("canonical terminal before permitted erasure");
        let hub_before = whole_store(&hub.db);
        let hub_outcomes = outcome_words(&hub.db);
        let pending_before = client
            .pending_push_change_count()
            .expect("the edge's own outstanding work");
        let discarded = edge
            .execute(list, &params)
            .expect("the erasure goes through");
        assert_eq!(
            normalize_erasure(&discarded),
            BTreeMap::from([
                (
                    "records".to_string(),
                    ErasureTableResult {
                        affected: 1,
                        pending_units: Some(0),
                        survivors: serde_json::json!([]),
                    },
                ),
                (
                    "record_parts".to_string(),
                    ErasureTableResult {
                        affected: 1,
                        pending_units: Some(0),
                        survivors: serde_json::json!([]),
                    },
                ),
                (
                    "record_extracts".to_string(),
                    ErasureTableResult {
                        affected: 1,
                        pending_units: Some(0),
                        survivors: serde_json::json!([]),
                    },
                ),
            ]),
            "every selected table reports affected rows, survivors, and no pending unit"
        );
        assert_eq!(
            (
                row_count(&edge, "records", "waiting"),
                row_count(&edge, "record_parts", "waiting"),
                row_count(&edge, "record_extracts", "waiting"),
            ),
            (0, 0, 0),
            "the whole selection is gone from this node"
        );
        assert_eq!(
            edge.delivery_status("records")
                .expect("edge totals")
                .eligible,
            0,
            "the edge no longer counts a unit it no longer holds"
        );
        assert!(
            client
                .pending_push_change_count()
                .expect("remaining edge work")
                <= pending_before,
            "discard creates no new obligation to the fleet"
        );
        within(client.push())
            .await
            .expect("the next exchange carries no erasure to the hub");
        assert_eq!(
            whole_store(&hub.db),
            hub_before,
            "the hub's rows are exactly as they were: the authority's copy is not the \
             edge's to drop"
        );
        assert_eq!(
            outcome_words(&hub.db),
            hub_outcomes,
            "and the hub's own record of the unit is untouched"
        );

        stage_manifested_unit(&edge, &client, "waiting", waiting_root, &[waiting_member]);
        within(client.push()).await.unwrap();
        assert_eq!(
            edge.delivery_outcome("records", &key_of(waiting_root))
                .unwrap()
                .unwrap()
                .kind(),
            DeliveryOutcomeKind::Equivalent,
            "discard permits identical re-import while the hub retains its copy"
        );
        assert_eq!(whole_store(&hub.db), hub_before);
        within(client.shutdown()).await;
        // Durable hub authority survives closing the server and handle.
        let hub_metadata = hub.db.__delivery_metadata_bytes_for_test().unwrap();
        let hub_rows = whole_store(&hub.db);
        hub.stop().await;
        let offline = Database::open(root.path().join("hub.db")).expect("reopen the hub offline");
        let (refusal, selections) = observe_selection(&offline, || {
            offline.execute(
                list,
                &HashMap::from([("batch".to_string(), text("waiting"))]),
            )
        });
        assert!(
            matches!(refusal, Err(Error::DiscardNotOnHub)),
            "offline authority must use PURGE: {refusal:?}"
        );
        assert_eq!(selections, 0, "hub refusal precedes every row selection");
        assert_eq!(whole_store(&offline), hub_rows);
        assert_eq!(
            offline.__delivery_metadata_bytes_for_test().unwrap(),
            hub_metadata,
            "manifests and outcomes survive the refused erasure"
        );
        drop(offline);
        let reopened = Database::open(root.path().join("hub.db")).unwrap();
        assert_eq!(whole_store(&reopened), hub_rows);
        assert_eq!(
            reopened.__delivery_metadata_bytes_for_test().unwrap(),
            hub_metadata
        );
    }
}

async fn discard_always() {
    let _clock = Wallclock::test_clock_guard(|| T0);
    let list = "DISCARD FROM records WHERE batch_ref = $batch, \
         record_parts WHERE batch_ref = $batch, \
         record_extracts WHERE batch_ref = $batch";
    // ---- the tenant says always, and the refusals that precede selection ----
    {
        let root = tempfile::tempdir().expect("temporary test directory");
        let tenant = "edge-discard-always";
        let hub = start_hub_with_member_clauses(
            root.path(),
            tenant,
            ROOT_CLAUSES_ALWAYS,
            MEMBER_CLAUSES_ALWAYS,
        )
        .await;
        let edge = open_edge(root.path(), "edge");
        let identity = root.path().join("edge.db.fabric-identity.key");
        let client = SyncClient::new(
            edge.clone(),
            &peer_dial_spec(&hub.ticket, &identity),
            TenantId::from(tenant),
        );
        bind_edge_with_member_clauses(&edge, &client, ROOT_CLAUSES_ALWAYS, MEMBER_CLAUSES_ALWAYS)
            .await;
        let unanswered_root = Uuid::new_v4();
        let unanswered_member = Uuid::new_v4();
        let tx = stage_unit(&edge, "unanswered", unanswered_root, &[unanswered_member]);
        client
            .__stage_delivery_manifest_for_test(
                tx,
                DeliveryManifest {
                    root_table: "records",
                    root_key: key_of(unanswered_root),
                    members: vec![("record_parts", key_of(unanswered_member))],
                },
            )
            .expect("canonical pending registration");
        edge.commit(tx)
            .expect("commit rows and delivery metadata atomically");

        let params = HashMap::from([("batch".to_string(), text("unanswered"))]);

        // A table the node can be asked to serve back may not be listed, and the
        // refusal comes before any selection.
        edge.execute(
            "CREATE TABLE shared_notes (id UUID PRIMARY KEY, batch_ref TEXT) SYNC TWO WAY",
            &p(),
        )
        .expect("a two-way table on the same node");
        let before = whole_store(&edge);
        let (result, selections) = observe_selection(&edge, || {
            edge.execute(
                "DISCARD FROM records WHERE batch_ref = $batch, \
                 shared_notes WHERE batch_ref = $batch",
                &params,
            )
        });
        assert_eq!(
            selections, 0,
            "an ineligible table refuses before any listed selection"
        );
        match result {
            Err(Error::DiscardNotEligible { table, direction }) => {
                assert_eq!(table, "shared_notes", "the refusal names the table");
                assert_eq!(
                    direction, "two_way",
                    "and why it cannot be dropped: the next exchange would just fill \
                     it again"
                );
            }
            other => {
                panic!("a table this node can be asked to serve back is refused, got {other:?}")
            }
        }
        assert_eq!(whole_store(&edge), before, "the refusal changed no row");

        // The authority itself never discards; it is told the verb it owns.
        let hub_before = whole_store(&hub.db);
        let (result, selections) = observe_selection(&hub.db, || {
            hub.db
                .execute("DISCARD FROM records WHERE batch_ref = $batch", &params)
        });
        assert_eq!(selections, 0, "hub DISCARD refuses before row selection");
        match result {
            Err(error @ Error::DiscardNotOnHub) => assert!(error.to_string().contains("PURGE")),
            other => panic!("the authority's copy is not dropped this way, got {other:?}"),
        }
        assert_eq!(whole_store(&hub.db), hub_before);

        // A crash between the statement and its commit leaves nothing half-done.
        let edge_path = root.path().join("edge.db");
        let before_crash = whole_store(&edge);
        let tx = edge.begin().expect("open a transaction");
        let (result, selections) =
            observe_selection(&edge, || edge.execute_in_tx(tx, list, &params));
        result.expect(
            "the erasure is legal inside a transaction, because it owes the \
                     fleet nothing",
        );
        assert!(
            selections > 0,
            "legal DISCARD enters the actual selection source"
        );
        within(client.shutdown()).await;
        drop(client);
        drop(edge);
        let reopened = Arc::new(Database::open(&edge_path).expect("reopen the edge store"));
        assert_eq!(
            whole_store(&reopened),
            before_crash,
            "an erasure whose transaction never committed destroyed nothing, in any \
             of the listed tables"
        );

        // Explicit rollback and commit use real transaction overlays. The
        // unrelated insert never overlaps a selected row or its table.
        reopened
            .execute(
                "CREATE TABLE local_notes (id INTEGER PRIMARY KEY, body TEXT) SYNC OFF",
                &p(),
            )
            .unwrap();
        let independent = reopened.scoped_with_constraints(None, None, None);
        let reader = independent.read_session(Default::default()).unwrap();
        let committed_rows = whole_store(&independent);
        let committed_metadata = reopened.__delivery_metadata_bytes_for_test().unwrap();
        let committed_changes = reopened.current_lsn();
        let selected_origins = [
            ("records", unanswered_root),
            ("record_parts", unanswered_member),
        ]
        .map(|(table, key)| {
            let origin = reopened
                .authoritative_purge_current_live_row_sidecar_for_test(table, &key_of(key))
                .expect("the selected manifested row has genuine source lineage");
            (table, key, origin)
        });
        let mut discarded = None;
        for commit in [false, true] {
            let tx = reopened
                .begin()
                .expect("begin explicit local erasure transaction");
            reopened
                .execute_in_tx(
                    tx,
                    "INSERT INTO local_notes (id, body) VALUES (1, 'committed together')",
                    &p(),
                )
                .expect("stage unrelated ordinary work before selecting erasure");
            let (result, selections) =
                observe_selection(&reopened, || reopened.execute_in_tx(tx, list, &params));
            let result = result.expect("every selected bound table permits pending erasure");
            assert!(
                selections > 0,
                "legal selection is observed inside the transaction"
            );
            for table in ["records", "record_parts", "record_extracts"] {
                let sql = format!("SELECT id FROM {table} WHERE batch_ref = $batch");
                assert!(
                    reopened
                        .execute_in_tx(tx, &sql, &params)
                        .unwrap()
                        .rows
                        .is_empty(),
                    "the erasing transaction sees its own absent {table} rows"
                );
                assert_eq!(
                    reader.execute(&sql, &params).unwrap().rows.len(),
                    1,
                    "an independent reader still sees the committed {table} selection"
                );
            }
            assert_eq!(
                reopened
                    .execute_in_tx(tx, "SELECT id FROM local_notes", &p())
                    .unwrap()
                    .rows
                    .len(),
                1
            );
            assert!(
                reader
                    .execute("SELECT id FROM local_notes", &p())
                    .unwrap()
                    .rows
                    .is_empty()
            );
            assert_eq!(whole_store(&independent), committed_rows);
            assert_eq!(
                reopened.__delivery_metadata_bytes_for_test().unwrap(),
                committed_metadata,
                "selection stages no committed delivery-metadata erasure"
            );
            assert_eq!(
                reopened.current_lsn(),
                committed_changes,
                "neither erasure nor ordinary writes publish before COMMIT"
            );
            if commit {
                reopened
                    .commit(tx)
                    .expect("publish erasure and unrelated ordinary work together");
                discarded = Some(result);
            } else {
                reopened
                    .rollback(tx)
                    .expect("explicit rollback restores the selection");
                assert_eq!(whole_store(&independent), committed_rows);
                assert_eq!(
                    reopened.__delivery_metadata_bytes_for_test().unwrap(),
                    committed_metadata
                );
                assert!(
                    reader
                        .execute("SELECT id FROM local_notes", &p())
                        .unwrap()
                        .rows
                        .is_empty()
                );
                assert_eq!(reopened.current_lsn(), committed_changes);
            }
        }
        let discarded = discarded.expect("the second transaction committed");
        for table in ["records", "record_parts", "record_extracts"] {
            assert!(
                reader
                    .execute(
                        &format!("SELECT id FROM {table} WHERE batch_ref = $batch"),
                        &params,
                    )
                    .unwrap()
                    .rows
                    .is_empty(),
                "the independent reader sees the committed {table} erasure"
            );
        }
        assert_eq!(
            reader
                .execute("SELECT body FROM local_notes", &p())
                .unwrap()
                .rows,
            vec![vec![text("committed together")]],
            "the unrelated ordinary write becomes visible with the erasure"
        );
        assert!(reopened.current_lsn() > committed_changes);
        let changes = reopened.changes_since(committed_changes);
        for (table, key, origin) in &selected_origins {
            assert_eq!(
                reopened.classify_authoritative_purge_root_for_test(
                    table,
                    origin.table_generation,
                    &key_of(*key),
                    &origin.lineage_root,
                ),
                contextdb_engine::database::AuthoritativePurgeRootClassification::NotPurged,
                "the discarded source lineage has no fleet tombstone"
            );
        }
        assert!(
            changes.rows.iter().all(|row| !row.deleted),
            "local discard produces no ordinary synced delete either"
        );
        assert_eq!(
            whole_store(&hub.db),
            hub_before,
            "local commit leaves the hub untouched"
        );
        assert_eq!(
            normalize_erasure(&discarded),
            BTreeMap::from([
                (
                    "records".to_string(),
                    ErasureTableResult {
                        affected: 1,
                        pending_units: Some(1),
                        survivors: serde_json::json!([]),
                    },
                ),
                (
                    "record_parts".to_string(),
                    ErasureTableResult {
                        affected: 1,
                        pending_units: Some(0),
                        survivors: serde_json::json!([]),
                    },
                ),
                (
                    "record_extracts".to_string(),
                    ErasureTableResult {
                        affected: 1,
                        pending_units: Some(0),
                        survivors: serde_json::json!([]),
                    },
                ),
            ]),
            "each selected table reports its affected rows, survivors, and its real pending count"
        );
        assert_eq!(
            row_count(&reopened, "records", "unanswered"),
            0,
            "the selection is gone"
        );
        assert_eq!(
            reopened
                .delivery_status("records")
                .expect("edge totals")
                .eligible,
            0,
            "and nothing of it is counted any more"
        );
        drop(reader);
        drop(independent);
        drop(reopened);
        let committed = Database::open(&edge_path).expect("reopen after explicit COMMIT");
        for table in ["records", "record_parts", "record_extracts"] {
            assert_eq!(row_count(&committed, table, "unanswered"), 0);
        }
        assert_eq!(
            committed
                .execute("SELECT body FROM local_notes", &p())
                .unwrap()
                .rows,
            vec![vec![text("committed together")]],
            "erasure and the unrelated ordinary write survive reopen together"
        );
        for (table, key, origin) in &selected_origins {
            assert_eq!(
                committed.classify_authoritative_purge_root_for_test(
                    table,
                    origin.table_generation,
                    &key_of(*key),
                    &origin.lineage_root,
                ),
                contextdb_engine::database::AuthoritativePurgeRootClassification::NotPurged
            );
        }
        hub.stop().await;
    }
}

async fn discard_persistence_failure() {
    let root = tempfile::tempdir().unwrap();
    let tenant = "discard-persistence-failure";
    let hub = start_hub_with_member_clauses(
        root.path(),
        tenant,
        ROOT_CLAUSES_ALWAYS,
        MEMBER_CLAUSES_ALWAYS,
    )
    .await;
    let edge = open_edge(root.path(), "edge");
    let client = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&hub.ticket, &root.path().join("edge.key")),
        TenantId::from(tenant),
    );
    bind_edge_with_member_clauses(&edge, &client, ROOT_CLAUSES_ALWAYS, MEMBER_CLAUSES_ALWAYS).await;
    stage_manifested_unit(
        &edge,
        &client,
        "selected",
        Uuid::new_v4(),
        &[Uuid::new_v4()],
    );
    let before = (
        whole_store(&edge),
        edge.__delivery_metadata_bytes_for_test().unwrap(),
    );
    edge.__arm_erasure_boundary_persist_fault_for_test();
    let failed = edge.execute("DISCARD FROM records, record_parts, record_extracts", &p());
    assert!(edge.__erasure_boundary_persist_fault_reached_for_test());
    assert!(
        matches!(failed, Err(Error::Other(ref message)) if message == "storage error: erasure first table staged fault injected")
    );
    within(client.shutdown()).await;
    drop(client);
    drop(edge);
    let reopened = Database::open(root.path().join("edge.db")).unwrap();
    assert_eq!(
        (
            whole_store(&reopened),
            reopened.__delivery_metadata_bytes_for_test().unwrap()
        ),
        before,
        "a failure after the first table is staged preserves every table and its custody metadata on reopen"
    );
    hub.stop().await;
}

// Insert/update followed by DISCARD is one ordinary transaction.
fn discard_transaction_writes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("transaction-discard.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, embedding VECTOR(2)) SYNC OFF",
        &p(),
    )
    .unwrap();
    db.execute("INSERT INTO notes VALUES (1, 'original', [1.0, 0.0])", &p())
        .unwrap();
    let original = db.execute("SELECT * FROM notes", &p()).unwrap().rows;
    for commit in [false, true] {
        let tx = db.begin().unwrap();
        db.execute_in_tx(
            tx,
            "UPDATE notes SET body = 'updated', embedding = [0.0, 1.0] WHERE id = 1",
            &p(),
        )
        .unwrap();
        db.execute_in_tx(
            tx,
            "INSERT INTO notes VALUES (2, 'fresh', [1.0, 1.0])",
            &p(),
        )
        .unwrap();
        let result = db
            .execute_in_tx(tx, "DISCARD FROM notes WHERE id IN (1, 2)", &p())
            .unwrap();
        assert_eq!(result.rows_affected, 2);
        assert!(
            db.execute_in_tx(tx, "SELECT * FROM notes", &p())
                .unwrap()
                .rows
                .is_empty()
        );
        assert_eq!(
            db.execute("SELECT * FROM notes", &p()).unwrap().rows,
            original
        );
        if commit {
            db.commit(tx).unwrap();
        } else {
            db.rollback(tx).unwrap();
            assert_eq!(
                db.execute("SELECT * FROM notes", &p()).unwrap().rows,
                original
            );
        }
    }
    drop(db);
    let db = Database::open(&path).unwrap();
    assert!(
        db.execute("SELECT * FROM notes", &p())
            .unwrap()
            .rows
            .is_empty()
    );
    db.execute("INSERT INTO notes VALUES (1, 'new life', [1.0, 0.0])", &p())
        .unwrap();
    assert_eq!(
        db.execute("SELECT body FROM notes", &p()).unwrap().rows,
        vec![vec![Value::Text("new life".into())]]
    );
}
