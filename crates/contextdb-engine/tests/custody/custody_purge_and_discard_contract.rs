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
            .columns()
            .iter()
            .position(|column| column == name)
            .unwrap_or_else(|| panic!("erasure representation supplies its {name} field"))
    };
    let table = column("table");
    let affected = column("rows_affected");
    let survivors = column("survivors");
    let pending_units = result
        .columns()
        .iter()
        .position(|column| column == "pending_units");
    result
        .rows()
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
    let server = Arc::new(
        SyncServer::new(db.clone(), &endpoint, TenantId::from(tenant))
            .expect("construct the authenticated sync endpoint"),
    );
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
            db.declared_reader_memory()
                .expect("admit the fixture client reader"),
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
            db.declared_reader_memory()
                .expect("admit the fixture client reader"),
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
    .rows()
    .len()
}

fn total_rows(db: &Database, table: &str) -> usize {
    db.execute(&format!("SELECT id FROM {table}"), &p())
        .unwrap_or_else(|err| panic!("{table} must answer: {err}"))
        .rows()
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
            format!("{table}:{:?}", result.rows())
        })
        .collect()
}

fn outcome_words(db: &Database) -> Vec<String> {
    let result = db
        .execute("SHOW DELIVERY OUTCOMES FOR records", &p())
        .expect("outcomes render");
    let outcome_column = result
        .columns()
        .iter()
        .position(|column| column == "outcome")
        .expect("outcomes name their verdict column");
    result
        .rows()
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
    )
    .expect("construct the authenticated sync endpoint");
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
            edge.declared_reader_memory()
                .expect("admit the fixture client reader"),
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
            edge.declared_reader_memory()
                .expect("admit the fixture client reader"),
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
    )
    .expect("construct the authenticated sync endpoint");
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
    )
    .expect("construct the authenticated sync endpoint");
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
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
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
    )
    .expect("construct the authenticated sync endpoint");
    bind_edge(&edge, &client, ROOT_CLAUSES_AFTER_OUTCOME).await;

    let selected = Uuid::new_v4();
    let selected_member = Uuid::new_v4();
    let survivor = Uuid::new_v4();
    stage_manifested_unit(&edge, &client, "selected", selected, &[selected_member]);
    within(
        client.__seed_delivery_outcome_for_test(
            "records",
            &key_of(selected),
            DeliveryOutcomeKind::Accepted,
            None,
            edge.declared_reader_memory()
                .expect("admit the fixture client reader"),
        ),
    )
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
        listed.rows_affected(),
        3,
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
    let mixed_rows_affected = mixed.rows_affected();
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
    let server = Arc::new(
        SyncServer::new(reopened.clone(), &endpoint, TenantId::from(tenant))
            .expect("construct the authenticated sync endpoint"),
    );
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
    )
    .expect("construct the authenticated sync endpoint");
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
    assert_eq!(zero.rows_affected(), 0);
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
    )
    .expect("construct the authenticated sync endpoint");
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

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
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
        )
        .expect("construct the authenticated sync endpoint");
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
        assert_eq!(discarded.rows_affected(), 1);
        assert!(
            edge.execute("SELECT * FROM unbound_notes", &p())
                .unwrap()
                .rows()
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
                edge.declared_reader_memory()
                    .expect("admit the fixture client reader"),
            )
            .expect("install the never-discard unit's signed manifest prerequisite");
        edge.commit(tx)
            .expect("commit rows and delivery metadata atomically");

        within(
            client.__seed_delivery_outcome_for_test(
                "records",
                &key_of(kept_root),
                DeliveryOutcomeKind::Accepted,
                None,
                edge.declared_reader_memory()
                    .expect("admit the fixture client reader"),
            ),
        )
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

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
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
        )
        .expect("construct the authenticated sync endpoint");
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
            .columns()
            .iter()
            .position(|column| column == "edge_discard")
            .unwrap();
        assert_eq!(policy.rows()[0][mode], text("after_outcome"));
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
                edge.declared_reader_memory()
                    .expect("admit the fixture client reader"),
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
        )
        .expect("construct the authenticated sync endpoint");
        // Once the hub has answered, the same statement goes through.
        within(
            client.__seed_delivery_outcome_for_test(
                "records",
                &key_of(waiting_root),
                DeliveryOutcomeKind::Accepted,
                None,
                edge.declared_reader_memory()
                    .expect("admit the fixture client reader"),
            ),
        )
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

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
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
        )
        .expect("construct the authenticated sync endpoint");
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
                edge.declared_reader_memory()
                    .expect("admit the fixture client reader"),
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
                        .rows()
                        .is_empty(),
                    "the erasing transaction sees its own absent {table} rows"
                );
                assert_eq!(
                    reader.execute(&sql, &params).unwrap().rows().len(),
                    1,
                    "an independent reader still sees the committed {table} selection"
                );
            }
            assert_eq!(
                reopened
                    .execute_in_tx(tx, "SELECT id FROM local_notes", &p())
                    .unwrap()
                    .rows()
                    .len(),
                1
            );
            assert!(
                reader
                    .execute("SELECT id FROM local_notes", &p())
                    .unwrap()
                    .rows()
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
                        .rows()
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
                    .rows()
                    .is_empty(),
                "the independent reader sees the committed {table} erasure"
            );
        }
        assert_eq!(
            reader
                .execute("SELECT body FROM local_notes", &p())
                .unwrap()
                .rows(),
            vec![vec![text("committed together")]],
            "the unrelated ordinary write becomes visible with the erasure"
        );
        assert!(reopened.current_lsn() > committed_changes);
        let changes = reopened
            .changes_since(committed_changes)
            .expect("read the declared changes");
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
                .rows(),
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
    )
    .expect("construct the authenticated sync endpoint");
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
    let original_answer = db.execute("SELECT * FROM notes", &p()).unwrap();
    let original = original_answer.rows();
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
        assert_eq!(result.rows_affected(), 2);
        assert!(
            db.execute_in_tx(tx, "SELECT * FROM notes", &p())
                .unwrap()
                .rows()
                .is_empty()
        );
        assert_eq!(
            db.execute("SELECT * FROM notes", &p()).unwrap().rows(),
            original
        );
        if commit {
            db.commit(tx).unwrap();
        } else {
            db.rollback(tx).unwrap();
            assert_eq!(
                db.execute("SELECT * FROM notes", &p()).unwrap().rows(),
                original
            );
        }
    }
    drop(db);
    let db = Database::open(&path).unwrap();
    assert!(
        db.execute("SELECT * FROM notes", &p())
            .unwrap()
            .rows()
            .is_empty()
    );
    db.execute("INSERT INTO notes VALUES (1, 'new life', [1.0, 0.0])", &p())
        .unwrap();
    assert_eq!(
        db.execute("SELECT body FROM notes", &p()).unwrap().rows(),
        vec![vec![Value::Text("new life".into())]]
    );
}

// A purge tombstone bans the purged lineage, never the key. A write
// to a purged key after the purge is a new life that PURGE erases like any
// other, while every earlier purged life stays permanently provable.
const REPEATED_KEY: Uuid = Uuid::from_u128(0x0000_0000_0000_4000_8000_0000_0000_0001);

fn repeated_key_params() -> HashMap<String, Value> {
    HashMap::from([("id".to_string(), Value::Uuid(REPEATED_KEY))])
}

fn insert_repeated_key(db: &Database, table: &str) {
    db.execute(
        &format!("INSERT INTO {table} (id, batch) VALUES ($id, 'a')"),
        &repeated_key_params(),
    )
    .expect("a write to a purged key is a new life and is accepted");
    assert_eq!(
        db.execute(
            &format!("SELECT batch FROM {table} WHERE id = $id"),
            &repeated_key_params(),
        )
        .expect("read the repeated key")
        .rows(),
        vec![vec![text("a")]],
        "the new life at the purged key is visible"
    );
}

fn purge_batch_a(db: &Database, table: &str) -> u64 {
    db.execute(&format!("PURGE FROM {table} WHERE batch = 'a'"), &p())
        .expect("a standalone PURGE of the batch commits")
        .rows_affected()
}

/// The lineage root and permanent frontier the key's current lifecycle names.
fn current_purged_life(db: &Database, table: &str) -> (String, u64) {
    let state = db.durable_deletion_state_for_test(table, &Value::Uuid(REPEATED_KEY));
    let root = state
        .lineage_root
        .expect("the purged key names the purged lineage root");
    let frontier = state
        .purge_frontier
        .expect("the purged key carries a permanent frontier")
        .parse::<u64>()
        .expect("the permanent frontier is an LSN");
    (root, frontier)
}

fn classify_repeated_key_root(
    db: &Database,
    table: &str,
    root: &str,
) -> contextdb_engine::database::AuthoritativePurgeRootClassification {
    let generation = db
        .durable_deletion_state_for_test(table, &Value::Uuid(REPEATED_KEY))
        .table_generation
        .expect("the key keeps its lineage lifecycle");
    db.classify_authoritative_purge_root_for_test(table, generation, &key_of(REPEATED_KEY), root)
}

fn purged_at(frontier: u64) -> contextdb_engine::database::AuthoritativePurgeRootClassification {
    contextdb_engine::database::AuthoritativePurgeRootClassification::Purged {
        permanent_frontier: contextdb_core::Lsn(frontier),
    }
}

fn row_total(db: &Database, table: &str) -> usize {
    db.execute(&format!("SELECT id FROM {table}"), &p())
        .expect("count the table")
        .rows()
        .len()
}

#[test]
fn a_purge_erases_a_row_written_again_at_a_purged_key_and_every_purged_life_stays_refused() {
    let root = tempfile::tempdir().expect("temporary test directory");
    let db = Database::open(root.path().join("repeated-key.db")).expect("open the store");
    db.execute(
        "CREATE TABLE records (id UUID PRIMARY KEY, batch TEXT NOT NULL) SYNC OFF",
        &p(),
    )
    .expect("declare the node-local table");

    insert_repeated_key(&db, "records");
    assert_eq!(
        purge_batch_a(&db, "records"),
        1,
        "the first PURGE erases one row"
    );
    let (first_root, first_frontier) = current_purged_life(&db, "records");

    insert_repeated_key(&db, "records");
    assert_eq!(
        purge_batch_a(&db, "records"),
        1,
        "the second PURGE erases the row written after the first purge"
    );
    assert_eq!(row_total(&db, "records"), 0, "no life of the key survives");
    let (second_root, second_frontier) = current_purged_life(&db, "records");
    assert_ne!(
        second_root, first_root,
        "the new write started a new lineage"
    );
    assert!(second_frontier > first_frontier);
    assert_eq!(
        classify_repeated_key_root(&db, "records", &first_root),
        purged_at(first_frontier),
        "the first life keeps its own permanent tombstone"
    );
    assert_eq!(
        classify_repeated_key_root(&db, "records", &second_root),
        purged_at(second_frontier)
    );

    // A local DISCARD of a later life leaves every fleet tombstone in place.
    insert_repeated_key(&db, "records");
    assert_eq!(
        db.execute("DISCARD FROM records WHERE batch = 'a'", &p())
            .expect("DISCARD of a later life at a purged key commits")
            .rows_affected(),
        1
    );
    assert_eq!(row_total(&db, "records"), 0);
    assert_eq!(
        classify_repeated_key_root(&db, "records", &first_root),
        purged_at(first_frontier)
    );
    assert_eq!(
        classify_repeated_key_root(&db, "records", &second_root),
        purged_at(second_frontier)
    );

    insert_repeated_key(&db, "records");
    assert_eq!(purge_batch_a(&db, "records"), 1);
    let (third_root, third_frontier) = current_purged_life(&db, "records");
    assert!(third_root != first_root && third_root != second_root);
    for (root, frontier) in [
        (&first_root, first_frontier),
        (&second_root, second_frontier),
        (&third_root, third_frontier),
    ] {
        assert_eq!(
            classify_repeated_key_root(&db, "records", root),
            purged_at(frontier)
        );
    }

    assert_eq!(
        purge_batch_a(&db, "records"),
        0,
        "a PURGE that selects nothing erases nothing"
    );
    for (root, frontier) in [
        (&first_root, first_frontier),
        (&second_root, second_frontier),
        (&third_root, third_frontier),
    ] {
        assert_eq!(
            classify_repeated_key_root(&db, "records", root),
            purged_at(frontier)
        );
    }
    db.export_snapshot(root.path().join("repeated-key.export.db"))
        .expect("a snapshot taken after every purge publishes");
}

#[test]
fn a_purged_key_accepts_a_new_write_after_reopen_and_every_purged_life_stays_refused() {
    let root = tempfile::tempdir().expect("temporary test directory");
    let path = root.path().join("reopened-key.db");
    let db = Database::open(&path).expect("open the store");
    db.execute(
        "CREATE TABLE records (id UUID PRIMARY KEY, batch TEXT NOT NULL) SYNC OFF",
        &p(),
    )
    .expect("declare the node-local table");
    insert_repeated_key(&db, "records");
    assert_eq!(purge_batch_a(&db, "records"), 1);
    let (first_root, first_frontier) = current_purged_life(&db, "records");
    drop(db);

    let db = Database::open(&path).expect("reopen the store");
    assert_eq!(
        classify_repeated_key_root(&db, "records", &first_root),
        purged_at(first_frontier),
        "the permanent tombstone survives reopen"
    );
    insert_repeated_key(&db, "records");
    assert_eq!(purge_batch_a(&db, "records"), 1);
    let (second_root, second_frontier) = current_purged_life(&db, "records");
    assert_ne!(second_root, first_root);
    drop(db);

    let db = Database::open(&path).expect("reopen the store again");
    assert_eq!(
        classify_repeated_key_root(&db, "records", &first_root),
        purged_at(first_frontier)
    );
    assert_eq!(
        classify_repeated_key_root(&db, "records", &second_root),
        purged_at(second_frontier)
    );
    insert_repeated_key(&db, "records");
    assert_eq!(purge_batch_a(&db, "records"), 1);
    assert_eq!(row_total(&db, "records"), 0);
}

#[test]
fn an_ordinary_delete_of_a_later_life_keeps_the_purged_life_refused() {
    let root = tempfile::tempdir().expect("temporary test directory");
    let db = Database::open(root.path().join("deleted-key.db")).expect("open the store");
    db.execute(
        "CREATE TABLE shared_records (id UUID PRIMARY KEY, batch TEXT NOT NULL) \
         SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        &p(),
    )
    .expect("declare the synchronized table");
    insert_repeated_key(&db, "shared_records");
    assert_eq!(purge_batch_a(&db, "shared_records"), 1);
    let (first_root, first_frontier) = current_purged_life(&db, "shared_records");

    insert_repeated_key(&db, "shared_records");
    assert_eq!(
        db.execute(
            "DELETE FROM shared_records WHERE id = $id",
            &repeated_key_params()
        )
        .expect("an ordinary delete of the new life commits")
        .rows_affected(),
        1
    );
    assert_eq!(
        classify_repeated_key_root(&db, "shared_records", &first_root),
        purged_at(first_frontier),
        "a later life's delete never replaces the purged life's tombstone"
    );

    insert_repeated_key(&db, "shared_records");
    assert_eq!(purge_batch_a(&db, "shared_records"), 1);
    let (third_root, third_frontier) = current_purged_life(&db, "shared_records");
    assert_ne!(third_root, first_root);
    assert_eq!(
        classify_repeated_key_root(&db, "shared_records", &first_root),
        purged_at(first_frontier)
    );
    assert_eq!(
        classify_repeated_key_root(&db, "shared_records", &third_root),
        purged_at(third_frontier)
    );
}

// ---------------------------------------------------------------------------
// Declared children follow their parent's purge.
// ---------------------------------------------------------------------------

fn id_params(id: Uuid) -> HashMap<String, Value> {
    HashMap::from([("id".to_string(), Value::Uuid(id))])
}

fn exec(db: &Database, sql: &str, params: &HashMap<String, Value>) {
    db.execute(sql, params)
        .unwrap_or_else(|err| panic!("{sql}: {err}"));
}

fn ids(db: &Database, table: &str) -> Vec<Uuid> {
    let mut ids = db
        .execute(&format!("SELECT id FROM {table}"), &p())
        .unwrap_or_else(|err| panic!("{table} must answer: {err}"))
        .rows()
        .iter()
        .map(|row| match &row[0] {
            Value::Uuid(id) => *id,
            other => panic!("{table}.id is a UUID, got {other:?}"),
        })
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

fn follower_lives(db: &Database, parent_table: &str, parent: Uuid) -> Vec<(String, bool)> {
    db.purge_follower_lives_for_test(parent_table, &key_of(parent))
        .expect("discover what follows the parent")
        .into_iter()
        .map(|(table, _, retained)| (table, retained))
        .collect()
}

fn insert_child(db: &Database, table: &str, id: Uuid, parent_column: &str, parent: Uuid) {
    exec(
        db,
        &format!("INSERT INTO {table} (id, {parent_column}, body) VALUES ($id, $parent, 'x')"),
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("parent".to_string(), Value::Uuid(parent)),
        ]),
    );
}

#[test]
fn a_purge_erases_every_retained_life_of_a_declared_child() {
    let root = tempfile::tempdir().expect("temporary test directory");
    let path = root.path().join("followers.db");
    let db = Database::open(&path).expect("open the store");
    for ddl in [
        "CREATE TABLE parents (id UUID PRIMARY KEY, batch TEXT) SYNC OFF",
        "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id) FOLLOWS PARENT PURGE, body TEXT) SYNC OFF",
        "CREATE TABLE grandchildren (id UUID PRIMARY KEY, child_id UUID REFERENCES children(id) FOLLOWS PARENT PURGE, body TEXT) SYNC OFF",
        "CREATE TABLE bystanders (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id), body TEXT) SYNC OFF",
    ] {
        exec(&db, ddl, &p());
    }
    let (parent, other) = (Uuid::new_v4(), Uuid::new_v4());
    for id in [parent, other] {
        exec(
            &db,
            "INSERT INTO parents (id, batch) VALUES ($id, 'b')",
            &id_params(id),
        );
    }
    let (live, deleted, twice, other_child) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    insert_child(&db, "children", live, "parent_id", parent);
    insert_child(&db, "children", deleted, "parent_id", parent);
    exec(
        &db,
        "DELETE FROM children WHERE id = $id",
        &id_params(deleted),
    );
    // One key, two lives: the first deleted, the second current.
    insert_child(&db, "children", twice, "parent_id", parent);
    exec(
        &db,
        "DELETE FROM children WHERE id = $id",
        &id_params(twice),
    );
    insert_child(&db, "children", twice, "parent_id", parent);
    insert_child(&db, "children", other_child, "parent_id", other);
    let grandchild = Uuid::new_v4();
    insert_child(&db, "grandchildren", grandchild, "child_id", live);
    let bystander = Uuid::new_v4();
    insert_child(&db, "bystanders", bystander, "parent_id", parent);

    let before = follower_lives(&db, "parents", parent);
    assert_eq!(
        before
            .iter()
            .filter(|(table, _)| table == "children")
            .count(),
        4,
        "discovery finds the current, the deleted and both lives at one key: {before:?}"
    );
    assert_eq!(
        before.iter().filter(|(_, retained)| *retained).count(),
        2,
        "two of the four are no longer the visible row at their key: {before:?}"
    );
    assert!(before.contains(&("grandchildren".to_string(), false)));
    assert!(
        !before.iter().any(|(table, _)| table == "bystanders"),
        "an undeclared reference never follows"
    );

    let result = db
        .execute("PURGE FROM parents WHERE id = $id", &id_params(parent))
        .expect("the purge of the parent commits");
    let report = normalize_erasure(&result);
    assert_eq!(report["parents"].affected, 1, "{report:?}");
    assert_eq!(
        report["children"].affected, 4,
        "every life is reported: {report:?}"
    );
    assert_eq!(report["grandchildren"].affected, 1, "{report:?}");
    assert!(!report.contains_key("bystanders"), "{report:?}");

    let check = |db: &Database| {
        assert_eq!(ids(db, "parents"), vec![other]);
        assert_eq!(ids(db, "children"), vec![other_child]);
        assert!(ids(db, "grandchildren").is_empty());
        assert_eq!(
            ids(db, "bystanders"),
            vec![bystander],
            "an undeclared child survives"
        );
        assert!(
            follower_lives(db, "parents", parent).is_empty(),
            "no retained life of any follower still references the purged parent"
        );
        assert_eq!(
            follower_lives(db, "parents", other),
            vec![("children".to_string(), false)],
            "the other parent's child is untouched"
        );
    };
    check(&db);
    db.close().expect("close the store");
    let reopened = Database::open(&path).expect("reopen the store");
    check(&reopened);
}

#[test]
fn cg_shaped_followers_erase_only_their_own_arm() {
    let root = tempfile::tempdir().expect("temporary test directory");
    let db = Database::open(root.path().join("cg-shaped.db")).expect("open the store");
    for ddl in [
        "CREATE TABLE digests (id UUID PRIMARY KEY, context_id UUID NOT NULL, summary TEXT, UNIQUE (context_id, id)) SYNC OFF",
        "CREATE TABLE evidence (id UUID PRIMARY KEY, context_id UUID NOT NULL, kind TEXT, UNIQUE (context_id, id)) SYNC OFF",
        "CREATE TABLE reproducible_vectors (target_table TEXT NOT NULL, target_id UUID NOT NULL, \
         embedding_space_id TEXT NOT NULL, model_generation INTEGER NOT NULL, context_id UUID NOT NULL, \
         digest_id UUID REFERENCES digests(id) FOLLOWS PARENT PURGE, \
         evidence_id UUID REFERENCES evidence(id) FOLLOWS PARENT PURGE, vector_text VECTOR(4), \
         PRIMARY KEY (target_table, target_id, embedding_space_id), \
         FOREIGN KEY (context_id, digest_id) REFERENCES digests(context_id, id), \
         FOREIGN KEY (context_id, evidence_id) REFERENCES evidence(context_id, id)) SYNC OFF",
        "CREATE TABLE reproducible_vector_handles (id UUID PRIMARY KEY, target_table TEXT NOT NULL, \
         target_id UUID NOT NULL, context_id UUID NOT NULL, \
         digest_id UUID REFERENCES digests(id) FOLLOWS PARENT PURGE, \
         evidence_id UUID REFERENCES evidence(id) FOLLOWS PARENT PURGE, \
         FOREIGN KEY (context_id, digest_id) REFERENCES digests(context_id, id), \
         FOREIGN KEY (context_id, evidence_id) REFERENCES evidence(context_id, id)) SYNC OFF",
        "CREATE TABLE rebuild_pending (embedding_space_id TEXT NOT NULL, model_generation INTEGER NOT NULL, \
         state TEXT NOT NULL, target_table TEXT NOT NULL, target_id UUID NOT NULL, context_id UUID NOT NULL, \
         digest_id UUID REFERENCES digests(id) FOLLOWS PARENT PURGE, \
         evidence_id UUID REFERENCES evidence(id) FOLLOWS PARENT PURGE, \
         PRIMARY KEY (embedding_space_id, model_generation, state, target_table, target_id), \
         FOREIGN KEY (context_id, digest_id) REFERENCES digests(context_id, id), \
         FOREIGN KEY (context_id, evidence_id) REFERENCES evidence(context_id, id)) SYNC OFF",
    ] {
        exec(&db, ddl, &p());
    }
    // One UUID names a digest and an evidence row; they are unrelated.
    let shared = Uuid::new_v4();
    let context = Uuid::new_v4();
    let params = HashMap::from([
        ("id".to_string(), Value::Uuid(shared)),
        ("context".to_string(), Value::Uuid(context)),
    ]);
    exec(
        &db,
        "INSERT INTO digests (id, context_id, summary) VALUES ($id, $context, 's')",
        &params,
    );
    exec(
        &db,
        "INSERT INTO evidence (id, context_id, kind) VALUES ($id, $context, 'text')",
        &params,
    );
    for (target, arm) in [("digests", "digest_id"), ("evidence", "evidence_id")] {
        exec(
            &db,
            &format!(
                "INSERT INTO reproducible_vectors (target_table, target_id, embedding_space_id, \
                 model_generation, context_id, {arm}, vector_text) \
                 VALUES ('{target}', $id, 'text', 1, $context, $id, $vector)"
            ),
            &HashMap::from([
                ("id".to_string(), Value::Uuid(shared)),
                ("context".to_string(), Value::Uuid(context)),
                (
                    "vector".to_string(),
                    Value::Vector(vec![0.1, 0.2, 0.3, 0.4]),
                ),
            ]),
        );
        exec(
            &db,
            &format!(
                "INSERT INTO reproducible_vector_handles (id, target_table, target_id, context_id, {arm}) \
                 VALUES ($handle, '{target}', $id, $context, $id)"
            ),
            &HashMap::from([
                ("id".to_string(), Value::Uuid(shared)),
                ("context".to_string(), Value::Uuid(context)),
                ("handle".to_string(), Value::Uuid(Uuid::new_v4())),
            ]),
        );
        exec(
            &db,
            &format!(
                "INSERT INTO rebuild_pending (embedding_space_id, model_generation, state, target_table, \
                 target_id, context_id, {arm}) VALUES ('text', 1, 'waiting', '{target}', $id, $context, $id)"
            ),
            &params,
        );
    }

    exec(&db, "PURGE FROM digests WHERE id = $id", &id_params(shared));

    for table in [
        "reproducible_vectors",
        "reproducible_vector_handles",
        "rebuild_pending",
    ] {
        let result = db
            .execute(
                &format!("SELECT target_table, digest_id, evidence_id FROM {table}"),
                &p(),
            )
            .unwrap_or_else(|err| panic!("{table} must answer: {err}"));
        let rows = result.rows();
        assert_eq!(
            rows,
            vec![vec![text("evidence"), Value::Null, Value::Uuid(shared)]],
            "{table}: the digest's row is erased and the evidence row with the same UUID survives"
        );
    }
    assert_eq!(
        ids(&db, "evidence"),
        vec![shared],
        "the unrelated parent survives"
    );
    assert!(ids(&db, "digests").is_empty());
}

/// An archived life is found from the parent it referenced, never from the
/// child table's current declaration: the table may since point elsewhere,
/// no longer follow, or be gone.
/// An edge store linked to `hub` over real authenticated sync transport: it
/// pushes its own schema and rows, and pulls the hub's, purges included.
struct PushingSource {
    db: Option<Arc<Database>>,
    path: std::path::PathBuf,
    client: Option<SyncClient>,
    identity: Arc<contextdb_server::FabricIdentity>,
    tenant: TenantId,
    server: Arc<SyncServer>,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
    broker: contextdb_server::InProcessBroker,
}

impl PushingSource {
    fn start(runtime: &tokio::runtime::Runtime, root: &Path, hub: Arc<Database>) -> Self {
        let tenant = TenantId::from("archived-followers");
        let hub_identity = Arc::new(contextdb_server::FabricIdentity::generate());
        let broker = contextdb_server::InProcessBroker::new();
        let node = hub_identity.node_id();
        let server = Arc::new(
            SyncServer::with_authenticated_transport_and_identity_for_test(
                hub,
                broker.server_as(&node),
                tenant.clone(),
                node,
                hub_identity,
            )
            .expect("construct the hub's authenticated sync endpoint"),
        );
        let stop = Arc::new(AtomicBool::new(false));
        let task = runtime.spawn({
            let server = server.clone();
            let stop = stop.clone();
            async move { server.run_until(stop).await }
        });
        runtime.block_on(broker.wait_for_registered_route_for_test(
            &contextdb_server::subjects::push_subject(tenant.as_str()),
        ));
        let path = root.join("source.db");
        let db = Arc::new(Database::open(&path).expect("open the edge store"));
        let mut link = Self {
            db: None,
            path,
            client: None,
            identity: Arc::new(contextdb_server::FabricIdentity::generate()),
            tenant,
            server,
            stop,
            task,
            broker,
        };
        link.connect(db);
        link
    }

    fn connect(&mut self, db: Arc<Database>) {
        self.client = Some(
            SyncClient::with_authenticated_transport_and_identity_for_test(
                db.clone(),
                self.broker.client_as(&self.identity.node_id()),
                self.tenant.clone(),
                self.identity.clone(),
            )
            .expect("construct the edge's authenticated sync endpoint"),
        );
        self.db = Some(db);
    }

    fn edge(&self) -> &Database {
        self.db.as_ref().expect("the edge store is open")
    }

    fn client(&self) -> &SyncClient {
        self.client.as_ref().expect("the edge is connected")
    }

    fn push(&self, runtime: &tokio::runtime::Runtime) {
        runtime
            .block_on(self.client().push())
            .expect("the hub receives the edge's schema and rows");
    }

    fn pull(&self, runtime: &tokio::runtime::Runtime) {
        runtime
            .block_on(self.client().pull_default())
            .expect("the edge receives the hub's schema, rows and purges");
    }

    /// Close and reopen the edge store. With `replay`, its pull watermark is
    /// rewound so the hub delivers everything again.
    fn reopen_edge(&mut self, runtime: &tokio::runtime::Runtime, replay: bool) {
        self.reopen_edge_with(runtime, replay, None);
    }

    /// Reopen the edge, optionally with a declared reader memory limit.
    fn reopen_edge_with(
        &mut self,
        runtime: &tokio::runtime::Runtime,
        replay: bool,
        reader_limit: Option<usize>,
    ) {
        if let Some(client) = self.client.take() {
            runtime.block_on(client.shutdown());
        }
        let db = self.db.take().expect("the edge store is open");
        db.close().expect("close the edge store");
        drop(db);
        let db = open_with_reader_limit(&self.path, reader_limit);
        if replay {
            db.persist_sync_pull_watermark(&self.tenant, contextdb_core::Lsn(0))
                .expect("rewind the edge's pull watermark");
        }
        self.connect(Arc::new(db));
    }

    fn stop(mut self, runtime: &tokio::runtime::Runtime) {
        if let Some(client) = self.client.take() {
            runtime.block_on(client.shutdown());
        }
        self.stop.store(true, Ordering::SeqCst);
        runtime.block_on(self.task).expect("hub stops cleanly");
        drop(self.server);
    }
}

/// Runs its closure when dropped, so a failing test still releases a pause
/// that would otherwise hold the operation under test.
struct OnDrop<F: FnMut()>(F);

impl<F: FnMut()> Drop for OnDrop<F> {
    fn drop(&mut self) {
        (self.0)()
    }
}

fn sync_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("sync runtime")
}

#[test]
fn a_purge_erases_child_lives_archived_by_an_earlier_drop() {
    let runtime = sync_runtime();
    for received in [false, true] {
        for recreated_as in [
            Some("parent_id UUID REFERENCES parents(id) FOLLOWS PARENT PURGE"),
            Some("parent_id UUID REFERENCES other_parents(id) FOLLOWS PARENT PURGE"),
            Some("parent_id UUID REFERENCES parents(id)"),
            None,
        ] {
            let case = format!(
                "{} drop, recreated as {recreated_as:?}",
                if received { "received" } else { "local" }
            );
            let root = tempfile::tempdir().expect("temporary test directory");
            let path = root.path().join("archived-followers.db");
            let db = Arc::new(Database::open(&path).expect("open the store"));
            // A received drop is authored on a source and pushed to this store
            // through real sync transport; a local one is authored here.
            let source = received.then(|| PushingSource::start(&runtime, root.path(), db.clone()));
            let author: &Database = source.as_ref().map_or(&*db, |source| source.edge());
            let sync = if received {
                " SYNC TWO WAY"
            } else {
                " SYNC OFF"
            };
            for ddl in [
                "CREATE TABLE parents (id UUID PRIMARY KEY, batch TEXT)",
                "CREATE TABLE other_parents (id UUID PRIMARY KEY, batch TEXT)",
                "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id) FOLLOWS PARENT PURGE, body TEXT)",
            ] {
                exec(author, &format!("{ddl}{sync}"), &p());
            }
            // Two parent tables share each key, so a key alone never says
            // which parent an archived life followed.
            let (purged, kept) = (Uuid::new_v4(), Uuid::new_v4());
            for id in [purged, kept] {
                exec(
                    author,
                    "INSERT INTO parents (id, batch) VALUES ($id, 'b')",
                    &id_params(id),
                );
                exec(
                    author,
                    "INSERT INTO other_parents (id, batch) VALUES ($id, 'b')",
                    &id_params(id),
                );
            }
            let (archived_purged, archived_kept) = (Uuid::new_v4(), Uuid::new_v4());
            insert_child(author, "children", archived_purged, "parent_id", purged);
            insert_child(author, "children", archived_kept, "parent_id", kept);
            if let Some(source) = &source {
                source.push(&runtime);
            }
            exec(author, "DROP TABLE children", &p());
            let mut fresh = Vec::new();
            if let Some(column) = recreated_as {
                exec(
                    author,
                    &format!(
                        "CREATE TABLE children (id UUID PRIMARY KEY, {column}, body TEXT){sync}"
                    ),
                    &p(),
                );
                for parent in [purged, kept] {
                    let id = Uuid::new_v4();
                    insert_child(author, "children", id, "parent_id", parent);
                    fresh.push(id);
                }
            } else if received {
                // A data commit after the drop carries the drop to the hub.
                exec(
                    author,
                    "INSERT INTO parents (id, batch) VALUES ($id, 'later')",
                    &id_params(Uuid::new_v4()),
                );
            }
            if let Some(source) = source {
                source.push(&runtime);
                source.stop(&runtime);
            }
            db.close().expect("close the store");
            drop(db);
            let db = Database::open(&path).expect("reopen the store");
            assert!(
                follower_lives(&db, "parents", purged)
                    .iter()
                    .any(|(table, retained)| table == "children" && *retained),
                "{case}: the archived life is found from its parent"
            );

            // The other parent table shares the key; purging it never reaches
            // a life that followed `parents`, whatever `children` follows now.
            exec(
                &db,
                "PURGE FROM other_parents WHERE id = $id",
                &id_params(purged),
            );
            assert!(
                follower_lives(&db, "parents", purged)
                    .iter()
                    .any(|(table, retained)| table == "children" && *retained),
                "{case}: purging the other parent leaves this parent's archived life"
            );

            db.execute("PURGE FROM parents WHERE id = $id", &id_params(purged))
                .unwrap_or_else(|err| panic!("{case}: purge the parent: {err}"));

            let check = |db: &Database| {
                assert!(
                    !follower_lives(db, "parents", purged)
                        .iter()
                        .any(|(_, retained)| *retained),
                    "{case}: no archived life of the purged parent survives"
                );
                assert!(
                    follower_lives(db, "parents", kept)
                        .iter()
                        .any(|(table, retained)| table == "children" && *retained),
                    "{case}: the other parent's archived life survives"
                );
                if recreated_as.is_some() {
                    // The fresh row of the purged key follows through the
                    // current declaration, to whichever parent it names.
                    let mut expected = fresh.clone();
                    if recreated_as.is_some_and(|column| column.contains("FOLLOWS")) {
                        expected.retain(|id| *id != fresh[0]);
                    }
                    expected.sort();
                    assert_eq!(
                        ids(db, "children"),
                        expected,
                        "{case}: a fresh row follows only through the current declaration"
                    );
                }
            };
            check(&db);
            db.close().expect("close the store");
            check(&Database::open(&path).expect("reopen the store"));
        }
    }
}

#[test]
fn follows_parent_purge_is_validated_on_the_projected_catalog() {
    let root = tempfile::tempdir().expect("temporary test directory");
    let db = Database::open(root.path().join("validated.db")).expect("open the store");
    exec(
        &db,
        "CREATE TABLE parents (id UUID PRIMARY KEY, code TEXT UNIQUE) SYNC OFF",
        &p(),
    );
    let refused = db
        .execute(
            "CREATE TABLE by_code (id UUID PRIMARY KEY, code TEXT REFERENCES parents(code) FOLLOWS PARENT PURGE) SYNC OFF",
            &p(),
        )
        .expect_err("a reference through a key other than the parent's erasure key cannot follow");
    assert!(
        matches!(&refused, Error::SchemaInvalid { reason } if reason.contains("erasure key")),
        "{refused}"
    );
    assert!(
        db.table_meta("by_code").is_none(),
        "a refused declaration creates nothing"
    );
    exec(
        &db,
        "CREATE TABLE by_id (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id) FOLLOWS PARENT PURGE) SYNC OFF",
        &p(),
    );
    let explained = db
        .explain("PURGE FROM parents WHERE code = 'x'")
        .expect("explain the purge");
    assert!(
        explained.contains("follows parent purge: by_id(parent_id) -> parents(id)"),
        "EXPLAIN names what follows: {explained}"
    );

    // Following never forms a cycle, declared at once or by a later ALTER.
    let cycle = db
        .execute(
            "CREATE TABLE self_ring (id UUID PRIMARY KEY, parent_id UUID REFERENCES self_ring(id) FOLLOWS PARENT PURGE) SYNC OFF",
            &p(),
        )
        .expect_err("a table cannot follow its own purge");
    assert!(
        matches!(&cycle, Error::SchemaInvalid { reason } if reason.contains("cycle")),
        "{cycle}"
    );
    exec(
        &db,
        "CREATE TABLE ring_a (id UUID PRIMARY KEY) SYNC OFF",
        &p(),
    );
    exec(
        &db,
        "CREATE TABLE ring_b (id UUID PRIMARY KEY, a_id UUID REFERENCES ring_a(id) FOLLOWS PARENT PURGE) SYNC OFF",
        &p(),
    );
    let cycle = db
        .execute(
            "ALTER TABLE ring_a ADD COLUMN b_id UUID REFERENCES ring_b(id) FOLLOWS PARENT PURGE",
            &p(),
        )
        .expect_err("a later ALTER cannot close a following cycle");
    assert!(
        matches!(&cycle, Error::SchemaInvalid { reason } if reason.contains("cycle")),
        "{cycle}"
    );

    // The covering index is the automatic foreign-key index; it cannot be
    // dropped while the follower needs it.
    let covering = db
        .table_meta("by_id")
        .expect("by_id is declared")
        .indexes
        .iter()
        .find(|index| {
            index
                .columns
                .first()
                .is_some_and(|(column, _)| column == "parent_id")
        })
        .map(|index| index.name.clone())
        .expect("an index leads with the following column");
    assert!(
        db.execute(&format!("DROP INDEX {covering} ON by_id"), &p())
            .is_err(),
        "the follower's covering index cannot be dropped"
    );

    // The parent's erasure key and the parent itself stay while followed.
    assert!(
        db.execute("ALTER TABLE parents DROP COLUMN id", &p())
            .is_err(),
        "the referenced erasure-key column cannot be removed"
    );
    assert!(
        db.execute("DROP TABLE parents", &p()).is_err(),
        "a followed parent cannot be dropped from under its follower"
    );
    assert!(
        db.table_meta("by_id")
            .is_some_and(|meta| meta.columns.iter().any(|column| column
                .references
                .as_ref()
                .is_some_and(|r| r.follows_parent_purge))),
        "the follower keeps its declaration"
    );

    // The same rules judge a received declaration on the projected catalog.
    let source = Database::open(root.path().join("validation-source.db")).expect("open the source");
    exec(
        &source,
        "CREATE TABLE parents (id UUID PRIMARY KEY, code UUID UNIQUE) SYNC TWO WAY",
        &p(),
    );
    exec(
        &source,
        "CREATE TABLE by_ref (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id) FOLLOWS PARENT PURGE) SYNC TWO WAY",
        &p(),
    );
    let mut changes = source
        .changes_since(contextdb_core::Lsn(0))
        .expect("read the source's schema");
    let mut rewritten = false;
    for ddl in &mut changes.ddl {
        if let contextdb_engine::sync_types::DdlChange::CreateTable {
            name,
            columns,
            foreign_keys,
            ..
        } = ddl
            && name == "by_ref"
        {
            for (column, ty) in columns.iter_mut() {
                if column == "parent_id" {
                    *ty = ty.replace("parents(id)", "parents(code)");
                }
            }
            for key in foreign_keys.iter_mut() {
                key.parent_column = "code".into();
            }
            rewritten = true;
        }
    }
    assert!(rewritten, "the source declares by_ref");
    let receiver =
        Database::open(root.path().join("validation-receiver.db")).expect("open the receiver");
    let refused = receiver
        .apply_changes(
            changes,
            &contextdb_engine::sync_types::ConflictPolicies::uniform(
                contextdb_engine::sync_types::ConflictPolicy::ServerWins,
            ),
        )
        .expect_err("a received follower through a key other than the erasure key is refused");
    assert!(refused.to_string().contains("erasure key"), "{refused}");
    assert!(
        receiver.table_meta("by_ref").is_none(),
        "a refused received declaration creates nothing"
    );
}

/// `.schema`, EXPLAIN and the canonical schema a peer receives all carry the
/// declaration, so it survives every schema path.
#[test]
fn schema_and_explain_show_follows_parent_purge() {
    let root = tempfile::tempdir().expect("temporary test directory");
    let source = Database::open(root.path().join("schema-source.db")).expect("open the source");
    exec(
        &source,
        "CREATE TABLE parents (id UUID PRIMARY KEY, batch TEXT) SYNC TWO WAY",
        &p(),
    );
    exec(
        &source,
        "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id) FOLLOWS PARENT PURGE, body TEXT) SYNC TWO WAY",
        &p(),
    );
    let shows = |db: &Database, label: &str| {
        let meta = db.table_meta("children").expect("children is declared");
        let rendered = contextdb_engine::cli_render::render_table_meta("children", &meta);
        assert!(
            rendered.contains("REFERENCES parents(id) FOLLOWS PARENT PURGE"),
            "{label}: .schema shows the declaration: {rendered}"
        );
        let explained = db
            .explain("PURGE FROM parents WHERE batch = 'x'")
            .expect("explain the purge");
        assert!(
            explained.contains("follows parent purge: children(parent_id) -> parents(id)"),
            "{label}: EXPLAIN names what follows: {explained}"
        );
    };
    shows(&source, "source");

    let changes = source
        .changes_since(contextdb_core::Lsn(0))
        .expect("read the canonical schema");
    assert!(
        changes.ddl.iter().any(|ddl| matches!(
            ddl,
            contextdb_engine::sync_types::DdlChange::CreateTable { name, columns, foreign_keys, .. }
                if name == "children"
                    && (foreign_keys.iter().any(|key| key.follows_parent_purge)
                        || columns.iter().any(|(_, ty)| ty.contains("FOLLOWS PARENT PURGE")))
        )),
        "the canonical schema carries the declaration"
    );
    let receiver =
        Database::open(root.path().join("schema-receiver.db")).expect("open the receiver");
    receiver
        .apply_changes(
            changes,
            &contextdb_engine::sync_types::ConflictPolicies::uniform(
                contextdb_engine::sync_types::ConflictPolicy::ServerWins,
            ),
        )
        .expect("the receiver applies the canonical schema");
    shows(&receiver, "receiver");
    receiver.close().expect("close the receiver");
    shows(
        &Database::open(root.path().join("schema-receiver.db")).expect("reopen the receiver"),
        "reopened receiver",
    );
}

fn create_following_tables(db: &Database) {
    for ddl in [
        "CREATE TABLE parents (id UUID PRIMARY KEY, batch TEXT) SYNC OFF",
        "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id) FOLLOWS PARENT PURGE, body TEXT) SYNC OFF",
        "CREATE TABLE grandchildren (id UUID PRIMARY KEY, child_id UUID REFERENCES children(id) FOLLOWS PARENT PURGE, body TEXT) SYNC OFF",
    ] {
        exec(db, ddl, &p());
    }
}

/// The edge's node-local followers of the hub's `parents`, declared after the
/// hub's schema arrived by real sync.
const EDGE_CHILDREN_DDL: &str = "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id) FOLLOWS PARENT PURGE, body TEXT) SYNC OFF";
const HUB_PARENTS_DDL: &str = "CREATE TABLE parents (id UUID PRIMARY KEY, batch TEXT) SYNC TWO WAY";

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn a_child_committed_during_a_purge_is_erased_with_its_parent() {
    // Local: the purge pauses after it found its followers and before it takes
    // any lock; a child committed there is erased with the parent.
    let root = tempfile::tempdir().expect("temporary test directory");
    let path = root.path().join("concurrent.db");
    let db = Database::open(&path).expect("open the store");
    create_following_tables(&db);
    let (parent, early, late) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    exec(
        &db,
        "INSERT INTO parents (id, batch) VALUES ($id, 'b')",
        &id_params(parent),
    );
    insert_child(&db, "children", early, "parent_id", parent);
    let pause = db.pause_purge_after_follower_expansion_for_test();
    std::thread::scope(|scope| {
        let _release = OnDrop(|| pause.release());
        let purge =
            scope.spawn(|| db.execute("PURGE FROM parents WHERE id = $id", &id_params(parent)));
        assert!(
            pause.wait_until_reached_blocking(),
            "the purge pauses after finding its followers"
        );
        insert_child(&db, "children", late, "parent_id", parent);
        pause.release();
        purge
            .join()
            .expect("the purge thread finishes")
            .expect("the purge commits");
    });
    let erased = |db: &Database, label: &str| {
        assert!(ids(db, "parents").is_empty(), "{label}: the parent is gone");
        assert!(
            ids(db, "children").is_empty(),
            "{label}: the child committed during the purge is gone with the one before it"
        );
        assert!(
            follower_lives(db, "parents", parent).is_empty(),
            "{label}: no retained life of either child survives"
        );
    };
    erased(&db, "local");
    db.close().expect("close the store");
    erased(
        &Database::open(&path).expect("reopen the store"),
        "local reopened",
    );

    // Received: the edge applying the hub's purge pauses inside that apply.
    let runtime = sync_runtime();
    let root = tempfile::tempdir().expect("temporary test directory");
    let hub = Arc::new(Database::open(root.path().join("hub.db")).expect("open the hub"));
    exec(&hub, HUB_PARENTS_DDL, &p());
    let link = PushingSource::start(&runtime, root.path(), hub.clone());
    exec(
        &hub,
        "INSERT INTO parents (id, batch) VALUES ($id, 'b')",
        &id_params(parent),
    );
    link.pull(&runtime);
    exec(link.edge(), EDGE_CHILDREN_DDL, &p());
    insert_child(link.edge(), "children", early, "parent_id", parent);
    exec(
        &hub,
        "PURGE FROM parents WHERE id = $id",
        &id_params(parent),
    );
    let pause = link.edge().pause_purge_after_follower_expansion_for_test();
    std::thread::scope(|scope| {
        let _release = OnDrop(|| pause.release());
        let pull = scope.spawn(|| runtime.block_on(link.client().pull_default()));
        assert!(
            pause.wait_until_reached_blocking(),
            "the received purge pauses after finding the edge's followers"
        );
        // The received apply holds the edge's writer from its expansion to its
        // commit, so a local child cannot commit in between: it waits behind
        // the purge and is then refused by its foreign key, or, had it got in
        // first, it would be erased with the parent. Either way none survives.
        let insert = scope.spawn(|| {
            link.edge().execute(
                "INSERT INTO children (id, parent_id, body) VALUES ($id, $parent, 'x')",
                &HashMap::from([
                    ("id".to_string(), Value::Uuid(late)),
                    ("parent".to_string(), Value::Uuid(parent)),
                ]),
            )
        });
        pause.release();
        let _ = insert.join().expect("the insert thread finishes");
        pull.join()
            .expect("the pull thread finishes")
            .expect("the edge applies the hub's purge");
    });
    erased(link.edge(), "received");
    link.stop(&runtime);
}

#[test]
fn a_failure_at_the_erasure_boundary_leaves_parent_and_children_together() {
    let root = tempfile::tempdir().expect("temporary test directory");
    let path = root.path().join("erasure-failure.db");
    let db = Database::open(&path).expect("open the store");
    create_following_tables(&db);
    let (parent, child, grandchild) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    exec(
        &db,
        "INSERT INTO parents (id, batch) VALUES ($id, 'b')",
        &id_params(parent),
    );
    insert_child(&db, "children", child, "parent_id", parent);
    insert_child(&db, "grandchildren", grandchild, "child_id", child);
    db.arm_authoritative_purge_point_remove_persistence_failure_for_test();
    db.execute("PURGE FROM parents WHERE id = $id", &id_params(parent))
        .expect_err("the injected failure at the erasure boundary refuses the purge");
    let together = |db: &Database, label: &str| {
        assert_eq!(
            ids(db, "parents"),
            vec![parent],
            "{label}: the parent stays"
        );
        assert_eq!(ids(db, "children"), vec![child], "{label}: the child stays");
        assert_eq!(
            ids(db, "grandchildren"),
            vec![grandchild],
            "{label}: the grandchild stays"
        );
        assert_eq!(
            follower_lives(db, "parents", parent).len(),
            2,
            "{label}: both followers are still found"
        );
    };
    together(&db, "after the failure");
    db.close().expect("close the store");
    let db = Database::open(&path).expect("reopen the store");
    together(&db, "reopened");
    exec(&db, "PURGE FROM parents WHERE id = $id", &id_params(parent));
    for table in ["parents", "children", "grandchildren"] {
        assert!(ids(&db, table).is_empty(), "a retried purge erases {table}");
    }
}

#[test]
fn a_purge_refused_after_some_followers_were_admitted_changes_nothing() {
    let root = tempfile::tempdir().expect("temporary test directory");
    let path = root.path().join("refused-followers.db");
    let db = Database::open(&path).expect("open the store");
    create_following_tables(&db);
    let (parent, child, grandchild) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    exec(
        &db,
        "INSERT INTO parents (id, batch) VALUES ($id, 'b')",
        &id_params(parent),
    );
    insert_child(&db, "children", child, "parent_id", parent);
    insert_child(&db, "grandchildren", grandchild, "child_id", child);
    let before = db.accountant().usage().used;
    // The child is admitted; the grandchild is refused before it is held.
    Database::__refuse_purge_follower_after_for_test(1);
    let refused = db
        .execute("PURGE FROM parents WHERE id = $id", &id_params(parent))
        .expect_err("the purge is refused after a follower was admitted");
    assert!(refused.to_string().contains("refused"), "{refused}");
    assert_eq!(
        db.accountant().usage().used,
        before,
        "a refused purge leaves no credit outstanding"
    );
    let unchanged = |db: &Database, label: &str| {
        assert_eq!(
            ids(db, "parents"),
            vec![parent],
            "{label}: the parent stays"
        );
        assert_eq!(ids(db, "children"), vec![child], "{label}: the child stays");
        assert_eq!(
            ids(db, "grandchildren"),
            vec![grandchild],
            "{label}: the grandchild stays"
        );
        assert_eq!(
            follower_lives(db, "parents", parent).len(),
            2,
            "{label}: every follower's history is still there"
        );
    };
    unchanged(&db, "after the refusal");
    db.close().expect("close the store");
    let db = Database::open(&path).expect("reopen the store");
    unchanged(&db, "reopened");
    exec(&db, "PURGE FROM parents WHERE id = $id", &id_params(parent));
    for table in ["parents", "children", "grandchildren"] {
        assert!(
            ids(&db, table).is_empty(),
            "an unrefused purge erases {table}"
        );
    }
}

#[test]
fn a_redelivered_purge_never_expands() {
    let runtime = sync_runtime();
    let root = tempfile::tempdir().expect("temporary test directory");
    let hub = Arc::new(Database::open(root.path().join("hub.db")).expect("open the hub"));
    exec(&hub, HUB_PARENTS_DDL, &p());
    let mut link = PushingSource::start(&runtime, root.path(), hub.clone());
    let (keyed, predicated) = (Uuid::new_v4(), Uuid::new_v4());
    exec(
        &hub,
        "INSERT INTO parents (id, batch) VALUES ($id, 'keyed')",
        &id_params(keyed),
    );
    exec(
        &hub,
        "INSERT INTO parents (id, batch) VALUES ($id, 'predicated')",
        &id_params(predicated),
    );
    link.pull(&runtime);
    exec(link.edge(), EDGE_CHILDREN_DDL, &p());
    insert_child(link.edge(), "children", Uuid::new_v4(), "parent_id", keyed);
    insert_child(
        link.edge(),
        "children",
        Uuid::new_v4(),
        "parent_id",
        predicated,
    );
    // One purge by key, one by predicate.
    exec(&hub, "PURGE FROM parents WHERE id = $id", &id_params(keyed));
    exec(&hub, "PURGE FROM parents WHERE batch = 'predicated'", &p());
    link.pull(&runtime);
    assert!(
        ids(link.edge(), "children").is_empty(),
        "the first delivery erases both children"
    );

    // The old parents come back at their keys as new lives, with fresh children.
    for (id, batch) in [(keyed, "keyed"), (predicated, "predicated")] {
        exec(
            &hub,
            &format!("INSERT INTO parents (id, batch) VALUES ($id, '{batch}')"),
            &id_params(id),
        );
    }
    link.pull(&runtime);
    let (fresh_keyed, fresh_predicated) = (Uuid::new_v4(), Uuid::new_v4());
    insert_child(link.edge(), "children", fresh_keyed, "parent_id", keyed);
    insert_child(
        link.edge(),
        "children",
        fresh_predicated,
        "parent_id",
        predicated,
    );

    // Reopen and rewind: the hub delivers both old purges again.
    link.reopen_edge(&runtime, true);
    link.pull(&runtime);
    let mut fresh = vec![fresh_keyed, fresh_predicated];
    fresh.sort();
    let survives = |db: &Database, label: &str| {
        assert_eq!(
            ids(db, "children"),
            fresh,
            "{label}: a re-delivered purge never reaches a fresh child"
        );
        let mut parents = vec![keyed, predicated];
        parents.sort();
        assert_eq!(
            ids(db, "parents"),
            parents,
            "{label}: the new parent lives stay"
        );
    };
    survives(link.edge(), "re-delivered");
    link.reopen_edge(&runtime, false);
    survives(link.edge(), "reopened");
    link.stop(&runtime);
}

#[test]
fn a_received_purge_erases_the_receivers_declared_node_local_children() {
    let runtime = sync_runtime();
    let root = tempfile::tempdir().expect("temporary test directory");
    let hub = Arc::new(Database::open(root.path().join("hub.db")).expect("open the hub"));
    exec(&hub, HUB_PARENTS_DDL, &p());
    let mut link = PushingSource::start(&runtime, root.path(), hub.clone());
    let (purged, kept) = (Uuid::new_v4(), Uuid::new_v4());
    for id in [purged, kept] {
        exec(
            &hub,
            "INSERT INTO parents (id, batch) VALUES ($id, 'b')",
            &id_params(id),
        );
    }
    // The schema reaches the edge by real delivery; the follower is the edge's own.
    link.pull(&runtime);
    exec(link.edge(), EDGE_CHILDREN_DDL, &p());
    let (purged_child, kept_child) = (Uuid::new_v4(), Uuid::new_v4());
    insert_child(link.edge(), "children", purged_child, "parent_id", purged);
    insert_child(link.edge(), "children", kept_child, "parent_id", kept);
    link.reopen_edge(&runtime, false);

    exec(
        &hub,
        "PURGE FROM parents WHERE id = $id",
        &id_params(purged),
    );
    link.pull(&runtime);
    let erased = |db: &Database, label: &str| {
        assert_eq!(
            ids(db, "parents"),
            vec![kept],
            "{label}: the purged parent is gone"
        );
        assert_eq!(
            ids(db, "children"),
            vec![kept_child],
            "{label}: only the purged parent's node-local child is erased"
        );
        assert!(
            follower_lives(db, "parents", purged).is_empty(),
            "{label}: no retained life of the erased child survives"
        );
    };
    erased(link.edge(), "received");
    link.reopen_edge(&runtime, false);
    erased(link.edge(), "reopened");
    link.stop(&runtime);
}

/// Open a store whose readers, the purge's follower discovery among them,
/// may hold at most `reader_limit` bytes.
fn open_with_reader_limit(path: &Path, reader_limit: Option<usize>) -> Database {
    let mut options = contextdb_engine::DatabaseOpenOptions::default();
    if let Some(limit) = reader_limit {
        options.owner_reads.limits.limits.memory = limit as _;
    }
    Database::open_with_options(path, options).expect("open the store")
}

const MANY: usize = 400;

fn insert_family(db: &Database, parent: Uuid) {
    for _ in 0..MANY {
        let child = Uuid::new_v4();
        insert_child(db, "children", child, "parent_id", parent);
        if db.table_meta("grandchildren").is_some() {
            insert_child(db, "grandchildren", Uuid::new_v4(), "child_id", child);
        }
    }
}

/// One refused attempt must be the existing typed budget refusal. True when
/// that refusal came from admitting followers, named by its operation.
fn budget_refusal(refused: &Error, label: &str, limit: usize, history: &[String]) -> bool {
    match refused {
        Error::MemoryBudgetExceeded { operation, .. } => operation.starts_with("purge_follower"),
        other => panic!(
            "{label}: a refusal at a {limit}-byte limit is Error::MemoryBudgetExceeded: {other:?}; before: {history:?}"
        ),
    }
}

#[test]
fn a_purge_whose_followers_exceed_the_memory_limit_is_refused_typed_and_changes_nothing() {
    // Local: children and grandchildren followed by one parent. The reader
    // limit grows until the purge fits; every refusal before it is typed and
    // changes nothing, and some refusal comes from admitting followers.
    let root = tempfile::tempdir().expect("temporary test directory");
    let path = root.path().join("budget.db");
    let parent = Uuid::new_v4();
    {
        let db = Database::open(&path).expect("open the store");
        create_following_tables(&db);
        exec(
            &db,
            "INSERT INTO parents (id, batch) VALUES ($id, 'b')",
            &id_params(parent),
        );
        insert_family(&db, parent);
        db.close().expect("close the store");
    }
    // The reader limit grows until a refusal comes from admitting followers;
    // every refusal up to it is the typed budget refusal and changes nothing.
    let mut history = Vec::new();
    let mut limit = 256 * 1024;
    loop {
        let db = open_with_reader_limit(&path, Some(limit));
        let refused = match db.execute("PURGE FROM parents WHERE id = $id", &id_params(parent)) {
            Ok(_) => panic!("local: the purge fit before any follower refusal: {history:?}"),
            Err(refused) => refused,
        };
        let from_followers = budget_refusal(&refused, "local", limit, &history);
        assert!(
            ids(&db, "parents") == vec![parent]
                && ids(&db, "children").len() == MANY
                && ids(&db, "grandchildren").len() == MANY,
            "local: a refused purge changes nothing: {refused}"
        );
        history.push(format!("{limit} {refused}"));
        db.close().expect("close the store");
        if from_followers {
            break;
        }
        limit += limit / 4;
        assert!(
            limit < 1 << 34,
            "local: no limit refused while admitting followers: {history:?}"
        );
    }
    let db = Database::open(&path).expect("reopen the store with room");
    exec(&db, "PURGE FROM parents WHERE id = $id", &id_params(parent));
    for table in ["parents", "children", "grandchildren"] {
        assert!(
            ids(&db, table).is_empty(),
            "local: with room the purge erases {table}"
        );
    }

    // Received: the edge's node-local children follow the hub's purge.
    let runtime = sync_runtime();
    let root = tempfile::tempdir().expect("temporary test directory");
    let hub = Arc::new(Database::open(root.path().join("hub.db")).expect("open the hub"));
    exec(&hub, HUB_PARENTS_DDL, &p());
    let link = PushingSource::start(&runtime, root.path(), hub.clone());
    exec(
        &hub,
        "INSERT INTO parents (id, batch) VALUES ($id, 'b')",
        &id_params(parent),
    );
    link.pull(&runtime);
    exec(link.edge(), EDGE_CHILDREN_DDL, &p());
    exec(
        link.edge(),
        "CREATE TABLE grandchildren (id UUID PRIMARY KEY, child_id UUID REFERENCES children(id) FOLLOWS PARENT PURGE, body TEXT) SYNC OFF",
        &p(),
    );
    insert_family(link.edge(), parent);
    exec(
        &hub,
        "PURGE FROM parents WHERE id = $id",
        &id_params(parent),
    );
    // A received purge prepares under the edge's engine sync budget, which
    // MEMORY_LIMIT bounds. Each limit leaves SET MEMORY_LIMIT its dedicated transaction
    // room; the steps stop at the first refusal from admitting followers.
    let edge = link.edge();
    let mut history = Vec::new();
    let mut limit_k = 0_usize;
    for step in 0.. {
        assert!(
            step < 8192,
            "received: no limit refused while admitting followers: {history:?}"
        );
        limit_k = (limit_k + 4).max(edge.accountant().usage().used.div_ceil(1024) + 64);
        exec(edge, &format!("SET MEMORY_LIMIT '{limit_k}K'"), &p());
        let refused = match runtime.block_on(link.client().pull_default()) {
            Ok(_) => panic!("received: the purge fit before any follower refusal: {history:?}"),
            Err(refused) => refused,
        };
        let from_followers = budget_refusal(&refused, "received", limit_k * 1024, &history);
        assert!(
            ids(edge, "parents") == vec![parent]
                && ids(edge, "children").len() == MANY
                && ids(edge, "grandchildren").len() == MANY,
            "received: a refused purge changes nothing: {refused}"
        );
        history.push(format!("{limit_k}K {refused}"));
        if from_followers {
            break;
        }
    }
    exec(edge, "SET MEMORY_LIMIT 'none'", &p());
    link.pull(&runtime);
    for table in ["parents", "children", "grandchildren"] {
        assert!(
            ids(edge, table).is_empty(),
            "received: with room the purge erases {table}"
        );
    }
    link.stop(&runtime);
}

/// A following child whose old declaration made graph edges: its DROP
/// already erased those edges, so purging its parent erases the archived body
/// without asking for a witness that no longer exists. A recreated child's
/// fresh row still carries its own witness, and unrelated edges survive.
#[test]
fn a_purge_erases_archived_edge_shaped_children() {
    const EDGE_CHILDREN: &str = "CREATE TABLE edge_children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id) FOLLOWS PARENT PURGE, source_id UUID, target_id UUID, edge_type TEXT) SYNC OFF";
    const TRAVERSE: &str = "SELECT t FROM GRAPH_TABLE(other_edges MATCH (a)-[:REL]->(b) WHERE a.id = $source_id COLUMNS (b.id AS t))";
    for recreated in [false, true] {
        let root = tempfile::tempdir().expect("temporary test directory");
        let path = root.path().join("edge-children.db");
        let db = Database::open(&path).expect("open the store");
        for ddl in [
            "CREATE TABLE parents (id UUID PRIMARY KEY, batch TEXT) SYNC OFF",
            EDGE_CHILDREN,
            "CREATE TABLE other_edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) SYNC OFF",
        ] {
            exec(&db, ddl, &p());
        }
        let parent = Uuid::new_v4();
        exec(
            &db,
            "INSERT INTO parents (id, batch) VALUES ($id, 'b')",
            &id_params(parent),
        );
        let edge = |db: &Database, table: &str, parent: Option<Uuid>| -> (Uuid, Uuid) {
            let (source, target) = (Uuid::new_v4(), Uuid::new_v4());
            let mut params = HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("source_id".to_string(), Value::Uuid(source)),
                ("target_id".to_string(), Value::Uuid(target)),
            ]);
            let sql = match parent {
                Some(parent) => {
                    params.insert("parent".to_string(), Value::Uuid(parent));
                    format!(
                        "INSERT INTO {table} (id, parent_id, source_id, target_id, edge_type) VALUES ($id, $parent, $source_id, $target_id, 'REL')"
                    )
                }
                None => format!(
                    "INSERT INTO {table} (id, source_id, target_id, edge_type) VALUES ($id, $source_id, $target_id, 'REL')"
                ),
            };
            exec(db, &sql, &params);
            (source, target)
        };
        edge(&db, "edge_children", Some(parent));
        let (other_source, other_target) = edge(&db, "other_edges", None);
        exec(&db, "DROP TABLE edge_children", &p());
        if recreated {
            exec(&db, EDGE_CHILDREN, &p());
            edge(&db, "edge_children", Some(parent));
        }
        db.close().expect("close the store");
        let db = Database::open(&path).expect("reopen the store");
        assert!(
            follower_lives(&db, "parents", parent)
                .iter()
                .any(|(table, retained)| table == "edge_children" && *retained),
            "recreated={recreated}: the archived edge-shaped life is found"
        );
        exec(&db, "PURGE FROM parents WHERE id = $id", &id_params(parent));
        let check = |db: &Database| {
            assert!(
                follower_lives(db, "parents", parent).is_empty(),
                "recreated={recreated}: no life of the purged parent's children survives"
            );
            if recreated {
                assert!(
                    ids(db, "edge_children").is_empty(),
                    "recreated={recreated}: the fresh edge-shaped child goes with its parent"
                );
            }
            let reached = db
                .execute(
                    TRAVERSE,
                    &HashMap::from([("source_id".to_string(), Value::Uuid(other_source))]),
                )
                .expect("traverse the unrelated edges")
                .rows()
                .iter()
                .map(|row| row[0].clone())
                .collect::<Vec<_>>();
            assert_eq!(
                reached,
                vec![Value::Uuid(other_target)],
                "recreated={recreated}: unrelated graph data survives"
            );
        };
        check(&db);
        db.close().expect("close the store");
        check(&Database::open(&path).expect("reopen the store"));
    }
}

// ---------------------------------------------------------------------------
// A purge erases received rows wherever their change-log record is filed.
// ---------------------------------------------------------------------------

/// A row whose two declared foreign keys join two parents written by earlier
/// commits: delivery carries the three as one dependency-complete unit, filed
/// under one acceptance envelope later than either parent's own position.
const LINKS_DDL: &str = "CREATE TABLE links (id UUID PRIMARY KEY, p_ref UUID REFERENCES parents(id) FOLLOWS PARENT PURGE, q_ref UUID REFERENCES parents(id), body TEXT) SYNC TWO WAY";

/// Three commits: parent P, parent Q, then a link that requires both.
fn write_multi_commit_unit(db: &Database) -> (Uuid, Uuid, Uuid) {
    let (p_id, q_id, link) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    for id in [p_id, q_id] {
        exec(
            db,
            "INSERT INTO parents (id, batch) VALUES ($id, 'b')",
            &id_params(id),
        );
    }
    exec(
        db,
        "INSERT INTO links (id, p_ref, q_ref, body) VALUES ($id, $p, $q, 'x')",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(link)),
            ("p".to_string(), Value::Uuid(p_id)),
            ("q".to_string(), Value::Uuid(q_id)),
        ]),
    );
    (p_id, q_id, link)
}

struct FixedClock;

impl contextdb_core::read_contract::DeadlineClock for FixedClock {
    fn now_ms(&self) -> u64 {
        1
    }

    fn wait_until(&self, _deadline_ms: u64) -> contextdb_core::read_contract::DeadlineWait<'_> {
        Box::pin(async {})
    }
}

/// A strict committed-history read of the closed store at `path`: it walks
/// every committed coordinate in lockstep with its physical record, so a
/// mis-filed or orphaned record is typed damage, not an answer.
fn strict_history_rows(path: &Path) -> usize {
    use contextdb_engine::direct_file_reader::test_seams::{
        DirectImageMetadataKind, DirectImageState, DirectMetadataBody, DirectMetadataRequest,
        DirectReaderConfig, open_for_test,
    };
    let config = DirectReaderConfig::new(
        contextdb_core::read_contract::ReadLimits {
            result_rows: 1_000,
            result_bytes: 64 * 1024 * 1024,
            work: 1_000_000,
            active_ms: 60_000,
            memory: 1024 * 1024 * 1024,
            cursor_page_rows: 100,
            cursor_page_bytes: 2 * 1024 * 1024,
            cursor_idle_ms: 10_000,
            cursor_lifetime_ms: 20_000,
        },
        Arc::new(FixedClock),
    );
    let reader = open_for_test(path, config).expect("open the store as a strict reader");
    match reader
        .metadata(DirectMetadataRequest::ImageState {
            kind: DirectImageMetadataKind::ChangeLog,
        })
        .expect("a strict committed-history read answers")
        .body
        .clone()
    {
        DirectMetadataBody::ImageState {
            state: DirectImageState::ChangeLog(changes),
        } => changes.rows.len(),
        other => panic!("change history answers as change-log state: {other:?}"),
    }
}

/// The hub received the unit by push; its records sit under one envelope.
fn hub_received_unit(
    runtime: &tokio::runtime::Runtime,
    root: &Path,
) -> (Arc<Database>, Uuid, Uuid, Uuid) {
    let hub = Arc::new(Database::open(root.join("hub.db")).expect("open the hub"));
    let link = PushingSource::start(runtime, root, hub.clone());
    exec(link.edge(), HUB_PARENTS_DDL, &p());
    exec(link.edge(), LINKS_DDL, &p());
    let ids = write_multi_commit_unit(link.edge());
    link.push(runtime);
    link.stop(runtime);
    (hub, ids.0, ids.1, ids.2)
}

/// Hub route: P's record is filed under the link's later envelope, with Q and
/// the link beside it in the same group.
#[test]
fn a_hub_purge_erases_pushed_rows_filed_under_a_later_envelope() {
    let runtime = sync_runtime();
    let root = tempfile::tempdir().expect("temporary test directory");
    let path = root.path().join("hub.db");
    let (hub, p_id, q_id, link) = hub_received_unit(&runtime, root.path());
    exec(&hub, "PURGE FROM parents WHERE id = $id", &id_params(p_id));
    assert_eq!(
        ids(&hub, "parents"),
        vec![q_id],
        "hub: P is gone and Q, filed beside it, stays"
    );
    assert!(ids(&hub, "links").is_empty(), "hub: the link follows P");
    hub.close().expect("close the hub");
    drop(hub);
    let after_p = strict_history_rows(&path);
    let hub = Database::open(&path).expect("reopen the hub");
    assert_eq!(
        ids(&hub, "parents"),
        vec![q_id],
        "hub reopened: Q survives P's erasure"
    );
    exec(&hub, "PURGE FROM parents WHERE id = $id", &id_params(q_id));
    assert!(
        ids(&hub, "parents").is_empty(),
        "hub: the survivor is erased from its re-densified group"
    );
    assert!(
        follower_lives(&hub, "parents", p_id).is_empty()
            && follower_lives(&hub, "parents", q_id).is_empty()
    );
    hub.close().expect("close the hub");
    drop(hub);
    assert!(
        strict_history_rows(&path) < after_p,
        "hub: the strict history read answers and has lost Q's record"
    );
    let hub = Database::open(&path).expect("reopen the hub");
    assert!(ids(&hub, "parents").is_empty() && ids(&hub, "links").is_empty());
    let _ = link;
}

/// Edge route: the hub authors the same three commits; the edge pulls them as
/// one dependency unit and applies the hub's purges.
#[test]
fn an_edge_applies_a_purge_of_pulled_rows_filed_under_a_later_envelope() {
    let runtime = sync_runtime();
    let root = tempfile::tempdir().expect("temporary test directory");
    let hub = Arc::new(Database::open(root.path().join("hub.db")).expect("open the hub"));
    exec(&hub, HUB_PARENTS_DDL, &p());
    exec(&hub, LINKS_DDL, &p());
    let mut edge = PushingSource::start(&runtime, root.path(), hub.clone());
    let (p_id, q_id, _) = write_multi_commit_unit(&hub);
    edge.pull(&runtime);
    assert_eq!(
        ids(edge.edge(), "parents").len(),
        2,
        "edge: the unit arrived"
    );
    exec(&hub, "PURGE FROM parents WHERE id = $id", &id_params(p_id));
    edge.pull(&runtime);
    assert_eq!(
        ids(edge.edge(), "parents"),
        vec![q_id],
        "edge: P is gone and Q stays"
    );
    assert!(
        ids(edge.edge(), "links").is_empty(),
        "edge: the link follows P"
    );
    edge.reopen_edge(&runtime, false);
    exec(&hub, "PURGE FROM parents WHERE id = $id", &id_params(q_id));
    edge.pull(&runtime);
    assert!(
        ids(edge.edge(), "parents").is_empty(),
        "edge: Q is erased from its group"
    );
    let edge_path = root.path().join("source.db");
    edge.stop(&runtime);
    // Panics with typed damage if any record is mis-filed or orphaned.
    strict_history_rows(&edge_path);
    let reopened = Database::open(&edge_path).expect("reopen the edge");
    assert!(ids(&reopened, "parents").is_empty() && ids(&reopened, "links").is_empty());
}

/// A purge refused for memory inside its commit is the typed memory
/// refusal, by category, never a corrupt store, and the store keeps serving.
/// The memory limit steps up until the purge fits. A refusal past the commit
/// boundary still latches the handle until reopen: the latched handle's next
/// answer is storage-io, and the sweep
/// reopens and continues.
#[test]
fn a_purge_refused_for_memory_inside_its_commit_is_the_typed_memory_refusal() {
    use contextdb_core::read_memory::NativeRefusal;
    let root = tempfile::tempdir().expect("temporary test directory");
    let path = root.path().join("budget.db");
    let mut db = Database::open(&path).expect("open the store");
    // A purge whose commit writes many table roots: the row, three secondary
    // indexes and a vector column.
    for ddl in [
        "CREATE TABLE parents (id UUID PRIMARY KEY, batch TEXT, tag TEXT, note TEXT, embedding VECTOR(2)) SYNC OFF",
        "CREATE INDEX parents_batch ON parents (batch)",
        "CREATE INDEX parents_tag ON parents (tag)",
        "CREATE INDEX parents_note ON parents (note)",
    ] {
        db.execute(ddl, &p()).unwrap();
    }
    let rows: Vec<Uuid> = (0..400).map(|_| Uuid::new_v4()).collect();
    let body = "b".repeat(200);
    for (n, id) in rows.iter().enumerate() {
        db.execute(
            &format!(
                "INSERT INTO parents (id, batch, tag, note, embedding) VALUES ($id, $batch, 't{n}', 'n{n}', [1, {n}])"
            ),
            &HashMap::from([
                ("id".to_string(), Value::Uuid(*id)),
                ("batch".to_string(), Value::Text(body.clone())),
            ]),
        )
        .unwrap();
    }
    let target = HashMap::from([("id".to_string(), Value::Uuid(rows[0]))]);
    let floor_k = db.accountant().usage().used.div_ceil(1024) + 256;
    let mut refusals = Vec::new();
    let mut latched = 0_usize;
    for step in 1..8192_usize {
        let limit_k = floor_k + step * 4;
        db.execute(&format!("SET MEMORY_LIMIT '{limit_k}K'"), &p())
            .unwrap();
        match db.execute("PURGE FROM parents WHERE id = $id", &target) {
            Ok(_) => break,
            Err(Error::Other(message))
                if message.to_string().contains("Previous I/O error occurred") =>
            {
                latched += 1;
                drop(db);
                db = Database::open(&path).expect("a latched store reopens");
            }
            Err(error) => {
                assert!(
                    matches!(
                        &error,
                        Error::MemoryBudgetExceeded { .. }
                            | Error::NativeEngineOther(
                                NativeRefusal::MemoryBudgetExceeded { .. }
                                    | NativeRefusal::ProcessMemoryBudgetExceeded { .. }
                            )
                    ),
                    "a purge refused at {limit_k}K is the typed memory refusal: {error:?}"
                );
                refusals.push(limit_k);
            }
        }
        assert!(step < 8191, "the purge never fitted");
    }
    eprintln!(
        "memory refusals before the purge fitted: {}, latched reopens: {latched}",
        refusals.len()
    );
    assert!(
        !refusals.is_empty(),
        "the sweep met at least one memory refusal"
    );
    db.execute("SET MEMORY_LIMIT 'none'", &p()).unwrap();
    db.execute(
        "INSERT INTO parents (id, batch) VALUES ($id, 'after')",
        &HashMap::from([("id".to_string(), Value::Uuid(Uuid::new_v4()))]),
    )
    .expect("the store keeps writing after the refusals");
    db.close().unwrap();
    drop(db);
    let db = Database::open(&path).expect("the store reopens");
    let count = db
        .execute("SELECT id FROM parents", &p())
        .unwrap()
        .rows()
        .len();
    assert_eq!(count, 400, "the purged row is gone and the others stay");
    db.close().unwrap();
}
