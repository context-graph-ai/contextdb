//! Shared production-path schema assertions and authenticated sync fixture.
#![allow(dead_code)]

pub mod largest;
#[path = "../pinned_cg_startup/mod.rs"]
pub mod pinned;

use contextdb_core::{Lsn, TenantId, Value};
use contextdb_engine::database::ReceivedSchemaApplyObservationForTest;
use contextdb_engine::sync_client::ApplicationTablePolicyExpectation;
use contextdb_engine::sync_types::DdlChange;
use contextdb_engine::{Database, MaintenancePolicy};
use contextdb_server::subjects::{binding_subject, pull_subject, push_subject};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use redb::{TableWriteForTest, TableWriteOperationForTest};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

/// Test callbacks are explicit host registrations, as at cg startup. Preserve
/// specialized callbacks already installed by behavior tests; never enable relay mode.
pub fn initialize_fixture_callbacks(db: &Database) {
    let registered = db.registered_trigger_callbacks();
    for trigger in db.list_triggers() {
        if !registered.contains(&trigger.name) {
            db.register_trigger_callback(&trigger.name, |_db, _ctx| Ok(()))
                .unwrap();
        }
    }
    db.complete_initialization()
        .expect("all fixture trigger callbacks registered before writes or sync apply");
}

/// Existing dependent tables must be staged in full, not merely a subset.
/// New-table tests keep the separate affected-only bound: those tables do not
/// have old images to copy. This equality is for the existing-table migration.
pub fn assert_dependency_complete(
    observation: &ReceivedSchemaApplyObservationForTest,
    affected: &BTreeSet<String>,
    contact_exchanges: u64,
) {
    assert_eq!(
        &observation.staged_table_names, affected,
        "every FK/propagation/trigger dependent is staged, and no unrelated table is staged"
    );
    assert_affected_only(observation, affected, contact_exchanges);
}

pub fn declare(db: &Database, ddls: &[&str]) {
    for (ordinal, ddl) in ddls.iter().enumerate() {
        db.execute(ddl, &empty()).unwrap_or_else(|error| {
            panic!("standalone source prerequisite ordinal={ordinal} DDL={ddl}: {error}")
        });
    }
}

pub fn install(db: &Database, ddls: &[&str]) {
    let tx = db.begin().expect("begin whole source migration");
    for (ordinal, ddl) in ddls.iter().enumerate() {
        if let Err(error) = db.execute_in_tx(tx, ddl, &empty()) {
            db.rollback(tx)
                .expect("rollback the refused complete migration");
            panic!(
                "SCHEMA_FIRST_FAILURE phase=whole-startup-stage ordinal={ordinal} DDL={ddl}: {error}"
            );
        }
    }
    db.commit(tx)
        .expect("commit every startup declaration once");
}

pub fn schema(db: &Database) -> Vec<String> {
    let mut answer = db
        .table_names()
        .into_iter()
        // Hub-local server bookkeeping, not application schema: a node that has
        // run a `SyncServer` declares the per-exchange liveness table itself and
        // never syncs it to an edge (`contextdb-server/src/work_ledger.rs`), so
        // comparing a hub's image against an edge's would otherwise report it as
        // a difference in the authored schema. The write-side oracle
        // (`is_data_table` below) already excludes its physical table.
        .filter(|name| name != "work_node_contacts")
        .map(|name| {
            let meta = db.table_meta(&name).expect("visible table metadata");
            let ddl = contextdb_engine::cli_render::render_table_meta(&name, &meta);
            // Public DDL rendering intentionally omits automatic indexes and
            // is not a complete metadata oracle (for example composite FKs).
            // Include every actual serializable field, with map ordering made
            // deterministic rather than Debug-printing randomized HashMaps.
            format!(
                "{ddl}\nmetadata:{}",
                canonical_json(&serde_json::to_value(&meta).unwrap())
            )
        })
        .collect::<Vec<_>>();
    answer.extend(db.list_triggers().iter().map(|v| format!("trigger:{v:?}")));
    let events = db.event_bus_status();
    answer.extend(events.event_types.iter().map(|v| format!("event:{v:?}")));
    // Sinks/routes are only authored by the supplemental schema fixture; cg's
    // exact pinned startup has none. Keep their declarations in the comparison.
    answer.extend(
        events
            .sinks
            .iter()
            .map(|v| format!("sink:{}:{}", v.name, v.sink_type)),
    );
    answer.extend(
        events
            .routes
            .iter()
            .map(|v| format!("route:{}:{}:{}", v.name, v.event_type, v.sink)),
    );
    answer.sort();
    answer
}

fn canonical_json(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Object(map) => {
            let sorted = map.iter().collect::<std::collections::BTreeMap<_, _>>();
            format!(
                "{{{}}}",
                sorted
                    .into_iter()
                    .map(|(key, value)| format!(
                        "{}:{}",
                        serde_json::to_string(key).unwrap(),
                        canonical_json(value)
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
        serde_json::Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",")
        ),
        _ => serde_json::to_string(value).unwrap(),
    }
}

pub fn reference(ddls: &[&str]) -> Database {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    declare(&db, ddls);
    db
}

pub fn assert_full_schema(db: &Database, expected: &Database) {
    let actual = schema(db);
    let expected = schema(expected);
    let missing = expected
        .iter()
        .filter(|declaration| !actual.contains(declaration))
        .collect::<Vec<_>>();
    let unexpected = actual
        .iter()
        .filter(|declaration| !expected.contains(declaration))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty() && unexpected.is_empty(),
        "all columns/types/constraints/indexes/vector/rank/DAG/event/trigger declarations match standalone installation; missing={missing:?}; unexpected={unexpected:?}"
    );
}

/// THE WHOLE IMAGE, ASSERTED AS ONE THING, AT EITHER SIDE OF A DURABLE
/// BOUNDARY.
///
/// The gate is "exactly the old complete state or exactly the complete
/// startup schema, never a proper subset", so one helper has to be able to
/// state BOTH, and has to fail on an extra declaration as loudly as on a
/// missing one. `expected` is a reference database carrying exactly the image
/// the caller claims -- the old one for a pre-durability fault, the whole
/// startup schema for a post-durability one -- and this compares every
/// declaration `schema()` renders: each table with its columns, types,
/// constraints and indexes, its vector/rank/partition and DAG declarations,
/// and every trigger, event type, sink and route, by complete serialized
/// metadata, not by name.
pub fn assert_whole_image(db: &Database, expected: &Database, phase: &str) {
    let actual = schema(db);
    let expected = schema(expected);
    let missing = expected
        .iter()
        .filter(|declaration| !actual.contains(declaration))
        .collect::<Vec<_>>();
    let unexpected = actual
        .iter()
        .filter(|declaration| !expected.contains(declaration))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty() && unexpected.is_empty(),
        "{phase}: the state after restart must be this exact whole image, never a proper subset \
         and never a stray extra declaration; missing={missing:?}; unexpected={unexpected:?}"
    );
}

/// The same whole-image claim, stated from the DDL text the image is built
/// from. Every crash-boundary assertion names its image this way so the old
/// complete state and the complete startup schema are written in one register.
pub fn assert_whole_image_of(db: &Database, statements: &[&str], phase: &str) {
    assert_whole_image(db, &reference(statements), phase);
}

/// The author's own order, kept once: the exact authored vector appears in the
/// receiver's durable DDL log exactly once, under one commit boundary, with no
/// unrelated declaration regrouped into it -- so a retry that re-applied a
/// prefix, or a receiver that split the migration, fails here.
pub fn assert_unit_kept_once(db: &Database, expected: &[DdlChange], phase: &str) -> Lsn {
    let changes = db.changes_since(Lsn(0));
    let occurrences = changes
        .ddl
        .windows(expected.len())
        .enumerate()
        .filter_map(|(index, window)| (window == expected).then_some(index))
        .collect::<Vec<_>>();
    assert_eq!(
        occurrences.len(),
        1,
        "{phase}: the authored migration must appear exactly once, in author order; \
         occurrences={occurrences:?} total_ddl={}",
        changes.ddl.len()
    );
    let start = occurrences[0];
    let unit_lsns = &changes.ddl_lsn[start..start + expected.len()];
    let lsn = unit_lsns[0];
    assert!(
        unit_lsns.iter().all(|value| *value == lsn),
        "{phase}: the authored migration must keep one receiver commit boundary: {unit_lsns:?}"
    );
    assert!(
        changes
            .ddl_lsn
            .iter()
            .enumerate()
            .all(|(index, value)| *value != lsn
                || (start..start + expected.len()).contains(&index)),
        "{phase}: nothing unrelated may be regrouped into the received migration"
    );
    let repeats = expected
        .iter()
        .filter(|statement| {
            changes
                .ddl
                .iter()
                .filter(|candidate| candidate == statement)
                .count()
                > 1
        })
        .collect::<Vec<_>>();
    assert!(
        repeats.is_empty(),
        "{phase}: a retry applied part of the migration twice: {repeats:?}"
    );
    lsn
}

pub fn assert_schema_contains(db: &Database, expected: &Database) {
    let actual = schema(db);
    let missing = schema(expected)
        .into_iter()
        .filter(|declaration| !actual.contains(declaration))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "the database is missing declarations from the expected application schema: {missing:?}"
    );
}

pub fn assert_one_ordered_unit(db: &Database, after: Lsn, expected: &[DdlChange]) -> Lsn {
    let changes = db.changes_since(after);
    assert_eq!(
        changes.ddl, expected,
        "exact authored DDL vector, with neither regrouping nor duplicate prefix"
    );
    assert_eq!(changes.ddl_lsn.len(), expected.len());
    let lsn = *changes
        .ddl_lsn
        .first()
        .expect("nonempty full startup migration");
    assert!(
        changes.ddl_lsn.iter().all(|value| *value == lsn),
        "one source migration must have one receiver commit boundary"
    );
    lsn
}

pub fn assert_contains_one_ordered_unit(
    db: &Database,
    after: Lsn,
    expected: &[DdlChange],
    phase: &str,
) -> Lsn {
    let changes = db.changes_since(after);
    let positions = changes
        .ddl
        .windows(expected.len())
        .enumerate()
        .filter_map(|(index, window)| (window == expected).then_some(index))
        .collect::<Vec<_>>();
    assert_eq!(
        positions.len(),
        1,
        "{phase}: the exact authored DDL vector must appear once amid unrelated schema progress; positions={positions:?} total={} lsns={:?}",
        changes.ddl.len(),
        positions
            .iter()
            .map(|start| changes.ddl_lsn[*start])
            .collect::<Vec<_>>()
    );
    let start = positions[0];
    let unit_lsns = &changes.ddl_lsn[start..start + expected.len()];
    let lsn = unit_lsns[0];
    assert!(
        unit_lsns.iter().all(|value| *value == lsn),
        "{phase}: the authored vector must retain one receiver commit boundary"
    );
    assert!(
        changes
            .ddl_lsn
            .iter()
            .enumerate()
            .all(|(index, value)| *value != lsn
                || (start..start + expected.len()).contains(&index)),
        "{phase}: unrelated declarations must not be regrouped into the received migration"
    );
    lsn
}

pub fn assert_no_schema(db: &Database) {
    assert_schema_not_visible(db);
    assert!(db.changes_since(Lsn(0)).ddl.is_empty());
}

/// Metadata inspection is available at an armed commit fence. Change-log
/// extraction deliberately waits for the commit mutex and belongs after reopen.
pub fn assert_schema_not_visible(db: &Database) {
    assert!(
        schema(db).is_empty(),
        "no application table/index/vector/graph declaration before publication; tables={:?}",
        db.table_names()
    );
    assert!(db.list_triggers().is_empty());
    let events = db.event_bus_status();
    assert!(events.event_types.is_empty());
    assert!(events.sinks.is_empty());
    assert!(events.routes.is_empty());
}

pub fn seed_cg_artifacts(db: &Database, seed: u128) {
    initialize_fixture_callbacks(db);
    let tx = db.begin().expect("real cg row/vector/graph transaction");
    let mut p = cg_artifact_params(seed);
    p.insert("tx".into(), Value::TxId(tx));
    for sql in [
        "INSERT INTO contexts (id, name, created_at) VALUES ($context, $name, $tx)",
        "INSERT INTO contexts (id, name, created_at) VALUES ($target, $target_name, $tx)",
        "INSERT INTO digests (id, source, summary, kind, vector_text, dimension, context_id, created_at) VALUES ($digest, 'schema-proof', 'durable schema artifact', 'text', $query, 768, $context, $tx)",
        "INSERT INTO edges (id, source_id, target_id, edge_type, context_id, valid_from) VALUES ($edge, $context, $target, 'CITES', $context, $tx)",
    ] {
        db.execute_in_tx(tx, sql, &p)
            .unwrap_or_else(|error| panic!("the exact cg schema must implement {sql}: {error}"));
    }
    db.commit(tx)
        .expect("publish actual cg vector/graph artifacts");
}

pub fn cg_artifact_params(seed: u128) -> HashMap<String, Value> {
    let mut vector = vec![0.0; 768];
    vector[0] = 1.0;
    HashMap::from([
        ("context".into(), Value::Uuid(uuid::Uuid::from_u128(seed))),
        (
            "target".into(),
            Value::Uuid(uuid::Uuid::from_u128(seed + 1)),
        ),
        (
            "digest".into(),
            Value::Uuid(uuid::Uuid::from_u128(seed + 2)),
        ),
        ("edge".into(), Value::Uuid(uuid::Uuid::from_u128(seed + 3))),
        ("name".into(), Value::Text(format!("schema-context-{seed}"))),
        (
            "target_name".into(),
            Value::Text(format!("schema-target-{seed}")),
        ),
        ("query".into(), Value::Vector(vector)),
    ])
}

pub fn assert_cg_artifact_queries(
    mut execute: impl FnMut(&str, &HashMap<String, Value>) -> Vec<Vec<Value>>,
    seed: u128,
) {
    let p = cg_artifact_params(seed);
    assert_eq!(
        execute(
            "SELECT id FROM digests WHERE context_id = $context ORDER BY vector_text <=> $query LIMIT 1",
            &p
        ),
        vec![vec![p["digest"].clone()]]
    );
    assert_eq!(
        execute(
            "SELECT target FROM GRAPH_TABLE(edges MATCH (a)-[:CITES]->(b) WHERE a.id = $context COLUMNS (b.id AS target))",
            &p
        ),
        vec![vec![p["target"].clone()]]
    );
    assert_eq!(
        execute(
            "SELECT source FROM GRAPH_TABLE(edges MATCH (a)<-[:CITES]-(b) WHERE a.id = $target COLUMNS (b.id AS source))",
            &p
        ),
        vec![vec![p["context"].clone()]]
    );
}

pub fn assert_cg_artifacts(db: &Database, seed: u128) {
    assert!(
        db.column_has_covering_index("edges", "source_id"),
        "the actual cg composite source index is installed"
    );
    assert_cg_artifact_queries(|sql, p| db.execute(sql, p).unwrap().rows, seed);
    let mut p = cg_artifact_params(seed);
    p.insert(
        "back_edge".into(),
        Value::Uuid(uuid::Uuid::from_u128(seed + 4)),
    );
    p.insert("tx".into(), Value::TxId(contextdb_core::TxId(1)));
    assert!(db.execute("INSERT INTO edges (id, source_id, target_id, edge_type, context_id, valid_from) VALUES ($back_edge, $target, $context, 'CITES', $context, $tx)", &p).is_err(), "the recovered actual cg DAG still refuses a cycle");
}

pub struct Hub {
    pub db: Arc<Database>,
    pub broker: InProcessBroker,
    pub server: Arc<SyncServer>,
    stop: Arc<AtomicBool>,
    task: Option<tokio::task::JoinHandle<()>>,
    tenant: String,
}

impl Hub {
    pub async fn start(db: Arc<Database>, tenant: &str) -> Self {
        let broker = InProcessBroker::new();
        let identity = Arc::new(FabricIdentity::generate());
        let node = identity.node_id();
        let server = Arc::new(
            SyncServer::with_authenticated_transport_and_identity_for_test(
                db.clone(),
                broker.server_as(&node),
                TenantId::from(tenant),
                node,
                identity,
            ),
        );
        let stop = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let server = server.clone();
            let stop = stop.clone();
            async move { server.run_until(stop).await }
        });
        for subject in [
            push_subject(tenant),
            pull_subject(tenant),
            binding_subject(tenant),
        ] {
            broker.wait_for_registered_route_for_test(&subject).await;
        }
        Self {
            db,
            broker,
            server,
            stop,
            task: Some(task),
            tenant: tenant.to_string(),
        }
    }

    pub fn client(&self, db: Arc<Database>) -> SyncClient {
        let identity = Arc::new(FabricIdentity::generate());
        SyncClient::with_authenticated_transport_and_identity_for_test(
            db,
            self.broker.client_as(&identity.node_id()),
            TenantId::from(self.tenant.as_str()),
            identity,
        )
    }

    pub async fn bind(&self, client: &SyncClient) {
        // These fixtures declare no tenant-governed application table. An empty
        // expectation still authenticates and durably binds the exact hub life.
        client
            .bind_application_table_policy(ApplicationTablePolicyExpectation::new())
            .await
            .expect("bind the actual authenticated tenant hub before installing schema");
    }

    pub async fn stop(mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.task.take().unwrap().await.expect("stop schema hub");
    }
}

pub fn is_data_table(table: &str) -> bool {
    // Receipt/log/format bookkeeping and the required per-exchange liveness
    // row with its authenticated roots are excluded from the schema-image
    // assertion. Unknown artifacts still count. All of these writes remain in
    // the unfiltered operation/byte receipt and focused work counters.
    //
    // `read_open_config` binds the copied open settings to the committed
    // Configuration content root. A schema commit touching a bound key can
    // update that durable artifact, which is configuration bookkeeping rather
    // than a data-table schema write.
    !matches!(
        table,
        "config"
            | "change_log"
            | "change_log_by_table"
            | "ddl_log"
            | "commit_index"
            | "metadata"
            | "read_open_config"
            | CONTACT_TABLE
            | "__content_identity_nodes"
            | "__content_identity_roots"
    )
}

const CONTACT_TABLE: &str = "rel_work_node_contacts";

pub fn is_data_write(event: &TableWriteForTest) -> bool {
    if event.table == "config" {
        return event.key == b"__trigger_declarations"
            || event.key == b"__trigger_including_sync"
            || event.key.starts_with(b"__event_bus_");
    }
    if event.table == "__redb_user_table_directory" {
        return is_data_table(std::str::from_utf8(&event.key).expect("directory uses UTF-8 names"));
    }
    is_data_table(&event.table)
}

/// Actual serialized payload inserted/removed, grouped by operation in receipts.
/// Table deletion has no row payload; its directory removal is observed separately.
pub fn serialized_bytes(event: &TableWriteForTest, count: u64) -> u64 {
    match event.operation {
        TableWriteOperationForTest::DeleteTable => 0,
        _ => (event.key.len() as u64 + event.value_bytes as u64) * count,
    }
}

pub fn assert_zero_images(observation: &ReceivedSchemaApplyObservationForTest) {
    assert!(
        observation.durable_observer_attached,
        "zero work requires an actual instance-scoped storage observer"
    );
    assert_eq!(observation.staged_table_images_copied, 0, "{observation:?}");
    assert_eq!(observation.staged_rows_copied, 0);
    assert_eq!(observation.staged_graph_edges_copied, 0);
    assert_eq!(observation.staged_vectors_copied, 0);
    assert_eq!(observation.durable_table_rewrites, 0);
    assert_eq!(observation.durable_rows_rewritten, 0);
    let writes = observation
        .artifact_writes
        .iter()
        .filter(|(event, _)| is_data_write(event))
        .collect::<Vec<_>>();
    assert!(
        writes.is_empty(),
        "identical received declarations performed actual image writes: {writes:?}"
    );
    assert_eq!(
        writes
            .iter()
            .map(|(event, count)| serialized_bytes(event, **count))
            .sum::<u64>(),
        0
    );
}

pub fn assert_affected_only(
    observation: &ReceivedSchemaApplyObservationForTest,
    affected: &BTreeSet<String>,
    contact_exchanges: u64,
) {
    assert!(observation.durable_observer_attached);
    assert!(
        observation.staged_table_names.is_subset(affected),
        "unrelated detached copies: {observation:?}"
    );
    assert!(
        observation.durable_table_names.is_subset(affected),
        "unrelated relational rewrites: {observation:?}"
    );
    let identity_writes = observation
        .artifact_writes
        .iter()
        .filter(|(event, _)| {
            event.table.starts_with("__content_identity_")
                || (event.table == "__redb_user_table_directory"
                    && event.key.starts_with(b"__content_identity_"))
        })
        .map(|(_, count)| *count)
        .sum::<u64>();
    // One logical leaf update rewrites the existing Merkle-treap ancestor
    // path in each typed family. Keep that bounded per affected table while
    // remaining orders of magnitude below the 10,000 unrelated resident
    // leaves, so a reset or corpus rebuild still fails this oracle.
    assert!(
        identity_writes <= affected.len() as u64 * 256,
        "content identity work exceeded the affected schema closure: {observation:?}"
    );
    let index_artifact_writes = observation
        .artifact_writes
        .iter()
        .filter(|(event, _)| {
            event.table.starts_with("__rel_index_")
                || (event.table == "__redb_user_table_directory"
                    && event.key.starts_with(b"__rel_index_"))
        })
        .map(|(_, count)| *count)
        .sum::<u64>();
    assert!(
        index_artifact_writes <= affected.len() as u64 * 8,
        "physical index work exceeded the affected schema closure: {observation:?}"
    );
    let rewritten_row_routes = observation.artifact_writes.iter()
        .filter_map(|(event, _)| {
            let owner = event.table.strip_prefix("rel_")?;
            if event.operation != TableWriteOperationForTest::Insert || event.key.len() != 24 {
                return None;
            }
            let row = u64::from_be_bytes(event.key[..8].try_into().unwrap());
            Some((owner.to_owned(), row))
        }).collect::<BTreeSet<_>>();
    let mut contact_versions = 0;
    let mut contact_directory_installs = 0;
    for (event, count) in &observation.artifact_writes {
        assert!(*count > 0);
        if event.table == "meta" && event.key.starts_with(b"table:") {
            let owner = std::str::from_utf8(&event.key[6..]).unwrap();
            assert!(
                affected.contains(owner),
                "unrelated metadata rewrite {event:?}"
            );
        }
        let table = if event.table == "__redb_user_table_directory" {
            std::str::from_utf8(&event.key).unwrap()
        } else {
            &event.table
        };
        if table == "writer_recovery" {
            if event.table == "__redb_user_table_directory" {
                assert_eq!(event.operation, TableWriteOperationForTest::DirectoryInsert);
                // Each of the three fixed controls opens and updates the table root.
                assert!(*count <= 3 * (1 + contact_exchanges));
            } else {
                assert_eq!(event.operation, TableWriteOperationForTest::Insert);
                assert!(matches!(event.key.as_slice(), b"bounds" | b"commit_directory_root" | b"row_heads_root"),
                    "unexpected recovery control coordinate {event:?}");
                assert!(*count <= 1 + contact_exchanges);
            }
            continue;
        }
        if table == "writer_row_heads" {
            if event.table == "__redb_user_table_directory" {
                assert_eq!(event.operation, TableWriteOperationForTest::DirectoryInsert);
                assert!(*count <= 1 + observation.durable_rows_rewritten as u64 * 2 + contact_exchanges * 2);
            } else {
                assert_eq!(event.operation, TableWriteOperationForTest::Insert);
                let owner_len = u64::from_le_bytes(event.key[..8].try_into().unwrap()) as usize;
                assert_eq!(event.key.len(), owner_len + 24);
                let owner = std::str::from_utf8(&event.key[8..8 + owner_len]).unwrap();
                assert!(affected.contains(owner) || owner == "work_node_contacts", "unaffected row-head owner {event:?}");
                assert_eq!(u64::from_le_bytes(event.key[8 + owner_len..16 + owner_len].try_into().unwrap()), 8);
                let row_id = u64::from_le_bytes(event.key[16 + owner_len..].try_into().unwrap());
                assert!(rewritten_row_routes.contains(&(owner.to_owned(), row_id)),
                    "row-head route must match an actual affected relational rewrite {event:?}");
                assert_eq!(event.value_bytes, 56);
                assert!(*count <= 2 + contact_exchanges * 2);
            }
            continue;
        }
        if table == "change_log_by_table" {
            if event.table != "__redb_user_table_directory" {
                let end = event
                    .key
                    .iter()
                    .position(|byte| *byte == 0)
                    .expect("table-selected history key has an owner delimiter");
                let owner = std::str::from_utf8(&event.key[..end]).unwrap();
                assert!(
                    affected.contains(owner) || owner == "work_node_contacts",
                    "unrelated table-selected history write {event:?}"
                );
                assert_eq!(
                    event.key.len(),
                    end + 1 + 16,
                    "table-selected history carries an exact LSN/ordinal coordinate"
                );
            }
            continue;
        }
        if table == CONTACT_TABLE {
            match event.operation {
                TableWriteOperationForTest::Insert if event.table == CONTACT_TABLE => {
                    contact_versions += count;
                }
                TableWriteOperationForTest::DirectoryInsert
                    if event.table == "__redb_user_table_directory" =>
                {
                    contact_directory_installs += count;
                }
                _ => panic!("unexpected contact receipt write {event:?}"),
            }
            continue;
        }
        if table.starts_with("__content_identity_") || table.starts_with("__rel_index_") {
            continue;
        }
        if let Some(owner) = table.strip_prefix("rel_") {
            assert!(
                affected.contains(owner),
                "unrelated relational/catalog write {event:?}"
            );
        }
        // The exercised migration has no graph/vector/sync-row mutations.
        // Even clearing/recreating an empty artifact is real forbidden work.
        assert!(
            !table.starts_with("graph_")
                && !table.starts_with("vector_")
                && !table.starts_with("sync_row_source_"),
            "unaffected image write {event:?}"
        );
        assert!(
            !is_data_table(table) || table == "meta" || table.starts_with("rel_"),
            "an unanticipated durable artifact cannot be omitted from the affected-set proof: {event:?}"
        );
    }
    let row_head_writes = observation.artifact_writes.iter()
        .filter(|(event, _)| event.table == "writer_row_heads")
        .map(|(_, count)| *count).sum::<u64>();
    assert!(row_head_writes <= observation.durable_rows_rewritten as u64 * 2 + contact_exchanges * 2,
        "row-head publication exceeds actual affected mutations: {observation:?}");
    let recovery_writes = observation.artifact_writes.iter()
        .filter(|(event, _)| event.table == "writer_recovery")
        .map(|(_, count)| *count).sum::<u64>();
    assert!(recovery_writes <= 3 * (1 + contact_exchanges));
    let table_history_writes = observation
        .artifact_writes
        .iter()
        .filter(|(event, _)| {
            event.table == "change_log_by_table"
                || (event.table == "__redb_user_table_directory"
                    && event.key == b"change_log_by_table")
        })
        .map(|(_, count)| *count)
        .sum::<u64>();
    assert!(
        table_history_writes <= affected.len() as u64 * 2 + contact_exchanges * 4 + 1,
        "derived history work must stay inside the affected set, never replay unrelated history: {observation:?}"
    );
    assert!(
        contact_versions == contact_exchanges.saturating_mul(2),
        "each authenticated exchange has exactly its old and new contact versions: observed={contact_versions} exchanges={contact_exchanges}"
    );
    // DirectoryInsert covers first install and existing-table root updates in the
    // redb fork (table_tree.rs:590 and :401), so bound it per contact exchange.
    assert!(
        contact_directory_installs <= contact_exchanges,
        "contact directory writes exceed the one-root-update-per-exchange bound: installs={contact_directory_installs} exchanges={contact_exchanges}"
    );
}

impl Drop for Hub {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub fn assert_authored_wire(
    broker: &InProcessBroker,
    expected: &[DdlChange],
    source_lsn: Lsn,
    incarnation: contextdb_core::Incarnation,
) -> usize {
    use contextdb_server::protocol::{
        MessageType, PushRequest, canonical_ddl_provenance_digest, decode,
    };
    let mut units = 0;
    for exchange in broker.recorded_exchanges() {
        let envelope = decode(&exchange.request_bytes).expect("decode actual transport envelope");
        if !matches!(
            envelope.message_type,
            MessageType::PushRequest | MessageType::DependencyCompletePushRequest
        ) {
            continue;
        }
        let request: PushRequest =
            rmp_serde::from_slice(&envelope.payload).expect("actual push payload");
        if request.changeset.ddl.is_empty() {
            continue;
        }
        units += 1;
        assert_eq!(
            request.incarnation, incarnation,
            "wire preserves the source database life"
        );
        let changes = &request.changeset;
        let actual = changes
            .ddl
            .clone()
            .into_iter()
            .map(DdlChange::from)
            .collect::<Vec<_>>();
        let mismatch = actual
            .iter()
            .zip(expected)
            .position(|(actual, expected)| actual != expected);
        assert!(
            actual == expected,
            "transport never splits/regroups the source migration: actual_len={} expected_len={} first_mismatch={mismatch:?} actual_at_mismatch={:?} expected_at_mismatch={:?}",
            actual.len(),
            expected.len(),
            mismatch.and_then(|index| actual.get(index)),
            mismatch.and_then(|index| expected.get(index)),
        );
        assert_eq!(changes.ddl_lsn.len(), expected.len());
        assert!(changes.ddl_lsn.iter().all(|lsn| *lsn == source_lsn));
        assert_eq!(changes.ddl_provenance.len(), expected.len());
        for (ordinal, (ddl, provenance)) in
            changes.ddl.iter().zip(&changes.ddl_provenance).enumerate()
        {
            assert_eq!(provenance.source_ddl_lsn, source_lsn);
            assert_eq!(provenance.ordinal as usize, ordinal);
            assert_eq!(
                provenance.digest,
                canonical_ddl_provenance_digest(
                    ddl,
                    source_lsn,
                    ordinal as u32,
                    provenance.table.as_deref(),
                    provenance.table_generation
                )
                .unwrap()
            );
        }
    }
    units
}

struct Clock;
impl contextdb_core::read_contract::DeadlineClock for Clock {
    fn now_ms(&self) -> u64 {
        1
    }
    fn wait_until(&self, _deadline: u64) -> contextdb_core::read_contract::DeadlineWait<'_> {
        Box::pin(std::future::pending())
    }
}

pub fn committed_identity(path: &std::path::Path) -> [u8; 32] {
    use contextdb_engine::direct_file_reader::test_seams::{
        DirectReaderConfig, open_for_test, verified_image_digest_for_test,
    };
    let reader = open_for_test(
        path,
        DirectReaderConfig::new(Default::default(), Arc::new(Clock)),
    )
    .expect("open the actual committed file image");
    verified_image_digest_for_test(&reader)
        .expect("verified content identity")
        .complete
}
