use contextdb_core::{Error, Lsn, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{
    ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange, NaturalKey,
};
use contextdb_server::chunking::CHUNKING_THRESHOLD;
use contextdb_server::protocol::{
    DependencyCompletePullResponse, Envelope, MessageType, PullRequest, PullResponse, PushRequest,
    PushResponse, SyncStatusRequest, SyncStatusResponse, WireApplyResult, WireChangeSet,
    WireDdlChange, WireDdlProvenance, canonical_ddl_provenance_digest, decode, encode,
};
use contextdb_server::subjects::{pull_subject, push_subject, status_subject};
use contextdb_server::transport::{
    ClientTransport, TransportError, TransportFuture, TransportResult, TransportStatusFuture,
};
use contextdb_server::{
    FabricIdentity, InProcessBroker, SyncClient, SyncServer,
    acceptance_stamped_push_batches_for_test, split_changeset_for_test,
};
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

const CONCRETE_TRANSPORT_ADAPTER: &str = concat!("ir", "oh");

async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(20), fut)
        .await
        .expect("bounded transport operation exceeded 20s")
}

fn authenticated_server(
    db: Arc<Database>,
    broker: &InProcessBroker,
    tenant: &str,
) -> Arc<SyncServer> {
    let identity = Arc::new(FabricIdentity::generate());
    let node_id = identity.node_id();
    Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            db,
            broker.server_as(&node_id),
            contextdb_core::TenantId::from(tenant),
            node_id,
            identity,
        ),
    )
}

fn authenticated_client(db: Arc<Database>, broker: &InProcessBroker, tenant: &str) -> SyncClient {
    let identity = Arc::new(FabricIdentity::generate());
    authenticated_client_with_identity(db, broker, tenant, identity)
}

fn authenticated_client_with_identity(
    db: Arc<Database>,
    broker: &InProcessBroker,
    tenant: &str,
    identity: Arc<FabricIdentity>,
) -> SyncClient {
    let node_id = identity.node_id();
    SyncClient::with_authenticated_transport_and_identity_for_test(
        db,
        broker.client_as(&node_id),
        contextdb_core::TenantId::from(tenant),
        identity,
    )
}

/// Construct an in-process fake hub over `broker` and immediately spawn its
/// serving loop, returning the hub's store, its shutdown flag, and the server
/// task. (`ip_00` deliberately spawns late to probe the pre-run no-responder
/// state, so it keeps its own bespoke bring-up.)
fn start_fake_hub(
    broker: &InProcessBroker,
    tenant: &str,
) -> (Arc<Database>, Arc<AtomicBool>, tokio::task::JoinHandle<()>) {
    let server_db = Arc::new(Database::open_memory());
    let server = authenticated_server(server_db.clone(), broker, tenant);
    let shutdown = Arc::new(AtomicBool::new(false));
    let server_task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    (server_db, shutdown, server_task)
}

fn column_index(columns: &[String], name: &str) -> usize {
    columns
        .iter()
        .position(|c| c == name)
        .unwrap_or_else(|| panic!("result must project column {name}: {columns:?}"))
}

fn expect_text_value(db: &Database, table: &str, column: &str, id: Uuid, expected: &str) {
    let mut key = HashMap::new();
    key.insert("id".to_string(), Value::Uuid(id));
    let sql = format!("SELECT {column} FROM {table} WHERE id = $id");
    let result = db
        .execute(&sql, &key)
        .unwrap_or_else(|err| panic!("{table}.{column} lookup must succeed: {err}"));
    assert_eq!(
        result.rows.len(),
        1,
        "{table} must hold exactly the row {id}"
    );
    let idx = column_index(&result.columns, column);
    assert_eq!(
        result.rows[0][idx],
        Value::Text(expected.to_string()),
        "{table}.{column} for {id} must match"
    );
}

fn uuid_set(db: &Database, table: &str) -> BTreeSet<Uuid> {
    let result = db
        .execute(&format!("SELECT id FROM {table}"), &HashMap::new())
        .unwrap_or_else(|err| panic!("{table} id scan must succeed: {err}"));
    let id_idx = column_index(&result.columns, "id");
    result
        .rows
        .iter()
        .map(|row| match &row[id_idx] {
            Value::Uuid(id) => *id,
            other => panic!("{table}.id must be UUID, got {other:?}"),
        })
        .collect()
}

fn exchanges_on(broker: &InProcessBroker, subject: &str) -> Vec<(Envelope, Envelope)> {
    broker
        .recorded_exchanges()
        .into_iter()
        .filter(|exchange| exchange.subject == subject)
        .map(|exchange| {
            let request = decode(&exchange.request_bytes)
                .unwrap_or_else(|err| panic!("decode request bytes on {subject}: {err}"));
            let response = decode(&exchange.response_bytes)
                .unwrap_or_else(|err| panic!("decode response bytes on {subject}: {err}"));
            (request, response)
        })
        .collect()
}

fn payload<T: DeserializeOwned>(envelope: &Envelope, expected: MessageType, label: &str) -> T {
    assert_eq!(
        envelope.message_type, expected,
        "{label} envelope type must match"
    );
    rmp_serde::from_slice(&envelope.payload)
        .unwrap_or_else(|err| panic!("decode {label} payload: {err}"))
}

/// An authenticated status probe records the contacting edge in the hub's
/// system state before answering. Its hub LSN is therefore a live position,
/// not a fixture-owned constant, while the per-edge source watermark remains
/// exact.
fn assert_authenticated_status_exchange(
    exchange: &(Envelope, Envelope),
    expected_applied_push_watermark: Lsn,
    minimum_hub_lsn_exclusive: Lsn,
    label: &str,
) -> SyncStatusResponse {
    let request: SyncStatusRequest = payload(
        &exchange.0,
        MessageType::StatusRequest,
        &format!("{label} request"),
    );
    let reencoded = rmp_serde::to_vec(&request).expect("re-encode status request payload");
    assert_eq!(
        reencoded, exchange.0.payload,
        "{label} request payload must round-trip exactly"
    );
    let response: SyncStatusResponse = payload(
        &exchange.1,
        MessageType::StatusResponse,
        &format!("{label} response"),
    );
    assert_eq!(
        response.applied_push_watermark,
        Some(expected_applied_push_watermark),
        "{label} must preserve the exact per-edge source watermark"
    );
    assert!(
        response
            .server_current_lsn
            .is_some_and(|lsn| lsn > minimum_hub_lsn_exclusive),
        "{label} must report the live hub position after authenticated contact: {response:?}"
    );
    response
}

/// Both ordinary and dependency-complete pushes share the exact same
/// `PushRequest` payload.  The envelope type says whether the receiving hub
/// must validate the batch as one indivisible dependency unit before commit.
fn push_request_payload(envelope: &Envelope, label: &str) -> (PushRequest, bool) {
    let dependency_complete = match &envelope.message_type {
        MessageType::PushRequest => false,
        MessageType::DependencyCompletePushRequest => true,
        other => panic!(
            "{label} envelope must be an ordinary or dependency-complete push request, got {other:?}"
        ),
    };
    let request = rmp_serde::from_slice(&envelope.payload)
        .unwrap_or_else(|err| panic!("decode {label} payload: {err}"));
    (request, dependency_complete)
}

fn assert_push_exchange(
    exchange: &(Envelope, Envelope),
    expected_dependency_complete: bool,
    expected_changeset: WireChangeSet,
    expected_response: PushResponse,
    label: &str,
) {
    let (request, dependency_complete) =
        push_request_payload(&exchange.0, &format!("{label} request"));
    assert_eq!(
        dependency_complete, expected_dependency_complete,
        "{label} request envelope must preserve whether this exact batch is an indivisible dependency unit"
    );
    assert_eq!(
        request.changeset, expected_changeset,
        "{label} request payload must be the expected sync batch"
    );
    let response: PushResponse = payload(
        &exchange.1,
        MessageType::PushResponse,
        &format!("{label} response"),
    );
    assert_eq!(
        response, expected_response,
        "{label} response payload must be the server apply result"
    );
}

/// A pull may carry multi-megabyte text rows. Failure diagnostics name every
/// structural component needed to locate a wire split/provenance defect, but
/// deliberately never render row values; the final equality below still
/// compares every byte-bearing field exactly.
fn wire_changeset_summary(changes: &WireChangeSet) -> String {
    let rows = changes
        .rows
        .iter()
        .map(|row| {
            let lineage = row.lineage.as_ref().map(|lineage| {
                format!(
                    "{}:{}:{}:{}:{}:{}",
                    lineage.author_node_id,
                    lineage.author_database_incarnation,
                    lineage.author_local_mutation_position.0,
                    lineage.table_generation,
                    lineage.lineage_root,
                    lineage.attestation.len(),
                )
            });
            format!(
                "{}:{:?}:{}:{}:{:?}:{}",
                row.table,
                row.natural_key,
                row.lsn.0,
                row.deleted,
                row.arrival,
                lineage.unwrap_or_else(|| "none".to_string()),
            )
        })
        .collect::<Vec<_>>();
    let ddl = changes
        .ddl
        .iter()
        .zip(&changes.ddl_lsn)
        .map(|(ddl, lsn)| format!("{ddl:?}@{}", lsn.0))
        .collect::<Vec<_>>();
    format!(
        "ddl={ddl:?}; ddl_provenance={}; rows={rows:?}; edges={}; vectors={}; purges={}",
        changes.ddl_provenance.len(),
        changes.edges.len(),
        changes.vectors.len(),
        changes.purges.len(),
    )
}

fn assert_pull_exchange(
    exchange: &(Envelope, Envelope),
    expected_request: PullRequest,
    expected_response: DependencyCompletePullResponse,
    label: &str,
) {
    let request: PullRequest = payload(
        &exchange.0,
        MessageType::PullRequest,
        &format!("{label} request"),
    );
    assert_eq!(request, expected_request, "{label} request payload");
    let response: DependencyCompletePullResponse = payload(
        &exchange.1,
        MessageType::DependencyCompletePullResponse,
        &format!("{label} response"),
    );
    assert_eq!(
        response.ordinary.has_more, expected_response.ordinary.has_more,
        "{label} ordinary page completion flag"
    );
    assert_eq!(
        response.ordinary.cursor, expected_response.ordinary.cursor,
        "{label} ordinary page cursor"
    );
    assert_eq!(
        response.ordinary.source, expected_response.ordinary.source,
        "{label} ordinary page source identity"
    );
    assert_eq!(
        response.units.len(),
        expected_response.units.len(),
        "{label} dependency-complete unit count"
    );
    assert_eq!(
        wire_changeset_summary(&response.ordinary.changeset),
        wire_changeset_summary(&expected_response.ordinary.changeset),
        "{label} ordinary changeset structure"
    );
    for (index, (actual, expected)) in response
        .units
        .iter()
        .zip(expected_response.units.iter())
        .enumerate()
    {
        assert_eq!(
            wire_changeset_summary(actual),
            wire_changeset_summary(expected),
            "{label} dependency-complete unit {index} structure"
        );
    }
    assert!(
        response == expected_response,
        "{label} dependency-complete response differs after matching bounded structure; payload values or attestation bytes differ"
    );
}

fn expected_apply_results_for_batches(
    batches: &[ChangeSet],
    policies: &ConflictPolicies,
) -> Vec<WireApplyResult> {
    let mirror = Database::open_memory();
    batches
        .iter()
        .enumerate()
        .map(|(idx, batch)| {
            mirror
                .apply_changes(batch.clone(), policies)
                .unwrap_or_else(|err| panic!("mirror apply for expected batch {idx}: {err}"))
                .into()
        })
        .collect()
}

/// The `new_lsn` in a push reply belongs to the accepting hub, not the edge
/// batch or an independent mirror. Recover that exact live position from the
/// hub's post-apply history by matching the same schema/data members that the
/// batch carried.
fn live_hub_position_for_batch(hub_changes: &ChangeSet, batch: &ChangeSet) -> Lsn {
    let row_positions = batch.rows.iter().flat_map(|expected| {
        hub_changes
            .rows
            .iter()
            .filter(move |observed| {
                observed.table == expected.table && observed.natural_key == expected.natural_key
            })
            .map(|observed| observed.lsn)
    });
    let ddl_positions = batch.ddl.iter().flat_map(|expected| {
        let expected_table = match expected {
            DdlChange::CreateTable { name, .. }
            | DdlChange::DropTable { name }
            | DdlChange::AlterTable { name, .. } => Some(name),
            _ => None,
        };
        hub_changes
            .ddl
            .iter()
            .zip(&hub_changes.ddl_lsn)
            .filter(move |(observed, _)| match (expected_table, observed) {
                (
                    Some(expected_table),
                    DdlChange::CreateTable { name, .. }
                    | DdlChange::DropTable { name }
                    | DdlChange::AlterTable { name, .. },
                ) => name == expected_table,
                _ => false,
            })
            .map(|(_, lsn)| *lsn)
    });
    row_positions
        .chain(ddl_positions)
        .max()
        .expect("every accepted source batch has a matching live hub history member")
}

/// The fixture schemas are newly authored tables, so their first durable
/// schema instance is generation one. Build the v6 sidecar through the
/// production canonical digest rather than discarding it in the expected
/// `WireChangeSet` conversion. Rows are authored by an authenticated edge, so
/// their immutable creator sidecars (including the signature) are part of the
/// expected bytes, not optional fixture decoration.
fn expected_wire_changeset_for_newly_authored_batches(
    source_db: &Database,
    changes: ChangeSet,
) -> WireChangeSet {
    let mut wire = WireChangeSet::from(changes);
    let mut next_ordinal = HashMap::<Lsn, u32>::new();
    wire.ddl_provenance = wire
        .ddl
        .iter()
        .zip(&wire.ddl_lsn)
        .map(|(ddl, source_ddl_lsn)| {
            let ordinal = next_ordinal.entry(*source_ddl_lsn).or_default();
            let provenance_ordinal = *ordinal;
            *ordinal += 1;
            let table = match ddl {
                WireDdlChange::CreateTable { name, .. } => Some(name.clone()),
                other => panic!(
                    "transport fixture expects a newly authored CREATE TABLE schema batch, got {other:?}"
                ),
            };
            let table_generation = Some(1);
            WireDdlProvenance {
                source_ddl_lsn: *source_ddl_lsn,
                ordinal: provenance_ordinal,
                digest: canonical_ddl_provenance_digest(
                    ddl,
                    *source_ddl_lsn,
                    provenance_ordinal,
                    table.as_deref(),
                    table_generation,
                )
                .expect("canonical fixture DDL provenance digest"),
                table,
                table_generation,
            }
        })
        .collect();
    for row in &mut wire.rows {
        let natural_key = NaturalKey::from(row.natural_key.clone());
        let sidecar = source_db
            .authoritative_purge_current_live_row_sidecar_for_test(&row.table, &natural_key)
            .unwrap_or_else(|| {
                panic!(
                    "authenticated fixture row {} {:?} must have a creator lineage sidecar",
                    row.table, natural_key
                )
            });
        row.lineage = Some(contextdb_server::protocol::WireRowLineage {
            author_node_id: sidecar.author_node_id,
            author_database_incarnation: sidecar.author_database_incarnation,
            author_local_mutation_position: sidecar.author_local_mutation_position,
            table_generation: sidecar.table_generation,
            lineage_root: sidecar.lineage_root,
            attestation: sidecar.lineage_attestation,
        });
    }
    wire
}

/// The expected served changeset for a page whose every row was pushed
/// fresh, with no established fleet-lineage ordering — the shape both
/// `ip_01` and `ip_03` push. Computed from first principles, independent of
/// the server's own arrival-stamping helpers (`changes_since_with_arrivals`
/// / `wire_changeset_with_arrivals`) so this assertion cannot pass merely
/// because the test calls the same machinery it is meant to check: a row
/// with no incoming arrival is stamped with the accepting server's own
/// commit position -- exactly its own resulting `.lsn`, never a value
/// sampled before that commit (see `database.rs`'s `SYNC_SOURCE_LSN_OWN_
/// COMMIT` sentinel; a sampled `current_lsn() == lsn - 1` value was the
/// pre-fix defect this row would otherwise still pin).
fn expected_wire_changeset_for_freshly_pushed_rows(
    source_db: &Database,
    changes: ChangeSet,
) -> WireChangeSet {
    let mut wire = expected_wire_changeset_for_newly_authored_batches(source_db, changes);
    for row in &mut wire.rows {
        row.arrival = Some(row.lsn);
    }
    wire
}

/// Keep a schema migration in its dependency-complete reply unit while the
/// ordinary response carries rows from later source positions. This mirrors
/// the public wire contract without flattening a dependency envelope into an
/// ordinary response in the fixture assertion.
fn split_expected_pull_units(changes: ChangeSet) -> (ChangeSet, Vec<ChangeSet>) {
    let mut migrations = BTreeMap::<Lsn, ChangeSet>::new();
    for (ddl, lsn) in changes.ddl.into_iter().zip(changes.ddl_lsn) {
        let migration = migrations.entry(lsn).or_default();
        migration.ddl.push(ddl);
        migration.ddl_lsn.push(lsn);
    }
    let mut ordinary = ChangeSet::default();
    for row in changes.rows {
        if let Some(migration) = migrations.get_mut(&row.lsn) {
            migration.rows.push(row);
        } else {
            ordinary.rows.push(row);
        }
    }
    for edge in changes.edges {
        if let Some(migration) = migrations.get_mut(&edge.lsn) {
            migration.edges.push(edge);
        } else {
            ordinary.edges.push(edge);
        }
    }
    for vector in changes.vectors {
        if let Some(migration) = migrations.get_mut(&vector.lsn) {
            migration.vectors.push(vector);
        } else {
            ordinary.vectors.push(vector);
        }
    }
    (ordinary, migrations.into_values().collect())
}

/// Status/pull contact bookkeeping is hub-local `SYNC OFF` state. Its rows
/// never leave the hub, but authenticated schema is deliberately delivered
/// regardless of direction so a fresh reader receives the complete schema
/// vector. Keep only the requested application's rows/vectors while retaining
/// every DDL entry (including the DDL-only contact-table migration).
fn expected_deliverable_table_changes(mut changes: ChangeSet, table: &str) -> ChangeSet {
    changes.rows.retain(|row| row.table == table);
    changes.vectors.retain(|vector| vector.index.table == table);
    changes
}

fn collect_rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|err| panic!("read {dir:?}: {err}")) {
        let entry = entry.unwrap_or_else(|err| panic!("read entry under {dir:?}: {err}"));
        let path = entry.path();
        if path.is_dir() {
            collect_rust_sources(&path, out);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// The installed smoke driver is a fixed release verifier, not a second sync
/// implementation. Remove only its explicitly sanctioned orchestration
/// imports/calls before applying the normal source-boundary scanner, so any
/// new engine/apply surface in that file still fails this test.
fn remove_exact_sanctioned_occurrences(
    source: String,
    sanctioned: &str,
    expected_count: usize,
    label: &str,
) -> String {
    let actual_count = source.matches(sanctioned).count();
    assert_eq!(
        actual_count, expected_count,
        "{label} sanctioned source form count changed; update this fixed verifier allowance deliberately"
    );
    source.replace(sanctioned, "")
}

fn smoke_driver_without_authorized_orchestration(src: &str) -> String {
    let mut audited = src.to_string();
    let smoke_transport_import = format!(
        "use contextdb_engine::transport::{CONCRETE_TRANSPORT_ADAPTER}::{{\n    ProductionSmokeCheckpoint, ProductionSmokeGateKind, arm_production_smoke_gate,\n}};\n"
    );
    for (authorized_import, expected_count) in [
        (
            "use contextdb_engine::database::open_with_startup_limits;\n",
            1,
        ),
        (
            "use contextdb_engine::plugin::{CorePlugin, DatabasePlugin};\n",
            1,
        ),
        (
            "use contextdb_engine::sync_types::{ChangeSet, DdlChange};\n",
            1,
        ),
        (smoke_transport_import.as_str(), 1),
        (
            "use contextdb_engine::{Database, SyncClient, SyncServer};\n",
            1,
        ),
    ] {
        audited = remove_exact_sanctioned_occurrences(
            audited,
            authorized_import,
            expected_count,
            "smoke driver import",
        );
    }
    // The plugin hook, durable database opening, and DDL-vector inspection
    // are the three fixed verifier operations the smoke command is allowed to
    // orchestrate. Do not add general engine names here.
    for (authorized_use, expected_count) in [
        ("fn on_sync_pull(&self, changes: &mut ChangeSet)", 1),
        // The three fixed smoke subcommands all open the caller-selected
        // database argument. Do not suppress a future open at any other
        // callsite.
        ("Database::open(&args.db)", 3),
        ("database.changes_since(", 1),
    ] {
        audited = remove_exact_sanctioned_occurrences(
            audited,
            authorized_use,
            expected_count,
            "smoke driver verifier expression",
        );
    }
    audited
}

/// The server crate root may name the engine only through these established
/// facade re-exports. Remove those exact forms before the ownership scan, so
/// a new database/apply implementation at the crate root remains visible.
fn lib_facade_without_authorized_engine_reexports(src: &str) -> String {
    let mut audited = src.to_string();
    let blob_resolver_reexport = format!(
        "#[cfg(feature = \"{CONCRETE_TRANSPORT_ADAPTER}\")]\npub mod blob_resolver {{\n    pub use contextdb_engine::blob_store::{{\n        BlobFetchPolicy, BlobStore, ClaimRefreshHook, ResolveError,\n    }};\n}}\n"
    );
    for reexport in [
        blob_resolver_reexport.as_str(),
        "pub use contextdb_engine::error;\n",
        "pub use contextdb_engine::identity;\n",
        "pub use contextdb_engine::protocol;\n",
        "pub use contextdb_engine::subjects;\n",
        "pub mod sync_client {\n    pub use contextdb_engine::sync_client::*;\n}\n",
        "pub mod sync_server {\n    pub use contextdb_engine::sync_server::*;\n}\n",
        "pub mod transfer_receipts {\n    pub use contextdb_engine::transfer_receipts::*;\n}\n",
        "pub mod transport {\n    pub use contextdb_engine::transport::*;\n}\n",
        "pub use contextdb_engine::FabricIdentity;\n",
    ] {
        audited = audited.replace(reexport, "");
    }
    audited
}

/// Transfer receipts owns only a local accumulator. Its engine counter types
/// are public facade values, not permission for database/apply logic here.
fn transfer_receipts_without_authorized_counter_reexport(src: &str) -> String {
    src.replace(
        "pub use contextdb_engine::transfer_receipts::{\n    TransferCounters, TransferDirection, TransferPlane, TransferReceipt,\n};\n",
        "",
    )
}

/// The policy journey is also an installed-release verifier: it opens durable
/// databases and inspects the public deletion snapshot to prove policy
/// behavior. Strip precisely that verifier surface before scanning; ordinary
/// source files still fail for any engine/apply import.
fn smoke_policy_journey_without_authorized_verifier_orchestration(src: &str) -> String {
    let mut audited = src.to_string();
    for (authorized_import, expected_count) in [
        (
            "use contextdb_engine::database::{DeleteObligationInspection, SnapshotInspector};\n",
            1,
        ),
        ("use contextdb_engine::sync_types::NaturalKey;\n", 1),
        (
            "use contextdb_engine::{Database, SyncClient, SyncServer};\n",
            1,
        ),
    ] {
        audited = remove_exact_sanctioned_occurrences(
            audited,
            authorized_import,
            expected_count,
            "smoke policy import",
        );
    }
    for (authorized_use, expected_count) in [
        // Each is an existing durable verifier handle. A new open expression
        // must stay visible to the ownership scanner.
        ("Arc<Database>", 2),
        ("Database::open(&db_path)", 2),
        ("Database::open(db_path)", 1),
        ("Database::open(&manual_db)", 2),
        ("Database::open(&auto_db)", 2),
    ] {
        audited = remove_exact_sanctioned_occurrences(
            audited,
            authorized_use,
            expected_count,
            "smoke policy verifier expression",
        );
    }
    audited
}

#[tokio::test]
async fn ip_00_fake_has_no_responder_until_server_run_loop_serves() {
    let broker = InProcessBroker::new();
    let tenant = "ip-00";
    let server_db = Arc::new(Database::open_memory());
    let server = authenticated_server(server_db.clone(), &broker, tenant);

    let edge_db = Arc::new(Database::open_memory());
    edge_db
        .execute(
            "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST",
            &HashMap::new(),
        )
        .expect("create notes on edge");
    let id = Uuid::new_v4();
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Uuid(id));
    row.insert(
        "body".to_string(),
        Value::Text("requires-run-loop".to_string()),
    );
    edge_db
        .execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &row)
        .expect("insert on edge");
    let edge_identity = Arc::new(FabricIdentity::generate());
    let before_run =
        authenticated_client_with_identity(edge_db.clone(), &broker, tenant, edge_identity.clone());
    let before_run_result = within(before_run.push()).await;
    assert!(
        before_run_result.is_err(),
        "constructing a server must not register fake responders; server.run_until must own serving"
    );
    let rendered_before_run_error = before_run_result
        .expect_err("checked err above")
        .to_string()
        .to_ascii_lowercase();
    assert!(
        rendered_before_run_error.contains("no responder"),
        "pre-run fake push must fail as a no-responder transport miss, got {rendered_before_run_error}"
    );
    assert!(
        broker.recorded_exchanges().is_empty(),
        "the fake must record only completed request/reply exchanges, not no-responder attempts"
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    let server_task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });

    let after_run =
        authenticated_client_with_identity(edge_db.clone(), &broker, tenant, edge_identity);
    let after_run_result = within(after_run.push()).await;
    assert!(
        after_run_result.is_ok(),
        "after server.run_until starts, the fake must route through the real server hub"
    );
    expect_text_value(&server_db, "notes", "body", id, "requires-run-loop");

    shutdown.store(true, Ordering::SeqCst);
    let _ = server_task.await;

    let completed_before_shutdown_probe = broker.recorded_exchanges().len();
    let after_id = Uuid::new_v4();
    let mut after_row = HashMap::new();
    after_row.insert("id".to_string(), Value::Uuid(after_id));
    after_row.insert(
        "body".to_string(),
        Value::Text("after-server-loop".to_string()),
    );
    edge_db
        .execute(
            "INSERT INTO notes (id, body) VALUES ($id, $body)",
            &after_row,
        )
        .expect("insert after server shutdown");
    let after_shutdown_result = within(after_run.push()).await;
    assert!(
        after_shutdown_result.is_err(),
        "after server.run_until exits, the fake must remove responders"
    );
    let rendered_error = after_shutdown_result
        .expect_err("checked err above")
        .to_string()
        .to_ascii_lowercase();
    assert!(
        rendered_error.contains("no responder"),
        "post-shutdown fake push must fail as a no-responder transport miss, got {rendered_error}"
    );
    assert_eq!(
        broker.recorded_exchanges().len(),
        completed_before_shutdown_probe,
        "no-responder attempts after shutdown must not be recorded as completed exchanges"
    );
}

#[tokio::test]
async fn ip_01_push_pull_converges_over_fake() {
    let broker = InProcessBroker::new();
    let tenant = "ip-01";
    let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);

    let (server_db, shutdown, server_task) = start_fake_hub(&broker, tenant);

    let edge_a_db = Arc::new(Database::open_memory());
    edge_a_db
        .execute(
            "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST",
            &HashMap::new(),
        )
        .expect("create notes on edge A");
    let id = Uuid::new_v4();
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Uuid(id));
    row.insert("body".to_string(), Value::Text("hello-fabric".to_string()));
    edge_a_db
        .execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &row)
        .expect("insert on edge A");
    let expected_push_batches =
        acceptance_stamped_push_batches_for_test(edge_a_db.changes_since(Lsn(0)));
    let expected_batch_results =
        expected_apply_results_for_batches(&expected_push_batches, &policies);
    let edge_a = authenticated_client(edge_a_db.clone(), &broker, tenant);
    let edge_a_push = within(edge_a.push()).await;
    assert!(
        edge_a_push.is_ok(),
        "push over the in-process fake must succeed with no broker"
    );
    let edge_a_push = edge_a_push.expect("checked ok above");
    expect_text_value(&server_db, "notes", "body", id, "hello-fabric");
    let status_exchanges = exchanges_on(&broker, &status_subject(tenant));
    assert_eq!(
        status_exchanges.len(),
        1,
        "push must use one broker-free status exchange before sending data"
    );
    let push_status =
        assert_authenticated_status_exchange(&status_exchanges[0], Lsn(0), Lsn(0), "push status");
    assert!(
        push_status
            .server_current_lsn
            .is_some_and(|lsn| lsn <= edge_a_push.new_lsn),
        "push status must not claim a hub position beyond the following accepted push: {push_status:?}"
    );
    let live_hub_history = server_db.changes_since(Lsn(0));
    let push_exchanges = exchanges_on(&broker, &push_subject(tenant));
    assert_eq!(
        push_exchanges.len(),
        expected_push_batches.len(),
        "each atomic source-LSN group must cross the fake in its own complete PushRequest envelope"
    );
    let mut applied = 0usize;
    for (idx, (exchange, expected_batch)) in push_exchanges
        .iter()
        .zip(&expected_push_batches)
        .enumerate()
    {
        let mut expected_result = expected_batch_results[idx].clone();
        expected_result.new_lsn = live_hub_position_for_batch(&live_hub_history, expected_batch);
        let expected_dependency_complete = idx == 0;
        assert_eq!(
            !expected_batch.ddl.is_empty(),
            expected_dependency_complete,
            "acceptance-stamped push group {idx} must be the schema migration first, followed only by ordinary row groups"
        );
        assert_push_exchange(
            exchange,
            expected_dependency_complete,
            expected_wire_changeset_for_newly_authored_batches(&edge_a_db, expected_batch.clone()),
            PushResponse {
                result: Some(expected_result),
                error: None,
                application_error: None,
            },
            &format!("acceptance-stamped push group {idx}"),
        );
        applied += expected_batch_results[idx].applied_rows;
    }
    assert_eq!(
        applied, edge_a_push.applied_rows,
        "the caller-visible result accumulates every acceptance-stamped response"
    );

    let edge_b_db = Arc::new(Database::open_memory());
    let edge_b = authenticated_client(edge_b_db.clone(), &broker, tenant);
    assert!(
        within(edge_b.pull_default()).await.is_ok(),
        "pull over the in-process fake must succeed with no broker"
    );

    expect_text_value(&edge_b_db, "notes", "body", id, "hello-fabric");
    let status_exchanges = exchanges_on(&broker, &status_subject(tenant));
    assert_eq!(
        status_exchanges.len(),
        2,
        "push and pull must each make the normal status probe through the fake"
    );
    let pull_status =
        assert_authenticated_status_exchange(&status_exchanges[1], Lsn(0), Lsn(0), "pull status");
    assert!(
        pull_status
            .server_current_lsn
            .is_some_and(|lsn| lsn <= server_db.current_lsn()),
        "the pull status must be a real hub position, before or at the following pull contact: {pull_status:?}"
    );
    let pull_exchanges = exchanges_on(&broker, &pull_subject(tenant));
    assert_eq!(
        pull_exchanges.len(),
        1,
        "pull must send one complete PullRequest envelope through the fake"
    );
    let expected_pull_changes =
        expected_deliverable_table_changes(server_db.changes_since(Lsn(0)), "notes");
    let (expected_ordinary_pull, expected_dependency_units) =
        split_expected_pull_units(expected_pull_changes);
    assert_eq!(
        expected_ordinary_pull.rows.len(),
        1,
        "the row authored after the schema migration must stay in the ordinary pull page"
    );
    assert!(
        expected_ordinary_pull.ddl.is_empty(),
        "the ordinary pull page must not flatten the schema migration"
    );
    assert_eq!(
        expected_dependency_units.len(),
        2,
        "the authenticated schema vector must contain the hub's DDL-only contact-table migration and the application migration"
    );
    assert!(
        expected_dependency_units[0].rows.is_empty(),
        "the SYNC OFF contact-table migration must never carry its hub-local row"
    );
    assert_eq!(
        expected_dependency_units[0].ddl.len(),
        1,
        "the contact-table migration must contain one CREATE TABLE statement"
    );
    assert!(matches!(
        expected_dependency_units[0].ddl.as_slice(),
        [DdlChange::CreateTable { name, .. }] if name == "work_node_contacts"
    ));
    assert!(
        expected_dependency_units[1].rows.is_empty(),
        "the application migration is DDL-only because its row was authored later"
    );
    assert_eq!(
        expected_dependency_units[1].ddl.len(),
        1,
        "the application migration must contain one CREATE TABLE statement"
    );
    assert!(matches!(
        expected_dependency_units[1].ddl.as_slice(),
        [DdlChange::CreateTable { name, .. }] if name == "notes"
    ));
    let expected_pull_response = DependencyCompletePullResponse {
        ordinary: PullResponse {
            changeset: expected_wire_changeset_for_freshly_pushed_rows(
                &server_db,
                expected_ordinary_pull,
            ),
            has_more: false,
            cursor: Some(server_db.current_lsn()),
            source: server_db
                .sync_incarnation(&contextdb_core::TenantId::from(tenant))
                .ok(),
        },
        units: expected_dependency_units
            .into_iter()
            .map(|unit| expected_wire_changeset_for_freshly_pushed_rows(&server_db, unit))
            .collect(),
    };
    assert_pull_exchange(
        &pull_exchanges[0],
        PullRequest {
            since_lsn: Lsn(0),
            max_entries: Some(500),
        },
        expected_pull_response,
        "single-page pull",
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = server_task.await;
}

/// A client transport that forwards the push batch to the real hub (so the
/// server applies + commits it) and then *drops the acknowledgement*, exactly
/// as a hub that is killed a few milliseconds into applying the batch looks to
/// the edge: the write has landed durably, but the edge never sees the ack. All
/// other subjects (status, pull, connect) forward untouched, so the edge can
/// still reach the hub to reconcile.
struct DropPushAckAfterApply {
    inner: Arc<dyn ClientTransport>,
    push_subject: String,
}

impl ClientTransport for DropPushAckAfterApply {
    fn peer_node_id(&self) -> Option<String> {
        self.inner.peer_node_id()
    }

    fn local_node_id(&self) -> Option<String> {
        self.inner.local_node_id()
    }

    fn has_stable_edge_identity(&self) -> bool {
        self.inner.has_stable_edge_identity()
    }

    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.ensure_connected()
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        self.inner.is_connected()
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        self.inner.request(subject, request_bytes, timeout)
    }

    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        if subject == self.push_subject {
            let inner = self.inner.clone();
            let subject = subject.to_string();
            Box::pin(async move {
                // Let the hub apply + commit the batch first ...
                let _ = inner
                    .request_single_reply(&subject, request_bytes, timeout)
                    .await;
                // ... then lose the acknowledgement on the way back.
                Err(TransportError::Unreachable(
                    "dial timed out (fault-injected after the hub committed the batch)".to_string(),
                ))
            })
        } else {
            self.inner
                .request_single_reply(subject, request_bytes, timeout)
        }
    }

    fn ensure_single_reply_retry_safe(&self, request_bytes: &[u8]) -> TransportResult<()> {
        self.inner.ensure_single_reply_retry_safe(request_bytes)
    }

    fn shutdown<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.shutdown()
    }
}

/// The false-negative from usability job USR-19: a push whose batch reached the
/// hub and committed durably, but whose acknowledgement was lost in transit,
/// must NOT be reported as a plain failure. With the hub still reachable, the
/// edge reconciles against the server's applied-push watermark and reports the
/// push as the success it actually was (watermark advanced), never an
/// undifferentiated `SyncError` that drives a needless re-push-as-if-lost.
#[tokio::test]
async fn ip_interrupted_push_after_commit_must_not_report_false_failure() {
    let broker = InProcessBroker::new();
    let tenant = "ip-interrupted-push";
    let (server_db, shutdown, server_task) = start_fake_hub(&broker, tenant);

    let edge_db = Arc::new(Database::open_memory());
    edge_db
        .execute(
            "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST",
            &HashMap::new(),
        )
        .expect("create notes on edge");
    let id = Uuid::new_v4();
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Uuid(id));
    row.insert(
        "body".to_string(),
        Value::Text("landed-despite-lost-ack".to_string()),
    );
    edge_db
        .execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &row)
        .expect("insert on edge");
    let transmitted_source_ceiling = edge_db
        .changes_since(Lsn(0))
        .max_lsn()
        .expect("the interrupted push has source work to transmit");

    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = edge_identity.node_id();
    let faulty = Arc::new(DropPushAckAfterApply {
        inner: broker.client_as(&edge_node_id),
        push_subject: push_subject(tenant),
    });
    let edge = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge_db.clone(),
        faulty,
        contextdb_core::TenantId::from(tenant),
        edge_identity,
    );

    let push = within(edge.push()).await;

    // The batch reached the hub and committed durably ...
    expect_text_value(&server_db, "notes", "body", id, "landed-despite-lost-ack");

    // ... so the edge, which can still reach the hub, must reconcile and report
    // the push as the success it actually was — never a plain `SyncError`, and
    // with the watermark advanced so the next push is a no-op, not a re-push.
    assert!(
        push.is_ok(),
        "an interrupted-after-commit push whose data landed must reconcile to success, got {push:?}"
    );
    assert!(
        !matches!(push, Err(Error::SyncError(_))),
        "the false failure this test guards against is a bare SyncError, got {push:?}"
    );
    assert_eq!(
        edge.push_watermark(),
        transmitted_source_ceiling,
        "a confirmed push must advance the edge watermark to exactly the source LSN it transmitted"
    );
    assert_eq!(
        server_db
            .persisted_sync_applied_push_watermark_for_node(
                &contextdb_core::TenantId::from(tenant),
                &edge_node_id,
            )
            .expect("read authenticated edge receipt watermark"),
        Some(transmitted_source_ceiling),
        "the hub receipt must confirm that same source frontier"
    );
    assert!(
        edge.push_watermark() <= server_db.current_lsn(),
        "the source frontier cannot exceed the hub's live global position"
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = server_task.await;
}

#[tokio::test]
async fn ip_02_conflicting_push_reports_real_apply_result_over_fake() {
    let broker = InProcessBroker::new();
    let tenant = "ip-02";
    let server_db = Arc::new(Database::open_memory());
    let server = authenticated_server(server_db.clone(), &broker, tenant);
    let shutdown = Arc::new(AtomicBool::new(false));
    let server_task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });

    let key = Uuid::new_v4();
    let ddl = "CREATE TABLE claims (id UUID PRIMARY KEY, owner TEXT)";
    server_db.execute(ddl, &HashMap::new()).expect("ddl on hub");
    let mut first_row = HashMap::new();
    first_row.insert("id".to_string(), Value::Uuid(key));
    first_row.insert("owner".to_string(), Value::Text("worker-a".to_string()));
    server_db
        .execute(
            "INSERT INTO claims (id, owner) VALUES ($id, $owner)",
            &first_row,
        )
        .expect("insert hub winner");
    let first_lsn = server_db.current_lsn();

    let second_db = Arc::new(Database::open_memory());
    second_db
        .execute(ddl, &HashMap::new())
        .expect("ddl on second writer");
    second_db
        .persist_sync_push_watermark(
            &contextdb_core::TenantId::from(tenant),
            second_db.current_lsn(),
        )
        .expect("mark local DDL as already synced");
    let mut second_row = HashMap::new();
    second_row.insert("id".to_string(), Value::Uuid(key));
    second_row.insert("owner".to_string(), Value::Text("worker-b".to_string()));
    second_db
        .execute(
            "INSERT INTO claims (id, owner) VALUES ($id, $owner)",
            &second_row,
        )
        .expect("insert second");
    let losing_lsn = second_db.current_lsn();
    let expected_push_batches =
        acceptance_stamped_push_batches_for_test(second_db.changes_since(Lsn(0)));
    let second = authenticated_client(second_db.clone(), &broker, tenant);
    let second_push = within(second.push()).await;
    assert!(
        second_push.is_ok(),
        "second push over the in-process fake must succeed with no broker"
    );
    let second_result = second_push.expect("checked ok above");

    let status_exchanges = exchanges_on(&broker, &status_subject(tenant));
    assert_eq!(
        status_exchanges.len(),
        1,
        "the losing push must still probe hub status through the fake"
    );
    let conflict_status = assert_authenticated_status_exchange(
        &status_exchanges[0],
        Lsn(0),
        first_lsn,
        "conflict push status",
    );
    assert!(
        conflict_status
            .server_current_lsn
            .is_some_and(|lsn| lsn <= server_db.current_lsn()),
        "the status reply must be a real hub position, not a future LSN: {conflict_status:?}"
    );
    let push_exchanges = exchanges_on(&broker, &push_subject(tenant));
    assert_eq!(
        push_exchanges.len(),
        expected_push_batches.len(),
        "every source-LSN group, including bootstrap schema, must travel as its own request"
    );
    let mut response_skipped = 0usize;
    for (idx, (exchange, expected_batch)) in push_exchanges
        .iter()
        .zip(&expected_push_batches)
        .enumerate()
    {
        let (request, _dependency_complete) =
            push_request_payload(&exchange.0, &format!("conflicting group {idx} request"));
        assert_eq!(
            request.changeset,
            expected_wire_changeset_for_newly_authored_batches(&second_db, expected_batch.clone())
        );
        let response: PushResponse = payload(
            &exchange.1,
            MessageType::PushResponse,
            &format!("conflicting group {idx} response"),
        );
        let result = response.result.expect("group response result");
        response_skipped += result.skipped_rows;
    }
    assert_eq!(
        response_skipped, second_result.skipped_rows,
        "caller result accumulates the individual group responses"
    );
    assert_eq!(
        second_result.applied_rows, 0,
        "the colliding push must not replace the existing row under the declaration's KEEP FIRST default"
    );
    assert_eq!(
        second_result.skipped_rows, 1,
        "the KEEP FIRST collision must report one skipped row"
    );
    assert_eq!(
        second_result.conflicts.len(),
        1,
        "the KEEP FIRST refusal must retain its one typed conflict receipt"
    );
    let conflict = &second_result.conflicts[0];
    assert_eq!(
        conflict.resolution,
        ConflictPolicy::InsertIfNotExists,
        "the typed refusal must name the declaration's KEEP FIRST resolution"
    );
    assert_eq!(
        conflict.reason.as_deref(),
        Some("dependency_complete_refused"),
        "the typed refusal must say the complete schema/data group was rejected under KEEP FIRST"
    );
    assert_eq!(
        conflict.natural_key.column, "id",
        "the typed refusal must identify the rejected natural-key column"
    );
    assert_eq!(
        conflict.natural_key.value,
        Value::Uuid(key),
        "the typed refusal must identify the rejected natural key"
    );
    assert_eq!(
        server_db
            .persisted_sync_applied_push_watermark(&contextdb_core::TenantId::from(tenant))
            .expect("read applied-push watermark"),
        Some(losing_lsn),
        "the server must record that it processed the losing push LSN"
    );

    expect_text_value(&server_db, "claims", "owner", key, "worker-a");

    shutdown.store(true, Ordering::SeqCst);
    let _ = server_task.await;
}

#[tokio::test]
async fn ip_03_large_changeset_batch_split_converges_over_fake() {
    let broker = InProcessBroker::new();
    let tenant = "ip-03";
    let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);

    let (server_db, shutdown, server_task) = start_fake_hub(&broker, tenant);

    let edge_db = Arc::new(Database::open_memory());
    edge_db
        .execute(
            "CREATE TABLE blobs (id UUID PRIMARY KEY, payload TEXT) SYNC CONFLICT KEEP LATEST",
            &HashMap::new(),
        )
        .expect("create blobs");
    let oversized = "x".repeat(900 * 1024);
    let regular = "x".repeat(100 * 1024);
    let mut expected_ids = BTreeSet::new();
    for idx in 0..10 {
        let id = Uuid::new_v4();
        assert!(expected_ids.insert(id), "generated duplicate UUID");
        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::Uuid(id));
        let payload = if idx == 0 {
            oversized.clone()
        } else {
            regular.clone()
        };
        row.insert("payload".to_string(), Value::Text(payload));
        edge_db
            .execute(
                "INSERT INTO blobs (id, payload) VALUES ($id, $payload)",
                &row,
            )
            .expect("insert blob");
    }
    let size_split_batches = split_changeset_for_test(edge_db.changes_since(Lsn(0)));
    assert!(
        size_split_batches.len() > 1,
        "fixture must exercise the independent size/barrier splitter"
    );
    let expected_batches = acceptance_stamped_push_batches_for_test(edge_db.changes_since(Lsn(0)));
    assert!(
        expected_batches.len() > 1,
        "fixture must force several sync batches, got {}",
        expected_batches.len()
    );
    let oversized_push_batches = expected_batches
        .iter()
        .filter(|batch| {
            let request = PushRequest {
                changeset: (*batch).clone().into(),
                incarnation: contextdb_core::Incarnation::default(),
            };
            encode(MessageType::PushRequest, &request)
                .expect("encode expected push batch")
                .len()
                > CHUNKING_THRESHOLD
        })
        .count();
    assert!(
        oversized_push_batches >= 1,
        "fixture must include a complete PushRequest larger than the transport chunk threshold"
    );
    // The mirror establishes the deterministic row/conflict effects only.
    // `new_lsn` belongs to the live hub, whose authenticated-contact writes
    // interleave with each accepted batch and therefore cannot be borrowed
    // from a separate engine instance.
    let expected_batch_effects = expected_apply_results_for_batches(&expected_batches, &policies);
    let edge = authenticated_client(edge_db.clone(), &broker, tenant);
    let push_result = within(edge.push()).await;
    assert!(
        push_result.is_ok(),
        "multi-batch push over the fake must succeed"
    );
    let push_result = push_result.expect("checked ok above");
    assert_eq!(
        push_result.applied_rows, 10,
        "the hub must apply every row across all split batches"
    );
    let status_exchanges = exchanges_on(&broker, &status_subject(tenant));
    assert_eq!(
        status_exchanges.len(),
        1,
        "the multi-batch push must establish one authenticated live-hub position before data"
    );
    let mut preceding_live_hub_position = assert_authenticated_status_exchange(
        &status_exchanges[0],
        Lsn(0),
        Lsn(0),
        "multi-batch push status",
    )
    .server_current_lsn
    .expect("authenticated status carries the live hub position");
    let live_hub_history = server_db.changes_since(Lsn(0));
    let push_exchanges = exchanges_on(&broker, &push_subject(tenant));
    assert_eq!(
        push_exchanges.len(),
        expected_batches.len(),
        "each split batch must be its own complete push request over the fake"
    );
    let mut response_applied_rows = 0usize;
    let mut response_skipped_rows = 0usize;
    let mut response_conflicts = 0usize;
    for (idx, (exchange, expected_batch)) in push_exchanges
        .iter()
        .zip(expected_batches.iter())
        .enumerate()
    {
        let (request, dependency_complete) =
            push_request_payload(&exchange.0, &format!("batch {idx} push request"));
        let expected_dependency_complete = idx == 0;
        assert_eq!(
            !expected_batch.ddl.is_empty(),
            expected_dependency_complete,
            "batch {idx} must be the CREATE TABLE migration first, followed only by row-only batches"
        );
        assert_eq!(
            dependency_complete, expected_dependency_complete,
            "batch {idx} request envelope must preserve the exact dependency-complete boundary"
        );
        assert_eq!(
            request.changeset,
            expected_wire_changeset_for_newly_authored_batches(&edge_db, expected_batch.clone()),
            "batch {idx} request payload must equal the splitter output"
        );
        let response: PushResponse = payload(
            &exchange.1,
            MessageType::PushResponse,
            &format!("batch {idx} push response"),
        );
        assert_eq!(response.error, None, "batch {idx} must not error");
        let result = response
            .result
            .unwrap_or_else(|| panic!("batch {idx} response must carry an apply result"));
        let expected = &expected_batch_effects[idx];
        assert_eq!(
            result.applied_rows, expected.applied_rows,
            "batch {idx} response must preserve the exact applied-row result"
        );
        assert_eq!(
            result.skipped_rows, expected.skipped_rows,
            "batch {idx} response must preserve the exact skipped-row result"
        );
        assert_eq!(
            result.conflicts, expected.conflicts,
            "batch {idx} response must preserve the exact conflict result"
        );
        assert_eq!(
            result.new_lsn,
            live_hub_position_for_batch(&live_hub_history, expected_batch),
            "batch {idx} response must carry the exact accepting hub position for its schema/data"
        );
        assert!(
            result.new_lsn > preceding_live_hub_position
                && result.new_lsn <= server_db.current_lsn(),
            "batch {idx} response must name its own live hub commit position, after the preceding live position and never beyond the hub: {result:?}"
        );
        preceding_live_hub_position = result.new_lsn;
        response_applied_rows += result.applied_rows;
        response_skipped_rows += result.skipped_rows;
        response_conflicts += result.conflicts.len();
        if idx + 1 == expected_batches.len() {
            assert_eq!(
                result.new_lsn, push_result.new_lsn,
                "last batch response must match the caller-visible final LSN"
            );
        }
    }
    assert_eq!(
        response_applied_rows, push_result.applied_rows,
        "per-batch response row counts must sum to the caller-visible result"
    );
    assert_eq!(
        response_skipped_rows, push_result.skipped_rows,
        "per-batch skipped rows must sum to the caller-visible result"
    );
    assert_eq!(
        response_conflicts,
        push_result.conflicts.len(),
        "per-batch conflicts must sum to the caller-visible result"
    );
    assert_eq!(
        uuid_set(&server_db, "blobs"),
        expected_ids,
        "the hub must hold the exact pushed row identities"
    );

    let reader_db = Arc::new(Database::open_memory());
    let reader = authenticated_client(reader_db.clone(), &broker, tenant);
    assert!(
        within(reader.pull_default()).await.is_ok(),
        "pull of a multi-batch history over the fake must succeed"
    );

    assert_eq!(
        uuid_set(&reader_db, "blobs"),
        expected_ids,
        "every exact row of a batch-split push must converge over the fake"
    );
    let pull_exchanges = exchanges_on(&broker, &pull_subject(tenant));
    assert_eq!(
        pull_exchanges.len(),
        1,
        "the reader pull must travel as a complete pull request over the fake"
    );
    let expected_pull_changes =
        expected_deliverable_table_changes(server_db.changes_since(Lsn(0)), "blobs");
    let (expected_ordinary_pull, expected_dependency_units) =
        split_expected_pull_units(expected_pull_changes);
    assert_eq!(
        expected_ordinary_pull.rows.len(),
        expected_ids.len(),
        "all later data groups must remain in the ordinary pull page"
    );
    assert!(
        expected_ordinary_pull.ddl.is_empty(),
        "the ordinary pull page must not flatten the bootstrap migration"
    );
    assert_eq!(
        expected_dependency_units.len(),
        2,
        "the authenticated schema vector must contain the hub's DDL-only contact-table migration and the blob bootstrap migration"
    );
    assert!(
        expected_dependency_units[0].rows.is_empty(),
        "the SYNC OFF contact-table migration must never carry its hub-local row"
    );
    assert_eq!(
        expected_dependency_units[0].ddl.len(),
        1,
        "the contact-table migration must contain one CREATE TABLE statement"
    );
    assert!(matches!(
        expected_dependency_units[0].ddl.as_slice(),
        [DdlChange::CreateTable { name, .. }] if name == "work_node_contacts"
    ));
    assert!(
        expected_dependency_units[1].rows.is_empty(),
        "the blob bootstrap migration is DDL-only because every blob row was authored later"
    );
    assert_eq!(
        expected_dependency_units[1].ddl.len(),
        1,
        "the blob bootstrap migration must contain one CREATE TABLE statement"
    );
    assert!(matches!(
        expected_dependency_units[1].ddl.as_slice(),
        [DdlChange::CreateTable { name, .. }] if name == "blobs"
    ));
    let expected_pull_response = DependencyCompletePullResponse {
        ordinary: PullResponse {
            changeset: expected_wire_changeset_for_freshly_pushed_rows(
                &server_db,
                expected_ordinary_pull,
            ),
            has_more: false,
            cursor: Some(server_db.current_lsn()),
            source: server_db
                .sync_incarnation(&contextdb_core::TenantId::from(tenant))
                .ok(),
        },
        units: expected_dependency_units
            .into_iter()
            .map(|unit| expected_wire_changeset_for_freshly_pushed_rows(&server_db, unit))
            .collect(),
    };
    assert_pull_exchange(
        &pull_exchanges[0],
        PullRequest {
            since_lsn: Lsn(0),
            max_entries: Some(500),
        },
        expected_pull_response,
        "multi-batch history pull",
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = server_task.await;
}

#[test]
fn sync_logic_stays_behind_the_transport_and_protocol_seams() {
    let root = env!("CARGO_MANIFEST_DIR");
    let mut sources = Vec::new();
    collect_rust_sources(&Path::new(root).join("src"), &mut sources);

    let client_src = std::fs::read_to_string(format!("{root}/src/sync_client.rs"))
        .expect("read sync client source");
    let server_src = std::fs::read_to_string(format!("{root}/src/sync_server.rs"))
        .expect("read sync server source");
    assert!(
        client_src.contains("ClientTransport") && client_src.contains(".request("),
        "sync client must send complete request bytes through ClientTransport::request"
    );
    assert!(
        server_src.contains("ServerTransport") && server_src.contains(".serve("),
        "sync server must register complete byte handlers through ServerTransport::serve"
    );

    for (rel, needle) in [
        ("src/sync_client.rs", "InProcess"),
        ("src/sync_server.rs", "InProcess"),
        ("src/sync_client.rs", "transport::in_process"),
        ("src/sync_server.rs", "transport::in_process"),
        ("src/sync_client.rs", "downcast"),
        ("src/sync_server.rs", "downcast"),
        ("src/sync_client.rs", "std::any::Any"),
        ("src/sync_server.rs", "std::any::Any"),
        ("src/sync_server.rs", "subscribe"),
    ] {
        let path = format!("{root}/{rel}");
        let src = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        assert!(
            !src.contains(needle),
            "{rel} must not special-case the in-process fake; sync logic must use only the transport traits"
        );
    }

    let chunking_allowed = ["src/chunking.rs", "src/protocol.rs"];
    for path in &sources {
        let rel = path
            .strip_prefix(root)
            .unwrap_or_else(|err| panic!("strip source prefix for {path:?}: {err}"))
            .to_string_lossy()
            .replace('\\', "/");
        if chunking_allowed.contains(&rel.as_str()) {
            continue;
        }
        let src = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        for needle in [
            "crate::chunking",
            "chunking::",
            "use crate::chunking",
            "use crate::{chunking",
            "MessageType::Chunk",
            "MessageType::ChunkAck",
            "ChunkMessage",
            "ChunkAck",
            "CHUNK_COLLECT_TIMEOUT",
            "MAX_CHUNK",
            "chunk_buffer",
            "needs_chunking",
        ] {
            assert!(
                !src.contains(needle),
                "{rel} must not own transport chunking after the cut; chunk framing belongs below the transport seam"
            );
        }
    }

    for path in &sources {
        let rel = path
            .strip_prefix(root)
            .unwrap_or_else(|err| panic!("strip source prefix for {path:?}: {err}"))
            .to_string_lossy()
            .replace('\\', "/");
        let src = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        let engine_scan_src = match rel.as_str() {
            "src/lib.rs" => lib_facade_without_authorized_engine_reexports(&src),
            "src/smoke_driver.rs" => smoke_driver_without_authorized_orchestration(&src),
            "src/smoke_policy_journey.rs" => {
                smoke_policy_journey_without_authorized_verifier_orchestration(&src)
            }
            "src/transfer_receipts.rs" => {
                transfer_receipts_without_authorized_counter_reexport(&src)
            }
            _ => src.clone(),
        };
        // This is an enum checkpoint name emitted by the fixed smoke driver,
        // not a parsed wire request. Keep the protocol scanner active for
        // every other token in the file.
        let protocol_scan_src = if rel == "src/smoke_driver.rs" {
            src.replace("ProductionSmokeCheckpoint::PushRequestPath", "")
        } else {
            src.clone()
        };

        let sync_engine_allowed = [
            "src/sync_client.rs",
            "src/sync_server.rs",
            "src/protocol.rs",
            "src/main.rs",
            "src/sync_plugin.rs",
            // The work ledger's distributed half is a sync CONSUMER by
            // design (claim-by-push over SyncClient + the engine ledger
            // API), so this exemption lets it name engine/sync types. Its
            // class- and transport-blindness is enforced by its own source
            // guard (wl_g02 in the engine integration suite); the protocol
            // needles below still apply to it.
            "src/work_ledger.rs",
            // This fixed installed-release verifier is invoked only by the
            // smoke driver. It deliberately inspects public engine state to
            // prove PURGE's permanent-erasure journey; it is not a second
            // sync implementation surface.
            "src/smoke_purge_journey.rs",
            // This fixed installed-release verifier drives the public SQL,
            // vector-query, and SyncClient/SyncServer APIs to prove that a
            // post-sync vector enrichment retains its owner. It contains no
            // wire parsing or apply/reconciliation implementation.
            "src/smoke_vector_journey.rs",
        ];
        if !sync_engine_allowed.contains(&rel.as_str()) {
            for needle in [
                "contextdb_engine::",
                "Arc<Database>",
                "Database::",
                "apply_changes",
                "changes_since",
                "ChangeSet",
                "ApplyResult",
                "ConflictPolicies",
                "ConflictPolicy",
            ] {
                assert!(
                    !engine_scan_src.contains(needle),
                    "{rel} must not hide sync-domain engine/apply logic outside the sync/protocol surface"
                );
            }
        }

        let concrete_adapter_path = format!("src/transport/{CONCRETE_TRANSPORT_ADAPTER}.rs");
        let sync_protocol_allowed = [
            "src/sync_client.rs",
            "src/sync_server.rs",
            "src/protocol.rs",
            // The concrete byte-stream adapter is below the transport seam and owns
            // byte framing/parsing for its streams. No other transport file
            // gets protocol parsing authority.
            concrete_adapter_path.as_str(),
            // The staged-request helper is the concrete adapter's bounded
            // transport framing component. It parses only those transport
            // frames, not sync-domain protocol in general.
            "src/transport/large_request_staging.rs",
            // The installed-release PURGE verifier sends one deliberately
            // malformed/raw request to prove the authoritative-hub boundary.
            // Keep this exception limited to that journey; no general smoke
            // source may parse or construct the sync wire protocol.
            "src/smoke_purge_journey.rs",
        ];
        if !sync_protocol_allowed.contains(&rel.as_str()) {
            for needle in [
                "PushRequest",
                "PushResponse",
                "PullRequest",
                "PullResponse",
                "SyncStatusRequest",
                "SyncStatusResponse",
                "MessageType::PushRequest",
                "MessageType::PushResponse",
                "MessageType::PullRequest",
                "MessageType::PullResponse",
                "MessageType::StatusRequest",
                "MessageType::StatusResponse",
                "Envelope",
                "decode(",
                "encode(",
                "rmp_serde",
            ] {
                assert!(
                    !protocol_scan_src.contains(needle),
                    "{rel} must not hide sync protocol parsing outside the sync/protocol surface"
                );
            }
        }
    }

    let mut transport_sources = Vec::new();
    collect_rust_sources(
        &Path::new(root).join("src/transport"),
        &mut transport_sources,
    );
    for path in &transport_sources {
        let rel = path
            .strip_prefix(root)
            .unwrap_or_else(|err| panic!("strip transport source prefix for {path:?}: {err}"))
            .to_string_lossy()
            .replace('\\', "/");
        let src = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        // This production-smoke checkpoint names the opaque request route
        // reached by the fixed verifier; it does not decode or construct a
        // sync PushRequest. Keep every other token in the concrete adapter under
        // the transport-only scanner.
        let concrete_adapter_path = format!("src/transport/{CONCRETE_TRANSPORT_ADAPTER}.rs");
        let transport_scan_src = if rel == concrete_adapter_path {
            src.replace("PushRequestPath", "")
        } else {
            src.clone()
        };
        for needle in [
            "contextdb_engine",
            "Database",
            "apply_changes",
            "changes_since",
            "ChangeSet",
            "ApplyResult",
            "ConflictPolicies",
            "ConflictPolicy",
            "PushRequest",
            "PushResponse",
            "PullRequest",
            "PullResponse",
            "SyncStatusRequest",
            "SyncStatusResponse",
            "MessageType::PushRequest",
            "MessageType::PushResponse",
            "MessageType::PullRequest",
            "MessageType::PullResponse",
            "MessageType::StatusRequest",
            "MessageType::StatusResponse",
        ] {
            assert!(
                !transport_scan_src.contains(needle),
                "{rel} must only move request/reply bytes; `{needle}` belongs in sync logic"
            );
        }
    }

    for path in &transport_sources {
        let rel = path
            .strip_prefix(root)
            .unwrap_or_else(|err| panic!("strip transport source prefix for {path:?}: {err}"))
            .to_string_lossy()
            .replace('\\', "/");
        if rel != "src/transport/in_process.rs" {
            continue;
        }
        let src = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        for needle in ["Envelope", "MessageType", "decode", "encode", "rmp_serde"] {
            assert!(
                !src.contains(needle),
                "{rel} must not parse protocol envelopes; the fake moves opaque bytes"
            );
        }
    }
}
