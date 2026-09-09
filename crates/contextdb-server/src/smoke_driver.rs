//! Fixed-scenario installed-release verifier for the production ticketed-Iroh
//! path. This binary exists only under `production-smoke-driver`; it is not a
//! product command and exposes no arbitrary SQL, plugin, transport, or fault
//! controls.

use clap::{Parser, Subcommand, ValueEnum};
use contextdb_core::{Error, Incarnation, TenantId, Value};
use contextdb_engine::database::open_with_startup_limits;
use contextdb_engine::plugin::{CorePlugin, DatabasePlugin};
use contextdb_engine::sync_client::ApplicationTablePolicyExpectation;
use contextdb_engine::sync_types::NaturalKey;
use contextdb_engine::sync_types::{ChangeSet, DdlChange};
use contextdb_engine::transport::iroh::{
    ProductionSmokeCheckpoint, ProductionSmokeGateKind, arm_production_smoke_gate,
};
use contextdb_engine::{Database, DeliveryManifest, DeliveryOutcomeKind, SyncClient, SyncServer};
use contextdb_server::{FabricIdentity, PeerEndpoint, peer_bind_spec, peer_dial_spec};
use serde_json::json;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;
use uuid::Uuid;

mod smoke_policy_journey;
mod smoke_purge_journey;
mod smoke_vector_journey;

const DDL_TABLE: &str = "authored_migration";
const DDL_TRIGGER: &str = "authored_migration_insert";
const PARENT_ID: &str = "11111111-1111-4111-8111-111111111111";
const CHILD_ID: &str = "22222222-2222-4222-8222-222222222222";
const OVERSIZED_BODY_BYTES: usize = 64 * 1024 * 1024;
const FITTING_BODY_BYTES: usize = 1024 * 1024;
const IMMUTABLE_DDL_REFUSAL: &str =
    "authenticated received DDL is immutable after transport validation";

#[derive(Parser)]
#[command(name = "contextdb-smoke-driver")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Hub(HubArgs),
    DdlSource(DdlSourceArgs),
    OversizedSource(OversizedSourceArgs),
    Identity(IdentityArgs),
    Policy(PolicyArgs),
    Purge(PurgeArgs),
    Vector(VectorArgs),
    Custody(CustodyArgs),
}

/// The fixed custody receipt accepts only the installed CLI and one scoped
/// work directory. The scenario, policy clauses, identities and transport are
/// all fixed below; this is not a product command or a general test API.
#[derive(clap::Args)]
struct CustodyArgs {
    #[arg(long)]
    root: PathBuf,
    #[arg(long)]
    cli: PathBuf,
}

#[derive(clap::Args)]
struct PolicyArgs {
    #[arg(long)]
    root: PathBuf,
    #[arg(long)]
    cli: PathBuf,
}

#[derive(clap::Args)]
struct VectorArgs {
    #[arg(long)]
    root: PathBuf,
}

#[derive(clap::Args)]
struct PurgeArgs {
    #[arg(long)]
    root: PathBuf,
}

#[derive(clap::Args)]
struct IdentityArgs {
    #[arg(long)]
    db: PathBuf,
    #[arg(long)]
    identity: PathBuf,
    #[arg(long)]
    tenant_id: String,
}

#[derive(clap::Args)]
struct HubArgs {
    #[arg(long)]
    db: PathBuf,
    #[arg(long)]
    identity: PathBuf,
    #[arg(long)]
    ticket_file: PathBuf,
    #[arg(long)]
    tenant_id: String,
    #[arg(long, value_enum, default_value_t = ReceiverMode::Core)]
    receiver: ReceiverMode,
    #[arg(long, value_enum, default_value_t = CheckpointMode::Observe)]
    checkpoint: CheckpointMode,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ReceiverMode {
    Core,
    DdlAdd,
    DdlRemove,
    DdlReplace,
    DdlReorder,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CheckpointMode {
    Observe,
    AfterFragment0,
    AfterApply,
}

#[derive(clap::Args)]
struct DdlSourceArgs {
    #[arg(long)]
    db: PathBuf,
    #[arg(long)]
    identity: PathBuf,
    #[arg(long)]
    ticket_file: PathBuf,
    #[arg(long)]
    tenant_id: String,
    #[arg(long, value_enum, default_value_t = DdlPhase::AuthorPush)]
    phase: DdlPhase,
    #[arg(long, value_enum, default_value_t = ExpectedPush::Success)]
    expect: ExpectedPush,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum DdlPhase {
    AuthorPush,
    PullInspect,
    InspectLocal,
    InspectRefusal,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ExpectedPush {
    Success,
    ImmutableDdlRefusal,
}

#[derive(clap::Args)]
struct OversizedSourceArgs {
    #[arg(long)]
    db: PathBuf,
    #[arg(long)]
    identity: Option<PathBuf>,
    #[arg(long)]
    ticket_file: Option<PathBuf>,
    #[arg(long)]
    tenant_id: String,
    #[arg(long, value_enum)]
    phase: OversizedPhase,
    #[arg(long)]
    source_node_id: Option<String>,
    #[arg(long)]
    source_incarnation: Option<String>,
    #[arg(long, value_enum, default_value_t = RequestFixture::OversizedDependency)]
    fixture: RequestFixture,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OversizedPhase {
    BootstrapAndSeed,
    InitializeHub,
    InspectHub,
    PushExisting,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum RequestFixture {
    OversizedDependency,
    FittingDependency,
    Ordinary,
}

#[derive(Debug)]
struct DdlRewritePlugin {
    mode: ReceiverMode,
}

impl DatabasePlugin for DdlRewritePlugin {
    fn on_sync_pull(&self, changes: &mut ChangeSet) -> contextdb_core::Result<()> {
        match self.mode {
            ReceiverMode::Core => {}
            ReceiverMode::DdlAdd => changes.ddl.push(DdlChange::DropTable {
                name: "receiver_injected_table".to_string(),
            }),
            ReceiverMode::DdlRemove => {
                if changes.ddl.len() >= 2 {
                    changes.ddl.remove(1);
                }
            }
            ReceiverMode::DdlReplace => {
                if changes.ddl.len() >= 2 {
                    changes.ddl[1] = DdlChange::DropTable {
                        name: "receiver_replaced_table".to_string(),
                    };
                }
            }
            ReceiverMode::DdlReorder => {
                if changes.ddl.len() >= 2 {
                    let last = changes.ddl.len() - 1;
                    changes.ddl.swap(0, last);
                }
            }
        }
        Ok(())
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(message) = run(Args::parse()).await {
        eprintln!("smoke-driver error: {message}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), String> {
    match args.command {
        Command::Hub(args) => run_hub(args).await,
        Command::DdlSource(args) => run_ddl_source(args).await,
        Command::OversizedSource(args) => run_oversized_source(args).await,
        Command::Identity(args) => run_identity(args),
        Command::Policy(args) => smoke_policy_journey::run(&args.root, &args.cli).await,
        Command::Purge(args) => smoke_purge_journey::run(&args.root).await,
        Command::Vector(args) => smoke_vector_journey::run(&args.root).await,
        Command::Custody(args) => run_custody(args).await,
    }
}

const CUSTODY_TENANT: &str = "installed-custody";
const CUSTODY_ROOT_TABLE: &str = "custody_roots";
const CUSTODY_MEMBER_TABLE: &str = "custody_parts";
const CUSTODY_ROOT_CLAUSES: &str =
    "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE DELIVERY MANIFEST OVER custody_parts";
const CUSTODY_MEMBER_CLAUSES: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST";

async fn run_custody(args: CustodyArgs) -> Result<(), String> {
    if !args.cli.is_file() {
        return Err("custody journey requires the installed contextdb CLI".into());
    }
    std::fs::create_dir_all(&args.root).map_err(|e| e.to_string())?;
    let hub_path = args.root.join("hub.db");
    let hub_identity = args.root.join("hub.identity");
    let edge_path = args.root.join("edge.db");
    let edge_identity = args.root.join("edge.identity");
    let declarations = custody_declarations();

    // 1. Declare before the hub owns the writer; inspect the same durable
    // declarations through the live owner's ordinary read route afterwards.
    let output = run_installed_cli_write(
        &args.cli,
        &hub_path,
        &format!(
            "{}\n{}\nSHOW TENANT TABLE POLICY;\n",
            declarations[0], declarations[1]
        ),
    )?;
    let declared = custody_result_rows(&output, "pre-server DECLARE / SHOW")?;
    require_custody_policies(&declared)?;
    let mut hub = CustodyHub::start(&hub_path, &hub_identity, CheckpointMode::Observe).await?;
    let shown = custody_cli_rows(&args.cli, &hub_path, ".sync policy\n")?;
    require_custody_policies(&shown)?;
    for before in &declared {
        let after = policy_row(&shown, before["table"].as_str().unwrap_or_default())?;
        for field in CUSTODY_POLICY_FIELDS {
            if before.get(*field) != after.get(*field) {
                return Err(format!("owner-read policy changed declared {field}"));
            }
        }
    }
    custody_pass(
        1,
        "pre-server declarations and live owner-read policy agree",
    )?;

    let edge_db = Arc::new(Database::open(&edge_path).map_err(|e| e.to_string())?);
    let client = custody_client(&edge_db, &hub, &edge_identity);
    custody_within(client.push(), "initial authenticated exchange").await?;
    if edge_db.retention_sync_peer().as_deref() != Some(hub.node_id.as_str()) {
        return Err("normal exchange did not enroll the edge with this hub".into());
    }

    // 2. Database::execute accepts exactly one statement. Sending the CLI's
    // whole DECLARE/SHOW input here would only test the parser's EOI refusal.
    let before_locality = custody_cli_rows(&args.cli, &edge_path, "SHOW SYNC BINDINGS;\n")?;
    let before_policies = custody_cli_rows(&args.cli, &edge_path, "SHOW TENANT TABLE POLICY;\n")?;
    let before_lsn = edge_db.current_lsn();
    for declaration in &declarations {
        match edge_db.execute(declaration, &HashMap::new()) {
            Err(Error::DeclareRequiresAuthoritativeHub { hub_node_id })
                if hub_node_id == hub.node_id => {}
            other => {
                return Err(format!(
                    "edge DECLARE did not refuse with this hub's locality: {other:?}"
                ));
            }
        }
    }
    if edge_db.current_lsn() != before_lsn
        || custody_cli_rows(&args.cli, &edge_path, "SHOW SYNC BINDINGS;\n")? != before_locality
        || custody_cli_rows(&args.cli, &edge_path, "SHOW TENANT TABLE POLICY;\n")?
            != before_policies
        || edge_db.table_meta(CUSTODY_ROOT_TABLE).is_some()
        || edge_db.table_meta(CUSTODY_MEMBER_TABLE).is_some()
    {
        return Err(
            "locality refusal changed declarations, bindings, schema or committed state".into(),
        );
    }
    custody_pass(
        2,
        "each edge DECLARE names the authoritative hub and leaves state unchanged",
    )?;

    // 3. Validate all returned metadata, then close every edge handle and
    // inspect the persisted binding before any new client can refresh it.
    let expectation = custody_expectation()?;
    let binding = custody_within(
        client.bind_application_table_policy(expectation.clone()),
        "matching bind",
    )
    .await?;
    // Statement 3: binding names the tenant and authoritative hub; edge-local
    // identity is not part of this installed journey's public assertion.
    if binding.tenant_id().as_str() != CUSTODY_TENANT
        || binding.hub_node_id() != hub.node_id
        || binding.hub_incarnation().to_hex() == "00000000000000000000000000000000"
        || binding.tables().len() != expectation.tables.len()
    {
        return Err("matching binding has incomplete or incorrect participant metadata".into());
    }
    for (table, expected) in &expectation.tables {
        let bound = binding
            .tables()
            .get(table)
            .ok_or_else(|| format!("binding omitted {table}"))?;
        if bound.policy() != expected
            || bound.version() != 1
            || policy_row(&declared, table)?["digest"] != json!(hex(&bound.digest()))
        {
            return Err(format!(
                "binding changed the declared policy/version/digest for {table}"
            ));
        }
    }
    let bound_rows = custody_cli_rows(&args.cli, &edge_path, "SHOW SYNC BINDINGS;\n")?;
    require_custody_bindings(&bound_rows, &declared, &binding)?;
    let mismatch = ApplicationTablePolicyExpectation::new()
        .expect_table(CUSTODY_ROOT_TABLE, "SYNC PUSH ONLY SYNC CONFLICT KEEP LATEST IMMUTABLE DELIVERY MANIFEST OVER custody_parts")
        .map_err(|e| e.to_string())?;
    let mismatch_lsn = edge_db.current_lsn();
    match tokio::time::timeout(
        Duration::from_secs(30),
        client.bind_application_table_policy(mismatch),
    )
    .await
    {
        Ok(Err(Error::TenantPolicyMismatch { table, clause }))
            if table == CUSTODY_ROOT_TABLE && clause == "conflict" => {}
        other => {
            return Err(format!(
                "mismatching bind did not name the table and conflict clause: {other:?}"
            ));
        }
    }
    if edge_db.current_lsn() != mismatch_lsn
        || custody_cli_rows(&args.cli, &edge_path, "SHOW SYNC BINDINGS;\n")? != bound_rows
        || edge_db.table_meta(CUSTODY_ROOT_TABLE).is_some()
        || edge_db.table_meta(CUSTODY_MEMBER_TABLE).is_some()
    {
        return Err("mismatching bind changed the edge's binding or schema".into());
    }
    client.shutdown().await;
    drop(client);
    edge_db.close().map_err(|e| e.to_string())?;
    drop(edge_db);
    let edge_db = Arc::new(Database::open(&edge_path).map_err(|e| e.to_string())?);
    if custody_cli_rows(&args.cli, &edge_path, "SHOW SYNC BINDINGS;\n")? != bound_rows {
        return Err("complete binding did not survive closing and reopening the edge".into());
    }
    custody_pass(
        3,
        "complete binding survives reopen; mismatching expectation changes nothing",
    )?;
    let client = custody_client(&edge_db, &hub, &edge_identity);

    // 4. Check the mismatching door before the member table exists.
    edge_db
        .execute(&custody_root_ddl(), &HashMap::new())
        .map_err(|e| format!("matching root CREATE: {e}"))?;
    let ddl_lsn = edge_db.current_lsn();
    match edge_db.execute(&custody_member_mismatch_ddl(), &HashMap::new()) {
        Err(Error::TableBindingMismatch { table, clause })
            if table == CUSTODY_MEMBER_TABLE && clause == "conflict" => {}
        other => {
            return Err(format!(
                "mismatching member CREATE did not return TableBindingMismatch: {other:?}"
            ));
        }
    }
    if edge_db.current_lsn() != ddl_lsn || edge_db.table_meta(CUSTODY_MEMBER_TABLE).is_some() {
        return Err("refused CREATE installed a member table or committed state".into());
    }
    edge_db
        .execute(&custody_member_ddl(), &HashMap::new())
        .map_err(|e| format!("matching member CREATE: {e}"))?;
    custody_pass(
        4,
        "matching CREATEs succeed and mismatching CREATE leaves no table",
    )?;

    let root_one = Uuid::from_u128(0x7c000000000040008000000000000001);
    let member_one_a = Uuid::from_u128(0x7c000000000040008000000000000011);
    let member_one_b = Uuid::from_u128(0x7c000000000040008000000000000012);
    let root_two = Uuid::from_u128(0x7c000000000040008000000000000002);
    let root_three = Uuid::from_u128(0x7c000000000040008000000000000003);
    let members = [(member_one_a, "part-a"), (member_one_b, "part-b")];

    // 5. Each registration belongs to a real writing transaction. Erase and
    // re-import the first unit so its equivalent outcome cannot be exact replay.
    commit_custody_unit(&edge_db, root_one, "numbered-root", &members)?;
    let original = require_push_outcome(
        &client,
        &edge_db,
        root_one,
        DeliveryOutcomeKind::Accepted,
        "two-member unit",
    )
    .await?;
    require_custody_unit_rows(&args.cli, &hub_path, root_one, "numbered-root", &members)?;
    let original_rows = custody_cli_rows(
        &args.cli,
        &hub_path,
        "SHOW DELIVERY OUTCOMES FOR custody_roots;\n",
    )?;
    let root_one_key = outcome_row(&original_rows, &original)?["root_key"].clone();
    if root_one_key.is_null() {
        return Err("original hub outcome omitted its typed root key".into());
    }
    commit_custody_unit(&edge_db, root_two, "explicit-empty-root", &[])?;
    require_push_outcome(
        &client,
        &edge_db,
        root_two,
        DeliveryOutcomeKind::Accepted,
        "empty-member unit",
    )
    .await?;
    require_custody_unit_rows(&args.cli, &hub_path, root_two, "explicit-empty-root", &[])?;
    edge_db
        .execute(
            "DISCARD FROM custody_roots WHERE id=$id, custody_parts WHERE root_id=$id",
            &HashMap::from([("id".into(), Value::Uuid(root_one))]),
        )
        .map_err(|e| format!("same-edge discard before re-import: {e}"))?;
    if custody_row_count(&edge_db, CUSTODY_ROOT_TABLE, root_one)? != 0
        || custody_row_count(&edge_db, CUSTODY_MEMBER_TABLE, member_one_a)? != 0
        || custody_row_count(&edge_db, CUSTODY_MEMBER_TABLE, member_one_b)? != 0
        || edge_db
            .delivery_outcome(CUSTODY_ROOT_TABLE, &custody_key(root_one))
            .map_err(|e| e.to_string())?
            .is_some()
    {
        return Err("discard did not erase the first local unit before re-import".into());
    }
    // Statements 17b/20: a fresh local unit starts pending after the prior unit was erased.
    commit_custody_unit(&edge_db, root_one, "numbered-root", &members)?;
    if edge_db
        .delivery_outcome(CUSTODY_ROOT_TABLE, &custody_key(root_one))
        .map_err(|e| e.to_string())?
        .is_some()
        || edge_db
            .delivery_status(CUSTODY_ROOT_TABLE)
            .map_err(|e| e.to_string())?
            .pending
            != 1
    {
        return Err("re-import reused a prior outcome instead of starting pending".into());
    }
    let equivalent = require_push_outcome(
        &client,
        &edge_db,
        root_one,
        DeliveryOutcomeKind::Equivalent,
        "re-imported same-key unit",
    )
    .await?;
    if equivalent.unit_digest() != original.unit_digest() {
        return Err("re-import did not preserve the original unit content".into());
    }
    require_custody_unit_rows(&args.cli, &hub_path, root_one, "numbered-root", &members)?;
    require_custody_counts(&edge_db, 1, 1, 0)?;
    custody_pass(
        5,
        "two-member and explicit-empty units accepted; distinct re-import equivalent",
    )?;

    // 6. Snapshot export opens an independent writer. Reap the hub FIRST.
    client.shutdown().await;
    drop(client);
    hub.stop()?;
    let backup = args.root.join("pre-fourth-outcome.snapshot");
    run_snapshot_export(&args.cli, &hub_path, &backup)?;
    let mut killed_hub =
        CustodyHub::start(&hub_path, &hub_identity, CheckpointMode::AfterApply).await?;
    let interrupted = custody_client(&edge_db, &killed_hub, &edge_identity);
    commit_custody_unit(&edge_db, root_three, "lost-ack-root", &[])?;
    let fourth_lsn = edge_db.current_lsn();
    require_custody_counts(&edge_db, 1, 1, 1)?;
    let mut push_task = tokio::spawn(async move {
        let result = interrupted.push().await;
        interrupted.shutdown().await;
        result
    });
    let checkpoint = match killed_hub.wait_for_after_apply() {
        Ok(event) => event,
        Err(error) => {
            killed_hub.kill()?;
            push_task.abort();
            let _ = push_task.await;
            return Err(error);
        }
    };
    // Statement 11: the installed lost-ack checkpoint identifies its sender.
    let edge_node = FabricIdentity::load_or_generate(&edge_identity)
        .map_err(|e| e.to_string())?
        .node_id();
    let checkpoint_matches = checkpoint["authenticated_node_id"] == json!(edge_node)
        && checkpoint["source_lsn"] == json!(fourth_lsn.0)
        && checkpoint["hub_lsn"].as_u64().is_some_and(|lsn| lsn > 0);
    killed_hub.kill()?;
    // Match the existing installed lost-ack journey's allowance for the
    // transport timeout and its status reconciliation after the process dies.
    match tokio::time::timeout(Duration::from_secs(180), &mut push_task).await {
        Ok(Ok(Err(Error::SyncPushUnconfirmed { .. }))) => {}
        Err(_) => {
            push_task.abort();
            let _ = push_task.await;
            return Err("killed-ack push did not settle within 180 seconds".into());
        }
        other => {
            return Err(format!(
                "killed acknowledgement did not produce an unconfirmed push: {other:?}"
            ));
        }
    }
    if !checkpoint_matches {
        return Err(
            "killed-ack checkpoint did not identify the fourth unit's actual source commit".into(),
        );
    }
    require_custody_counts(&edge_db, 1, 1, 1)?;
    if edge_db
        .delivery_outcome(CUSTODY_ROOT_TABLE, &custody_key(root_three))
        .map_err(|e| e.to_string())?
        .is_some()
    {
        return Err("unacknowledged fourth unit already has local terminal credit".into());
    }
    let committed = args.root.join("fourth-committed-before-fetch.snapshot");
    run_snapshot_export(&args.cli, &hub_path, &committed)?;
    let fourth_key = custody_inspect_key(&args.cli, &committed, CUSTODY_ROOT_TABLE, root_three)?;
    if fourth_key["total_retained_versions"] != json!(1)
        || fourth_key["versions_truncated"] != json!(false)
    {
        return Err(
            "killed-ack checkpoint did not leave exactly one committed fourth-root version".into(),
        );
    }
    let hub_outcomes_before_fetch = custody_cli_rows(
        &args.cli,
        &hub_path,
        "SHOW DELIVERY OUTCOMES FOR custody_roots;\n",
    )?;
    let mut restarted =
        CustodyHub::start(&hub_path, &hub_identity, CheckpointMode::Observe).await?;
    if restarted.node_id != binding.hub_node_id() {
        return Err("same-state restart changed the hub's node identity".into());
    }
    let recovered = custody_client(&edge_db, &restarted, &edge_identity);
    let fetched = custody_within(
        recovered.fetch_delivery_outcomes(None),
        "lost-ack outcome fetch",
    )
    .await?;
    let fourth = require_local_outcome(
        &edge_db,
        root_three,
        DeliveryOutcomeKind::Accepted,
        "fetched fourth unit",
    )?;
    if !fetched.iter().any(|o| same_outcome(o, &fourth))
        || fourth.hub_incarnation() != binding.hub_incarnation()
        || fourth.acceptance_position().0 != checkpoint["hub_lsn"].as_u64().unwrap_or_default()
    {
        return Err(
            "fetch did not return the original fourth-unit outcome from the same hub life".into(),
        );
    }
    outcome_row(&hub_outcomes_before_fetch, &fourth)?;
    require_custody_counts(&edge_db, 2, 1, 0)?;
    recovered.shutdown().await;
    drop(recovered);
    restarted.stop()?;
    let after_fetch = args.root.join("fourth-after-fetch.snapshot");
    run_snapshot_export(&args.cli, &hub_path, &after_fetch)?;
    for (table, key) in [
        (CUSTODY_ROOT_TABLE, root_one),
        (CUSTODY_ROOT_TABLE, root_two),
        (CUSTODY_ROOT_TABLE, root_three),
        (CUSTODY_MEMBER_TABLE, member_one_a),
        (CUSTODY_MEMBER_TABLE, member_one_b),
    ] {
        if custody_inspect_key(&args.cli, &committed, table, key)?
            != custody_inspect_key(&args.cli, &after_fetch, table, key)?
        {
            return Err(format!(
                "outcome fetch reapplied or changed retained {table} row state"
            ));
        }
    }
    if custody_cli_rows(
        &args.cli,
        &hub_path,
        "SHOW DELIVERY OUTCOMES FOR custody_roots;\n",
    )? != hub_outcomes_before_fetch
    {
        return Err("outcome fetch changed the hub's committed outcome journal".into());
    }
    custody_pass(
        6,
        "lost-ack fetch restores terminal credit with identical hub rows, versions and outcomes",
    )?;

    // 7. Open actual copied backup and identity artifacts. The surviving edge
    // keeps its real certificates. pull_default performs the production status
    // probe; .sync status is only a local-session read and cannot discover it.
    let old_outcomes = [
        require_local_outcome(
            &edge_db,
            root_one,
            DeliveryOutcomeKind::Equivalent,
            "pre-restore unit 1",
        )?,
        require_local_outcome(
            &edge_db,
            root_two,
            DeliveryOutcomeKind::Accepted,
            "pre-restore unit 2",
        )?,
        fourth,
    ];
    let local_rows = custody_application_rows(&args.cli, &edge_path)?;
    let restored_dir = args.root.join("restored-hub");
    std::fs::create_dir_all(&restored_dir).map_err(|e| e.to_string())?;
    let restored_path = restored_dir.join("hub.db");
    let restored_identity = restored_dir.join("hub.identity");
    std::fs::copy(&backup, &restored_path).map_err(|e| e.to_string())?;
    std::fs::copy(&hub_identity, &restored_identity).map_err(|e| e.to_string())?;
    if std::fs::read(&backup).map_err(|e| e.to_string())?
        != std::fs::read(&restored_path).map_err(|e| e.to_string())?
        || std::fs::read(&hub_identity).map_err(|e| e.to_string())?
            != std::fs::read(&restored_identity).map_err(|e| e.to_string())?
    {
        return Err(
            "restored database or identity differs from its copied artifact before open".into(),
        );
    }
    let mut restored =
        CustodyHub::start(&restored_path, &restored_identity, CheckpointMode::Observe).await?;
    if restored.node_id != binding.hub_node_id() {
        return Err("copied identity did not preserve the hub node".into());
    }
    require_custody_unit_rows(
        &args.cli,
        &restored_path,
        root_one,
        "numbered-root",
        &members,
    )?;
    require_custody_unit_rows(
        &args.cli,
        &restored_path,
        root_two,
        "explicit-empty-root",
        &[],
    )?;
    if !custody_cli_rows(
        &args.cli,
        &restored_path,
        &format!("SELECT id FROM custody_roots WHERE id='{root_three}';\n"),
    )?
    .is_empty()
    {
        return Err("pre-outcome backup unexpectedly contains the fourth unit".into());
    }
    let restore_client = custody_client(&edge_db, &restored, &edge_identity);
    custody_within(
        restore_client.pull_default(),
        "restore authority discovery through pull's status probe",
    )
    .await?;
    require_custody_counts(&edge_db, 0, 0, 3)?;
    if custody_application_rows(&args.cli, &edge_path)? != local_rows
        || !custody_cli_rows(
            &args.cli,
            &edge_path,
            "SHOW DELIVERY OUTCOMES FOR custody_roots;\n",
        )?
        .is_empty()
    {
        return Err(
            "restore discovery changed local rows or retained old SHOW terminal credit".into(),
        );
    }
    for old in &old_outcomes {
        if edge_db
            .delivery_outcome(CUSTODY_ROOT_TABLE, old.root_key())
            .map_err(|e| e.to_string())?
            .is_some()
        {
            return Err("old-life outcome remained current before actual re-push".into());
        }
    }
    custody_within(
        restore_client.push(),
        "actual re-push after authority invalidation",
    )
    .await?;
    let new_outcomes = [
        require_local_outcome(
            &edge_db,
            root_one,
            DeliveryOutcomeKind::Equivalent,
            "restored present unit 1",
        )?,
        require_local_outcome(
            &edge_db,
            root_two,
            DeliveryOutcomeKind::Equivalent,
            "restored present unit 2",
        )?,
        require_local_outcome(
            &edge_db,
            root_three,
            DeliveryOutcomeKind::Accepted,
            "restored absent unit 4",
        )?,
    ];
    let new_life = new_outcomes[0].hub_incarnation();
    for (old, new) in old_outcomes.iter().zip(&new_outcomes) {
        if new.hub_node_id() != old.hub_node_id()
            || new.hub_incarnation() == old.hub_incarnation()
            || new.hub_incarnation() != new_life
            || new.edge_node_id() != old.edge_node_id()
            || new.edge_incarnation() != old.edge_incarnation()
            || new.unit_digest() != old.unit_digest()
        {
            return Err(
                "re-push reused an old hub life or changed the surviving edge/unit identity".into(),
            );
        }
    }
    let restored_outcomes = custody_within(
        restore_client.fetch_delivery_outcomes(None),
        "current-life outcome fetch after re-push",
    )
    .await?;
    for new in &new_outcomes {
        if !restored_outcomes.iter().any(|o| same_outcome(o, new)) {
            return Err(
                "freshly re-pushed outcome is absent from authenticated current-life fetch".into(),
            );
        }
    }
    require_custody_counts(&edge_db, 1, 2, 0)?;
    require_custody_unit_rows(&args.cli, &restored_path, root_three, "lost-ack-root", &[])?;
    custody_pass(
        7,
        "actual restore invalidates all old credit; re-push accepts absent and equivalents present units",
    )?;

    // 8. This root has both the original materialized owner and the distinct
    // equivalent owner. Inspect every SHOW record for its key, not only the
    // current edge outcome, and inspect retained versions on both machines.
    let hub_before_purge = custody_cli_rows(
        &args.cli,
        &restored_path,
        "SHOW DELIVERY OUTCOMES FOR custody_roots;\n",
    )?;
    let edge_before_purge = custody_cli_rows(
        &args.cli,
        &edge_path,
        "SHOW DELIVERY OUTCOMES FOR custody_roots;\n",
    )?;
    outcome_row(&hub_before_purge, &new_outcomes[0])?;
    outcome_row(&edge_before_purge, &new_outcomes[0])?;
    restore_client.shutdown().await;
    drop(restore_client);
    restored.stop()?;
    let restored_db = Database::open(&restored_path).map_err(|e| e.to_string())?;
    restored_db
        .execute(
            "PURGE FROM custody_roots WHERE id=$id",
            &HashMap::from([("id".into(), Value::Uuid(root_one))]),
        )
        .map_err(|e| format!("hub root PURGE: {e}"))?;
    if custody_row_count(&restored_db, CUSTODY_ROOT_TABLE, root_one)? != 0
        || restored_db
            .delivery_outcome(CUSTODY_ROOT_TABLE, &custody_key(root_one))
            .map_err(|e| e.to_string())?
            .is_some()
    {
        return Err("hub PURGE left the root row or typed custody outcome addressable".into());
    }
    // Received hub materializations are not locally eligible source units.
    require_custody_counts(&restored_db, 0, 0, 0)?;
    restored_db.close().map_err(|e| e.to_string())?;
    drop(restored_db);
    let hub_after_purge = custody_cli_rows(
        &args.cli,
        &restored_path,
        "SHOW DELIVERY OUTCOMES FOR custody_roots;\n",
    )?;
    require_root_outcomes_erased(&hub_before_purge, &hub_after_purge, &root_one_key)?;
    let mut purged_hub =
        CustodyHub::start(&restored_path, &restored_identity, CheckpointMode::Observe).await?;
    let purge_client = custody_client(&edge_db, &purged_hub, &edge_identity);
    custody_within(purge_client.pull_default(), "purge reconnect").await?;
    let after_purge_fetch = custody_within(
        purge_client.fetch_delivery_outcomes(None),
        "post-purge outcome fetch",
    )
    .await?;
    if custody_row_count(&edge_db, CUSTODY_ROOT_TABLE, root_one)? != 0
        || edge_db
            .delivery_outcome(CUSTODY_ROOT_TABLE, &custody_key(root_one))
            .map_err(|e| e.to_string())?
            .is_some()
        || after_purge_fetch
            .iter()
            .any(|o| o.root_table() == CUSTODY_ROOT_TABLE && o.root_key() == &custody_key(root_one))
    {
        return Err(
            "purged equivalent owner still has a row, current outcome or fetchable outcome".into(),
        );
    }
    require_custody_counts(&edge_db, 1, 1, 0)?;
    require_root_outcomes_erased(
        &edge_before_purge,
        &custody_cli_rows(
            &args.cli,
            &edge_path,
            "SHOW DELIVERY OUTCOMES FOR custody_roots;\n",
        )?,
        &root_one_key,
    )?;
    let purged_edge = args.root.join("edge-after-root-purge.snapshot");
    edge_db
        .export_snapshot(&purged_edge)
        .map_err(|e| e.to_string())?;
    purge_client.shutdown().await;
    drop(purge_client);
    purged_hub.stop()?;
    let purged_artifact = args.root.join("hub-after-root-purge.snapshot");
    run_snapshot_export(&args.cli, &restored_path, &purged_artifact)?;
    for artifact in [&purged_artifact, &purged_edge] {
        let key = custody_inspect_key(&args.cli, artifact, CUSTODY_ROOT_TABLE, root_one)?;
        if key["total_retained_versions"] != json!(0) || key["versions_truncated"] != json!(false) {
            return Err(
                "root PURGE retained addressable original/equivalent-owner versions".into(),
            );
        }
    }
    custody_pass(
        8,
        "root PURGE removes original/equivalent-owner rows and all current outcomes on both sides",
    )?;

    // 9. Discard offline under the bound default AFTER OUTCOME. The production
    // key inspector exposes deletion lineage: hub KEEP FIRST alone could hide
    // an erroneous tombstone, so unchanged hub rows cannot prove its absence.
    let hub_before_discard = custody_cli_rows(
        &args.cli,
        &restored_path,
        "SHOW DELIVERY OUTCOMES FOR custody_roots;\n",
    )?;
    let before_discard_key =
        custody_inspect_key(&args.cli, &purged_artifact, CUSTODY_ROOT_TABLE, root_two)?;
    let remaining_before = require_local_outcome(
        &edge_db,
        root_three,
        DeliveryOutcomeKind::Accepted,
        "discard survivor",
    )?;
    let persisted = custody_cli_rows(&args.cli, &edge_path, "SHOW SYNC BINDINGS;\n")?;
    if policy_row(&persisted, CUSTODY_ROOT_TABLE)?["edge_discard"] != json!("after_outcome") {
        return Err("empty-unit discard is not governed by the bound AFTER OUTCOME mode".into());
    }
    edge_db
        .execute(
            "DISCARD FROM custody_roots WHERE id=$id",
            &HashMap::from([("id".into(), Value::Uuid(root_two))]),
        )
        .map_err(|e| format!("AFTER OUTCOME discard: {e}"))?;
    if custody_row_count(&edge_db, CUSTODY_ROOT_TABLE, root_two)? != 0
        || edge_db
            .delivery_outcome(CUSTODY_ROOT_TABLE, &custody_key(root_two))
            .map_err(|e| e.to_string())?
            .is_some()
    {
        return Err("DISCARD left the empty unit's local row or outcome".into());
    }
    require_custody_counts(&edge_db, 1, 0, 0)?;
    let local_after_discard = custody_cli_rows(
        &args.cli,
        &edge_path,
        "SHOW DELIVERY OUTCOMES FOR custody_roots;\n",
    )?;
    if local_after_discard.len() != 1 {
        return Err("DISCARD did not omit exactly the empty local unit from SHOW".into());
    }
    outcome_row(&local_after_discard, &remaining_before)?;
    let discarded_edge = args.root.join("edge-after-empty-discard.snapshot");
    edge_db
        .export_snapshot(&discarded_edge)
        .map_err(|e| e.to_string())?;
    let discarded_key =
        custody_inspect_key(&args.cli, &discarded_edge, CUSTODY_ROOT_TABLE, root_two)?;
    if discarded_key["total_retained_versions"] != json!(0)
        || discarded_key["versions_truncated"] != json!(false)
        || discarded_key.get("lineage") != Some(&serde_json::Value::Null)
    {
        return Err("local DISCARD left retained versions or a deletion/tombstone lineage".into());
    }
    let mut discard_hub =
        CustodyHub::start(&restored_path, &restored_identity, CheckpointMode::Observe).await?;
    let discard_client = custody_client(&edge_db, &discard_hub, &edge_identity);
    if discard_client
        .pending_push_change_count()
        .map_err(|e| e.to_string())?
        != 0
    {
        return Err("local DISCARD created outbound work".into());
    }
    custody_within(discard_client.push(), "post-discard exchange").await?;
    discard_client.shutdown().await;
    drop(discard_client);
    discard_hub.stop()?;
    let final_hub = args.root.join("hub-after-empty-discard.snapshot");
    run_snapshot_export(&args.cli, &restored_path, &final_hub)?;
    if custody_inspect_key(&args.cli, &final_hub, CUSTODY_ROOT_TABLE, root_two)?
        != before_discard_key
        || custody_cli_rows(
            &args.cli,
            &restored_path,
            "SHOW DELIVERY OUTCOMES FOR custody_roots;\n",
        )? != hub_before_discard
    {
        return Err(
            "edge DISCARD changed the hub's empty-unit row/history or outcome journal".into(),
        );
    }
    require_custody_counts(&edge_db, 1, 0, 0)?;
    edge_db.close().map_err(|e| e.to_string())?;
    custody_pass(
        9,
        "offline AFTER OUTCOME discard has no tombstone or outbound work; hub row and outcome unchanged",
    )?;
    Ok(())
}

const CUSTODY_POLICY_FIELDS: &[&str] = &[
    "table",
    "version",
    "digest",
    "sync_direction",
    "sync_conflict",
    "immutable",
    "retain_seconds",
    "retain_unit",
    "sync_safe",
    "history",
    "manifest_tables",
    // Statement 20: whole-row policy has no content-exclusion clause.
    "edge_discard",
];

fn custody_declarations() -> [String; 2] {
    [
        format!("DECLARE TENANT TABLE POLICY {CUSTODY_ROOT_TABLE} {CUSTODY_ROOT_CLAUSES};"),
        format!("DECLARE TENANT TABLE POLICY {CUSTODY_MEMBER_TABLE} {CUSTODY_MEMBER_CLAUSES};"),
    ]
}

fn custody_expectation() -> Result<ApplicationTablePolicyExpectation, String> {
    ApplicationTablePolicyExpectation::new()
        .expect_table(CUSTODY_ROOT_TABLE, CUSTODY_ROOT_CLAUSES)
        .and_then(|e| e.expect_table(CUSTODY_MEMBER_TABLE, CUSTODY_MEMBER_CLAUSES))
        .map_err(|e| e.to_string())
}

fn custody_client(db: &Arc<Database>, hub: &CustodyHub, identity: &Path) -> SyncClient {
    SyncClient::new(
        db.clone(),
        &peer_dial_spec(&hub.ticket, identity),
        TenantId::from(CUSTODY_TENANT),
    )
}

async fn custody_within<T>(
    future: impl std::future::Future<Output = contextdb_core::Result<T>>,
    operation: &str,
) -> Result<T, String> {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .map_err(|_| format!("{operation} exceeded 30 seconds"))?
        .map_err(|e| format!("{operation}: {e}"))
}

fn custody_result_rows(
    output: &std::process::Output,
    operation: &str,
) -> Result<Vec<serde_json::Value>, String> {
    if !output.status.success() {
        return Err(format!(
            "installed CLI {operation} exited {}",
            output.status
        ));
    }
    let mut results = Vec::new();
    for line in output
        .stdout
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
    {
        let document: serde_json::Value = serde_json::from_slice(line)
            .map_err(|e| format!("{operation} emitted invalid JSON: {e}"))?;
        if let Some(result) = document.get("result") {
            let rows = result
                .get("rows")
                .and_then(|r| r.as_array())
                .ok_or_else(|| format!("{operation} omitted result.rows"))?;
            if rows.iter().any(|row| !row.is_object()) {
                return Err(format!("{operation} emitted unnamed result rows"));
            }
            results.push(rows.clone());
        }
    }
    if results.len() != 1 {
        return Err(format!(
            "{operation} must emit exactly one named query result, got {}",
            results.len()
        ));
    }
    Ok(results.remove(0))
}

fn custody_cli_rows(
    cli: &Path,
    database: &Path,
    input: &str,
) -> Result<Vec<serde_json::Value>, String> {
    custody_result_rows(&run_installed_cli_read(cli, database, input)?, input.trim())
}

fn policy_row<'a>(
    rows: &'a [serde_json::Value],
    table: &str,
) -> Result<&'a serde_json::Value, String> {
    let mut matches = rows.iter().filter(|row| row["table"] == table);
    let row = matches
        .next()
        .ok_or_else(|| format!("inspection omitted table {table}"))?;
    if matches.next().is_some() {
        return Err(format!("inspection duplicated table {table}"));
    }
    Ok(row)
}

fn require_custody_policies(rows: &[serde_json::Value]) -> Result<(), String> {
    if rows.len() != 2 {
        return Err(format!(
            "policy inspection must show the two declared tables, got {}",
            rows.len()
        ));
    }
    for table in [CUSTODY_ROOT_TABLE, CUSTODY_MEMBER_TABLE] {
        let row = policy_row(rows, table)?;
        let digest = row["digest"].as_str().unwrap_or_default();
        if row["version"] != json!(1)
            || digest.len() != 64
            || !digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || digest.bytes().all(|b| b == b'0')
            || row["sync_direction"] != "push_only"
            || row["sync_conflict"] != "keep_first"
            || row["immutable"] != json!(table == CUSTODY_ROOT_TABLE)
            || row.get("retain_seconds") != Some(&serde_json::Value::Null)
            || row.get("retain_unit") != Some(&serde_json::Value::Null)
            || row["sync_safe"] != json!(false)
            || row["history"] != "all"
            // Statement 20: retain all other declared/default policy assertions.
            || row["edge_discard"] != "after_outcome"
            || (table == CUSTODY_ROOT_TABLE
                && row["manifest_tables"] != json!([CUSTODY_MEMBER_TABLE]))
        {
            return Err(format!(
                "{table} policy inspection omitted or changed declared/default metadata"
            ));
        }
    }
    Ok(())
}

fn require_custody_bindings(
    rows: &[serde_json::Value],
    declared: &[serde_json::Value],
    binding: &contextdb_engine::AuthenticatedTenantPolicyBinding,
) -> Result<(), String> {
    if rows.len() != 2 {
        return Err("SHOW SYNC BINDINGS did not render both bound tables".into());
    }
    for table in [CUSTODY_ROOT_TABLE, CUSTODY_MEMBER_TABLE] {
        let row = policy_row(rows, table)?;
        let policy = policy_row(declared, table)?;
        // Statement 3: binding inspection carries policy metadata, never row content.
        if row["tenant_id"] != CUSTODY_TENANT
            || row["hub_node_id"] != binding.hub_node_id()
            || row["hub_incarnation"] != binding.hub_incarnation().to_hex()
            || row.get("id").is_some()
            || row.get("record_id").is_some()
            || row.get("body").is_some()
        {
            return Err(
                "SHOW SYNC BINDINGS omitted policy metadata or included row content".into(),
            );
        }
        for field in CUSTODY_POLICY_FIELDS {
            if row.get(*field) != policy.get(*field) {
                return Err(format!(
                    "persisted binding differs from declared {table}.{field}"
                ));
            }
        }
    }
    Ok(())
}

fn require_custody_counts(
    db: &Database,
    accepted: u64,
    equivalent: u64,
    pending: u64,
) -> Result<(), String> {
    let actual = db
        .delivery_status(CUSTODY_ROOT_TABLE)
        .map_err(|e| e.to_string())?;
    let expected = contextdb_engine::DeliveryStatusCounts {
        disabled: false,
        eligible: accepted + equivalent + pending,
        accepted,
        equivalent,
        pending,
        refused: 0,
    };
    if actual != expected {
        return Err(format!(
            "custody counts do not match current local units: expected {expected:?}, got {actual:?}"
        ));
    }
    Ok(())
}

fn require_local_outcome(
    db: &Database,
    root: Uuid,
    expected: DeliveryOutcomeKind,
    label: &str,
) -> Result<contextdb_engine::DeliveryOutcome, String> {
    let outcome = db
        .delivery_outcome(CUSTODY_ROOT_TABLE, &custody_key(root))
        .map_err(|e| format!("{label} outcome inspection: {e}"))?
        .ok_or_else(|| format!("{label} has no current terminal outcome"))?;
    if outcome.kind() != expected
        || outcome.tenant_id().as_str() != CUSTODY_TENANT
        || outcome.root_table() != CUSTODY_ROOT_TABLE
        || outcome.root_key() != &custody_key(root)
        || outcome.unit_digest().is_none_or(|digest| digest == [0; 32])
    {
        return Err(format!(
            "{label} returned an incorrect/incomplete outcome: {outcome:?}"
        ));
    }
    Ok(outcome)
}

fn same_outcome(
    a: &contextdb_engine::DeliveryOutcome,
    b: &contextdb_engine::DeliveryOutcome,
) -> bool {
    // Statements 11/13/20: compare promised identity, content and acceptance facts.
    a.tenant_id() == b.tenant_id()
        && a.hub_node_id() == b.hub_node_id()
        && a.hub_incarnation() == b.hub_incarnation()
        && a.edge_node_id() == b.edge_node_id()
        && a.edge_incarnation() == b.edge_incarnation()
        && a.root_table() == b.root_table()
        && a.root_key() == b.root_key()
        && a.unit_digest() == b.unit_digest()
        && a.kind() == b.kind()
        && a.cause() == b.cause()
        && a.conflicts() == b.conflicts()
        && a.acceptance_position() == b.acceptance_position()
}

fn outcome_row<'a>(
    rows: &'a [serde_json::Value],
    outcome: &contextdb_engine::DeliveryOutcome,
) -> Result<&'a serde_json::Value, String> {
    // Statements 19/20: inspect promised facts without requiring private submission/cursor fields.
    let root_key = json!(outcome.root_key());
    let digest = outcome.unit_digest().map(|digest| hex(&digest));
    let word = match outcome.kind() {
        DeliveryOutcomeKind::Accepted => "accepted",
        DeliveryOutcomeKind::Equivalent => "equivalent",
        DeliveryOutcomeKind::Refused => "refused",
    };
    rows.iter()
        .find(|row| {
            row["root_table"] == outcome.root_table()
                && row["root_key"] == root_key
                && row["unit_digest"] == json!(digest)
                && row["hub_node_id"] == outcome.hub_node_id()
                && row["hub_incarnation"] == outcome.hub_incarnation().to_hex()
                && row["edge_node_id"] == outcome.edge_node_id()
                && row["edge_incarnation"] == outcome.edge_incarnation().to_hex()
                && row["outcome"] == word
        })
        .ok_or_else(|| {
            "SHOW did not render the current root, digest, outcome and hub/edge identities".into()
        })
}

fn require_root_outcomes_erased(
    before: &[serde_json::Value],
    after: &[serde_json::Value],
    root_key: &serde_json::Value,
) -> Result<(), String> {
    if !before.iter().any(|r| &r["root_key"] == root_key) {
        return Err("purge comparison lacks the original root's outcome".into());
    }
    let survivors: Vec<_> = before
        .iter()
        .filter(|r| &r["root_key"] != root_key)
        .cloned()
        .collect();
    if after != survivors {
        return Err(
            "PURGE left an original/current owner outcome or changed another root's outcome".into(),
        );
    }
    Ok(())
}

fn require_custody_unit_rows(
    cli: &Path,
    database: &Path,
    root: Uuid,
    body: &str,
    members: &[(Uuid, &str)],
) -> Result<(), String> {
    let rows = custody_cli_rows(
        cli,
        database,
        &format!("SELECT id, body FROM custody_roots WHERE id='{root}';\n"),
    )?;
    if rows != vec![json!({"id":root.to_string(), "body":body})] {
        return Err("hub does not hold the complete expected root row".into());
    }
    let rows = custody_cli_rows(
        cli,
        database,
        &format!(
            "SELECT id, root_id, body FROM custody_parts WHERE root_id='{root}' ORDER BY id;\n"
        ),
    )?;
    let mut expected: Vec<_> = members
        .iter()
        .map(|(id, value)| json!({"id":id.to_string(), "root_id":root.to_string(), "body":value}))
        .collect();
    expected.sort_by_key(|row| row["id"].as_str().unwrap_or_default().to_string());
    if rows != expected {
        return Err(
            "hub does not hold exactly the unit's declared members (including explicit empty)"
                .into(),
        );
    }
    Ok(())
}

fn custody_application_rows(
    cli: &Path,
    database: &Path,
) -> Result<Vec<Vec<serde_json::Value>>, String> {
    Ok(vec![
        custody_cli_rows(cli, database, "SELECT * FROM custody_roots ORDER BY id;\n")?,
        custody_cli_rows(cli, database, "SELECT * FROM custody_parts ORDER BY id;\n")?,
    ])
}

fn custody_inspect_key(
    cli: &Path,
    artifact: &Path,
    table: &str,
    id: Uuid,
) -> Result<serde_json::Value, String> {
    let output = ProcessCommand::new(cli)
        .args(["inspect", "key"])
        .arg(artifact)
        .args(["--table", table, "--key-json"])
        .arg(serde_json::to_string(&custody_key(id)).map_err(|e| e.to_string())?)
        .args(["--column", "id", "--column", "body", "--json"])
        .output()
        .map_err(|e| e.to_string())?;
    custody_record_cli_output(&output, "inspect key")?;
    if !output.status.success() {
        return Err(format!("installed key inspection exited {}", output.status));
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|e| format!("key inspection did not emit JSON: {e}"))
}

fn custody_record_cli_output(output: &std::process::Output, operation: &str) -> Result<(), String> {
    // Preserve real child streams in the enclosing driver's captured stderr.
    let mut stderr = std::io::stderr().lock();
    writeln!(
        stderr,
        "installed CLI {operation}: {}\nstdout:",
        output.status
    )
    .map_err(|e| e.to_string())?;
    stderr
        .write_all(&output.stdout)
        .map_err(|e| e.to_string())?;
    writeln!(stderr, "\nstderr:").map_err(|e| e.to_string())?;
    stderr
        .write_all(&output.stderr)
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn run_installed_cli_write(
    cli: &Path,
    database: &Path,
    input: &str,
) -> Result<std::process::Output, String> {
    run_installed_custody_cli(cli, database, input, true)
}

fn run_installed_cli_read(
    cli: &Path,
    database: &Path,
    input: &str,
) -> Result<std::process::Output, String> {
    run_installed_custody_cli(cli, database, input, false)
}

fn run_installed_custody_cli(
    cli: &Path,
    database: &Path,
    input: &str,
    write: bool,
) -> Result<std::process::Output, String> {
    let mut command = ProcessCommand::new(cli);
    command.arg(database).arg("--json");
    if write {
        command.arg("--write");
    }
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| e.to_string())?;
    let sent = child
        .stdin
        .take()
        .ok_or_else(|| "installed CLI has no stdin".to_string())?
        .write_all(input.as_bytes());
    // Reap even when sending input fails; the actual child refusal is useful evidence.
    let output = child.wait_with_output().map_err(|e| e.to_string())?;
    custody_record_cli_output(&output, input.trim())?;
    sent.map_err(|e| format!("sending fixed CLI input failed: {e}"))?;
    Ok(output)
}

fn run_snapshot_export(cli: &Path, database: &Path, artifact: &Path) -> Result<(), String> {
    let output = ProcessCommand::new(cli)
        .args(["snapshot", "export"])
        .arg(database)
        .arg(artifact)
        .arg("--json")
        .output()
        .map_err(|e| e.to_string())?;
    custody_record_cli_output(&output, "snapshot export")?;
    if !output.status.success() || !artifact.is_file() {
        return Err("installed snapshot export did not produce the requested artifact".into());
    }
    Ok(())
}

fn custody_root_ddl() -> String {
    format!(
        "CREATE TABLE {CUSTODY_ROOT_TABLE} (id UUID PRIMARY KEY, body TEXT) {CUSTODY_ROOT_CLAUSES}"
    )
}
fn custody_member_ddl() -> String {
    format!(
        "CREATE TABLE {CUSTODY_MEMBER_TABLE} (id UUID PRIMARY KEY, root_id UUID REFERENCES {CUSTODY_ROOT_TABLE}(id), body TEXT) {CUSTODY_MEMBER_CLAUSES}"
    )
}
fn custody_member_mismatch_ddl() -> String {
    format!(
        "CREATE TABLE {CUSTODY_MEMBER_TABLE} (id UUID PRIMARY KEY, root_id UUID REFERENCES {CUSTODY_ROOT_TABLE}(id), body TEXT) SYNC PUSH ONLY SYNC CONFLICT KEEP LATEST"
    )
}
fn custody_key(id: Uuid) -> NaturalKey {
    NaturalKey::single("id".to_string(), Value::Uuid(id))
}
fn custody_pass(number: u8, detail: &str) -> Result<(), String> {
    println!(
        "{}",
        json!({"event":"custody_assertion","assertion":number,"detail":detail})
    );
    std::io::stdout()
        .flush()
        .map_err(|_| "cannot flush custody assertion".to_string())
}

fn commit_custody_unit(
    db: &Database,
    root: Uuid,
    body: &str,
    members: &[(Uuid, &str)],
) -> Result<(), String> {
    let tx = db
        .begin()
        .map_err(|error| format!("cannot begin custody unit: {error}"))?;
    db.insert_row(
        tx,
        CUSTODY_ROOT_TABLE,
        HashMap::from([
            ("id".to_string(), Value::Uuid(root)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .map_err(|error| format!("cannot stage custody root: {error}"))?;
    let mut manifest_members = Vec::new();
    for (id, member_body) in members {
        db.insert_row(
            tx,
            CUSTODY_MEMBER_TABLE,
            HashMap::from([
                ("id".to_string(), Value::Uuid(*id)),
                ("root_id".to_string(), Value::Uuid(root)),
                ("body".to_string(), Value::Text((*member_body).to_string())),
            ]),
        )
        .map_err(|error| format!("cannot stage custody member: {error}"))?;
        manifest_members.push((CUSTODY_MEMBER_TABLE, custody_key(*id)));
    }
    db.register_delivery_manifest(
        tx,
        DeliveryManifest {
            root_table: CUSTODY_ROOT_TABLE,
            root_key: custody_key(root),
            members: manifest_members,
        },
    )
    .map_err(|error| format!("cannot register fixed custody manifest: {error}"))?;
    db.commit(tx)
        .map_err(|error| format!("cannot commit fixed custody unit: {error}"))
}

async fn require_push_outcome(
    client: &SyncClient,
    db: &Database,
    root: Uuid,
    expected: DeliveryOutcomeKind,
    label: &str,
) -> Result<contextdb_engine::DeliveryOutcome, String> {
    custody_within(client.push(), label).await?;
    require_local_outcome(db, root, expected, label)
}

fn custody_row_count(db: &Database, table: &str, id: Uuid) -> Result<usize, String> {
    Ok(db
        .execute(
            &format!("SELECT id FROM {table} WHERE id=$id"),
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .map_err(|error| format!("cannot inspect custody row: {error}"))?
        .rows
        .len())
}

struct CustodyHub {
    child: Option<Child>,
    ticket: String,
    node_id: String,
    events: Receiver<String>,
}
impl CustodyHub {
    async fn start(db: &Path, identity: &Path, checkpoint: CheckpointMode) -> Result<Self, String> {
        Self::start_executable(
            &std::env::current_exe().map_err(|e| e.to_string())?,
            db,
            identity,
            checkpoint,
        )
    }

    fn start_executable(
        executable: &Path,
        db: &Path,
        identity: &Path,
        checkpoint: CheckpointMode,
    ) -> Result<Self, String> {
        let ticket_file = db
            .parent()
            .ok_or_else(|| "custody hub database has no parent".to_string())?
            .join("ticket");
        let checkpoint = match checkpoint {
            CheckpointMode::Observe => "observe",
            CheckpointMode::AfterFragment0 => "after-fragment0",
            CheckpointMode::AfterApply => "after-apply",
        };
        let mut child = ProcessCommand::new(executable)
            .args(["hub", "--db"])
            .arg(db)
            .arg("--identity")
            .arg(identity)
            .arg("--ticket-file")
            .arg(&ticket_file)
            .args([
                "--tenant-id",
                CUSTODY_TENANT,
                "--receiver",
                "core",
                "--checkpoint",
                checkpoint,
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| e.to_string())?;
        let (sender, events) = mpsc::channel();
        // Install cleanup before the first fallible readiness/ticket read.
        let stdout = child.stdout.take();
        let mut hub = Self {
            child: Some(child),
            ticket: String::new(),
            node_id: String::new(),
            events,
        };
        let stdout = stdout.ok_or_else(|| "custody hub has no stdout".to_string())?;
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                eprintln!("custody hub stdout: {line}");
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        let ready = custody_wait_event(&hub.events, "ready")?;
        hub.node_id = ready["node_id"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| "routes-ready event omitted the actual hub node".to_string())?
            .to_string();
        hub.ticket = read_ticket(&ticket_file)?;
        Ok(hub)
    }

    fn wait_for_after_apply(&self) -> Result<serde_json::Value, String> {
        let event = custody_wait_event(&self.events, "completed_apply_before_reply")?;
        if event["dependency_complete"] != json!(true) || event["response_success"] != json!(true) {
            return Err(
                "custody hub checkpoint was not a successful complete apply before reply".into(),
            );
        }
        Ok(event)
    }

    fn kill(&mut self) -> Result<(), String> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        let killed = child.kill();
        let reaped = child
            .wait()
            .map_err(|e| format!("cannot reap custody hub: {e}"))?;
        killed.map_err(|e| {
            format!("custody hub exited before its requested process boundary ({reaped}): {e}")
        })?;
        Ok(())
    }

    fn stop(&mut self) -> Result<(), String> {
        // Every relevant commit is already durable. Reaping this process is
        // the writer-lifetime boundary before CLI export/open or artifact copy.
        self.kill()
    }
}
impl Drop for CustodyHub {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn custody_wait_event(
    events: &Receiver<String>,
    wanted: &str,
) -> Result<serde_json::Value, String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let line = events
            .recv_timeout(remaining)
            .map_err(|e| format!("custody hub did not reach {wanted}: {e}"))?;
        let event: serde_json::Value =
            serde_json::from_str(&line).map_err(|e| format!("invalid hub checkpoint JSON: {e}"))?;
        if event["event"] == wanted {
            return Ok(event);
        }
        if event["event"] != "push_request_path" {
            return Err(format!(
                "custody hub emitted an unexpected event while waiting for {wanted}: {event}"
            ));
        }
    }
}

#[cfg(test)]
mod custody_operational_tests {
    use super::*;

    #[test]
    fn checkpoint_reader_follows_path_events_and_parses_reordered_keys() {
        let (sender, events) = mpsc::channel();
        sender
            .send(json!({"event": "push_request_path", "chunked": false}).to_string())
            .unwrap();
        sender
            .send(
                json!({"event": "completed_apply_before_reply", "dependency_complete": true})
                    .to_string(),
            )
            .unwrap();
        drop(sender);
        let checkpoint = custody_wait_event(&events, "completed_apply_before_reply").unwrap();
        assert_eq!(checkpoint["dependency_complete"], json!(true));
        assert!(
            custody_wait_event(&events, "ready").is_err(),
            "closed output is not readiness"
        );
    }

    #[test]
    fn a_checkpoint_name_inside_another_field_does_not_signal_readiness() {
        let (sender, events) = mpsc::channel();
        sender
            .send(json!({"event": "failure", "detail": "ready"}).to_string())
            .unwrap();
        drop(sender);
        assert!(custody_wait_event(&events, "ready").is_err());
    }

    #[test]
    fn custody_queries_reach_single_statement_execution_with_required_for() {
        let db = Database::open_memory();
        let params = HashMap::new();
        db.execute("SHOW DELIVERY OUTCOMES FOR custody_roots", &params)
            .unwrap();
        assert!(matches!(
            db.execute("SHOW DELIVERY OUTCOMES custody_roots", &params),
            Err(Error::ParseError(_))
        ));
        for declaration in custody_declarations() {
            assert!(
                !matches!(db.execute(&declaration, &params), Err(Error::ParseError(_))),
                "single DECLARE must reach execution"
            );
            assert!(
                matches!(
                    db.execute(
                        &format!("{declaration}\nSHOW TENANT TABLE POLICY;"),
                        &params
                    ),
                    Err(Error::ParseError(_))
                ),
                "Database::execute is not a CLI statement stream"
            );
        }
        custody_expectation().unwrap();
    }
}

fn run_identity(args: IdentityArgs) -> Result<(), String> {
    validate_path_parent(&args.db)?;
    validate_path_parent(&args.identity)?;
    let identity = FabricIdentity::load_or_generate(&args.identity)
        .map_err(|_| "cannot load verifier fabric identity".to_string())?;
    let database = Database::open(&args.db)
        .map_err(|_| "cannot open verifier identity database".to_string())?;
    let incarnation = database
        .production_smoke_sync_incarnation(&TenantId::from(args.tenant_id.as_str()))
        .map_err(|_| "cannot inspect verifier database incarnation".to_string())?;
    println!(
        "{}",
        json!({
            "event": "identity",
            "node_id": identity.node_id(),
            "database_incarnation": incarnation.to_hex(),
        })
    );
    database
        .close()
        .map_err(|_| "cannot close verifier identity database".to_string())?;
    Ok(())
}

async fn run_hub(args: HubArgs) -> Result<(), String> {
    validate_path_parent(&args.db)?;
    validate_path_parent(&args.identity)?;
    validate_path_parent(&args.ticket_file)?;
    let gate_kind = match args.checkpoint {
        CheckpointMode::Observe => ProductionSmokeGateKind::ObserveOnly,
        CheckpointMode::AfterFragment0 => ProductionSmokeGateKind::AfterFirstDurableRequestFragment,
        CheckpointMode::AfterApply => ProductionSmokeGateKind::AfterCompletedApplyBeforeReply,
    };
    let checkpoints = arm_production_smoke_gate(gate_kind).map_err(str::to_string)?;
    let plugin: Arc<dyn DatabasePlugin> = match args.receiver {
        ReceiverMode::Core => Arc::new(CorePlugin),
        mode => Arc::new(DdlRewritePlugin { mode }),
    };
    let database = Arc::new(
        open_with_startup_limits(&args.db, plugin, None, None)
            .map_err(|_| "cannot open verifier hub database".to_string())?,
    );
    let endpoint = PeerEndpoint::bind(&peer_bind_spec(&args.identity))
        .await
        .map_err(|_| "cannot bind verifier hub endpoint".to_string())?;
    write_ticket(&args.ticket_file, &endpoint.ticket())?;
    let node_id = endpoint.node_id();
    // Statement 11: exercise the same established constructor as the server binary.
    let server = SyncServer::new(
        database.clone(),
        &endpoint,
        TenantId::from(args.tenant_id.as_str()),
    );
    let reported_db = args.db.display().to_string();
    std::thread::spawn(move || {
        while let Ok(checkpoint) = checkpoints.recv() {
            let event = match checkpoint {
                ProductionSmokeCheckpoint::RoutesReady => json!({
                    "event": "ready",
                    "db": reported_db,
                    "node_id": node_id,
                }),
                ProductionSmokeCheckpoint::DurableRequestFragment {
                    transfer_digest,
                    sequence,
                    next_missing,
                } => json!({
                    "event": "durable_request_fragment",
                    "transfer_digest": hex(&transfer_digest),
                    "sequence": sequence,
                    "next_missing": next_missing,
                }),
                ProductionSmokeCheckpoint::PushRequestPath {
                    request_digest,
                    chunked,
                } => json!({
                    "event": "push_request_path",
                    "request_digest": hex(&request_digest),
                    "chunked": chunked,
                }),
                ProductionSmokeCheckpoint::CompletedApplyBeforeReply {
                    request_digest,
                    authenticated_node_id,
                    source_lsn,
                    hub_lsn,
                    dependency_complete,
                } => json!({
                    "event": "completed_apply_before_reply",
                    "request_digest": hex(&request_digest),
                    "authenticated_node_id": authenticated_node_id,
                    "source_lsn": source_lsn,
                    "hub_lsn": hub_lsn,
                    "dependency_complete": dependency_complete,
                    "response_success": true,
                }),
            };
            println!("{event}");
            let _ = std::io::stdout().flush();
        }
    });

    let shutdown = Arc::new(AtomicBool::new(false));
    let signal = shutdown.clone();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        signal.store(true, Ordering::SeqCst);
    });
    server.run_until(shutdown).await;
    endpoint.close().await;
    database
        .close()
        .map_err(|_| "cannot close verifier hub database".to_string())?;
    Ok(())
}

async fn run_ddl_source(args: DdlSourceArgs) -> Result<(), String> {
    let database = Arc::new(
        Database::open(&args.db).map_err(|_| "cannot open DDL source database".to_string())?,
    );
    if matches!(
        args.phase,
        DdlPhase::InspectLocal | DdlPhase::InspectRefusal
    ) {
        match args.phase {
            DdlPhase::InspectLocal => print_ddl_vector(&database, "local_ddl_vector")?,
            DdlPhase::InspectRefusal => print_refused_ddl_state(&database)?,
            _ => unreachable!(),
        }
        database
            .close()
            .map_err(|_| "cannot close DDL inspection database".to_string())?;
        return Ok(());
    }
    let ticket = read_ticket(&args.ticket_file)?;
    if matches!(args.phase, DdlPhase::AuthorPush) && database.table_meta(DDL_TABLE).is_none() {
        let empty = HashMap::new();
        database
            .execute("BEGIN", &empty)
            .map_err(|_| "DDL BEGIN failed")?;
        database
            .execute(
                "CREATE TABLE authored_migration (id UUID PRIMARY KEY, body TEXT) \
                 SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
                &empty,
            )
            .map_err(|_| "DDL CREATE TABLE failed")?;
        database
            .execute(
                "CREATE TRIGGER authored_migration_insert ON authored_migration WHEN INSERT",
                &empty,
            )
            .map_err(|_| "DDL CREATE TRIGGER failed")?;
        database
            .execute(
                "ALTER TABLE authored_migration ADD COLUMN detail TEXT",
                &empty,
            )
            .map_err(|_| "DDL ALTER TABLE failed")?;
        database
            .execute("COMMIT", &empty)
            .map_err(|_| "DDL COMMIT failed")?;
    }
    let dial = peer_dial_spec(&ticket, &args.identity);
    let client = SyncClient::new(
        database.clone(),
        &dial,
        TenantId::from(args.tenant_id.as_str()),
    );
    match args.phase {
        DdlPhase::AuthorPush => {
            print_ddl_vector(&database, "authored_ddl_vector")?;
            let pushed = client.push().await;
            match (args.expect, pushed) {
                (ExpectedPush::Success, Ok(result)) => println!(
                    "{}",
                    json!({
                        "event": "ddl_push_accepted",
                        "new_lsn": result.new_lsn.0,
                        "applied_rows": result.applied_rows,
                    })
                ),
                (ExpectedPush::ImmutableDdlRefusal, Err(error))
                    if error.to_string().contains(IMMUTABLE_DDL_REFUSAL) =>
                {
                    println!(
                        "{}",
                        json!({
                            "event": "ddl_push_refused",
                            "error": IMMUTABLE_DDL_REFUSAL,
                        })
                    )
                }
                (ExpectedPush::Success, Err(_)) => {
                    return Err("expected DDL push success".to_string());
                }
                (ExpectedPush::ImmutableDdlRefusal, Ok(_)) => {
                    return Err("receiver accepted an authenticated DDL rewrite".to_string());
                }
                (ExpectedPush::ImmutableDdlRefusal, Err(_)) => {
                    return Err("receiver returned the wrong DDL refusal".to_string());
                }
            }
        }
        DdlPhase::PullInspect => {
            if !matches!(args.expect, ExpectedPush::Success) {
                return Err("pull-inspect accepts only --expect success".to_string());
            }
            client
                .pull_default()
                .await
                .map_err(|_| "DDL pull failed".to_string())?;
            print_ddl_vector(&database, "received_ddl_vector")?;
        }
        DdlPhase::InspectLocal | DdlPhase::InspectRefusal => {
            unreachable!("handled before transport setup")
        }
    }
    client.shutdown().await;
    database
        .close()
        .map_err(|_| "cannot close DDL source database".to_string())?;
    Ok(())
}

fn print_ddl_vector(database: &Database, event: &str) -> Result<(), String> {
    let authored = database.changes_since(contextdb_core::Lsn(0));
    let (kinds, ddl_lsn) = exact_fixture_ddl_vector(&authored)?;
    println!(
        "{}",
        json!({
            "event": event,
            "order": kinds,
            "source_lsn": ddl_lsn.0,
        })
    );
    std::io::stdout()
        .flush()
        .map_err(|_| "stdout failed".to_string())
}

fn exact_fixture_ddl_vector(
    changes: &ChangeSet,
) -> Result<(Vec<&'static str>, contextdb_core::Lsn), String> {
    if changes.ddl.len() != changes.ddl_lsn.len() {
        return Err("DDL is not one exact same-LSN three-item vector".to_string());
    }
    let fixture_entries = changes
        .ddl
        .iter()
        .zip(changes.ddl_lsn.iter().copied())
        .filter(|(change, _)| ddl_targets_fixture(change))
        .collect::<Vec<_>>();
    let kinds = fixture_entries
        .iter()
        .map(|(change, _)| ddl_kind(change))
        .collect::<Vec<_>>();
    let Some((_, ddl_lsn)) = fixture_entries.first() else {
        return Err("DDL is not one exact same-LSN three-item vector".to_string());
    };
    if kinds != ["create_table", "create_trigger", "alter_table"]
        || fixture_entries.len() != 3
        || fixture_entries.iter().any(|(_, lsn)| lsn != ddl_lsn)
    {
        return Err("DDL is not one exact same-LSN three-item vector".to_string());
    }
    Ok((kinds, *ddl_lsn))
}

fn ddl_targets_fixture(change: &DdlChange) -> bool {
    match change {
        DdlChange::CreateTable { name, .. }
        | DdlChange::DropTable { name }
        | DdlChange::AlterTable { name, .. } => name == DDL_TABLE,
        DdlChange::CreateIndex { table, .. }
        | DdlChange::DropIndex { table, .. }
        | DdlChange::CreateEventType { table, .. }
        | DdlChange::CreateRoute { table, .. }
        | DdlChange::DropRoute { table, .. } => table == DDL_TABLE,
        DdlChange::CreateTrigger { name, table, .. }
        | DdlChange::CreateTriggerIncludingSync { name, table, .. } => {
            name == DDL_TRIGGER || table == DDL_TABLE
        }
        DdlChange::DropTrigger { name } => name == DDL_TRIGGER,
        DdlChange::CreateSink { .. } => false,
    }
}

fn print_refused_ddl_state(database: &Database) -> Result<(), String> {
    const CONTACT_TABLE: &str = "work_node_contacts";
    let table_names = database.table_names();
    let canonical = Database::open_memory();
    contextdb_server::work_ledger::install_node_contacts_schema(&canonical)
        .map_err(|_| "cannot construct canonical transport schema".to_string())?;
    let canonical_changes = canonical.changes_since(contextdb_core::Lsn(0));
    let canonical_meta = canonical
        .table_meta(CONTACT_TABLE)
        .ok_or_else(|| "canonical transport schema has no table metadata".to_string())?;

    let changes = database.changes_since(contextdb_core::Lsn(0));
    validate_refused_ddl_state(
        &table_names,
        database.list_triggers().len(),
        database.table_meta(CONTACT_TABLE).as_ref(),
        &changes,
        &canonical_meta,
        &canonical_changes.ddl,
    )?;
    println!("{}", json!({ "event": "ddl_refusal_transport_only" }));
    std::io::stdout()
        .flush()
        .map_err(|_| "stdout failed".to_string())
}

fn validate_refused_ddl_state(
    table_names: &[String],
    trigger_count: usize,
    table_meta: Option<&contextdb_core::TableMeta>,
    changes: &ChangeSet,
    canonical_meta: &contextdb_core::TableMeta,
    canonical_ddl: &[DdlChange],
) -> Result<(), String> {
    const CONTACT_TABLE: &str = "work_node_contacts";
    if table_names.len() != 1 || table_names[0] != CONTACT_TABLE || trigger_count != 0 {
        return Err("refused DDL published non-transport schema".to_string());
    }
    if changes.ddl.len() != 1
        || changes.ddl_lsn.len() != 1
        || changes.ddl != canonical_ddl
        || table_meta != Some(canonical_meta)
        || changes.rows.iter().any(|row| row.table != CONTACT_TABLE)
        || !changes.edges.is_empty()
        || !changes.vectors.is_empty()
    {
        return Err("refused DDL published non-transport data".to_string());
    }
    Ok(())
}

async fn run_oversized_source(args: OversizedSourceArgs) -> Result<(), String> {
    if matches!(args.phase, OversizedPhase::InitializeHub) {
        return run_oversized_hub_initialization(&args);
    }
    if matches!(args.phase, OversizedPhase::InspectHub) {
        return run_oversized_hub_inspection(&args);
    }

    let identity_path = args
        .identity
        .as_ref()
        .ok_or_else(|| "oversized source phase requires --identity".to_string())?;
    let ticket_file = args
        .ticket_file
        .as_ref()
        .ok_or_else(|| "oversized source phase requires --ticket-file".to_string())?;
    let ticket = read_ticket(ticket_file)?;
    let database = Arc::new(
        Database::open(&args.db)
            .map_err(|_| "cannot open oversized source database".to_string())?,
    );
    let source_identity = FabricIdentity::load_or_generate(identity_path)
        .map_err(|_| "cannot load oversized source fabric identity".to_string())?;
    let source_node_id = source_identity.node_id();
    let dial = peer_dial_spec(&ticket, identity_path);
    let client = SyncClient::new(
        database.clone(),
        &dial,
        TenantId::from(args.tenant_id.as_str()),
    );
    if matches!(args.phase, OversizedPhase::BootstrapAndSeed) {
        let fixture_table = match args.fixture {
            RequestFixture::Ordinary => "smoke_ordinary",
            RequestFixture::OversizedDependency | RequestFixture::FittingDependency => {
                "smoke_parents"
            }
        };
        if database.table_meta(fixture_table).is_some() {
            return Err("bootstrap-and-seed requires a fresh request fixture".to_string());
        }
        match args.fixture {
            RequestFixture::Ordinary => declare_ordinary_schema(&database)?,
            RequestFixture::OversizedDependency | RequestFixture::FittingDependency => {
                declare_oversized_schema(&database)?
            }
        }
        client
            .push()
            .await
            .map_err(|_| "request fixture schema bootstrap push failed".to_string())?;
        let body_bytes = match args.fixture {
            RequestFixture::OversizedDependency => {
                seed_dependency_unit(&database, OVERSIZED_BODY_BYTES)?;
                OVERSIZED_BODY_BYTES
            }
            RequestFixture::FittingDependency => {
                seed_dependency_unit(&database, FITTING_BODY_BYTES)?;
                FITTING_BODY_BYTES
            }
            RequestFixture::Ordinary => {
                seed_ordinary_unit(&database)?;
                0
            }
        };
        let source_incarnation = database
            .production_smoke_sync_incarnation(&TenantId::from(args.tenant_id.as_str()))
            .map_err(|_| "cannot inspect oversized source incarnation".to_string())?;
        println!(
            "{}",
            json!({
                "event": "request_fixture_prepared",
                "fixture": request_fixture_name(args.fixture),
                "source_lsn": database.current_lsn().0,
                "push_watermark": client.push_watermark().0,
                "body_bytes": body_bytes,
                "parent_id": PARENT_ID,
                "child_id": CHILD_ID,
                "source_node_id": source_node_id,
                "source_incarnation": source_incarnation.to_hex(),
            })
        );
        client.shutdown().await;
        database
            .close()
            .map_err(|_| "cannot close oversized source database".to_string())?;
        return Ok(());
    }
    let fixture_table = match args.fixture {
        RequestFixture::Ordinary => "smoke_ordinary",
        RequestFixture::OversizedDependency | RequestFixture::FittingDependency => "smoke_parents",
    };
    if database.table_meta(fixture_table).is_none() {
        return Err("push-existing requires a prepared request fixture".to_string());
    }
    let source_lsn = database.current_lsn().0;
    let push_watermark_before = client.push_watermark().0;
    println!(
        "{}",
        json!({
            "event": "oversized_push_started",
            "fixture": request_fixture_name(args.fixture),
            "phase": "push_existing",
            "source_lsn": source_lsn,
            "push_watermark_before": push_watermark_before,
            "body_bytes": match args.fixture {
                RequestFixture::OversizedDependency => OVERSIZED_BODY_BYTES,
                RequestFixture::FittingDependency => FITTING_BODY_BYTES,
                RequestFixture::Ordinary => 0,
            },
            "parent_id": PARENT_ID,
            "child_id": CHILD_ID,
        })
    );
    std::io::stdout().flush().map_err(|_| "stdout failed")?;
    let result = client.push().await;
    match result {
        Ok(result) => println!(
            "{}",
            json!({
                "event": "oversized_push_confirmed",
                "source_lsn": source_lsn,
                "push_watermark_after": client.push_watermark().0,
                "hub_lsn": result.new_lsn.0,
                "applied_rows": result.applied_rows,
            })
        ),
        Err(_) => {
            println!(
                "{}",
                json!({
                    "event": "oversized_push_unconfirmed",
                    "source_lsn": source_lsn,
                    "push_watermark_after": client.push_watermark().0,
                })
            );
            std::io::stdout().flush().map_err(|_| "stdout failed")?;
            client.shutdown().await;
            database
                .close()
                .map_err(|_| "cannot close oversized source database".to_string())?;
            return Err("oversized push was not confirmed".to_string());
        }
    }
    client.shutdown().await;
    database
        .close()
        .map_err(|_| "cannot close oversized source database".to_string())?;
    Ok(())
}

fn run_oversized_hub_initialization(args: &OversizedSourceArgs) -> Result<(), String> {
    let tenant_id = TenantId::from(args.tenant_id.as_str());
    let database = Database::open(&args.db)
        .map_err(|_| "cannot open stopped hub database for initialization".to_string())?;
    let hub_incarnation = database
        .production_smoke_sync_incarnation(&tenant_id)
        .map_err(|_| "cannot initialize hub sync incarnation".to_string())?;
    database
        .close()
        .map_err(|_| "cannot close stopped hub database after initialization".to_string())?;
    println!(
        "{}",
        json!({
            "event": "oversized_hub_initialized",
            "tenant_id": args.tenant_id,
            "hub_incarnation": hub_incarnation.to_hex(),
        })
    );
    Ok(())
}

fn run_oversized_hub_inspection(args: &OversizedSourceArgs) -> Result<(), String> {
    let source_node_id = args
        .source_node_id
        .as_deref()
        .ok_or_else(|| "inspect-hub requires --source-node-id".to_string())?;
    let source_incarnation_hex = args
        .source_incarnation
        .as_deref()
        .ok_or_else(|| "inspect-hub requires --source-incarnation".to_string())?;
    let source_incarnation = Incarnation::from_hex(source_incarnation_hex)
        .ok_or_else(|| "inspect-hub source incarnation is not valid hex".to_string())?;
    let tenant_id = TenantId::from(args.tenant_id.as_str());
    let database = Database::open(&args.db)
        .map_err(|_| "cannot open stopped hub database for inspection".to_string())?;
    let (push_watermark, pull_watermark) = database
        .persisted_sync_watermarks(&tenant_id)
        .map_err(|_| "cannot inspect hub sync watermarks".to_string())?;
    let pending_push_confirmation = database
        .persisted_sync_pending_push_confirmation(&tenant_id)
        .map_err(|_| "cannot inspect hub pending push confirmation".to_string())?;
    let pull_cursor = database
        .persisted_sync_pull_cursor(&tenant_id)
        .map_err(|_| "cannot inspect hub pull cursor".to_string())?;
    let applied_push_watermark = database
        .persisted_sync_applied_push_watermark(&tenant_id)
        .map_err(|_| "cannot inspect hub applied-push watermark".to_string())?;
    let source_receipt = database
        .persisted_sync_applied_push_watermark_for_node_incarnation(
            &tenant_id,
            source_node_id,
            source_incarnation,
        )
        .map_err(|_| "cannot inspect exact source receipt".to_string())?;
    let report = json!({
        "event": "oversized_hub_progress",
        "tenant_id": args.tenant_id,
        "push_watermark": push_watermark.0,
        "pull_watermark": pull_watermark.0,
        "pending_push_confirmation": pending_push_confirmation.map(|lsn| lsn.0),
        "pull_cursor": pull_cursor.map(|(source, lsn)| json!({
            "source_incarnation": source.to_hex(),
            "lsn": lsn.0,
        })),
        "applied_push_watermark": applied_push_watermark.map(|lsn| lsn.0),
        "source_node_id": source_node_id,
        "source_incarnation": source_incarnation.to_hex(),
        "source_receipt": source_receipt.map(|lsn| lsn.0),
    });
    database
        .close()
        .map_err(|_| "cannot close stopped hub database after inspection".to_string())?;
    println!("{report}");
    Ok(())
}

fn declare_oversized_schema(database: &Database) -> Result<(), String> {
    let empty = HashMap::new();
    database
        .execute(
            "CREATE TABLE smoke_parents (id UUID PRIMARY KEY, body TEXT) \
             SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
            &empty,
        )
        .map_err(|_| "oversized parent schema failed")?;
    database
        .execute(
            "CREATE TABLE smoke_children (id UUID PRIMARY KEY, parent_id UUID REFERENCES smoke_parents(id), body TEXT) \
             SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
            &empty,
        )
        .map_err(|_| "oversized child schema failed")?;
    Ok(())
}

fn declare_ordinary_schema(database: &Database) -> Result<(), String> {
    database
        .execute(
            "CREATE TABLE smoke_ordinary (id UUID PRIMARY KEY, body TEXT) \
             SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
            &HashMap::new(),
        )
        .map_err(|_| "ordinary schema failed")?;
    Ok(())
}

fn seed_dependency_unit(database: &Database, body_bytes: usize) -> Result<(), String> {
    let parent_id = Uuid::parse_str(PARENT_ID).expect("fixed parent UUID");
    let child_id = Uuid::parse_str(CHILD_ID).expect("fixed child UUID");
    database
        .execute(
            "INSERT INTO smoke_parents (id, body) VALUES ($id, $body)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(parent_id)),
                (
                    "body".to_string(),
                    Value::Text("decision-before-evidence".to_string()),
                ),
            ]),
        )
        .map_err(|_| "oversized parent insert failed")?;
    database
        .execute(
            "INSERT INTO smoke_children (id, parent_id, body) VALUES ($id, $parent_id, $body)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(child_id)),
                ("parent_id".to_string(), Value::Uuid(parent_id)),
                ("body".to_string(), Value::Text("outcome".to_string())),
            ]),
        )
        .map_err(|_| "oversized child insert failed")?;
    database
        .execute(
            "UPDATE smoke_parents SET body = $body WHERE id = $id",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(parent_id)),
                ("body".to_string(), Value::Text("x".repeat(body_bytes))),
            ]),
        )
        .map_err(|_| "oversized parent update failed")?;
    Ok(())
}

fn seed_ordinary_unit(database: &Database) -> Result<(), String> {
    database
        .execute(
            "INSERT INTO smoke_ordinary (id, body) VALUES ($id, $body)",
            &HashMap::from([
                (
                    "id".to_string(),
                    Value::Uuid(Uuid::parse_str(PARENT_ID).expect("fixed ordinary UUID")),
                ),
                (
                    "body".to_string(),
                    Value::Text("unrelated-ordinary-memory".to_string()),
                ),
            ]),
        )
        .map_err(|_| "ordinary row insert failed")?;
    Ok(())
}

fn request_fixture_name(fixture: RequestFixture) -> &'static str {
    match fixture {
        RequestFixture::OversizedDependency => "oversized_dependency",
        RequestFixture::FittingDependency => "fitting_dependency",
        RequestFixture::Ordinary => "ordinary",
    }
}

fn ddl_kind(change: &DdlChange) -> &'static str {
    match change {
        DdlChange::CreateTable { name, .. } if name == DDL_TABLE => "create_table",
        DdlChange::CreateTrigger { name, table, .. }
        | DdlChange::CreateTriggerIncludingSync { name, table, .. }
            if name == DDL_TRIGGER && table == DDL_TABLE =>
        {
            "create_trigger"
        }
        DdlChange::AlterTable { name, .. } if name == DDL_TABLE => "alter_table",
        _ => "unexpected",
    }
}

fn validate_path_parent(path: &Path) -> Result<(), String> {
    let parent = path
        .parent()
        .filter(|parent| parent.is_dir())
        .ok_or_else(|| "verifier path parent does not exist".to_string())?;
    if parent.as_os_str().is_empty() {
        return Err("verifier paths must have an explicit parent".to_string());
    }
    Ok(())
}

fn write_ticket(path: &Path, ticket: &str) -> Result<(), String> {
    use std::fs::OpenOptions;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

        let open = |create_new| {
            let mut options = OpenOptions::new();
            options
                .write(true)
                .create_new(create_new)
                .custom_flags(libc::O_NOFOLLOW)
                .mode(0o600);
            options.open(path)
        };
        let mut file = match open(true) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => open(false)
                .map_err(|_| "cannot securely replace verifier ticket file".to_string())?,
            Err(_) => return Err("cannot create verifier ticket file".to_string()),
        };
        let metadata = file
            .metadata()
            .map_err(|_| "cannot inspect verifier ticket file".to_string())?;
        if !metadata.file_type().is_file()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.nlink() != 1
        {
            return Err(
                "verifier ticket path must be one regular file owned by this user".to_string(),
            );
        }
        if unsafe { libc::fchmod(file.as_raw_fd(), 0o600) } != 0 {
            return Err("cannot make verifier ticket file private".to_string());
        }
        file.set_len(0)
            .map_err(|_| "cannot truncate verifier ticket file".to_string())?;
        file.write_all(ticket.as_bytes())
            .and_then(|_| file.sync_all())
            .map_err(|_| "cannot durably write verifier ticket file".to_string())
    }
    #[cfg(not(unix))]
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|_| "cannot create verifier ticket file".to_string())?;
        file.write_all(ticket.as_bytes())
            .and_then(|_| file.sync_all())
            .map_err(|_| "cannot durably write verifier ticket file".to_string())
    }
}

fn read_ticket(path: &Path) -> Result<String, String> {
    let ticket = std::fs::read_to_string(path)
        .map_err(|_| "cannot read verifier ticket file".to_string())?;
    let ticket = ticket.trim().to_string();
    if ticket.is_empty() {
        return Err("verifier ticket file is empty".to_string());
    }
    Ok(ticket)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod ddl_vector_tests {
    use super::*;
    use contextdb_core::Lsn;

    fn fixture_vector(lsn: Lsn) -> ChangeSet {
        ChangeSet {
            ddl: vec![
                DdlChange::CreateTable {
                    name: DDL_TABLE.to_string(),
                    columns: vec![("id".to_string(), "UUID PRIMARY KEY".to_string())],
                    constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                },
                DdlChange::CreateTrigger {
                    name: DDL_TRIGGER.to_string(),
                    table: DDL_TABLE.to_string(),
                    on_events: vec!["INSERT".to_string()],
                },
                DdlChange::AlterTable {
                    name: DDL_TABLE.to_string(),
                    columns: vec![
                        ("id".to_string(), "UUID PRIMARY KEY".to_string()),
                        ("detail".to_string(), "TEXT".to_string()),
                    ],
                    constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                },
            ],
            ddl_lsn: vec![lsn; 3],
            ..ChangeSet::default()
        }
    }

    fn unrelated_hub_table() -> DdlChange {
        DdlChange::CreateTable {
            name: "work_node_contacts".to_string(),
            columns: vec![("node_id".to_string(), "TEXT PRIMARY KEY".to_string())],
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        }
    }

    #[test]
    fn exact_fixture_vector_ignores_unrelated_hub_schema() {
        let mut changes = fixture_vector(Lsn(4));
        changes.ddl.insert(0, unrelated_hub_table());
        changes.ddl_lsn.insert(0, Lsn(1));

        let (kinds, lsn) = exact_fixture_ddl_vector(&changes).unwrap();

        assert_eq!(kinds, ["create_table", "create_trigger", "alter_table"]);
        assert_eq!(lsn, Lsn(4));
    }

    #[test]
    fn exact_fixture_vector_rejects_extra_fixture_ddl() {
        let mut changes = fixture_vector(Lsn(4));
        changes.ddl.push(DdlChange::CreateIndex {
            table: DDL_TABLE.to_string(),
            name: "unexpected_fixture_index".to_string(),
            columns: vec![("detail".to_string(), contextdb_core::SortDirection::Asc)],
        });
        changes.ddl_lsn.push(Lsn(4));

        assert!(exact_fixture_ddl_vector(&changes).is_err());
    }

    #[test]
    fn exact_fixture_vector_rejects_split_receiver_lsns() {
        let mut changes = fixture_vector(Lsn(4));
        changes.ddl_lsn[2] = Lsn(5);

        assert!(exact_fixture_ddl_vector(&changes).is_err());
    }

    #[test]
    fn refused_ddl_state_allows_only_transport_contact_bookkeeping() {
        let database = Database::open_memory();
        database
            .execute(
                "CREATE TABLE work_node_contacts (node_id TEXT PRIMARY KEY, last_contact_ms TIMESTAMP NOT NULL) HISTORY CURRENT ONLY SYNC OFF",
                &HashMap::new(),
            )
            .unwrap();
        print_refused_ddl_state(&database).unwrap();

        database
            .execute(
                "CREATE TABLE receiver_injected_table (id UUID PRIMARY KEY)",
                &HashMap::new(),
            )
            .unwrap();
        assert!(print_refused_ddl_state(&database).is_err());
    }

    #[test]
    fn refused_ddl_state_rejects_empty_database() {
        assert!(print_refused_ddl_state(&Database::open_memory()).is_err());
    }

    #[test]
    fn refused_ddl_state_rejects_duplicate_transport_ddl() {
        let database = Database::open_memory();
        database
            .execute(
                "CREATE TABLE work_node_contacts (node_id TEXT PRIMARY KEY, last_contact_ms TIMESTAMP NOT NULL) HISTORY CURRENT ONLY SYNC OFF",
                &HashMap::new(),
            )
            .unwrap();
        database
            .execute("DROP TABLE work_node_contacts", &HashMap::new())
            .unwrap();
        database
            .execute(
                "CREATE TABLE work_node_contacts (node_id TEXT PRIMARY KEY, last_contact_ms TIMESTAMP NOT NULL) HISTORY CURRENT ONLY SYNC OFF",
                &HashMap::new(),
            )
            .unwrap();

        assert!(print_refused_ddl_state(&database).is_err());
    }

    #[test]
    fn refused_ddl_state_rejects_wrong_transport_shape() {
        let database = Database::open_memory();
        database
            .execute(
                "CREATE TABLE work_node_contacts (node_id TEXT PRIMARY KEY, last_contact_ms TIMESTAMP NOT NULL) HISTORY CURRENT ONLY SYNC OFF",
                &HashMap::new(),
            )
            .unwrap();
        let mut changes = database.changes_since(Lsn(0));
        let canonical_ddl = changes.ddl.clone();
        let canonical_meta = database.table_meta("work_node_contacts").unwrap();
        let DdlChange::CreateTable { columns, .. } = &mut changes.ddl[0] else {
            panic!("contact schema must begin with CREATE TABLE");
        };
        columns[0].1 = "INTEGER PRIMARY KEY".to_string();

        assert!(
            validate_refused_ddl_state(
                &database.table_names(),
                database.list_triggers().len(),
                Some(&canonical_meta),
                &changes,
                &canonical_meta,
                &canonical_ddl,
            )
            .is_err()
        );
    }
}
