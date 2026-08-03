//! Fixed-scenario installed-release verifier for the production ticketed-Iroh
//! path. This binary exists only under `production-smoke-driver`; it is not a
//! product command and exposes no arbitrary SQL, plugin, transport, or fault
//! controls.

use clap::{Parser, Subcommand, ValueEnum};
use contextdb_core::{TenantId, Value};
use contextdb_engine::database::open_with_startup_limits;
use contextdb_engine::plugin::{CorePlugin, DatabasePlugin};
use contextdb_engine::sync_types::{ChangeSet, DdlChange};
use contextdb_engine::transport::iroh::{
    ProductionSmokeCheckpoint, ProductionSmokeGateKind, arm_production_smoke_gate,
};
use contextdb_engine::{Database, SyncClient, SyncServer};
use contextdb_server::{FabricIdentity, PeerEndpoint, peer_bind_spec, peer_dial_spec};
use serde_json::json;
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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
    identity: PathBuf,
    #[arg(long)]
    ticket_file: PathBuf,
    #[arg(long)]
    tenant_id: String,
    #[arg(long, value_enum)]
    phase: OversizedPhase,
    #[arg(long, value_enum, default_value_t = RequestFixture::OversizedDependency)]
    fixture: RequestFixture,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OversizedPhase {
    BootstrapAndSeed,
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
    if matches!(args.phase, DdlPhase::InspectLocal) {
        print_ddl_vector(&database, "local_ddl_vector")?;
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
        DdlPhase::InspectLocal => unreachable!("handled before transport setup"),
    }
    client.shutdown().await;
    database
        .close()
        .map_err(|_| "cannot close DDL source database".to_string())?;
    Ok(())
}

fn print_ddl_vector(database: &Database, event: &str) -> Result<(), String> {
    let authored = database.changes_since(contextdb_core::Lsn(0));
    let kinds = authored.ddl.iter().map(ddl_kind).collect::<Vec<_>>();
    if kinds != ["create_table", "create_trigger", "alter_table"]
        || authored.ddl_lsn.len() != 3
        || authored
            .ddl_lsn
            .iter()
            .any(|lsn| *lsn != authored.ddl_lsn[0])
    {
        return Err("DDL is not one exact same-LSN three-item vector".to_string());
    }
    println!(
        "{}",
        json!({
            "event": event,
            "order": kinds,
            "source_lsn": authored.ddl_lsn[0].0,
        })
    );
    std::io::stdout()
        .flush()
        .map_err(|_| "stdout failed".to_string())
}

async fn run_oversized_source(args: OversizedSourceArgs) -> Result<(), String> {
    let ticket = read_ticket(&args.ticket_file)?;
    let database = Arc::new(
        Database::open(&args.db)
            .map_err(|_| "cannot open oversized source database".to_string())?,
    );
    let dial = peer_dial_spec(&ticket, &args.identity);
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
        DdlChange::CreateTrigger { name, .. } if name == DDL_TRIGGER => "create_trigger",
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
        return file
            .write_all(ticket.as_bytes())
            .and_then(|_| file.sync_all())
            .map_err(|_| "cannot durably write verifier ticket file".to_string());
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
