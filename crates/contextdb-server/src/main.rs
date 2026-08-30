use clap::Parser;
use clap::error::{ContextKind, ContextValue, ErrorKind};
use contextdb_core::read_contract::{OwnerServingReason, OwnerServingState};
use contextdb_engine::{Database, DatabaseOpenOptions, OwnerReadConfig};
use contextdb_server::SyncServer;
use contextdb_server::exit_codes::{EXIT_ERROR, EXIT_USAGE};
use contextdb_server::owner_read_options::OwnerReadOptions;
use contextdb_server::protocol::PROTOCOL_VERSION;
use contextdb_server::transport::ServerResourcePolicy;
use contextdb_server::transport::iroh::{EndpointSpec, IrohServer};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// The `--version` disclosure, computed from the real `PROTOCOL_VERSION`
/// constant rather than a hand-maintained literal — a hardcoded string here
/// drifted silently from the real wire version once already (the v5→v6
/// bump), which is exactly the failure this function exists to make
/// structurally impossible: there is no second number to forget to update.
/// `clap::Command::version` requires a `&'static str`; leaking is the
/// standard way to produce one from a value only known at runtime, and is
/// harmless here — this runs at most once per process, for a CLI that
/// either exits immediately (`--version`) or serves for the life of the
/// process.
fn disclosed_version() -> &'static str {
    Box::leak(
        format!(
            "{} protocol_version={PROTOCOL_VERSION}",
            env!("CARGO_PKG_VERSION")
        )
        .into_boxed_str(),
    )
}

#[derive(Parser)]
#[command(
    name = "contextdb-server",
    version = disclosed_version(),
    after_help = "EXAMPLES:\n  \
        contextdb-server --db-path mydata.db --tenant-id acme\n    \
            Serve mydata.db, binding a dial-by-key sync endpoint and printing an\n    \
            enrollment ticket plus the exact client command to connect with it.\n\n  \
        contextdb-server --db-path :memory: --tenant-id acme --show-ticket\n    \
            Print the enrollment ticket and exit without serving (scripting).\n\n  \
        contextdb-server --db-path mydata.db --tenant-id acme --ticket-file ticket.txt\n    \
            Serve and also write the ticket to ticket.txt for other processes to read.\n\n  \
        contextdb-server --db-path mydata.db --tenant-id acme \\\n    \
            --sync-endpoint \"iroh:?identity=mydata.db.fabric-identity.key&port=4433\"\n    \
            Bind a stable port instead of an ephemeral one."
)]
struct Args {
    /// Database path (:memory: for in-memory)
    #[arg(long, env = "CONTEXTDB_DB_PATH", default_value = ":memory:")]
    db_path: String,
    /// Sync endpoint to serve on. Default form `iroh:?identity=<key-file>`
    /// binds a dial-by-key endpoint with no relay and no third-party contact;
    /// add `&port=<u16>` for a stable port and `&relay=<url|n0>` to opt into
    /// a relay. When omitted, an identity next to the database file is used.
    #[arg(long)]
    sync_endpoint: Option<String>,
    #[arg(long)]
    tenant_id: String,
    /// Maximum bytes retained for durable oversized-response staging.
    #[arg(long, allow_hyphen_values = true)]
    response_staging_bytes: Option<u64>,
    /// Maximum simultaneous connections before route admission.
    #[arg(long, default_value_t = 128, allow_hyphen_values = true)]
    pre_admission_connections: usize,
    /// Maximum aggregate request bytes reserved before route admission.
    #[arg(
        long,
        default_value_t = 64 * 1024 * 1024,
        allow_hyphen_values = true
    )]
    pre_admission_bytes: usize,
    /// Maximum milliseconds without request-body read progress.
    #[arg(long, default_value_t = 30_000, allow_hyphen_values = true)]
    request_read_idle_ms: u64,
    /// Write the endpoint's enrollment ticket to this file once bound, so
    /// scripts and operators can pick it up without parsing logs.
    #[arg(long)]
    ticket_file: Option<std::path::PathBuf>,
    /// Print the endpoint's enrollment ticket and exit without serving.
    #[arg(long)]
    show_ticket: bool,
    /// Emit operational output (enrollment ticket + dial command) as a single
    /// JSON object to stdout, for scripts and agents enrolling a machine.
    #[arg(long)]
    json: bool,

    /// The owner-read policy, defined once beside the writer's so an operator
    /// states the same thing to either command in the same words.
    #[command(flatten)]
    owner_read: OwnerReadOptions,
}

const RESOURCE_POLICY_ARGUMENTS: [(&str, &str); 4] = [
    ("response_staging_bytes", "--response-staging-bytes"),
    ("pre_admission_connections", "--pre-admission-connections"),
    ("pre_admission_bytes", "--pre-admission-bytes"),
    ("request_read_idle_ms", "--request-read-idle-ms"),
];

fn resource_policy_flag_from_error(error: &clap::Error) -> Option<&'static str> {
    let ContextValue::String(argument) = error.get(ContextKind::InvalidArg)? else {
        return None;
    };
    RESOURCE_POLICY_ARGUMENTS
        .into_iter()
        .find_map(|(id, flag)| {
            let displays_flag = argument.strip_prefix(flag).is_some_and(|suffix| {
                suffix.is_empty()
                    || suffix.starts_with(' ')
                    || suffix.starts_with('=')
                    || suffix.starts_with('<')
            });
            (argument == id || displays_flag).then_some(flag)
        })
}

fn parse_args_or_exit() -> Args {
    match Args::try_parse() {
        Ok(args) => args,
        Err(error) if error.kind() == ErrorKind::ValueValidation => {
            if let Some(flag) = resource_policy_flag_from_error(&error) {
                eprintln!("Error: invalid value for {flag}: expected a positive whole number");
                std::process::exit(EXIT_USAGE);
            }
            error.exit();
        }
        Err(error) => error.exit(),
    }
}

impl Args {
    fn server_resource_policy(&self) -> ServerResourcePolicy {
        ServerResourcePolicy {
            response_staging_bytes: self.response_staging_bytes,
            pre_admission_connections: self.pre_admission_connections,
            pre_admission_bytes: self.pre_admission_bytes,
            request_read_idle_ms: self.request_read_idle_ms,
        }
    }
}

/// The path that asks for an ephemeral store with no file behind it.
const MEMORY_PATH: &str = ":memory:";

/// Say once, at startup, when this server is not serving local reads.
///
/// A channel it could not place never fails the open -- the server still syncs,
/// which is most of what it is for -- so the only thing an operator would
/// otherwise have to discover by getting no answer from a reader is stated here
/// instead. Exactly one line: a fact, not an alarm that repeats. Disablement is
/// not reported, because an operator who passed the flag already knows.
fn report_owner_read_startup(db: &Database) {
    let status = db.owner_read_status();
    if status.state != OwnerServingState::NotServing {
        return;
    }
    let detail = match &status.reason {
        Some(OwnerServingReason::StartupFailure(detail)) => detail.clone(),
        _ => "the local read channel could not be placed".to_string(),
    };
    eprintln!(
        "Warning: this server is not serving local reads: {detail}; \
         pass --owner-read-runtime-dir with a directory it can create the channel in"
    );
}

fn default_endpoint_spec(db_path: &str) -> String {
    let identity = if db_path == MEMORY_PATH {
        // Per-process: two in-memory servers on one host must not share an
        // identity (or its sticky port).
        std::env::temp_dir().join(format!(
            "contextdb-ephemeral-identity-{}.key",
            std::process::id()
        ))
    } else {
        std::path::PathBuf::from(format!("{db_path}.fabric-identity.key"))
    };
    format!("iroh:?identity={}", identity.display())
}

/// A failure of the INVOCATION rather than of the run: the command line asked
/// for something this build or this endpoint cannot do, and nothing was
/// attempted. Carried as a distinct type so `main` can report it with the usage
/// exit code, the same one `clap` uses for the errors it raises itself.
#[derive(Debug)]
struct UsageError(String);

impl std::fmt::Display for UsageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for UsageError {}

async fn bind_sync_endpoint(
    endpoint_spec: &str,
    resource_policy: ServerResourcePolicy,
) -> contextdb_server::transport::TransportResult<IrohServer> {
    IrohServer::bind_with_resource_policy(endpoint_spec, resource_policy).await
}

fn main() {
    let args = parse_args_or_exit();
    if let Err(err) = run(args) {
        eprintln!("Error: {err}");
        let code = if err.is::<UsageError>() {
            EXIT_USAGE
        } else {
            EXIT_ERROR
        };
        std::process::exit(code);
    }
}

#[tokio::main]
async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    tracing::info!(
        protocol_version = PROTOCOL_VERSION,
        "contextdb-server starting"
    );

    let resource_policy = args.server_resource_policy();
    resource_policy
        .validate()
        .map_err(|error| UsageError(error.to_string()))?;

    // Settled before anything is opened: exit 2 promises the invocation was
    // never attempted, and that has to include not creating a store for a
    // server that was never going to run.
    let owner = args.owner_read.resolve();
    if args.db_path == MEMORY_PATH && args.owner_read.declared_any() {
        return Err(UsageError(
            "an in-memory database has no read route, no owner channel, and no ceilings to \
             declare, so --owner-read-*, --no-owner-reads, and --owner-read-runtime-dir are \
             invalid with `:memory:`"
                .to_string(),
        )
        .into());
    }
    if let Some(violation) = owner.violation() {
        return Err(UsageError(violation).into());
    }

    let endpoint_spec = args
        .sync_endpoint
        .clone()
        .unwrap_or_else(|| default_endpoint_spec(&args.db_path));

    // Validate the bind specification before opening the database. Exit 2
    // promises that an invalid invocation creates no database file.
    match EndpointSpec::parse_detailed(&endpoint_spec) {
        Ok(Some(parsed)) if parsed.dial_ticket().is_none() => {}
        Ok(_) => {
            return Err(
                UsageError("invalid --sync-endpoint bind specification".to_string()).into(),
            );
        }
        Err(message) => {
            return Err(UsageError(format!(
                "invalid --sync-endpoint bind specification: {message}"
            ))
            .into());
        }
    }

    let db = if args.db_path == MEMORY_PATH {
        Arc::new(Database::open_memory())
    } else {
        // The same writable door the writer uses, so the server advertises the
        // owner-read policy it was asked for rather than the shipped default.
        Arc::new(Database::open_with_options(
            std::path::Path::new(&args.db_path),
            DatabaseOpenOptions {
                owner_reads: OwnerReadConfig {
                    enabled: owner.enabled,
                    limits: owner.limits,
                    timeouts: owner.timeouts,
                    runtime_dir: owner.runtime_dir.clone(),
                    ..OwnerReadConfig::default()
                },
                ..DatabaseOpenOptions::default()
            },
        )?)
    };
    report_owner_read_startup(&db);
    // Bind eagerly so the enrollment ticket is
    // available up front (logged, and written to --ticket-file when asked).
    let endpoint = bind_sync_endpoint(&endpoint_spec, resource_policy).await?;
    let ticket = endpoint.ticket();
    tracing::info!(
        ticket = %ticket,
        node_id = %endpoint.node_id(),
        "sync endpoint bound; enroll edges with this ticket"
    );
    if let Some(path) = &args.ticket_file {
        std::fs::write(path, &ticket)?;
    }
    // `--write` is part of the command, not decoration: sync is a writer
    // capability, so a bare `contextdb <path> --sync-endpoint ... --tenant-id ...`
    // is refused at argument validation (exit 2) before anything runs. Printing
    // it without the flag would hand every operator a command that cannot work.
    let dial_command = format!(
        "contextdb <client-db-path> --write --sync-endpoint {ticket} --tenant-id {}",
        args.tenant_id
    );
    if args.json {
        // Machine channel: one stable JSON object carrying the ticket and
        // the exact dial command so an agent can enroll a machine without
        // scraping human text.
        let obj = serde_json::json!({
            "enrollment_ticket": ticket.to_string(),
            "tenant_id": args.tenant_id,
            "dial_command": dial_command,
            "endpoint": endpoint.node_id().to_string(),
        });
        println!(
            "{}",
            serde_json::to_string(&obj).expect("serialize enrollment object")
        );
    } else if args.show_ticket {
        // Bare ticket on stdout: script-friendly capture (non-JSON).
        println!("{ticket}");
    } else {
        // The enrollment ticket is product surface, not logging: print it
        // unconditionally so the documented "copy the ticket" flow works at
        // any log level.
        println!("enrollment ticket: {ticket}");
        println!("To connect a client, run:");
        println!("  {dial_command}");
    }
    if args.show_ticket {
        endpoint.close().await;
        return Ok(());
    }
    let server = SyncServer::new(
        db,
        &endpoint,
        contextdb_core::TenantId::from(args.tenant_id.as_str()),
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    let signal_shutdown = shutdown.clone();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        signal_shutdown.store(true, Ordering::SeqCst);
    });
    server.run_until(shutdown).await;
    server.db().close()?;
    Ok(())
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
mod tests {
    use super::*;

    fn parse_resource_args(endpoint: &str, values: [&str; 4]) -> Args {
        Args::try_parse_from([
            "contextdb-server",
            "--db-path",
            ":memory:",
            "--tenant-id",
            "acme",
            "--sync-endpoint",
            endpoint,
            "--response-staging-bytes",
            values[0],
            "--pre-admission-connections",
            values[1],
            "--pre-admission-bytes",
            values[2],
            "--request-read-idle-ms",
            values[3],
            "--show-ticket",
        ])
        .expect("valid server arguments")
    }

    #[test]
    fn omitted_resource_flags_build_shipped_defaults() {
        let args = Args::try_parse_from([
            "contextdb-server",
            "--db-path",
            ":memory:",
            "--tenant-id",
            "acme",
            "--show-ticket",
        ])
        .expect("server arguments with resource defaults");
        assert_eq!(
            args.server_resource_policy(),
            ServerResourcePolicy::default()
        );
    }

    #[test]
    fn top_level_flags_build_distinct_typed_resource_policies() {
        let first = parse_resource_args(
            "iroh:?identity=/tmp/first.key",
            ["7340032", "3", "2097152", "1234"],
        );
        let second = parse_resource_args(
            "iroh:?identity=/tmp/second.key",
            ["9437184", "5", "3145728", "2345"],
        );
        assert_eq!(
            first.server_resource_policy(),
            ServerResourcePolicy {
                response_staging_bytes: Some(7 * 1024 * 1024),
                pre_admission_connections: 3,
                pre_admission_bytes: 2 * 1024 * 1024,
                request_read_idle_ms: 1_234,
            }
        );
        assert_eq!(
            second.server_resource_policy(),
            ServerResourcePolicy {
                response_staging_bytes: Some(9 * 1024 * 1024),
                pre_admission_connections: 5,
                pre_admission_bytes: 3 * 1024 * 1024,
                request_read_idle_ms: 2_345,
            }
        );
    }

    #[tokio::test]
    async fn parsed_resource_policies_bind_through_the_startup_helper() {
        let root = tempfile::tempdir().expect("temporary identities");
        for (name, values) in [
            ("first", ["7340032", "3", "2097152", "1234"]),
            ("second", ["9437184", "5", "3145728", "2345"]),
        ] {
            let endpoint = format!("iroh:?identity={}", root.path().join(name).display());
            let args = parse_resource_args(&endpoint, values);
            let bound = bind_sync_endpoint(&endpoint, args.server_resource_policy())
                .await
                .expect("valid typed policy binds through the server startup helper");
            bound.close().await;
        }
    }

    #[tokio::test]
    async fn startup_helper_cannot_drop_invalid_typed_policy() {
        let root = tempfile::tempdir().expect("temporary identity directory");
        let identity = root.path().join("must-not-exist.key");
        let endpoint = format!("iroh:?identity={}", identity.display());
        let policy = ServerResourcePolicy {
            response_staging_bytes: Some(0),
            ..ServerResourcePolicy::default()
        };
        match bind_sync_endpoint(&endpoint, policy).await {
            Err(_) => {}
            Ok(bound) => {
                bound.close().await;
                panic!("invalid typed policy must reach and be refused by eager bind")
            }
        }
        assert!(
            !identity.exists(),
            "invalid typed policy must fail before identity creation"
        );
    }
}
