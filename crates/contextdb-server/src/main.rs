use clap::Parser;
use contextdb_engine::Database;
use contextdb_server::SyncServer;
use contextdb_server::exit_codes::{EXIT_ERROR, EXIT_USAGE};
use contextdb_server::protocol::PROTOCOL_VERSION;
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
    #[arg(long, env = "CONTEXTDB_SYNC_ENDPOINT")]
    sync_endpoint: Option<String>,
    #[arg(long, env = "CONTEXTDB_TENANT_ID")]
    tenant_id: String,
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
}

fn default_endpoint_spec(db_path: &str) -> String {
    let identity = if db_path == ":memory:" {
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

fn main() {
    if let Err(err) = run() {
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
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    tracing::info!(
        protocol_version = PROTOCOL_VERSION,
        "contextdb-server starting"
    );

    let endpoint_spec = args
        .sync_endpoint
        .clone()
        .unwrap_or_else(|| default_endpoint_spec(&args.db_path));

    // Validate the bind specification before opening the database. Exit 2
    // promises that an invalid invocation creates no database file.
    if EndpointSpec::parse(&endpoint_spec).is_none() {
        return Err(UsageError(format!(
            "invalid --sync-endpoint bind specification: {endpoint_spec}"
        ))
        .into());
    }

    let db = if args.db_path == ":memory:" {
        Arc::new(Database::open_memory())
    } else {
        Arc::new(Database::open(std::path::Path::new(&args.db_path))?)
    };
    // Bind eagerly so the enrollment ticket is
    // available up front (logged, and written to --ticket-file when asked).
    let endpoint = IrohServer::bind(&endpoint_spec).await?;
    let ticket = endpoint.ticket();
    tracing::info!(
        ticket = %ticket,
        node_id = %endpoint.node_id(),
        "sync endpoint bound; enroll edges with this ticket"
    );
    if let Some(path) = &args.ticket_file {
        std::fs::write(path, &ticket)?;
    }
    let dial_command = format!(
        "contextdb <client-db-path> --sync-endpoint {ticket} --tenant-id {}",
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
