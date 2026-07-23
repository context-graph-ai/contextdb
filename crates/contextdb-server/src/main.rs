use clap::Parser;
use contextdb_engine::Database;
use contextdb_engine::sync_types::ConflictPolicies;
use contextdb_server::SyncServer;
use contextdb_server::exit_codes::{EXIT_ERROR, EXIT_USAGE};
use contextdb_server::protocol::PROTOCOL_VERSION;
use contextdb_server::transport::iroh::{EndpointSpec, IrohServer};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Parser)]
#[command(
    name = "contextdb-server",
    version = concat!(env!("CARGO_PKG_VERSION"), " protocol_version=5"),
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
    /// DEPRECATED: broker URL for the retained NATS adapter (requires the
    /// `nats` cargo feature). Use --sync-endpoint instead.
    #[arg(long, env = "CONTEXTDB_NATS_URL")]
    nats_url: Option<String>,
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

    let endpoint_spec = match (&args.sync_endpoint, &args.nats_url) {
        (Some(spec), _) => spec.clone(),
        (None, Some(broker_url)) => {
            tracing::warn!(
                "--nats-url is deprecated; the NATS adapter is retained off-default only. \
                 Use --sync-endpoint."
            );
            broker_url.clone()
        }
        (None, None) => default_endpoint_spec(&args.db_path),
    };

    // Whether this endpoint is one this build can bind and serve. Decided
    // BEFORE the database is opened, because the flag combinations it rules out
    // are usage errors, and exit 2 promises that nothing was attempted — which
    // has to include not creating a database file on a fresh path for a command
    // that was never going to run.
    let binds_sync_endpoint = EndpointSpec::parse(&endpoint_spec).is_some();
    if !binds_sync_endpoint {
        if args.show_ticket || args.ticket_file.is_some() {
            return Err(UsageError(
                "tickets exist only for sync endpoints, not broker URLs".to_string(),
            )
            .into());
        }
        #[cfg(not(feature = "nats"))]
        {
            return Err(UsageError(format!(
                "{endpoint_spec} is a broker URL, but this build carries no deprecated NATS \
                 adapter; pass a sync-endpoint ticket/spec, or rebuild with the `nats` cargo \
                 feature"
            ))
            .into());
        }
    }

    let db = if args.db_path == ":memory:" {
        Arc::new(Database::open_memory())
    } else {
        Arc::new(Database::open(std::path::Path::new(&args.db_path))?)
    };
    // The hub honors each table's DECLARED conflict policy (carried on its
    // meta); this uniform default only decides a table that declared none, and
    // it is the engine's non-overwriting default, agreeing with Database::open.
    let policies = ConflictPolicies::uniform(contextdb_core::DEFAULT_CONFLICT_POLICY);

    // For a dial-by-key endpoint, bind eagerly so the enrollment ticket is
    // available up front (logged, and written to --ticket-file when asked).
    let server = if binds_sync_endpoint {
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
            "contextdb-cli <client-db-path> --sync-endpoint {ticket} --tenant-id {}",
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
        SyncServer::with_transport(
            db,
            endpoint.transport(),
            contextdb_core::TenantId::from(args.tenant_id.as_str()),
            policies,
        )
    } else {
        // The ticket-flag and build-support checks already ran, above the
        // database open.
        if args.json {
            eprintln!(
                "Note: --json emits nothing for a broker URL — it carries no enrollment ticket; use a sync endpoint."
            );
        }
        #[cfg(not(feature = "nats"))]
        {
            unreachable!(
                "a broker URL is refused before the database is opened when this build carries no NATS adapter"
            )
        }
        #[cfg(feature = "nats")]
        SyncServer::new(
            db,
            &endpoint_spec,
            contextdb_core::TenantId::from(args.tenant_id.as_str()),
            policies,
        )
    };

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
