use clap::{Parser, Subcommand};
use contextdb_core::read_contract::{ReadClientTimeouts, ReadLimits};
use contextdb_engine::{DatabaseOpenOptions, OwnerReadConfig, ReadSession, ReadSessionOptions};
use contextdb_server::owner_read_options::{OwnerConfiguration, OwnerReadOptions};
use std::io::IsTerminal;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

use contextdb_cli::{
    EXIT_ERROR, EXIT_INTERRUPTED_PUSH_UNCONFIRMED, EXIT_USAGE, ErrorClass, OutputOptions,
    ReadProgressReporter, Session, auto_sync, canonical_help_signatures,
    operational_command_discovery, run,
};
use contextdb_server::exit_codes::exit_code_for;

// The store-maintenance operations, in the one typed command tree so they are
// discoverable in ordinary `--help` rather than through a separate pre-parse
// path. Each carries its own argument tail verbatim: the operation's
// established contract owns its arguments, and this tree owns only the
// spelling that reaches it.
//
// The removed diagnosis spelling is present but HIDDEN, which is what lets it be
// rejected BY NAME instead of arriving as an unexplained stray argument. Hidden
// keeps it off every discovery surface -- `--help`, `help`, the registry -- while
// still letting someone who types the old word be told which word replaced it.
// These comments are NOT doc comments: clap renders an enum's doc comment into
// `--help`, where naming the removed word would put it back on the surface the
// contract took it off.
#[derive(Subcommand)]
enum Operation {
    /// Bring a legacy-format store forward in place, backing up the original first.
    Migrate {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        arguments: Vec<String>,
    },
    /// Recreate a wedged or corrupt store from scratch. Destructive; needs `--force`.
    Reset {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        arguments: Vec<String>,
    },
    /// Report a store's format and schema layout read-only, never modifying it.
    Diagnose {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        arguments: Vec<String>,
    },
    /// Publish a transactionally consistent, purge-fenced backup artifact.
    Snapshot {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        arguments: Vec<String>,
    },
    /// Read durable key and media state from a snapshot artifact or database file.
    Inspect {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        arguments: Vec<String>,
    },
    /// Force-gated whole-table authoritative erasure.
    Purge {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        arguments: Vec<String>,
    },
    #[command(hide = true)]
    Repair {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        arguments: Vec<String>,
    },
}

impl Operation {
    /// The spelling this operation was typed as. It comes from the same word
    /// the registry publishes, so the tree and the discovery list cannot drift.
    fn spelling(&self) -> &'static str {
        match self {
            Operation::Migrate { .. } => "migrate",
            Operation::Reset { .. } => "reset",
            Operation::Diagnose { .. } => "diagnose",
            Operation::Snapshot { .. } => "snapshot",
            Operation::Inspect { .. } => "inspect",
            Operation::Purge { .. } => "purge",
            Operation::Repair { .. } => "repair",
        }
    }

    fn arguments(&self) -> &[String] {
        match self {
            Operation::Migrate { arguments }
            | Operation::Reset { arguments }
            | Operation::Diagnose { arguments }
            | Operation::Snapshot { arguments }
            | Operation::Inspect { arguments }
            | Operation::Purge { arguments }
            | Operation::Repair { arguments } => arguments,
        }
    }
}

#[derive(Parser)]
#[command(
    name = "contextdb",
    version,
    after_help = "EXAMPLES:\n  \
        contextdb mydata.db\n    \
            Read an existing database: bounded, read-only, creates and changes nothing.\n\n  \
        contextdb mydata.db --write\n    \
            Open for writing, creating the database if it does not exist.\n\n  \
        echo \"SELECT * FROM decisions LIMIT 5\" | contextdb mydata.db\n    \
            Pipe one or more SQL statements in non-interactively and exit.\n\n  \
        contextdb :memory:\n    \
            Open a throwaway in-memory database (always writable).\n\n  \
        contextdb mydata.db --write --sync-endpoint <server-ticket> --tenant-id acme\n    \
            Open for writing and connect to a contextdb-server using the enrollment\n    \
            ticket it printed (see `contextdb-server --help`).\n\n  \
        In the shell: .help propagate\n    \
            Show the PROPAGATE DDL grammar with a worked example."
)]
struct Args {
    /// Database path (:memory: for in-memory). Use `.help vector` for vector index syntax.
    ///
    /// This is one of the two environment names the CLI reads at all, and the
    /// path on the command line always wins over it.
    #[arg(env = "CONTEXTDB_DB_PATH")]
    path: Option<String>,

    #[command(subcommand)]
    operation: Option<Operation>,

    /// Authorize creation, mutation, transactions, maintenance, and sync. Also
    /// starts this writer's local owner-reading service.
    #[arg(long)]
    write: bool,

    /// Machine output: stdout is JSON Lines, one document per statement or
    /// meta-command; errors and notices are JSON documents on stderr.
    #[arg(long)]
    json: bool,

    /// Sync endpoint: the server's enrollment ticket (paste it verbatim). Writer-only.
    #[arg(long)]
    sync_endpoint: Option<String>,

    /// Tenant ID for sync. Writer-only.
    #[arg(long)]
    tenant_id: Option<String>,

    /// Memory limit for the whole session (e.g. 4G, 512M).
    #[arg(long)]
    memory_limit: Option<String>,

    /// Disk limit for file-backed databases (e.g. 4G, 512M). Never authorizes writes.
    #[arg(long)]
    disk_limit: Option<String>,

    /// Debounce interval for background auto-sync pushes. Writer-only. [default: 500]
    #[arg(long, default_value_t = 500)]
    sync_debounce_ms: u64,

    /// Complete ordinary-result rows, and the most one cursor fetch may ask for. [default: 500]
    #[arg(long, value_name = "ROWS")]
    read_result_rows: Option<u64>,

    /// Canonical bytes of one complete result, metadata page, or metadata object. [default: 4 MiB]
    #[arg(long, value_name = "BYTES")]
    read_result_bytes: Option<u64>,

    /// Items examined by one read or one fetch. [default: 50000]
    #[arg(long, value_name = "ITEMS")]
    read_work: Option<u64>,

    /// Active execution per read or fetch, in milliseconds. [default: 5000]
    #[arg(long, value_name = "MS")]
    read_active_ms: Option<u64>,

    /// Temporary memory held by one read, in bytes. [default: 16 MiB]
    #[arg(long, value_name = "BYTES")]
    read_memory: Option<u64>,

    /// Fetch size when `.cursor fetch` omits the row count. [default: 100]
    #[arg(long, value_name = "ROWS")]
    read_cursor_page_rows: Option<u64>,

    /// Canonical bytes in one cursor page. [default: 1 MiB]
    #[arg(long, value_name = "BYTES")]
    read_cursor_page_bytes: Option<u64>,

    /// Time allowed between cursor fetches, in milliseconds. [default: 300000]
    #[arg(long, value_name = "MS")]
    read_cursor_idle_ms: Option<u64>,

    /// Total cursor lifetime, in milliseconds. [default: 1800000]
    #[arg(long, value_name = "MS")]
    read_cursor_lifetime_ms: Option<u64>,

    /// Threshold for the loading and statement-progress notices, in milliseconds. [default: 1000]
    #[arg(long, value_name = "MS")]
    read_hydration_notice_ms: Option<u64>,

    /// Owner-route connect and handshake deadline, in milliseconds. [default: 1000]
    #[arg(long, value_name = "MS")]
    read_owner_connect_ms: Option<u64>,

    /// Owner startup/shutdown race retry window, in milliseconds. [default: 1000]
    #[arg(long, value_name = "MS")]
    read_owner_routing_retry_ms: Option<u64>,

    /// Complete owner reply after admission, in milliseconds. [default: 11000]
    #[arg(long, value_name = "MS")]
    read_owner_response_ms: Option<u64>,

    /// Every owner-read ceiling, deadline, and switch, defined once and taken
    /// by the server under the same names so one policy reads the same way
    /// whichever process is holding the store.
    #[command(flatten)]
    owner_read: OwnerReadOptions,
}

/// Every per-invocation read ceiling and deadline, resolved from the command
/// line over the shipped defaults.
struct ReadConfiguration {
    limits: ReadLimits,
    timeouts: ReadClientTimeouts,
    hydration_notice_ms: u64,
}

impl Args {
    /// Whether any read ceiling, owner-read ceiling, or owner-read switch was
    /// named on this command line. An in-memory database has no route, no
    /// channel, and no ceilings to declare, so naming one there is an invalid
    /// invocation rather than a value that quietly does nothing.
    fn declared_any_read_setting(&self) -> bool {
        self.owner_read.declared_any()
            || [
                self.read_result_rows,
                self.read_result_bytes,
                self.read_work,
                self.read_active_ms,
                self.read_memory,
                self.read_cursor_page_rows,
                self.read_cursor_page_bytes,
                self.read_cursor_idle_ms,
                self.read_cursor_lifetime_ms,
                self.read_hydration_notice_ms,
                self.read_owner_connect_ms,
                self.read_owner_routing_retry_ms,
                self.read_owner_response_ms,
            ]
            .iter()
            .any(Option::is_some)
    }

    fn read_configuration(&self) -> ReadConfiguration {
        let shipped = ReadLimits::default();
        let deadlines = ReadClientTimeouts::default();
        ReadConfiguration {
            limits: ReadLimits {
                result_rows: self.read_result_rows.unwrap_or(shipped.result_rows),
                result_bytes: self.read_result_bytes.unwrap_or(shipped.result_bytes),
                work: self.read_work.unwrap_or(shipped.work),
                active_ms: self.read_active_ms.unwrap_or(shipped.active_ms),
                memory: self.read_memory.unwrap_or(shipped.memory),
                cursor_page_rows: self
                    .read_cursor_page_rows
                    .unwrap_or(shipped.cursor_page_rows),
                cursor_page_bytes: self
                    .read_cursor_page_bytes
                    .unwrap_or(shipped.cursor_page_bytes),
                cursor_idle_ms: self.read_cursor_idle_ms.unwrap_or(shipped.cursor_idle_ms),
                cursor_lifetime_ms: self
                    .read_cursor_lifetime_ms
                    .unwrap_or(shipped.cursor_lifetime_ms),
            },
            timeouts: ReadClientTimeouts {
                connect_ms: self.read_owner_connect_ms.unwrap_or(deadlines.connect_ms),
                routing_retry_ms: self
                    .read_owner_routing_retry_ms
                    .unwrap_or(deadlines.routing_retry_ms),
                response_ms: self.read_owner_response_ms.unwrap_or(deadlines.response_ms),
            },
            hydration_notice_ms: self.read_hydration_notice_ms.unwrap_or(SHIPPED_NOTICE_MS),
        }
    }

    fn owner_configuration(&self) -> OwnerConfiguration {
        self.owner_read.resolve()
    }
}

/// The shipped threshold, in milliseconds, at which a still-loading store and
/// a still-running statement each start reporting progress.
const SHIPPED_NOTICE_MS: u64 = 1_000;

/// Refuse the invocation itself with the invalid-invocation exit code.
///
/// Nothing has been opened or attempted at this point, which is exactly what
/// that code promises. The refusal speaks whichever format the session speaks,
/// so a `--json` consumer parses one stderr shape for the whole process rather
/// than one shape for startup and another for statements. What the argument
/// PARSER itself rejects — an unknown flag, a value of the wrong type — never
/// reaches here: clap answers those in its own rendering, which is where the
/// JSON promise starts.
fn refuse_invocation(json: bool, message: &str) -> ! {
    OutputOptions { json }.report_error(ErrorClass::Usage, message);
    std::process::exit(EXIT_USAGE);
}

/// Every configuration answer that must be settled before the store is opened.
///
/// A ceiling that is not positive, a cursor page larger than the result it
/// pages, an idle window longer than the lifetime containing it, or any read
/// setting at all on an in-memory database are all wrong about the invocation
/// rather than about the data — so they are answered here, before an open
/// could create or touch anything.
fn validated_configuration(args: &Args, path: &str) -> (ReadConfiguration, OwnerConfiguration) {
    if path == ":memory:" && args.declared_any_read_setting() {
        refuse_invocation(
            args.json,
            "an in-memory database has no read route, no owner channel, and no ceilings to \
             declare, so --read-*, --owner-read-*, --no-owner-reads, and \
             --owner-read-runtime-dir are invalid with `:memory:`",
        );
    }

    if path == ":memory:" && args.disk_limit.is_some() {
        refuse_invocation(
            args.json,
            "an in-memory database has no file to bound, so --disk-limit is invalid with \
             `:memory:`",
        );
    }

    let read = args.read_configuration();
    if let Err(violation) = read.limits.validate() {
        refuse_invocation(
            args.json,
            &format!("invalid read configuration: {violation}"),
        );
    }
    if let Err(violation) = read.timeouts.validate() {
        refuse_invocation(args.json, &format!("invalid read deadline: {violation}"));
    }
    if read.hydration_notice_ms == 0 {
        refuse_invocation(
            args.json,
            "invalid read configuration: --read-hydration-notice-ms must be positive",
        );
    }

    let owner = args.owner_configuration();
    if let Some(violation) = owner.violation() {
        refuse_invocation(args.json, &violation);
    }

    (read, owner)
}

fn main() {
    let args = Args::parse();

    if let Some(operation) = &args.operation {
        // A word the contract removed is answered by name. Someone who types it
        // learns which word replaced it, instead of reading a parser complaint
        // about the path they typed after it.
        if let Operation::Repair { .. } = operation {
            refuse_invocation(
                args.json,
                "`repair` was replaced by `diagnose`, which reports a store's format and \
                 schema layout without modifying it; rerun with `diagnose`",
            );
        }
        // The registry publishes the operational spellings, and the command
        // tree above resolves them. Reading the discovery list here is what
        // keeps a word from existing in one surface and not the other.
        debug_assert!(
            operational_command_discovery().contains(&operation.spelling()),
            "the command tree offers a spelling the registry does not publish"
        );
        debug_assert!(
            canonical_help_signatures().contains(&operation.spelling()),
            "every operational spelling is part of the one discovery surface"
        );
        let exit_code =
            contextdb_cli::ops::run_operational(operation.spelling(), operation.arguments())
                .expect("the command tree only resolves spellings the operations answer to");
        std::process::exit(exit_code);
    }

    let Some(path) = args.path.clone() else {
        refuse_invocation(
            args.json,
            "a database path is required: pass one, or set CONTEXTDB_DB_PATH; \
             use `:memory:` for an ephemeral in-memory database",
        );
    };

    // Settled before anything is opened: exit 2 promises nothing was attempted,
    // and that has to include not creating a store for a session that was never
    // going to run.
    let (read, owner) = validated_configuration(&args, &path);

    // `:memory:` is always writable: it creates no file, so there is nothing
    // for a flag to authorize, and an explicit `--write` there is an accepted
    // no-op rather than the thing that turns mutation on.
    let memory_backed = path == ":memory:";
    let writes_permitted = args.write || memory_backed;

    if !writes_permitted && args.tenant_id.is_some() {
        refuse_invocation(
            args.json,
            "sync is a writer capability: --tenant-id and --sync-endpoint configure a --write \
             session's edge enrollment, so add --write",
        );
    }

    let interactive = std::io::stdin().is_terminal();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    // Every message this binary writes — startup, shutdown, warnings — goes
    // through these, so a `--json` consumer parses one stderr format for the
    // whole process and not just for the REPL's own statements.
    let output = OutputOptions { json: args.json };
    let memory_limit = args
        .memory_limit
        .as_ref()
        .map(|limit| parse_size_limit(limit))
        .transpose()
        .unwrap_or_else(|err| {
            refuse_invocation(args.json, &format!("invalid --memory-limit: {err}"));
        });
    let disk_limit = args
        .disk_limit
        .as_ref()
        .map(|limit| parse_size_limit(limit).map(|bytes| bytes as u64))
        .transpose()
        .unwrap_or_else(|err| {
            refuse_invocation(args.json, &format!("invalid --disk-limit: {err}"));
        });

    if !writes_permitted {
        // A reading session opens no database at all. There is no handle here
        // that could create the store, change a byte of it, or start an owner
        // service — the route is selected once, and reading is all this
        // process can do.
        // The observer is handed over at OPEN because loading the store is
        // itself part of the wait: a session that only started reporting once
        // a statement ran would stay silent through the longest pause there is.
        let progress: Arc<dyn contextdb_engine::ReadProgressObserver> =
            Arc::new(ReadProgressReporter::new(output, read.hydration_notice_ms));
        // The runtime directory is the writer's own flag, and one validated
        // directory serves both sides: a container or packaged service names
        // it once, and the reader looks for the channel exactly where the
        // writer put it. A root only the writer honored would let a packaged
        // deployment start a writer nobody can ever inspect.
        let reader = match ReadSession::open_with_progress_in_runtime_dir(
            std::path::Path::new(&path),
            ReadSessionOptions {
                limits: read.limits,
                timeouts: read.timeouts,
                ..ReadSessionOptions::default()
            },
            owner.runtime_dir.clone(),
            progress,
        ) {
            Ok(reader) => reader,
            Err(contextdb_core::Error::ReadFailure(failure)) => {
                output.report_read_failure(&failure, None);
                std::process::exit(EXIT_ERROR);
            }
            Err(error) => {
                output.report_error(
                    ErrorClass::of(&error),
                    &format!("failed to read the database at '{path}': {error}"),
                );
                std::process::exit(EXIT_ERROR);
            }
        };
        let exit_code = run(
            &Session::reading(reader, std::path::PathBuf::from(&path)),
            None,
            None,
            None,
            output,
        );
        if exit_code != 0 {
            std::process::exit(exit_code);
        }
        return;
    }

    // If sync is configured, create the SyncPlugin before opening the DB.
    // Keep the rx end alive — a background task will consume it for debounced pushes.
    let (sync_plugin_arc, push_rx) = if args.tenant_id.is_some() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        (
            Some(Arc::new(contextdb_server::SyncPlugin::new(tx))),
            Some(rx),
        )
    } else {
        (None, None)
    };

    // Resolve the sync endpoint BEFORE the database is opened. An incomplete
    // flag combination is a usage error, and exit 2 promises that nothing was
    // attempted — which has to include not creating a database file on a fresh
    // path for a command that was never going to run.
    let sync_endpoint = args.tenant_id.as_ref().map(|_| {
        let Some(endpoint) = args.sync_endpoint.clone() else {
            refuse_invocation(
                args.json,
                "--tenant-id needs a sync endpoint. Pass --sync-endpoint with the server's ticket.",
            );
        };
        // A bare ticket gets the edge's own fabric identity pinned to it
        // (derived from the database path), so this machine dials as its
        // enrolled identity instead of an ephemeral key. In-memory databases
        // have no data root and stay ephemeral.
        use contextdb_server::transport::iroh::EndpointSpec;
        match EndpointSpec::parse(&endpoint) {
            Some(spec)
                if spec.dial_ticket().is_some()
                    && spec.identity_path().is_none()
                    && path != ":memory:" =>
            {
                format!(
                    "iroh:?to={}&identity={}.fabric-identity.key",
                    spec.dial_ticket().expect("checked above"),
                    path
                )
            }
            _ => endpoint,
        }
    });

    debug!(path = %path, "opening database");
    // One writable-open configuration, so the owner-reading policy this
    // session advertises is the policy it was asked for. An in-memory database
    // has no file to bound and no channel to serve, which the argument parser
    // has already refused above.
    let opened = contextdb_engine::Database::open_with_options(
        std::path::Path::new(&path),
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                enabled: owner.enabled,
                limits: owner.limits,
                timeouts: owner.timeouts,
                runtime_dir: owner.runtime_dir.clone(),
                ..OwnerReadConfig::default()
            },
            plugin: match &sync_plugin_arc {
                Some(plugin) => plugin.clone(),
                None => Arc::new(contextdb_engine::plugin::CorePlugin),
            },
            memory_limit,
            disk_limit: if memory_backed { None } else { disk_limit },
            ..DatabaseOpenOptions::default()
        },
    );
    let db = match opened {
        Ok(db) => db,
        // A store already held by a writer, or held by hydrating readers, is a
        // typed refusal with a next action attached. Flattening it into a
        // sentence would throw away the `detail.kind` a script branches on and
        // the guidance a person needs.
        Err(contextdb_core::Error::ReadFailure(failure)) => {
            output.report_read_failure(&failure, None);
            std::process::exit(EXIT_ERROR);
        }
        Err(e) => {
            output.report_error(
                ErrorClass::of(&e),
                &format!("failed to open database at '{path}': {e}"),
            );
            std::process::exit(EXIT_ERROR);
        }
    };

    let db = Arc::new(db);

    // Single tokio runtime for the session.
    let rt_and_client = args.tenant_id.as_ref().map(|tenant_id| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("failed to create tokio runtime");
        let endpoint = sync_endpoint
            .clone()
            .expect("an endpoint is resolved whenever --tenant-id is present");
        let client = Arc::new(contextdb_server::SyncClient::new(
            db.clone(),
            &endpoint,
            contextdb_core::TenantId::from(tenant_id.as_str()),
        ));
        (rt, client)
    });

    let (rt, sync_client) = match &rt_and_client {
        Some((rt, client)) => (Some(rt), Some(client)),
        None => (None, None),
    };

    let probe_handle = if !interactive && let (Some(rt), Some(client)) = (rt, sync_client) {
        // Warm the sync connection in the background. Never wrap this in a
        // timeout that abandons the future: dropping a mid-dial endpoint
        // aborts it ungracefully (a scary transport ERROR on stderr). A down
        // endpoint surfaces as one clear warning instead of silence. The
        // handle is settled before shutdown so a late-finishing probe can
        // never store an endpoint after the state was drained.
        let probe = Arc::clone(client);
        Some(rt.spawn(async move {
            if let Err(err) = probe.ensure_connected().await {
                // Not a failure: the session runs locally and sync retries.
                output.report_notice(
                    ErrorClass::Sync,
                    &format!("Warning: sync endpoint unreachable: {err}"),
                );
            }
        }))
    } else {
        None
    };

    // Spawn background debounced push task if sync is configured.
    let push_handle = if let (Some(rt_ref), Some(client), Some(rx)) = (rt, sync_client, push_rx) {
        let client_clone = Arc::clone(client);
        let plugin_clone = sync_plugin_arc.clone().expect("sync plugin configured");
        let config = auto_sync::AutoSyncConfig {
            debounce: Duration::from_millis(args.sync_debounce_ms),
            ..auto_sync::AutoSyncConfig::default()
        };
        Some(rt_ref.spawn(auto_sync::run_loop(
            rx,
            config,
            move || {
                let client = client_clone.clone();
                let plugin = plugin_clone.clone();
                async move {
                    let result = match client.push().await {
                        Ok(result) => result,
                        Err(contextdb_core::Error::SyncPushUnconfirmed { detail }) => {
                            // Interrupted after the batch was sent: the data may
                            // already have landed on the hub. This is NOT a
                            // failure — auto-sync will re-push, which reconciles
                            // idempotently whether or not the batch committed.
                            output.report_notice(
                                ErrorClass::Sync,
                                &format!("Background auto-sync push was interrupted before the hub acknowledged: {detail}. It will re-push to reconcile."),
                            );
                            return Ok(auto_sync::PushOutcome {
                                conflicts: Vec::new(),
                                caught_up: false,
                            });
                        }
                        Err(contextdb_core::Error::SyncReplayOfAcceptedDelete { table, key }) => {
                            // The store already agrees: benign convergence,
                            // not a failure -- see
                            // `Error::SyncReplayOfAcceptedDelete`'s doc
                            // comment. Report it once as a notice (not a
                            // repeating "could not push" failure message)
                            // and mark caught up so auto-sync stops
                            // retrying a push that will never resolve any
                            // differently.
                            output.report_notice(
                                ErrorClass::Sync,
                                &format!(
                                    "Background auto-sync push re-offered {table} {key:?}, which \
                                     the hub already converged on as deleted; nothing to do."
                                ),
                            );
                            return Ok(auto_sync::PushOutcome {
                                conflicts: Vec::new(),
                                caught_up: true,
                            });
                        }
                        Err(err) => return Err(err.to_string()),
                    };
                    Ok(auto_sync::PushOutcome {
                        conflicts: result
                            .conflicts
                            .iter()
                            .map(contextdb_engine::cli_render::sync_conflict_document)
                            .collect::<Vec<_>>(),
                        caught_up: client.push_watermark() >= plugin.pending_lsn(),
                    })
                }
            },
            // The worker keeps retrying, so its report is a notice, not a
            // failure of this run.
            move |report| match report {
                auto_sync::AutoSyncReport::Conflict(conflict) => output
                    .report_notice_document(ErrorClass::Sync, "sync conflict", &conflict),
                auto_sync::AutoSyncReport::Message(message) => {
                    output.report_notice(ErrorClass::Sync, &message)
                }
            },
        )))
    } else {
        None
    };

    // Process exit code, from the table in `contextdb_server::exit_codes`
    // (documented at docs/cli.md, "Exit Codes"): 0 = clean, 1 = definitive
    // error, 3 = an interrupted push whose outcome the hub could not confirm.
    // A definitive error always dominates an unconfirmed push; an unconfirmed
    // push never downgrades a 1.
    // A writing session still reads through a bounded view over its own live
    // database, so a writer's SELECTs are bounded exactly like anyone else's.
    let session = match Session::writing(db.clone(), read.limits) {
        Ok(session) => session,
        Err(error) => {
            output.report_error(
                ErrorClass::of(&error),
                &format!("failed to open a bounded read view over '{path}': {error}"),
            );
            std::process::exit(EXIT_ERROR);
        }
    };

    let mut exit_code = run(
        &session,
        sync_client.map(|c| c.as_ref()),
        rt,
        sync_plugin_arc.as_deref(),
        output,
    );

    // The bounded read view holds its own handle on this database. Release it
    // BEFORE shutdown: a store still held open cannot close cleanly, and a
    // writer that never closed cleanly leaves its local read channel behind
    // for the next reader to dial into and find nobody home.
    drop(session);

    // Graceful shutdown: stop background notifications, wait for any in-flight
    // auto-sync work to finish, then do one final flush before closing the DB.
    if let Some((rt, client)) = rt_and_client {
        if let Err(err) = db.execute("ROLLBACK", &std::collections::HashMap::new()) {
            output.report_error(
                ErrorClass::of(&err),
                &format!("Final transaction rollback failed: {err}"),
            );
            exit_code = EXIT_ERROR;
        }
        if let Some(ref plugin) = sync_plugin_arc {
            plugin.shutdown();
        }
        if let Some(handle) = push_handle
            && let Err(err) = rt.block_on(handle)
        {
            // A join failure of the auto-sync worker is a failure of the sync
            // plane, not of the statements this session ran.
            output.report_error(
                ErrorClass::Sync,
                &format!("Auto-sync worker failed during shutdown: {err}"),
            );
            exit_code = EXIT_ERROR;
        }
        match client.has_pending_push_changes() {
            Ok(true) => match rt.block_on(client.push()) {
                Ok(_) => {}
                Err(contextdb_core::Error::SyncPushUnconfirmed { detail }) => {
                    // Interrupted after send: the data may already have landed.
                    // Not a definitive failure, but the outcome is unknown, so
                    // exit with the distinct unconfirmed code (without
                    // downgrading a harder error) — a `push && shutdown` caller
                    // must not read this as a clean success. The next push
                    // reconciles idempotently.
                    // Indeterminate, not failed: a notice, with exit code 3
                    // carrying the "re-push is safe" signal.
                    output.report_notice(
                        ErrorClass::Sync,
                        &format!("Final sync push was interrupted before the hub acknowledged: {detail}. The data may already have landed; run `.sync push` on next start to reconcile."),
                    );
                    if exit_code == 0 {
                        exit_code = EXIT_INTERRUPTED_PUSH_UNCONFIRMED;
                    }
                }
                Err(contextdb_core::Error::SyncReplayOfAcceptedDelete { table, key }) => {
                    // The store already agrees: this exit-push re-offered a
                    // row the hub had already terminated by an accepted
                    // delete (the durable delete row itself has no local
                    // "arrived by sync" provenance surviving a fresh
                    // process, so the preflight above could not tell it
                    // apart from a genuine unpushed local change — see
                    // `Error::SyncReplayOfAcceptedDelete`'s doc comment).
                    // The hub's typed refusal proves both sides already
                    // converged on the delete, so this is benign, not a
                    // failure: a quiet notice, exit code left at whatever it
                    // already was (never downgraded to success, never
                    // bumped to a failure).
                    output.report_notice(
                        ErrorClass::Sync,
                        &format!(
                            "Final sync push re-offered {table} {key:?}, which the hub already \
                             converged on as deleted; nothing to do."
                        ),
                    );
                }
                Err(err) => {
                    output.report_error(
                        ErrorClass::of(&err),
                        &format!("Final sync push failed: {err}"),
                    );
                    exit_code = exit_code_for(&err);
                }
            },
            Ok(false) => {}
            Err(err) => {
                output.report_error(
                    ErrorClass::of(&err),
                    &format!("Final sync preflight failed: {err}"),
                );
                exit_code = exit_code_for(&err);
            }
        }
        rt.block_on(async {
            // Settle the startup probe first, then close the sync connection
            // and its endpoint gracefully so exit never aborts the transport
            // mid-flight.
            if let Some(handle) = probe_handle {
                let _ = handle.await;
            }
            client.shutdown().await;
            drop(client);
        });
    }

    if let Err(e) = db.close() {
        output.report_error(
            ErrorClass::of(&e),
            &format!("failed to close database: {e}"),
        );
        std::process::exit(EXIT_ERROR);
    }

    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

fn parse_size_limit(value: &str) -> Result<usize, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("limit cannot be empty".to_string());
    }

    let split_at = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (digits, suffix) = trimmed.split_at(split_at);
    let base = digits
        .parse::<usize>()
        .map_err(|_| format!("invalid byte value '{trimmed}'"))?;
    let multiplier = match suffix.trim().to_ascii_uppercase().as_str() {
        "" => 1usize,
        "K" => 1024usize,
        "M" => 1024usize * 1024,
        "G" => 1024usize * 1024 * 1024,
        other => return Err(format!("unsupported memory suffix '{other}'")),
    };

    base.checked_mul(multiplier)
        .ok_or_else(|| format!("memory limit '{trimmed}' is too large"))
}
