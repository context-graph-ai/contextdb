//! SQL shell and operational commands, all through the library's public doors.
mod commands;
mod input;
mod interrupt;
mod output;
mod progress;
mod shell;
#[cfg(test)]
mod tests;

use clap::{CommandFactory, Parser, Subcommand};
use contextdb::{
    Database, DatabaseOpenOptions, Maintenance, ReadLimits, ReadSession, ReadSessionOptions,
};
use output::Output;
use std::{
    io::IsTerminal,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::Arc,
};

#[derive(Parser)]
#[command(
    name = "contextdb",
    version,
    after_help = "SQL ends at ;. .help lists shell commands. Refusals continue by default and fail piped runs.
Read sessions create nothing. --write creates a missing store and waits for an existing writer.
Read statements have no row or byte ceiling unless given; time defaults to 5,000 ms; read memory to 16 MiB.
A piped writer defaults to caller-driven maintenance and runs one cycle before close.
Exit: 0 success; 1 refused input or storage failure; 2 invalid arguments; 130 interrupted."
)]
struct Args {
    /// Store folder.
    #[arg(env = "CONTEXTDB_DB_PATH")]
    path: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
    /// Open for writing, creating a missing store.
    #[arg(long)]
    write: bool,
    /// One JSON document per answer; diagnostics go to stderr.
    #[arg(long, global = true)]
    json: bool,
    #[arg(long, value_parser=contextdb::parse_size)]
    memory_limit: Option<usize>,
    #[arg(long, value_parser=contextdb::parse_size, requires="write")]
    disk_limit: Option<usize>,
    /// engine or caller; requires --write.
    #[arg(long, value_parser=maintenance, requires="write")]
    maintenance: Option<Maintenance>,
    #[arg(long, value_parser=clap::value_parser!(u64).range(1..), requires="write")]
    maintenance_poll_ms: Option<u64>,
    #[arg(long)]
    stop_at_first_refusal: bool,
    #[arg(long, value_parser=clap::value_parser!(u64).range(1..))]
    read_result_rows: Option<u64>,
    #[arg(long, value_parser=clap::value_parser!(u64).range(1..))]
    read_result_bytes: Option<u64>,
    #[arg(long, value_parser=clap::value_parser!(u64).range(1..))]
    read_work: Option<u64>,
    #[arg(long, value_parser=clap::value_parser!(u64).range(1..))]
    read_active_ms: Option<u64>,
    #[arg(long, value_parser=contextdb::parse_size)]
    read_memory: Option<usize>,
    #[arg(long, value_parser=clap::value_parser!(u64).range(0..=100))]
    read_cache_share: Option<u64>,
    #[arg(long, value_parser=clap::value_parser!(u64).range(1..))]
    read_cursor_page_rows: Option<u64>,
    #[arg(long, value_parser=clap::value_parser!(u64).range(1..))]
    read_cursor_page_bytes: Option<u64>,
    #[arg(long, value_parser=clap::value_parser!(u64).range(1..))]
    read_cursor_idle_ms: Option<u64>,
    #[arg(long, value_parser=clap::value_parser!(u64).range(1..))]
    read_cursor_lifetime_ms: Option<u64>,
    #[arg(long, default_value_t = 1000)]
    read_hydration_notice_ms: u64,
}
#[derive(Subcommand)]
enum Command {
    /// Erase a table's rows in one commit.
    Purge {
        path: PathBuf,
        #[arg(long)]
        table: String,
        #[arg(long, required = true)]
        force: bool,
    },
    /// Export a committed snapshot.
    Snapshot {
        #[command(subcommand)]
        command: Snapshot,
    },
    /// Inspect current values, also beside a live writer.
    Inspect {
        #[command(subcommand)]
        command: Inspect,
    },
    /// Check every stored record without changing data.
    Diagnose { path: PathBuf },
    /// Recreate an existing store, destroying its data.
    Reset {
        path: PathBuf,
        #[arg(long, required = true)]
        force: bool,
    },
}
#[derive(Subcommand)]
enum Snapshot {
    Export { path: PathBuf, artifact: PathBuf },
}
#[derive(Subcommand)]
enum Inspect {
    Key {
        path: PathBuf,
        #[arg(long)]
        table: String,
        #[arg(long)]
        key_json: String,
        #[arg(long)]
        column: Vec<String>,
    },
    Blob {
        path: PathBuf,
        #[arg(long)]
        hash: String,
    },
    SyncApplyState {
        path: PathBuf,
    },
}
fn maintenance(text: &str) -> Result<Maintenance, String> {
    match text {
        "engine" => Ok(Maintenance::Engine),
        "caller" => Ok(Maintenance::Caller),
        _ => Err("use engine or caller".into()),
    }
}
impl Args {
    fn limits(&self) -> ReadLimits {
        let mut l = ReadLimits::default();
        l.result_rows = self.read_result_rows.unwrap_or(u64::MAX);
        l.result_bytes = self.read_result_bytes.unwrap_or(u64::MAX);
        l.caller_checks_metadata = true;
        l.work = self.read_work.unwrap_or(l.work);
        l.active_ms = self.read_active_ms.unwrap_or(l.active_ms);
        l.memory = self
            .read_memory
            .or(self.memory_limit)
            .unwrap_or(l.memory as usize)
            .min(self.memory_limit.unwrap_or(usize::MAX)) as u64;
        l.cache_share = self.read_cache_share;
        l.cursor_page_rows = self.read_cursor_page_rows.unwrap_or(l.cursor_page_rows);
        l.cursor_page_bytes = self.read_cursor_page_bytes.unwrap_or(l.cursor_page_bytes);
        l.cursor_idle_ms = self.read_cursor_idle_ms.unwrap_or(l.cursor_idle_ms);
        l.cursor_lifetime_ms = self.read_cursor_lifetime_ms.unwrap_or(l.cursor_lifetime_ms);
        l
    }
}
fn main() -> ExitCode {
    let args = Args::parse();
    let l = args.limits();
    let snap = matches!(args.command, Some(Command::Snapshot { .. }));
    let both = Some(l.memory as usize) == args.memory_limit;
    let read = args.read_memory.is_some() || snap;
    let flag = ["--read-memory", "--memory-limit and --read-memory"][usize::from(both)];
    let output = Output {
        json: args.json,
        read: read.then_some((l.memory, flag)),
        result: l.result_bytes,
    };
    let invalid = l.memory == 0
        || l.cursor_page_rows > l.result_rows
        || l.cursor_page_bytes > l.result_bytes
        || l.cursor_idle_ms > l.cursor_lifetime_ms;
    if invalid {
        Args::command()
            .error(
                clap::error::ErrorKind::ValueValidation,
                "invalid read memory or cursor limit relationships",
            )
            .exit();
    }
    let path = args.path.as_ref();
    if args.command.is_none() && path.as_ref().is_none_or(|p| p.as_os_str() == "server") {
        eprintln!(
            "give a store path; server arrives with sync in step 6 (use ./server for that folder)"
        );
        return ExitCode::from(2);
    }
    let result = match &args.command {
        Some(command) => commands::run(command, &args, output).map(|ok| u8::from(!ok)),
        None => run(path.unwrap(), &args, output),
    };
    ExitCode::from(match result {
        Ok(code) => code,
        Err(error) => {
            output.error(&error, None);
            1
        }
    })
}
fn run(path: &Path, args: &Args, output: Output) -> contextdb::Result<u8> {
    let terminal = std::io::stdin().is_terminal();
    let mode = args.maintenance.unwrap_or(if terminal {
        Maintenance::Engine
    } else {
        Maintenance::Caller
    });
    let writer = if args.write {
        let mut options = DatabaseOpenOptions::default();
        options.memory_limit = args.memory_limit;
        options.disk_limit = args.disk_limit.map(|n| n as u64);
        options.maintenance = mode;
        let db = open(path, options, output)?;
        if db.created()? {
            output.notice(
                "created",
                &format!("created a new store at {}", path.display()),
            );
        }
        if let Some(n) = args.maintenance_poll_ms {
            db.execute(
                &format!("SET MAINTENANCE_POLL_INTERVAL '{n} milliseconds'"),
                &Default::default(),
            )?;
        }
        Some(db)
    } else {
        None
    };
    let observer = Arc::new(progress::Progress {
        output,
        threshold: args.read_hydration_notice_ms,
        next: std::sync::atomic::AtomicU64::new(args.read_hydration_notice_ms),
    });
    let reader = Arc::new(match &writer {
        Some(db) => db.read_session_with_progress(args.limits(), observer)?,
        None => {
            let mut options = ReadSessionOptions::default();
            options.limits = args.limits();
            options.observer = Some(observer);
            ReadSession::open_with_options(path, options)?
        }
    });
    interrupt::install().expect("installing the Ctrl-C handler");
    let store = shell::Store {
        reader,
        writer,
        mode,
    };
    let (ok, interrupted) = shell::run(&store, output, args.stop_at_first_refusal, terminal);
    let cycled = finish(store, terminal, output)?;
    Ok(if interrupted {
        130
    } else {
        u8::from(!(ok && cycled))
    })
}
/// The run's end, however it ended: a piped caller-driven writer's closing cycle, whether it
/// succeeded, and the close, whose failure is the run's.
fn finish(store: shell::Store, terminal: bool, output: Output) -> contextdb::Result<bool> {
    drop(store.reader);
    let Some(db) = store.writer else {
        return Ok(true);
    };
    let cycle = || {
        db.run_maintenance_cycle()
            .map_err(|e| output.error(&e, None))
    };
    let cycled = terminal || store.mode != Maintenance::Caller || cycle().is_ok();
    db.close().map(|()| cycled)
}
fn open(path: &Path, mut options: DatabaseOpenOptions, out: Output) -> contextdb::Result<Database> {
    options.deadline = Some(std::time::UNIX_EPOCH);
    match Database::open_with_options(path, options.clone()) {
        Err(contextdb::Error::DatabaseLocked { path }) => {
            let waiting = format!("waiting for the writer that holds {}", path.display());
            out.notice("waiting", &waiting);
            options.deadline = None;
            Database::open_with_options(path, options)
        }
        opened => opened,
    }
}
