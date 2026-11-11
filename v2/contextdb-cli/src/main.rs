//! The contextdb program: a SQL shell for a store. `server` and the other names of commands
//! that arrive with later versions are reserved and refused; they are not commands it has.
//!
//! It never does what the library cannot: every statement and every command goes through the
//! public API of `contextdb`, and nothing here opens the store's files itself.

mod input;
mod output;
mod shell;
#[cfg(test)]
mod tests;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use contextdb::{Database, DatabaseOpenOptions, ReadSession, ReadSessionOptions};

use output::Output;
use shell::Store;

/// The exit status of a run in which the store could not be opened or closed, or a piped
/// statement or shell command was refused.
const EXIT_FAILED: u8 = 1;
/// The exit status of a command line that asks for something the program cannot do. Nothing
/// was opened. The argument parser exits with the same status for a line it cannot read.
const EXIT_USAGE: u8 = 2;

/// Commands that arrive with later versions. The word is theirs already, so a folder with one
/// of these names is opened by writing it as a path.
const LATER_COMMANDS: [&str; 6] = [
    "server", "snapshot", "inspect", "diagnose", "reset", "purge",
];

/// What `--help` says after the options.
const AFTER_HELP: &str = r#"INPUT:
  Statements are read from standard input, each ending at its `;`. On a terminal the
  shell prompts and edits lines; piped input gets no prompt.
  A refused statement in piped input is reported on the error stream, the statements
  after it still run, and the run exits 1.
  With --write the store is held for writing until the input ends: other processes can
  read it meanwhile, and a second --write is refused until then.
  A transaction that BEGIN opened and no COMMIT ended before the input ended is rolled
  back.

EXIT STATUS:
  0  every piped statement and command ran; a session at a terminal ends with 0
     whatever was refused in it
  1  a piped statement or command was refused, or the store could not be opened or
     closed (a session at a terminal too)
  2  the command line was refused; nothing was opened

MEMORY:
  --memory-limit is this session's ceiling, in bytes or with K, M or G; the store does
  not remember it. Without it a --write session has the limit the store remembers and
  a read session has none.
  A session sets aside 1M and its page cache, sized when it starts: a quarter of its
  limit and never less than 8M, which is also its size with no limit. So the smallest
  limit that opens a store is 9M.
  `SET MEMORY_LIMIT 512M;` (or NONE) declares the limit the store remembers; it needs
  --write, and inside a transaction it still takes effect at once and ROLLBACK does not
  undo it. The smallest limit a session takes is 1M plus its page cache plus what it
  holds, which is 9M in a session started with no limit; a limit below it is refused.
  To go below the page cache of a session started under a larger limit, start with the
  smaller one, `--memory-limit 64M`, and then run `SET MEMORY_LIMIT 64M;`.
  `SHOW MEMORY_LIMIT;` reports this session's limit and what is in use: with no limit,
  `none` and 9437184 bytes used, the 1M and the 8M page cache.

JSON (--json), one document a line:
  a SELECT or SHOW MEMORY_LIMIT   {"result":{"columns":[...],"rows":[{...}]}}
                                  a vector is an array of numbers, each at the full
                                  precision of the stored 32-bit value, so a number
                                  may print with more digits than were typed
  any other statement             {"rows_affected":N}
  .tables                         {"tables":{"items":[...]}}
  .schema <table>                 {"schema":{"table":...,"ddl":...}}
  .help                           {"help":[...]}
  a refusal                       {"error":{"class":...,"message":...,"detail":{"kind":...}}}
                                  on the error stream; "detail" is left out when the
                                  refusal is the shell's own (an unknown command, a
                                  reserved name)
  A command line the program cannot read is refused in plain text.

RESERVED NAMES:
  server, snapshot, inspect, diagnose, reset and purge are kept for commands of later
  versions. A folder with one of these names is written as a path: ./server

THE STORE:
  The folder holds `data`, `lock` and `readers`; together they are the store. The
  README of the contextdb library says what each is.

EXAMPLES:
  contextdb ./notes
    Read a store that is there, also while another process writes it. Never creates a
    store and never changes its data.

  contextdb ./notes --write
    Open the store for writing, creating it if it is missing.

  echo "SELECT * FROM notes LIMIT 5;" | contextdb ./notes --json
    Pipe statements in and read one JSON document per statement.

  In the shell: .help
    List the shell's commands."#;

/// A SQL shell for a contextdb store.
#[derive(Parser)]
#[command(
    name = "contextdb",
    version,
    override_usage = "contextdb [OPTIONS] <PATH>",
    after_help = AFTER_HELP
)]
struct Args {
    /// The store's folder. A folder named like a command is written as a path: ./server
    path: PathBuf,

    /// Open the store for writing, creating it if it is missing. Without this the shell
    /// only reads.
    #[arg(long)]
    write: bool,

    /// Machine output: one JSON document per statement or command on the output, and every
    /// refusal as a JSON document on the error stream.
    #[arg(long)]
    json: bool,

    /// The most memory the session may use for data, in bytes or with K, M or G (64M, 4G).
    #[arg(long, value_name = "SIZE", value_parser = parse_size)]
    memory_limit: Option<usize>,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let output = Output { json: args.json };

    if let Some(command) = LATER_COMMANDS
        .iter()
        .find(|command| args.path.as_os_str() == **command)
    {
        output.refusal(
            "usage",
            &format!(
                "`{command}` is a command, and this version does not have it yet; to open a \
                 folder with that name write ./{command}"
            ),
        );
        return ExitCode::from(EXIT_USAGE);
    }

    let store = match open(&args) {
        Ok(store) => store,
        Err(error) => {
            output.error(&error);
            return ExitCode::from(EXIT_FAILED);
        }
    };
    let mut succeeded = shell::run(&store, output);
    if let Store::Write(db) = &store
        && let Err(error) = db.close()
    {
        output.error(&error);
        succeeded = false;
    }
    if succeeded {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(EXIT_FAILED)
    }
}

/// Opens the store the way the command line asked: for writing, or for reading only.
fn open(args: &Args) -> contextdb::Result<Store> {
    if args.write {
        let mut options = DatabaseOpenOptions::default();
        options.memory_limit = args.memory_limit;
        Database::open_with_options(&args.path, options).map(Store::Write)
    } else {
        let mut options = ReadSessionOptions::default();
        options.memory_limit = args.memory_limit;
        ReadSession::open_with_options(&args.path, options).map(Store::Read)
    }
}

/// Reads a size in bytes: digits, then nothing, K, M or G as powers of 1024.
fn parse_size(text: &str) -> Result<usize, String> {
    let digits_end = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (digits, suffix) = text.split_at(digits_end);
    let number: usize = digits.parse().map_err(|_| {
        format!("'{text}' is not a size: write digits, then K, M or G, or nothing for bytes")
    })?;
    let unit: usize = match suffix.to_ascii_uppercase().as_str() {
        "" => 1,
        "K" => 1 << 10,
        "M" => 1 << 20,
        "G" => 1 << 30,
        other => {
            return Err(format!(
                "unknown size suffix '{other}': use K, M or G, or no suffix for bytes"
            ));
        }
    };
    number
        .checked_mul(unit)
        .ok_or_else(|| format!("'{text}' is too large"))
}
