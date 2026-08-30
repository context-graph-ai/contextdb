//! The owner-reading policy a process holding a file-backed store advertises.
//!
//! A writer and a server are the same holder wearing different clothes: both
//! keep the store open, and both can answer another process's reads through
//! the local channel. An operator who knows one command's read policy should
//! be able to state the same policy to the other, in the same words, and read
//! back the same help. That only stays true if there is ONE definition of the
//! flags, their defaults, their validation, and the configuration they resolve
//! to -- which is this module, consumed by both binaries.
//!
//! No setting here is taken from the environment. What a holder will serve is
//! readable from the command that started it. The single exception is where
//! the channel file goes, which a container or packaged service has to be able
//! to supply before it knows anything else; the flag still wins over it.

use contextdb_core::read_contract::{OwnerReadLimits, OwnerServiceTimeouts, ReadLimits};
use std::path::PathBuf;

/// Every owner-read ceiling, deadline, and switch a store-holding process
/// takes on its command line.
// `about = None` keeps this struct's own documentation out of the help of
// whichever command flattens it: the group describes itself to a reader of the
// source, while each binary keeps its own one-line summary.
#[derive(Debug, Clone, clap::Args)]
#[command(about = None, long_about = None)]
pub struct OwnerReadOptions {
    /// Rows this writer will serve in one complete result. [default: 500]
    #[arg(long, value_name = "ROWS")]
    pub owner_read_result_rows: Option<u64>,

    /// Bytes this writer will serve in one complete result. [default: 4 MiB]
    #[arg(long, value_name = "BYTES")]
    pub owner_read_result_bytes: Option<u64>,

    /// Items this writer will examine for one served read. [default: 50000]
    #[arg(long, value_name = "ITEMS")]
    pub owner_read_work: Option<u64>,

    /// Active execution this writer allows one served read, in milliseconds. [default: 5000]
    #[arg(long, value_name = "MS")]
    pub owner_read_active_ms: Option<u64>,

    /// Temporary memory this writer allows one served read, in bytes. [default: 16 MiB]
    #[arg(long, value_name = "BYTES")]
    pub owner_read_memory: Option<u64>,

    /// Rows this writer serves in one cursor page by default. [default: 100]
    #[arg(long, value_name = "ROWS")]
    pub owner_read_cursor_page_rows: Option<u64>,

    /// Bytes this writer serves in one cursor page. [default: 1 MiB]
    #[arg(long, value_name = "BYTES")]
    pub owner_read_cursor_page_bytes: Option<u64>,

    /// Time this writer allows between cursor fetches, in milliseconds. [default: 300000]
    #[arg(long, value_name = "MS")]
    pub owner_read_cursor_idle_ms: Option<u64>,

    /// Total cursor lifetime this writer allows, in milliseconds. [default: 1800000]
    #[arg(long, value_name = "MS")]
    pub owner_read_cursor_lifetime_ms: Option<u64>,

    /// Simultaneous readers this writer admits. No queue forms past it. [default: 4]
    #[arg(long, value_name = "READERS")]
    pub owner_read_concurrency: Option<u64>,

    /// Deadline for one served request, in milliseconds. [default: 10000]
    #[arg(long, value_name = "MS")]
    pub owner_read_request_ms: Option<u64>,

    /// Time this writer drains in-flight reads at shutdown, in milliseconds. [default: 10000]
    #[arg(long, value_name = "MS")]
    pub owner_read_shutdown_drain_ms: Option<u64>,

    /// Do not serve local owner reads from this writer at all.
    #[arg(long)]
    pub no_owner_reads: bool,

    /// Absolute directory holding this writer's local read channel. Required
    /// for containers and packaged services.
    ///
    /// This is the second of the two environment names the CLI reads, and the
    /// flag always wins over it.
    #[arg(long, env = "CONTEXTDB_OWNER_READ_RUNTIME_DIR", value_name = "DIR")]
    pub owner_read_runtime_dir: Option<PathBuf>,
}

/// The owner-reading policy this session advertises when it holds the store,
/// resolved from a command line.
///
/// This is stated in the read-contract vocabulary and nothing else. The
/// open-time configuration a policy becomes is named at each holder's own open
/// site, which is where opening a store is decided; the flags, their defaults,
/// their resolution, and their validation -- everything a reader of two
/// commands' help has to find identical -- live here.
#[derive(Debug, Clone)]
pub struct OwnerConfiguration {
    pub enabled: bool,
    pub limits: OwnerReadLimits,
    pub timeouts: OwnerServiceTimeouts,
    pub runtime_dir: Option<PathBuf>,
}

impl OwnerConfiguration {
    /// What is wrong with this policy as an invocation, worded once so both
    /// binaries refuse the same mistake in the same sentence.
    ///
    /// A ceiling that is not positive, a cursor page larger than the result it
    /// pages, or an idle window longer than the lifetime containing it is
    /// wrong about the command line rather than about the data, so it is
    /// answered before anything is opened.
    #[must_use]
    pub fn violation(&self) -> Option<String> {
        if let Err(violation) = self.limits.validate() {
            return Some(format!("invalid owner-read configuration: {violation}"));
        }
        if let Err(violation) = self.timeouts.validate() {
            return Some(format!("invalid owner-read deadline: {violation}"));
        }
        None
    }
}

impl OwnerReadOptions {
    /// Whether any owner-read ceiling, deadline, or switch was named on this
    /// command line. An in-memory database has no route, no channel, and no
    /// ceilings to declare, so naming one there is an invalid invocation
    /// rather than a value that quietly does nothing.
    #[must_use]
    pub fn declared_any(&self) -> bool {
        self.no_owner_reads
            || self.owner_read_runtime_dir.is_some()
            || [
                self.owner_read_result_rows,
                self.owner_read_result_bytes,
                self.owner_read_work,
                self.owner_read_active_ms,
                self.owner_read_memory,
                self.owner_read_cursor_page_rows,
                self.owner_read_cursor_page_bytes,
                self.owner_read_cursor_idle_ms,
                self.owner_read_cursor_lifetime_ms,
                self.owner_read_concurrency,
                self.owner_read_request_ms,
                self.owner_read_shutdown_drain_ms,
            ]
            .iter()
            .any(Option::is_some)
    }

    /// This command line resolved over the shipped defaults.
    #[must_use]
    pub fn resolve(&self) -> OwnerConfiguration {
        let shipped = OwnerReadLimits::default();
        let deadlines = OwnerServiceTimeouts::default();
        OwnerConfiguration {
            enabled: !self.no_owner_reads,
            limits: OwnerReadLimits {
                limits: ReadLimits {
                    result_rows: self
                        .owner_read_result_rows
                        .unwrap_or(shipped.limits.result_rows),
                    result_bytes: self
                        .owner_read_result_bytes
                        .unwrap_or(shipped.limits.result_bytes),
                    work: self.owner_read_work.unwrap_or(shipped.limits.work),
                    active_ms: self
                        .owner_read_active_ms
                        .unwrap_or(shipped.limits.active_ms),
                    memory: self.owner_read_memory.unwrap_or(shipped.limits.memory),
                    cursor_page_rows: self
                        .owner_read_cursor_page_rows
                        .unwrap_or(shipped.limits.cursor_page_rows),
                    cursor_page_bytes: self
                        .owner_read_cursor_page_bytes
                        .unwrap_or(shipped.limits.cursor_page_bytes),
                    cursor_idle_ms: self
                        .owner_read_cursor_idle_ms
                        .unwrap_or(shipped.limits.cursor_idle_ms),
                    cursor_lifetime_ms: self
                        .owner_read_cursor_lifetime_ms
                        .unwrap_or(shipped.limits.cursor_lifetime_ms),
                },
                concurrency: self.owner_read_concurrency.unwrap_or(shipped.concurrency),
            },
            timeouts: OwnerServiceTimeouts {
                request_ms: self.owner_read_request_ms.unwrap_or(deadlines.request_ms),
                shutdown_drain_ms: self
                    .owner_read_shutdown_drain_ms
                    .unwrap_or(deadlines.shutdown_drain_ms),
            },
            runtime_dir: self.owner_read_runtime_dir.clone(),
        }
    }
}
