pub mod auto_sync;
pub mod formatter;
mod json_output;
mod repl;
pub mod sync_status;
// The internal test seam: unstable, hidden from rustdoc, and off by default.
// It compiles only under the `test-seams` feature, which this crate's own test
// targets turn on through the self-dependency in Cargo.toml, so an ordinary
// build of this crate does not contain it. A consumer that enables the feature
// deliberately can still call it, and nothing here carries a stability
// promise — it may change or disappear in any release.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub mod testing;

/// The library's REPL surface: run a session, choose how results are rendered,
/// and read the process exit-code table every contextdb binary honors — including
/// the distinct code for an interrupted push whose outcome the hub never
/// confirmed. The table itself is defined once, in
/// [`contextdb_server::exit_codes`], and re-exported here so a CLI consumer needs
/// one import path.
pub use contextdb_server::exit_codes::{
    EXIT_ERROR, EXIT_INTERRUPTED_PUSH_UNCONFIRMED, EXIT_OK, EXIT_USAGE,
};
/// How output is rendered, plus the error vocabulary the `--json` envelope
/// speaks — the binary reports its own startup and shutdown failures through
/// the same two, so one process emits one format.
pub use json_output::ErrorClass;
pub use repl::{OutputOptions, run};
