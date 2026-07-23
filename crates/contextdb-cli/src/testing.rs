//! Internal test seam. Everything re-exported here exists so this crate's own
//! tests can drive internals directly; none of it is part of the public API and
//! none of it carries a stability promise. It may change or disappear in any
//! release, including a patch release. Production code must not depend on it,
//! and neither should anything outside this crate.
/// Statement framing — how input lines become whole SQL statements. The
/// interactive adapter and the scripted adapter both drive these, so asserting
/// them pins the multi-line contract for both without needing a terminal:
/// [`StatementBuffer::is_empty`] is the continuation state (and, at exit, the
/// unfinished-statement signal), [`route_line`] decides that a meta-command is
/// only a meta-command when no statement is open, and [`statement_terminator`]
/// is the quote- and comment-aware `;` scan.
pub use crate::repl::{
    LineRouting, PendingStatement, StatementBuffer, route_line, statement_terminator,
};

/// The per-line path both adapters call. `feed_line` plus `InputContext` and
/// `SessionState` lets a test drive the interactive path exactly as the terminal
/// would — no PTY — and drive the scripted path beside it to compare the two;
/// `session_exit_code` turns the accumulated state into the process exit code.
pub use crate::repl::{InputContext, SessionState, feed_line, session_exit_code};

/// Re-exported here too so a test that builds an [`InputContext`] needs one
/// import path, not two. This one is genuine public API — it is also
/// `contextdb_cli::OutputOptions`.
pub use crate::repl::OutputOptions;

/// The interactive adapter's own per-line step, ABOVE `feed_line`: what the
/// terminal loop does to a line before framing ever sees it. Exposed so a test
/// drives the real decision instead of reproducing it — reproducing it is how
/// the trim-and-skip data corruption survived inside a test that looked like it
/// was pinning the contract.
pub use crate::repl::interactive_readline_filter;
