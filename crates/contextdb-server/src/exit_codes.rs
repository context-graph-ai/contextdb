//! The process exit-code contract every contextdb binary honors.
//!
//! Results go to stdout, every error and diagnostic goes to stderr, and a run
//! that hit any error exits non-zero from this four-value table. The table is
//! deliberately single-digit and small: a caller branches on "did it work"
//! (`0`), "the run failed" (`1`), "my command line is wrong" (`2`), and the one
//! genuinely different outcome — "the push left the edge and nobody knows
//! whether it landed" (`3`), where the safe move is to re-push rather than to
//! treat the data as lost or delivered.
//!
//! Finer classification belongs on the output, not the exit status: under
//! `--json` the CLI writes an error envelope carrying an error class, so an
//! agent can tell a SQL mistake from an unreachable hub without the table
//! growing a code per error family.
//!
//! This lives here, outside `contextdb-core`, because which process code an
//! error deserves is binary presentation policy, not a property of the engine
//! error. `contextdb-server` is the one crate every contextdb binary already
//! links (the CLI depends on it, the server binary is it, the examples are its
//! own), so one definition serves all of them without a new crate.
//!
//! Documented for users at `docs/cli.md` ("Exit Codes").

/// Everything the run attempted succeeded.
pub const EXIT_OK: i32 = 0;

/// The invocation was valid; something in the run failed. Every SQL and engine
/// error, every failed meta-command, a definitive sync failure, a database that
/// could not be opened or closed, a demo whose observed outcome deviated from
/// its intent.
pub const EXIT_ERROR: i32 = 1;

/// The invocation itself is wrong, and no work was attempted: an unknown flag,
/// a missing argument, an unparseable flag value, an incomplete flag
/// combination. `clap` already exits with this value for the errors it raises
/// itself; a binary's own argument validation must use this constant so the two
/// agree.
pub const EXIT_USAGE: i32 = 2;

/// A push was interrupted after its batch left the edge, and the hub could not
/// be reached to confirm whether the batch landed. The outcome is
/// INDETERMINATE, not failed: the push watermark is left unadvanced and
/// re-pushing reconciles idempotently whether or not the batch committed. Kept
/// distinct from both success and a definitive failure so a scripted
/// `push && shutdown` never reads an unknown outcome as "landed".
pub const EXIT_INTERRUPTED_PUSH_UNCONFIRMED: i32 = 3;

/// The process exit code for an engine error.
///
/// A `match` on [`contextdb_core::Error`] variants: the interrupted push whose
/// outcome the hub never confirmed keeps its own code, and every other variant
/// is a definitive failure of a valid invocation. New engine variants join the
/// definitive-failure class by default, which is the safe reading — an error
/// nobody classified must never look like success.
pub fn exit_code_for(error: &contextdb_core::Error) -> i32 {
    match error {
        contextdb_core::Error::SyncPushUnconfirmed { .. } => EXIT_INTERRUPTED_PUSH_UNCONFIRMED,
        _ => EXIT_ERROR,
    }
}

/// The process exit code for a driver or demo verdict: whether the scenario's
/// observed outcome matched its intent.
///
/// A deviation is a definitive failure, never a silent `0` and never the
/// push-unconfirmed code — a scan that finds what it was looking for, or a
/// security check that a rogue passed, must fail the process so a caller
/// branching on the exit status sees it.
pub fn verdict_exit_code(matched_intent: bool) -> i32 {
    if matched_intent { EXIT_OK } else { EXIT_ERROR }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contextdb_core::Error;

    /// The mapping is a match on variants, and exactly one variant escapes
    /// the definitive-failure class.
    #[test]
    fn sync_push_unconfirmed_is_the_only_variant_that_is_not_a_plain_error() {
        assert_eq!(EXIT_OK, 0);
        assert_eq!(EXIT_ERROR, 1);
        assert_eq!(EXIT_USAGE, 2);
        assert_eq!(EXIT_INTERRUPTED_PUSH_UNCONFIRMED, 3);

        assert_eq!(
            exit_code_for(&Error::SyncPushUnconfirmed {
                detail: "batch sent, hub never acknowledged".to_string(),
            }),
            EXIT_INTERRUPTED_PUSH_UNCONFIRMED,
            "an unconfirmed push is indeterminate, not a definitive failure"
        );

        let definitive = [
            Error::SyncError("hub refused the changeset".to_string()),
            Error::ParseError("unexpected token".to_string()),
            Error::TableNotFound("decisions".to_string()),
            Error::UniqueViolation {
                table: "decisions".to_string(),
                column: "id".to_string(),
            },
            Error::ImmutableColumn {
                table: "decisions".to_string(),
                column: "decision_type".to_string(),
            },
            Error::DatabaseLocked {
                holder_pid: 4242,
                path: std::path::PathBuf::from("/tmp/held.db"),
            },
            Error::MemoryBudgetExceeded {
                subsystem: "vector".to_string(),
                operation: "build".to_string(),
                requested_bytes: 2,
                available_bytes: 1,
                budget_limit_bytes: 1,
                hint: "raise the budget".to_string(),
            },
        ];
        for error in &definitive {
            assert_eq!(
                exit_code_for(error),
                EXIT_ERROR,
                "{error} must report a definitive failure"
            );
        }
    }

    #[test]
    fn verdict_exit_code_never_returns_zero_for_a_deviation() {
        assert_eq!(verdict_exit_code(true), EXIT_OK);
        assert_eq!(verdict_exit_code(false), EXIT_ERROR);
        assert_ne!(
            verdict_exit_code(false),
            EXIT_INTERRUPTED_PUSH_UNCONFIRMED,
            "a deviation is definitive; it must not borrow the retry-is-safe code"
        );
    }
}
