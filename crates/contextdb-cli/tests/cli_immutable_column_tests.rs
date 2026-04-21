//! CLI fatal-error classification for Error::ImmutableColumn.

use contextdb_cli::testing::classify_cli_error;
use contextdb_core::Error;

// ============================================================================
// CL02 — RED — ImmutableColumn is classified as NON-fatal in the CLI.
// ============================================================================
#[test]
fn cl02_immutable_column_is_not_fatal() {
    let err = Error::ImmutableColumn {
        table: "decisions".to_string(),
        column: "decision_type".to_string(),
    };
    assert!(
        !classify_cli_error(&err),
        "ImmutableColumn must NOT be classified as fatal (matches ImmutableTable behavior)"
    );

    // Regression: unrelated variant still classifies as fatal (ParseError).
    let pe = Error::ParseError("x".to_string());
    assert!(classify_cli_error(&pe));
}
