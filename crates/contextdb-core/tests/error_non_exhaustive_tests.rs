//! `contextdb_core::Error` (`crates/contextdb-core/src/error.rs:79`) does not
//! carry `#[non_exhaustive]` today. A published error-classification enum
//! elsewhere in this workspace, `ErrorClass`
//! (`crates/contextdb-cli/src/json_output.rs`), already sets the precedent
//! for this exact question: it is `#[non_exhaustive]` specifically so a
//! downstream crate can never assume its variant list is closed. `Error`
//! itself has ~80 variants and grows routinely as new failure modes are
//! typed — without `#[non_exhaustive]`, adding one more variant is a
//! breaking change for any external crate that matched exhaustively,
//! silently turning a routine engine change into a downstream compile break.

#[test]
fn error_enum_rejects_an_exhaustive_match_from_an_external_crate() {
    let t = trybuild::TestCases::new();
    t.compile_fail(
        "tests/fixtures/compile_fail/error_enum_exhaustive_match_from_external_crate.rs",
    );
    // A `.stderr` baseline IS pinned, at
    // `tests/fixtures/compile_fail/error_enum_exhaustive_match_from_external_crate.stderr`
    // (trybuild fails the outer test on any mismatch, including a missing
    // baseline it would otherwise have to write). The baseline does not
    // enumerate `Error`'s variants or reference any line inside the enum
    // definition — it only names the fixture's own match expression and the
    // fixed "non-exhaustive patterns: `&_` not covered" diagnostic — so it
    // does not need updating every time `Error` gains a variant. Today the
    // fixture compiles cleanly (nothing left uncovered), so the outer test
    // fails ("expected test case to fail to compile, but it succeeded");
    // once `#[non_exhaustive]` lands on `Error`, the identical fixture must
    // fail to compile with exactly this pinned diagnostic.
}
