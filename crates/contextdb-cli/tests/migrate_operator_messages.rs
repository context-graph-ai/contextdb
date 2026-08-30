//! What `contextdb migrate` tells an operator when it fails.
//!
//! A failed migration leaves the operator holding one of three different
//! situations, and the wording is the only thing that tells them which: the
//! store is untouched and every artifact is gone; the swap specifically did
//! not happen; or the store IS migrated and only the release of the old
//! handles went wrong. Moving the swap into the engine changed who decides
//! that, so these are pinned verbatim: a reword is a change to what an
//! operator is told, and has to be made deliberately.
//!
//! The behaviour behind the middle message — the store left usable and every
//! generated artifact removed after an injected final-rename failure — is
//! exercised end to end by the companion migration contract suite's
//! `final-rename-failure` scenario. What is pinned here is the wording, and
//! that each situation keeps its own.

use std::path::PathBuf;

fn command_source() -> String {
    std::fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/ops.rs"))
        .expect("read the command that reports a failed migration")
}

/// The migrate command's failure reporting, from the stage match to the end
/// of its final arm.
fn failure_report(source: &str) -> String {
    let from = source
        .find("match err.stage() {")
        .expect("the command chooses its message from the failure's stage");
    let rest = &source[from..];
    let to = rest
        .find("return EXIT_ERROR;")
        .expect("the failure report ends by exiting");
    rest[..to].to_owned()
}

#[test]
fn each_failed_migration_says_which_of_the_three_situations_it_left() {
    let source = command_source();
    let report = failure_report(&source);

    // Before the swap: the original store never changed and nothing is left
    // behind, so the operator is told where the backup is and what went wrong.
    assert!(
        report.contains(r#""Error: '{}' has a backup at '{}', but {err}","#),
        "the before-swap message must name the backup and the cause: {report}",
    );

    // At the swap: the operator most needs to know the store is still usable
    // and that they have nothing to clean up by hand.
    for phrase in [
        "Error: migrating '{}' failed at the final swap: {err}. The store is \\",
        "unchanged and still usable, its pre-migration backup is at '{}', and every \\",
        "artifact the migration generated has been removed.",
    ] {
        assert!(
            report.contains(phrase),
            "the at-swap message must keep the phrase {phrase:?}: {report}",
        );
    }

    // After the swap: the migration succeeded. Saying otherwise would send an
    // operator to restore a backup over a store that is already correct.
    for phrase in [
        "Error: '{}' was migrated, but replacement publication or final source close \\",
        "failed: {err}",
    ] {
        assert!(
            report.contains(phrase),
            "the after-swap message must keep the phrase {phrase:?}: {report}",
        );
    }
}

#[test]
fn every_situation_keeps_its_own_message_and_none_can_fall_through() {
    let report = failure_report(&command_source());
    for stage in [
        "MigrationFailureStage::BeforeSwap =>",
        "MigrationFailureStage::AtSwap =>",
        "MigrationFailureStage::AfterSwap =>",
    ] {
        assert_eq!(
            report.matches(stage).count(),
            1,
            "{stage} must select exactly one message",
        );
    }
    assert_eq!(
        report.matches("eprintln!(").count(),
        3,
        "three situations, three messages -- no two situations may share wording",
    );
    assert!(
        !report.contains("_ =>"),
        "a catch-all would let a new situation silently reuse another's message: {report}",
    );
}

/// The success line is what an operator sees on the ordinary path, and it is
/// built from the receipt rather than from anything the command still holds.
#[test]
fn a_finished_migration_reports_what_the_receipt_says() {
    let source = command_source();
    assert!(
        source.contains("let MigrationReceipt {"),
        "the command reads its counts out of the receipt the door returned",
    );
    for phrase in [
        "migrated '{}' in place ({applied_rows} rows from changeset + {keyless_rows_copied} \\",
        "migrated '{}' in place ({applied_rows} rows applied); the pre-migration store is \\",
        "keyless table '{table_name}': {copied} row(s) copied from current \\",
    ] {
        assert!(
            source.contains(phrase),
            "the success report must keep the phrase {phrase:?}",
        );
    }
    assert!(
        !source.contains("tmp_db"),
        "the command must not name a handle onto the store the door built",
    );
}
