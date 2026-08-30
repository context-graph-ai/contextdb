//! Journey: a writer holds the store and will not serve inspection.
//!
//! Contract held here: when the writer was started with `--no-owner-reads`,
//! the store still has a recorded owner, and that record says why inspection
//! is unavailable. A reading session is entitled to be told that — typed
//! `owner_not_serving`, carrying the recorded reason, exit 1 — rather than
//! being dropped onto the committed file where the writer's own lock produces
//! whatever the storage layer happened to say.
//!
//! The owner here is a genuinely separate process, because a recorded owner
//! state is only real when another process wrote it.

mod read_cli_support;

use read_cli_support::*;

#[test]
fn a_writer_that_will_not_serve_inspection_says_so_by_name() {
    let store = store_with(&create_seeded_table("held", 3));
    let _owner = live_owner_with(&store.path, &["--no-owner-reads"], "");

    let outcome = run(&[store.path_str(), "--json"], "SELECT id FROM held;\n");

    assert_eq!(
        outcome.code,
        Some(1),
        "a store nobody can inspect fails the run.\n{}",
        outcome.describe()
    );
    let refusal = outcome
        .errors()
        .into_iter()
        .find(|error| detail_kind(error).as_deref() == Some("owner_not_serving"))
        .unwrap_or_else(|| {
            panic!(
                "a writer holding the store with no usable inspection channel is \
                 owner_not_serving.\n{}",
                outcome.describe()
            )
        });
    assert_eq!(
        refusal.get("class").and_then(|class| class.as_str()),
        Some("io"),
        "the refusal is an I/O-class refusal, not a SQL complaint: {refusal}"
    );
    let reason = refusal
        .get("detail")
        .and_then(|detail| detail.get("reason"))
        .and_then(|reason| reason.as_str())
        .unwrap_or_else(|| panic!("the refusal carries the reason the writer recorded: {refusal}"));
    assert!(
        !reason.trim().is_empty(),
        "the recorded reason is a word a script can branch on: {refusal}"
    );
}

/// The control: nobody owns the store, so there is no recorded owner state to
/// report and the session reads the committed file exactly as it does today.
#[test]
fn a_store_nobody_owns_keeps_reading_from_the_file() {
    let store = store_with(&create_seeded_table("held", 3));

    let outcome = run(
        &[store.path_str(), "--json"],
        "SELECT id FROM held;\n.owner status\n",
    );

    assert_eq!(
        outcome.code,
        Some(0),
        "an unowned store reads without a writer.\n{}",
        outcome.describe()
    );
    assert!(
        !outcome
            .error_kinds()
            .iter()
            .any(|kind| kind == "owner_not_serving"),
        "an absent owner is not an unserved one.\n{}",
        outcome.describe()
    );
    let status = expect_document(
        &outcome.stdout_docs(),
        "owner",
        "the session reports the owner state it found",
    );
    assert_eq!(
        status.get("state").and_then(|state| state.as_str()),
        Some("not_running"),
        "nobody owns this store: {status}"
    );
}
