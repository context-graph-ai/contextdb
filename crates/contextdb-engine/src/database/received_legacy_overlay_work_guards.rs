// Included inside received_legacy_growth_guards (CDB-217). Applying a received
// legacy schema stage costs work linear in the versions it replaces: on the
// same funded production receiver and real signed unit as the lifecycle
// guards, the stage's manifest predecessor seeks are counted in both walks,
// the stored successors authenticated and skipped and the pending entries
// visited. Deterministic counters, no clocks.
use crate::persistence::content_identity::original_version_manifest::overlay_work;

/// The overlay work of the first apply of the restating stage over `count`
/// seeded rows, which the injected durable failure refuses after staging.
/// `check` asserts on the counters before either database closes.
fn first_apply_overlay_work(count: usize, shadow: bool, check: impl FnOnce(overlay_work::Work)) {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let (author, receiver, before, _retained_at) = seeded(root.path(), count);
    let delivery = Delivery::authored_by(&author, before);
    receiver
        .db
        .fail_next_received_schema_durable_commit_for_test();
    overlay_work::take();
    overlay_work::set_shadow(shadow);
    let outcome = delivery.apply(&receiver);
    overlay_work::set_shadow(false);
    let work = overlay_work::take();
    assert!(
        outcome.is_err(),
        "the injected durable failure refuses the unit after full staging"
    );
    drop(outcome);
    check(work);
    // CDB-224: closing after the refused stage can be refused at
    // redb/pending_directory. CDB-226: that memory refusal arrives flattened
    // to storage I/O, so this text match stands in for a typed match until
    // CDB-226 is fixed. Remove the acceptance when CDB-224 is fixed.
    if let Err(error) = receiver.db.close() {
        let accepted = matches!(&error, Error::Other(message)
            if message.starts_with("contextdb-engine-storage-io:")
                && message.contains("memory budget exceeded: redb/pending_directory"));
        assert!(accepted, "receiver close: {error:?}");
        eprintln!("CDB-224 receiver close refused after the failed stage: {error:?}");
    }
    drop(receiver);
    author.db.close().unwrap();
}

/// The declared counts (cdb217-bound-sheet-r2.md): V = 2N+1 versions; calls
/// 4V-2; stored skips V-1, none outside the caller's scope; pending entries
/// visited at most 2 per call (the row's one removed entry and the entry that
/// ends the scan); the paged fixture does no slice-overlay work.
fn assert_linear(count: usize, work: overlay_work::Work) {
    let versions = (2 * count + 1) as u64;
    eprintln!("CDB217 overlay work N={count} V={versions}: {work:?}");
    assert_eq!(work.calls, 4 * versions - 2, "seeks per stage");
    assert_eq!(
        work.stored_skips_outside_scope, 0,
        "no stored skip past the caller's scope"
    );
    assert_eq!(
        work.stored_skips,
        versions - 1,
        "one skip per row: its own removed version"
    );
    assert!(
        work.pending_visited <= 8 * versions - 4,
        "pending entries visited stay within two per seek"
    );
    assert_eq!(
        work.slice_visited, 0,
        "the paged stage does no slice-overlay work"
    );
}

#[test]
#[serial]
fn legacy_stage_overlay_steps_are_linear_in_rows_replaced_at_512() {
    first_apply_overlay_work(512, false, |work| assert_linear(512, work));
}

#[test]
#[serial]
fn legacy_stage_overlay_steps_are_linear_in_rows_replaced_at_2043() {
    assert_eq!(N, 2043, "the measured size");
    first_apply_overlay_work(N, false, |work| assert_linear(N, work));
}

#[test]
#[serial]
fn legacy_stage_scoped_overlay_answers_as_the_unbounded_walk_at_512() {
    first_apply_overlay_work(512, true, |work| {
        eprintln!("CDB217 overlay shadow N=512: {work:?}");
        assert_eq!(
            work.shadow_compared, work.calls,
            "every seek of the stage was shadowed"
        );
        assert!(work.shadow_compared > 0);
        assert_eq!(
            work.shadow_mismatches, 0,
            "the scoped walk gives each caller the unbounded walk's in-scope answer and authenticates the same in-scope members"
        );
    });
}
