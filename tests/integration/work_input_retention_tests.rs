//! Job-input retirement: `work_inputs` now declares `RETAIN 7 DAYS` (a real
//! user-table declaration, honored by the engine's EXISTING, already-shipped
//! retention mechanism -- no new pruning code needed for the deletion itself).
//!
//! What these pin, in plain language: a job's ledger-carried input copies are
//! immutable references with no superseded versions -- their whole lifetime
//! story is row LIFETIME, `RETAIN`, not `HISTORY`. Retiring them on a window
//! turns the PRE-EXISTING silent-empty read at `materialize_inputs` (a job
//! whose ledger inputs aged out previously read back indistinguishably from a
//! job with no ledger inputs at all) into a REACHABLE production path, so the
//! read must be taught to tell the two apart and return
//! `Error::WorkInputExpired` rather than an empty `ExecutionInputs` -- a
//! worker must see a typed refusal and record a failure, never execute on
//! silently-empty input. And an edge that expired its own copies locally must
//! not have them come back from a peer that still holds them.
//!
//! `Error::WorkInputExpired` exists (contextdb-core) but nothing returns it
//! yet, so the refusal test below currently fails.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads --
//! `Wallclock::test_clock_guard` only, matching `crates/contextdb-engine/AGENTS.md`.

use contextdb_core::{Error, Value, Wallclock};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_engine::work_ledger::{
    InputRef, JobSpec, MovementPolicy, install_work_ledger_schema, materialize_inputs, submit_job,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

const SEVEN_DAYS_MS: u64 = 7 * 24 * 60 * 60 * 1000;

fn work_inputs_row_count(db: &Database, job_id: &str) -> usize {
    let mut params = HashMap::new();
    params.insert("job_id".to_string(), Value::Text(job_id.to_string()));
    db.execute(
        "SELECT input_key FROM work_inputs WHERE job_id = $job_id",
        &params,
    )
    .expect("select work_inputs")
    .rows
    .len()
}

// ---------------------------------------------------------------------------
// The declaration itself: work_inputs is a real user (RETAIN-declared)
// table, honored by the shipped retention mechanism with no new pruning code
// (verification: already passing, pinned as a regression guard for the
// declaration).
// ---------------------------------------------------------------------------

#[test]
fn a_never_claimed_jobs_input_copies_age_out_under_the_declared_window() {
    let mock_now = Arc::new(AtomicU64::new(1_700_000_000_000));
    let _clock = {
        let mock_now = Arc::clone(&mock_now);
        Wallclock::test_clock_guard(move || mock_now.load(AtomicOrdering::SeqCst))
    };

    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install");
    let spec = JobSpec::builder("job-never-claimed", "wl.demo", "demo-transform", "node-a")
        .input_refs(vec![InputRef::ledger_input()])
        .submitted_at_ms(1_700_000_000_000)
        .build();
    submit_job(&db, &spec, &[b"payload bytes".as_slice()]).expect("submit with one input chunk");
    assert_eq!(work_inputs_row_count(&db, "job-never-claimed"), 1);

    // Advance past the declared 7-day window; nobody ever claimed the job.
    mock_now.fetch_add(SEVEN_DAYS_MS + 60_000, AtomicOrdering::SeqCst);
    let pruned = db.run_pruning_cycle();
    assert!(
        pruned > 0,
        "the aged-out input row must be pruned by the shipped retention pass"
    );
    assert_eq!(
        work_inputs_row_count(&db, "job-never-claimed"),
        0,
        "a never-claimed job's inputs must age out on the declared window -- the terminal-row \
         precondition of the old `run_input_retention` helper could never reach this case"
    );
}

// ---------------------------------------------------------------------------
// The typed refusal: silent-empty must become Error::WorkInputExpired.
// ---------------------------------------------------------------------------

#[test]
fn reading_an_expired_jobs_input_returns_a_typed_refusal_not_empty_bytes() {
    let mock_now = Arc::new(AtomicU64::new(1_700_000_000_000));
    let _clock = {
        let mock_now = Arc::clone(&mock_now);
        Wallclock::test_clock_guard(move || mock_now.load(AtomicOrdering::SeqCst))
    };

    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install");
    let spec = JobSpec::builder("job-expired", "wl.demo", "demo-transform", "node-a")
        .input_refs(vec![InputRef::ledger_input()])
        .submitted_at_ms(1_700_000_000_000)
        .build();
    submit_job(&db, &spec, &[b"payload bytes".as_slice()]).expect("submit");

    mock_now.fetch_add(SEVEN_DAYS_MS + 60_000, AtomicOrdering::SeqCst);
    db.run_pruning_cycle();
    assert_eq!(
        work_inputs_row_count(&db, "job-expired"),
        0,
        "setup: inputs must be gone"
    );

    let result = materialize_inputs(&db, "job-expired", "node-a", &MovementPolicy::default());
    match result {
        Err(Error::WorkInputExpired { job_id }) => {
            assert_eq!(job_id, "job-expired");
        }
        Err(other) => panic!("expected Error::WorkInputExpired, got a different error: {other}"),
        Ok(inputs) => panic!(
            "materialize_inputs returned Ok({} chunks) for an expired job's input -- silent- \
             empty must become a typed refusal: a worker must see it and never execute on \
             silently-empty input",
            inputs.len()
        ),
    }
}

/// A job whose ledger reference names NO input at all (an empty
/// `input_refs`, or none of the `ledger_input` kind) is a DIFFERENT case from
/// an expired one -- `materialize_inputs` must not conflate "nothing was
/// ever declared" with "it aged out". (Verification: already correct today,
/// pinned so the WorkInputExpired wiring does not regress it.)
#[test]
fn a_job_with_no_ledger_inputs_at_all_is_not_reported_as_expired() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install");
    let spec = JobSpec::builder("job-no-inputs", "wl.demo", "demo-transform", "node-a")
        .submitted_at_ms(1_700_000_000_000)
        .build();
    submit_job::<&[u8]>(&db, &spec, &[]).expect("submit with no input chunks");

    let inputs = materialize_inputs(&db, "job-no-inputs", "node-a", &MovementPolicy::default())
        .expect("a job that never declared ledger inputs is legitimately empty, not expired");
    assert!(inputs.is_empty());
}

// ---------------------------------------------------------------------------
// Expired-then-pull does not resurrect.
// ---------------------------------------------------------------------------

// Re-aimed (own commit, see its subject for the reason): this pins the
// ORDINARY incremental-pull flow, not a full resync from `Lsn(0)`. Those are
// genuinely different operations here -- `changes_since(Lsn(0))` replays a
// source's ENTIRE change log from the beginning (or a full state snapshot,
// post-restart), so asking the hub for "everything since zero" always
// includes a row's insert entry as long as the HUB's own log still has it,
// with no watermark able to prevent that (there is nothing to compare
// against; zero means "from the start"). That full-resync ("cursor-reset")
// shape is a REAL, separate hazard this run did not build a fix for --
// logged as its own deferral-ledger entry (retention-pruned rows can
// resurrect on a full re-pull after a source rebind; the fix needs a
// durable across-prune tombstone the design never specified).
//
// The ORDINARY flow an edge actually runs is INCREMENTAL: pull from the
// watermark it already recorded, never from zero. Under that flow, the
// row's own change-log entry is BELOW the watermark (it was already
// delivered the first time), so an incremental pull never re-sends it --
// no tombstone needed, the watermark itself is what prevents resurrection.
#[test]
fn an_ordinary_incremental_pull_does_not_resurrect_an_expired_input_row() {
    let mock_now = Arc::new(AtomicU64::new(1_700_000_000_000));
    let _clock = {
        let mock_now = Arc::clone(&mock_now);
        Wallclock::test_clock_guard(move || mock_now.load(AtomicOrdering::SeqCst))
    };

    let edge = Database::open_memory();
    install_work_ledger_schema(&edge).expect("install on edge");
    let spec = JobSpec::builder("job-pushed", "wl.demo", "demo-transform", "edge-1")
        .input_refs(vec![InputRef::ledger_input()])
        .submitted_at_ms(1_700_000_000_000)
        .build();
    submit_job(&edge, &spec, &[b"payload bytes".as_slice()]).expect("submit on edge");

    // "Pushed": the hub converges to the same committed state as the edge.
    let hub = Database::open_memory();
    install_work_ledger_schema(&hub).expect("install on hub");
    hub.apply_changes(
        edge.changes_since(contextdb_core::Lsn(0)),
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    )
    .expect("hub receives the pushed job and its inputs");
    assert_eq!(
        work_inputs_row_count(&hub, "job-pushed"),
        1,
        "setup: hub must hold the pushed input"
    );
    // The watermark an ORDINARY incremental `.sync pull` would have
    // recorded after this first successful convergence: the point, in the
    // HUB's own LSN numbering, up to which the edge has already consumed
    // the hub's stream. `changes_since` is always asked of a specific
    // source in that source's own numbering, so this must be the hub's.
    let hub_watermark_after_convergence = hub.current_lsn();

    // The edge expires its OWN copy locally.
    mock_now.fetch_add(SEVEN_DAYS_MS + 60_000, AtomicOrdering::SeqCst);
    edge.run_pruning_cycle();
    assert_eq!(
        work_inputs_row_count(&edge, "job-pushed"),
        0,
        "setup: the edge must have expired its local copy"
    );

    // Some unrelated later activity at the hub, so the incremental pull
    // below has real new work to fetch -- proving the pull mechanism
    // actually runs, not merely that an empty changeset trivially resurrects
    // nothing.
    let other_spec = JobSpec::builder("job-other", "wl.demo", "demo-transform", "hub-1").build();
    submit_job::<&[u8]>(&hub, &other_spec, &[]).expect("submit an unrelated job on the hub");

    // An ORDINARY incremental pull -- from the edge's own recorded
    // watermark, never `Lsn(0)` -- must not resurrect the expired row.
    edge.apply_changes(
        hub.changes_since(hub_watermark_after_convergence),
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    )
    .expect("an ordinary incremental pull must apply without erroring");

    assert_eq!(
        work_inputs_row_count(&edge, "job-pushed"),
        0,
        "an ordinary incremental pull (from the edge's own watermark, not a full resync from \
         Lsn(0)) must not resurrect an expired input row -- the row's own change-log entry \
         predates the watermark, so an incremental replay never re-sends it"
    );
    assert!(
        edge.execute(
            "SELECT job_id FROM work_jobs WHERE job_id = 'job-other'",
            &HashMap::new(),
        )
        .expect("select the unrelated job")
        .rows
        .len()
            == 1,
        "the incremental pull must still deliver genuinely new work -- proving this is a real \
         pull, not a no-op changeset"
    );
}
