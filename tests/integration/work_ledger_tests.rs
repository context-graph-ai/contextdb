//! Work ledger — engine-half contract tests covering job submission, claims
//! and leases, input materialization, input retention, and conflict-policy
//! entries. Everything here runs on one in-memory database: no sync, no
//! network, no fake transport. The distributed half (claims-by-push over the
//! in-process fake) lives in
//! `crates/contextdb-server/tests/work_ledger_tests.rs`.

use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_engine::work_ledger::{
    ClaimInsert, InputRef, JobSpec, JobSpecBuilder, JobState, MovementPolicy, WORK_LEDGER_TABLES,
    add_job_input_in_tx, advertise_capability, advertised_tags, apply_work_ledger_policy_overrides,
    cancel_job, claim_holder, claimable_jobs, insert_claim, install_work_ledger_schema, job_result,
    job_snapshot, job_state, materialize_inputs, next_attempt, record_failure, record_result,
    run_input_retention, should_abandon, submit_job, submit_job_in_tx,
    work_ledger_conflict_policy_entries,
};
use std::collections::HashMap;

const T0: i64 = 1_700_000_000_000;
const MINUTE: i64 = 60_000;

fn ledger_db() -> Database {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install work ledger schema");
    db
}

fn tags(list: &[&str]) -> Vec<String> {
    list.iter().map(|t| t.to_string()).collect()
}

fn spec_builder(
    job_id: &str,
    submitter: &str,
    requirement_tags: &[&str],
    refs: Vec<InputRef>,
) -> JobSpecBuilder {
    JobSpec::builder(job_id, "wl.demo", "demo-transform", submitter)
        .requirement_tags(tags(requirement_tags))
        .input_refs(refs)
        .output_schema(Some("demo-output-v1".to_string()))
        .priority(5)
        .provenance(Some(serde_json::json!({ "origin": "engine-tests" })))
        .submitted_at_ms(T0)
}

fn spec(job_id: &str, submitter: &str, requirement_tags: &[&str], refs: Vec<InputRef>) -> JobSpec {
    spec_builder(job_id, submitter, requirement_tags, refs).build()
}

fn count_rows(db: &Database, table: &str) -> usize {
    db.execute(&format!("SELECT * FROM {table}"), &HashMap::new())
        .unwrap_or_else(|err| panic!("scan {table}: {err}"))
        .rows
        .len()
}

const OPEN_POLICY: MovementPolicy = MovementPolicy {
    auto_propagate: true,
};
const CLOSED_POLICY: MovementPolicy = MovementPolicy {
    auto_propagate: false,
};

// ---------------------------------------------------------------------------
// wl_e01 — install (criterion C6 substrate: everything runs with no network)
// ---------------------------------------------------------------------------

#[test]
fn wl_e01_install_creates_all_tables_and_is_idempotent() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("first install");
    let names = db.table_names();
    for table in WORK_LEDGER_TABLES {
        assert!(
            names.iter().any(|n| n == table),
            "install must create {table}; tables present: {names:?}"
        );
    }

    // A second install is a no-op, and existing rows survive it.
    submit_job(
        &db,
        &spec(
            "job-e01",
            "node-a",
            &["class:wl.demo"],
            vec![InputRef::ledger_input()],
        ),
        &[b"chunk"],
    )
    .expect("submit before reinstall");
    install_work_ledger_schema(&db).expect("second install is a no-op");
    assert_eq!(
        count_rows(&db, "work_jobs"),
        1,
        "reinstall must not drop or duplicate existing ledger rows"
    );
}

// ---------------------------------------------------------------------------
// wl_e02 — atomic submission with the caller's own writes (criterion C10),
// and payload opacity (criterion C5): bytes in, identical bytes out.
// ---------------------------------------------------------------------------

#[test]
fn wl_e02_submission_commits_atomically_with_caller_writes_and_rolls_back_whole() {
    let db = ledger_db();
    db.execute(
        "CREATE TABLE caller_notes (id TEXT PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .expect("caller table");

    // Non-UTF8 payloads: the ledger must round-trip raw bytes untouched.
    let chunk_a: Vec<u8> = vec![0, 159, 146, 150, 255, 10, 13];
    let chunk_b: Vec<u8> = b"plain text chunk".to_vec();

    let tx = db.begin().expect("begin");
    let mut note = HashMap::new();
    note.insert("id".to_string(), contextdb_core::Value::Text("n1".into()));
    note.insert(
        "body".to_string(),
        contextdb_core::Value::Text("caller row in the same tx".into()),
    );
    db.execute_in_tx(
        tx,
        "INSERT INTO caller_notes (id, body) VALUES ($id, $body)",
        &note,
    )
    .expect("caller insert in tx");
    submit_job_in_tx(
        &db,
        tx,
        &spec(
            "job-e02",
            "node-a",
            &["class:wl.demo"],
            vec![InputRef::ledger_input()],
        ),
    )
    .expect("submit in tx");
    add_job_input_in_tx(&db, tx, "job-e02", 0, &chunk_a).expect("input 0 in tx");
    add_job_input_in_tx(&db, tx, "job-e02", 1, &chunk_b).expect("input 1 in tx");
    db.commit(tx).expect("commit");

    let snapshot = job_snapshot(&db, "job-e02")
        .expect("snapshot read")
        .expect("job row committed");
    assert_eq!(snapshot.work_class, "wl.demo");
    assert_eq!(snapshot.submitter_node_id, "node-a");
    assert_eq!(snapshot.input_refs, vec![InputRef::ledger_input()]);
    assert_eq!(count_rows(&db, "caller_notes"), 1);

    let inputs = materialize_inputs(&db, "job-e02", "node-a", &CLOSED_POLICY)
        .expect("submitter reads own inputs under a closed policy");
    assert_eq!(
        inputs,
        vec![(0, chunk_a.clone()), (1, chunk_b.clone())],
        "payload bytes must round-trip exactly, in sequence order"
    );

    // A rolled-back submission leaves NOTHING: no job, no inputs.
    let tx2 = db.begin().expect("begin 2");
    submit_job_in_tx(
        &db,
        tx2,
        &spec(
            "job-e02-rollback",
            "node-a",
            &["class:wl.demo"],
            vec![InputRef::ledger_input()],
        ),
    )
    .expect("submit in tx 2");
    add_job_input_in_tx(&db, tx2, "job-e02-rollback", 0, b"never committed")
        .expect("input in tx 2");
    db.rollback(tx2).expect("rollback");
    assert!(
        job_snapshot(&db, "job-e02-rollback")
            .expect("snapshot read")
            .is_none(),
        "a rolled-back submission must leave no job row"
    );
    assert_eq!(
        count_rows(&db, "work_inputs"),
        2,
        "a rolled-back submission must leave no input rows"
    );
}

// ---------------------------------------------------------------------------
// wl_e03 — job state is computed from rows per the fixed rules (criterion C4)
// ---------------------------------------------------------------------------

#[test]
fn wl_e03_job_state_follows_the_fixed_rules() {
    let db = ledger_db();
    submit_job(
        &db,
        &spec(
            "job-e03",
            "node-a",
            &["class:wl.demo"],
            vec![InputRef::ledger_input()],
        ),
        &[b"x"],
    )
    .expect("submit");

    assert_eq!(
        job_state(&db, "job-e03", T0 + 1).expect("state"),
        JobState::Pending,
        "a job with no claim, result, failure, or cancellation is pending"
    );

    let deadline = T0 + MINUTE;
    assert_eq!(
        insert_claim(&db, "job-e03", 1, "node-b", deadline, T0 + 2).expect("claim"),
        ClaimInsert::Inserted
    );
    assert_eq!(
        job_state(&db, "job-e03", T0 + 3).expect("state"),
        JobState::Leased {
            node_id: "node-b".to_string(),
            attempt: 1,
            lease_deadline_ms: deadline,
        },
        "a live claim on the highest attempt means leased"
    );

    assert_eq!(
        job_state(&db, "job-e03", deadline + 1).expect("state"),
        JobState::Pending,
        "a passed lease deadline puts the job back up for grabs"
    );

    record_failure(&db, "job-e03", 1, "node-b", "simulated error", deadline + 2)
        .expect("failure attempt 1");
    assert_eq!(
        job_state(&db, "job-e03", deadline + 3).expect("state"),
        JobState::Pending,
        "one failure below the ceiling of 2 keeps the job pending"
    );

    insert_claim(&db, "job-e03", 2, "node-c", deadline + MINUTE, deadline + 4).expect("claim 2");
    record_result(
        &db,
        "job-e03",
        2,
        "node-c",
        b"result bytes",
        serde_json::json!({ "elapsed_ms": 7 }),
        deadline + 5,
    )
    .expect("result");
    assert_eq!(
        job_state(&db, "job-e03", deadline + 6).expect("state"),
        JobState::Done,
        "a result row means done"
    );

    // A cancellation that loses the race with the result is a no-op.
    cancel_job(&db, "job-e03", "node-a", Some("too late"), deadline + 7).expect("late cancel");
    assert_eq!(
        job_state(&db, "job-e03", deadline + 8).expect("state"),
        JobState::Done,
        "a cancellation arriving after the result must not shadow it"
    );

    // Cancellation before any result is terminal.
    submit_job(
        &db,
        &spec(
            "job-e03-cancel",
            "node-a",
            &["class:wl.demo"],
            vec![InputRef::ledger_input()],
        ),
        &[b"y"],
    )
    .expect("submit cancellable");
    cancel_job(&db, "job-e03-cancel", "node-a", None, T0 + 10).expect("cancel");
    assert_eq!(
        job_state(&db, "job-e03-cancel", T0 + 11).expect("state"),
        JobState::Cancelled
    );

    // Failure rows at the ceiling are terminal.
    let one_shot = spec_builder(
        "job-e03-fail",
        "node-a",
        &["class:wl.demo"],
        vec![InputRef::ledger_input()],
    )
    .max_attempts(1)
    .build();
    submit_job(&db, &one_shot, &[b"z"]).expect("submit one-shot");
    record_failure(&db, "job-e03-fail", 1, "node-b", "boom", T0 + 20).expect("failure");
    assert_eq!(
        job_state(&db, "job-e03-fail", T0 + 21).expect("state"),
        JobState::Failed,
        "failure rows at max_attempts mean failed, never silently retried"
    );
}

// ---------------------------------------------------------------------------
// wl_e04 — retention deletes ONLY expired input copies and can never change
// a job's computed state (criterion C7)
// ---------------------------------------------------------------------------

#[test]
fn wl_e04_retention_deletes_only_terminal_inputs_past_grace_and_never_changes_state() {
    let db = ledger_db();
    let grace = 10 * MINUTE;

    submit_job(
        &db,
        &spec(
            "job-done",
            "node-a",
            &["class:wl.demo"],
            vec![InputRef::ledger_input()],
        ),
        &[b"d0", b"d1"],
    )
    .expect("submit done-job");
    insert_claim(&db, "job-done", 1, "node-b", T0 + MINUTE, T0 + 1).expect("claim");
    let completed_at = T0 + 2;
    record_result(
        &db,
        "job-done",
        1,
        "node-b",
        b"out",
        serde_json::json!({}),
        completed_at,
    )
    .expect("result");

    submit_job(
        &db,
        &spec(
            "job-open",
            "node-a",
            &["class:wl.demo"],
            vec![InputRef::ledger_input()],
        ),
        &[b"o0"],
    )
    .expect("submit open-job");

    submit_job(
        &db,
        &spec(
            "job-cancelled",
            "node-a",
            &["class:wl.demo"],
            vec![InputRef::ledger_input()],
        ),
        &[b"c0"],
    )
    .expect("submit cancelled-job");
    cancel_job(&db, "job-cancelled", "node-a", None, completed_at).expect("cancel");

    // Inside the grace period nothing is deleted.
    assert_eq!(
        run_input_retention(&db, completed_at + grace - 1, grace).expect("retention early"),
        0,
        "retention must respect the grace period"
    );
    assert_eq!(count_rows(&db, "work_inputs"), 4);

    // Past grace: the done and cancelled jobs' inputs go; the open job's stay.
    let deleted = run_input_retention(&db, completed_at + grace + 1, grace).expect("retention due");
    assert_eq!(deleted, 3, "two done-job chunks + one cancelled-job chunk");
    assert_eq!(
        count_rows(&db, "work_inputs"),
        1,
        "the open job's input must survive"
    );

    // The deletion provably changed no computed state, and the result stands.
    assert_eq!(
        job_state(&db, "job-done", completed_at + grace + 2).expect("state"),
        JobState::Done
    );
    assert_eq!(
        job_state(&db, "job-open", completed_at + grace + 2).expect("state"),
        JobState::Pending
    );
    assert_eq!(
        job_state(&db, "job-cancelled", completed_at + grace + 2).expect("state"),
        JobState::Cancelled
    );
    let result = job_result(&db, "job-done")
        .expect("read result")
        .expect("result row");
    assert_eq!(result.output, b"out".to_vec());
}

// ---------------------------------------------------------------------------
// wl_e05 — capability registry + matching over the open vocabulary
// (criteria C9, C11), including ordering.
// ---------------------------------------------------------------------------

#[test]
fn wl_e05_capabilities_match_by_tag_subset_reference_kind_and_policy() {
    let db = ledger_db();

    advertise_capability(
        &db,
        "node-b",
        "wl.demo",
        &tags(&["class:wl.demo", "output:hex"]),
        T0,
    )
    .expect("advertise");
    advertise_capability(
        &db,
        "node-b",
        "wl.demo",
        &tags(&["class:wl.demo", "output:hex"]),
        T0 + 1,
    )
    .expect("re-advertising the same key upserts the one row (no second row)");
    assert_eq!(count_rows(&db, "work_capabilities"), 1);
    advertise_capability(
        &db,
        "node-b",
        "wl.extra",
        &tags(&["ref:ledger_input"]),
        T0 + 2,
    )
    .expect("second capability");
    let mut advertised = advertised_tags(&db, "node-b").expect("tags");
    advertised.sort();
    assert_eq!(
        advertised,
        tags(&["class:wl.demo", "output:hex", "ref:ledger_input"]),
        "advertised tags are the union across the node's capability rows"
    );

    submit_job(
        &db,
        &spec(
            "job-match",
            "node-a",
            &["class:wl.demo", "output:hex"],
            vec![InputRef::ledger_input()],
        ),
        &[b"m"],
    )
    .expect("matching job");
    submit_job(
        &db,
        &spec(
            "job-too-demanding",
            "node-a",
            &["class:wl.demo", "output:native"],
            vec![InputRef::ledger_input()],
        ),
        &[b"m"],
    )
    .expect("unmatchable job");
    const NO_INPUTS: &[&[u8]] = &[];
    submit_job(
        &db,
        &spec(
            "job-local-only",
            "node-a",
            &["class:wl.demo"],
            vec![InputRef::local_path("/tmp/x")],
        ),
        NO_INPUTS,
    )
    .expect("local-path job");

    let b_tags = advertised_tags(&db, "node-b").expect("tags");
    let visible = claimable_jobs(&db, "node-b", &b_tags, &OPEN_POLICY, T0 + 5).expect("claimable");
    let ids: Vec<&str> = visible.iter().map(|j| j.job_id.as_str()).collect();
    assert_eq!(
        ids,
        vec!["job-match"],
        "tag-subset must hold, and a local_path job is never claimable off the submitting node"
    );

    // The movement policy withholds foreign input at claim time.
    let closed =
        claimable_jobs(&db, "node-b", &b_tags, &CLOSED_POLICY, T0 + 5).expect("claimable closed");
    assert!(
        closed.is_empty(),
        "with auto_propagate off a node may not claim another node's ledger-input job"
    );

    // The submitter itself may always claim its own jobs, policy regardless —
    // and sees its local_path job too (given tags match).
    advertise_capability(
        &db,
        "node-a",
        "wl.demo",
        &tags(&["class:wl.demo", "output:hex", "output:native"]),
        T0,
    )
    .expect("submitter advertises");
    let a_tags = advertised_tags(&db, "node-a").expect("tags");
    let own =
        claimable_jobs(&db, "node-a", &a_tags, &CLOSED_POLICY, T0 + 5).expect("own claimable");
    let mut own_ids: Vec<&str> = own.iter().map(|j| j.job_id.as_str()).collect();
    own_ids.sort();
    assert_eq!(
        own_ids,
        vec!["job-local-only", "job-match", "job-too-demanding"]
    );

    // The movement policy also gates the read itself, not only the claim.
    let denied = materialize_inputs(&db, "job-match", "node-b", &CLOSED_POLICY);
    assert!(
        denied.is_err(),
        "reading a foreign job's input under a closed policy must be refused"
    );

    // Leased/terminal jobs leave the claimable list.
    insert_claim(&db, "job-match", 1, "node-b", T0 + 10 * MINUTE, T0 + 6).expect("claim");
    assert!(
        claimable_jobs(&db, "node-b", &b_tags, &OPEN_POLICY, T0 + 7)
            .expect("claimable")
            .is_empty(),
        "a leased job is not claimable"
    );
}

#[test]
fn wl_e05b_claimable_ordering_is_priority_then_deadline_then_age() {
    let db = ledger_db();
    advertise_capability(&db, "node-b", "wl.demo", &tags(&["class:wl.demo"]), T0)
        .expect("advertise");

    let low = spec_builder(
        "job-low",
        "node-a",
        &["class:wl.demo"],
        vec![InputRef::ledger_input()],
    )
    .priority(1)
    .submitted_at_ms(T0)
    .build();
    let high_late = spec_builder(
        "job-high-late",
        "node-a",
        &["class:wl.demo"],
        vec![InputRef::ledger_input()],
    )
    .priority(9)
    .deadline_ms(Some(T0 + 100 * MINUTE))
    .submitted_at_ms(T0 + 1)
    .build();
    let high_soon = spec_builder(
        "job-high-soon",
        "node-a",
        &["class:wl.demo"],
        vec![InputRef::ledger_input()],
    )
    .priority(9)
    .deadline_ms(Some(T0 + 10 * MINUTE))
    .submitted_at_ms(T0 + 2)
    .build();
    let high_nodeadline_old = spec_builder(
        "job-high-old",
        "node-a",
        &["class:wl.demo"],
        vec![InputRef::ledger_input()],
    )
    .priority(9)
    .submitted_at_ms(T0 + 3)
    .build();
    let high_nodeadline_new = spec_builder(
        "job-high-new",
        "node-a",
        &["class:wl.demo"],
        vec![InputRef::ledger_input()],
    )
    .priority(9)
    .submitted_at_ms(T0 + 4)
    .build();

    for s in [
        &low,
        &high_late,
        &high_soon,
        &high_nodeadline_old,
        &high_nodeadline_new,
    ] {
        submit_job(&db, s, &[b"i"]).expect("submit");
    }

    let b_tags = advertised_tags(&db, "node-b").expect("tags");
    let ordered = claimable_jobs(&db, "node-b", &b_tags, &OPEN_POLICY, T0 + 5).expect("claimable");
    let ids: Vec<&str> = ordered.iter().map(|j| j.job_id.as_str()).collect();
    assert_eq!(
        ids,
        vec![
            "job-high-soon",
            "job-high-late",
            "job-high-old",
            "job-high-new",
            "job-low"
        ],
        "order: priority desc, then deadline asc with none last, then submission age"
    );
}

// ---------------------------------------------------------------------------
// wl_e06 — attempt numbering + the local half of the lease (criterion C2/C3
// substrate)
// ---------------------------------------------------------------------------

#[test]
fn wl_e06_next_attempt_counts_claims_and_failures_and_local_claim_arbitrates() {
    let db = ledger_db();
    submit_job(
        &db,
        &spec(
            "job-e06",
            "node-a",
            &["class:wl.demo"],
            vec![InputRef::ledger_input()],
        ),
        &[b"x"],
    )
    .expect("submit");

    assert_eq!(next_attempt(&db, "job-e06").expect("fresh"), 1);

    assert_eq!(
        insert_claim(&db, "job-e06", 1, "node-b", T0 + MINUTE, T0).expect("claim"),
        ClaimInsert::Inserted
    );
    assert_eq!(
        insert_claim(&db, "job-e06", 1, "node-c", T0 + MINUTE, T0 + 1).expect("second local claim"),
        ClaimInsert::AlreadyHeld {
            holder: "node-b".to_string()
        },
        "the primary key is the same-machine half of the lease"
    );
    assert_eq!(
        claim_holder(&db, "job-e06", 1).expect("holder"),
        Some("node-b".to_string())
    );
    assert_eq!(next_attempt(&db, "job-e06").expect("after claim"), 2);

    record_failure(&db, "job-e06", 2, "node-b", "err", T0 + 2).expect("failure on attempt 2");
    assert_eq!(
        next_attempt(&db, "job-e06").expect("after failure"),
        3,
        "next attempt is one past the highest attempt any claim or failure records"
    );
}

// ---------------------------------------------------------------------------
// wl_e07 — exactly-once result; idempotent failure/cancel writes
// (criterion C2 substrate)
// ---------------------------------------------------------------------------

#[test]
fn wl_e07_result_is_exactly_once_and_duplicate_writes_are_noops() {
    let db = ledger_db();
    submit_job(
        &db,
        &spec(
            "job-e07",
            "node-a",
            &["class:wl.demo"],
            vec![InputRef::ledger_input()],
        ),
        &[b"x"],
    )
    .expect("submit");

    record_result(
        &db,
        "job-e07",
        1,
        "node-b",
        b"first",
        serde_json::json!({"n": 1}),
        T0,
    )
    .expect("first result");
    record_result(
        &db,
        "job-e07",
        2,
        "node-c",
        b"second",
        serde_json::json!({"n": 2}),
        T0 + 1,
    )
    .expect("duplicate result is a caught no-op, not an error");

    assert_eq!(
        count_rows(&db, "work_results"),
        1,
        "exactly one result row ever exists"
    );
    let result = job_result(&db, "job-e07").expect("read").expect("row");
    assert_eq!(result.output, b"first".to_vec(), "the first result stands");
    assert_eq!(result.executor_node_id, "node-b");
    assert_eq!(result.receipt, serde_json::json!({"n": 1}));

    record_failure(&db, "job-e07", 1, "node-b", "dup", T0 + 2).expect("failure once");
    record_failure(&db, "job-e07", 1, "node-b", "dup again", T0 + 3).expect("failure twice");
    assert_eq!(
        count_rows(&db, "work_failures"),
        1,
        "same (job, attempt) failure writes once"
    );

    cancel_job(&db, "job-e07", "node-a", None, T0 + 4).expect("cancel once");
    cancel_job(&db, "job-e07", "node-a", None, T0 + 5).expect("cancel twice");
    assert_eq!(count_rows(&db, "work_cancellations"), 1);
}

// ---------------------------------------------------------------------------
// wl_e08 — the abandonment check a long-running worker consults between
// chunks (criterion C2 substrate)
// ---------------------------------------------------------------------------

#[test]
fn wl_e08_should_abandon_on_result_cancellation_or_lost_claim_but_not_expiry() {
    let db = ledger_db();
    submit_job(
        &db,
        &spec(
            "job-e08",
            "node-a",
            &["class:wl.demo"],
            vec![InputRef::ledger_input()],
        ),
        &[b"x"],
    )
    .expect("submit");
    insert_claim(&db, "job-e08", 1, "node-b", T0 + MINUTE, T0).expect("claim");

    assert!(
        !should_abandon(&db, "job-e08", 1, "node-b", T0 + 1).expect("live claim"),
        "a live claim with no result/cancellation keeps working"
    );
    assert!(
        !should_abandon(&db, "job-e08", 1, "node-b", T0 + 10 * MINUTE).expect("expired"),
        "lease expiry alone is not abandonment: a revived worker may finish; the result stays exactly-once by key"
    );

    // A claim reconciled under the worker to another holder means stop.
    assert!(
        should_abandon(&db, "job-e08", 1, "node-z", T0 + 2).expect("lost race"),
        "when the claim row names a different holder, the worker lost the race and must stop"
    );

    cancel_job(&db, "job-e08", "node-a", None, T0 + 3).expect("cancel");
    assert!(
        should_abandon(&db, "job-e08", 1, "node-b", T0 + 4).expect("cancelled"),
        "a cancellation row means stop"
    );

    submit_job(
        &db,
        &spec(
            "job-e08b",
            "node-a",
            &["class:wl.demo"],
            vec![InputRef::ledger_input()],
        ),
        &[b"y"],
    )
    .expect("submit b");
    insert_claim(&db, "job-e08b", 1, "node-b", T0 + MINUTE, T0).expect("claim b");
    record_result(
        &db,
        "job-e08b",
        1,
        "node-b",
        b"done",
        serde_json::json!({}),
        T0 + 5,
    )
    .expect("result");
    assert!(
        should_abandon(&db, "job-e08b", 1, "node-b", T0 + 6).expect("done"),
        "an existing result means stop — the work is already done"
    );
}

// ---------------------------------------------------------------------------
// wl_e09 — the per-table conflict contract (criterion C8 substrate)
// ---------------------------------------------------------------------------

#[test]
fn wl_e09_policy_entries_cover_every_table_and_overrides_win() {
    let entries = work_ledger_conflict_policy_entries();
    let by_table: HashMap<&str, ConflictPolicy> = entries.iter().copied().collect();
    assert_eq!(
        by_table.len(),
        WORK_LEDGER_TABLES.len(),
        "one entry per ledger table"
    );
    for table in WORK_LEDGER_TABLES {
        assert!(
            by_table.contains_key(table),
            "missing policy entry for {table}"
        );
    }
    // Hub-refereed tables: first row to the hub stands; later writers get an
    // attributed conflict; losing edges reconcile on pull.
    for table in ["work_claims", "work_results", "work_cancellations"] {
        assert_eq!(
            by_table[table],
            ConflictPolicy::ServerWins,
            "{table} must be hub-refereed"
        );
    }
    // Single-writer-per-key tables: idempotent re-delivery.
    for table in ["work_jobs", "work_failures", "work_inputs"] {
        assert_eq!(
            by_table[table],
            ConflictPolicy::InsertIfNotExists,
            "{table} is single-writer-per-key"
        );
    }
    // The capability registry is current-truth, not single-writer-per-key: a
    // re-advertise refreshes `advertised_at`, and LatestWins (ordered by
    // LSN/TxId) makes that newer claim win at the hub so the fleet converges on
    // the node's present offer — InsertIfNotExists would drop every refresh.
    assert_eq!(
        by_table["work_capabilities"],
        ConflictPolicy::LatestWins,
        "work_capabilities is a current-truth registry: the latest advertisement wins"
    );

    // The overrides replace wrong per-table entries and leave everything else.
    let mut policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);
    policies
        .per_table
        .insert("work_claims".to_string(), ConflictPolicy::LatestWins);
    policies
        .per_table
        .insert("user_table".to_string(), ConflictPolicy::EdgeWins);
    apply_work_ledger_policy_overrides(&mut policies);
    assert_eq!(
        policies.per_table["work_claims"],
        ConflictPolicy::ServerWins
    );
    assert_eq!(
        policies.per_table["work_jobs"],
        ConflictPolicy::InsertIfNotExists
    );
    assert_eq!(
        policies.per_table["user_table"],
        ConflictPolicy::EdgeWins,
        "non-ledger tables are untouched"
    );
    assert_eq!(
        policies.default,
        ConflictPolicy::LatestWins,
        "the default is untouched"
    );
}

// ---------------------------------------------------------------------------
// wl_g01/wl_g02 — source-scan guards (criteria C4, C5): the workflow stays
// append-only, class-blind, and transport-blind.
// ---------------------------------------------------------------------------

const ENGINE_LEDGER_SOURCE: &str = include_str!("../../crates/contextdb-engine/src/work_ledger.rs");
const SERVER_LEDGER_SOURCE: &str = include_str!("../../crates/contextdb-server/src/work_ledger.rs");

// ---------------------------------------------------------------------------
// wl_e10 — capability currency: a node's CURRENT capability is its
// LATEST-advertised row. Re-advertising an existing (node, capability) key
// must refresh advertised_at (upsert), so a worker restarted onto a
// different-but-same-id backend, or a live promotion, makes the newer claim
// current instead of being silently dropped by the write-once no-op.
// (capability currency refresh.)
// ---------------------------------------------------------------------------

fn capability_advertised_at(db: &Database, node_id: &str, capability_id: &str) -> Option<i64> {
    let key = format!("{node_id}#{capability_id}");
    let mut params = HashMap::new();
    params.insert(
        "capability_key".to_string(),
        contextdb_core::Value::Text(key),
    );
    let result = db
        .execute(
            "SELECT advertised_at FROM work_capabilities WHERE capability_key = $capability_key",
            &params,
        )
        .expect("scan work_capabilities advertised_at");
    let idx = result.columns.iter().position(|c| c == "advertised_at")?;
    match result.rows.first()?.get(idx)? {
        contextdb_core::Value::Timestamp(ms) => Some(*ms),
        _ => None,
    }
}

#[test]
fn wl_e10_re_advertising_the_same_key_refreshes_advertised_at() {
    let db = ledger_db();

    advertise_capability(&db, "node-b", "wl.demo", &tags(&["class:wl.demo"]), T0)
        .expect("first advertise");
    assert_eq!(
        capability_advertised_at(&db, "node-b", "wl.demo"),
        Some(T0),
        "the first advertisement records T0"
    );

    // The node re-advertises the SAME (node, capability) key later — a
    // restart onto a different backend, or a promotion re-driving the loop.
    advertise_capability(
        &db,
        "node-b",
        "wl.demo",
        &tags(&["class:wl.demo"]),
        T0 + MINUTE,
    )
    .expect("re-advertise same key later");

    // Still exactly one row (an upsert, not a second row) ...
    assert_eq!(count_rows(&db, "work_capabilities"), 1);
    // ... but its advertised_at reflects the LATER call — that is what makes
    // the newer claim current. Write-once drops the refresh (stays at T0).
    assert_eq!(
        capability_advertised_at(&db, "node-b", "wl.demo"),
        Some(T0 + MINUTE),
        "re-advertising an existing key must refresh advertised_at to the later call"
    );
}

#[test]
fn wl_g01_workflow_source_is_append_only_except_input_retention() {
    // Workflow HISTORY is append-only: jobs/claims/results/failures/
    // cancellations/inputs only ever gain rows, so a recorded step is never
    // rewritten. The capability/contact REGISTRY is current-truth, not
    // history — `work_capabilities` (advertise_capability) and
    // `work_node_contacts` (the hub last-contact recorder) carry a node's
    // *present* offer / last contact, so they upsert by design. Those two
    // tables are exempt BY NAME: the only UPDATEs permitted anywhere in the
    // ledger are the `DO UPDATE SET` arm of an `INSERT ... ON CONFLICT`
    // upsert whose INSERT targets a registry table. Any other UPDATE — a
    // standalone `UPDATE <history-table> SET`, or an upsert on a history
    // table — is a workflow-history mutation and a defect.
    const REGISTRY_UPSERT_TABLES: [&str; 2] = ["work_capabilities", "work_node_contacts"];
    for source in [ENGINE_LEDGER_SOURCE, SERVER_LEDGER_SOURCE] {
        for (idx, _) in source.match_indices("UPDATE ") {
            // Every permitted UPDATE is the ON-CONFLICT upsert arm: the three
            // characters before it are `DO `. A standalone `UPDATE <table> SET`
            // (a history mutation) fails this immediately.
            let is_on_conflict_arm = idx >= 3 && &source[idx - 3..idx] == "DO ";
            // ...and its enclosing `INSERT INTO` must name a registry table.
            let enclosing_insert = source[..idx]
                .rfind("INSERT INTO ")
                .map(|start| &source[start..idx]);
            let targets_registry = enclosing_insert.is_some_and(|stmt| {
                stmt.contains("ON CONFLICT")
                    && REGISTRY_UPSERT_TABLES
                        .iter()
                        .any(|table| stmt.contains(table))
            });
            assert!(
                is_on_conflict_arm && targets_registry,
                "the only UPDATEs permitted in the ledger are the current-truth \
                 capability/contact registry upserts (work_capabilities / \
                 work_node_contacts, each an INSERT ... ON CONFLICT DO UPDATE SET); \
                 workflow history stays append-only. Found a non-registry UPDATE near: {}",
                &source[idx..(idx + 72).min(source.len())]
            );
        }
    }
    let deletes: Vec<&str> = ENGINE_LEDGER_SOURCE
        .lines()
        .filter(|line| line.contains("DELETE FROM"))
        .collect();
    assert_eq!(
        deletes.len(),
        1,
        "exactly one DELETE lives in the ledger: input retention; found {deletes:?}"
    );
    assert!(
        deletes[0].contains("work_inputs"),
        "the single DELETE must target work_inputs, found {deletes:?}"
    );
    assert_eq!(
        SERVER_LEDGER_SOURCE.matches("DELETE FROM").count(),
        0,
        "the distributed half issues no deletes at all"
    );
}

#[test]
fn wl_g02_ledger_source_is_class_blind_and_transport_blind() {
    // Sanity: the scan is reading the real module sources.
    assert!(ENGINE_LEDGER_SOURCE.contains("work_jobs"));
    assert!(SERVER_LEDGER_SOURCE.contains("poll_and_execute_once"));

    for (name, source) in [
        ("engine", ENGINE_LEDGER_SOURCE),
        ("server", SERVER_LEDGER_SOURCE),
    ] {
        let lower = source.to_lowercase();
        // The dial-by-key transport's name is built by concatenation so the
        // workspace-wide transport-word sweep does not count this guard's
        // own needle list as a violation.
        let dial_by_key_transport = ["ir", "oh"].concat();
        for forbidden in [
            "nats",
            dial_by_key_transport.as_str(),
            "relay",
            "broker", // transport words
            "understanding",
            "detector",
            "ontology",
            "digest",
            "decision",
            "intention",
            "embedding",
            "vision", // work-class / product words
        ] {
            assert!(
                !lower.contains(forbidden),
                "the {name} ledger source must not mention {forbidden:?} — the ledger is class- and transport-blind"
            );
        }
    }
}
