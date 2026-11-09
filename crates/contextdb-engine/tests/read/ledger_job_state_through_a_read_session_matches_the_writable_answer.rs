#![cfg(unix)]

//! Read-only ledger surfaces must match writable answers.
//!
//! This proof lives in the ordinary `read` root so the public API cannot
//! regress behind an unselected feature or standalone target.

use contextdb_core::read_contract::ReadRoute;
use contextdb_engine::work_ledger::{
    ClaimInsert, InputRef, JobResult, JobSpec, JobState, JobStateCounts, LEDGER_TABLE_DECLARATIONS,
    cancel_job, insert_claim, install_work_ledger_schema, job_result, job_state, job_state_counts,
    read, record_failure, record_result, submit_job,
};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

const NOW_MS: i64 = 1_700_000_060_000;
const LEASE_DEADLINE_MS: i64 = NOW_MS + 60_000;

fn secure_runtime_root(directory: &Path) -> PathBuf {
    let root = directory.join("owner-runtime");
    std::fs::create_dir(&root).expect("create the task-scoped owner runtime directory");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the task-scoped owner runtime directory");
    root
}

fn spec(job_id: &str, attempts: i64) -> JobSpec {
    JobSpec::builder(job_id, "cg.event-import", "projection", "node-submitter")
        .input_refs(vec![InputRef::ledger_input()])
        .max_attempts(attempts)
        .submitted_at_ms(NOW_MS - 1_000)
        .build()
}

fn submit(database: &Database, job_id: &str, attempts: i64) {
    submit_job(database, &spec(job_id, attempts), &[b"fixture" as &[u8]])
        .unwrap_or_else(|error| panic!("submit {job_id}: {error:?}"));
}

fn claim(database: &Database, job_id: &str, attempt: i64) {
    assert_eq!(
        insert_claim(
            database,
            job_id,
            attempt,
            "node-worker",
            LEASE_DEADLINE_MS,
            NOW_MS,
        )
        .unwrap_or_else(|error| panic!("claim {job_id}: {error:?}")),
        ClaimInsert::Inserted,
        "the fixture claim is the first claim for {job_id}"
    );
}

/// One submitted job for every public `JobState` variant.
#[derive(Debug, Clone)]
struct ExpectedJobState {
    job_id: String,
    state: JobState,
}

fn assert_writable_state(database: &Database, expected: &ExpectedJobState) {
    assert_eq!(
        job_state(database, &expected.job_id, NOW_MS)
            .unwrap_or_else(|error| panic!("writable answer for {}: {error:?}", expected.job_id)),
        expected.state,
        "the seeded {} job has its independently specified state",
        expected.job_id
    );
}

fn seed_every_state(database: &Database) -> Vec<ExpectedJobState> {
    install_work_ledger_schema(database).expect("install the engine-owned work ledger schema");
    let pending = ExpectedJobState {
        job_id: "pending".to_owned(),
        state: JobState::Pending,
    };
    submit(database, &pending.job_id, 2);

    let leased = ExpectedJobState {
        job_id: "leased".to_owned(),
        state: JobState::Leased {
            node_id: "node-worker".to_owned(),
            attempt: 1,
            lease_deadline_ms: LEASE_DEADLINE_MS,
        },
    };
    submit(database, &leased.job_id, 2);
    claim(database, &leased.job_id, 1);

    let done = ExpectedJobState {
        job_id: "done".to_owned(),
        state: JobState::Done,
    };
    submit(database, &done.job_id, 2);
    claim(database, &done.job_id, 1);
    record_result(
        database,
        &done.job_id,
        1,
        "node-worker",
        b"result",
        serde_json::json!({"receipt": "done"}),
        NOW_MS,
    )
    .expect("record the completed fixture result");

    let failed = ExpectedJobState {
        job_id: "failed".to_owned(),
        state: JobState::Failed,
    };
    submit(database, &failed.job_id, 2);
    for attempt in 1..=2 {
        claim(database, &failed.job_id, attempt);
        record_failure(
            database,
            &failed.job_id,
            attempt,
            "node-worker",
            "failed",
            NOW_MS,
        )
        .unwrap_or_else(|error| panic!("record failed attempt {attempt}: {error:?}"));
    }

    let cancelled = ExpectedJobState {
        job_id: "cancelled".to_owned(),
        state: JobState::Cancelled,
    };
    submit(database, &cancelled.job_id, 2);
    cancel_job(
        database,
        &cancelled.job_id,
        "node-submitter",
        Some("withdrawn"),
        NOW_MS,
    )
    .expect("record the withdrawn fixture job");

    let expected = vec![pending, leased, done, failed, cancelled];
    for job in &expected {
        assert_writable_state(database, job);
    }
    expected
}

fn writable_answers(
    database: &Database,
    expected: &[ExpectedJobState],
) -> HashMap<String, JobState> {
    expected
        .iter()
        .map(|job| {
            (
                job.job_id.clone(),
                job_state(database, &job.job_id, NOW_MS).unwrap_or_else(|error| {
                    panic!("writable answer for {}: {error:?}", job.job_id)
                }),
            )
        })
        .collect()
}

fn writable_counts(database: &Database, expected: &[ExpectedJobState]) -> JobStateCounts {
    let ids: Vec<_> = expected.iter().map(|job| job.job_id.as_str()).collect();
    job_state_counts(database, &ids, NOW_MS)
        .unwrap_or_else(|error| panic!("writable state counts: {error:?}"))
}

fn writable_results(
    database: &Database,
    expected: &[ExpectedJobState],
) -> HashMap<String, Option<JobResult>> {
    expected
        .iter()
        .map(|job| {
            (
                job.job_id.clone(),
                job_result(database, &job.job_id).unwrap_or_else(|error| {
                    panic!("writable result for {}: {error:?}", job.job_id)
                }),
            )
        })
        .collect()
}

fn assert_reader_answers(
    route: ReadRoute,
    session: &ReadSession,
    expected: &HashMap<String, JobState>,
    expected_counts: &JobStateCounts,
    expected_results: &HashMap<String, Option<JobResult>>,
) {
    for (job_id, writable_answer) in expected {
        let reader_answer = read::job_state(session, job_id, NOW_MS)
            .unwrap_or_else(|error| panic!("{route:?} reader answer for {job_id}: {error:?}"));
        assert_eq!(
            reader_answer, *writable_answer,
            "the {route:?} read session derives {job_id}'s ledger state without taking writer ownership"
        );
    }
    let ids: Vec<_> = expected.keys().map(String::as_str).collect();
    assert_eq!(
        read::job_state_counts(session, &ids, NOW_MS)
            .unwrap_or_else(|error| panic!("{route:?} reader state counts: {error:?}")),
        *expected_counts,
        "the {route:?} read session counts all five fixture states as the writable handle does"
    );
    for (job_id, writable_result) in expected_results {
        assert_eq!(
            read::job_result(session, job_id)
                .unwrap_or_else(|error| panic!("{route:?} reader result for {job_id}: {error:?}")),
            *writable_result,
            "the {route:?} read session returns the done result and every other fixture job's absence"
        );
    }
}

fn ledger_schema_snapshot(
    database: &Database,
) -> Vec<(String, contextdb_core::table_meta::TableMeta)> {
    let mut tables = database.table_names();
    tables.sort();
    tables
        .into_iter()
        .map(|table| {
            let meta = database
                .table_meta(&table)
                .unwrap_or_else(|| panic!("ledger schema declares {table}"));
            (table, meta)
        })
        .collect()
}

fn assert_published_declarations_match_installer() {
    let installed = Database::open_memory();
    install_work_ledger_schema(&installed)
        .expect("install the ledger through its production installer");
    let declared = Database::open_memory();
    for declaration in LEDGER_TABLE_DECLARATIONS {
        declared
            .execute(declaration, &HashMap::new())
            .unwrap_or_else(|error| {
                panic!("apply published ledger declaration {declaration:?}: {error:?}")
            });
    }
    assert_eq!(
        ledger_schema_snapshot(&declared),
        ledger_schema_snapshot(&installed),
        "the public ledger declarations install the same complete .schema, including job-id indexes, as install_work_ledger_schema"
    );
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn ledger_job_state_through_a_read_session_matches_the_writable_answer() {
    let file_directory = tempfile::tempdir().expect("task-scoped file-route directory");
    let file_path = file_directory.path().join("ledger-file.db");
    let file_writer = Database::open(&file_path).expect("open the file-route fixture writer");
    let file_seed = seed_every_state(&file_writer);
    let file_expected = writable_answers(&file_writer, &file_seed);
    let file_counts = writable_counts(&file_writer, &file_seed);
    let file_results = writable_results(&file_writer, &file_seed);
    file_writer
        .close()
        .expect("release the file-route fixture writer");

    let file_session =
        ReadSession::open(&file_path).expect("open the committed file through ReadSession");
    assert_eq!(file_session.route(), ReadRoute::File);
    assert_reader_answers(
        ReadRoute::File,
        &file_session,
        &file_expected,
        &file_counts,
        &file_results,
    );
    drop(file_session);

    let owner_directory = tempfile::tempdir().expect("task-scoped owner-route directory");
    let runtime_root = secure_runtime_root(owner_directory.path());
    let owner_path = owner_directory.path().join("ledger-owner.db");
    let owner_writer = Database::open_with_options(
        &owner_path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime_root.clone()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the owner-route fixture writer");
    let owner_seed = seed_every_state(&owner_writer);
    let owner_expected = writable_answers(&owner_writer, &owner_seed);
    let owner_counts = writable_counts(&owner_writer, &owner_seed);
    let owner_results = writable_results(&owner_writer, &owner_seed);

    let owner_session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_options(&owner_path, ReadSessionOptions::default())
    })
    .expect("open the live owner through ReadSession");
    assert_eq!(owner_session.route(), ReadRoute::Owner);
    assert_reader_answers(
        ReadRoute::Owner,
        &owner_session,
        &owner_expected,
        &owner_counts,
        &owner_results,
    );
    drop(owner_session);
    owner_writer
        .close()
        .expect("release the owner-route fixture writer");

    assert_published_declarations_match_installer();
}
