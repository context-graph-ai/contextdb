use contextdb_core::Error;
use contextdb_engine::Database;
use contextdb_engine::peer_directory::{
    install_peer_directory_schema, lookup_peer_ticket, register_peer_ticket,
};
use contextdb_engine::work_ledger::{
    BlobHash, ClaimInsert, InputRef, JobSnapshot, JobSpec, MovementPolicy, REF_KIND_BLOB_REF,
    claimable_jobs, insert_claim, install_work_ledger_schema, job_snapshot, materialize_inputs,
    node_holds_claim_for_blob, record_failure, submit_job,
};

const T0: i64 = 1_700_000_000_000;
const LEASE: i64 = 5 * 60_000;

fn blob_spec(job_id: &str, submitter: &str, hash: &BlobHash) -> JobSpec {
    JobSpec::builder(job_id, "media.demo", "batch", submitter)
        .input_refs(vec![InputRef::blob_ref(hash.clone())])
        .submitted_at_ms(T0)
        .build()
}

fn blob_snapshot(job_id: &str, submitter: &str, hash: &BlobHash) -> JobSnapshot {
    JobSnapshot {
        job_id: job_id.to_string(),
        work_class: "media.demo".to_string(),
        mode: "batch".to_string(),
        requirement_tags: vec![],
        input_refs: vec![InputRef::blob_ref(hash.clone())],
        output_schema: None,
        priority: 0,
        deadline_ms: None,
        max_attempts: 2,
        submitter_node_id: submitter.to_string(),
        submitted_at_ms: T0,
    }
}

fn snapshot_with(submitter: &str, input_refs: Vec<InputRef>) -> JobSnapshot {
    JobSnapshot {
        job_id: "job-mix".to_string(),
        work_class: "media.demo".to_string(),
        mode: "batch".to_string(),
        requirement_tags: vec![],
        input_refs,
        output_schema: None,
        priority: 0,
        deadline_ms: None,
        max_attempts: 2,
        submitter_node_id: submitter.to_string(),
        submitted_at_ms: T0,
    }
}

#[test]
fn permits_input_read_admits_non_submitter_blob_ref_under_auto_propagate() {
    let job = blob_snapshot("job-1", "node-holder", &BlobHash::of(b"clip"));
    assert_eq!(job.input_refs[0].kind, REF_KIND_BLOB_REF);
    let on = MovementPolicy {
        auto_propagate: true,
    };
    assert!(
        on.permits_input_read(&job, "node-consumer"),
        "with auto_propagate ON, a non-submitter must be admitted to a blob_ref input"
    );
}

#[test]
fn job_snapshot_reads_back_deadline_and_submitter_for_a_leased_job() {
    // The dead-claimant steal reads a job's own
    // deadline_ms + submitter_node_id while it is Leased — the two fields
    // job_state does not carry. Prove the public accessor round-trips them,
    // and returns None for a job that does not exist.
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("schema");
    let deadline_ms = T0 + 5_000;
    let spec = JobSpec::builder("job-snap", "media.demo", "batch", "node-submitter")
        .input_refs(vec![InputRef::ledger_input()])
        .deadline_ms(Some(deadline_ms))
        .submitted_at_ms(T0)
        .build();
    submit_job(&db, &spec, &[b"payload" as &[u8]]).expect("submit");

    let snapshot = job_snapshot(&db, "job-snap")
        .expect("read job snapshot")
        .expect("job exists");
    assert_eq!(snapshot.deadline_ms, Some(deadline_ms));
    assert_eq!(snapshot.submitter_node_id, "node-submitter");

    assert!(
        job_snapshot(&db, "no-such-job")
            .expect("read missing job")
            .is_none(),
        "a job id with no row must read back as None, never an error"
    );
}

#[test]
fn permits_input_read_still_refuses_blob_ref_when_auto_propagate_off() {
    let job = blob_snapshot("job-1", "node-holder", &BlobHash::of(b"clip"));
    let off = MovementPolicy {
        auto_propagate: false,
    };
    assert!(!off.permits_input_read(&job, "node-consumer"));
    assert!(off.permits_input_read(&job, "node-holder"));
}

#[test]
fn node_holds_claim_for_blob_tracks_live_claim_on_the_hashs_job() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("schema");
    let h = BlobHash::of(b"the-clip-bytes");
    let other = BlobHash::of(b"a-different-clip");
    submit_job(&db, &blob_spec("job-1", "node-holder", &h), &[] as &[&[u8]]).expect("submit");

    assert!(!node_holds_claim_for_blob(&db, "node-worker", &h, T0).unwrap());
    assert!(matches!(
        insert_claim(&db, "job-1", 1, "node-worker", T0 + LEASE, T0).unwrap(),
        ClaimInsert::Inserted
    ));
    assert!(
        node_holds_claim_for_blob(&db, "node-worker", &h, T0).unwrap(),
        "a live claim on the hash's job must entitle the worker"
    );
    assert!(!node_holds_claim_for_blob(&db, "node-stranger", &h, T0).unwrap());
    assert!(!node_holds_claim_for_blob(&db, "node-worker", &other, T0).unwrap());
    assert!(!node_holds_claim_for_blob(&db, "node-worker", &h, T0 + LEASE + 1).unwrap());

    let hm = BlobHash::of(b"multi-attempt-clip");
    submit_job(
        &db,
        &blob_spec("job-multi", "node-holder", &hm),
        &[] as &[&[u8]],
    )
    .expect("submit multi");
    assert!(matches!(
        insert_claim(&db, "job-multi", 1, "node-A", T0 + LEASE, T0).unwrap(),
        ClaimInsert::Inserted
    ));
    record_failure(&db, "job-multi", 1, "node-A", "boom", T0).expect("record failure");
    assert!(matches!(
        insert_claim(&db, "job-multi", 2, "node-B", T0 + LEASE, T0).unwrap(),
        ClaimInsert::Inserted
    ));
    assert!(!node_holds_claim_for_blob(&db, "node-A", &hm, T0).unwrap());
    assert!(
        node_holds_claim_for_blob(&db, "node-B", &hm, T0).unwrap(),
        "the current live attempt claimant must be entitled"
    );

    let h_fail = BlobHash::of(b"highest-still-failed-clip");
    submit_job(
        &db,
        &blob_spec("job-fail", "node-holder", &h_fail),
        &[] as &[&[u8]],
    )
    .expect("submit fail");
    assert!(matches!(
        insert_claim(&db, "job-fail", 1, "node-A", T0 + LEASE, T0).unwrap(),
        ClaimInsert::Inserted
    ));
    record_failure(&db, "job-fail", 1, "node-A", "boom", T0).expect("record failure");
    assert!(!node_holds_claim_for_blob(&db, "node-A", &h_fail, T0).unwrap());
}

#[test]
fn permits_input_read_grants_only_blob_ref_and_ledger_input_never_local_path() {
    let on = MovementPolicy {
        auto_propagate: true,
    };
    let h = BlobHash::of(b"clip");

    assert!(on.permits_input_read(
        &snapshot_with("node-holder", vec![InputRef::blob_ref(h.clone())]),
        "node-consumer"
    ));
    assert!(on.permits_input_read(
        &snapshot_with(
            "node-holder",
            vec![InputRef::blob_ref(h.clone()), InputRef::ledger_input()]
        ),
        "node-consumer",
    ));
    assert!(!on.permits_input_read(
        &snapshot_with("node-holder", vec![InputRef::local_path("/data/clip.bin")]),
        "node-consumer"
    ));
    assert!(!on.permits_input_read(
        &snapshot_with(
            "node-holder",
            vec![
                InputRef::blob_ref(h.clone()),
                InputRef::local_path("/data/clip.bin")
            ]
        ),
        "node-consumer",
    ));
    assert!(!on.permits_input_read(
        &snapshot_with(
            "node-holder",
            vec![InputRef {
                kind: "future_ref".to_string(),
                detail: serde_json::Value::Null,
            }]
        ),
        "node-consumer",
    ));
}

#[test]
fn materialize_inputs_surfaces_a_blob_ref_never_silent_empty() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("schema");
    let h = BlobHash::of(b"the-clip-bytes");

    submit_job(
        &db,
        &blob_spec("job-blob", "node-holder", &h),
        &[] as &[&[u8]],
    )
    .expect("submit blob");
    let result = materialize_inputs(
        &db,
        "job-blob",
        "node-holder",
        &MovementPolicy {
            auto_propagate: true,
        },
    );
    assert!(
        matches!(result, Err(Error::InputRequiresBlobResolver { .. })),
        "blob_ref materialization must route to the resolver, got {result:?}"
    );

    let mixed = JobSpec::builder("job-mixed", "media.demo", "batch", "node-holder")
        .input_refs(vec![
            InputRef::ledger_input(),
            InputRef::blob_ref(h.clone()),
        ])
        .submitted_at_ms(T0)
        .build();
    submit_job(&db, &mixed, &[b"ledger-text" as &[u8]]).expect("submit mixed");
    let mixed_result = materialize_inputs(
        &db,
        "job-mixed",
        "node-holder",
        &MovementPolicy {
            auto_propagate: true,
        },
    );
    assert!(
        matches!(mixed_result, Err(Error::InputRequiresBlobResolver { .. })),
        "mixed ledger_input/blob_ref materialization must route to the resolver, got {mixed_result:?}"
    );
}

#[test]
fn claimable_jobs_offers_a_blob_ref_job_only_when_auto_propagate_is_on() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("schema");
    let h = BlobHash::of(b"claimable-clip");
    let spec = JobSpec::builder("job-1", "media.demo", "batch", "node-holder")
        .requirement_tags(vec!["media".to_string()])
        .input_refs(vec![InputRef::blob_ref(h)])
        .submitted_at_ms(T0)
        .build();
    submit_job(&db, &spec, &[] as &[&[u8]]).expect("submit");
    let tags = vec!["media".to_string()];

    let on = claimable_jobs(
        &db,
        "node-worker",
        &tags,
        &MovementPolicy {
            auto_propagate: true,
        },
        T0,
    )
    .expect("claimable on");
    assert!(on.iter().any(|j| j.job_id == "job-1"));
    let off = claimable_jobs(
        &db,
        "node-worker",
        &tags,
        &MovementPolicy {
            auto_propagate: false,
        },
        T0,
    )
    .expect("claimable off");
    assert!(!off.iter().any(|j| j.job_id == "job-1"));
}

#[test]
fn peer_directory_resolves_a_node_id_to_its_enrolled_ticket() {
    let db = Database::open_memory();
    install_peer_directory_schema(&db).expect("schema");

    assert_eq!(
        lookup_peer_ticket(&db, "node-stranger").unwrap(),
        None,
        "an un-enrolled node has no ticket"
    );

    register_peer_ticket(&db, "node-A", "ticket-A-v1", T0).expect("register A");
    assert_eq!(
        lookup_peer_ticket(&db, "node-A").unwrap().as_deref(),
        Some("ticket-A-v1"),
        "an enrolled node resolves to its current ticket"
    );

    register_peer_ticket(&db, "node-A", "ticket-A-v2", T0 + 1).expect("re-register A");
    assert_eq!(
        lookup_peer_ticket(&db, "node-A").unwrap().as_deref(),
        Some("ticket-A-v2"),
        "a rotated ticket replaces the prior one"
    );

    assert_eq!(
        lookup_peer_ticket(&db, "node-stranger").unwrap(),
        None,
        "lookup is node-keyed"
    );
}
