//! Work-ledger, peer-directory and explain-rendering routes, each on a store
//! whose ledger holds one submitted job with a ledger input and a blob
//! reference, an advertised capability, a claim and a registered peer.

use super::fixture::{Store, none};
use super::{Row, call};
use contextdb_engine::cli_render::{render_explain, render_explain_with_read_memory};
use contextdb_engine::peer_directory::*;
use contextdb_engine::work_ledger::*;

const NOW: i64 = 2_000;
const JOB: &str = "job-1";
const NODE: &str = "node-a";

fn blob() -> BlobHash {
    BlobHash::of(b"route blob")
}

fn spec(job: &str) -> JobSpec {
    JobSpec::builder(job, "embed", "batch", NODE)
        .requirement_tags(vec!["gpu".to_string()])
        .input_refs(vec![InputRef::ledger_input(), InputRef::blob_ref(blob())])
        .submitted_at_ms(1_000)
        .build()
}

fn tags() -> Vec<String> {
    vec!["gpu".to_string()]
}

fn policy() -> MovementPolicy {
    MovementPolicy {
        auto_propagate: true,
    }
}

/// A store with the ledger and peer schemas installed and nothing in them.
fn schemas() -> Store {
    let store = Store::open();
    install_work_ledger_schema(&store.db).expect("fixture ledger schema");
    install_peer_directory_schema(&store.db).expect("fixture peer schema");
    store
}

/// A store whose ledger holds one job, a capability, a claim and a peer.
fn ledger() -> Store {
    let store = schemas();
    submit_job(&store.db, &spec(JOB), &[b"input-0".as_slice()]).expect("fixture job");
    advertise_capability(&store.db, NODE, "cap-1", &tags(), 1_000).expect("fixture capability");
    insert_claim(&store.db, JOB, 1, NODE, 10_000, 1_500).expect("fixture claim");
    register_peer_ticket(&store.db, NODE, "ticket-a", 1_000).expect("fixture peer");
    store
}

pub fn rows() -> Vec<Row> {
    vec![
        Row {
            route: "contextdb_engine::work_ledger::install_work_ledger_schema",
            drive: || {
                let store = Store::open();
                call(|| install_work_ledger_schema(&store.db))
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::install_work_ledger_schema_with_read_memory",
            drive: || {
                let store = Store::open();
                call(|| {
                    install_work_ledger_schema_with_read_memory(
                        &store.db,
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::submit_job",
            drive: || {
                let store = schemas();
                call(|| submit_job(&store.db, &spec("job-2"), &[b"input".as_slice()]))
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::submit_job_with_read_memory",
            drive: || {
                let store = schemas();
                call(|| {
                    submit_job_with_read_memory(
                        &store.db,
                        &spec("job-2"),
                        &[b"input".as_slice()],
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::submit_job_in_tx",
            drive: || {
                let store = schemas();
                let tx = store.db.begin().expect("fixture transaction");
                call(|| submit_job_in_tx(&store.db, tx, &spec("job-2")))
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::submit_job_in_tx_with_read_memory",
            drive: || {
                let store = schemas();
                let tx = store.db.begin().expect("fixture transaction");
                call(|| {
                    submit_job_in_tx_with_read_memory(
                        &store.db,
                        tx,
                        &spec("job-2"),
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::add_job_input_in_tx",
            drive: || {
                let store = schemas();
                let tx = store.db.begin().expect("fixture transaction");
                submit_job_in_tx(&store.db, tx, &spec("job-2")).expect("fixture staged job");
                call(|| add_job_input_in_tx(&store.db, tx, "job-2", 0, b"input"))
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::add_job_input_in_tx_with_read_memory",
            drive: || {
                let store = schemas();
                let tx = store.db.begin().expect("fixture transaction");
                submit_job_in_tx(&store.db, tx, &spec("job-2")).expect("fixture staged job");
                call(|| {
                    add_job_input_in_tx_with_read_memory(
                        &store.db,
                        tx,
                        "job-2",
                        0,
                        b"input",
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::advertise_capability",
            drive: || {
                let store = ledger();
                call(|| advertise_capability(&store.db, NODE, "cap-2", &tags(), NOW))
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::advertise_capability_with_read_memory",
            drive: || {
                let store = ledger();
                call(|| {
                    advertise_capability_with_read_memory(
                        &store.db,
                        NODE,
                        "cap-2",
                        &tags(),
                        NOW,
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::advertised_tags",
            drive: || {
                let store = ledger();
                call(|| advertised_tags(&store.db, NODE))
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::advertised_tags_with_read_memory",
            drive: || {
                let store = ledger();
                call(|| {
                    advertised_tags_with_read_memory(
                        &store.db,
                        NODE,
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::claimable_jobs",
            drive: || {
                let store = ledger();
                call(|| claimable_jobs(&store.db, "node-b", &tags(), &policy(), NOW))
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::claimable_jobs_with_read_memory",
            drive: || {
                let store = ledger();
                call(|| {
                    claimable_jobs_with_read_memory(
                        &store.db,
                        "node-b",
                        &tags(),
                        &policy(),
                        NOW,
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::next_attempt",
            drive: || {
                let store = ledger();
                call(|| next_attempt(&store.db, JOB))
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::next_attempt_with_read_memory",
            drive: || {
                let store = ledger();
                call(|| {
                    next_attempt_with_read_memory(
                        &store.db,
                        JOB,
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::insert_claim",
            drive: || {
                let store = ledger();
                call(|| insert_claim(&store.db, JOB, 2, "node-b", 20_000, NOW))
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::insert_claim_with_read_memory",
            drive: || {
                let store = ledger();
                call(|| {
                    insert_claim_with_read_memory(
                        &store.db,
                        JOB,
                        2,
                        "node-b",
                        20_000,
                        NOW,
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::claim_holder",
            drive: || {
                let store = ledger();
                call(|| claim_holder(&store.db, JOB, 1))
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::claim_holder_with_read_memory",
            drive: || {
                let store = ledger();
                call(|| {
                    claim_holder_with_read_memory(
                        &store.db,
                        JOB,
                        1,
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::node_holds_claim_for_blob",
            drive: || {
                let store = ledger();
                call(|| node_holds_claim_for_blob(&store.db, NODE, &blob(), NOW))
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::node_holds_claim_for_blob_with_read_memory",
            drive: || {
                let store = ledger();
                call(|| {
                    node_holds_claim_for_blob_with_read_memory(
                        &store.db,
                        NODE,
                        &blob(),
                        NOW,
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::any_node_holds_claim_for_blob",
            drive: || {
                let store = ledger();
                call(|| any_node_holds_claim_for_blob(&store.db, &blob(), NOW))
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::any_node_holds_claim_for_blob_with_read_memory",
            drive: || {
                let store = ledger();
                call(|| {
                    any_node_holds_claim_for_blob_with_read_memory(
                        &store.db,
                        &blob(),
                        NOW,
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::node_claim_permits_movement",
            drive: || {
                let store = ledger();
                call(|| node_claim_permits_movement(&store.db, "node-b", &blob(), policy(), NOW))
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::node_claim_permits_movement_with_read_memory",
            drive: || {
                let store = ledger();
                call(|| {
                    node_claim_permits_movement_with_read_memory(
                        &store.db,
                        "node-b",
                        &blob(),
                        policy(),
                        NOW,
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::blob_ref_retention_eligible",
            drive: || {
                let store = ledger();
                call(|| blob_ref_retention_eligible(&store.db, &blob(), NOW, 0))
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::blob_ref_retention_eligible_with_read_memory",
            drive: || {
                let store = ledger();
                call(|| {
                    blob_ref_retention_eligible_with_read_memory(
                        &store.db,
                        &blob(),
                        NOW,
                        0,
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::materialize_inputs",
            drive: || {
                let store = ledger();
                call(|| materialize_inputs(&store.db, JOB, NODE, &policy()))
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::materialize_inputs_with_read_memory",
            drive: || {
                let store = ledger();
                call(|| {
                    materialize_inputs_with_read_memory(
                        &store.db,
                        JOB,
                        NODE,
                        &policy(),
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::record_result",
            drive: || {
                let store = ledger();
                call(|| {
                    record_result(
                        &store.db,
                        JOB,
                        1,
                        NODE,
                        b"output",
                        serde_json::json!({ "ok": true }),
                        NOW,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::record_result_with_read_memory",
            drive: || {
                let store = ledger();
                call(|| {
                    record_result_with_read_memory(
                        &store.db,
                        JOB,
                        1,
                        NODE,
                        b"output",
                        serde_json::json!({ "ok": true }),
                        NOW,
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::job_result",
            drive: || {
                let store = ledger();
                record_result(
                    &store.db,
                    JOB,
                    1,
                    NODE,
                    b"output",
                    serde_json::json!({}),
                    NOW,
                )
                .expect("fixture result");
                call(|| job_result(&store.db, JOB))
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::job_result_with_read_memory",
            drive: || {
                let store = ledger();
                record_result(
                    &store.db,
                    JOB,
                    1,
                    NODE,
                    b"output",
                    serde_json::json!({}),
                    NOW,
                )
                .expect("fixture result");
                call(|| {
                    job_result_with_read_memory(&store.db, JOB, store.db.declared_reader_memory()?)
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::record_failure",
            drive: || {
                let store = ledger();
                call(|| record_failure(&store.db, JOB, 1, NODE, "route failure", NOW))
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::record_failure_with_read_memory",
            drive: || {
                let store = ledger();
                call(|| {
                    record_failure_with_read_memory(
                        &store.db,
                        JOB,
                        1,
                        NODE,
                        "route failure",
                        NOW,
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::cancel_job",
            drive: || {
                let store = ledger();
                call(|| cancel_job(&store.db, JOB, NODE, Some("route cancel"), NOW))
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::cancel_job_with_read_memory",
            drive: || {
                let store = ledger();
                call(|| {
                    cancel_job_with_read_memory(
                        &store.db,
                        JOB,
                        NODE,
                        Some("route cancel"),
                        NOW,
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::should_abandon",
            drive: || {
                let store = ledger();
                call(|| should_abandon(&store.db, JOB, 1, NODE, NOW))
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::should_abandon_with_read_memory",
            drive: || {
                let store = ledger();
                call(|| {
                    should_abandon_with_read_memory(
                        &store.db,
                        JOB,
                        1,
                        NODE,
                        NOW,
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::run_input_retention",
            drive: || {
                let store = ledger();
                record_result(
                    &store.db,
                    JOB,
                    1,
                    NODE,
                    b"output",
                    serde_json::json!({}),
                    NOW,
                )
                .expect("fixture result");
                call(|| run_input_retention(&store.db, NOW + 1_000, 0))
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::run_input_retention_with_read_memory",
            drive: || {
                let store = ledger();
                record_result(
                    &store.db,
                    JOB,
                    1,
                    NODE,
                    b"output",
                    serde_json::json!({}),
                    NOW,
                )
                .expect("fixture result");
                call(|| {
                    run_input_retention_with_read_memory(
                        &store.db,
                        NOW + 1_000,
                        0,
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::work_ledger::job_state_counts_with_work_with_read_memory",
            drive: || {
                let store = ledger();
                call(|| {
                    job_state_counts_with_work_with_read_memory(
                        &store.db,
                        &[JOB, "job-absent"],
                        NOW,
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        // Peer directory.
        Row {
            route: "contextdb_engine::peer_directory::install_peer_directory_schema",
            drive: || {
                let store = Store::open();
                call(|| install_peer_directory_schema(&store.db))
            },
        },
        Row {
            route: "contextdb_engine::peer_directory::install_peer_directory_schema_with_read_memory",
            drive: || {
                let store = Store::open();
                call(|| {
                    install_peer_directory_schema_with_read_memory(
                        &store.db,
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::peer_directory::register_peer_ticket",
            drive: || {
                let store = ledger();
                call(|| register_peer_ticket(&store.db, "node-b", "ticket-b", NOW))
            },
        },
        Row {
            route: "contextdb_engine::peer_directory::register_peer_ticket_with_read_memory",
            drive: || {
                let store = ledger();
                call(|| {
                    register_peer_ticket_with_read_memory(
                        &store.db,
                        "node-b",
                        "ticket-b",
                        NOW,
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::peer_directory::lookup_peer_ticket",
            drive: || {
                let store = ledger();
                call(|| lookup_peer_ticket(&store.db, NODE))
            },
        },
        Row {
            route: "contextdb_engine::peer_directory::lookup_peer_ticket_with_read_memory",
            drive: || {
                let store = ledger();
                call(|| {
                    lookup_peer_ticket_with_read_memory(
                        &store.db,
                        NODE,
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
        // Explain rendering for the CLI.
        Row {
            route: "contextdb_engine::cli_render::render_explain",
            drive: || {
                let store = Store::open();
                call(|| render_explain(&store.db, "SELECT id FROM notes WHERE bucket = 1", &none()))
            },
        },
        Row {
            route: "contextdb_engine::cli_render::render_explain_with_read_memory",
            drive: || {
                let store = Store::open();
                call(|| {
                    render_explain_with_read_memory(
                        &store.db,
                        "SELECT id FROM notes WHERE bucket = 1",
                        &none(),
                        store.db.declared_reader_memory()?,
                    )
                })
            },
        },
    ]
}
