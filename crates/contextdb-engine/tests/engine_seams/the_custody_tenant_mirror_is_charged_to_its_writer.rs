//! A durable store mirrors its tenant policy and binding records in memory,
//! and that mirror is charged to the writer before it is built. For a binding
//! that declares 1, 12 and 67 tables (its body grows with each), what
//! building the mirror leaves allocated
//! never exceeds what the writer admitted for it, and the admission stays
//! within a bounded multiple of what is allocated. Dropping the store
//! releases exactly the mirror's admission.
//!
//! Each size runs in a fresh child process, where the counted allocator sees
//! only this test's work.
use crate::custody_fixture::*;
use crate::writer_reopen_and_doubling_keep_a_fixed_resident_envelope::live_allocations_for_test;
use contextdb_engine::Database;
use contextdb_engine::sync_client::ApplicationTablePolicyExpectation;
use contextdb_server::InProcessBroker;
use std::sync::Arc;

const MODE: &str = "CONTEXTDB_TENANT_MIRROR_TABLES";
/// Allocator rounding and the read transaction the build opens.
const SLACK: usize = 16 * 1024;

/// One policy per table `t000..`, beside the fixture's own two.
fn expectation_for(tables: usize) -> ApplicationTablePolicyExpectation {
    let mut expectation = expectation();
    for table in 0..tables {
        expectation = expectation
            .expect_table(format!("t{table:03}"), MEMBER_CLAUSES)
            .unwrap();
    }
    expectation
}
fn declare_tables(db: &Database, tables: usize) {
    for table in 0..tables {
        db.execute(
            &format!("CREATE TABLE t{table:03} (id UUID PRIMARY KEY) {MEMBER_CLAUSES}"),
            &p(),
        )
        .unwrap();
    }
}

async fn mirror(tables: usize) {
    let dir = tempfile::tempdir().unwrap();
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, Arc::new(Database::open_memory())).await;
    declare_tables(&hub.db, tables);
    hub.db
        .__seed_tenant_table_policies_for_test(
            contextdb_core::TenantId::from(TENANT),
            expectation_for(tables),
        )
        .unwrap();
    let path = dir.path().join("edge.db");
    {
        let edge = Arc::new(open(&path, None));
        let client = edge_client(&edge, &broker, &dir.path().join("edge.key"));
        client
            .__seed_application_table_policy_binding_for_test(
                expectation_for(tables),
                edge.declared_reader_memory().unwrap(),
            )
            .await
            .unwrap();
        declare(&edge);
        declare_tables(&edge, tables);
        drop(client);
        Arc::into_inner(edge).unwrap().close().unwrap();
    }
    let edge = open(&path, None);
    // Warm the pages the build reads, then measure a fresh build.
    edge.__custody_tenant_mirror_for_test().unwrap();
    edge.__drop_custody_store_for_test();
    let used = edge.accountant().usage().used;
    let before = live_allocations_for_test();
    let (charge, entries) = edge.__custody_tenant_mirror_for_test().unwrap();
    let allocated = live_allocations_for_test().saturating_sub(before);
    // An edge mirrors its one policy binding, which carries every declared
    // table's policy: the mirrored body grows with `tables`.
    assert_eq!(
        entries, 1,
        "{tables} tables: the edge mirrors its policy binding"
    );
    assert_eq!(
        edge.accountant().usage().used - used,
        charge,
        "{tables} tables: the writer holds exactly the mirror's admission"
    );
    assert!(
        allocated <= charge + SLACK,
        "{tables} tables: the mirror allocated {allocated} bytes against {charge} admitted"
    );
    assert!(
        charge <= 16 * allocated.max(1) + SLACK,
        "{tables} tables: {charge} admitted for {allocated} allocated is not bounded"
    );
    edge.__drop_custody_store_for_test();
    assert_eq!(
        edge.accountant().usage().used,
        used,
        "{tables} tables: dropping the store releases the mirror's admission"
    );
    println!("S3_MIRROR tables={tables} entries={entries} charge={charge} allocated={allocated}");
    drop(edge);
    hub.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn the_custody_tenant_mirror_is_charged_to_its_writer() {
    if let Ok(tables) = std::env::var(MODE) {
        mirror(tables.parse().unwrap()).await;
        return;
    }
    for tables in ["1", "12", "67"] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &crate::child_test_filter(
                    module_path!(),
                    "the_custody_tenant_mirror_is_charged_to_its_writer",
                ),
                "--nocapture",
                "--test-threads=1",
            ])
            .env(MODE, tables)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines().filter(|line| line.contains("S3_MIRROR")) {
            eprintln!("{line}");
        }
        assert!(
            output.status.success(),
            "{tables} tables failed:\n{stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
