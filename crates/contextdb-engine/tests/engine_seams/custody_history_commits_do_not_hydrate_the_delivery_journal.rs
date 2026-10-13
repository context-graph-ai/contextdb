//! "Continued growth": a file-backed writer's ordinary commits do work fixed by
//! the commit, not by how many delivery units the store has ever registered.
//! The signed custody journal stays complete and durable; it is read through
//! the durable group index, never loaded whole into a resident copy.
use contextdb_core::{TenantId, Value};
use contextdb_engine::database::DeliveryManifest;
use contextdb_engine::sync_client::ApplicationTablePolicyExpectation;
use contextdb_engine::sync_types::NaturalKey;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use contextdb_server::subjects::push_subject;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use uuid::Uuid;

use crate::writer_reopen_and_doubling_keep_a_fixed_resident_envelope::live_allocations_for_test;

const ARM: &str = "CONTEXTDB_CUSTODY_HISTORY_UNITS";
const FIXTURE: &str = "CONTEXTDB_CUSTODY_HISTORY_FIXTURE";
const TENANT: &str = "custody-history";
const CEILING: usize = 64 * 1024 * 1024;
const SETTLING_SLACK: usize = 1024 * 1024;
/// Run-to-run spread of one arm's live allocation after its measured commits
/// (observed 4.145-4.209 MB for the same N arm, while its pre-commit sample is
/// byte-identical). A resident journal differs by megabytes between arms; any
/// owner of about one kilobyte per registered unit still exceeds this.
const ALLOCATION_SPREAD: usize = 256 * 1024;
/// Measured commits per kind: K registrations, then K root updates.
const K: usize = 8;
/// Custody records one registered unit keeps in the journal; the RED run
/// measured 1286 delivery records for 256 units plus one measured unit.
const RECORDS_PER_UNIT: u64 = 5;
const ROOT_CLAUSES: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST DELIVERY MANIFEST OVER parts";
const MEMBER_CLAUSES: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}
fn open(path: &Path, limit: Option<usize>) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            memory_limit: limit,
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the task-scoped writer")
}
fn root_id(unit: usize) -> Uuid {
    Uuid::from_u128(0x5100_0000_0000_0000_0000_0000_0000_0000 + unit as u128)
}
fn member_id(unit: usize) -> Uuid {
    Uuid::from_u128(0x5200_0000_0000_0000_0000_0000_0000_0000 + unit as u128)
}
fn key(id: Uuid) -> NaturalKey {
    NaturalKey::single("id".into(), Value::Uuid(id))
}
fn expectation() -> ApplicationTablePolicyExpectation {
    ApplicationTablePolicyExpectation::new()
        .expect_table("records", ROOT_CLAUSES)
        .expect("root clauses parse")
        .expect_table("parts", MEMBER_CLAUSES)
        .expect("member clauses parse")
}
/// One unit: a root row, a member on alternate units, and its registration,
/// all in one transaction.
fn commit_unit(db: &Database, unit: usize) {
    let tx = db.begin().unwrap();
    db.insert_row(
        tx,
        "records",
        HashMap::from([
            ("id".into(), Value::Uuid(root_id(unit))),
            ("body".into(), Value::Text(format!("unit {unit}"))),
        ]),
    )
    .unwrap();
    let members = if unit.is_multiple_of(2) {
        db.insert_row(
            tx,
            "parts",
            HashMap::from([
                ("id".into(), Value::Uuid(member_id(unit))),
                ("record_id".into(), Value::Uuid(root_id(unit))),
                ("body".into(), Value::Text(format!("member {unit}"))),
            ]),
        )
        .unwrap();
        vec![("parts", key(member_id(unit)))]
    } else {
        Vec::new()
    };
    db.register_delivery_manifest(
        tx,
        DeliveryManifest {
            root_table: "records",
            root_key: key(root_id(unit)),
            members,
        },
    )
    .unwrap();
    db.commit(tx).unwrap();
}
/// The edge's custody runtime is loaded by constructing its sync component on
/// the same handle, with the same persisted fabric key, in this process.
fn edge_client(db: &Arc<Database>, broker: &InProcessBroker, key_path: &Path) -> SyncClient {
    let identity = Arc::new(FabricIdentity::load_or_generate(key_path).unwrap());
    let node_id = identity.node_id();
    SyncClient::with_authenticated_transport_and_identity_for_test(
        db.clone(),
        broker.client_as(&node_id),
        TenantId::from(TENANT),
        identity,
    )
    .expect("construct the authenticated sync endpoint")
}
fn prepare(dir: &Path, n: usize) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let broker = InProcessBroker::new();
        let hub_db = Arc::new(Database::open_memory());
        hub_db
            .execute(&format!("CREATE TABLE records (id UUID PRIMARY KEY, body TEXT) {ROOT_CLAUSES}"), &p())
            .unwrap();
        hub_db
            .execute(&format!("CREATE TABLE parts (id UUID PRIMARY KEY, record_id UUID REFERENCES records(id), body TEXT) {MEMBER_CLAUSES}"), &p())
            .unwrap();
        hub_db
            .__seed_tenant_table_policies_for_test(TenantId::from(TENANT), expectation())
            .unwrap();
        let hub_identity = Arc::new(FabricIdentity::generate());
        let hub_node = hub_identity.node_id();
        let server = Arc::new(
            SyncServer::with_authenticated_transport_and_identity_for_test(
                hub_db.clone(),
                broker.server_as(&hub_node),
                TenantId::from(TENANT),
                hub_node,
                hub_identity,
            )
            .unwrap(),
        );
        let shutdown = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let server = server.clone();
            let shutdown = shutdown.clone();
            async move { server.run_until(shutdown).await }
        });
        broker
            .wait_for_registered_route_for_test(&push_subject(TENANT))
            .await;

        let edge = Arc::new(open(&dir.join("edge.db"), Some(CEILING)));
        let client = edge_client(&edge, &broker, &dir.join("edge.key"));
        client
            .__seed_application_table_policy_binding_for_test(
                expectation(),
                edge.declared_reader_memory().unwrap(),
            )
            .await
            .expect("bind the hub's declaration before writing units");
        edge.execute(
            &format!("CREATE TABLE records (id UUID PRIMARY KEY, body TEXT) {ROOT_CLAUSES}"),
            &p(),
        )
        .unwrap();
        edge.execute(&format!("CREATE TABLE parts (id UUID PRIMARY KEY, record_id UUID REFERENCES records(id), body TEXT) {MEMBER_CLAUSES}"), &p())
            .unwrap();
        // The arms differ only in how many units the journal holds.
        for unit in 0..n {
            commit_unit(&edge, unit);
        }
        // The same 8 MiB of unrelated rows in both arms lets the measured
        // process fill the writer cache's fixed share before it measures, as
        // the resident-envelope wall does.
        edge.execute(
            "CREATE TABLE cache_pressure (id INT PRIMARY KEY, body TEXT) HISTORY ALL SYNC OFF",
            &p(),
        )
        .unwrap();
        for batch in 0..64 {
            let mut params = HashMap::from([("body".to_owned(), Value::Text("p".repeat(1024)))]);
            let values = (0..128)
                .map(|offset| {
                    let name = format!("id{offset}");
                    params.insert(name.clone(), Value::Int64(batch * 128 + offset));
                    format!("(${name},$body)")
                })
                .collect::<Vec<_>>()
                .join(",");
            edge.execute(&format!("INSERT INTO cache_pressure VALUES {values}"), &params)
                .unwrap();
        }
        drop(client);
        shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = task.await;
        Arc::into_inner(edge)
            .expect("the fixture edge has one owner")
            .close()
            .unwrap();
    });
}

#[derive(Debug, Serialize, Deserialize)]
struct Arm {
    /// (hydrated, decoded) deltas across all 2K measured commits.
    hydrated: u64,
    decoded: u64,
    /// Resident delivery-journal (records, bytes) after each measured commit.
    resident: Vec<(u64, u64)>,
    /// Writer cache page occupancy right after saturation (its fixed share is CEILING/16).
    cache_occupancy: usize,
    charged: usize,
    gap: i64,
    /// Live allocation once the cache is saturated, before any measured commit.
    allocated_start: usize,
    allocated: usize,
    /// `delivery_status` (eligible, pending, accepted), or its refusal.
    status: Result<(u64, u64, u64), String>,
    /// Durable root-ownership and manifest keys after the measured commits.
    roots: u64,
    manifests: u64,
    status_hydrated: u64,
    status_decoded: u64,
}

fn arm(dir: &Path, n: usize) -> Arm {
    // A current-thread runtime that is never driven: the sync component only
    // loads the custody identity, and no background task allocates while the
    // arm measures.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _guard = runtime.enter();
    let broker = InProcessBroker::new();
    let edge = Arc::new(open(&dir.join("edge.db"), Some(CEILING)));
    let _client = edge_client(&edge, &broker, &dir.join("edge.key"));
    // Fill the writer cache's fixed share, so live allocation compares owners
    // rather than how much of a small file the cache happened to hold.
    drop(
        edge.__hold_row_page_for_test("cache_pressure", 8191)
            .unwrap(),
    );
    let cache = edge.__writer_cache_stats_for_test().unwrap();
    let allocated_start = live_allocations_for_test();
    let start = edge.__custody_writer_work_for_test();
    let mut resident = Vec::with_capacity(2 * K);
    for unit in n..n + K {
        commit_unit(&edge, unit);
        let w = edge.__custody_writer_work_for_test();
        resident.push((w.2, w.3));
    }
    for i in 0..K {
        // Distinct, already-registered roots spread across the whole history.
        let unit = i * (n / K);
        edge.execute(
            "UPDATE records SET body = $body WHERE id = $id",
            &HashMap::from([
                ("id".into(), Value::Uuid(root_id(unit))),
                ("body".into(), Value::Text(format!("updated {unit}"))),
            ]),
        )
        .unwrap();
        let w = edge.__custody_writer_work_for_test();
        resident.push((w.2, w.3));
    }
    let end = edge.__custody_writer_work_for_test();
    let r = edge.writer_memory_ownership_receipt_for_test();
    let allocated = live_allocations_for_test();
    let status = edge
        .delivery_status("records")
        .map(|s| (s.eligible, s.pending, s.accepted))
        .map_err(|e| format!("{e:?}"));
    let after_status = edge.__custody_writer_work_for_test();
    let roots = edge
        .__custody_durable_key_count_for_test("delivery_root.v1.")
        .unwrap();
    let manifests = edge
        .__custody_durable_key_count_for_test("delivery_manifest.v1.")
        .unwrap();
    Arm {
        hydrated: end.0 - start.0,
        decoded: end.1 - start.1,
        resident,
        cache_occupancy: cache.3,
        allocated_start,
        charged: r.named_owner_total,
        gap: r.reconciliation_gap,
        allocated,
        status,
        roots,
        manifests,
        status_hydrated: after_status.0 - end.0,
        status_decoded: after_status.1 - end.1,
    }
}

#[test]
fn custody_history_commits_do_not_hydrate_the_delivery_journal() {
    if let Ok(n) = std::env::var(ARM) {
        let dir = std::env::var(FIXTURE).unwrap();
        println!(
            "CUSTODY_ARM {}",
            serde_json::to_string(&arm(Path::new(&dir), n.parse().unwrap())).unwrap()
        );
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let mut arms = Vec::<(usize, Arm)>::new();
    for n in [256usize, 512] {
        let dir = root.path().join(format!("units-{n}"));
        std::fs::create_dir_all(&dir).unwrap();
        prepare(&dir, n);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &crate::child_test_filter(
                    module_path!(),
                    "custody_history_commits_do_not_hydrate_the_delivery_journal",
                ),
                "--nocapture",
            ])
            .env(ARM, n.to_string())
            .env(FIXTURE, &dir)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "measured arm failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        let json = stdout
            .lines()
            .find_map(|line| line.strip_prefix("CUSTODY_ARM "))
            .expect("child custody evidence");
        arms.push((n, serde_json::from_str(json).unwrap()));
    }
    eprintln!("N/2N fresh-process custody arms: {arms:?}");
    let (n1, a) = &arms[0];
    let (n2, b) = &arms[1];
    // 1. No measured commit loads the delivery journal into a resident copy.
    assert!(
        a.hydrated == 0 && b.hydrated == 0,
        "ordinary commits must not hydrate the delivery journal: N={n1} hydrated {}, 2N={n2} hydrated {}",
        a.hydrated,
        b.hydrated
    );
    // 2. Decode work is fixed by the commits, not by the journal.
    assert_eq!(
        a.decoded, b.decoded,
        "custody decodes cannot grow with registered history: {arms:?}"
    );
    assert!(
        b.decoded <= 4 * (2 * K as u64) * RECORDS_PER_UNIT,
        "custody decodes stay within the commits' own records and companions: {arms:?}"
    );
    // 3. No delivery-journal record stays resident after any measured commit.
    for (_, arm) in &arms {
        assert!(
            arm.resident.iter().all(|r| *r == (0, 0)),
            "the delivery journal is never resident on a file-backed writer: {arms:?}"
        );
    }
    // 4. Charged ownership settles and reconciles.
    assert!(
        b.charged <= a.charged + SETTLING_SLACK,
        "charged ownership cannot grow with registered history: {arms:?}"
    );
    assert!(
        a.gap == 0 && b.gap == 0,
        "every charged owner reconciles: {arms:?}"
    );
    // 5. Live allocation settles, measured with the writer cache at its share.
    for (_, arm) in &arms {
        assert!(
            arm.cache_occupancy >= CEILING / 16 - 4096 && arm.cache_occupancy <= CEILING / 16,
            "the writer cache has saturated its fixed share before measuring: {arms:?}"
        );
    }
    assert!(
        b.allocated_start <= a.allocated_start + 4096,
        "reopening the larger journal starts from the same saturated live allocation: {arms:?}"
    );
    assert!(
        b.allocated <= a.allocated + ALLOCATION_SPREAD,
        "live allocation cannot grow with registered history: {arms:?}"
    );
    // Controls against a no-op: the signed history stays complete, each
    // update retired exactly its root's ownership, and a status answer, when
    // the reader limit admits it, claims no hub verdict.
    for (n, arm) in &arms {
        assert_eq!(
            arm.manifests,
            (*n + K) as u64,
            "every registered manifest stays in the durable journal: {arms:?}"
        );
        assert_eq!(
            arm.roots, *n as u64,
            "N fixture roots plus K registrations minus K retired ownerships: {arms:?}"
        );
        match &arm.status {
            Ok((eligible, pending, accepted)) => {
                assert_eq!(*eligible, *n as u64, "{arms:?}");
                assert_eq!(pending, eligible, "no hub outcome exists: {arms:?}");
                assert_eq!(*accepted, 0, "a local commit is not a hub verdict");
            }
            Err(refusal) => assert!(
                refusal.starts_with("MemoryBudgetExceeded"),
                "status may only refuse for its declared reader limit: {refusal}"
            ),
        }
        assert_eq!(
            arm.status_hydrated, 0,
            "a status read does not hydrate the delivery journal: {arms:?}"
        );
    }
}
