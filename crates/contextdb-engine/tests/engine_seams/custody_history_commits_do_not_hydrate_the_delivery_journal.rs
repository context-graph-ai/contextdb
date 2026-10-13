//! "Continued growth": a file-backed writer's ordinary commits, and a hub's
//! received units, do custody work fixed by the commit, not by how many
//! delivery units the store has ever registered or received. The signed
//! custody journal stays complete and durable; it is read through the durable
//! group index at the operation's captured root, never loaded whole.
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
/// Regression sensitivity of the live-allocation comparison, not a
/// boundedness proof: the RED resident journal differs by megabytes between
/// arms, while the checkpoint attribution below names every owner that moves.
const ALLOCATION_SPREAD: usize = 256 * 1024;
/// Child process selector: the edge's own commits, or the hub's received units.
const MODE: &str = "CONTEXTDB_CUSTODY_HISTORY_MODE";
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
async fn start_server(
    broker: &InProcessBroker,
    hub: &Arc<Database>,
    key_path: &Path,
) -> (Arc<AtomicBool>, tokio::task::JoinHandle<()>) {
    let identity = Arc::new(FabricIdentity::load_or_generate(key_path).unwrap());
    let node = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            hub.clone(),
            broker.server_as(&node),
            TenantId::from(TENANT),
            node,
            identity,
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
    (shutdown, task)
}
fn declare(db: &Database) {
    db.execute(
        &format!("CREATE TABLE records (id UUID PRIMARY KEY, body TEXT) {ROOT_CLAUSES}"),
        &p(),
    )
    .unwrap();
    db.execute(&format!("CREATE TABLE parts (id UUID PRIMARY KEY, record_id UUID REFERENCES records(id), body TEXT) {MEMBER_CLAUSES}"), &p())
        .unwrap();
}
/// A bound, declared file-backed edge at `edge.db`/`key` in `dir`.
async fn bound_edge(broker: &InProcessBroker, dir: &Path, name: &str) -> (Arc<Database>, SyncClient) {
    let edge = Arc::new(open(&dir.join(format!("{name}.db")), Some(CEILING)));
    let client = edge_client(&edge, broker, &dir.join(format!("{name}.key")));
    client
        .__seed_application_table_policy_binding_for_test(
            expectation(),
            edge.declared_reader_memory().unwrap(),
        )
        .await
        .expect("bind the hub's declaration before writing units");
    declare(&edge);
    (edge, client)
}
fn cache_pressure(db: &Database) {
    // The same 8 MiB of unrelated rows in both arms lets the measured process
    // fill the writer cache's fixed share before it measures, as the
    // resident-envelope wall does.
    db.execute(
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
        db.execute(&format!("INSERT INTO cache_pressure VALUES {values}"), &params)
            .unwrap();
    }
}
/// The arms differ only in how many units the edge registered and delivered
/// and the hub received.
fn prepare(dir: &Path, n: usize) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let broker = InProcessBroker::new();
        let hub = Arc::new(open(&dir.join("hub.db"), Some(CEILING)));
        declare(&hub);
        hub.__seed_tenant_table_policies_for_test(TenantId::from(TENANT), expectation())
            .unwrap();
        let (shutdown, task) = start_server(&broker, &hub, &dir.join("hub.key")).await;
        let (edge, client) = bound_edge(&broker, dir, "edge").await;
        // Delivered in exchanges of 16 units: one exchange carrying hundreds
        // of pending units exceeds the default reader limit.
        for unit in 0..n {
            commit_unit(&edge, unit);
            if (unit + 1).is_multiple_of(16) {
                client.push().await.expect("deliver the fixture units");
            }
        }
        client.push().await.expect("deliver every fixture unit");
        cache_pressure(&edge);
        cache_pressure(&hub);
        drop(client);
        shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = task.await;
        for db in [edge, hub] {
            Arc::into_inner(db)
                .expect("the fixture database has one owner")
                .close()
                .unwrap();
        }
    });
}

/// One growth checkpoint: the custody work so far, and every named owner of
/// live allocation that can move while the arm measures.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Checkpoint {
    /// Measured commits so far.
    commits: usize,
    hydrated: u64,
    decoded: u64,
    resident: (u64, u64),
    allocated: usize,
    cache_occupancy: usize,
    cache_index: usize,
    cache_evictions: u64,
    allocator_metadata: usize,
    charged: usize,
    gap: i64,
}
impl Checkpoint {
    /// Live allocation not owned by the saturated writer cache or the redb
    /// allocator metadata.
    fn unattributed(&self) -> isize {
        self.allocated as isize
            - self.cache_occupancy as isize
            - self.cache_index as isize
            - self.allocator_metadata as isize
    }
}
fn checkpoint(db: &Database, commits: usize, start: (u64, u64, u64, u64)) -> Checkpoint {
    let w = db.__custody_writer_work_for_test();
    let r = db.writer_memory_ownership_receipt_for_test();
    let cache = db.__writer_cache_stats_for_test().unwrap();
    Checkpoint {
        commits,
        hydrated: w.0 - start.0,
        decoded: w.1 - start.1,
        resident: (w.2, w.3),
        cache_occupancy: cache.3,
        cache_index: db.__writer_cache_index_bytes_for_test().unwrap(),
        cache_evictions: cache.2,
        allocator_metadata: db.__allocator_metadata_bytes_for_test().unwrap(),
        charged: r.named_owner_total,
        gap: r.reconciliation_gap,
        // Sampled last, after every observation above has dropped.
        allocated: live_allocations_for_test(),
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Arm {
    /// Writer cache page occupancy right after saturation (its fixed share is CEILING/16).
    cache_occupancy: usize,
    /// Live allocation once the cache is saturated, before any measured commit.
    allocated_start: usize,
    /// After every 4 measured commits: K registrations, then K retiring updates.
    checkpoints: Vec<Checkpoint>,
    /// `delivery_status` (eligible, pending, accepted), or its refusal.
    status: Result<(u64, u64, u64), String>,
    status_hydrated: u64,
    /// Durable root-ownership and manifest keys after the measured commits.
    roots: u64,
    manifests: u64,
    /// A measured unit's source history removed: the next read refuses.
    tampered_refused: bool,
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
    let mut checkpoints = Vec::with_capacity(4);
    checkpoints.push(checkpoint(&edge, 0, start));
    let mut commits: usize = 0;
    for unit in n..n + K {
        commit_unit(&edge, unit);
        commits += 1;
        if commits.is_multiple_of(4) {
            checkpoints.push(checkpoint(&edge, commits, start));
        }
    }
    for i in 0..K {
        // Distinct, already-registered and delivered roots across the history.
        let unit = i * (n / K);
        edge.execute(
            "UPDATE records SET body = $body WHERE id = $id",
            &HashMap::from([
                ("id".into(), Value::Uuid(root_id(unit))),
                ("body".into(), Value::Text(format!("updated {unit}"))),
            ]),
        )
        .unwrap();
        commits += 1;
        if commits.is_multiple_of(4) {
            checkpoints.push(checkpoint(&edge, commits, start));
        }
    }
    let before_status = edge.__custody_writer_work_for_test();
    let status = edge
        .delivery_status("records")
        .map(|s| (s.eligible, s.pending, s.accepted))
        .map_err(|e| format!("{e:?}"));
    let status_hydrated = edge.__custody_writer_work_for_test().0 - before_status.0;
    let roots = edge
        .__custody_durable_key_count_for_test("delivery_root.v1.")
        .unwrap();
    let manifests = edge
        .__custody_durable_key_count_for_test("delivery_manifest.v1.")
        .unwrap();
    // Control: the durable reads really validate. Remove one measured unit's
    // source history; selecting roots must now refuse as corruption.
    let history = edge
        .__custody_keys_for_test(None, "delivery_source_history.v1.")
        .unwrap();
    let victim = history
        .iter()
        .find(|key| key.ends_with(&root_id(n).simple().to_string()))
        .cloned()
        .unwrap_or_else(|| history[history.len() - 1].clone());
    edge.__tamper_custody_record_for_test(&victim, None).unwrap();
    let tampered_refused = edge
        .__custody_keys_for_test(None, "delivery_root.v1.")
        .is_err_and(|e| e.to_string().contains("invalid canonical delivery metadata"));
    Arm {
        cache_occupancy: cache.3,
        allocated_start,
        checkpoints,
        status,
        status_hydrated,
        roots,
        manifests,
        tampered_refused,
    }
}

/// The hub's received units: a fresh edge registers K units and pushes them
/// to a hub that already holds N (or 2N) received units.
#[derive(Debug, Serialize, Deserialize)]
struct HubArm {
    hydrated: u64,
    decoded: u64,
    resident: (u64, u64),
    applied_rows: usize,
    hub_received: u64,
}

fn hub_arm(dir: &Path) -> HubArm {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let broker = InProcessBroker::new();
        let hub = Arc::new(open(&dir.join("hub.db"), Some(CEILING)));
        let (shutdown, task) = start_server(&broker, &hub, &dir.join("hub.key")).await;
        // The second edge is created fresh in each arm: its own work is equal.
        let (edge, client) = bound_edge(&broker, dir, "second-edge").await;
        for unit in 0..K {
            commit_unit(&edge, 1_000_000 + unit);
        }
        let start = hub.__custody_writer_work_for_test();
        let applied = client.push().await.expect("the hub receives the units");
        let end = hub.__custody_writer_work_for_test();
        let hub_received = hub
            .__custody_durable_key_count_for_test("delivery_hub_outcome.v1.")
            .unwrap();
        drop(client);
        shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = task.await;
        HubArm {
            hydrated: end.0 - start.0,
            decoded: end.1 - start.1,
            resident: (end.2, end.3),
            applied_rows: applied.applied_rows,
            hub_received,
        }
    })
}

fn child<T: serde::de::DeserializeOwned>(dir: &Path, n: usize, mode: &str) -> T {
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
        .env(FIXTURE, dir)
        .env(MODE, mode)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "measured {mode} arm failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json = stdout
        .lines()
        .find_map(|line| line.strip_prefix("CUSTODY_ARM "))
        .expect("child custody evidence");
    serde_json::from_str(json).unwrap()
}

#[test]
fn custody_history_commits_do_not_hydrate_the_delivery_journal() {
    if let Ok(n) = std::env::var(ARM) {
        let dir = std::env::var(FIXTURE).unwrap();
        let json = match std::env::var(MODE).as_deref() {
            Ok("hub") => serde_json::to_string(&hub_arm(Path::new(&dir))),
            _ => serde_json::to_string(&arm(Path::new(&dir), n.parse().unwrap())),
        };
        println!("CUSTODY_ARM {}", json.unwrap());
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let mut arms = Vec::<(usize, Arm)>::new();
    let mut hubs = Vec::<(usize, HubArm)>::new();
    for n in [256usize, 512] {
        let dir = root.path().join(format!("units-{n}"));
        std::fs::create_dir_all(&dir).unwrap();
        prepare(&dir, n);
        arms.push((n, child(&dir, n, "edge")));
        hubs.push((n, child(&dir, n, "hub")));
    }
    eprintln!("N/2N fresh-process custody arms: {arms:?}");
    eprintln!("N/2N fresh-process hub arms: {hubs:?}");
    for (_, arm) in &arms {
        for c in &arm.checkpoints {
            eprintln!(
                "checkpoint commits={} allocated={} occupancy={} cache_index={} evictions={} allocator_metadata={} unattributed={} charged={}",
                c.commits,
                c.allocated,
                c.cache_occupancy,
                c.cache_index,
                c.cache_evictions,
                c.allocator_metadata,
                c.unattributed(),
                c.charged
            );
        }
    }
    let (n1, a) = &arms[0];
    let (n2, b) = &arms[1];
    let (fa, fb) = (a.checkpoints.last().unwrap(), b.checkpoints.last().unwrap());
    // 1. No measured commit loads the delivery journal into a resident copy.
    assert!(
        fa.hydrated == 0 && fb.hydrated == 0,
        "ordinary commits must not hydrate the delivery journal: N={n1} hydrated {}, 2N={n2} hydrated {}",
        fa.hydrated,
        fb.hydrated
    );
    // 2. Decode work is fixed by the commits, real, and equal at every checkpoint.
    assert_eq!(a.checkpoints.len(), b.checkpoints.len());
    for (ca, cb) in a.checkpoints.iter().zip(&b.checkpoints) {
        assert_eq!(
            ca.decoded, cb.decoded,
            "custody decodes cannot grow with registered history at {} commits: {arms:?}",
            ca.commits
        );
        assert_eq!(ca.hydrated + cb.hydrated, 0);
    }
    assert!(fa.decoded > 0, "the measured commits did real custody work: {arms:?}");
    assert!(
        fb.decoded <= 4 * (2 * K as u64) * RECORDS_PER_UNIT,
        "custody decodes stay within the commits' own records and companions: {arms:?}"
    );
    // 3. No delivery-journal record stays resident at any checkpoint.
    for (_, arm) in &arms {
        assert!(
            arm.checkpoints.iter().all(|c| c.resident == (0, 0)),
            "the delivery journal is never resident on a file-backed writer: {arms:?}"
        );
    }
    // 4. Charged ownership settles and reconciles at every checkpoint.
    for (ca, cb) in a.checkpoints.iter().zip(&b.checkpoints) {
        assert!(
            cb.charged <= ca.charged + SETTLING_SLACK,
            "charged ownership cannot grow with registered history: {arms:?}"
        );
        assert!(ca.gap == 0 && cb.gap == 0, "every charged owner reconciles: {arms:?}");
    }
    // 5. Live allocation, with the writer cache saturated and every moving
    // owner attributed at each growth checkpoint.
    for (_, arm) in &arms {
        assert!(
            arm.cache_occupancy >= CEILING / 16 - 4096 && arm.cache_occupancy <= CEILING / 16,
            "the writer cache has saturated its fixed share before measuring: {arms:?}"
        );
        for c in &arm.checkpoints {
            assert!(
                c.cache_occupancy <= CEILING / 16,
                "the cache never exceeds its share while measuring: {arms:?}"
            );
        }
        // Within one arm, unattributed allocation does not climb with commits.
        let first = arm.checkpoints[0].unattributed();
        for c in &arm.checkpoints {
            assert!(
                c.unattributed() <= first + ALLOCATION_SPREAD as isize,
                "unattributed live allocation grows with measured commits: {arms:?}"
            );
        }
    }
    assert!(
        b.allocated_start <= a.allocated_start + 4096,
        "reopening the larger journal starts from the same saturated live allocation: {arms:?}"
    );
    for (ca, cb) in a.checkpoints.iter().zip(&b.checkpoints) {
        assert!(
            cb.unattributed() <= ca.unattributed() + ALLOCATION_SPREAD as isize,
            "unattributed live allocation cannot grow with registered history at {} commits: {arms:?}",
            ca.commits
        );
    }
    // Controls against a no-op: the signed history stays complete, each update
    // retired exactly its root's ownership, the reads really validate, and a
    // status answer, when the reader limit admits it, counts every unit.
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
        assert!(
            arm.tampered_refused,
            "a removed companion makes the next durable read refuse: {arms:?}"
        );
        match &arm.status {
            Ok((eligible, pending, accepted)) => {
                assert_eq!(*eligible, *n as u64, "{arms:?}");
                assert_eq!(*pending, K as u64, "{arms:?}");
                assert_eq!(*accepted, (*n - K) as u64, "{arms:?}");
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
    // The received-unit arm: the hub's custody work for K arriving units does
    // not grow with the units it already received.
    let (_, ha) = &hubs[0];
    let (_, hb) = &hubs[1];
    for (n, hub) in &hubs {
        assert_eq!(hub.hydrated, 0, "the hub never hydrates its journal: {hubs:?}");
        assert_eq!(hub.resident, (0, 0), "{hubs:?}");
        assert_eq!(hub.applied_rows, K + K / 2, "{hubs:?}");
        assert_eq!(hub.hub_received, (*n + K) as u64, "{hubs:?}");
    }
    assert!(ha.decoded > 0, "the received units did real custody work: {hubs:?}");
    assert_eq!(
        ha.decoded, hb.decoded,
        "the hub's custody decodes cannot grow with received history: {hubs:?}"
    );
}
