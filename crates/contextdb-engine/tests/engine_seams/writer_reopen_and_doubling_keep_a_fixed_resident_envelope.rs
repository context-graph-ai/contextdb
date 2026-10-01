//! "Continued growth" and "Paging and eviction": memory settles within a fixed
//! working set; durable rows, indexes and metadata load on demand and are released.
use crate::resident_history_support as support;
use contextdb_core::{Lsn, TenantId, Value, Wallclock};
use contextdb_engine::{
    Database,
    sync_types::{ConflictPolicies, ConflictPolicy},
};
use serde::{Deserialize, Serialize};
use std::path::Path;
use support::*;

// Fresh single-test child arms measure live allocations independently of RSS.
// This catches proportional owners smaller than allocator retained-page slack.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
static LIVE_ALLOCATIONS: AtomicIsize = AtomicIsize::new(0);
static ALLOCATION_PEAK: AtomicUsize = AtomicUsize::new(0);
struct ResidentAllocator;
fn allocated_growth(bytes: isize) {
    let live = LIVE_ALLOCATIONS.fetch_add(bytes, Ordering::Relaxed) + bytes;
    ALLOCATION_PEAK.fetch_max(live.max(0) as usize, Ordering::Relaxed);
}
unsafe impl GlobalAlloc for ResidentAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            allocated_growth(layout.size() as isize);
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            allocated_growth(layout.size() as isize);
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE_ALLOCATIONS.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // Keep the old/new overlap visible instead of hiding System::realloc's
        // internal copy from the peak oracle. Failure preserves the old owner.
        let new_layout = unsafe { Layout::from_size_align_unchecked(size, layout.align()) };
        let updated = unsafe { System.alloc(new_layout) };
        if !updated.is_null() {
            allocated_growth(size as isize);
            unsafe { std::ptr::copy_nonoverlapping(pointer, updated, layout.size().min(size)) };
            unsafe { System.dealloc(pointer, layout) };
            LIVE_ALLOCATIONS.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        }
        updated
    }
}
#[global_allocator]
static RESIDENT_ALLOCATOR: ResidentAllocator = ResidentAllocator;
pub fn live_allocations_for_test() -> usize {
    LIVE_ALLOCATIONS.load(Ordering::Relaxed).max(0) as usize
}
pub fn reset_allocation_peak_for_test() {
    ALLOCATION_PEAK.store(live_allocations_for_test(), Ordering::Relaxed);
}
pub fn allocation_peak_for_test() -> usize {
    ALLOCATION_PEAK.load(Ordering::Relaxed)
}

const ARM: &str = "CONTEXTDB_RESIDENT_ENVELOPE_ROWS";
const FIXTURE: &str = "CONTEXTDB_RESIDENT_ENVELOPE_FIXTURE";
const CEILING: usize = 64 * 1024 * 1024;
const SETTLING_SLACK: usize = 1024 * 1024;
#[derive(Debug, Serialize, Deserialize)]
struct Sample {
    charged: usize,
    anonymous: u64,
    allocated: usize,
    allocated_peak: usize,
    redb_allocator_metadata: usize,
    mirror_families: [usize; 13],
    sidecars: contextdb_engine::database::resident_history_test_seams::SidecarOwners,
    file_bytes: u64,
    gap: i64,
}
fn sample(db: &Database, path: &Path) -> Sample {
    let r = db.writer_memory_ownership_receipt_for_test();
    let inventory = &r.relational_inventory;
    eprintln!("ownership checkpoint: {r:?}");
    // Fixed exact-probe filters belong to schema. They are accounted in charged,
    // but cannot be mistaken for a per-history mirror in this zero-owner check.
    Sample {
        charged: r.named_owner_total,
        anonymous: anonymous_bytes(),
        allocated: live_allocations_for_test(),
        allocated_peak: allocation_peak_for_test(),
        redb_allocator_metadata: db.__allocator_metadata_bytes_for_test().unwrap(),
        gap: r.reconciliation_gap,
        mirror_families: [
            db.__resident_row_directory_capacity_for_test(),
            inventory.row_positions,
            inventory.row_version_positions,
            inventory.sync_provenance,
            inventory.index_trees,
            inventory.index_exact,
            inventory.index_membership,
            r.graph_entries,
            r.change_log_charges.iter().sum(),
            db.__resident_commit_directory_bytes_for_test(),
            db.vector_store_for_test()
                .resident_membership_directory_bytes_for_test(),
            r.vector.mutable_tail,
            db.__retention_directory_bytes_for_test(),
        ],
        sidecars: db.__history_sidecar_owners_for_test(),
        file_bytes: std::fs::metadata(path).unwrap().len(),
    }
}
fn prepare(path: &Path, n: usize) {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let source = Database::open_memory();
    source.set_maintenance_policy(contextdb_engine::MaintenancePolicy::CallerDriven);
    exec(
        &source,
        "CREATE TABLE items (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(16) PARTITION_KEY (scope) MAX_PARTITIONS 2, revision INT) HISTORY ALL SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
    );
    exec(
        &source,
        "CREATE INDEX items_revision ON items (revision,id)",
    );
    // Large nonresident bodies grow the actual durable page allocator, too.
    exec(
        &source,
        "CREATE TABLE payloads (id INT PRIMARY KEY, body TEXT) HISTORY ALL SYNC TWO WAY",
    );
    for id in 0..n {
        source
            .execute(
                "INSERT INTO items VALUES ($id,0,$v,0)",
                &std::collections::HashMap::from([
                    ("id".into(), Value::Int64(id as i64)),
                    ("v".into(), Value::Vector(vector(id, 16))),
                ]),
            )
            .unwrap();
        source
            .execute(
                "INSERT INTO payloads VALUES ($id,$body)",
                &std::collections::HashMap::from([
                    ("id".into(), Value::Int64(id as i64)),
                    (
                        "body".into(),
                        Value::Text(format!("{id}:{}", "p".repeat(8192))),
                    ),
                ]),
            )
            .unwrap();
        let tx = source.begin_or_panic();
        source
            .insert_edge(
                tx,
                uuid::Uuid::from_u128(20000 + id as u128),
                uuid::Uuid::from_u128(20001 + id as u128),
                "LINKS".into(),
                empty(),
            )
            .unwrap();
        source.commit(tx).unwrap();
    }
    let db = open(path, Some(CEILING));
    db.apply_changes(
        source.changes_since(Lsn(0)),
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .unwrap();
    for id in 0..n {
        exec(
            &db,
            &format!("UPDATE items SET revision = 1 WHERE id = {id}"),
        );
    }
    let received_source_lsn = source.current_lsn();
    // Authored tombstone and graph-arrival sidecars must participate in growth.
    for id in 0..n / 4 {
        exec(&source, &format!("DELETE FROM payloads WHERE id = {id}"));
    }
    db.apply_changes(
        source.changes_since(received_source_lsn),
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .unwrap();
    let ddl_since = source.current_lsn();
    for ordinal in 0..n / 4 {
        let ddl = format!(
            "ALTER TABLE items ALTER COLUMN v SET HNSW (EF_SEARCH = {})",
            32 + ordinal
        );
        exec(&db, &ddl);
        exec(&source, &ddl);
    }
    db.apply_changes(
        source.changes_since(ddl_since),
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .unwrap();

    // Real authenticated-reply semantics populate terminal refusal evidence;
    // the helper is the existing test-only door to the actual reply commit.
    exec(
        &db,
        "CREATE TABLE refused_items (id INT PRIMARY KEY) HISTORY ALL SYNC TWO WAY",
    );
    for id in 0..n / 4 {
        exec(&db, &format!("INSERT INTO refused_items VALUES ({id})"));
    }
    let refused = db
        .changes_since(Lsn(0))
        .rows
        .into_iter()
        .filter(|row| row.table == "refused_items" && !row.deleted)
        .collect::<Vec<_>>();
    assert_eq!(refused.len(), n / 4);
    let tenant = TenantId::from("resident-fixture");
    db.register_retention_sync_peer("fixture-hub").unwrap();
    db.record_hub_push_reply_effects_for_hub(
        &tenant,
        "fixture-hub",
        &refused,
        &[],
        &[],
        Lsn(900_000),
    )
    .unwrap();
    assert!(
        db.has_terminal_refusal_markers_for_test(&tenant, "fixture-hub")
            .unwrap()
    );

    // A retained old frontier keeps real, durable expiry identities after
    // current answers expire; no observer-created mirror is inserted.
    exec(
        &db,
        "CREATE TABLE expired_items (id INT PRIMARY KEY, expires TIMESTAMP EXPIRES) RETAIN 1 HOURS SYNC OFF",
    );
    for id in 0..n / 4 {
        db.execute(
            "INSERT INTO expired_items VALUES ($id,$expiry)",
            &std::collections::HashMap::from([
                ("id".into(), Value::Int64(id as i64)),
                ("expiry".into(), Value::Timestamp(100)),
            ]),
        )
        .unwrap();
    }
    let expiry_frontier = db.snapshot();
    let pin = db.pin_snapshot(expiry_frontier);
    db.run_pruning_cycle_checked().unwrap();
    assert_eq!(
        exec(&db, "SELECT COUNT(*) FROM expired_items"),
        vec![vec![Value::Int64(0)]]
    );
    assert_eq!(
        db.execute_at_snapshot(
            "SELECT COUNT(*) FROM expired_items",
            &empty(),
            expiry_frontier
        )
        .unwrap()
        .rows,
        vec![vec![Value::Int64((n / 4) as i64)]]
    );
    drop(pin);
    db.close().unwrap();
}
fn arm(path: &Path, n: usize) -> Vec<Sample> {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    // The measured process never constructs a source, fixture, or full oracle.
    reset_allocation_peak_for_test();
    let db = open(path, Some(CEILING));
    let mut samples = vec![sample(&db, path)];
    for checkpoint in 0..2 {
        for id in n + checkpoint * n / 2..n + (checkpoint + 1) * n / 2 {
            db.execute(
                "INSERT INTO items VALUES ($id,0,$v,1)",
                &std::collections::HashMap::from([
                    ("id".into(), Value::Int64(id as i64)),
                    ("v".into(), Value::Vector(vector(id, 16))),
                ]),
            )
            .unwrap();
            let tx = db.begin_or_panic();
            db.insert_edge(
                tx,
                uuid::Uuid::from_u128(30000 + id as u128),
                uuid::Uuid::from_u128(30001 + id as u128),
                "LINKS".into(),
                empty(),
            )
            .unwrap();
            db.commit(tx).unwrap();
        }
        assert!(
            db.has_terminal_refusal_markers_for_test(
                &TenantId::from("resident-fixture"),
                "fixture-hub"
            )
            .unwrap(),
            "durable refusal evidence survives reopen and each append checkpoint"
        );
        for id in [0, 1, 2, 3] {
            assert_eq!(
                exec(&db, &format!("SELECT revision FROM items WHERE id = {id}")),
                vec![vec![Value::Int64(1)]]
            );
        }
        assert_eq!(
            exec(
                &db,
                "SELECT id FROM items WHERE revision = 1 ORDER BY id LIMIT 4"
            ),
            (0..4).map(|id| vec![Value::Int64(id)]).collect::<Vec<_>>()
        );
        assert_eq!(
            query_vector(&db, "items", 0, 16, "EXACT"),
            vec![vec![Value::Int64(0)]]
        );
        assert_eq!(
            db.edge_count(uuid::Uuid::from_u128(20000), "LINKS", db.snapshot())
                .unwrap(),
            1
        );
        samples.push(sample(&db, path));
    }
    assert_eq!(
        exec(&db, "SELECT COUNT(*) FROM items"),
        vec![vec![Value::Int64((2 * n) as i64)]]
    );
    db.close().unwrap();
    samples
}
#[test]
fn writer_reopen_and_doubling_keep_a_fixed_resident_envelope() {
    if let Ok(n) = std::env::var(ARM) {
        let path = std::env::var(FIXTURE).unwrap();
        println!(
            "ENVELOPE {}",
            serde_json::to_string(&arm(Path::new(&path), n.parse().unwrap())).unwrap()
        );
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let mut arms = Vec::<Vec<Sample>>::new();
    for n in [128, 256] {
        let path = dir.path().join(format!("history-{n}.db"));
        prepare(&path, n);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &crate::child_test_filter(
                    module_path!(),
                    "writer_reopen_and_doubling_keep_a_fixed_resident_envelope",
                ),
                "--nocapture",
            ])
            .env(ARM, n.to_string())
            .env(FIXTURE, &path)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "valid fixture arm failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        let json = stdout
            .lines()
            .find_map(|line| line.strip_prefix("ENVELOPE "))
            .expect("child allocation evidence");
        arms.push(serde_json::from_str(json).unwrap());
    }
    eprintln!("N/2N fresh-process checkpoints (RSS includes redb allocator): {arms:?}");
    assert!(
        arms[1][0].file_bytes > arms[0][0].file_bytes,
        "durable fixture really grows allocator pages"
    );
    for arm in &arms {
        for sample in arm {
            assert!(sample.charged <= CEILING, "frozen writer ceiling");
            assert!(
                sample.allocated_peak <= CEILING + 32 * 1024 * 1024,
                "independent peak includes startup, transient allocator overlap and append scratch"
            );
            assert!(
                sample.redb_allocator_metadata <= 256 * 1024,
                "retained allocator metadata has its own frozen capacity envelope"
            );
            assert!(
                sample.anonymous <= (CEILING + 32 * 1024 * 1024) as u64,
                "absolute anonymous allocation envelope includes fixed process overhead"
            );
        }
    }
    for arm in &arms {
        assert!(
            arm[2].allocated <= arm[1].allocated + 4096,
            "live allocator ownership must settle after equal hot sets; a reduced proportional coefficient still fails: {arm:?}"
        );
        assert!(
            arm[2].charged <= arm[1].charged + SETTLING_SLACK,
            "successive append checkpoints settle inside one fixed ownership envelope"
        );
        assert!(
            arm[2].anonymous <= arm[1].anonymous + SETTLING_SLACK as u64,
            "successive append checkpoints cannot retain proportional anonymous allocation"
        );
    }
    for i in 0..3 {
        assert!(
            arms[1][i].redb_allocator_metadata <= arms[0][i].redb_allocator_metadata + 4096,
            "actual allocator capacities cannot grow proportionally with durable pages: {arms:?}"
        );
        assert!(
            arms[1][i].allocated <= arms[0][i].allocated + 4096,
            "doubling durable history must not retain proportional live allocation, including allocator state: {arms:?}"
        );
        assert!(
            arms[1][i].charged <= arms[0][i].charged + SETTLING_SLACK,
            "charged residency settles at fixed checkpoints"
        );
        assert!(
            arms[1][i].anonymous <= arms[0][i].anonymous + SETTLING_SLACK as u64,
            "actual allocation includes allocator and sidecars; it settles without source-fixture allocator reuse"
        );
    }
    assert!(
        arms.iter().flatten().all(|sample| sample.gap == 0),
        "every charged owner reconciles: {arms:?}"
    );
    assert!(
        arms.iter().flatten().all(
            |sample| sample.mirror_families.iter().all(|bytes| *bytes == 0)
                && sample.sidecars.total() == 0
        ),
        "every per-history mirror family must be absent; fixed schema filters are accounted separately: {arms:?}"
    );
}
