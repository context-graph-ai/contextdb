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
    additional_owners: [usize; 6],
    startup_peak: usize,
    append_peak: usize,
    redb_allocator_metadata: usize,
    mirror_families: [usize; 13],
    vector_membership_bytes: usize,
    sidecars: contextdb_engine::database::resident_history_test_seams::SidecarOwners,
    file_bytes: u64,
    gap: i64,
}
fn sample(db: &Database, path: &Path, startup_peak: usize, append_peak: usize) -> Sample {
    let allocated_peak = allocation_peak_for_test();
    let allocated = live_allocations_for_test();
    let r = db.writer_memory_ownership_receipt_for_test();
    let inventory = &r.relational_inventory;
    eprintln!("ownership checkpoint: {r:?}");
    // Fixed exact-probe filters belong to schema. They are accounted in charged,
    // but cannot be mistaken for a per-history mirror. Vector membership
    // includes current and historical placement owners, measured separately.
    Sample {
        charged: r.named_owner_total,
        anonymous: anonymous_bytes(),
        allocated,
        allocated_peak,
        additional_owners: db.__additional_resident_owners_for_test(),
        startup_peak,
        append_peak,
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
            r.vector.raw_vector_bodies
                + r.vector.raw_reverse_directory
                + r.vector.partition_vectors,
            r.vector.mutable_tail,
            db.__retention_directory_bytes_for_test(),
        ],
        vector_membership_bytes: db
            .vector_store_for_test()
            .resident_membership_directory_bytes_for_test(),
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
    let db = open(path, Some(CEILING));
    let mut applied = Lsn(0);
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
                        Value::Text({
                            let mut state = id as u64 + 1;
                            (0..256 * 1024)
                                .map(|_| {
                                    state ^= state << 13;
                                    state ^= state >> 7;
                                    state ^= state << 17;
                                    char::from(32 + (state % 95) as u8)
                                })
                                .collect()
                        }),
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
        // Grow the durable fixture through real fixed-sized sync units under
        // the same writer ceiling, rather than one unbounded output batch.
        db.apply_changes(
            source
                .changes_since(applied)
                .expect("read the declared changes"),
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();
        applied = source.current_lsn();
    }
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
        source
            .changes_since(received_source_lsn)
            .expect("read the declared changes"),
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
        source
            .changes_since(ddl_since)
            .expect("read the declared changes"),
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
        .expect("read the declared changes")
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
    let received_schema_read_memory_1 = db
        .declared_reader_memory()
        .expect("admit the sync observation reader");

    assert!(
        db.has_terminal_refusal_markers_for_test(
            &tenant,
            "fixture-hub",
            received_schema_read_memory_1
        )
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
    let pin = db.pin_snapshot(expiry_frontier).unwrap();
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
    exec(
        &db,
        "CREATE TABLE cache_pressure (id INT PRIMARY KEY, body TEXT) HISTORY ALL SYNC OFF",
    );
    for batch in 0..64 {
        let mut params =
            std::collections::HashMap::from([("body".to_owned(), Value::Text("p".repeat(1024)))]);
        let values = (0..128)
            .map(|offset| {
                let name = format!("id{offset}");
                params.insert(name.clone(), Value::Int64(batch * 128 + offset));
                format!("(${name},$body)")
            })
            .collect::<Vec<_>>()
            .join(",");
        db.execute(
            &format!("INSERT INTO cache_pressure VALUES {values}"),
            &params,
        )
        .unwrap();
    }
    db.close().unwrap();
}
fn saturate_cache(db: &Database) {
    // Uniform small leaves fill the declared share without wide-page eviction
    // overshoot. The same 8 MiB fixture warms every measured checkpoint.
    // The ordinal seam walks every prior wide leaf; visit the range once.
    drop(db.__hold_row_page_for_test("cache_pressure", 8191).unwrap());
    let stats = db.__writer_cache_stats_for_test().unwrap();
    assert!(
        stats.2 > 0,
        "the declared share must reach real eviction: {stats:?}"
    );
    assert!(
        stats.3 >= CEILING / 16 - 4096 && stats.3 <= CEILING / 16,
        "cache occupancy has saturated its fixed share: {stats:?}"
    );
}
fn arm(path: &Path, n: usize) -> Vec<Sample> {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    // The measured process never constructs a source, fixture, or full oracle.
    reset_allocation_peak_for_test();
    let db = open(path, Some(CEILING));
    let startup_peak = allocation_peak_for_test();
    reset_allocation_peak_for_test();
    saturate_cache(&db);
    // Fund all three measurement records before measuring the first checkpoint.
    let mut samples = Vec::with_capacity(3);
    samples.push(sample(&db, path, startup_peak, 0));
    for checkpoint in 0..2 {
        reset_allocation_peak_for_test();
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
        let membership_before_moves = db
            .vector_store_for_test()
            .resident_membership_directory_bytes_for_test();
        exec(&db, "UPDATE items SET scope = 1 WHERE id = 1");
        assert_eq!(
            exec(&db, "SELECT scope FROM items WHERE id = 1"),
            vec![vec![Value::Int64(1)]]
        );
        assert_eq!(
            db.vector_store_for_test()
                .resident_membership_directory_bytes_for_test(),
            membership_before_moves,
            "moving a row cannot retain additional placement ownership"
        );
        exec(&db, "UPDATE items SET v = NULL WHERE id = 1");
        assert_eq!(
            exec(&db, "SELECT v FROM items WHERE id = 1"),
            vec![vec![Value::Null]]
        );
        assert_eq!(
            db.vector_store_for_test()
                .resident_membership_directory_bytes_for_test(),
            membership_before_moves,
            "deleting a vector cannot retain additional membership history"
        );
        db.execute(
            "UPDATE items SET scope = 0, v = $v WHERE id = 1",
            &std::collections::HashMap::from([("v".into(), Value::Vector(vector(1, 16)))]),
        )
        .unwrap();
        assert_eq!(
            query_vector(&db, "items", 1, 16, "EXACT"),
            vec![vec![Value::Int64(1)]]
        );
        assert_eq!(
            db.vector_store_for_test()
                .resident_membership_directory_bytes_for_test(),
            membership_before_moves
        );
        let append_peak = allocation_peak_for_test();
        reset_allocation_peak_for_test();
        let received_schema_read_memory_2 = db
            .declared_reader_memory()
            .expect("admit the sync observation reader");

        assert!(
            db.has_terminal_refusal_markers_for_test(
                &TenantId::from("resident-fixture"),
                "fixture-hub",
                received_schema_read_memory_2
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
        saturate_cache(&db);
        samples.push(sample(&db, path, startup_peak, append_peak));
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
    let mut durable_populations = Vec::new();
    for n in [128, 256] {
        let path = dir.path().join(format!("history-{n}.db"));
        Database::__create_region_fixture_for_test(&path, 1024 * 1024).unwrap();
        prepare(&path, n);
        let prepared = open(&path, None);
        durable_populations.push(prepared.__durable_page_population_for_test().unwrap());
        prepared.close().unwrap();
        drop(prepared);
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
        eprintln!(
            "{n}-row arm owner receipts:\n{}",
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
    assert!(
        durable_populations[0].0 >= 8,
        "actual durable population crosses many declared fixture regions"
    );
    assert!(
        durable_populations[1].0 * 2 >= durable_populations[0].0 * 3,
        "durable region population grows materially rather than only preallocated file length: {arms:?}"
    );
    assert!(
        durable_populations[1].1 * 2 >= durable_populations[0].1 * 3,
        "used pages grow alongside doubled incompressible content: {arms:?}"
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
        arms.iter()
            .flatten()
            .all(|sample| sample.additional_owners == [0; 6]),
        "reverse adjacency, sync authors, refusal scans, DDL log, paged publications and vector graphs remain absent at every measured checkpoint: {arms:?}"
    );
    assert!(
        arms.iter().flatten().all(|sample| sample.gap == 0),
        "every charged owner reconciles: {arms:?}"
    );
    #[allow(clippy::needless_range_loop)] // Preserve checkpoint indexing in the assertion contract.
    for checkpoint in 0..3 {
        assert_eq!(
            arms[1][checkpoint].vector_membership_bytes,
            arms[0][checkpoint].vector_membership_bytes,
            "doubling durable vectors cannot grow retained membership ownership"
        );
    }
    assert!(
        arms.iter().flatten().all(
            |sample| sample.mirror_families.iter().all(|bytes| *bytes == 0)
                && sample.sidecars.total() == 0
        ),
        "every per-history mirror family must be absent; fixed schema filters are accounted separately: {arms:?}"
    );
}

// Every writer operation copies only its selected declarations: unrelated
// schema width must not grow either the actual or admitted operation peak.
const COPY_SCHEMA_WIDTH: &str = "CONTEXTDB_WRITER_COPY_SCHEMA_WIDTH";
#[derive(Debug, Serialize, Deserialize)]
struct CopyOperationSample {
    actual_peak: usize,
    charged_peak: usize,
    actual_retained: isize,
    charged_retained: isize,
    source_work: [usize; 4],
}
fn measure_writer_copy<T>(
    db: &Database,
    operation: impl FnOnce() -> T,
) -> (T, CopyOperationSample) {
    let actual = live_allocations_for_test();
    let charged = db.accountant().usage().used;
    db.accountant().reset_high_water_for_test();
    reset_allocation_peak_for_test();
    let (result, work) =
        contextdb_engine::database::resident_history_test_seams::observe(operation);
    // Capture all gauges before JSON, debug, receipt, or readback allocations.
    let actual_peak = allocation_peak_for_test().checked_sub(actual).unwrap();
    let charged_peak = db
        .accountant()
        .high_water_for_test()
        .checked_sub(charged)
        .unwrap();
    let actual_retained = live_allocations_for_test() as isize - actual as isize;
    let charged_retained = db.accountant().usage().used as isize - charged as isize;
    (
        result,
        CopyOperationSample {
            actual_peak,
            charged_peak,
            actual_retained,
            charged_retained,
            source_work: [
                work.cleanup_versions_examined,
                work.retention_keys_examined,
                work.graph_keys_examined,
                work.log_keys_examined,
            ],
        },
    )
}
#[test]
#[allow(clippy::int_plus_one)] // Preserve the existing test expression and its assertion contract.
fn writer_copy_operations_keep_fixed_work_with_wide_unrelated_schemas() {
    if let Ok(width) = std::env::var(COPY_SCHEMA_WIDTH) {
        let width: usize = width.parse().unwrap();
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("writer-copies.db");
        let db = open(&path, Some(CEILING));
        for ordinal in 0..width {
            let columns = (0..32)
                .map(|i| format!("field_{i:02} TEXT"))
                .collect::<Vec<_>>()
                .join(",");
            exec(
                &db,
                &format!(
                    "CREATE TABLE unrelated_{ordinal:04} (id INT PRIMARY KEY,{columns}) HISTORY ALL SYNC OFF"
                ),
            );
        }
        exec(
            &db,
            "CREATE TABLE selected_versions (id INT PRIMARY KEY,revision INT) HISTORY CURRENT ONLY SYNC OFF",
        );
        exec(
            &db,
            "CREATE TABLE selected_purge (id INT PRIMARY KEY,body TEXT) HISTORY ALL SYNC OFF",
        );
        exec(&db, "INSERT INTO selected_versions VALUES (1,0)");
        exec(
            &db,
            "UPDATE selected_versions SET revision = 1 WHERE id = 1",
        );
        exec(&db, "INSERT INTO selected_purge VALUES (1,'erase-me')");
        assert_eq!(
            db.table_names().len(),
            width + 2,
            "the unrelated schema population really grows"
        );
        assert_eq!(
            exec(&db, "SELECT revision FROM selected_versions WHERE id = 1"),
            vec![vec![Value::Int64(1)]]
        );

        let source = Database::open_memory();
        source.set_maintenance_policy(contextdb_engine::MaintenancePolicy::CallerDriven);
        exec(
            &source,
            "CREATE TABLE selected_received (id INT PRIMARY KEY,body TEXT) HISTORY ALL SYNC TWO WAY",
        );
        exec(
            &source,
            "INSERT INTO selected_received VALUES (1,'received')",
        );
        let changes = source
            .changes_since(Lsn(0))
            .expect("read the declared changes");
        assert_eq!(changes.rows.len(), 1, "the received data work is fixed");
        let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);

        let (commit, commit_peak) = measure_writer_copy(&db, || {
            db.execute(
                "UPDATE selected_versions SET revision = 2 WHERE id = 1",
                &empty(),
            )
        });
        drop(commit.expect("commit the one selected row"));
        assert_eq!(
            exec(&db, "SELECT revision FROM selected_versions WHERE id = 1"),
            vec![vec![Value::Int64(2)]]
        );
        let (cleanup, cleanup_peak) = measure_writer_copy(&db, || db.compact_currency_versions());
        let cleanup = cleanup.expect("reclaim the selected current-only history");
        assert_eq!(
            cleanup.pruned_versions, 2,
            "cleanup performs the same nonzero work"
        );
        assert!(
            cleanup_peak.source_work[0] > 0,
            "observe the actual cleanup cursor"
        );
        assert_eq!(
            exec(&db, "SELECT revision FROM selected_versions WHERE id = 1"),
            vec![vec![Value::Int64(2)]]
        );
        let (purge, purge_peak) = measure_writer_copy(&db, || {
            db.execute("PURGE FROM selected_purge WHERE id = 1", &empty())
        });
        drop(purge.expect("authoritatively purge the one selected row"));
        assert!(exec(&db, "SELECT id FROM selected_purge").is_empty());
        let (received, received_peak) =
            measure_writer_copy(&db, || db.apply_changes(changes, &policies));
        assert_eq!(
            received
                .expect("publish the selected received declaration and row")
                .applied_rows,
            1
        );
        assert_eq!(
            exec(&db, "SELECT id,body FROM selected_received"),
            vec![vec![Value::Int64(1), Value::Text("received".into())]]
        );
        assert_eq!(
            db.table_names().len(),
            width + 3,
            "unrelated declarations survive every writer operation"
        );
        assert_eq!(
            db.writer_memory_ownership_receipt_for_test()
                .reconciliation_gap,
            0
        );
        let samples = [commit_peak, cleanup_peak, purge_peak, received_peak];
        assert!(
            samples
                .iter()
                .all(|s| s.actual_peak > 0 && s.charged_peak > 0),
            "real allocator and writer admissions must both be observed: {samples:?}"
        );
        println!(
            "writer-copy-samples:{}",
            serde_json::to_string(&samples).unwrap()
        );
        db.close().unwrap();
        return;
    }
    let mut arms = Vec::<Vec<CopyOperationSample>>::new();
    for width in [8, 32, 128] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &crate::child_test_filter(
                    module_path!(),
                    "writer_copy_operations_keep_fixed_work_with_wide_unrelated_schemas",
                ),
                "--nocapture",
                "--test-threads=1",
            ])
            .env(COPY_SCHEMA_WIDTH, width.to_string())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "width={width}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        let receipt = stdout
            .lines()
            .find_map(|line| line.strip_prefix("writer-copy-samples:"))
            .expect("fresh child emits the actual operation receipts");
        arms.push(serde_json::from_str(receipt).unwrap());
    }
    for phase in 0..4 {
        for arm in &arms[1..] {
            assert_eq!(
                arm[phase].source_work, arms[0][phase].source_work,
                "fixed selected work cannot examine growing unrelated schemas: phase={phase}, {arms:?}"
            );
            assert!(
                arm[phase].actual_peak
                    <= arms[0][phase].actual_peak + std::mem::size_of::<usize>() - 1,
                "actual operation peak cannot grow with unrelated schema copies: phase={phase}, {arms:?}"
            );
            assert_eq!(
                arm[phase].charged_peak, arms[0][phase].charged_peak,
                "admitted operation peak stays fixed: phase={phase}, {arms:?}"
            );
            assert!(
                arm[phase].actual_retained
                    <= arms[0][phase].actual_retained + (std::mem::size_of::<usize>() - 1) as isize,
                "settled allocator ownership cannot retain unrelated schema copies: phase={phase}, {arms:?}"
            );
            assert_eq!(
                arm[phase].charged_retained, arms[0][phase].charged_retained,
                "retained selected ownership is the same: phase={phase}, {arms:?}"
            );
        }
    }
}

#[test]
fn received_and_exported_partition_control_components_are_fixed_at_n_2n_4n() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let directory = tempfile::tempdir().unwrap();
    let mut reference: Option<[[[usize; 13]; 2]; 2]> = None;
    // Four current rows, two live partitions, one index and the same final
    // declarations/data at every size. Only superseded durable history grows.
    for updates in [16usize, 32, 64] {
        let received_path = directory.path().join(format!("received-{updates}.db"));
        let export_path = directory.path().join(format!("export-{updates}.db"));
        let source = Database::open_memory();
        source.set_maintenance_policy(contextdb_engine::MaintenancePolicy::CallerDriven);
        exec(
            &source,
            "CREATE TABLE controls (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(16) PARTITION_KEY (scope) MAX_PARTITIONS 2, revision INT) HISTORY ALL SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        );
        for id in 0..4 {
            source
                .execute(
                    "INSERT INTO controls VALUES ($id,$scope,$v,0)",
                    &std::collections::HashMap::from([
                        ("id".into(), Value::Int64(id)),
                        ("scope".into(), Value::Int64(id % 2)),
                        ("v".into(), Value::Vector(vector(id as usize, 16))),
                    ]),
                )
                .unwrap();
        }
        let received = open(&received_path, Some(CEILING));
        let mut frontier = Lsn(0);
        received
            .apply_changes(
                source.changes_since(frontier).unwrap(),
                &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
            )
            .unwrap();
        frontier = source.current_lsn();
        for revision in 1..=updates {
            let id = revision % 4;
            source
                .execute(
                    "UPDATE controls SET v = $v, revision = $revision WHERE id = $id",
                    &std::collections::HashMap::from([
                        ("id".into(), Value::Int64(id as i64)),
                        ("revision".into(), Value::Int64(revision as i64)),
                        ("v".into(), Value::Vector(vector(revision + 4, 16))),
                    ]),
                )
                .unwrap();
            received
                .apply_changes(
                    source.changes_since(frontier).unwrap(),
                    &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
                )
                .unwrap();
            frontier = source.current_lsn();
        }
        // Put the final live hotset into exactly the same state, through real
        // authored SQL and emitted received units, without trimming history.
        for id in 0..4 {
            source
                .execute(
                    "UPDATE controls SET v = $v, revision = 1 WHERE id = $id",
                    &std::collections::HashMap::from([
                        ("id".into(), Value::Int64(id)),
                        ("v".into(), Value::Vector(vector(id as usize, 16))),
                    ]),
                )
                .unwrap();
            received
                .apply_changes(
                    source.changes_since(frontier).unwrap(),
                    &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
                )
                .unwrap();
            frontier = source.current_lsn();
        }
        received.export_snapshot(&export_path).unwrap();
        received.close().unwrap();
        drop(received);
        drop(source);
        let mut cuts = [[[0usize; 13]; 2]; 2];
        for (image, path) in [&received_path, &export_path].into_iter().enumerate() {
            let db = open(path, Some(CEILING));
            db.set_maintenance_policy(contextdb_engine::MaintenancePolicy::CallerDriven);
            for phase in 0..2 {
                if phase == 1 {
                    assert_eq!(
                        exec(&db, "SELECT id,revision FROM controls ORDER BY id LIMIT 4"),
                        (0..4)
                            .map(|id| vec![Value::Int64(id), Value::Int64(1)])
                            .collect::<Vec<_>>()
                    );
                    for scope in 0..2 {
                        let rows = db.execute("SELECT id FROM controls WHERE scope = $scope ORDER BY v <=> $q USE VECTOR EXACT LIMIT 1",
                            &std::collections::HashMap::from([
                                ("scope".into(), Value::Int64(scope)),
                                ("q".into(), Value::Vector(vector(scope as usize, 16))),
                            ])).unwrap().rows;
                        assert_eq!(rows, vec![vec![Value::Int64(scope)]]);
                    }
                }
                let components = db
                    .vector_store_for_test()
                    .partition_vector_components_for_test();
                assert_eq!(
                    components[10..],
                    [0, 0, 0],
                    "all three actual history-mirror terms stay absent: updates={updates} image={image} phase={phase}"
                );
                let actual = db.vector_memory_ownership_receipt_for_test();
                assert_eq!(
                    components.iter().sum::<usize>(),
                    actual.vector_partition_controls + actual.partition_vectors,
                    "the shared source function exactly accounts for both families"
                );
                assert_eq!(
                    components[..10].iter().sum::<usize>(),
                    actual.vector_partition_controls
                );
                assert_eq!(
                    components[10..].iter().sum::<usize>(),
                    actual.partition_vectors
                );
                assert_eq!(
                    db.vector_store_for_test()
                        .partition_vector_components_for_test(),
                    components,
                    "passive native observation does not publish or resolve another owner"
                );
                if let Some(reference) = reference {
                    for term in 0..10 {
                        assert_eq!(
                            components[term], reference[image][phase][term],
                            "every moved component must be fixed individually: term={term} updates={updates} image={image} phase={phase}"
                        );
                    }
                }
                cuts[image][phase] = components;
            }
            db.close().unwrap();
        }
        if reference.is_none() {
            reference = Some(cuts);
        }
    }
}

#[test]
fn reclaimed_partition_churn_keeps_each_control_component_fixed_at_n_2n_4n() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let directory = tempfile::tempdir().unwrap();
    let mut reference: Option<[usize; 13]> = None;
    for moves in [16usize, 32, 64] {
        let path = directory.path().join(format!("partition-churn-{moves}.db"));
        let db = open(&path, Some(CEILING));
        db.set_maintenance_policy(contextdb_engine::MaintenancePolicy::CallerDriven);
        exec(
            &db,
            "CREATE TABLE churn (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(16) PARTITION_KEY (scope) MAX_PARTITIONS 2) HISTORY CURRENT ONLY SYNC OFF",
        );
        db.execute(
            "INSERT INTO churn VALUES (1,0,$v)",
            &std::collections::HashMap::from([("v".into(), Value::Vector(vector(1, 16)))]),
        )
        .unwrap();
        for scope in 1..=moves {
            db.execute(
                "UPDATE churn SET scope = $scope WHERE id = 1",
                &std::collections::HashMap::from([("scope".into(), Value::Int64(scope as i64))]),
            )
            .unwrap();
            let cleanup = db.compact_currency_versions().unwrap();
            assert_eq!(
                cleanup.versions_deferred_for_readers, 0,
                "no old snapshot keeps an obsolete partition live"
            );
            assert!(
                cleanup.pruned_versions > 0,
                "every new partition key leaves real history to reclaim"
            );
            assert_eq!(db.__physical_version_count_for_test("churn"), 1);
        }
        // All arms end at the same live key and data after the same reclaim phase.
        db.execute("UPDATE churn SET scope = 0 WHERE id = 1", &empty())
            .unwrap();
        let cleanup = db.compact_currency_versions().unwrap();
        assert_eq!(cleanup.versions_deferred_for_readers, 0);
        assert!(cleanup.pruned_versions > 0);
        assert_eq!(db.__physical_version_count_for_test("churn"), 1);
        assert_eq!(
            exec(&db, "SELECT id,scope FROM churn"),
            vec![vec![Value::Int64(1), Value::Int64(0)]]
        );
        let components = db
            .vector_store_for_test()
            .partition_vector_components_for_test();
        let receipt = db.vector_memory_ownership_receipt_for_test();
        assert_eq!(
            components[..10].iter().sum::<usize>(),
            receipt.vector_partition_controls
        );
        assert_eq!(
            components[10..].iter().sum::<usize>(),
            receipt.partition_vectors
        );
        if let Some(reference) = reference {
            for term in 0..10 {
                assert_eq!(
                    components[term], reference[term],
                    "reclaimed churn cannot retain obsolete partition controls: term={term} moves={moves}"
                );
            }
            assert_eq!(
                receipt.vector_partition_controls,
                reference[..10].iter().sum::<usize>()
            );
        } else {
            assert!(
                receipt.vector_partition_controls > 0,
                "observe the actual remaining live partition"
            );
            reference = Some(components);
        }
        db.close().unwrap();
    }
}
