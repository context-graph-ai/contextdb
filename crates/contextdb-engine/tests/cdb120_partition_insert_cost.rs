//! CDB-120: adding one vector to a partitioned column costs work bounded by its
//! own partition. It stays flat as the partition count and the retained history
//! grow, keeps the declared cap exact, and never refuses below the cap.
//!
//! Every row drives the public SQL surface of `Database` and reads the
//! test-seam work counters of its vector store. Each row is paired with a
//! mutant that removes the mechanism it proves (recorded in the evidence note).

use contextdb_core::{Error, Value, VectorIndexRef};
use contextdb_engine::memory_accounting::MemoryAccountant;
use contextdb_engine::{Database, MaintenancePolicy, SnapshotPin};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use tempfile::TempDir;
use uuid::Uuid;

/// The durable membership-record read counter is process-wide; rows that read
/// it must not interleave with any other row of this binary.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    ONE_AT_A_TIME.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn none() -> HashMap<String, Value> {
    HashMap::new()
}

fn id(n: u64) -> Uuid {
    Uuid::from_u128(0xC0DB_0120_0000_0000_0000_0000_0000_0000 | u128::from(n))
}

fn index() -> VectorIndexRef {
    VectorIndexRef::new("items", "embedding")
}

fn create(db: &Database, cap: u32, history: &str) {
    db.execute(
        &format!(
            "CREATE TABLE items (id UUID PRIMARY KEY, scope TEXT NOT NULL, \
             embedding VECTOR(2) PARTITION_KEY (scope) MAX_PARTITIONS {cap}) \
             {history} SYNC OFF"
        ),
        &none(),
    )
    .expect("create the partitioned fixture table");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
}

fn sql_insert(db: &Database, tx: Option<contextdb_core::TxId>, row: u64, scope: &str) -> contextdb_core::Result<()> {
    let params = HashMap::from([
        ("id".to_owned(), Value::Uuid(id(row))),
        ("scope".to_owned(), Value::Text(scope.to_owned())),
        ("v".to_owned(), Value::Vector(vec![1.0, (row % 97) as f32 / 97.0])),
    ]);
    let sql = "INSERT INTO items (id, scope, embedding) VALUES ($id, $scope, $v)";
    match tx {
        Some(tx) => db.execute_in_tx(tx, sql, &params).map(|_| ()),
        None => db.execute(sql, &params).map(|_| ()),
    }
}

fn sql_delete(db: &Database, tx: Option<contextdb_core::TxId>, row: u64) {
    let params = HashMap::from([("id".to_owned(), Value::Uuid(id(row)))]);
    let sql = "DELETE FROM items WHERE id = $id";
    match tx {
        Some(tx) => db.execute_in_tx(tx, sql, &params).map(|_| ()),
        None => db.execute(sql, &params).map(|_| ()),
    }
    .expect("delete one row");
}

fn sql_move(db: &Database, row: u64, scope: &str) -> contextdb_core::Result<()> {
    db.execute(
        "UPDATE items SET scope = $scope WHERE id = $id",
        &HashMap::from([
            ("id".to_owned(), Value::Uuid(id(row))),
            ("scope".to_owned(), Value::Text(scope.to_owned())),
        ]),
    )
    .map(|_| ())
}

/// New rows, committed in chunks of 128 per transaction: one commit must stay
/// inside the default 16 MiB copy budget even at 4096 partitions.
fn seed(db: &Database, scopes: impl IntoIterator<Item = (u64, String)>) {
    let rows: Vec<(u64, String)> = scopes.into_iter().collect();
    for chunk in rows.chunks(128) {
        let tx = db.begin().expect("begin seed transaction");
        for (row, scope) in chunk {
            sql_insert(db, Some(tx), *row, scope).expect("seed one row");
        }
        db.commit(tx).expect("commit seed transaction");
    }
}

fn counters(db: &Database) -> contextdb_vector::VectorCapWorkCountersForTest {
    db.vector_store_for_test().cap_work_counters_for_test()
}

/// The maintained cap figures equal a full recount.
fn assert_exact(db: &Database, step: &str) {
    let audit = db
        .vector_store_for_test()
        .partition_limit_audit_for_test(&index())
        .expect("the partitioned column is registered");
    assert_eq!(audit.occupied, audit.recounted_occupied, "{step}: occupied");
    let (mut maintained, mut recounted) = (audit.empty_counted.clone(), audit.recounted_empty_counted.clone());
    maintained.sort();
    recounted.sort();
    assert_eq!(maintained, recounted, "{step}: counted-empty set");
    assert_eq!(audit.directory_bytes, audit.recounted_directory_bytes, "{step}: directory bytes");
}

fn is_cap_refusal(error: &Error, cap: u32) -> bool {
    matches!(error, Error::VectorPartitionLimitExceeded { index: got, max_partitions } if *got == index() && *max_partitions == cap)
}

enum Flavor {
    Resident,
    Durable(TempDir),
}

impl Flavor {
    fn open(&self) -> Database {
        match self {
            Flavor::Resident => Database::open_memory(),
            Flavor::Durable(dir) => Database::open(dir.path().join("cdb120.redb")).expect("open durable fixture"),
        }
    }
    fn all() -> Vec<(&'static str, Flavor)> {
        vec![
            ("resident", Flavor::Resident),
            ("durable", Flavor::Durable(TempDir::new().expect("temp dir"))),
        ]
    }
}

// ---------------------------------------------------------------------------
// Scaling. One new-key insert and one existing-key insert cost one
// projected route, no cap-edge visit and no hold lookup, at 64 and 4096
// non-empty partitions alike.
// ---------------------------------------------------------------------------

#[test]
fn r1_one_row_insert_costs_one_route_at_64_and_at_4096_partitions() {
    let _one = serial();
    for (label, _) in Flavor::all() {
        let mut per_size = Vec::new();
        for partitions in [64_u64, 4096] {
            let flavor = match label {
                "resident" => Flavor::Resident,
                _ => Flavor::Durable(TempDir::new().unwrap()),
            };
            let db = flavor.open();
            create(&db, 8192, "HISTORY CURRENT ONLY");
            seed(&db, (0..partitions).map(|n| (n, format!("p{n}"))));
            assert_exact(&db, "after seeding");

            let before = counters(&db);
            sql_insert(&db, None, 1_000_000, "brand-new-key").unwrap_or_else(|e| {
                panic!("{label} K={partitions}: a new-key insert far below the cap must be admitted: {e}")
            });
            let after_new = counters(&db);
            sql_insert(&db, None, 1_000_001, "p0").unwrap_or_else(|e| {
                panic!("{label} K={partitions}: an existing-key insert must be admitted: {e}")
            });
            let after_existing = counters(&db);

            let new_key = (
                after_new.projected_routes - before.projected_routes,
                after_new.cap_edge_route_visits - before.cap_edge_route_visits,
                after_new.hold_lookups - before.hold_lookups,
            );
            let existing = (
                after_existing.projected_routes - after_new.projected_routes,
                after_existing.cap_edge_route_visits - after_new.cap_edge_route_visits,
                after_existing.hold_lookups - after_new.hold_lookups,
            );
            assert_eq!(new_key, (1, 0, 0), "{label} K={partitions}: new-key insert work");
            assert_eq!(existing, (1, 0, 0), "{label} K={partitions}: existing-key insert work");
            assert_exact(&db, "after the two inserts");
            per_size.push((new_key, existing));
        }
        assert_eq!(per_size[0], per_size[1], "{label}: per-insert work is identical at 64 and 4096 partitions");
    }
}

// ---------------------------------------------------------------------------
// R1b: exact refusal at the cap, admission below it.
// ---------------------------------------------------------------------------

#[test]
fn r1b_cap_four_refuses_the_fifth_key_and_admits_the_fourth() {
    let _one = serial();
    for (label, flavor) in Flavor::all() {
        let db = flavor.open();
        create(&db, 4, "HISTORY CURRENT ONLY");
        for n in 0..3 {
            sql_insert(&db, None, n, &format!("k{n}")).unwrap_or_else(|e| panic!("{label}: key {n} below the cap: {e}"));
        }
        sql_insert(&db, None, 3, "k3").unwrap_or_else(|e| panic!("{label}: the fourth key is the cap, not above it: {e}"));
        let refusal = sql_insert(&db, None, 4, "k4").expect_err("the fifth key exceeds the cap");
        assert!(is_cap_refusal(&refusal, 4), "{label}: typed refusal, got {refusal:?}");
        assert_exact(&db, "after refusal");
        sql_insert(&db, None, 5, "k0").unwrap_or_else(|e| panic!("{label}: an existing key stays writable at the cap: {e}"));
        let rows = db.execute("SELECT id FROM items", &none()).unwrap().rows.len();
        assert_eq!(rows, 5, "{label}: the refused write left no row behind");
    }
}

// ---------------------------------------------------------------------------
// The cap edge through SQL. A deleted-out route keeps a pending tombstone
// until maintenance retires it, so the cap-time sweep has no authority over it
// (inherited rule); the edge must neither visit historical routes nor refuse
// differently from the base, and must admit as soon as maintenance retires.
// ---------------------------------------------------------------------------

const EDGE_CAP: u32 = 8;
const EMPTIES: u64 = 7;

/// Retire `EDGE_CAP` keys twice so 2 x cap routes are historical-only.
fn make_historical_routes(db: &Database) {
    let mut row = 5_000;
    for wave in ["h", "g"] {
        let keys: Vec<(u64, String)> = (0..u64::from(EDGE_CAP))
            .map(|n| {
                row += 1;
                (row, format!("{wave}{n}"))
            })
            .collect();
        seed(db, keys.clone());
        for (row, _) in &keys {
            sql_delete(db, None, *row);
        }
        db.run_maintenance_cycle().expect("retire the emptied routes to historical-only");
        assert_exact(db, "after a historical wave");
    }
    let audit = db.vector_store_for_test().partition_limit_audit_for_test(&index()).unwrap();
    assert_eq!(audit.occupied, 0, "historical-only routes do not count against the cap");
}

#[test]
fn r3_edge_refuses_exactly_as_the_base_does_visits_no_historical_route_and_admits_after_retirement() {
    let _one = serial();
    for (label, flavor) in Flavor::all() {
        let db = flavor.open();
        create(&db, EDGE_CAP, "HISTORY CURRENT ONLY");
        make_historical_routes(&db);
        // Seven routes whose last vector is gone but whose old image a pin
        // still sees, and one live route: the cap is full.
        seed(&db, (0..EMPTIES).map(|n| (100 + n, format!("a{n}"))).chain([(300, "l".to_owned())]));
        let held = db.pin_snapshot(db.snapshot());
        for n in 0..EMPTIES {
            sql_delete(&db, None, 100 + n);
        }
        let audit = db.vector_store_for_test().partition_limit_audit_for_test(&index()).unwrap();
        assert_eq!(audit.occupied, EDGE_CAP as usize, "{label}: the cap is full");
        assert_eq!(audit.empty_counted.len() as u64, EMPTIES, "{label}: seven counted-empty routes");
        assert_exact(&db, "cap is full of counted routes");

        let before = counters(&db);
        let refusal = sql_insert(&db, None, 400, "brand-new").expect_err("every counted route is held");
        assert!(is_cap_refusal(&refusal, EDGE_CAP), "{label}: typed refusal, got {refusal:?}");
        let visits = counters(&db).cap_edge_route_visits - before.cap_edge_route_visits;
        assert!(
            visits <= EMPTIES,
            "{label}: the edge visits at most the {EMPTIES} counted-empty routes (never the 16 historical ones), visited {visits}"
        );
        assert_exact(&db, "after the refusal");
        assert_eq!(
            db.execute("SELECT id FROM items", &none()).unwrap().rows.len(),
            1,
            "{label}: the refused write left no row"
        );

        drop(held);
        db.run_maintenance_cycle().expect("retire the unheld emptied routes");
        sql_insert(&db, None, 400, "brand-new")
            .unwrap_or_else(|e| panic!("{label}: after retirement the cap has room: {e}"));
        assert_exact(&db, "after retirement and admission");
    }
}

// ---------------------------------------------------------------------------
// R3 extension, SQL door: the hold lookup for a route whose last vector moves
// out reads no membership history. Route `x` carries two bursts of `n` closed
// memberships, then one live vector; the live routes fill the cap (4); the
// last vector of `x` moves to a new key. Whether the move is admitted depends
// only on whether a registered pin sees `x`, wherever the pin sits relative to
// x's retained intervals.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PinAt {
    InsideFirstBurst,
    InsideSecondBurst,
    InGap,
    BeforeEverything,
    AfterLastCloseBeforeLiveVector,
    SeesTheLiveVector,
}

impl PinAt {
    const ALL: [PinAt; 6] = [
        PinAt::InsideFirstBurst,
        PinAt::InsideSecondBurst,
        PinAt::InGap,
        PinAt::BeforeEverything,
        PinAt::AfterLastCloseBeforeLiveVector,
        PinAt::SeesTheLiveVector,
    ];
    fn route_is_held(self) -> bool {
        matches!(self, PinAt::InsideFirstBurst | PinAt::InsideSecondBurst | PinAt::SeesTheLiveVector)
    }
}

fn delete_rows(db: &Database, first: u64, n: u64) {
    for chunk in (0..n).collect::<Vec<_>>().chunks(128) {
        let tx = db.begin().unwrap();
        for k in chunk {
            sql_delete(db, Some(tx), first + k);
        }
        db.commit(tx).unwrap();
    }
}

fn history_route<'a>(db: &'a Database, n: u64, at: PinAt) -> SnapshotPin<'a> {
    create(db, 4, "HISTORY ALL");
    let mut pins: Vec<(PinAt, SnapshotPin<'a>)> = Vec::new();
    let mut pin = |db: &'a Database, which: PinAt| pins.push((which, db.pin_snapshot(db.snapshot())));
    pin(db, PinAt::BeforeEverything);
    seed(db, (0..3).map(|k| (900 + k, format!("live{k}"))));
    seed(db, (0..n).map(|k| (10_000 + k, "x".to_owned())));
    pin(db, PinAt::InsideFirstBurst);
    delete_rows(db, 10_000, n);
    pin(db, PinAt::InGap);
    seed(db, (0..n).map(|k| (20_000 + k, "x".to_owned())));
    pin(db, PinAt::InsideSecondBurst);
    delete_rows(db, 20_000, n);
    pin(db, PinAt::AfterLastCloseBeforeLiveVector);
    seed(db, [(30_000, "x".to_owned())]);
    pin(db, PinAt::SeesTheLiveVector);
    let chosen = pins.iter().position(|(which, _)| *which == at).expect("the chosen pin was taken");
    pins.swap_remove(chosen).1
}

#[test]
fn r3_extension_the_last_vector_move_reads_no_history_while_a_route_grows_from_16_to_4096_memberships() {
    let _one = serial();
    for (label, _) in Flavor::all() {
        for at in PinAt::ALL {
            let mut observed = Vec::new();
            // Durable commits cost ~12 ms a row, so the durable column grows 16x, the resident 256x.
            let sizes = if label == "resident" { [16_u64, 4096] } else { [16_u64, 256] };
            for n in sizes {
                let flavor = if label == "resident" {
                    Flavor::Resident
                } else {
                    Flavor::Durable(TempDir::new().unwrap())
                };
                let db = flavor.open();
                let _pin = history_route(&db, n, at);
                assert_exact(&db, "after the history was built");
                db.__reset_vector_membership_records_decoded_for_test();
                let before = counters(&db);
                let outcome = sql_move(&db, 30_000, "newcomer");
                let after = counters(&db);
                let decoded = db.__vector_membership_records_decoded_for_test();
                match (&outcome, at.route_is_held()) {
                    (Err(e), true) => assert!(is_cap_refusal(e, 4), "{label} {at:?} n={n}: {e:?}"),
                    (Ok(()), false) => {}
                    (Ok(()), true) => panic!("{label} {at:?} n={n}: a pin that sees x must keep it counted"),
                    (Err(e), false) => panic!("{label} {at:?} n={n}: no pin sees x but the move was refused: {e}"),
                }
                assert_exact(&db, "after the move");
                let streamed = after.hold_streamed_records - before.hold_streamed_records;
                assert_eq!(
                    streamed, 0,
                    "{label} {at:?} n={n}: the hold check streamed {streamed} membership records instead of using the interval index"
                );
                observed.push((after.hold_lookups - before.hold_lookups, outcome.is_ok(), decoded));
            }
            assert_eq!(
                observed[0], observed[1],
                "{label} {at:?}: hold lookups, verdict and membership-record reads do not move with 256x more history"
            );
            assert!(observed[0].0 >= 1, "{label} {at:?}: the hold lookup ran");
        }
    }
}

// ---------------------------------------------------------------------------
// One fixed writer budget admits the same one-row insert while unrelated
// partitions and rows grow 16x.
// ---------------------------------------------------------------------------

const HEADROOM: usize = 192 * 1024;

#[test]
fn r4_the_same_insert_fits_the_same_headroom_as_unrelated_partitions_grow_16x() {
    let _one = serial();
    for partitions in [256_u64, 4096] {
        let accountant = Arc::new(MemoryAccountant::no_limit());
        let db = Database::open_memory_with_accountant(accountant.clone());
        create(&db, 8192, "HISTORY CURRENT ONLY");
        seed(&db, (0..partitions).flat_map(|n| [(n * 2, format!("p{n}")), (n * 2 + 1, format!("p{n}"))]));
        let settled = accountant.usage().used;
        accountant.set_budget(Some(settled + HEADROOM)).unwrap();
        sql_insert(&db, None, 900_000, "fresh-key")
            .unwrap_or_else(|e| panic!("{partitions} partitions: a new-key insert inside {HEADROOM} bytes of headroom: {e}"));
        sql_insert(&db, None, 900_001, "p3")
            .unwrap_or_else(|e| panic!("{partitions} partitions: an existing-key insert inside the headroom: {e}"));
        assert_eq!(accountant.underflow_count_for_test(), 0);
        assert_exact(&db, "after the budgeted inserts");
    }
}

// ---------------------------------------------------------------------------
// R2 (SQL door): the maintained cap figures equal a full recount after every
// step of a mixed sequence, including a reopen.
// ---------------------------------------------------------------------------

#[test]
fn r2_cap_figures_stay_exact_through_insert_move_delete_reclaim_refusal_reopen() {
    let _one = serial();
    for (label, flavor) in Flavor::all() {
        let db = flavor.open();
        create(&db, 4, "HISTORY CURRENT ONLY");
        for n in 0..4 {
            sql_insert(&db, None, n, &format!("k{n}")).unwrap();
            assert_exact(&db, "insert");
        }
        let refusal = sql_insert(&db, None, 9, "k9").unwrap_err();
        assert!(is_cap_refusal(&refusal, 4), "{label}: {refusal:?}");
        assert_exact(&db, "refused insert");

        let pin = db.pin_snapshot(db.snapshot());
        sql_delete(&db, None, 0);
        assert_exact(&db, "delete under a pin");
        let held_move = sql_move(&db, 1, "k5").unwrap_err();
        assert!(is_cap_refusal(&held_move, 4), "{label}: a pinned source keeps its slot: {held_move:?}");
        assert_exact(&db, "refused move");
        drop(pin);
        sql_move(&db, 1, "k5").unwrap_or_else(|e| panic!("{label}: the unpinned last-vector move reuses its slot: {e}"));
        assert_exact(&db, "move");

        db.run_maintenance_cycle().unwrap();
        assert_exact(&db, "maintenance reclaim");
        sql_insert(&db, None, 10, "k6").unwrap_or_else(|e| panic!("{label}: the reclaimed slot admits a new key: {e}"));
        assert_exact(&db, "insert into a reclaimed slot");

        if let Flavor::Durable(_) = &flavor {
            drop(db);
            let reopened = flavor.open();
            assert_exact(&reopened, "reopen");
            reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            let refusal = sql_insert(&reopened, None, 11, "k7").unwrap_err();
            assert!(is_cap_refusal(&refusal, 4), "reopened store enforces the same cap: {refusal:?}");
            assert_exact(&reopened, "refusal after reopen");
        }
    }
}

// A real process death: the child commits durable writes, then aborts with no
// unwind and no clean close; the parent reopens the file it left behind.
const CRASH_PATH_ENV: &str = "CDB120_CRASH_CHILD_PATH";

#[test]
fn r2_crash_child() {
    let Ok(path) = std::env::var(CRASH_PATH_ENV) else {
        return;
    };
    let db = Database::open(&path).expect("child opens its store");
    create(&db, 4, "HISTORY CURRENT ONLY");
    for n in 0..4 {
        sql_insert(&db, None, n, &format!("k{n}")).unwrap();
    }
    let _pin = db.pin_snapshot(db.snapshot());
    sql_delete(&db, None, 0);
    sql_insert(&db, None, 20, "k1").unwrap();
    std::process::abort();
}

#[test]
fn r2_a_process_death_after_durable_commits_reopens_with_exact_cap_figures() {
    let _one = serial();
    if std::env::var(CRASH_PATH_ENV).is_ok() {
        return;
    }
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("crash.redb");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "r2_crash_child", "--test-threads=1"])
        .env(CRASH_PATH_ENV, &path)
        .status()
        .expect("spawn the crash child");
    assert!(!status.success(), "the child must die by abort, got {status:?}");
    let db = Database::open(&path).expect("reopen after the process death");
    assert_exact(&db, "after the crash");
    assert_eq!(db.execute("SELECT id FROM items", &none()).unwrap().rows.len(), 4);
    // The emptied route does not survive the restart, so one slot is free again.
    sql_insert(&db, None, 29, "k8").expect("a free slot admits a new key");
    let refusal = sql_insert(&db, None, 30, "k9").unwrap_err();
    assert!(is_cap_refusal(&refusal, 4), "{refusal:?}");
    assert_exact(&db, "refusal after the crash");
}
