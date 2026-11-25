//! Sealing: vectors move from pending lists into blocks only when a maintenance cycle is
//! called, a bounded number in each cycle; a reader in another process searches while the
//! writer seals and gets the answer of one commit every time; and a small store of fixed
//! content stays within the size that was recorded for it.

mod support;
#[path = "support/vecmaint.rs"]
mod vecmaint;

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};

use contextdb::{Database, DatabaseOpenOptions, QueryResult, Value, WaitingOn, params};

use support::{Child, Run, count};

/// What one cycle reports: the vectors it sealed and the vectors still unindexed.
fn cycle(db: &Database) -> (u64, u64) {
    vecmaint::done_left(&support::cycle(db))
}

/// Options whose storage door counts each write and sync of the store in `writes`.
fn counting_writes(writes: &Arc<AtomicU64>) -> DatabaseOpenOptions {
    let (writes, mut options) = (writes.clone(), DatabaseOpenOptions::default());
    options.storage = Some(support::storage(move |call| {
        if matches!(call, support::Call::Write(..) | support::Call::Sync) {
            writes.fetch_add(1, Ordering::SeqCst);
        }
    }));
    options
}

/// Options whose clock reads the second held in `at`.
fn at_clock(at: &Arc<AtomicU64>) -> DatabaseOpenOptions {
    let mut options = DatabaseOpenOptions::default();
    options.clock = Some(Arc::new(support::At(at.clone())));
    options
}

#[test]
fn sealing_advances_only_when_asked_and_in_bounded_cycles() {
    const DIMENSION: usize = 8;
    let folder = support::folder("sealing-in-cycles");
    let db = support::open(&folder);
    // The column declares a size it never reaches, so no partition here is ever grouped.
    support::declare_docs(
        &db,
        DIMENSION,
        "PARTITION_KEY (ctx) INDEXING_THRESHOLD 1000000000",
    );
    // 4,196 vectors leave 100 pending after the bounded 4,096-vector cycle.
    let tx = db.transaction().expect("begin");
    support::insert_docs(&tx, 1..=4196, DIMENSION, |n| {
        String::from(if n <= 3000 { "a" } else { "b" })
    });
    tx.commit().expect("commit");
    assert_eq!(
        db.last_maintenance_cycle().unwrap(),
        None,
        "no cycle started by itself"
    );
    let db = support::reopen(db, &folder);
    assert_eq!(
        db.last_maintenance_cycle().unwrap(),
        None,
        "nor does a reopen"
    );
    let queries = [
        support::vector(94_001, DIMENSION),
        support::vector(94_002, DIMENSION),
    ];
    // Ten rows of everything, of a and of b: each equals the reference for both vectors.
    // Returns the rows the three searches read for the first vector.
    let examined = |on: &dyn Run, state: &str| -> [u64; 3] {
        let stored = support::stored(&on, support::DOCS);
        let scopes = [
            (None, None),
            (Some("ctx = 'a'"), Some("a")),
            (Some("ctx = 'b'"), Some("b")),
        ];
        scopes.map(|(filter, ctx): (Option<&str>, Option<&str>)| {
            let passes =
                |row: &[contextdb::Value]| ctx.is_none_or(|ctx| row[1] == support::text(ctx));
            let sql = support::nearest_sql("docs", "v", filter, 10);
            let mut read = Vec::new();
            for query in &queries {
                let wanted = stored.nearest(passes, query, 10);
                assert_eq!(wanted.len(), 10, "{state}");
                let result = support::assert_nearest(&on, &sql, query, &wanted, state);
                read.push(result.rows_examined());
            }
            read[0]
        })
    };

    // Before any cycle every vector is pending; a reopen and a reader read the same rows.
    let pending = examined(&db, "nothing sealed");
    // Nothing seals unless a cycle is called: not a search, not a close, not an open.
    support::close(db);
    let db = support::open(&folder);
    assert_eq!(examined(&db, "nothing sealed, after reopen"), pending);
    let reader = support::open_reader(&folder, None);
    assert_eq!(examined(&reader, "nothing sealed, a reader"), pending);

    // The first cycle seals 4,096 vectors in one commit: the first in the order of partition
    // and row key, so all of a and the first 1,096 of b. 100 of b stay pending.
    let unsealed = support::watermark(&db);
    let first = db.run_maintenance_cycle().unwrap();
    assert_eq!(
        first.work_remaining,
        Some(true),
        "100 vectors wait, so work remains"
    );
    assert_eq!(vecmaint::done_left(&first.vector), (4096, 100));
    let after_first = support::watermark(&db);
    assert!(after_first > unsealed, "a cycle that sealed made no commit");
    let partly = examined(&db, "after the first cycle");
    assert_eq!(examined(&reader, "after the first cycle, a reader"), partly);

    // The second seals the rest; later physical maintenance leaves the logical position alone.
    support::run(&db, "CREATE TABLE side (id INTEGER PRIMARY KEY)"); // a write between two cycles
    assert_eq!(cycle(&db), (100, 0));
    let after_second = support::watermark(&db);
    assert!(
        after_second > after_first,
        "a cycle that sealed made no commit"
    );
    assert_eq!(cycle(&db), (0, 0));
    assert_eq!(
        support::watermark(&db),
        after_second,
        "a cycle with nothing to seal leaves the logical position alone"
    );
    // A copy nobody else has open waits on no reader.
    let copy = support::folder("sealing-copy").join("snapshot");
    db.export_snapshot(&copy).expect("export");
    let waiting = support::open(&copy)
        .run_maintenance_cycle()
        .unwrap()
        .compaction_waiting_on;
    assert_eq!(waiting, None, "a copy has no reader to wait on");
    let sealed = examined(&db, "after the last cycle");
    for (read, pending) in sealed.into_iter().zip([4196, 3000, 1196]) {
        assert!(
            (10..pending).contains(&read),
            "a sealed search read {read} rows"
        );
    }
    assert_eq!(examined(&reader, "after the last cycle, a reader"), sealed);
    // Pending vectors are screened as a block's slots (F1): a scope reads no more rows while
    // some wait than once all are sealed, plus the ten asked for (as vectors_memory.rs).
    for ((pending, partly), sealed) in pending.into_iter().zip(partly).zip(sealed) {
        let fewer = pending.max(partly) <= sealed + 10;
        assert!(
            fewer,
            "rows read pending {pending}, partly {partly}, sealed {sealed}"
        );
    }

    // A cycle reports exactly the vectors that came to wait since the one before.
    let position = |db: &Database| support::watermark(db);
    // Ten new rows.
    support::insert_docs(&db, 4197..=4206, DIMENSION, |_| "a".to_owned());
    assert_eq!(cycle(&db), (10, 0));
    // A sealed row whose vector changes, twice, waits once.
    for fresh in [94_101, 94_102] {
        db.execute(
            "UPDATE docs SET v = $v WHERE id = 1",
            &params! { "v" => support::vector(fresh, DIMENSION) },
        )
        .expect("change a vector");
    }
    assert_eq!(cycle(&db), (1, 0));
    // Five new rows, of which two are deleted and one loses its vector before the cycle.
    support::insert_docs(&db, 4207..=4211, DIMENSION, |_| "b".to_owned());
    support::run(&db, "DELETE FROM docs WHERE id IN (4207, 4208)");
    support::run(&db, "UPDATE docs SET v = NULL WHERE id = 4209");
    assert_eq!(cycle(&db), (2, 0));
    // A row with no vector, and a sealed row that is deleted, leave nothing to seal.
    support::insert_doc(&db, 4212, "a", None).expect("a row with no vector");
    support::run(&db, "DELETE FROM docs WHERE id = 2");
    let before = position(&db);
    assert_eq!(cycle(&db), (0, 0));
    // Deleted vector pages may compact without changing the logical position.
    assert_eq!(position(&db), before);
    // A sealed row that moves to the other partition waits there.
    support::run(&db, "UPDATE docs SET ctx = 'b' WHERE id = 3");
    assert_eq!(cycle(&db), (1, 0));
    examined(&db, "at the end");

    // An empty store seals nothing; its internal metadata can still compact.
    let folder = support::folder("sealing-nothing");
    let db = support::open(&folder);
    let before = position(&db);
    let report = db
        .run_maintenance_cycle()
        .expect("a cycle of an empty store");
    let vector = report.vector;
    assert_eq!(
        [
            report.delivered,
            report.expired_rows,
            report.deferred,
            vector.maintenance_vectors_done,
            vector.maintenance_vectors_remaining,
            vector.remaining_partitions,
            vector.stalled_partitions,
        ]
        .map(count),
        [0; 7]
    );
    assert_eq!(position(&db), before);
    support::declare(&db);
    support::insert(&db, 1..=10);
    let before = position(&db);
    assert_eq!(cycle(&db), (0, 0));
    // This table has no vectors; physical maintenance leaves the logical position alone.
    assert_eq!(position(&db), before);
    support::assert_rows_are(&db, 1..=10);

    // A commit that leaves a partition at its limit of unindexed vectors, one block of the
    // column by default, seals that partition's whole blocks itself, with no cycle called. A
    // block takes 63 vectors of 4,096 numbers under a UUID key, which is longer than an
    // integer's, and at most 63 under any key of a byte or more (`BLOCK_BYTES` over a slot of
    // 4,104 bytes and a key end of 4). So no one-row commit leaves 64 unindexed, the one that
    // seals has 63 unindexed and seals one block, and 64 such commits seal once.
    const WIDE: usize = 4096;
    let folder = support::folder("sealing-in-the-commit");
    let db = support::open(&folder);
    support::declare_docs(&db, WIDE, "");
    let (mut left, mut seals) = (0, 0);
    for n in 1..=64_i64 {
        support::insert_doc(&db, n, "a", Some(support::vector(n, WIDE))).expect("a one-row commit");
        let now = support::whole(&support::the_partition(&db), "pending_tuples");
        assert!(
            now < 64,
            "the one-row commit of row {n} left {now} vectors unindexed"
        );
        if now <= left {
            seals += 1;
            assert!(
                left + 1 >= 63 && left + 1 - now <= 63,
                "row {n}'s commit sealed {} of {} unindexed: one block, and only at the limit",
                left + 1 - now,
                left + 1
            );
        }
        left = now;
    }
    assert_eq!(seals, 1, "64 one-row commits completed one block");

    // A declared count seals its reaching commit. Once cycles finish their work,
    // a further idle cycle writes and syncs nothing.
    support::maintenance(&db);
    support::run(&db, "SET UNINDEXED_VECTOR_LIMIT 2");
    for (id, pending) in [(65, 1), (66, 0)] {
        support::insert_doc(&db, id, "a", Some(support::vector(id, WIDE))).unwrap();
        assert_eq!(
            support::whole(&support::the_partition(&db), "pending_tuples"),
            pending
        );
    }
    support::close(db);
    let writes = Arc::new(AtomicU64::new(0));
    let db = support::open_with(&folder, counting_writes(&writes))
        .expect("reopen through the counting door");
    let logical = support::watermark(&db);
    // Two cycles relocate metadata then retire and trim its pages; the third is idle.
    for step in 0..2 {
        let before = support::data_file(&folder);
        let report = db.run_maintenance_cycle().unwrap();
        assert_eq!(vecmaint::done_left(&report.vector), (0, 0));
        assert!(
            support::data_file(&folder) <= before,
            "drain cycle {step} grew the file"
        );
        assert_eq!(support::watermark(&db), logical);
    }
    let idle = support::data_file(&folder);
    let opened = writes.load(Ordering::SeqCst);
    assert_eq!(cycle(&db), (0, 0));
    assert_eq!(support::watermark(&db), logical);
    assert_eq!(support::data_file(&folder), idle);
    assert_eq!(
        writes.load(Ordering::SeqCst) - opened,
        0,
        "writes or syncs by a cycle of a store with nothing left to do"
    );
}

/// The worker's test opens its stores with the engine's worker, the default, here and not
/// through `support::open_with`, which gives every other test the caller's cycles. It waits on
/// the worker's own commits, never on time: the door tells it of each sync made on a thread
/// other than the test's. Under a clock that never moves no poll interval passes, so a commit
/// is all that can wake the worker.
fn open_with_worker(
    folder: &Path,
    poll: bool,
    first: Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>,
) -> (Database, mpsc::Receiver<()>) {
    struct Tick(
        AtomicU64,
        u64,
        std::thread::ThreadId,
        Mutex<Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>>,
    );
    impl contextdb::Clock for Tick {
        fn now(&self) -> std::time::SystemTime {
            let at = self.0.fetch_add(self.1, Ordering::SeqCst);
            if std::thread::current().id() != self.2
                && let Some((checked, allowed)) = self.3.lock().unwrap().take()
            {
                checked.send(()).unwrap();
                allowed.recv().unwrap();
            }
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(at)
        }
    }
    let test_thread = std::thread::current().id();
    let (told, syncs) = mpsc::channel();
    let mut options = DatabaseOpenOptions::default();
    options.clock = Some(Arc::new(Tick(
        AtomicU64::new(1_700_000_000),
        if poll { 60 } else { 0 },
        test_thread,
        Mutex::new(first),
    )));
    options.storage = Some(support::storage(move |call| {
        if matches!(call, support::Call::Sync) && std::thread::current().id() != test_thread {
            let _ = told.send(());
        }
    }));
    let db = Database::open_with_options(folder, options).expect("open with the worker");
    (db, syncs)
}

/// Waits for the worker's syncs until `rested` holds. A sync is seen before it reaches the
/// file, so a write of the test's own, which waits behind the commit the worker is in, comes
/// before each look.
fn until_rested(db: &Database, syncs: &mpsc::Receiver<()>, rested: impl Fn(&Database) -> bool) {
    for mark in 1_i64.. {
        syncs
            .recv()
            .expect("the door is let go only when the store is");
        support::run(db, &format!("INSERT INTO marks (id) VALUES ({mark})"));
        if rested(db) {
            return;
        }
    }
}

#[test]
fn the_worker_runs_cycles_until_nothing_is_left() {
    let grouped = |db: &Database| {
        let partition = support::shown(db, "SHOW VECTOR_PARTITIONS FOR docs.v").remove(0);
        lists_of(&partition).is_some() && state_of(&partition).0 == "idle"
    };
    let (db, syncs) = open_with_worker(&support::folder("the-worker"), false, None);
    support::run(&db, "CREATE TABLE marks (id INTEGER PRIMARY KEY)");
    // 336 wide vectors fill one block and leave fewer than 20% pending. Their commit makes
    // the first IVF due and wakes the worker; the test calls no cycle.
    support::declare_docs(&db, 768, "INDEXING_THRESHOLD 300");
    support::load_docs(&db, 1..=336, "a", |n| support::gathered(n, 768));
    until_rested(&db, &syncs, grouped);
    // What the worker leaves is under one block; a caller seals it.
    let report = support::cycle(&db);
    assert_eq!(
        (
            count(report.maintenance_vectors_remaining),
            count(report.remaining_partitions)
        ),
        (0, 0),
        "a caller's cycle after the worker came to rest"
    );
    // `close` stops the worker and joins it, so once it returns nothing holds the store, and
    // the door with the worker's end of the channel is let go.
    support::close(db);
    loop {
        match syncs.try_recv() {
            Ok(()) => {}
            Err(mpsc::TryRecvError::Disconnected) => break,
            Err(mpsc::TryRecvError::Empty) => panic!("a thread held the store after close"),
        }
    }

    // Partition a holds 99 sealed vectors; one pending row of b brings the column to its
    // INDEXING_THRESHOLD of 100. At the first 60-second clock gap, a persisted 120-second
    // poll leaves b pending. Closing waits behind that clock check and joins any due cycle;
    // after a 30-second declaration, the worker groups a and seals b without a caller cycle.
    let folder = support::folder("the-worker-woken");
    let db = support::open(&folder);
    support::run(&db, "CREATE TABLE marks (id INTEGER PRIMARY KEY)");
    support::declare_docs(&db, 16, "PARTITION_KEY (ctx) INDEXING_THRESHOLD 100");
    support::load_docs(&db, 1..=99, "a", gathered);
    support::maintenance(&db);
    assert!(!grouped(&db), "a column below its threshold groups nothing");
    support::load_docs(&db, 100..=100, "b", gathered);
    support::run(&db, "SET MAINTENANCE_POLL_INTERVAL 120 SECONDS");
    support::close(db);
    let (checked, entered) = mpsc::channel();
    let (release, allowed) = mpsc::channel();
    let (db, _) = open_with_worker(&folder, true, Some((checked, allowed)));
    entered.recv().unwrap();
    release.send(()).unwrap();
    support::close(db);
    let db = support::open(&folder);
    let pending = support::shown(&db, "SHOW VECTOR_PARTITIONS FOR docs.v").remove(1);
    assert_eq!(support::whole(&pending, "pending_tuples"), 1);
    support::run(&db, "SET MAINTENANCE_POLL_INTERVAL 30 SECONDS");
    support::close(db);
    let (db, syncs) = open_with_worker(&folder, true, None);
    until_rested(&db, &syncs, |db| {
        grouped(db)
            && support::shown(db, "SHOW VECTOR_PARTITIONS FOR docs.v")
                .iter()
                .all(|p| support::whole(p, "pending_tuples") == 0)
    });
    support::close(db);

    let folder = support::folder("worker-expiry-continuation");
    let frozen = Arc::new(AtomicU64::new(1_699_999_999));
    let db = support::open_with(&folder, at_clock(&frozen)).unwrap();
    for sql in [
        "CREATE TABLE marks (id INT PRIMARY KEY)",
        "CREATE TABLE a_parent (id INT PRIMARY KEY) RETAIN 1 SECONDS",
        "CREATE TABLE b_child (id INT PRIMARY KEY, p INT REFERENCES a_parent(id), expires TIMESTAMP EXPIRES) RETAIN 1 SECONDS",
        "INSERT INTO a_parent VALUES (1), (2)",
        "INSERT INTO b_child VALUES (1, 1, NULL)",
    ] {
        support::run(&db, sql);
    }
    db.execute(
        "INSERT INTO b_child VALUES (2, 2, $pin)",
        &params! {"pin"=>Value::Timestamp(i64::MAX)},
    )
    .unwrap();
    support::declare_docs(&db, 16, "INDEXING_THRESHOLD 1");
    support::close(db);
    let (db, syncs) = open_with_worker(&folder, false, None);
    // Crossing wakes one vector cycle; its only pending vector cannot be built without sealing.
    // The frozen clock permits no timer cycle, so only expiry continuation removes parent 1.
    support::load_docs(&db, 1..=1, "a", gathered);
    until_rested(&db, &syncs, |db| {
        support::rows(db, "SELECT id FROM a_parent") == [[Value::Int64(2)]]
    });
    assert_eq!(
        support::rows(&db, "SELECT id FROM b_child"),
        [[Value::Int64(2)]]
    );
    db.verify().unwrap();
    support::close(db);

    let folder = support::folder("worker-downward-crossing");
    let db = support::open(&folder);
    support::run(&db, "SET STORAGE_COMPACTION MIN_INTERVAL 0 SECONDS");
    support::run(&db, "CREATE TABLE marks (id INT PRIMARY KEY)");
    support::declare_docs(&db, 16, "PARTITION_KEY(ctx) INDEXING_THRESHOLD 2000");
    support::load_docs(&db, 1..=10, "a", gathered);
    support::maintenance(&db);
    support::run(&db, "DELETE FROM docs WHERE id <= 2");
    support::load_docs(&db, 1001..=3000, "b", gathered);
    support::maintenance(&db);
    support::close(db);
    let (db, syncs) = open_with_worker(&folder, true, None);
    support::run(&db, "DELETE FROM docs WHERE id BETWEEN 1001 AND 1010");
    until_rested(&db, &syncs, |db| {
        support::shown(db, "SHOW VECTOR_PARTITIONS FOR docs.v")
            .iter()
            .find(|p| support::whole(p, "live_rows") == 8)
            .is_some_and(|p| state_of(p).0 == "idle" && support::whole(p, "tombstones") == 0)
    });
    let before = support::data_file(&folder);
    support::run(&db, "DELETE FROM docs WHERE id > 1010");
    support::run(&db, "DELETE FROM marks");
    until_rested(&db, &syncs, |db| {
        support::data_file(&folder) < before && db.last_maintenance_cycle().unwrap().is_some()
    });
    db.verify().unwrap();
    support::close(db);

    // A no-commit cycle must wait for another poll, even above a declared zero stop ratio.
    type ClockGate = (u64, mpsc::Sender<()>, mpsc::Receiver<()>);
    struct PollClock {
        calls: Arc<AtomicU64>,
        main: std::thread::ThreadId,
        worker_only: bool,
        round: u64,
        // Whole windows the fixture has moved on, for both threads.
        shift: Arc<AtomicU64>,
        gate: Mutex<Vec<ClockGate>>,
    }
    impl contextdb::Clock for PollClock {
        fn now(&self) -> std::time::SystemTime {
            let worker = std::thread::current().id() != self.main;
            if worker || !self.worker_only {
                let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
                let mut gate = self.gate.lock().unwrap();
                if gate.first().is_some_and(|(at, _, _)| *at == call) {
                    let (_, checked, allowed) = gate.remove(0);
                    checked.send(()).unwrap();
                    allowed.recv().unwrap();
                }
            }
            std::time::UNIX_EPOCH
                + std::time::Duration::from_secs(
                    1_700_000_000
                        + self.shift.load(Ordering::SeqCst)
                        + 60 * (self.round + u64::from(worker)),
                )
        }
    }
    let folder = support::folder("worker-no-progress-poll");
    let calls = Arc::new(AtomicU64::new(0));
    let io = Arc::new(AtomicU64::new(0));
    let main = std::thread::current().id();
    let shift = Arc::new(AtomicU64::new(0));
    let mut options = counting_writes(&io);
    options.clock = Some(Arc::new(PollClock {
        calls: calls.clone(),
        main,
        worker_only: false,
        round: 0,
        shift: shift.clone(),
        gate: Mutex::new(Vec::new()),
    }));
    let db = support::open_with(&folder, options).unwrap();
    for sql in [
        "CREATE TABLE quiet (id INT PRIMARY KEY)",
        "INSERT INTO quiet VALUES (1)",
        "SET STORAGE_COMPACTION STOP_RATIO 0",
        "SET STORAGE_COMPACTION START_RATIO 0.000001",
        "SET MAINTENANCE_POLL_INTERVAL 60 SECONDS",
    ] {
        support::run(&db, sql);
    }
    // Each drain cycle runs one MIN_INTERVAL (60 seconds by default) after the last, so every
    // one is at a window's end and a pass may start. A window whose commits used its free pages
    // has no surplus at its end, so one quiet cycle proves nothing; a window with no commit
    // starts from nothing, and when the cycle at its end also moves nothing the fixture is
    // drained (design 4, S4).
    let mut idle = false;
    let mut quiet = 0;
    for _ in 0..32 {
        shift.fetch_add(60, Ordering::SeqCst);
        let before = io.load(Ordering::SeqCst);
        let length = support::data_file(&folder);
        let report = db.run_maintenance_cycle().unwrap();
        assert!(support::data_file(&folder) <= length);
        if io.load(Ordering::SeqCst) != before {
            quiet = 0;
            continue;
        }
        assert_eq!(report.compacted_pages, Some(0));
        quiet += 1;
        if quiet == 2 {
            idle = true;
            break;
        }
    }
    assert!(
        idle,
        "the no-progress fixture drains within thirty-two cycles"
    );
    // The cycle the worker's is bounded by is a drained one at the next window's end, as the
    // worker's first cycle of each round is.
    shift.fetch_add(60, Ordering::SeqCst);
    calls.store(0, Ordering::SeqCst);
    let before = io.load(Ordering::SeqCst);
    assert_eq!(db.run_maintenance_cycle().unwrap().compacted_pages, Some(0));
    assert_eq!(io.load(Ordering::SeqCst), before);
    let cycle_calls = calls.load(Ordering::SeqCst);
    let position = support::watermark(&db);
    support::close(db);
    for round in 0..2 {
        let (checked, entered) = mpsc::channel();
        let (release, allowed) = mpsc::channel();
        let (opened, at_open) = mpsc::channel();
        let (proceed, drained) = mpsc::channel();
        calls.store(0, Ordering::SeqCst);
        io.store(0, Ordering::SeqCst);
        let mut options = DatabaseOpenOptions::default();
        options.clock = Some(Arc::new(PollClock {
            calls: calls.clone(),
            main,
            worker_only: true,
            round,
            shift: shift.clone(),
            // Drain before the first poll; hold the worker at its next clock read.
            gate: Mutex::new(vec![(1, opened, drained), (2, checked, allowed)]),
        }));
        options.storage = Some(support::storage({
            let io = io.clone();
            move |call| {
                if std::thread::current().id() != main
                    && matches!(call, support::Call::Write(..) | support::Call::Sync)
                {
                    io.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
        let db = Database::open_with_options(&folder, options).unwrap();
        at_open.recv().unwrap();
        support::maintenance(&db);
        proceed.send(()).unwrap();
        entered.recv().unwrap();
        assert_eq!(support::watermark(&db), position);
        release.send(()).unwrap();
        support::close(db);
        assert!(
            calls.load(Ordering::SeqCst) <= cycle_calls + 3,
            "no second cycle before the next poll interval"
        );
        assert_eq!(
            io.load(Ordering::SeqCst),
            0,
            "no-progress worker writes nothing"
        );
        let reader = support::open_reader(&folder, None);
        assert_eq!(support::int(&reader, "SELECT COUNT(*) FROM quiet"), 1);
    }
}

/// The dimension, the vector and the statement of the searches in the next test.
const SEARCHED: usize = 16;

fn searched_for() -> Vec<f32> {
    support::vector(95_000, SEARCHED)
}

fn search_of_ten() -> String {
    support::nearest_sql("docs", "v", None, 10)
}

/// What one commit shows a reader as one line: the vectors still waiting to be sealed as
/// `SHOW VECTOR_INDEXES` gives them, the rows a search read, then each key with the bits of its
/// distance. Two readings of one commit give the same line.
fn told(progress: &support::Shown, result: &QueryResult) -> String {
    let pending = support::whole(progress, "pending_tuples");
    let rows: Vec<String> = support::found(result, "the search of ten")
        .iter()
        .map(|near| format!("{:?}:{:016x}", near.key, near.distance.to_bits()))
        .collect();
    let read = result.rows_examined();
    format!("{pending} | {read} | {}", rows.join(","))
}

/// The seal progress of `docs.v` that a door reads now.
fn seal_progress(on: &impl Run) -> support::Shown {
    let mut shown = support::shown(on, "SHOW VECTOR_INDEXES");
    assert_eq!(shown.len(), 1, "docs.v is the store's one vector column");
    shown.remove(0)
}

/// The vectors a line says are waiting to be sealed, and the rows of its search.
fn pending_of(line: &str) -> i64 {
    let pending = line.split(" | ").next().expect("a line");
    pending
        .parse()
        .expect("a line begins with its pending vectors")
}
fn rows_of(line: &str) -> &str {
    line.rsplit(" | ").next().expect("a line")
}

#[test]
fn a_reader_in_another_process_searches_while_the_writer_seals() {
    if let Some(role) = support::role() {
        return match role.name.as_str() {
            "looping" => looping(&role),
            "stopped" => stopped(&role),
            other => panic!("no role {other:?} in this test"),
        };
    }
    let test = "a_reader_in_another_process_searches_while_the_writer_seals";
    let folder = support::folder("reader-beside-sealing");
    let db = support::open(&folder);
    support::declare_docs(&db, SEARCHED, "PARTITION_KEY (ctx)");
    let insert = |numbers: std::ops::Range<i64>| {
        let tx = db.transaction().expect("begin");
        support::insert_docs(&tx, numbers, SEARCHED, |n| format!("c{}", n % 2));
        tx.commit().expect("commit");
    };
    for first in (1..6001).step_by(1000) {
        insert(first..first + 1000);
    }
    // The line of the newest commit: the search equals the reference there, and this is what
    // it reads and returns.
    let newest = || -> String {
        let stored = support::stored(&db, support::DOCS);
        let wanted = stored.nearest(|_| true, &searched_for(), 10);
        let sql = search_of_ten();
        let found = support::assert_nearest(&db, &sql, &searched_for(), &wanted, "the writer");
        told(&seal_progress(&db), &found)
    };
    let mut commits = vec![newest()];

    // One reader is stopped halfway through the storage reads of a search of the first commit,
    // where every vector is pending.
    let (lines, mut held) = support::stopped_halfway(test, "stopped", &folder);
    assert_eq!(lines.first(), commits.first(), "a search nothing stopped");

    // A second reader searches in a loop, and announces each answer that differs from its
    // last. Every line it announces must be the line of a commit, and it must come to the
    // newest one: it is never refused and never told to try again.
    let mut looping = Child::start(test, "looping", &folder);
    let mut follow = |commits: &[String]| {
        let newest = commits.last().expect("a commit");
        loop {
            let Some(line) = looping.line() else {
                panic!("the looping reader ended; it had still to reach\n{newest}")
            };
            assert!(
                commits.contains(&line),
                "the looping reader announced an answer that is no commit's:\n{line}"
            );
            if line == *newest {
                return;
            }
        }
    };
    follow(&commits);

    // The writer seals cycle by cycle and inserts and changes between. Each step is one
    // commit that changes what a reader is shown, so the reader can be seen to arrive at it:
    // a write changes the rows or the vectors waiting to be sealed, and a seal leaves fewer
    // vectors waiting and changes no row a search returns.
    let (seal, write) = (true, false);
    let steps: [(bool, &dyn Fn()); 7] = [
        (seal, &|| assert_eq!(cycle(&db), (4096, 1904))),
        (write, &|| insert(6001..6501)),
        (seal, &|| assert_eq!(cycle(&db), (2404, 0))),
        (write, &|| insert(6501..7001)),
        (seal, &|| assert_eq!(cycle(&db), (500, 0))),
        (write, &|| {
            let tx = db.transaction().expect("begin");
            for id in 1..=300_i64 {
                tx.execute(
                    "UPDATE docs SET v = $v WHERE id = $id",
                    &params! { "id" => id, "v" => support::vector(95_100 + id, SEARCHED) },
                )
                .expect("change a vector");
            }
            tx.commit().expect("commit");
        }),
        (seal, &|| assert_eq!(cycle(&db), (300, 0))),
    ];
    for (sealed, step) in steps {
        step();
        let line = newest();
        let was = commits.last().expect("a commit");
        assert_ne!(&line, was, "a step left what a reader is shown as it was");
        if sealed {
            assert_eq!(
                rows_of(&line),
                rows_of(was),
                "a seal moves vectors into blocks and changes no row a search returns"
            );
            assert!(
                pending_of(&line) < pending_of(was),
                "a seal that sealed vectors left as many waiting to be sealed:\n{was}\n{line}"
            );
        }
        commits.push(line);
        follow(&commits);
    }

    // The reader that was stopped inside its search through all of it answers with the rows
    // of the commit it began at, for the work that commit took; its next search is of the
    // newest commit.
    held.tell("go");
    assert_eq!(
        held.line().as_ref(),
        commits.first(),
        "the search that was stopped"
    );
    assert!(held.line().is_some_and(|line| line.starts_with("reads ")));
    assert_eq!(
        held.line().as_ref(),
        commits.last(),
        "the stopped reader's next search"
    );
    held.end();
    looping.kill();
}

/// Opens the store for reading and searches without end, announcing an answer's line each
/// time it differs from the one before. The parent kills it.
fn looping(role: &support::Role) {
    let (sql, query) = (search_of_ten(), searched_for());
    let mut last = String::new();
    loop {
        // Each round reads one commit: the newest when the round begins.
        let reader = support::open_reader(&role.folder, None)
            .into_one_moment()
            .expect("a reader is never refused while the writer seals");
        let sealing = seal_progress(&reader);
        let result = match support::search(&reader, &sql, &query) {
            Ok(result) => result,
            Err(error) => panic!("a reader was refused while the writer sealed: {error:?}"),
        };
        let line = told(&sealing, &result);
        if line != last {
            support::say(&line);
            last = line;
        }
    }
}

/// Opens the store for reading and searches once. At storage read number `pause_at` of that
/// search it announces `reading` and waits for a line; `pause_at` 0 never stops. Then it
/// announces the answer's line and `reads <n>`, the storage reads the search made. Then it
/// searches once more and announces that answer's line.
fn stopped(role: &support::Role) {
    let (reader, searching, reads) = support::reader_pausing(role);
    let (sql, query) = (search_of_ten(), searched_for());

    // The writer makes no commit until this reader is stopped inside its search, and none
    // after the parent lets it go, so each search and the progress read before it are of one
    // commit.
    let sealing = seal_progress(&reader);
    searching.store(true, Ordering::SeqCst);
    let result = support::search(&reader, &sql, &query).expect("a reader's search is not refused");
    searching.store(false, Ordering::SeqCst);
    support::say(told(&sealing, &result));
    support::say(format!("reads {}", reads.load(Ordering::SeqCst)));
    let sealing = seal_progress(&reader);
    let next = support::search(&reader, &sql, &query).expect("the reader's next search");
    support::say(told(&sealing, &next));
}

#[test]
fn a_small_fixed_store_stays_within_its_recorded_size() {
    // The bytes put in: 1,500 ids of 8 bytes, 1,500 vectors of 1,536 bytes and 4,800,000
    // bytes of text. The generated letters do not compress by an eighth, so their stored bytes
    // stay exact. And the most bytes the data file may have: bar 1, a store at most 1.5 times
    // its data once maintenance rests (design 8.1).
    const PUT_IN: u64 = 7_116_000;
    const RECORDED: u64 = PUT_IN * 3 / 2;

    let folder = support::folder("small-fixed-store");
    let writes = Arc::new(AtomicU64::new(0));
    let options = || counting_writes(&writes);
    let db = support::open_with(&folder, options()).expect("open through the counting door");
    support::run(
        &db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL, embedding VECTOR(384) INDEXING_THRESHOLD 1000000000)",
    );
    // The first batch leaves low pages free after its negative keys are removed.
    let mut put_in = 0_u64;
    for first in [-500_i64, 1, 501, 1001] {
        let tx = db.transaction().expect("begin");
        for id in first..first + 500 {
            // 200, 1,700, 3,200, 4,700 or 6,200 lower-case letters: one size that stays in
            // its row and four that do not.
            let body = support::letters(id, 200 + 1500 * id.rem_euclid(5) as usize);
            // Filler frees pages without leaving vector tombstones that trigger a rebuild.
            let embedding = if id > 0 {
                Value::Vector(support::vector(id, 384))
            } else {
                Value::Null
            };
            if id > 0 {
                put_in += 8 + body.len() as u64 + 4 * 384;
            }
            tx.execute(
                "INSERT INTO notes (id, body, embedding) VALUES ($id, $body, $embedding)",
                &params! { "id" => id, "body" => body, "embedding" => embedding },
            )
            .unwrap_or_else(|error| panic!("note {id} was refused: {error:?}"));
        }
        tx.commit().expect("commit five hundred notes");
    }
    assert_eq!(put_in, PUT_IN, "the bytes this test put in");
    // Every vector is in a block when the length is read: commits seal whole blocks, cycles
    // the rest.
    support::seal_all(&db);
    let partitions = support::shown(&db, "SHOW VECTOR_PARTITIONS FOR notes.embedding");
    let left = support::whole(&partitions[0], "pending_tuples");
    assert_eq!(left, 0, "vectors left unsealed");
    support::run(&db, "ALTER TABLE notes SET RETAIN 1 DAYS");
    support::run(
        &db,
        "ALTER TABLE notes ADD COLUMN expires TIMESTAMP EXPIRES",
    );
    db.execute(
        "UPDATE notes SET expires = $due WHERE id < 0",
        &params! { "due" => Value::Timestamp(-1) },
    )
    .unwrap();
    // The bar is the size at rest, not the interval: MIN_INTERVAL 0 keeps a window of one
    // commit, so passes start in this short session with no clock (design 4, S4).
    support::run(&db, "SET STORAGE_COMPACTION MIN_INTERVAL 0 SECONDS");
    let mut expired = 0;
    let done = (0..1000).any(|_| {
        let report = db.run_maintenance_cycle().unwrap();
        expired += count(report.expired_rows);
        report.work_remaining == Some(false) && report.owed == Some(false)
    });
    assert!(done && expired == 500, "{expired} of 500 rows expired");
    // Ratios declared through SQL survive reopen and steer both starting and an active walk.
    let configured = |db: Database, start: f64, stop: f64| {
        support::run(&db, "SET STORAGE_COMPACTION START_RATIO 1");
        support::run(&db, &format!("SET STORAGE_COMPACTION STOP_RATIO {stop}"));
        support::run(&db, &format!("SET STORAGE_COMPACTION START_RATIO {start}"));
        support::close(db);
        let db = support::open_with(&folder, options()).unwrap();
        assert_eq!(
            support::rows(&db, "SHOW STORAGE_COMPACTION"),
            [
                vec![
                    support::text("start_ratio"),
                    Value::Float64(start),
                    support::text("declared")
                ],
                vec![
                    support::text("stop_ratio"),
                    Value::Float64(stop),
                    support::text("declared")
                ],
                // The interval is shown beside the ratios, in whole milliseconds, and the
                // one declared above survives reopen with them (design 4, S4).
                vec![
                    support::text("min_interval"),
                    Value::Int64(0),
                    support::text("declared")
                ],
            ]
        );
        db
    };
    let stopped = |db: &Database| {
        for _ in 0..16 {
            let before = (support::data_file(&folder), support::watermark(db));
            let cycle = db.run_maintenance_cycle().unwrap();
            assert_eq!(cycle.compacted_pages, Some(0));
            assert_eq!(
                cycle.compaction_free_ratio, None,
                "a cycle that did not start measured nothing"
            );
            assert!(support::data_file(&folder) <= before.0);
            assert_eq!(support::watermark(db), before.1);
        }
    };
    let db = configured(db, 1.0, 0.99);
    stopped(&db);
    // Bar 1: the fixed load, with maintenance at rest, is within its recorded size.
    let rested = support::data_file(&folder);
    assert!(
        rested <= RECORDED,
        "the data file is {rested} bytes long; the most recorded is {RECORDED} for {PUT_IN} put in"
    );
    // A store that loses half its rows gives the space back with no call from the user: the
    // cycles alone bring the file within the recorded bar for what is left (design 4, S4).
    let db = configured(db, 0.05, 0.02);
    support::run(&db, "DELETE FROM notes WHERE id > 750");
    let kept: u64 = (1..=750_u64)
        .map(|id| 8 + 4 * 384 + 200 + 1500 * (id % 5))
        .sum();
    let mut idle = false;
    for _ in 0..support::data_file(&folder) / 4096 {
        let before = writes.load(Ordering::SeqCst);
        db.run_maintenance_cycle().unwrap();
        if writes.load(Ordering::SeqCst) == before {
            idle = true;
            break;
        }
    }
    assert!(
        idle,
        "an idle cycle writes and syncs nothing, and the cycles reach one"
    );
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM notes"), 750);
    assert_eq!(support::int(&db, "SELECT SUM(id) FROM notes"), 281_625);
    db.verify().unwrap();
    // Measure after maintenance, before clean close can shorten the file.
    let length = support::data_file(&folder);
    assert!(
        length <= 2 * kept,
        "the data file is {length} bytes long; the most recorded for a store is twice what it \
         holds, and {kept} bytes are left of the {PUT_IN} put in"
    );
    support::close(db);
}

#[test]
fn the_same_rows_in_more_commits_end_at_the_same_size() {
    // The notes of the test above, 24 times over (about 200 MB), written in 1, 2, 4 and 8
    // commits, the quarter of negative id deleted, then maintained until a cycle has nothing to
    // do and compacted to rest: the free pages left are below the start share, so no pass of
    // its own begins. Disk use follows content, not how it was committed: the data files end
    // within one compaction step, 256 pages of 4 KiB, of each other. At this size the same
    // stores left uncompacted differ by several steps. Runs in the release profile only.
    const SCALE: i64 = 24;
    const STEP: u64 = 256 * 4096;
    let sizes = [1_i64, 2, 4, 8].map(|commits| {
        let folder = support::folder(&format!("same-rows-in-{commits}-commits"));
        let db = support::open(&folder);
        support::run(
            &db,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL, embedding VECTOR(384) INDEXING_THRESHOLD 1000000000)",
        );
        let each = 2000 * SCALE / commits;
        for first in (0..commits).map(|commit| -500 * SCALE + commit * each) {
            let tx = db.transaction().expect("begin");
            for id in first..first + each {
                let body = support::letters(id, 200 + 1500 * id.rem_euclid(5) as usize);
                let embedding = (id > 0).then(|| Value::Vector(support::vector(id, 384)));
                let p = params! { "id" => id, "body" => body, "embedding" => embedding.unwrap_or(Value::Null) };
                tx.execute("INSERT INTO notes (id, body, embedding) VALUES ($id, $body, $embedding)", &p)
                    .unwrap_or_else(|error| panic!("note {id} was refused: {error:?}"));
            }
            tx.commit().expect("commit the notes");
        }
        support::run(&db, "DELETE FROM notes WHERE id < 0");
        support::maintenance(&db);
        while db.compact_now().unwrap().work_remaining != Some(false) {}
        assert_eq!(support::int(&db, "SELECT COUNT(*) FROM notes"), 1500 * SCALE);
        // Measured after maintenance, before a clean close can shorten the file.
        support::data_file(&folder)
    });
    let (least, most) = (sizes.iter().min().unwrap(), sizes.iter().max().unwrap());
    assert!(
        most - least <= STEP,
        "the same rows in 1, 2, 4 and 8 commits came to {sizes:?} bytes, more than one step apart"
    );
}

// ---- IVFs ----

/// The dimension of the tests of an IVF, and the clause under which a partition of 2,000
/// rows is cut into groups.
const GROUPED: usize = 16;
const AT_1000: &str = "INDEXING_THRESHOLD 1000";

fn gathered(n: i64) -> Vec<f32> {
    support::gathered(n, GROUPED)
}

/// A partition's `maintenance_state` and `maintenance_reason`.
fn state_of(partition: &support::Shown) -> (String, String) {
    (
        support::word(partition, "maintenance_state"),
        support::word(partition, "maintenance_reason"),
    )
}

/// A partition's `lists`: the groups of the IVF in force, `None` with none.
fn lists_of(partition: &support::Shown) -> Option<i64> {
    match support::field(partition, "lists") {
        Value::Null => None,
        _ => Some(support::whole(partition, "lists")),
    }
}

/// The state and reason, and the groups, of the one partition of `docs`.
fn state(db: &Database) -> (String, String) {
    state_of(&support::the_partition(db))
}
fn lists(db: &Database) -> Option<i64> {
    lists_of(&support::the_partition(db))
}

/// Asserts that a search for the ten rows nearest `query` under EXACT, AUTO and INDEXED each
/// returns the reference.
fn every_mode_answers(db: &Database, query: &[f32], state: &str) {
    let wanted = support::stored(db, support::DOCS).nearest(|_| true, query, 10);
    for mode in ["EXACT", "AUTO", "INDEXED"] {
        let sql = support::by_mode(None, mode, 10);
        let state = format!("{mode} {state}");
        support::assert_nearest(db, &sql, query, &wanted, &state);
    }
}

/// Asserts that a search under INDEXED for the vector of each of `ids` answers soundly with
/// that row first.
fn each_found_first(db: &Database, ids: impl Iterator<Item = i64>, vector: fn(i64) -> Vec<f32>) {
    let stored = support::stored(db, support::DOCS);
    let sql = support::by_mode(None, "INDEXED", 1);
    for id in ids {
        let query = vector(id);
        let result = support::answer(db, &sql, &query);
        let state = format!("row {id}, searched for under INDEXED by its own vector");
        let answer = support::assert_sound(&result, &stored, |_| true, &query, 1, &state);
        assert_eq!(answer[0].key, Value::Int64(id), "{state}");
    }
}

/// What a build reports of itself in a partition's row: the slots it reads, has packed and
/// has left.
fn progress(partition: &support::Shown) -> [i64; 3] {
    ["total", "done", "remaining"]
        .map(|part| support::whole(partition, &format!("maintenance_vectors_{part}")))
}

#[test]
fn a_ivf_is_built_in_bounded_cycles_and_switched_in_one() {
    let folder = support::folder("ivf-in-cycles");
    let second = Arc::new(AtomicU64::new(100));
    let db = support::open_with(&folder, at_clock(&second)).unwrap();
    support::run(
        &db,
        "CREATE TABLE expired (id INTEGER PRIMARY KEY, due TIMESTAMP EXPIRES) RETAIN 1 SECONDS",
    );
    // 2,000 rows are this column's size, and half of the 4,000 vectors a partition of it may
    // have outside an IVF and still be searched INDEXED: every search below is served.
    support::declare_docs(&db, GROUPED, "INDEXING_THRESHOLD 2000");
    support::load_docs(&db, 1..=2000, "a", gathered);
    for sql in [
        "CREATE TABLE edges (id INT PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        "INSERT INTO edges VALUES (1, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'D')",
    ] {
        support::run(&db, sql);
    }
    let walk = "SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[e]->(b) COLUMNS (b.id AS b_id))";
    let query = gathered(60_000);
    let late = support::vector(60_001, GROUPED);
    let late_sql = support::by_mode(None, "INDEXED", 1);
    let found_late = |state: &str| {
        let stored = support::stored(&db, support::DOCS);
        let result = support::answer(&db, &late_sql, &late);
        let answer = support::assert_sound(&result, &stored, |_| true, &late, 1, state);
        assert_eq!(answer[0].key, Value::Int64(2001), "{state}");
    };

    let first = support::watermark(&db);
    every_mode_answers(&db, &query, "before any cycle");
    assert_eq!(support::watermark(&db), first);
    for number in 1..=18_u64 {
        if number == 6 {
            // A write between two cycles commits and is found.
            support::insert_doc(&db, 2001, "a", Some(late.clone())).expect("a write in a build");
            found_late("written between two cycles");
        }
        db.execute(
            "INSERT INTO expired VALUES ($id, $due)",
            &params! { "id" => number as i64, "due" => Value::Timestamp((100 + number as i64) * 1000) },
        )
        .unwrap();
        second.store(100 + number, Ordering::SeqCst);
        let before = support::watermark(&db).0;
        let cycle = db.run_maintenance_cycle().unwrap();
        assert_eq!(
            cycle.expired_rows,
            Some(1),
            "expiry beside build cycle {number}"
        );
        assert_eq!(support::int(&db, "SELECT COUNT(*) FROM expired"), 0);
        assert_eq!(support::rows(&db, walk).len(), 1, "walk in cycle {number}");
        let report = cycle.vector;
        let commits = support::watermark(&db).0 - before - 1;
        // The vectors the cycle reports done and left, the partitions left, and its commits:
        // a cycle advances builds before it seals, so the first seals, the second begins, ten
        // find centroids, one gives every slot its group, one packs, two measure, one switches,
        // and the eighteenth has nothing to do. The sixth also seals the row written before it.
        // Physical compaction can advance on any cycle; it never adds a logical commit to the
        // build and expiry counts below.
        let wanted = match number {
            1 => (2000, 2000, 1, 1),
            6 => (1, 2000, 1, 2),
            14 => (2000, 0, 1, 1),
            15 | 16 => (0, 0, 1, 1),
            17 => (0, 0, 0, 1),
            18 => (0, 0, 0, 0),
            _ => (0, 2000, 1, 1),
        };
        assert_eq!(
            (
                count(report.maintenance_vectors_done),
                count(report.maintenance_vectors_remaining),
                count(report.remaining_partitions),
                commits
            ),
            wanted,
            "cycle {number}"
        );
        let partition = support::the_partition(&db);
        let probes = support::field(&partition, "probes").clone();
        if number < 17 {
            let state = if number == 1 { "pending" } else { "building" };
            assert_eq!(state_of(&partition).0, state, "after cycle {number}");
            assert_eq!((lists_of(&partition), probes), (None, Value::Null));
            let packed = if number < 14 { 0 } else { 2000 };
            assert_eq!(progress(&partition), [2000, packed, 2000 - packed]);
            // Until the switch every mode returns what EXACT returns.
            every_mode_answers(&db, &query, &format!("after cycle {number}"));
        } else {
            assert_eq!(state_of(&partition).0, "idle", "after cycle {number}");
            assert_eq!(lists_of(&partition), Some(45), "round(sqrt(2000))");
            // Recorded from the first run, within the 45 groups; 16 since the reach's distinct
            // middles (7364fc08b) changed the measured width.
            assert_eq!(probes, Value::Int64(16), "probes after the switch");
            assert_eq!(progress(&partition), [0, 0, 0]);
        }
    }
    found_late("after the switch");
}

/// The bytes inspection reports for the index of the one partition.
fn index_bytes(db: &Database) -> i64 {
    support::whole(&support::the_partition(db), "durable_index_bytes")
}

#[test]
fn a_ivf_keeps_a_code_of_every_slot_and_measures_its_width() {
    let (_, db) = support::built("ivf-index-bytes", GROUPED, AT_1000, 2000);
    let partition = support::the_partition(&db);
    assert_eq!(lists_of(&partition), Some(45));
    // 2,000 slots of 24 bytes with keys of 9 are one block. The index is 45 centroids of 16
    // numbers, the record of 45 groups, the block's 2,000 binary codes of 4 bytes behind their
    // count, and the mean.
    assert_eq!(
        support::whole(&partition, "durable_vector_bytes"),
        2000 * 24
    );
    assert_eq!(
        index_bytes(&db),
        45 * 4 * 16 + (12 + 8 * 46 + 4 * 16) + (4 + 2000 * 4) + 4 * 16
    );
}

#[test]
fn sealing_after_a_ivf_keeps_a_code_of_every_slot() {
    let (_, db) = support::built("sealing-after-ivf", GROUPED, AT_1000, 2000);
    let before = index_bytes(&db);
    support::load_docs(&db, 2001..=2300, "a", gathered);
    assert_eq!(count(support::cycle(&db).maintenance_vectors_done), 300);
    // 300 binary codes, and the count of the one tail block they are in.
    assert_eq!(index_bytes(&db) - before, 300 * 4 + 4);
    each_found_first(&db, 2001..=2300, gathered);
}

#[test]
fn vectors_sealed_while_a_first_build_starts_are_found_after_the_switch() {
    // Vectors of 64 numbers, of which a block takes 3,083: fewer than one cycle seals.
    const WIDE: usize = 64;
    fn wide(n: i64) -> Vec<f32> {
        support::gathered(n, WIDE)
    }
    let folder = support::folder("sealed-while-first-build-starts");
    let db = support::open(&folder);
    // The IVF is never built again, so the vectors sealed after it stay in its tail.
    support::declare_docs(&db, WIDE, "INDEXING_THRESHOLD 1000 COMPACTION NONE");
    support::load_docs(&db, 1..=2000, "a", wide);
    // A cycle advances builds before it seals: the first seals, and the second begins the
    // partition's first build. Of the 3,500 rows written then, their commit seals a whole
    // block, and the third cycle seals the start of the next.
    assert_eq!(count(support::cycle(&db).maintenance_vectors_done), 2000);
    assert_eq!(state(&db).0, "pending");
    assert_eq!(count(support::cycle(&db).maintenance_vectors_done), 0);
    assert_eq!(state(&db).0, "building");
    support::load_docs(&db, 2001..=5500, "a", wide);
    let left = support::whole(&support::the_partition(&db), "pending_tuples");
    assert!(0 < left && left < 3500, "the commit sealed a block");
    assert_eq!(
        count(support::cycle(&db).maintenance_vectors_done),
        left as u64
    );
    support::maintenance(&db);
    assert_eq!((state(&db).0, lists(&db)), ("idle".to_owned(), Some(45)));

    // Every search under INDEXED answers, and a row sealed in those cycles is the first row
    // of a search for its own vector, from the block that was filled and from the one begun.
    each_found_first(&db, (2001..=5500).step_by(233), wide);
    let stored = support::stored(&db, support::DOCS);
    let query = wide(60_002);
    let result = support::answer(&db, &support::by_mode(None, "INDEXED", 10), &query);
    support::assert_sound(&result, &stored, |_| true, &query, 10, "ten rows of 5,500");
}

#[test]
fn a_ivf_holds_every_live_vector_once_and_no_stale_slot() {
    let (_, small) = support::built("one-group-late-tail", GROUPED, "INDEXING_THRESHOLD 20", 20);
    support::run(&small, "DELETE FROM docs WHERE id <= 10");
    support::cycle(&small);
    support::load_docs(&small, 21..=21, "a", gathered);
    support::cycle(&small);
    support::cycle(&small);
    support::cycle(&small);
    assert_eq!(lists(&small), Some(1));
    support::load_docs(&small, 22..=22, "a", gathered);
    support::cycle(&small);
    small
        .verify()
        .expect("a one-group switch retires late-tail binary codes");

    let clauses = "PARTITION_KEY (ctx) INDEXING_THRESHOLD 1000";
    let (_, db) = support::built("ivf-no-stale", GROUPED, clauses, 1000);
    support::load_docs(&db, 1001..=2000, "b", gathered);
    support::maintenance(&db);
    // 150 rows of a get another vector and 150 rows of b are deleted: each leaves a slot.
    let tx = db.transaction().expect("begin");
    for id in 1..=150_i64 {
        tx.execute(
            "UPDATE docs SET v = $v WHERE id = $id",
            &params! { "id" => id, "v" => support::vector(61_000 + id, GROUPED) },
        )
        .expect("change a vector");
    }
    tx.commit().expect("commit");
    support::run(&db, "DELETE FROM docs WHERE id > 1850");
    let stale = |db: &Database| -> Vec<i64> {
        support::shown(db, "SHOW VECTOR_PARTITIONS")
            .iter()
            .map(|partition| support::whole(partition, "tombstones"))
            .collect()
    };
    assert_eq!(stale(&db), [150, 150]);

    for ran in 1..=100 {
        let report = support::cycle(&db);
        db.verify().expect("every published regroup state is sound");
        if report.maintenance_vectors_remaining == Some(0) && report.remaining_partitions == Some(0)
        {
            break;
        }
        assert!(ran < 100, "regrouping must drain");
    }
    // Metadata counts the partition, IVF (with its reach of 16 levels, 7364fc08b) and mean, and
    // each of the 1,850 rows left its one slot: 12 bytes under its key of 18. Codes and
    // centroids are block bytes.
    let expected: u64 = support::shown(&db, "SHOW VECTOR_PARTITIONS")
        .iter()
        .map(|p| 288 + 4 * 16 + 8 * lists_of(p).unwrap() as u64)
        .sum::<u64>()
        + 1850 * 30;
    assert_eq!(
        db.verify().unwrap().tables[0].vectors,
        expected,
        "applied deletion records are reclaimed"
    );
    assert_eq!(stale(&db), [0, 0], "after the regrouping");
    let stored = support::stored(&db, support::DOCS);
    let query = gathered(60_002);
    for (ctx, rows) in [("a", 1000), ("b", 850)] {
        let wanted = stored.nearest(|row| row[1] == support::text(ctx), &query, 5000);
        assert_eq!(wanted.len(), rows);
        let sql = support::by_mode(Some(&format!("ctx = '{ctx}'")), "EXACT", 5000);
        support::assert_nearest(&db, &sql, &query, &wanted, ctx);
    }
}

#[test]
fn a_build_packs_each_slot_live_at_its_source_once_whatever_departs_after() {
    // Cases (row 1 leaves grouped slot S1 for S2 before the build's source, S2 is sealed before
    // the build begins, vectors done): row 1 is deleted while the build runs. 17 grouped slots
    // are live at the source, with S2 when sealed before the build and S1 when row 1 moved
    // after the source; the case that moves it after also seals S2, one more vector done.
    let cases = [(true, true, 18), (true, false, 17), (false, false, 19)];
    for (n, (before, early, done)) in cases.into_iter().enumerate() {
        let name = format!("departed-in-build-{n}");
        let (_, db) = support::built(&name, GROUPED, "INDEXING_THRESHOLD 20", 20);
        let move_to = |id: i64| {
            let v = support::vector(61_000 + id, GROUPED);
            let sql = "UPDATE docs SET v = $v WHERE id = $id";
            db.execute(sql, &params! { "id" => id, "v" => v })
                .expect("move a vector");
        };
        let state = |db: &Database| state(db).0;
        before.then(|| move_to(1));
        if early {
            support::cycle(&db);
            assert_eq!(state(&db), "idle", "case {n}");
        }
        [2, 3].into_iter().for_each(move_to);
        support::cycle(&db); // begins the build, then seals what is pending
        assert_eq!(state(&db), "building", "case {n}");
        let mut packed = 0;
        if !before {
            move_to(1);
            packed += count(support::cycle(&db).maintenance_vectors_done);
        }
        support::run(&db, "DELETE FROM docs WHERE id = 1");
        for _ in 0..100 {
            packed += count(support::cycle(&db).maintenance_vectors_done);
            if state(&db) != "building" {
                break;
            }
        }
        assert_ne!(state(&db), "building", "case {n}: the build finishes");
        assert_eq!(
            packed, done,
            "case {n}: a build packs each slot live at its source once, and never a slot \
             that had departed by its source, whatever departs while it runs"
        );
        let stale = support::whole(&support::the_partition(&db), "tombstones");
        let wanted = if before { 1 } else { 2 }; // S2, and S1 when it was packed
        assert_eq!(stale, wanted, "case {n}: row 1's stale slots");
        db.verify().expect("the switched IVF is sound");
    }
}

#[test]
fn a_partition_is_grouped_again_only_when_its_changes_reach_the_declared_share() {
    let named = |state: &str, reason: &str| (state.to_owned(), reason.to_owned());
    let tombstones = |db: &Database| support::whole(&support::the_partition(db), "tombstones");

    // New vectors: 20 in 100 of the IVF's 100 slots when nothing is declared, and the
    // declared 50.
    for (name, clause, share) in [
        ("regroup-default", "", 20_i64),
        ("regroup-declared", " COMPACTION (CHANGE_PERCENT = 50)", 50),
    ] {
        let clauses = format!("INDEXING_THRESHOLD 50{clause}");
        let (_, db) = support::built(name, GROUPED, &clauses, 100);
        support::load_docs(&db, 101..=99 + share, "a", gathered);
        support::maintenance(&db);
        assert_eq!(state(&db), named("idle", "none"), "{name}: one below");
        assert_eq!(lists(&db), Some(10), "{name}: one below");
        support::load_docs(&db, 100 + share..=100 + share, "a", gathered);
        let was = (support::watermark(&db), state(&db));
        support::answer(&db, &support::by_mode(None, "INDEXED", 3), &gathered(1));
        let now = (support::watermark(&db), state(&db));
        assert_eq!(now, was, "{name}: a search starts no regrouping");
        support::cycle(&db);
        assert_eq!(state(&db), named("building", "changes"), "{name}");
        assert_eq!(support::maintenance(&db).0, 5, "{name}: no training rounds");
        assert_eq!(lists(&db), Some(10), "{name}: kept groups");
        support::load_docs(&db, 101 + share..=225, "a", gathered);
        assert!(support::maintenance(&db).0 > 10, "{name}: trained anew");
        assert_eq!(lists(&db), Some(15), "{name}: a doubling");
        assert_eq!(
            state(&db),
            named("idle", "none"),
            "{name}: nothing retained"
        );
        assert_eq!(tombstones(&db), 0);
    }
    for (shares, reason) in [
        ("10, TOMBSTONE_PERCENT = 50", "changes"),
        ("50, TOMBSTONE_PERCENT = 10", "tombstones"),
    ] {
        let clauses = format!("INDEXING_THRESHOLD 50 COMPACTION (CHANGE_PERCENT = {shares})");
        let (_, db) = support::built(reason, GROUPED, &clauses, 100);
        support::run(&db, "DELETE FROM docs WHERE id <= 10");
        support::cycle(&db);
        assert_eq!(state(&db), named("building", reason), "{shares}");
    }

    // Slots rows have left: 10 in 100.
    let (_, db) = support::built("regroup-tombstones", GROUPED, "INDEXING_THRESHOLD 50", 100);
    support::run(&db, "DELETE FROM docs WHERE id < 10");
    support::maintenance(&db);
    assert_eq!(state(&db), named("idle", "none"), "9 slots left behind");
    support::run(&db, "DELETE FROM docs WHERE id = 10");
    support::cycle(&db);
    assert_eq!(state(&db), named("building", "tombstones"));
    support::maintenance(&db);
    for round in 0..2 {
        db.execute(
            "UPDATE docs SET v = $v",
            &params! { "v" => gathered(70000 + round) },
        )
        .expect("turn over the vectors");
        support::maintenance(&db);
    }
    assert_eq!(lists(&db), Some(13), "turnover finds centroids anew");

    let (_, db) = support::built("regroup-mixed", GROUPED, "INDEXING_THRESHOLD 50", 100);
    support::load_docs(&db, 101..=115, "a", gathered);
    support::run(&db, "DELETE FROM docs WHERE id <= 4");
    support::maintenance(&db);
    assert_eq!(state(&db), named("idle", "none"), "19 changes");
    support::run(&db, "DELETE FROM docs WHERE id = 5");
    support::cycle(&db);
    assert_eq!(state(&db), named("building", "changes"), "20 changes");
    // Under COMPACTION NONE the first IVF stays, whatever changes.
    let clauses = "INDEXING_THRESHOLD 50 COMPACTION NONE";
    let (_, db) = support::built("regroup-none", GROUPED, clauses, 100);
    support::load_docs(&db, 101..=150, "a", gathered);
    support::run(&db, "DELETE FROM docs WHERE id <= 25");
    support::maintenance(&db);
    assert_eq!(state(&db), named("idle", "none"));
    assert_eq!(lists(&db), Some(10));
    let partition = support::the_partition(&db);
    assert_eq!(support::whole(&partition, "tombstones"), 25);
    let (stored, query) = (support::stored(&db, support::DOCS), gathered(60_000));
    let result = support::answer(&db, &support::by_mode(None, "INDEXED", 10), &query);
    support::assert_sound(&result, &stored, |_| true, &query, 10, "NONE");
    support::run(
        &db,
        "ALTER TABLE docs ALTER COLUMN v SET COMPACTION DEFAULT",
    );
    support::maintenance(&db);
    assert_eq!(state(&db), named("idle", "none"));
    assert_eq!(tombstones(&db), 0);
}

#[test]
fn a_cycle_under_a_tight_limit_records_the_stall_and_the_old_ivf_serves() {
    // A round over 22 centroids of 512 numbers takes 135,168 bytes for its sums and centroids;
    // the limit leaves 98,304 over what an open sets aside.
    const WIDE: usize = 512;
    let limit: usize = support::opening::open_floor() + support::limit_commit_bytes() + (96 << 10);
    let (folder, db) = support::built("stalled-build", WIDE, "INDEXING_THRESHOLD 200", 400);
    support::load_docs(&db, 401..=480, "a", |n| support::gathered(n, WIDE));
    // This cycle seals the 80 and begins the regrouping they make due.
    assert_eq!(count(support::cycle(&db).maintenance_vectors_done), 80);
    let stored = support::stored(&db, support::DOCS);
    support::close(db);
    let db = support::open_limited(&folder, limit);
    support::cycle(&db);
    let report = match db.run_maintenance_cycle() {
        Ok(report) => report.vector,
        Err(error) => panic!("a cycle whose step does not fit was refused: {error:?}"),
    };
    assert_eq!(
        (
            count(report.stalled_partitions),
            count(report.remaining_partitions)
        ),
        (1, 1)
    );
    let partition = support::the_partition(&db);
    assert_eq!(
        state_of(&partition),
        ("stalled".to_owned(), "memory_limit".to_owned())
    );
    assert_eq!(
        support::word(&partition, "recovery_action"),
        "raise the memory limit"
    );
    let index = support::shown(&db, "SHOW VECTOR_INDEXES").remove(0);
    assert_eq!(support::word(&index, "maintenance_state"), "stalled");
    assert_eq!(support::whole(&index, "stalled_partitions"), 1);
    assert_eq!(support::whole(&index, "live_partitions"), 1);
    let done = count(report.maintenance_vectors_done);
    assert!(done == 0, "a stalled step moves no vector");
    assert_eq!(
        support::word(&index, "recovery_action"),
        "raise the memory limit"
    );

    // The IVF in force still serves.
    assert_eq!(lists_of(&partition), Some(20));
    let query = support::gathered(60_003, WIDE);
    let result = support::answer(&db, &support::by_mode(None, "INDEXED", 10), &query);
    support::assert_sound(&result, &stored, |_| true, &query, 10, "while stalled");
    // A host's loop of cycles ends at its first cycle when all that is left is stalled.
    assert_eq!(
        support::maintenance(&db).0,
        1,
        "cycles run while all left is stalled"
    );

    // The open's limit is the session's ceiling, so room is made by opening with more.
    support::close(db);
    let db = support::open_limited(&folder, 64 << 20);
    support::maintenance(&db);
    let partition = support::the_partition(&db);
    assert_eq!(state_of(&partition), ("idle".to_owned(), "none".to_owned()));
    assert_eq!(lists_of(&partition), Some(20), "kept centroids");
    assert_eq!(count(support::cycle(&db).stalled_partitions), 0);
}

#[test]
fn deleted_edges_are_reclaimed_by_maintenance_and_compaction_rewrites_only_the_pruned() {
    let folder = support::folder("deleted-edges-reclaimed");
    let sent = Arc::new(AtomicU64::new(0));
    let mut options = DatabaseOpenOptions::default();
    options.storage = Some(support::storage({
        let sent = sent.clone();
        move |call| {
            if let support::Call::Write(_, bytes) = call {
                sent.fetch_add(bytes.len() as u64, Ordering::SeqCst);
            }
        }
    }));
    let db = support::open_with(&folder, options).expect("open");
    let sql = "CREATE TABLE edges (id INTEGER PRIMARY KEY, body TEXT, v VECTOR(16)) \
               HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST";
    support::run(&db, sql);
    let tx = db.transaction().unwrap();
    for id in 1..=400 {
        let (body, v) = (support::letters(id, 6200), support::vector(id, 16));
        let p = params! { "id" => id, "body" => body, "v" => Value::Vector(v) };
        tx.execute("INSERT INTO edges VALUES ($id, $body, $v)", &p)
            .unwrap();
    }
    tx.commit().unwrap();
    support::seal_all(&db);
    support::run(&db, "SET STORAGE_COMPACTION STOP_RATIO 0");
    support::run(&db, "SET STORAGE_COMPACTION START_RATIO 0.01");
    support::run(&db, "SET STORAGE_COMPACTION MIN_INTERVAL 0 SECONDS");
    support::run(&db, "DELETE FROM edges WHERE id <= 10");
    let before = support::data_file(&folder);
    let held = db.transaction().unwrap();
    let waiting = db.run_maintenance_cycle().unwrap().compaction_waiting_on;
    assert_eq!(waiting, Some(WaitingOn::Readers), "a reader holds pages");
    drop(held);
    let start = sent.load(Ordering::SeqCst);
    support::maintenance(&db);
    let idle = db.run_maintenance_cycle().unwrap();
    let written = sent.load(Ordering::SeqCst);
    assert_eq!(idle.work_remaining, Some(false), "reclaim ran to idle");
    assert_eq!((idle.compaction_waiting_on, idle.deferred), (None, Some(0)));
    let length = support::data_file(&folder);
    // The pages the delete freed belong to the newest commit, so this cycle gives back at most
    // the older free pages at the end of the file.
    assert!(length <= before, "reclaim report {idle:?}");
    assert!(
        2 * (written - start) < before,
        "only pruned pages are rewritten"
    );
    db.run_maintenance_cycle().unwrap();
    assert_eq!(sent.load(Ordering::SeqCst), written, "idle writes nothing");
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM edges"), 390);
    db.verify().unwrap();
}

#[test]
fn a_wake_advances_every_needy_partition_once_and_a_stall_skips_none_and_blocks_no_expiry() {
    const WIDE: usize = 512;
    // Room for a step over the 120 vectors of 2 KiB of each small partition (240 KiB), not
    // over the 800 of the large one (1,600 KiB).
    let limit = support::opening::open_floor() + support::limit_commit_bytes() + (640 << 10);
    let at = Arc::new(AtomicU64::new(100));
    let folder = support::folder("wake-all");
    let db = support::open_with(&folder, at_clock(&at)).unwrap();
    support::run(
        &db,
        "CREATE TABLE gone (id INTEGER PRIMARY KEY, due TIMESTAMP EXPIRES) RETAIN 1 SECONDS",
    );
    support::declare_docs(&db, WIDE, "PARTITION_KEY (ctx) INDEXING_THRESHOLD 20");
    // Ten grouped partitions, each given enough new rows to be due again.
    let fill = |db: &Database, from: i64, large: i64| {
        for p in 0..10_i64 {
            let (first, rows) = (p * 1000 + from, if p == 9 { large } else { 40 });
            support::load_docs(db, first..=first + rows - 1, &format!("p{p}"), |n| {
                support::gathered(n, WIDE)
            });
        }
    };
    fill(&db, 1, 400);
    support::maintenance(&db);
    fill(&db, 501, 200);
    let before = support::watermark(&db).0;
    let report = support::cycle(&db);
    let commits = support::watermark(&db).0 - before - 1;
    assert!(
        commits == 10 && count(report.remaining_partitions) == 10,
        "a commit for each of ten partitions, none twice"
    );
    support::maintenance(&db);
    // Due once more; the limit below holds a step of each small partition, not of the large.
    fill(&db, 701, 200);
    support::cycle(&db);
    support::close(db);
    let mut options = at_clock(&at);
    options.memory_limit = Some(limit);
    let db = support::open_with(&folder, options).unwrap();
    support::cycle(&db);
    db.execute(
        "INSERT INTO gone VALUES (1, $due)",
        &params! { "due" => Value::Timestamp(101_000) },
    )
    .unwrap();
    at.store(110, Ordering::SeqCst);
    let cycle = db.run_maintenance_cycle().unwrap();
    assert!(
        cycle.expired_rows == Some(1),
        "a stalled partition does not hold back expiry"
    );
    // Only the refused partition is stalled; the other due ones are built in the same cycle
    // and not marked (design 4, S2 rule 1).
    assert!(
        count(cycle.vector.stalled_partitions) == 1,
        "only the large partition is stalled: {cycle:?}"
    );
    assert!(
        db.last_maintenance_cycle().unwrap().is_some(),
        "the cycle ran to its end"
    );
}

/// A sweep begins no sooner than MIN_INTERVAL after the last ended, and none under way is held.
#[test]
fn automatic_compaction_waits_its_interval_after_a_sweep_ends() {
    let clock = Arc::new(AtomicU64::new(1_000_000));
    let db = support::open_with(&support::folder("compact-interval"), at_clock(&clock)).unwrap();
    support::run(
        &db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT NOT NULL) \
         HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST",
    );
    support::run(&db, "SET STORAGE_COMPACTION MIN_INTERVAL 1 HOURS");
    let body = support::letters(1, 6000);
    // The first batch is deleted: free pages low in the file with data above them.
    let slack = |at: i64| {
        for first in [at, at + 400] {
            let tx = db.transaction().unwrap();
            for id in first..first + 400 {
                tx.execute(
                    "INSERT INTO t VALUES ($id, $b)",
                    &params! { "id" => id, "b" => body.clone() },
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        support::run(
            &db,
            &format!("DELETE FROM t WHERE id >= {at} AND id < {}", at + 400),
        );
        support::run(&db, &format!("INSERT INTO t VALUES ({}, 'x')", -at - 1));
    };
    let moved = || count(db.run_maintenance_cycle().unwrap().compacted_pages);
    slack(0);
    assert_eq!(moved(), 0, "the first look opens the first window");
    clock.fetch_add(3600, Ordering::SeqCst);
    assert!(moved() > 0, "a pass starts at the window's end");
    // The interval never holds a pass under way: it runs to its end with the clock still.
    let end = |_| db.run_maintenance_cycle().unwrap().work_remaining == Some(false);
    assert!((0..1000).any(end), "no end of work in 1,000 cycles");
    slack(10_000);
    // The interval is stamped when the pass ends, and new free pages inside it start no pass
    // (design 4, S4).
    assert_eq!(moved(), 0, "slack again, but the last sweep ended just now");
    clock.fetch_add(3599, Ordering::SeqCst);
    assert_eq!(moved(), 0, "one second short of the interval from that end");
    // The window's statistic is the fewest free pages any commit of it left: the slack's
    // inserts used every free page before its delete freed more, so the first window to fall
    // due after the end has no surplus (design 4, S4).
    clock.fetch_add(1, Ordering::SeqCst);
    assert_eq!(
        moved(),
        0,
        "the window that held the slack's commits ends with no surplus"
    );
    // A new window starts from nothing, so space a large removal freed counts as surplus one
    // window later, and the sweep starts at that window's end (design 4, S4).
    clock.fetch_add(3599, Ordering::SeqCst);
    assert_eq!(moved(), 0, "one second short of the next window's end");
    clock.fetch_add(1, Ordering::SeqCst);
    assert!(
        moved() > 0,
        "a window with no commit leaves the removal's pages as surplus at its end"
    );
}

#[test]
fn space_no_commit_used_for_an_interval_is_given_back() {
    // Design 4, S4: a pass starts at a window's end for space no commit of the window used, and
    // one that ends having moved nothing is not started again until a commit frees a page: the
    // store gives back the half it lost with no call, then reads nothing while left alone.
    let at = Arc::new(AtomicU64::new(1_000));
    let mut options = DatabaseOpenOptions::default();
    options.clock = Some(Arc::new(support::At(at.clone())));
    let folder = support::folder("interval-surplus");
    let db = support::open_with(&folder, options).unwrap();
    let ddl = "CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT) \
               HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST";
    support::run(&db, ddl);
    let tx = db.transaction().unwrap();
    for id in 1..=2000 {
        let row = params! { "id" => id, "body" => support::letters(id, 1000) };
        tx.execute("INSERT INTO t VALUES ($id, $body)", &row)
            .unwrap();
    }
    tx.commit().unwrap();
    // The low half goes, so what is left lies above the free space and must move down.
    support::run(&db, "DELETE FROM t WHERE id <= 1000");
    let full = support::data_file(&folder);
    // One cycle a window later each time: the default MIN_INTERVAL is 60 seconds.
    let window = || {
        at.fetch_add(61, Ordering::SeqCst);
        db.run_maintenance_cycle().unwrap()
    };
    let (mut moved, mut rested) = (0, false);
    for _ in 0..full / 4096 {
        let report = window();
        // One step a cycle: 256, a page's copy (2), above it two branch levels and t's page (3).
        let step = count(report.compaction_pages_read) + count(report.compaction_pages_written);
        assert!(step <= 256 + 2 + 3, "more than one step: {report:?}");
        moved += count(report.compacted_pages);
        if moved > 0 && count(report.compaction_pages_read) == 0 {
            rested = true;
            break;
        }
    }
    assert!(
        moved > 0 && rested,
        "a pass moved {moved} pages and the tidy-up came to rest"
    );
    assert!(
        support::data_file(&folder) < full,
        "the lost half was given back with no call"
    );
    for _ in 0..3 {
        let report = window();
        let read = (count(report.compaction_pages_read), report.compacted_pages);
        assert_eq!(
            read,
            (0, Some(0)),
            "a store left alone reads nothing: {report:?}"
        );
    }
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM t"), 1000);
    db.verify().unwrap();
}
