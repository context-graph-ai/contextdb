//! The clock door: the time the engine uses is the time the host's clock gives, and a time that
//! was stored stays what it was.

mod support;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use contextdb::{Clock, Database, DatabaseOpenOptions, Value, params};

fn open_ticking(folder: &Path, second: u64) -> (Database, Arc<AtomicU64>) {
    let second = Arc::new(AtomicU64::new(second));
    let mut options = DatabaseOpenOptions::default();
    options.clock = Some(Arc::new(support::At(second.clone())));
    let db = support::open_with(folder, options).expect("open the store with a clock");
    (db, second)
}

fn open_at(folder: &Path, second: u64) -> Database {
    open_ticking(folder, second).0
}

#[test]
fn default_now_stores_the_time_the_clock_door_gives() {
    let folder = support::folder("clock-door");
    let db = open_at(&folder, 1_700_000_000);
    support::run(
        &db,
        "CREATE TABLE events (id INTEGER PRIMARY KEY, created TIMESTAMP NOT NULL DEFAULT NOW(), \
         seen TIMESTAMP)",
    );
    let times = "SELECT id, created, seen FROM events ORDER BY id";
    let row =
        |id: i64, created: i64, seen: Value| [Value::Int64(id), Value::Timestamp(created), seen];

    // A row that names no time gets the clock's time as its default, in milliseconds.
    support::run(&db, "INSERT INTO events (id) VALUES (1)");
    assert_eq!(
        support::rows(&db, times),
        [row(1, 1_700_000_000_000, Value::Null)]
    );
    // NOW() in a statement is the same clock.
    support::run(&db, "UPDATE events SET seen = NOW() WHERE id = 1");
    assert_eq!(
        support::rows(&db, times),
        [row(
            1,
            1_700_000_000_000,
            Value::Timestamp(1_700_000_000_000)
        )]
    );
    assert_eq!(
        support::int(&db, "SELECT COUNT(*) FROM events WHERE created = NOW()"),
        1
    );
    support::run(&db, "INSERT INTO events (id, created) VALUES (2, NOW())");
    let same = "SELECT COUNT(*) FROM events WHERE id = 2 AND created = NOW()";
    assert_eq!(support::int(&db, same), 1);
    support::run(&db, "DELETE FROM events WHERE id = 2");
    support::close(db);

    // Reopened under a later clock, the stored times are what they were, and a new row gets the
    // time of the clock it is written under.
    let db = open_at(&folder, 1_800_000_000);
    support::run(&db, "INSERT INTO events (id) VALUES (2)");
    assert_eq!(
        support::rows(&db, times),
        [
            row(1, 1_700_000_000_000, Value::Timestamp(1_700_000_000_000)),
            row(2, 1_800_000_000_000, Value::Null),
        ]
    );
    assert_eq!(
        support::int(&db, "SELECT COUNT(*) FROM events WHERE created < NOW()"),
        1
    );

    // A clock set before 1970 gives a negative number of milliseconds, not zero or a refusal.
    struct Before;
    impl Clock for Before {
        fn now(&self) -> SystemTime {
            UNIX_EPOCH - Duration::from_millis(1_500)
        }
    }
    let mut options = DatabaseOpenOptions::default();
    options.clock = Some(Arc::new(Before));
    let early = support::open_with(&support::folder("clock-before-1970"), options)
        .expect("open the store under a clock before 1970");
    let declared = "CREATE TABLE e (id INTEGER PRIMARY KEY, made TIMESTAMP NOT NULL DEFAULT NOW())";
    support::run(&early, declared);
    support::run(&early, "INSERT INTO e (id) VALUES (1)");
    assert_eq!(
        support::rows(&early, "SELECT made FROM e"),
        [[Value::Timestamp(-1_500)]]
    );
}

#[test]
fn now_in_a_declaration_is_read_once_even_when_the_transaction_commits_later() {
    let (db, second) = open_ticking(&support::folder("clock-declaration-once"), 1_700_000_000);
    support::run(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    support::run(&db, "CREATE TABLE unrelated (id INTEGER PRIMARY KEY)");
    support::run(&db, "INSERT INTO t (id) VALUES (1)");

    let tx = db.transaction().expect("begin");
    support::run(
        &tx,
        "ALTER TABLE t ADD COLUMN ts TIMESTAMP NOT NULL DEFAULT NOW()",
    );
    let read = support::rows(&tx, "SELECT ts FROM t");
    assert_eq!(read, [[Value::Timestamp(1_700_000_000_000)]]);
    // Another commit lands, the clock moves, and the transaction commits.
    support::run(&db, "INSERT INTO unrelated (id) VALUES (1)");
    second.store(1_800_000_000, Ordering::SeqCst);
    tx.commit().expect("commit the declaration");
    assert_eq!(
        support::rows(&db, "SELECT ts FROM t"),
        read,
        "the time the declaration read is the time it stored"
    );
}

fn expired(db: &Database) -> u64 {
    support::count(db.run_maintenance_cycle().unwrap().expired_rows)
}

// No existing test expires rows: this extends the clock door with the retention judgement.
#[test]
fn each_retained_row_leaves_at_the_second_its_judgement_gives() {
    let seconds = " RETAIN 10 SECONDS";
    let future = Some(Value::Timestamp(115_000));
    for (name, policy, stamp, due) in [
        ("MINUTES", " RETAIN 1 MINUTES", None, Some(160)),
        ("HOURS", " RETAIN 1 HOURS", None, Some(3700)),
        ("DAYS", " RETAIN 1 DAYS", None, Some(86_500)),
        ("period", seconds, None, Some(110)),
        ("null", seconds, Some(Value::Null), Some(110)),
        ("pinned", seconds, Some(Value::Timestamp(i64::MAX)), None),
        ("negative", seconds, Some(Value::Timestamp(-1)), Some(100)),
        ("zero", seconds, Some(Value::Timestamp(0)), Some(100)),
        ("past", seconds, Some(Value::Timestamp(99_000)), Some(100)),
        ("now", seconds, Some(Value::Timestamp(100_000)), Some(100)),
        ("future", seconds, future, Some(115)),
        ("no-retain", "", Some(Value::Timestamp(-1)), None),
    ] {
        let folder = support::folder(&format!("expiry-{name}"));
        let (db, second) = open_ticking(&folder, 100);
        let column = if stamp.is_some() {
            ", expires TIMESTAMP EXPIRES"
        } else {
            ""
        };
        support::run(
            &db,
            &format!("CREATE TABLE t (id INTEGER PRIMARY KEY{column}){policy}"),
        );
        let empty = db.verify().unwrap();
        let values = if stamp.is_some() { "1, $stamp" } else { "1" };
        let stamp = stamp.unwrap_or(Value::Null);
        let tx = db.transaction().unwrap();
        second.store(90, Ordering::SeqCst);
        tx.execute(
            &format!("INSERT INTO t VALUES ({values})"),
            &params! { "stamp" => stamp },
        )
        .unwrap();
        second.store(100, Ordering::SeqCst);
        tx.commit().unwrap();
        let mut present = true;
        let mut times = [
            100, 109, 110, 114, 115, 159, 160, 500, 3699, 3700, 86_499, 86_500,
        ];
        times.sort_unstable();
        for at in times {
            second.store(at, Ordering::SeqCst);
            assert_eq!(
                support::int(&db, "SELECT COUNT(*) FROM t"),
                i64::from(present),
                "{name}: reads do not expire"
            );
            let removed = present && due.is_some_and(|due| at >= due);
            let before = db.work_done().unwrap();
            let held = db.snapshot().unwrap();
            assert_eq!(expired(&db), u64::from(removed), "{name} at {at}");
            let count = "SELECT COUNT(*) FROM t";
            let seen = db.execute_at_snapshot(count, &params! {}, &held).unwrap();
            assert_eq!(
                seen.rows(),
                [[Value::Int64(i64::from(present))]],
                "{name}: snapshot"
            );
            drop(held);
            assert_eq!(
                db.work_done().unwrap().rows_read - before.rows_read,
                u64::from(removed),
                "{name}: only due keys read"
            );
            present &= !removed;
            assert_eq!(
                support::int(&db, "SELECT COUNT(*) FROM t"),
                i64::from(present),
                "{name} at {at}"
            );
            if !present {
                assert_eq!(*db.verify().unwrap(), *empty);
            }
        }
        support::close(db);
        let kept = support::int(&open_at(&folder, 86_500), "SELECT COUNT(*) FROM t");
        assert_eq!(kept, i64::from(present), "{name}: after a restart");
    }
    // A trigger's firings by their own stamps: after the clock stepped back, the later firing
    // stamped 60 expires at 110 under 30 seconds while the earlier one stamped 100 stays. Once
    // judged, a wider period after a reopen or another step back never returns it; the log
    // keeps both.
    let folder = support::folder("expiry-trigger-history");
    let (db, second) = open_ticking(&folder, 100);
    support::run(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    support::run(&db, "CREATE TRIGGER seen ON t WHEN INSERT");
    db.register_transactional_trigger_callback("seen", |_, _| Ok(()))
        .unwrap();
    db.complete_initialization().unwrap();
    support::run(&db, "INSERT INTO t VALUES (1)");
    second.store(60, Ordering::SeqCst);
    support::run(&db, "INSERT INTO t VALUES (2)");
    second.store(110, Ordering::SeqCst);
    db.set_trigger_audit_retention(std::time::Duration::from_secs(30))
        .unwrap();
    let log = db.trigger_audit_log().unwrap().to_vec();
    let history = |db: &Database| db.trigger_audit_history(|_| true).unwrap().to_vec();
    assert_eq!(
        (log.len(), history(&db)),
        (2, log[..1].to_vec()),
        "judged by stamp"
    );
    db.run_maintenance_cycle().unwrap();
    support::close(db);
    let db = open_at(&folder, 110);
    let week = std::time::Duration::from_secs(7 * 86_400);
    assert_eq!(
        db.trigger_audit_retention().unwrap(),
        week,
        "seven days on every open"
    );
    assert_eq!(history(&db), log[..1], "an expired firing never comes back");
    assert_eq!(*db.trigger_audit_log().unwrap(), log, "the log keeps both");
    support::close(db);
    let db = open_at(&folder, 50);
    assert_eq!(history(&db), log[..1], "nor after the clock steps back");
    // An unbounded period keeps a firing 0 ms old, and maintenance never judges it expired.
    db.set_trigger_audit_retention(std::time::Duration::MAX)
        .unwrap();
    db.register_transactional_trigger_callback("seen", |_, _| Ok(()))
        .unwrap();
    db.complete_initialization().unwrap();
    support::run(&db, "INSERT INTO t VALUES (3)");
    db.run_maintenance_cycle().unwrap();
    let fresh = db.trigger_audit_log().unwrap()[2].clone();
    assert_eq!(history(&db), [log[0].clone(), fresh], "Duration::MAX");
    support::close(db);
    let (db, second) = open_ticking(&support::folder("expiry-alter-and-indexes"), 100);
    support::run(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    support::run(&db, "INSERT INTO t VALUES (1)");
    second.store(200, Ordering::SeqCst);
    support::run(&db, "ALTER TABLE t SET RETAIN 10 SECONDS");
    for (at, removed) in [(209, 0), (210, 1)] {
        second.store(at, Ordering::SeqCst);
        assert_eq!(expired(&db), removed, "declaration time");
    }
    support::run(&db, "INSERT INTO t VALUES (2)");
    support::run(&db, "ALTER TABLE t SET RETAIN 20 SECONDS");
    second.store(220, Ordering::SeqCst);
    assert_eq!(expired(&db), 0, "extended");
    support::run(&db, "ALTER TABLE t SET RETAIN 1 SECONDS");
    assert_eq!(expired(&db), 1, "shortened");
    support::run(&db, "ALTER TABLE t ADD COLUMN expires TIMESTAMP EXPIRES");
    support::run(&db, "INSERT INTO t VALUES (3, NULL), (4, NULL)");
    db.execute(
        "UPDATE t SET expires = $stamp WHERE id = 3",
        &params! { "stamp" => Value::Timestamp(230_000) },
    )
    .unwrap();
    second.store(221, Ordering::SeqCst);
    assert_eq!(expired(&db), 1, "stamp overrides period");
    assert_eq!(
        support::rows(&db, "SELECT id FROM t WHERE expires IS NOT NULL"),
        [[Value::Int64(3)]]
    );
    second.store(230, Ordering::SeqCst);
    assert_eq!(expired(&db), 1, "updated stamp");
    let folder = support::folder("expiry-pace");
    let (mut db, mut second) = open_ticking(&folder, 100);
    let ddl = "CREATE TABLE t (id INTEGER PRIMARY KEY) RETAIN 1 SECONDS";
    support::run(&db, ddl);
    for (ids, at, removed) in [(0..8193_i64, 200, 8192_u64), (8193..17193, 201, 9001)] {
        let tx = db.transaction().unwrap();
        for id in ids {
            tx.execute("INSERT INTO t VALUES ($id)", &params! { "id" => id })
                .unwrap();
        }
        tx.commit().unwrap();
        if at == 200 {
            support::close(db);
            (db, second) = open_ticking(&folder, at);
        }
        second.store(at, Ordering::SeqCst);
        let before = support::watermark(&db).0;
        assert_eq!(expired(&db), removed, "expiry pace at {at}");
        assert!(support::watermark(&db).0 - before >= removed.div_ceil(1024));
    }
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM t"), 0);
}
