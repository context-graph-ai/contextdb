//! The clock door: the time the engine uses is the time the host's clock gives, and a time that
//! was stored stays what it was.

mod support;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use contextdb::{Clock, Database, DatabaseOpenOptions, Value};

/// A clock that shows the second it is set to.
struct At(Arc<AtomicU64>);

impl Clock for At {
    fn now(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(self.0.load(Ordering::SeqCst))
    }
}

fn open_ticking(folder: &Path, second: u64) -> (Database, Arc<AtomicU64>) {
    let second = Arc::new(AtomicU64::new(second));
    let mut options = DatabaseOpenOptions::default();
    options.clock = Some(Arc::new(At(second.clone())));
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
