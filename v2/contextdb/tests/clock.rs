//! The clock door: the time the engine uses is the time the host's clock gives, and a time that
//! was stored stays what it was.

mod support;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use contextdb::{Clock, Database, DatabaseOpenOptions, Value};

/// A clock that always shows the same second.
struct At(u64);

impl Clock for At {
    fn now(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(self.0)
    }
}

fn open_at(folder: &Path, second: u64) -> Database {
    let mut options = DatabaseOpenOptions::default();
    options.clock = Some(Arc::new(At(second)));
    support::open_with(folder, options).expect("open the store with a clock")
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
