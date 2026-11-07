//! Uninstrumented baseline cost for the maintenance cycle's handle recycle,
//! using only the existing public `Database`/`run_maintenance_cycle` surface
//! -- no observation seam, no `open_with_backend`. This file is written to
//! compile and run unchanged whether or not continuous descriptor retention
//! across recycle has landed, so the same diagnostic can be pointed at
//! either build of the host to compare recycle cost on the same machine.
//!
//! This prints numbers; it asserts only ordinary functional correctness
//! (maintenance completes, data survives) and makes no performance pass/fail
//! claim of its own. Every measurement covers only the in-process call it
//! names -- `Database::open` of the fixture, the maintenance call that
//! recycled, and `Database::open` of the store after that writer closed --
//! and spawning or waiting on a subprocess is never counted toward any
//! reported cost here.

use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy};
use redb::{MultimapTableDefinition, TableDefinition};
use std::collections::HashMap;
use std::path::Path;

const PAYLOAD: TableDefinition<u64, &[u8]> = TableDefinition::new("payload");
const MEMBERS: MultimapTableDefinition<u64, u64> = MultimapTableDefinition::new("members");

/// A small fixture: enough churn for `redb`'s fragmentation ratio to clear
/// the compaction threshold without a large workload.
fn fragmented_store(path: &Path) {
    let db = redb::Database::create(path).expect("create fixture store");
    let write = db.begin_write().expect("begin fixture write");
    {
        let mut table = write.open_table(PAYLOAD).expect("open payload table");
        let bytes = vec![91_u8; 4096];
        for id in 0..256 {
            table
                .insert(id, bytes.as_slice())
                .expect("insert fixture row");
        }
        let mut members = write
            .open_multimap_table(MEMBERS)
            .expect("open members table");
        for value in 0..512 {
            members.insert(1, value).expect("insert fixture member");
        }
    }
    write.commit().expect("commit fixture write");
    let write = db.begin_write().expect("begin fixture delete");
    {
        let mut table = write.open_table(PAYLOAD).expect("open payload table");
        for id in 0..224 {
            table.remove(id).expect("remove fixture row");
        }
    }
    write.commit().expect("commit fixture delete");
    drop(db);
}

/// This process's own `/proc/self/io` counters, when available: bytes
/// passed through read/write system calls (`rchar`/`wchar`) and bytes that
/// reached the storage layer (`read_bytes`/`write_bytes`).
#[derive(Debug, Clone, Copy)]
struct IoCounters {
    rchar: u64,
    wchar: u64,
    read_bytes: u64,
    write_bytes: u64,
}

#[cfg(target_os = "linux")]
fn self_io_snapshot() -> Option<IoCounters> {
    let text = std::fs::read_to_string("/proc/self/io").ok()?;
    let field = |name: &str| {
        text.lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|value| value.trim().parse::<u64>().ok())
    };
    Some(IoCounters {
        rchar: field("rchar:")?,
        wchar: field("wchar:")?,
        read_bytes: field("read_bytes:")?,
        write_bytes: field("write_bytes:")?,
    })
}

#[cfg(not(target_os = "linux"))]
fn self_io_snapshot() -> Option<IoCounters> {
    None
}

/// `label_`-prefixed deltas between two snapshots, or an unavailable note.
fn io_delta(label: &str, before: Option<IoCounters>, after: Option<IoCounters>) -> String {
    match (before, after) {
        (Some(b), Some(a)) => format!(
            "{label}_rchar_delta={} {label}_wchar_delta={} {label}_read_bytes_delta={} \
             {label}_write_bytes_delta={}",
            a.rchar.saturating_sub(b.rchar),
            a.wchar.saturating_sub(b.wchar),
            a.read_bytes.saturating_sub(b.read_bytes),
            a.write_bytes.saturating_sub(b.write_bytes),
        ),
        _ => format!("{label}_io_delta=unavailable"),
    }
}

/// Wall-clock micros and process I/O deltas of exactly one `Database::open`.
fn timed_open(path: &Path, label: &str) -> (Database, u64, String) {
    let io_before = self_io_snapshot();
    let started = std::time::Instant::now();
    let db = Database::open(path).unwrap_or_else(|error| panic!("{label}: open: {error}"));
    let micros = started.elapsed().as_micros() as u64;
    let io_after = self_io_snapshot();
    (db, micros, io_delta(label, io_before, io_after))
}

/// Selectable independently of the correctness suite: prints, for several
/// bounded repeats on a fixed small fixture, the wall-clock cost and
/// `/proc/self/io` deltas of the writer's `Database::open` of the fixture,
/// the recycle micros the report already carries, the wall-clock cost and
/// I/O deltas of the whole `run_maintenance_cycle` call that performed the
/// recycle, the file's byte size before/after, and the cost and I/O deltas
/// of reopening the store after that writer closed.
#[test]
fn writer_recycle_baseline_cost_diagnostic() {
    const REPEATS: usize = 3;
    for repeat in 0..REPEATS {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("baseline_cost.redb");

        let seed = Database::open(&path).expect("seed open");
        seed.close().expect("seed close");
        drop(seed);
        fragmented_store(&path);
        let bytes_before_fixture = std::fs::metadata(&path).expect("stat fixture").len();

        let (db, open_micros, open_io) = timed_open(&path, "open");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.execute(
            "CREATE TABLE recordings (id INT PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create recordings table");

        let mut recycled = false;
        let mut recycle_call_micros = 0_u64;
        for id in 0..1024_i64 {
            let io_before = self_io_snapshot();
            let call_started = std::time::Instant::now();
            let report = db
                .run_maintenance_cycle()
                .unwrap_or_else(|error| panic!("repeat {repeat}: maintenance cycle: {error}"));
            let call_micros = call_started.elapsed().as_micros() as u64;
            let io_after = self_io_snapshot();

            db.execute(
                "INSERT INTO recordings VALUES ($id)",
                &HashMap::from([("id".into(), Value::Int64(id))]),
            )
            .unwrap_or_else(|error| panic!("repeat {repeat}: recording insert: {error}"));

            if report.compaction.handle_recycled {
                recycled = true;
                recycle_call_micros = call_micros;
                let call_io = io_delta("whole_call", io_before, io_after);
                println!(
                    "repeat={repeat} open_micros={open_micros} {open_io} recycle_micros={} \
                     whole_call_micros={call_micros} bytes_before_fixture={bytes_before_fixture} \
                     bytes_before={:?} bytes_after={:?} {call_io}",
                    report.compaction.handle_recycle_micros,
                    report.compaction.bytes_before,
                    report.compaction.bytes_after,
                );
                break;
            }
            assert!(
                report.compaction.ran,
                "repeat {repeat}: the admitted maintenance sweep keeps advancing"
            );
        }
        assert!(
            recycled,
            "repeat {repeat}: maintenance must reach its recycling batch within the bounded attempt budget"
        );
        assert!(recycle_call_micros > 0);

        assert_eq!(
            db.execute("SELECT COUNT(*) FROM recordings", &HashMap::new())
                .unwrap()
                .rows()
                .len(),
            1,
            "repeat {repeat}: the store remains usable after the measured recycle"
        );
        db.close().expect("close after measuring");
        drop(db);

        let (reopened, reopen_micros, reopen_io) = timed_open(&path, "reopen");
        let bytes_at_reopen = std::fs::metadata(&path)
            .expect("stat store at reopen")
            .len();
        println!(
            "repeat={repeat} reopen_micros={reopen_micros} {reopen_io} bytes_at_reopen={bytes_at_reopen}"
        );
        reopened.close().expect("close the reopened store");
    }
}
