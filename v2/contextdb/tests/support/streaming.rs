//! The section-11 operations, each through the same allocation/failure/copy driver.
use super::*;
use std::path::Path;
use std::sync::{Arc, Mutex};
use support::{Child, Run, for_each_trial, learned};

// Extra setup is SQL; each operation preserves row zero as an unrelated confirmed neighbour.
const CASES: &[(&str, &str, &str)] = &[
    ("plain", "", "PURGE FROM t WHERE id > 0"),
    (
        "followers",
        "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES t(id) FOLLOWS PARENT PURGE, link INTEGER REFERENCES c(id) FOLLOWS PARENT PURGE); INSERT INTO c VALUES($id,$id,$id); UPDATE c SET link=2 WHERE id=1; UPDATE c SET link=1 WHERE id=2",
        "PURGE FROM t WHERE id > 0",
    ),
    (
        "vectors",
        "ALTER TABLE t ADD COLUMN v VECTOR(16) PARTITION_KEY(tag)",
        "PURGE FROM t WHERE id > 0",
    ),
    ("queue", "", "PURGE FROM t WHERE id = 1"),
    (
        "purge-in",
        "CREATE TABLE picked(id INTEGER PRIMARY KEY); INSERT INTO picked VALUES($id)",
        "PURGE FROM t WHERE id > 0 AND id IN(SELECT id FROM picked)",
    ),
    (
        "purge-not-in",
        "CREATE TABLE picked(id INTEGER PRIMARY KEY); INSERT INTO picked VALUES(-$id)",
        "PURGE FROM t WHERE id NOT IN(SELECT id FROM picked)",
    ),
    (
        "delete-in",
        "CREATE TABLE picked(id INTEGER PRIMARY KEY); INSERT INTO picked VALUES($id)",
        "DELETE FROM t WHERE id > 0 AND id IN(SELECT id FROM picked)",
    ),
    (
        "delete-not-in",
        "CREATE TABLE picked(id INTEGER PRIMARY KEY); INSERT INTO picked VALUES(-$id)",
        "DELETE FROM t WHERE id NOT IN(SELECT id FROM picked)",
    ),
    (
        "delete-route",
        "CREATE EVENT TYPE removed WHEN DELETE ON t; CREATE SINK outgoing TYPE callback; CREATE ROUTE notify EVENT removed TO outgoing",
        "DELETE FROM t WHERE id > 0",
    ),
    (
        "delete-parent",
        "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES t(id) FOLLOWS PARENT PURGE); INSERT INTO c VALUES($id,$id)",
        "DELETE FROM t WHERE id > 0",
    ),
    (
        "purge-text-in",
        "CREATE TABLE picked(id INTEGER PRIMARY KEY, name TEXT); INSERT INTO picked VALUES($id,$body)",
        "PURGE FROM t WHERE id > 0 AND name IN(SELECT DISTINCT name FROM picked ORDER BY name)",
    ),
    ("delivery", "", ""),
];
fn sql(db: &Database, text: &str, n: i64) {
    for statement in text.split(';').filter(|s| !s.trim().is_empty()) {
        let tx = db.transaction().unwrap();
        for id in 0..=if statement.contains("$id") { n } else { 0 } {
            let size = usize::from(text.contains("$body")) << 14;
            let body = format!("row-{id:07}-{}", "x".repeat(size));
            tx.execute(statement, &params! {"id"=>id,"body"=>body})
                .unwrap();
        }
        tx.commit().unwrap();
    }
}
fn seed(folder: &Path, case: usize, n: i64) {
    let db = support::open(folder);
    support::declare(&db);
    if matches!(CASES[case].0, "queue" | "delivery") {
        sql(
            &db,
            "CREATE EVENT TYPE added WHEN INSERT ON t; CREATE SINK outgoing TYPE callback; CREATE ROUTE notify EVENT added TO outgoing",
            n,
        );
        db.execute(
            "INSERT INTO t VALUES(-1,$large,0)",
            &params! {"large"=>"x".repeat(17 << 20)},
        )
        .unwrap();
    }
    let tx = db.transaction().unwrap();
    for id in 0..=n {
        tx.execute(
            "INSERT INTO t VALUES($id,$name,$tag)",
            &params! {
                "id"=>id, "name"=>format!("row-{id:07}-{}", "x".repeat(if CASES[case].0 == "purge-text-in" { 16384 } else { 256 })), "tag"=>id%2
            },
        )
        .unwrap();
    }
    tx.commit().unwrap();
    sql(&db, CASES[case].1, n);
    if CASES[case].0 == "vectors" {
        let tx = db.transaction().unwrap();
        for id in 0..=n {
            tx.execute(
                "UPDATE t SET v=$v WHERE id=$id",
                &params! {"v"=>support::vector(id,16),"id"=>id},
            )
            .unwrap();
        }
        tx.commit().unwrap();
        for _ in 0..3 {
            db.run_maintenance_cycle().unwrap();
        }
        sql(
            &db,
            "UPDATE t SET tag=1 WHERE id=1; UPDATE t SET v=NULL WHERE id=2",
            n,
        );
    }
    if CASES[case].0 == "queue" {
        sql(&db, "DROP ROUTE notify", n);
    }
    support::close(db);
}
// Ordered SQL is the oracle: complete before or complete after, never a mixture.
fn dump(db: &impl Run, case: usize, after: bool) -> String {
    let tables = if matches!(CASES[case].0, "followers" | "delete-parent") {
        "t c"
    } else if CASES[case].0.contains("-in") {
        "t picked"
    } else {
        "t"
    };
    tables
        .split_whitespace()
        .map(|table| {
            let filter = if !after
                || table == "picked"
                || matches!(CASES[case].0, "delete-parent" | "delivery")
            {
                ""
            } else if CASES[case].0 == "queue" {
                " WHERE id<>1"
            } else {
                " WHERE id=0"
            };
            format!(
                "{table}:{:?}\n",
                support::rows(db, &format!("SELECT * FROM {table}{filter} ORDER BY id"))
            )
        })
        .collect()
}
fn check(folder: &Path, case: usize, before: &str, after: &str, success: bool, refused: bool) {
    let db = support::open_limited(folder, 512 << 20);
    let now = dump(&db, case, false);
    assert!(
        !refused || now == before,
        "disk refusal changed {}",
        CASES[case].0
    );
    assert!(now == before || now == after, "partial {}", CASES[case].0);
    if success {
        assert!(
            now == after,
            "successful {} differs from after",
            CASES[case].0
        );
    }
    db.verify().unwrap();
    if now == after && CASES[case].2.starts_with("PURGE") {
        let dest = folder.join("export");
        db.export_snapshot(&dest).unwrap();
        assert_eq!(
            dump(&support::open_limited(&dest, 512 << 20), case, false),
            after
        );
        assert_eq!(
            dump(&support::open_reader(folder, None), case, false),
            after
        );
    }
    db.set_disk_limit(None).unwrap();
    support::run(&db, "INSERT INTO t(id,name,tag) VALUES(999999,'next',0)");
}
fn worker(role: support::Role) {
    let case = role.number("case") as usize;
    let mode = role.number("mode");
    let stop = if mode == 3 { 0 } else { role.number("stop_at") };
    let (storage, calls) = support::faulting_before_call(stop, mode == 2, true);
    let mut options = DatabaseOpenOptions::default();
    options.storage = Some(storage);
    options.memory_limit = Some(
        if matches!(CASES[case].0, "queue" | "delivery") && mode == 0 || CASES[case].0 == "delivery"
        {
            16 << 20
        } else if mode == 0 {
            64 << 20
        } else {
            512 << 20
        },
    );
    let warm_cache = mode == 0
        && matches!(
            CASES[case].0,
            "followers" | "vectors" | "queue" | "delete-route" | "purge-text-in"
        );
    let limit = options.memory_limit;
    let raise = warm_cache && limit.is_some_and(|bytes| bytes > 32 << 20);
    options.memory_limit = limit.filter(|_| !raise); // no option: an 8 MiB cache, no ceiling (M4)
    let db = support::open_with(&role.folder, options).unwrap();
    if warm_cache {
        if raise {
            db.set_memory_limit(limit).unwrap();
        }
        fill_the_cache(&db);
    }
    let delivered = Arc::new(Mutex::new(std::collections::BTreeSet::new()));
    if CASES[case].0 == "delivery" {
        db.register_sink("outgoing", None, {
            let delivered = delivered.clone();
            move |event| {
                assert!(
                    delivered.lock().unwrap().insert(event.action_id),
                    "event delivered twice"
                );
                Ok(())
            }
        })
        .unwrap();
    }
    let warm = if CASES[case].0 == "purge-text-in" {
        "SELECT id FROM t WHERE id>=0"
    } else {
        "SELECT id,name FROM t WHERE id>=0"
    };
    drop(support::run(&db, warm));
    // Every faulted run commits its disk limit before it begins, none for the control, so the
    // control's growth is the growth a limited run meets.
    if mode != 0 {
        db.set_disk_limit((mode == 3).then(|| role.number("limit")))
            .unwrap();
    }
    support::say(format!("begins {}", calls.load(Ordering::SeqCst)));
    let success = std::cell::Cell::new(false);
    let refused = measured("", || {
        let answer = if CASES[case].0 == "delivery" {
            db.run_maintenance_cycle().map(|_| ())
        } else {
            match db.execute(CASES[case].2, &params! {}) {
                Err(Error::ForeignKeyViolation { .. }) if CASES[case].0 == "delete-parent" => {
                    Ok(())
                }
                answer => answer.map(|_| ()),
            }
        };
        success.set(answer.is_ok());
        match answer {
            Err(Error::DiskLimitExceeded { .. }) if mode == 3 => {
                support::say("diskrefused 1");
                Ok(())
            }
            Err(Error::StoreIo { .. }) if stop > 0 => Ok(()),
            other => other,
        }
    });
    if mode == 0 || role.number("stop_at") == 0 {
        assert!(!refused);
    }
    if CASES[case].0 == "delivery" {
        let delivered = delivered.lock().unwrap().len() as u64;
        assert!(delivered <= role.number("rows") + 1);
        if success.get() {
            assert_eq!(delivered, role.number("rows") + 1);
        }
    }
    support::say(format!("calls {}", calls.load(Ordering::SeqCst)));
    support::say(format!("success {}", u8::from(success.get())));
}
macro_rules! operation_tests {
    ($($name:ident: ($case:literal, $part:literal, $parts:literal)),* $(,)?) => {$(
        #[test]
        fn $name() { run_case(concat!("streaming::", stringify!($name)), $case, $part, $parts); }
    )*};
}
operation_tests! {
    plain_streaming_keeps_memory_atomicity_and_erasure: (0, 0, 1),
    followers_streaming_keeps_memory_atomicity_and_erasure: (1, 0, 1),
    vectors_streaming_keeps_memory_atomicity_and_erasure: (2, 0, 1),
    queue_streaming_keeps_memory_atomicity_and_erasure: (3, 0, 1),
    purge_in_streaming_keeps_memory_atomicity_and_erasure: (4, 0, 1),
    purge_not_in_streaming_keeps_memory_atomicity_and_erasure: (5, 0, 1),
    delete_in_streaming_keeps_memory_atomicity_and_erasure: (6, 0, 1),
    delete_not_in_streaming_keeps_memory_atomicity_and_erasure: (7, 0, 1),
    delete_route_streaming_keeps_memory_atomicity_and_erasure: (8, 0, 1),
    delete_parent_streaming_keeps_memory_atomicity_and_erasure: (9, 0, 1),
    purge_text_in_streaming_keeps_memory_atomicity_and_erasure: (10, 0, 1),
    delivery_part_0_streaming_keeps_memory_atomicity_and_erasure: (11, 0, 4),
    delivery_part_1_streaming_keeps_memory_atomicity_and_erasure: (11, 1, 4),
    delivery_part_2_streaming_keeps_memory_atomicity_and_erasure: (11, 2, 4),
    delivery_part_3_streaming_keeps_memory_atomicity_and_erasure: (11, 3, 4),
}
fn run_case(test: &str, case: usize, part: u64, parts: u64) {
    if let Some(role) = support::role() {
        return worker(role);
    }
    let mut failures = Vec::new();
    let fixture = format!("{case}-{part}");
    let (name, _, _) = CASES[case];
    let run = std::panic::catch_unwind(|| {
        let mut peaks = Vec::new();
        for scale in [1, 10, 100] {
            if let Ok(peak) = std::panic::catch_unwind(|| {
                let n = if CASES[case].0 == "delivery" {
                    100
                } else {
                    100 * scale
                };
                let folder = support::folder(&format!("stream-{fixture}-{n}"));
                seed(&folder, case, n);
                let db = support::open(&folder);
                if matches!(
                    name,
                    "followers" | "vectors" | "queue" | "delete-route" | "purge-text-in"
                ) {
                    write_the_filler(&db);
                }
                let before = dump(&db, case, false);
                let after = dump(&db, case, true);
                support::close(db);
                let lines = Child::start(
                    test,
                    &format!("run case={case} mode=0 stop_at=0 rows={n}"),
                    &folder,
                )
                .end();
                check(&folder, case, &before, &after, true, false);
                support::last_number(&lines, "rise").unwrap()
            }) {
                peaks.push(peak);
            }
        }
        assert!(
            peaks.len() == 3
                && peaks.iter().max().unwrap() - peaks.iter().min().unwrap() <= 1 << 20,
            "size-following peak: {peaks:?}"
        );
    });
    eprintln!(
        "ROW {} memory {}",
        CASES[case].0,
        if run.is_ok() { "GREEN" } else { "RED" }
    );
    if run.is_err() {
        failures.push((CASES[case].0, "memory"));
    }
    let run = std::panic::catch_unwind(|| {
        let source = support::folder(&format!("stream-source-{fixture}"));
        seed(&source, case, 100);
        let db = support::open(&source);
        let before = dump(&db, case, false);
        let after = dump(&db, case, true);
        support::close(db);
        let control = support::folder(&format!("stream-control-{fixture}"));
        std::fs::copy(source.join("data"), control.join("data")).unwrap();
        let (begins, calls, lines) = learned(
            test,
            &format!("run case={case} mode=1 rows=100"),
            &control,
            Some("begins"),
        );
        check(&control, case, &before, &after, true, false);
        // The child's own word says whether it was acknowledged; a limit at the control's high
        // water, past its last call, admits the whole operation.
        let compare = |at: u64, folder: &Path, lines: &[String]| {
            let success = support::last_number(lines, "success") == Some(1);
            assert!(success || at <= calls, "{name} refused at its high water");
            let refused = support::last_number(lines, "diskrefused") == Some(1);
            check(folder, case, &before, &after, success, refused);
        };
        let mut passed = true;
        let owns = |at: u64| (at - begins - 1) % parts == part;
        for mode in [1, 2] {
            passed &= for_each_trial(
                test,
                &format!("run case={case} mode={mode} rows=100"),
                (begins + 1..=calls).filter(|at| owns(*at)),
                source.clone(),
                mode == 2,
                compare,
            );
        }
        let growth = lines
            .iter()
            .filter_map(|line| {
                let (at, size) = line.strip_prefix("growth ")?.split_once(' ')?;
                Some((at.parse::<u64>().unwrap(), size.parse::<u64>().unwrap()))
            })
            .filter(|(at, _)| (begins + 1..=calls).contains(at));
        let initial = std::fs::metadata(source.join("data")).unwrap().len();
        let high_water = growth.clone().fold(initial, |size, (_, len)| size.max(len));
        for (at, size) in growth.chain([(calls + 1, high_water + 1)]) {
            if !owns(at) {
                continue;
            }
            let limit = size - 1;
            passed &= for_each_trial(
                test,
                &format!("run case={case} mode=3 limit={limit} rows=100"),
                [at],
                source.clone(),
                false,
                compare,
            );
        }
        assert!(passed, "one or more storage fault points failed");
    });
    eprintln!(
        "ROW {} failure {}",
        CASES[case].0,
        if run.is_ok() { "GREEN" } else { "RED" }
    );
    if run.is_err() {
        failures.push((CASES[case].0, "failure"));
    }
    assert!(
        failures.is_empty(),
        "streaming properties failed: {failures:?}"
    );
}

/// M7: a bulk UPDATE outside a transaction is written through as it matches, as a DELETE is:
/// under a writer limit below its rows it commits, each row it changed fires its trigger once,
/// a broken reference refuses it and leaves nothing, and a shift of a unique run succeeds.
#[test]
fn a_bulk_update_is_written_through_as_it_matches() {
    let (folder, n) = (support::folder("bulk-update"), 2000);
    let seeding = support::open(&folder);
    sql(
        &seeding,
        "CREATE TABLE p(id INTEGER PRIMARY KEY); CREATE TABLE u(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id), n INTEGER UNIQUE, body TEXT); CREATE EVENT TYPE changed WHEN UPDATE ON u; CREATE SINK outgoing TYPE callback; CREATE ROUTE notify EVENT changed TO outgoing; INSERT INTO p VALUES($id); INSERT INTO u VALUES($id,$id,$id,$body)",
        n,
    );
    support::close(seeding);
    let db = support::open_limited(&folder, 16 << 20);
    let fired = Arc::new(Mutex::new(Vec::new()));
    let heard = fired.clone();
    db.register_sink("outgoing", None, move |event| {
        let id = format!("{:?}", event.row_values["id"]);
        heard.lock().unwrap().push(id);
        Ok(())
    })
    .unwrap();
    let body = "y".repeat(16 << 10);
    db.execute(
        "UPDATE u SET body = $body WHERE id >= 0",
        &params! {"body" => body},
    )
    .expect("2001 new values of 16 KiB under a 16 MiB limit");
    let (count, mut seen) = (|| fired.lock().unwrap().len(), usize::MAX);
    while seen != count() {
        seen = count();
        db.run_maintenance_cycle().unwrap();
    }
    let once: std::collections::BTreeSet<_> = fired.lock().unwrap().iter().cloned().collect();
    assert_eq!((count(), once.len()), (2001, 2001), "once a row");
    let state = "SELECT SUM(pid), SUM(n) FROM u";
    let before = support::rows(&db, state);
    let broken = db.execute("UPDATE u SET pid = pid + 1000000", &params! {});
    assert!(
        matches!(broken, Err(Error::ForeignKeyViolation { .. })),
        "{broken:?}"
    );
    assert_eq!(
        support::rows(&db, state),
        before,
        "refused, it leaves nothing"
    );
    support::run(&db, "UPDATE u SET n = n + 1 WHERE id >= 1990");
    assert_eq!(
        support::int(&db, "SELECT MAX(n) FROM u"),
        n + 1,
        "a run shifts"
    );
}
