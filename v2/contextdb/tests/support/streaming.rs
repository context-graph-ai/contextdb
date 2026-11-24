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

/// M7: a bulk UPDATE outside a transaction is written through as it matches, as a DELETE is,
/// under a writer limit below its rows. Each row it changed fires its trigger and its route
/// once, with its new values: the trigger as the statement left the row, whatever a callback
/// did to it first. An assignment reads a membership kept on disk; a vector it sets is found;
/// a broken reference refuses it and leaves nothing; a shift, a reversal whose swaps fall in
/// different parts and a shift whose held-back rows exceed the limit succeed, and each changed
/// key keeps one prior version, with its large bytes, across a reopen. One refused at its last
/// UNIQUE check after that much was held back leaves no row, version, entry or event. Inside
/// BEGIN the same statement is held in memory and refused by the limit.
#[test]
fn a_bulk_update_is_written_through_as_it_matches() {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;
    type Handed = Arc<Mutex<Vec<[Value; 4]>>>;
    fn pick(values: &HashMap<String, Value>) -> [Value; 4] {
        ["id", "n", "body", "seen"].map(|column| values[column].clone())
    }
    let (folder, n) = (support::folder("bulk-update"), 2000);
    let seeding = support::open(&folder);
    sql(
        &seeding,
        "CREATE TABLE p(id INTEGER PRIMARY KEY); CREATE TABLE picked(id INTEGER PRIMARY KEY, name TEXT); CREATE TABLE u(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id), n INTEGER UNIQUE, body TEXT, v VECTOR(3), seen BOOLEAN); CREATE EVENT TYPE changed WHEN UPDATE ON u; CREATE SINK outgoing TYPE callback; CREATE ROUTE notify EVENT changed TO outgoing; INSERT INTO p VALUES($id); INSERT INTO picked VALUES($id,$body); INSERT INTO u VALUES($id,$id,$id,$body,NULL,NULL); CREATE TRIGGER touched ON u WHEN UPDATE",
        n,
    );
    support::close(seeding);
    // What the route and the trigger were handed. Once told to, row 1's callback deletes a row
    // the statement has yet to reach, edits one it has yet to reach and one it has reached,
    // and edits the one row the statement never touches.
    let (sent, felt, meddle): (Handed, Handed, Arc<AtomicBool>) = Default::default();
    let register = |db: &Database| {
        let heard = sent.clone();
        db.register_sink("outgoing", None, move |event| {
            heard.lock().unwrap().push(pick(&event.row_values));
            Ok(())
        })
        .unwrap();
        let (felt, meddle) = (felt.clone(), meddle.clone());
        db.register_transactional_trigger_callback("touched", move |host, context| {
            felt.lock().unwrap().push(pick(&context.row_values));
            if meddle.load(Ordering::SeqCst) && context.row_values["id"] == Value::Int64(1) {
                host.execute("DELETE FROM u WHERE id = 1999", &params! {})?;
                host.execute(
                    "UPDATE u SET body = 'edited' WHERE id IN (0, 1998)",
                    &params! {},
                )?;
                host.execute("UPDATE u SET body = 'never' WHERE id = 2000", &params! {})?;
            }
            // Row 1 moved to 5000: a new row 1, inserted and updated, fires as any other.
            if context.row_values["id"] == Value::Int64(5000) {
                host.execute(
                    "INSERT INTO u VALUES (1, 1, 9001, 'back', NULL, NULL)",
                    &params! {},
                )?;
                host.execute("UPDATE u SET body = 'again' WHERE id = 1", &params! {})?;
            }
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
    };
    // What the route and then the trigger were handed since, each row once, by id.
    let once = |db: &Database, (routed, fired): (usize, usize)| {
        let (count, mut seen) = (|| sent.lock().unwrap().len(), usize::MAX);
        while seen != count() {
            seen = count();
            db.run_maintenance_cycle().unwrap();
        }
        let handed = [&sent, &felt].map(|h| std::mem::take(&mut *h.lock().unwrap()));
        let by_id = handed.map(|rows| {
            let id = |row: &[Value; 4]| match row[0] {
                Value::Int64(id) => id,
                _ => panic!("an id"),
            };
            let count = rows.len();
            let rows: HashMap<_, _> = rows.into_iter().map(|row| (id(&row), row)).collect();
            assert_eq!(rows.len(), count, "once a row");
            rows
        });
        assert_eq!((by_id[0].len(), by_id[1].len()), (routed, fired));
        by_id
    };
    let db = support::open_limited(&folder, 16 << 20);
    register(&db);
    // A membership of a thousand 16 KiB names, kept on disk, is read by the assignment.
    support::run(
        &db,
        "UPDATE u SET seen = (body IN (SELECT name FROM picked WHERE id < 1000))",
    );
    let [_, felt_rows] = once(&db, (2001, 2001));
    assert!(
        felt_rows
            .iter()
            .all(|(id, row)| row[3] == Value::Bool(*id < 1000))
    );
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM u WHERE seen"), 1000);
    let body = Value::Text("y".repeat(16 << 10));
    let (near, far) = (
        Value::Vector(vec![1.0, 0.0, 0.0]),
        Value::Vector(vec![0.0, 1.0, 0.0]),
    );
    db.execute(
        "UPDATE u SET body = $body, v = $far WHERE id >= 0",
        &params! {"body" => body.clone(), "far" => far},
    )
    .expect("2001 new values of 16 KiB under a 16 MiB limit");
    let handed = once(&db, (2001, 2001));
    assert!(
        handed.iter().flatten().all(|(_, row)| row[2] == body),
        "the route and the trigger are handed the new values"
    );
    db.execute(
        "UPDATE u SET v = $near WHERE id = 7",
        &params! {"near" => near},
    )
    .unwrap();
    once(&db, (1, 1));
    let nearest = "SELECT id FROM u ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 1";
    assert_eq!(
        support::int(&db, nearest),
        7,
        "a vector the statement set is found"
    );
    let state = "SELECT SUM(pid), SUM(n), COUNT(*) FROM u";
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
    // The newest 1,024 firings are all its own, rolled back under its one number for the
    // reference it broke, and stored before the refusal returned: an open reads them again.
    let audit = db.trigger_audit_log().unwrap().to_vec();
    assert_eq!(audit.len(), 1024, "the newest 1,024 firings");
    for entry in &audit {
        let rolled = matches!(&entry.status, contextdb::TriggerAuditStatus::RolledBack { reason }
            if reason.starts_with("foreign key violation"));
        assert!(rolled && entry.firing_tx == audit[0].firing_tx, "{entry:?}");
    }
    support::close(db);
    let db = support::open_limited(&folder, 16 << 20);
    assert_eq!(
        *db.trigger_audit_log().unwrap(),
        audit,
        "kept through a close"
    );
    register(&db);
    once(&db, (0, 2001));
    // Half the rows reversed meet a row the statement has yet to change, and are put back
    // after its walk; each swap of two values falls across parts.
    support::run(&db, "UPDATE u SET n = 3600 - n WHERE id >= 1600");
    let [routed, _] = once(&db, (401, 401));
    let row = |id: i64, n: i64| {
        [
            Value::Int64(id),
            Value::Int64(n),
            body.clone(),
            Value::Bool(false),
        ]
    };
    assert_eq!(routed[&2000], row(2000, 1600), "its delivery's values");
    assert_eq!(routed[&1600], row(1600, 2000), "its delivery's values");
    support::close(db);
    let db = support::open_limited(&folder, 16 << 20);
    register(&db);
    let swapped = "SELECT n, body FROM u WHERE id = 1600 OR id = 2000 ORDER BY id";
    let pair = [1600, 2000].map(|id| routed[&id][1..3].to_vec());
    assert_eq!(support::rows(&db, swapped), pair, "as delivered");
    // Each key keeps one version, with its large value, from before each of the membership,
    // the new bodies and the reversal.
    for id in [1600, 2000] {
        let versions = db.row_versions("u", &[Value::Int64(id)], 8).unwrap();
        let rows = versions.rows.rows();
        assert_eq!((rows.len(), versions.more), (3, false), "key {id}");
        assert_eq!(rows[0][3..5], [Value::Int64(id), body.clone()], "key {id}");
    }
    assert_eq!(
        support::int(&db, "SELECT COUNT(*) FROM u"),
        n + 1,
        "every row is there once"
    );
    // A shift of every row holds back the first 1600, 25 MiB of rows under the 16 MiB limit.
    let sums = |db: &Database| support::rows(db, "SELECT MIN(n), MAX(n), SUM(n) FROM u");
    let [low, high, sum] = [0, n, n * (n + 1) / 2].map(Value::Int64);
    assert_eq!(sums(&db), [[low, high, sum]]);
    support::run(&db, "UPDATE u SET n = n + 1");
    once(&db, (2001, 2001));
    let [low, high, sum] = [1, n + 1, n * (n + 1) / 2 + n + 1].map(Value::Int64);
    assert_eq!(sums(&db), [[low, high, sum.clone()]]);
    // The same held back and put back, refused at its last row by a value it leaves.
    let kept = |db: &Database| {
        db.row_versions("u", &[Value::Int64(0)], 16)
            .unwrap()
            .rows
            .rows()
            .len()
    };
    let versions = kept(&db);
    let refused = db.execute("UPDATE u SET n = n + 1 WHERE id < 1600", &params! {});
    assert!(
        matches!(refused, Err(Error::UniqueViolation { .. })),
        "{refused:?}"
    );
    assert_eq!(sums(&db)[0][2], sum, "no row");
    assert_eq!(kept(&db), versions, "no version");
    let entry = "SELECT id FROM u WHERE n = 2";
    assert_eq!(support::rows(&db, entry), [[Value::Int64(1)]], "no entry");
    once(&db, (0, 0));
    // A callback's writes to rows the statement wrote leave their own firings as they were.
    meddle.store(true, Ordering::SeqCst);
    support::run(&db, "UPDATE u SET body = 'b' WHERE id < 2000");
    meddle.store(false, Ordering::SeqCst);
    let [routed, fired] = once(&db, (2000, 2001));
    let text = |text: &str| Value::Text(text.into());
    let bodies = [0, 1998, 1999, 2000].map(|id| fired[&id][2].clone());
    assert_eq!(
        bodies,
        ["b", "b", "b", "never"].map(text),
        "as the statement left them"
    );
    let bodies = [0, 1998, 2000].map(|id| routed[&id][2].clone());
    assert_eq!(bodies, ["edited", "edited", "never"].map(text));
    assert!(
        !routed.contains_key(&1999),
        "a deleted row is not delivered"
    );
    let left = "SELECT body FROM u WHERE id IN (0, 1998, 1999, 2000) ORDER BY id";
    let left: Vec<Value> = support::rows(&db, left).into_iter().flatten().collect();
    assert_eq!(left, ["edited", "edited", "never"].map(text));
    // A callback's UPDATE of a key the statement emptied fires: the key fired nothing first.
    support::run(&db, "UPDATE u SET id = 5000 WHERE id = 1");
    let [_, fired] = once(&db, (1, 2));
    assert_eq!(fired[&1][2], text("again"));
    let tx = db.transaction().unwrap();
    let held = tx.execute("UPDATE u SET body = $body", &params! {"body" => body});
    assert!(
        matches!(held, Err(Error::MemoryBudgetExceeded { .. })),
        "{held:?}"
    );
}

/// A state change of `n` rows, each followed by the row that references it, is followed
/// through parts under a 16 MiB limit. `n` is the smallest the build that held its steps
/// refused while following, as measured.
#[test]
fn a_propagation_larger_than_memory_is_followed_through_parts() {
    let (folder, n) = (support::folder("bulk-propagation"), 99_999);
    let seeding = support::open(&folder);
    sql(
        &seeding,
        "CREATE TABLE d(id INTEGER PRIMARY KEY, status TEXT NOT NULL) STATE MACHINE (status: live -> [stale]); CREATE TABLE c(id INTEGER PRIMARY KEY, d_id INTEGER REFERENCES d(id) ON STATE stale PROPAGATE SET stale, status TEXT NOT NULL) STATE MACHINE (status: live -> [stale]); INSERT INTO d VALUES($id, 'live'); INSERT INTO c VALUES($id, $id, 'live')",
        n,
    );
    support::close(seeding);
    let db = support::open_limited(&folder, 16 << 20);
    support::run(&db, "UPDATE d SET status = 'stale'");
    let stale = "SELECT COUNT(*) FROM c WHERE status = 'stale'";
    assert_eq!(support::int(&db, stale), n + 1);
}

/// Inside BEGIN a statement's steps are given back when it ends: a state changed back and
/// forth `cycles` times over the same rows commits under a 16 MiB limit. Four references a
/// follower make four steps a changed row, against its one entry in the transaction's own
/// record of changes. `cycles` is the smallest the build that kept every statement's steps
/// to the commit refused, as measured.
#[test]
fn a_transaction_gives_back_each_statements_propagation() {
    let (folder, n, cycles) = (support::folder("held-steps"), 999, 2);
    let seeding = support::open(&folder);
    sql(
        &seeding,
        "CREATE TABLE d(id INTEGER PRIMARY KEY, status TEXT NOT NULL) STATE MACHINE (status: live -> [stale], stale -> [live]); CREATE TABLE c(id INTEGER PRIMARY KEY, a INTEGER REFERENCES d(id) ON STATE stale PROPAGATE SET stale, b INTEGER REFERENCES d(id) ON STATE stale PROPAGATE SET stale, e INTEGER REFERENCES d(id) ON STATE stale PROPAGATE SET stale, f INTEGER REFERENCES d(id) ON STATE stale PROPAGATE SET stale, status TEXT NOT NULL) STATE MACHINE (status: live -> [stale]); INSERT INTO d VALUES($id, 'live'); INSERT INTO c VALUES($id, $id, $id, $id, $id, 'live')",
        n,
    );
    support::close(seeding);
    let db = support::open_limited(&folder, 16 << 20);
    let tx = db.transaction().unwrap();
    for _ in 0..cycles {
        for status in ["stale", "live"] {
            let update = format!("UPDATE d SET status = '{status}'");
            tx.execute(&update, &params! {}).unwrap();
        }
    }
    tx.commit().unwrap();
    let stale = "SELECT COUNT(*) FROM c WHERE status = 'stale'";
    assert_eq!(support::int(&db, stale), n + 1);
}
