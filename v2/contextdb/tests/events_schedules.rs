mod support;
#[path = "support/workev.rs"]
mod workev;
use contextdb::{DatabaseOpenOptions, Error, params};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use support::*;
use workev::open_with;
#[test]
fn a_schedule_runs_its_callback_on_any_handle() {
    for (kind, (policy, within, missed)) in [
        ("", "", 1),
        ("skip-and-audit", "", 1),
        ("catch-up", "", 0),
        ("catch-up", " WITHIN 120 SECONDS", 3),
        ("catch-up", " WITHIN 10 SECONDS", 1),
        ("fail-loud", "", 0),
        ("refused", "", 1),
        ("absent", "", 1),
        ("drop-other", "", 1),
        ("row-tx", "", 1),
        ("purge", "", 1),
    ]
    .into_iter()
    .enumerate()
    {
        let folder = folder(&format!("schedule-{policy}-{missed}"));
        let now = Arc::new(AtomicU64::new(100));
        let options = || {
            let mut options = DatabaseOpenOptions::default();
            options.clock = Some(Arc::new(At(now.clone())));
            options
        };
        let db = open_with(&folder, options()).unwrap();
        run(&db, "CREATE TABLE ticks (id INT PRIMARY KEY, calls INT)");
        run(&db, "INSERT INTO ticks VALUES (1, 0)");
        run(&db, "CREATE TABLE unrelated (id INT PRIMARY KEY)");
        run(&db, "CREATE EVENT TYPE tick WHEN UPDATE ON ticks");
        run(&db, "CREATE SINK audit TYPE callback");
        run(&db, "CREATE ROUTE ticks_to_audit EVENT tick TO audit");
        let suffix = match policy {
            "" | "refused" | "absent" | "drop-other" | "row-tx" | "purge" => String::new(),
            _ => format!(" MISSED_TICK_POLICY '{policy}'{within}"),
        };
        let failed = |db: &contextdb::Database| {
            let status = serde_json::to_value(&*db.event_bus_status().unwrap()).unwrap();
            status["schedules"][0]["last_error"].is_string()
        };
        let ddl = format!("CREATE SCHEDULE timer EVERY '10 seconds' TX (advance){suffix}");
        let narrow = |db: &contextdb::Database| match kind % 3 {
            0 => in_contexts(db, &[]),
            1 => with_labels(db, &[]),
            _ => named(db, agent("a")),
        };
        let narrowed = narrow(&db);
        let denied = refusal(&narrowed, &ddl);
        assert!(matches!(denied, Error::AdminRequired { .. }));
        let denied = narrowed.register_cron_callback("advance", |_| Ok(()));
        assert!(matches!(denied, Err(Error::AdminRequired { .. })));
        run(&db, &ddl);
        let refusing = Arc::new(AtomicBool::new(policy == "refused"));
        let register = |db: &contextdb::Database| {
            let refusing = refusing.clone();
            db.register_cron_callback("advance", move |tx: &contextdb::Database| {
                tx.execute("UPDATE ticks SET calls = calls + 1", &params! {})?;
                // Another thread is refused at once instead of waiting for this callback.
                let other = std::thread::scope(|s| {
                    s.spawn(|| tx.execute("DELETE FROM ticks", &params! {}))
                        .join()
                        .unwrap()
                });
                assert!(
                    matches!(other, Err(Error::CallbackActive { .. })),
                    "{other:?}"
                );
                if refusing.load(SeqCst) {
                    return Err(Error::PlanError("callback refused".into()));
                }
                Ok(())
            })
            .unwrap();
            db.register_cron_callback("other", |_| {
                Err(Error::PlanError("not this schedule's TX".into()))
            })
            .unwrap();
        };
        if policy != "absent" {
            register(&db);
        }
        // The status lists the schedule, and says whether its callback is registered.
        let status = serde_json::to_value(&*db.event_bus_status().unwrap()).unwrap();
        let listed = &status["schedules"][0];
        assert_eq!(listed["name"], "timer");
        assert_eq!(listed["callback_registered"], policy != "absent");
        db.run_maintenance_cycle().unwrap();
        assert_eq!(
            int(&db, "SELECT calls FROM ticks"),
            0,
            "CREATE SCHEDULE fires nothing"
        );
        now.store(109, SeqCst);
        if policy == "drop-other" {
            run(&db, "CREATE ROUTE timer EVENT tick TO audit");
            run(&db, "DROP ROUTE timer");
        }
        if policy == "purge" {
            run(&db, "INSERT INTO unrelated VALUES (1)");
            run(&db, "PURGE FROM unrelated");
        }
        narrowed.run_maintenance_cycle().unwrap();
        assert_eq!(int(&db, "SELECT calls FROM ticks"), 0);
        if policy == "absent" {
            run(
                &db,
                "CREATE TABLE gone (id INT PRIMARY KEY, due TIMESTAMP EXPIRES) RETAIN 1 SECONDS",
            );
            let due = params! { "due" => contextdb::Value::Timestamp(105_000) };
            db.execute("INSERT INTO gone VALUES (1, $due)", &due)
                .unwrap();
        }
        let pending = (policy == "row-tx").then(|| {
            let tx = db.transaction().unwrap();
            run(&tx, "INSERT INTO unrelated VALUES (1)");
            tx
        });
        now.store(110, SeqCst);
        // A schedule whose callback is missing fails alone: the cycle goes on and its error is
        // reported against that schedule (design 4, S2 rule 1).
        let first = narrowed.run_maintenance_cycle();
        assert_eq!(first.is_err(), policy == "refused", "{policy}: {first:?}");
        if policy == "absent" {
            assert!(
                failed(&db),
                "the missing callback is reported against its schedule"
            );
            assert_eq!(
                int(&db, "SELECT COUNT(*) FROM gone"),
                0,
                "the cycle still removed"
            );
        }
        if let Some(tx) = pending {
            tx.commit()
                .expect("due tick must preserve row transactions");
            assert_eq!(int(&db, "SELECT id FROM unrelated"), 1);
        }
        let prior = i64::from(!matches!(policy, "refused" | "absent"));
        assert_eq!(int(&db, "SELECT calls FROM ticks"), prior);
        drop(narrowed);
        close(db);
        let db = open_with(&folder, options()).unwrap();
        refusing.store(false, SeqCst);
        register(&db);
        db.run_maintenance_cycle().unwrap();
        // No repeat at 110 after reopen: the due time advanced durably.
        assert_eq!(int(&db, "SELECT calls FROM ticks"), prior);
        close(db);
        now.store(145, SeqCst); // Closed across the ticks at 120, 130 and 140.
        let db = open_with(&folder, options()).unwrap();
        register(&db);
        let (delivered, ids) = (
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(Vec::new())),
        );
        let (seen, rising) = (delivered.clone(), ids.clone());
        db.register_sink("audit", None, move |event| {
            seen.fetch_add(1, SeqCst);
            rising.lock().unwrap().push(event.action_id);
            Ok(())
        })
        .unwrap();
        let narrowed = narrow(&db);
        let result = narrowed.run_maintenance_cycle();
        assert_eq!(result.is_err(), policy == "fail-loud", "{policy}");
        let calls = int(&db, "SELECT calls FROM ticks");
        assert_eq!(calls, prior + missed, "{policy}");
        assert_eq!(delivered.load(SeqCst), prior as u64);
        narrowed.run_maintenance_cycle().unwrap();
        assert_eq!(delivered.load(SeqCst), (prior + missed) as u64);
        assert!(
            ids.lock().unwrap().windows(2).all(|pair| pair[0] < pair[1]),
            "action ids rise, never repeat"
        );
        drop(narrowed);
        close(db);
        let db = open_with(&folder, options()).unwrap();
        register(&db);
        db.run_maintenance_cycle().unwrap();
        assert_eq!(int(&db, "SELECT calls FROM ticks"), prior + missed);
        now.store(150, SeqCst);
        db.run_maintenance_cycle().unwrap();
        assert_eq!(int(&db, "SELECT calls FROM ticks"), prior + missed + 1);
        // Reopened at a due tick with nothing registered: a typed refusal, and no callback runs.
        now.store(160, SeqCst);
        close(db);
        let db = open_with(&folder, options()).unwrap();
        db.run_maintenance_cycle()
            .expect("a missing callback ends no cycle");
        assert!(
            failed(&db),
            "the missing callback is reported against its schedule"
        );
        assert_eq!(int(&db, "SELECT calls FROM ticks"), prior + missed + 1);
    }
}
#[test]
fn a_write_behind_a_schedules_callback_is_refused_naming_the_schedule() {
    let mut options = DatabaseOpenOptions::default();
    options.clock = Some(Observer::new(Arc::new(AtomicU64::new(0)), 1000, false));
    options.callback_wait = Some(std::time::Duration::from_millis(10));
    let db = open_with(&folder("schedule-wait"), options).unwrap();
    run(&db, "CREATE TABLE ticks (id INT PRIMARY KEY)");
    run(&db, "CREATE SCHEDULE timer EVERY '1 seconds' TX (advance)");
    let writer = Arc::new(db.scoped_with_constraints(None, None, None));
    db.register_cron_callback("advance", move |_| {
        let write = || writer.execute("INSERT INTO ticks VALUES (1)", &params! {});
        std::thread::scope(|s| s.spawn(write).join().unwrap()).map(|_| ())
    })
    .unwrap();
    let error = db.run_maintenance_cycle().unwrap_err();
    assert!(
        matches!(&error, Error::CallbackReentry { operation } if operation.contains("timer")),
        "a write behind a schedule's callback is refused as reentry, not after a wait, naming the schedule: {error:?}"
    );
}
