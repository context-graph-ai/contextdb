//! Callbacks need a Rust host; no existing test exercises their commit or cascade rules.
mod support;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};

use contextdb::{
    ChangeSet, CommitSource, DatabaseOpenOptions, DatabasePlugin, Error, SinkError, SinkEvent,
    TriggerContext, TriggerEvent, Value, params,
};
use support::*;

const RECORDS: [&str; 3] = ["entities", "incidents", "notifications"];

#[test]
fn trigger_writes_and_the_firing_rows_commit_or_roll_back_together() {
    for event in [TriggerEvent::Insert, TriggerEvent::Update] {
        let updating = event == TriggerEvent::Update;
        let outcomes = "commit rollback refused narrowed reentry panic control rewind self overtaken swap reuse";
        for outcome in outcomes.split_whitespace() {
            let probe = matches!(outcome, "swap" | "reuse");
            if (outcome == "overtaken" && !updating) || (probe && updating) {
                continue;
            }
            let moved = 2 * i64::from(updating && outcome == "commit");
            let folder = folder(&format!("trigger-{event:?}-{outcome}"));
            let mut options = contextdb::DatabaseOpenOptions::default();
            options.clock = Some(Observer::new(Arc::new(Default::default()), 1000, false));
            options.callback_wait = Some(std::time::Duration::from_millis(10));
            let db = open_with(&folder, options).unwrap();
            let strong = Arc::new(db.scoped_with_constraints(None, None, None));
            let writer = Arc::downgrade(&strong);
            let parents = if outcome == "narrowed" {
                "CREATE TABLE parents (id INTEGER PRIMARY KEY, label TEXT DEFAULT 'private' SCOPE_LABEL ('private'))"
            } else {
                "CREATE TABLE parents (id INTEGER PRIMARY KEY)"
            };
            run(&db, parents);
            for table in RECORDS {
                run(
                    &db,
                    &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY)"),
                );
            }
            let payload = large_text(Kind::Scattered, 1, 2048);
            let schema = format!(
                "CREATE TABLE notes (id INT PRIMARY KEY, pid INT REFERENCES parents(id), payload TEXT DEFAULT '{}')",
                payload.replace(char::from(39), "''")
            );
            run(&db, &schema);
            run(&db, "INSERT INTO parents (id) VALUES (0)");
            if updating || matches!(outcome, "commit" | "rewind") {
                run(&db, "INSERT INTO notes (id, pid) VALUES (1, 0), (2, 0)");
            }
            run(&db, "INSERT INTO notes (id, pid) VALUES (99, 0)");
            let tx = db.transaction().unwrap();
            let when = if updating { "UPDATE" } else { "INSERT" };
            run(&tx, &format!("CREATE TRIGGER derive ON notes WHEN {when}"));
            // Policy 31: rollback or final validation failure leaves no deliverable action.
            for when in ["INSERT", "UPDATE"] {
                run(
                    &tx,
                    &format!("CREATE EVENT TYPE changed_{when} WHEN {when} ON notes"),
                );
            }
            run(&tx, "CREATE SINK audit TYPE callback");
            for when in ["INSERT", "UPDATE"] {
                run(
                    &tx,
                    &format!("CREATE ROUTE notify_{when} EVENT changed_{when} TO audit"),
                );
            }
            let when = if updating { "INSERT" } else { "UPDATE" };
            run(&tx, &format!("CREATE TRIGGER other ON notes WHEN {when}"));
            run(&tx, "CREATE EVENT TYPE edited WHEN UPDATE ON notes");
            tx.commit().unwrap();
            let delivered = Arc::new(Mutex::new(Vec::new()));
            let external = delivered.clone();
            db.register_sink("audit", None, move |event| {
                external.lock().unwrap().push(event.action_id);
                Ok(())
            })
            .expect("register external delivery");
            let refused = db.register_transactional_trigger_callback("missing", |_, _| Ok(()));
            let refused = refused.unwrap_err();
            assert!(matches!(refused, Error::TriggerNotDeclared { .. }));
            let words = "trigger not declared: missing; declare it with CREATE TRIGGER";
            assert_eq!(refused.to_string(), words);
            let refused = db
                .register_sink("undeclared", None, |_| Ok(()))
                .unwrap_err();
            assert!(matches!(refused, Error::SinkNotDeclared { .. }));
            let words = "sink not declared: undeclared; declare it with CREATE SINK";
            assert_eq!(refused.to_string(), words);
            if outcome == "narrowed" {
                for handle in [
                    in_contexts(&db, &[1]),
                    with_labels(&db, &["public"]),
                    named(&db, agent("a")),
                ] {
                    for result in [
                        handle.register_transactional_trigger_callback("derive", |_, _| Ok(())),
                        handle.complete_initialization(),
                    ] {
                        assert!(matches!(result, Err(Error::AdminRequired { .. })));
                    }
                }
            }
            let seen = Arc::new(Mutex::new(Vec::new()));
            let observed = seen.clone();
            let escaped = Arc::new(Mutex::new(None));
            let carried = escaped.clone();
            let expected_event = event;
            let body = payload.clone();
            let other = Arc::new(Mutex::new(
                (outcome == "reentry").then(|| db.scoped_with_constraints(None, None, None)),
            ));
            let callback = move |host: &contextdb::Database, ctx: &TriggerContext| {
                let id = ctx.row_values["id"].clone();
                observed.lock().unwrap().push((ctx.tx, id, ctx.event));
                if ctx.table != "notes" || ctx.trigger_name != "derive" || probe {
                    return Ok(());
                }
                assert_eq!(ctx.event, expected_event);
                assert_eq!(ctx.depth, 1, "sibling rows start at depth one");
                assert_eq!(ctx.row_values["payload"], text(&body));
                let handle = host.scoped_with_constraints(None, None, None);
                let off_thread = std::thread::scope(|s| {
                    s.spawn(|| host.execute("SELECT 1", &params! {}))
                        .join()
                        .unwrap()
                });
                assert!(matches!(off_thread, Err(Error::CallbackActive { .. })));
                let refused = std::thread::scope(|s| {
                    s.spawn(|| {
                        writer
                            .upgrade()
                            .unwrap()
                            .execute("INSERT INTO parents VALUES (97)", &params! {})
                    })
                    .join()
                    .unwrap()
                })
                .unwrap_err();
                assert!(
                    matches!(&refused, Error::CallbackActive { operation } if operation.contains("derive")),
                    "a write behind a callback is refused by the limit, naming the trigger"
                );
                *carried.lock().unwrap() = Some(handle);
                if outcome == "self" {
                    let e = refusal(host, "DROP TRIGGER derive");
                    assert!(
                        matches!(&e, Error::CallbackReentry { operation } if operation == "DROP TRIGGER"),
                        "a callback's own DROP TRIGGER is reentry, as SET: {e}"
                    );
                }
                let Value::Int64(pid) = ctx.row_values["pid"] else {
                    panic!("pid is an integer")
                };
                let parent = pid + if outcome == "refused" { 100 } else { 0 };
                host.execute(
                    "INSERT INTO parents (id) VALUES ($p)",
                    &params! { "p" => parent },
                )?;
                if let Some(other) = other.lock().unwrap().take() {
                    other.execute("INSERT INTO parents VALUES (99)", &params! {})?;
                }
                for table in RECORDS {
                    let sql = format!("INSERT INTO {table} (id) VALUES ($p)");
                    host.execute(&sql, &params! { "p" => parent })?;
                }
                match outcome {
                    "panic" => panic!("callback failed"),
                    "control" => {
                        host.execute("SET MEMORY_LIMIT 256M", &params! {})?;
                    }
                    "rewind" => {
                        refusal(host, "INSERT INTO parents VALUES (99), (99)");
                        refusal(host, "INSERT INTO parents VALUES (NULL)");
                    }
                    "self" => {
                        host.execute(
                            "UPDATE notes SET pid = 0 WHERE id = $id",
                            &params! { "id" => ctx.row_values["id"].clone() },
                        )?;
                    }
                    _ => {}
                }
                Ok(())
            };
            for name in ["derive", "other"] {
                db.register_transactional_trigger_callback(name, callback.clone())
                    .expect("register");
            }
            assert!(matches!(
                db.register_transactional_trigger_callback("derive", |_, _| panic!("replaced")),
                Err(Error::TriggerAlreadyRegistered { .. })
            ));
            assert_eq!(
                *db.registered_trigger_callbacks().unwrap(),
                ["derive", "other"]
            );
            db.complete_initialization().expect("activate callbacks");
            run(&db, "DELETE FROM notes WHERE id = 99");
            assert!(seen.lock().unwrap().is_empty());
            if probe {
                run(&db, "ALTER TABLE notes ADD COLUMN slug TEXT UNIQUE");
                let sql = if outcome == "swap" {
                    "INSERT INTO notes (id, slug) VALUES (2, 'beta');INSERT INTO notes (id, slug) VALUES (1, 'alpha');UPDATE notes SET slug = $s ->> id"
                } else {
                    "INSERT INTO notes (id) VALUES (1);UPDATE notes SET id = 2 WHERE id = 1;INSERT INTO notes (id) VALUES (1)"
                };
                let mut sql = sql.split(';');
                run(&db, sql.next().unwrap());
                seen.lock().unwrap().clear();
                let tx = db.transaction().unwrap();
                run(&tx, sql.next().unwrap());
                let p = params! { "s" => serde_json::json!([null, "beta", "alpha"]) };
                tx.execute(sql.next().unwrap(), &p).unwrap();
                tx.commit().unwrap();
                let calls = seen.lock().unwrap();
                let counts = |id, op| calls.iter().filter(|c| c.1 == id && c.2 == op).count();
                let counts = [Value::Int64(1), Value::Int64(2)]
                    .map(|id| [counts(id.clone(), event), counts(id, TriggerEvent::Update)]);
                let swap = usize::from(outcome == "swap");
                assert_eq!(counts, [[1, swap], [0, 1]]);
                assert_eq!(calls.len(), 2 + swap);
                continue;
            }
            let before = rows(&db, "SELECT * FROM notes ORDER BY id");
            let sql = if updating {
                "UPDATE notes SET pid = id + 10"
            } else {
                "INSERT INTO notes (id, pid) VALUES (1, 11), (2, 12)"
            };
            let handle = if outcome == "narrowed" {
                with_labels(&db, &["public"])
            } else {
                db.scoped_with_constraints(None, None, None)
            };
            if outcome == "reentry" {
                let error = refusal(&handle, sql);
                assert!(matches!(error, Error::CallbackReentry { .. }));
                assert_eq!(rows(&handle, "SELECT * FROM notes ORDER BY id"), before);
                assert_eq!(int(&handle, "SELECT COUNT(*) FROM parents"), 1);
                run(&handle, "INSERT INTO parents VALUES (99)");
                continue;
            }
            let tx = handle.transaction().expect("begin");
            if !updating && outcome == "rewind" {
                run(&tx, "UPDATE notes SET pid = 0");
            }
            if !updating && matches!(outcome, "commit" | "rewind") {
                run(&tx, "DELETE FROM notes");
            }
            if moved > 0 {
                run(&tx, "UPDATE notes SET id = id + 2");
            }
            run(&tx, sql);
            assert!(seen.lock().unwrap().is_empty(), "only fires at commit");
            if outcome == "overtaken" {
                run(&db, "DELETE FROM notes WHERE id = 1");
                let result = tx.commit();
                assert!(
                    matches!(result, Err(Error::ConditionalUpdateConflict { count: 1 })),
                    "{result:?}"
                );
                assert!(seen.lock().unwrap().is_empty());
                continue;
            }
            if outcome == "rollback" {
                tx.rollback();
            } else if matches!(outcome, "panic" | "control") {
                let committed = tx.commit();
                let shown = committed.as_ref().unwrap_err().to_string();
                assert!(
                    shown.contains(if outcome == "panic" {
                        "derive"
                    } else {
                        "refused in a callback"
                    }),
                    "{outcome}: {shown}"
                );
                assert!(matches!(
                    (committed, outcome),
                    (Err(Error::TriggerCallbackFailed { .. }), "panic")
                        | (Err(Error::CallbackReentry { .. }), "control")
                ));
            } else if outcome == "refused" {
                let result = tx.commit();
                assert!(matches!(result, Err(Error::ForeignKeyViolation { .. })));
                let log = format!("{:?}", db.trigger_audit_log().unwrap());
                let rolled = log.contains("RolledBack") && !log.contains("Fired");
                assert!(rolled, "a late failure rolls every entry back");
            } else if outcome == "narrowed" {
                assert!(
                    matches!(tx.commit(), Err(Error::ScopeLabelViolation { table }) if table == "parents")
                );
            } else {
                tx.commit().expect("callback supplies referenced parents");
            }
            if let Some(handle) = escaped.lock().unwrap().take() {
                let error = refusal(&handle, "SELECT 1");
                assert!(error.to_string().starts_with("callback handle"));
                assert!(matches!(error, Error::CallbackActive { .. }));
            }
            let committed = matches!(outcome, "commit" | "rewind" | "self");
            assert!(delivered.lock().unwrap().is_empty());
            let expected = usize::from(committed) * 2;
            assert_eq!(
                db.run_maintenance_cycle().unwrap().delivered,
                Some(expected as u64)
            );
            assert_eq!(delivered.lock().unwrap().len(), expected);
            let calls = seen.lock().unwrap().clone();
            let count: usize = match outcome {
                "rollback" => 0,
                "self" if !updating => 4,
                "narrowed" | "panic" | "control" => 1,
                _ => 2,
            };
            let primary_count = calls.iter().filter(|call| call.2 == event).count();
            assert_eq!(
                (primary_count, calls.len() - primary_count),
                (count.min(2), count.saturating_sub(2))
            );
            if count >= 2 {
                assert!(calls.iter().all(|call| call.0 == calls[0].0));
                let primary: Vec<_> = calls.iter().filter(|call| call.2 == event).collect();
                assert_ne!(primary[0].1, primary[1].1, "each changed row fires once");
            }
            let v = |n| Value::Int64(n + moved);
            let mut expected = if committed {
                vec![vec![v(1), v(11)], vec![v(2), v(12)]]
            } else {
                before
            };
            if outcome == "self" {
                expected.iter_mut().for_each(|row| row[1] = v(0));
            }
            if committed {
                expected.iter_mut().for_each(|row| row.push(text(&payload)));
            }
            if outcome == "commit" && !updating {
                run(&db, "CREATE TABLE tags (id INT PRIMARY KEY)");
                for rolled_back in [true, false] {
                    let tx = db.transaction().unwrap();
                    run(&tx, "DROP TRIGGER derive");
                    if rolled_back {
                        tx.rollback();
                    } else {
                        run(&tx, "CREATE TRIGGER derive ON tags WHEN UPDATE");
                        run(&db, "DROP TABLE tags");
                        assert!(tx.commit().is_err());
                    }
                    assert_eq!(
                        *db.registered_trigger_callbacks().unwrap(),
                        ["derive", "other"]
                    );
                }
                run(&db, "CREATE TABLE tags (id INT PRIMARY KEY)");
                run(&db, "INSERT INTO tags VALUES (1)");
                run(&db, "DROP TRIGGER derive");
                run(&db, "CREATE TRIGGER derive ON tags WHEN UPDATE");
                for ddl in ["DROP TRIGGER derive", "DROP TABLE tags"] {
                    assert_eq!(*db.registered_trigger_callbacks().unwrap(), ["other"]);
                    db.register_transactional_trigger_callback("derive", callback.clone())
                        .unwrap();
                    let before = seen.lock().unwrap().len();
                    run(&db, "UPDATE tags SET id = id + 1");
                    assert_eq!(seen.lock().unwrap().len(), before + 1);
                    let tx = db.transaction().unwrap();
                    run(&tx, ddl);
                    if ddl == "DROP TRIGGER derive" {
                        run(&tx, "CREATE TRIGGER derive ON tags WHEN UPDATE");
                    }
                    tx.commit().unwrap();
                }
                assert_eq!(*db.registered_trigger_callbacks().unwrap(), ["other"]);
            }
            drop((handle, strong));
            let db = reopen(db, &folder);
            let last = format!("{:?}", db.trigger_audit_log().unwrap().last());
            assert!(outcome != "panic" || last.contains("panic: callback"));
            assert_eq!(rows(&db, "SELECT * FROM notes ORDER BY id"), expected);
            let parents = int(&db, "SELECT COUNT(*) FROM parents");
            assert_eq!(parents, if committed { 3 } else { 1 });
            for table in RECORDS {
                let records = int(&db, &format!("SELECT COUNT(*) FROM {table}"));
                assert_eq!(
                    records,
                    parents - 1,
                    "{table} commit or vanish with the firing row"
                );
            }
        }
    }
}

#[test]
fn callback_writes_fire_triggers_through_the_cap_and_refuse_the_whole_commit_beyond_it() {
    for extra in [-1_i64, 0, 1] {
        let folder = folder(&format!("trigger-depth-{extra}"));
        let db = open(&folder);
        run(&db, "CREATE TABLE chain (id INTEGER PRIMARY KEY)");
        run(&db, "CREATE TRIGGER next ON chain WHEN INSERT");
        let cap = db.trigger_cascade_depth_cap();
        let end = i64::from(cap) + extra;
        let depths = Arc::new(Mutex::new(Vec::new()));
        let observed = depths.clone();
        db.register_transactional_trigger_callback("next", move |host, ctx| {
            observed.lock().unwrap().push(ctx.depth);
            let Value::Int64(id) = ctx.row_values["id"] else {
                panic!("id is an integer")
            };
            assert_eq!(i64::from(ctx.depth), id);
            if id < end {
                host.execute(
                    "INSERT INTO chain VALUES ($id)",
                    &params! { "id" => id + 1 },
                )?;
            }
            Ok(())
        })
        .expect("register cascade");
        db.complete_initialization().expect("activate callbacks");
        let result = db.execute("INSERT INTO chain VALUES (1)", &params! {});
        if extra > 0 {
            assert!(
                matches!(result, Err(Error::TriggerCascadeDepthExceeded { trigger_name, depth })
                if trigger_name == "next" && depth == cap + 1)
            );
        } else {
            result.expect("every depth through the cap commits");
        }
        assert_eq!(
            *depths.lock().unwrap(),
            (1..=cap.min(end as u32)).collect::<Vec<_>>()
        );
        let expected = if extra > 0 { 0 } else { end };
        let tx = format!("{:?}", support::watermark(&db));
        run(&db, "DROP TRIGGER next");
        let shown = |db: &contextdb::Database| {
            let log = db.trigger_audit_log().unwrap();
            let kinds = log.iter().map(|e| format!("{:?}", e.status).remove(0));
            (kinds.collect::<String>(), format!("{log:?}"))
        };
        let (kinds, before) = shown(&db);
        let refused = extra > 0;
        let want = if refused {
            "R".repeat(cap as usize) + "D"
        } else {
            "F".repeat(end as usize)
        };
        let narrowed = in_contexts(&db, &[1]).trigger_audit_log().is_err();
        let export = folder.join("export");
        db.export_snapshot(&export).unwrap();
        let db = reopen(db, &folder);
        let log = db.trigger_audit_log().unwrap();
        let cascade = log.iter().map(|e| (e.depth, e.cascade_row_count));
        let n = log.len() as u32;
        let each = (1..=n).map(|depth| (depth, u64::from(depth < n)));
        assert!(
            cascade.eq(each),
            "each firing's depth and rows written, reopened"
        );
        let kept = shown(&db).1 == before && shown(&open(&export)).1 == before;
        let carried = refused || before.matches(&tx).count() == kinds.len();
        assert_eq!(
            (kinds, narrowed, kept, carried),
            (want, true, true, true),
            "one entry per trigger taken, RolledBack with the refused one DepthExceeded; it carries the callback's transaction, survives a restart and an export, and a narrowed handle is refused"
        );
        assert_eq!(int(&db, "SELECT COUNT(*) FROM chain"), expected);
        if extra == 0 {
            declare(&db);
            run(&db, "CREATE TRIGGER wide ON t WHEN INSERT");
            let (started, entered) = std::sync::mpsc::sync_channel(0);
            let (release, resume) = std::sync::mpsc::sync_channel(0);
            let held = Mutex::new((started, resume));
            // The refusal's text is long: kept once, not once a firing, it fits the limit.
            let long = "x".repeat(32 << 10);
            let reason = format!("the last firing refuses {long}");
            let refused = reason.clone();
            db.register_transactional_trigger_callback("wide", move |host, ctx| {
                match ctx.row_values["id"] {
                    Value::Int64(1100) => host
                        .execute("INSERT INTO chain VALUES (101),(102),(103)", &params! {})
                        .map(drop),
                    Value::Int64(1101) => host
                        .execute("DELETE FROM chain WHERE id > 100", &params! {})
                        .map(drop),
                    // Held until a second handle has read the log.
                    Value::Int64(2000) => {
                        let held = held.lock().unwrap();
                        held.0.send(()).unwrap();
                        held.1.recv().unwrap();
                        Ok(())
                    }
                    Value::Int64(3099) => Err(Error::PlanError(refused.clone())),
                    _ => Ok(()),
                }
            })
            .unwrap();
            db.complete_initialization().unwrap();
            insert_together(&db, 0..1100);
            insert_together(&db, 1100..1102);
            let log = db.trigger_audit_log().unwrap().to_vec();
            let (first, last) = (log[0].firing_tx, log[1023].firing_tx);
            assert!(
                log.len() == 1024 && first == log[1021].firing_tx && first != last,
                "the ring keeps 1,024, oldest first"
            );
            // 1,100 rows, held on the first firing and refused on the last: a second handle
            // reads the earlier log while held, then the refused write's newest 1,024.
            let second = db.scoped_with_constraints(None, None, None);
            db.set_memory_limit(Some(support::opening::open_floor() + (16 << 20)))
                .unwrap();
            std::thread::scope(|scope| {
                let release = release;
                let running = scope.spawn(|| {
                    let tx = db.transaction().unwrap();
                    insert(&tx, 2000..3100);
                    tx.commit()
                });
                entered.recv().unwrap();
                assert_eq!(*second.trigger_audit_log().unwrap(), log, "held");
                release.send(()).unwrap();
                let refused = running.join().unwrap();
                db.set_memory_limit(Some(256 << 20)).unwrap();
                let after = second.trigger_audit_log().unwrap();
                assert!(matches!(refused, Err(Error::PlanError(_))) && after.len() == 1024);
                let reason = format!("plan error: {reason}");
                let rolled = contextdb::TriggerAuditStatus::RolledBack { reason };
                assert!(after.iter().all(|e| e.status == rolled));
            });
            // Reopened at once, the history holds every one of the refused write's firings.
            drop(second);
            let db = reopen(db, &folder);
            let tx = db.trigger_audit_log().unwrap()[1023].firing_tx;
            let history = db.trigger_audit_history(|e| e.firing_tx == tx).unwrap();
            assert_eq!(history.len(), 1100, "none of 1,100 refused firings is lost");
            let rows = |e: &contextdb::TriggerAuditEntry| (e.depth, e.cascade_row_count);
            let wrote = db
                .trigger_audit_history(|e| e.trigger_name == "wide" && e.cascade_row_count > 0)
                .unwrap();
            let none = db.trigger_audit_history(|e| e.firing_tx == first).unwrap();
            assert!(
                wrote.iter().map(rows).eq([(1, 3), (1, 3)])
                    && none.iter().all(|e| rows(e) == (1, 0)),
                "the firings that wrote and deleted three rows say so, and those that wrote none say 0: {:?}",
                wrote.iter().map(rows).collect::<Vec<_>>()
            );
            // A table whose only trigger has no callback refuses writes on both routes.
            run(&db, "CREATE TRIGGER lone ON chain WHEN INSERT");
            let delete = "DELETE FROM chain WHERE id = 1";
            let unready = |e: Error| matches!(e, Error::EngineNotInitialized { .. });
            assert!(unready(refusal(&db, delete)) && unready(refusal(&db, "PURGE FROM chain")));
            assert!(
                unready(refusal(&db.transaction().unwrap(), delete)),
                "at the statement"
            );
            run(&db, "CREATE TABLE plain (id INTEGER PRIMARY KEY)");
            run(&db, "INSERT INTO plain VALUES (1)");
            let lone = db.complete_initialization().unwrap_err().to_string();
            assert!(
                lone.contains("register_transactional_trigger_callback")
                    && int(&db, "SELECT COUNT(*) FROM chain WHERE id = 1") == 1,
                "a missing callback names its registering call, and the row survives: {lone}"
            );
            db.register_transactional_trigger_callback("lone", |_, _| Ok(()))
                .unwrap();
            let tx = db.transaction().unwrap();
            run(&tx, "DELETE FROM chain WHERE id = 2");
            tx.commit().unwrap();
            run(&db, delete);
        }
    }
}

#[test]
fn a_sink_is_delivered_to_once_for_each_event() {
    // Runnable sinks bypass pending work; deletes keep defaults; JSON stays inside the limit.
    for case in "delivered transient uncertain permanent pending denied dropped webhook replacement
                    starved settled evolved json json12"
        .split_whitespace()
    {
        let folder = folder(&format!("delivery-{case}"));
        let db = if case == "settled" {
            contextdb::Database::open(&folder).unwrap()
        } else {
            open(&folder)
        };
        run(&db, GRANTS);
        run(
            &db,
            "CREATE TABLE notices (id INT PRIMARY KEY,
                  acl_id UUID ACL REFERENCES acl_grants(acl_id), lane TEXT DEFAULT 'a', doc JSON)",
        );
        let when = ["INSERT", "DELETE"][usize::from(case == "evolved")];
        let sql = format!("CREATE EVENT TYPE added WHEN {when} ON notices");
        run(&db, &sql);
        let kind = if case == "webhook" {
            "webhook URL 'https://example.invalid/hook'"
        } else {
            "callback"
        };
        run(&db, &format!("CREATE SINK outgoing TYPE {kind}"));
        let route = "CREATE ROUTE notify EVENT added TO outgoing WHERE lane IN ('a')";
        run(&db, route);
        if case == "starved" {
            run(&db, "CREATE SINK absent TYPE callback");
            let route = "CREATE ROUTE backlog EVENT added TO absent WHERE lane IN ('b')";
            run(&db, route);
            let rows: Vec<_> = (1..=300).map(|id| format!("(-{id}, 'b')")).collect();
            let sql = format!("INSERT INTO notices (id, lane) VALUES {}", rows.join(","));
            run(&db, &sql);
        }
        if case == "settled" {
            let (sent, received) = std::sync::mpsc::channel();
            db.register_sink("outgoing", None, move |event| {
                if event.row_values["id"] != Value::Int64(1) {
                    return Err(SinkError::Permanent("bad".into()));
                }
                sent.send(event.action_id).unwrap();
                Ok(())
            })
            .unwrap();
            let refused: String = (1..=256).map(|id| format!("(-{id}),")).collect();
            run(
                &db,
                &format!("INSERT INTO notices (id) VALUES {refused}(1)"),
            );
            let action = received.recv().expect("worker continues");
            assert_eq!(action, 257);
            continue;
        }
        if case == "replacement" {
            replacing_a_sink_during_a_pass_keeps_one_delivery_per_action(&db);
            continue;
        }
        let charged = Arc::new(Mutex::new((0, None::<contextdb::Database>)));
        let attempts = Arc::new(Mutex::new(Vec::<SinkEvent>::new()));
        let register = |db: &contextdb::Database, case: &'static str| {
            let seen = attempts.clone();
            let charge = charged.clone();
            charged.lock().unwrap().1 = Some(db.scoped_with_constraints(None, None, None));
            let contexts = (case == "denied").then(|| context_set(&[]));
            db.scoped_with_constraints(contexts, None, None)
                .register_sink("outgoing", None, move |event| {
                    let mut charge = charge.lock().unwrap();
                    charge.0 = charge.1.as_ref().unwrap().memory_used().unwrap();
                    seen.lock().unwrap().push(event.clone());
                    match case {
                        "transient" | "json" | "json12" => {
                            Err(SinkError::Transient("retry".into()))
                        }
                        "uncertain" => Err(SinkError::OutcomeUncertain("retry".into())),
                        "permanent" => Err(SinkError::Permanent("refused".into())),
                        _ => Ok(()),
                    }
                })
                .unwrap();
        };
        if case != "pending" {
            register(&db, case);
        }
        // Policy 31: commit preserves pending work without invoking the external handler.
        let count = if case == "delivered" { 257 } else { 1 };
        let bytes = if case == "json12" { 3 << 20 } else { 2 << 20 };
        let doc = Value::Json(serde_json::json!("x".repeat(bytes)));
        let json = case.starts_with("json");
        if json {
            run(&db, "SET MEMORY_LIMIT 64M");
        }
        let baseline = used(&db) as u64;
        let skipped = db.transaction().unwrap();
        let rows: Vec<_> = (1..=count).map(|id| format!("({id}, $doc)")).collect();
        let sql = format!("INSERT INTO notices (id, doc) VALUES {}", rows.join(","));
        let value = if json { doc.clone() } else { Value::Null };
        let result = db.execute(&sql, &params! { "doc" => value }).unwrap();
        let ceiling = baseline + 2 * bytes as u64 + (1 << 20);
        assert!(!json || result.peak_bytes().unwrap() <= ceiling);
        let mut commits = vec![watermark(&db); count as usize];
        skipped.rollback();
        if case == "evolved" {
            let sql = "ALTER TABLE notices ADD COLUMN status TEXT DEFAULT 'open'";
            run(&db, sql);
            run(&db, "DELETE FROM notices WHERE id = 1");
            commits[0] = watermark(&db);
        }
        if json {
            let huge = Value::Json(serde_json::json!(vec![serde_json::json!({}); 100_000]));
            let sql = "INSERT INTO notices (id, doc) VALUES (2, $doc)";
            let refused = db.execute(sql, &params! { "doc" => huge });
            assert!(matches!(refused, Err(Error::MemoryBudgetExceeded { .. })));
        }
        if case == "dropped" {
            run(&db, "DROP TABLE notices");
        }
        assert!(attempts.lock().unwrap().is_empty());
        let success = ["delivered", "webhook", "starved", "evolved"].contains(&case);
        let pass = db.run_maintenance_cycle().unwrap();
        let ceiling = baseline + bytes as u64 + (1 << 20);
        assert!(!json || charged.lock().unwrap().0 as u64 <= ceiling);
        assert!(case != "dropped" || pass.deferred == Some(0));
        assert!(case != "starved" || pass.deferred == Some(300));
        let first = count.min(256) as u64;
        assert_eq!(pass.delivered, Some(if success { first } else { 0 }));
        let attempted =
            usize::from(!matches!(case, "pending" | "denied" | "dropped")) * first as usize;
        assert_eq!(attempts.lock().unwrap().len(), attempted);
        if matches!(
            case,
            "pending" | "transient" | "uncertain" | "permanent" | "webhook"
        ) {
            let metrics = db.sink_metrics("outgoing").unwrap();
            assert_eq!(metrics.queued, u64::from(!success && case != "permanent"));
            assert_eq!(metrics.delivered, u64::from(success));
            assert_eq!(metrics.uncertain, u64::from(case == "uncertain"));
            assert_eq!(metrics.permanent_failures, u64::from(case == "permanent"));
            let status = db.event_bus_status().unwrap();
            assert_eq!(status.event_types[0].name, "added");
            assert_eq!(status.sinks[0].metrics, metrics);
            assert_eq!(status.sinks[0].callback_registered, case != "pending");
            assert_eq!(status.routes[0].sink, "outgoing");
        }
        // K9: delivered disappears, permanent is terminal, other cases stay pending.
        charged.lock().unwrap().1.take();
        let db = reopen(db, &folder);
        register(&db, "delivered");
        let retry = matches!(case, "transient" | "uncertain" | "pending" | "denied") || json;
        let delivered = db.run_maintenance_cycle().unwrap().delivered;
        assert_eq!(delivered, Some(u64::from(retry || count > 256)));
        assert_eq!(db.run_maintenance_cycle().unwrap().delivered, Some(0));
        if case != "dropped" {
            let kept = count + 300 * i64::from(case == "starved") - i64::from(case == "evolved");
            assert_eq!(int(&db, "SELECT COUNT(*) FROM notices"), kept);
        }
        charged.lock().unwrap().1.take();
        let calls = attempts.lock().unwrap();
        assert_eq!(calls.len(), attempted + usize::from(retry || count > 256));
        for (index, event) in calls.iter().enumerate() {
            assert_ne!(event.action_id, 0);
            assert_eq!(event.event_type, "added");
            assert_eq!(event.table, "notices");
            let default = Value::Text("open".into());
            assert!(case != "evolved" || event.row_values["status"] == default);
            assert!(!json || event.row_values["doc"] == doc);
            let row = if count > 256 { index as i64 + 1 } else { 1 };
            assert_eq!(event.row_values["id"], Value::Int64(row));
            assert_eq!(event.at_lsn, commits[row as usize - 1]);
            if retry {
                assert_eq!(event.action_id, calls[0].action_id, "stable retry identity");
            } else if index > 0 {
                assert!(calls[index - 1].action_id < event.action_id);
            }
        }
    }
}

// The replacement row shares the outcome table's declarations and caller-driven store.
fn replacing_a_sink_during_a_pass_keeps_one_delivery_per_action(db: &contextdb::Database) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let seen = calls.clone();
    let (active, peak) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let enter = |active: &AtomicUsize, peak: &AtomicUsize| {
        peak.fetch_max(active.fetch_add(1, SeqCst) + 1, SeqCst);
    };
    let (running, high) = (active.clone(), peak.clone());
    let (started, entered) = std::sync::mpsc::sync_channel(0);
    let (release, resume) = std::sync::mpsc::sync_channel(0);
    let resume = Mutex::new(resume);
    db.register_sink("outgoing", None, move |event| {
        enter(&running, &high);
        seen.lock().unwrap().push(event.action_id);
        started.send(()).unwrap();
        resume.lock().unwrap().recv().unwrap();
        running.fetch_sub(1, SeqCst);
        Ok(())
    })
    .unwrap();
    run(db, "INSERT INTO notices (id) VALUES (1)");
    std::thread::scope(|scope| {
        let pass = scope.spawn(|| db.run_maintenance_cycle().unwrap());
        entered.recv().unwrap();
        let (seen, running, high) = (calls.clone(), active.clone(), peak.clone());
        db.register_sink("outgoing", None, move |event| {
            enter(&running, &high);
            seen.lock().unwrap().push(event.action_id);
            running.fetch_sub(1, SeqCst);
            Ok(())
        })
        .unwrap();
        // K9/CDB-030: replacement neither holds the commit lock nor duplicates in-flight work.
        run(db, "INSERT INTO notices (id) VALUES (2)");
        let another = scope.spawn(|| db.run_maintenance_cycle().unwrap());
        release.send(()).unwrap();
        let delivered =
            pass.join().unwrap().delivered.unwrap() + another.join().unwrap().delivered.unwrap();
        assert_eq!(
            delivered + db.run_maintenance_cycle().unwrap().delivered.unwrap(),
            2
        );
    });
    assert_eq!(peak.load(SeqCst), 1, "one worker delivers at a time");
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert!(
        calls[0] < calls[1],
        "the pass reads delivery records in order"
    );
}
#[test]
fn unloadable_head_defers_and_later_deliveries_arrive() {
    let path = folder("unloadable-head");
    let db = delivery_queue(&path, 2 << 20, 2);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let calls = seen.clone();
    db.register_sink("outgoing", None, move |event| {
        let id = event.row_values["id"].clone();
        let body = "x".repeat(if id == Value::Int64(1) { 2 << 20 } else { 8 });
        assert_eq!(event.row_values["body"], text(&body));
        calls.lock().unwrap().push((id, event.action_id));
        Ok(())
    })
    .unwrap();
    run(
        &db,
        "CREATE TABLE points(id INTEGER PRIMARY KEY,v VECTOR(2))",
    );
    run(&db, "INSERT INTO points VALUES(1,[1,0])");
    db.set_memory_limit(Some(support::opening::open_floor() + (1 << 20)))
        .unwrap();
    let first = db.run_maintenance_cycle().unwrap();
    assert_eq!((first.delivered, first.deferred), (Some(1), Some(1)));
    assert_eq!(first.vector.maintenance_vectors_done, Some(1));
    assert_eq!(db.run_maintenance_cycle().unwrap().deferred, Some(1));
    // Two cycles refused the head for memory; delivery clears the flag and keeps the count.
    let m = || db.sink_metrics("outgoing").unwrap();
    assert_eq!((m().window_refusals, m().window_refused), (2, true));
    assert_eq!(m().examined, 2, "each event is counted once");
    assert_eq!(&*seen.lock().unwrap(), &[(Value::Int64(2), 2)]);
    db.set_memory_limit(Some(128 << 20)).unwrap();
    assert_eq!(db.run_maintenance_cycle().unwrap().delivered, Some(1));
    assert_eq!(seen.lock().unwrap()[1], (Value::Int64(1), 1));
    assert_eq!((m().window_refusals, m().window_refused), (2, false));
}
#[test]
fn retry_keeps_body_and_slice_cleanup_is_atomic() {
    use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
    for terminal in ["success", "permanent", "purge"] {
        let path = folder(&format!("delivery-slices-{terminal}"));
        let db = delivery_queue(&path, 1 << 20, 1);
        close(db);
        let bytes = Arc::new(AtomicU64::new(0));
        let writes = bytes.clone();
        let mut options = contextdb::DatabaseOpenOptions::default();
        options.storage = Some(storage(move |call| {
            if let Call::Write(_, b) = call {
                writes.fetch_add(b.len() as u64, SeqCst);
            }
        }));
        let db = open_with(&path, options).unwrap();
        let tried = Arc::new(AtomicU64::new(0));
        let first = tried.clone();
        db.register_sink("outgoing", None, move |event| {
            first.store(event.action_id, SeqCst);
            Err(SinkError::OutcomeUncertain("retry".into()))
        })
        .unwrap();
        bytes.store(0, SeqCst);
        assert_eq!(db.run_maintenance_cycle().unwrap().deferred, Some(1));
        assert_eq!(
            tried.load(SeqCst),
            1,
            "the retry keeps the action id the later attempts see"
        );
        assert!(bytes.load(SeqCst) < 256 << 10, "retry rewrote the body");
        let db = reopen(db, &path);
        assert_eq!(db.sink_metrics("outgoing").unwrap().uncertain, 1);
        if terminal == "purge" {
            for _ in 0..128 {
                run(&db, "DELETE FROM queued");
                // Action 257 is selected, past the first 256-record batch.
                run(&db, "INSERT INTO queued VALUES(2,'small'),(1,'small')");
            }
            assert_eq!(db.sink_metrics("outgoing").unwrap().queued, 257);
        }
        run(&db, "DROP ROUTE notify");
        if terminal == "purge" {
            run(&db, "UPDATE queued SET body='small'");
            db.set_memory_limit(Some(support::opening::open_floor() + (1 << 20)))
                .unwrap();
            run(&db, "PURGE FROM queued WHERE id=1");
            db.register_sink("outgoing", None, |event| {
                assert_eq!(event.row_values["id"], Value::Int64(2));
                Ok(())
            })
            .unwrap();
            assert_eq!(db.run_maintenance_cycle().unwrap().delivered, Some(128));
            run(&db, "DELETE FROM queued");
        } else {
            run(&db, "DELETE FROM queued");
            db.register_sink("outgoing", None, move |event| {
                assert_eq!(event.action_id, 1);
                assert_eq!(event.row_values["body"], text(&"x".repeat(1 << 20)));
                if terminal == "success" {
                    Ok(())
                } else {
                    Err(SinkError::Permanent("stop".into()))
                }
            })
            .unwrap();
            db.run_maintenance_cycle().unwrap();
        }
        db.set_memory_limit(Some(256 << 20)).unwrap();
        assert_eq!(db.sink_metrics("outgoing").unwrap().queued, 0);
        let empty = delivery_queue(&path.join("empty"), 0, 0);
        let before = empty.export_snapshot(path.join("empty-export")).unwrap();
        let after = db.export_snapshot(path.join("clean-export")).unwrap();
        assert!(
            after.records <= before.records + 4,
            "removed delivery left slices"
        );
        open(&path.join("clean-export")).verify().unwrap();
    }
}

/// A PURGE from inside a sink callback, which the delivery drive is running, is refused by name
/// instead of waiting for the drive that is waiting for it.
#[test]
fn a_purge_is_refused_inside_a_sink() {
    let db = Arc::new(delivery_queue(&folder("purge-sink"), 8, 2));
    let refusal = Arc::new(Mutex::new(None));
    let (weak, seen) = (Arc::downgrade(&db), refusal.clone());
    db.register_sink("outgoing", None, move |_| {
        let own = weak
            .upgrade()
            .unwrap()
            .execute("PURGE FROM queued", &params! {});
        *seen.lock().unwrap() = Some(own.err());
        Ok(())
    })
    .unwrap();
    db.run_maintenance_cycle().expect("the drive completes");
    assert!(
        matches!(
            refusal.lock().unwrap().take(),
            Some(Some(Error::CallbackReentry { .. }))
        ),
        "a PURGE from inside a sink must be refused as CallbackReentry"
    );
    db.execute("PURGE FROM queued", &params! {})
        .expect("a PURGE outside the sink completes");
    assert_eq!(
        int(&*db, "SELECT COUNT(*) FROM queued"),
        0,
        "a read after the PURGE saw a row"
    );
}

/// The event bus status is an answer charged to the writer: by its entries, held while it is
/// held, given back when dropped, and refused when it does not fit.
#[test]
fn the_event_bus_status_is_charged_by_entry_and_given_back() {
    let db = open_limited(&folder("bus-status"), 256 << 20);
    run(&db, "CREATE TABLE notes (id INT PRIMARY KEY)");
    let tx = db.transaction().unwrap();
    for n in 0..120 {
        let (event, sink) = (
            format!("event_with_a_long_name_{n:04}"),
            format!("sink_with_a_long_name_{n:04}"),
        );
        run(
            &tx,
            &format!("CREATE EVENT TYPE {event} WHEN INSERT ON notes"),
        );
        run(&tx, &format!("CREATE SINK {sink} TYPE callback"));
        run(
            &tx,
            &format!("CREATE ROUTE route_with_a_long_name_{n:04} EVENT {event} TO {sink}"),
        );
    }
    tx.commit().unwrap();
    drop(db.event_bus_status().unwrap());
    let base = used(&db);
    let status = db.event_bus_status().unwrap();
    let held = used(&db) - base;
    let text: usize = status
        .event_types
        .iter()
        .map(|e| e.name.len() + e.trigger.len() + e.table.len())
        .sum::<usize>()
        + status.sinks.iter().map(|s| s.name.len()).sum::<usize>()
        + status
            .routes
            .iter()
            .map(|r| r.name.len() + r.event_type.len() + r.sink.len())
            .sum::<usize>();
    assert!(
        held >= text,
        "a status of {text} bytes of names was charged {held}: entries are not charged"
    );
    drop(status);
    assert_eq!(
        used(&db),
        base,
        "the status was not given back when dropped"
    );
    // Setting a limit takes room of its own, so the room is set first and then used up by held
    // statuses until one byte less (or exactly) one status's charge is left.
    let whole = (1 << 20) / held + 1;
    for (spare, fits) in [(held - 1, false), (held, true)] {
        db.set_memory_limit(Some(base + whole * held + spare))
            .unwrap();
        assert_eq!(used(&db), base, "setting the limit changed memory in use");
        let kept: Vec<_> = (0..whole)
            .map(|_| db.event_bus_status().expect("room"))
            .collect();
        let last = db.event_bus_status();
        assert_eq!(
            last.is_ok(),
            fits,
            "a status with {spare} bytes of room, its charge being {held}, must {}be admitted",
            if fits { "" } else { "not " }
        );
        drop((kept, last));
        assert_eq!(used(&db), base, "statuses were not all given back");
    }
}

#[test]
fn a_cancel_during_a_writers_statement_refuses_it() {
    let db = open(&folder("cancel-a-statement"));
    db.cancel(); // with nothing running, it ends nothing: every statement below runs
    run(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    run(&db, "CREATE TRIGGER stop ON t WHEN INSERT");
    let stop = |host: &contextdb::Database, _: &_| Ok(()).inspect(|()| host.cancel());
    db.register_transactional_trigger_callback("stop", stop)
        .unwrap();
    db.complete_initialization().unwrap();
    let e = refusal(&db, "INSERT INTO t VALUES (1)");
    assert!(matches!(e, Error::QueryCanceled { .. }), "{e}");
    assert_eq!(int(&db, "SELECT COUNT(*) FROM t"), 0, "left nothing");
}

#[test]
fn delivery_reads_a_long_backlog_in_batches_from_the_store_under_a_small_limit() {
    let path = folder("long-queue");
    let db = delivery_queue(&path, 8, 0);
    let (tx, body) = (db.transaction().unwrap(), "x".repeat(4096));
    for id in 1..=600 {
        let values = params! { "id" => id, "body" => body.clone() };
        tx.execute("INSERT INTO queued VALUES ($id, $body)", &values)
            .unwrap();
    }
    tx.commit().unwrap();
    let db = reopen(db, &path);
    assert_eq!(db.sink_metrics("outgoing").unwrap().queued, 600);
    assert!(!db.event_bus_status().unwrap().sinks[0].callback_registered);
    let limit = support::opening::open_floor() + (2 << 20);
    db.set_memory_limit(Some(limit)).unwrap();
    let ids = Arc::new(Mutex::new(Vec::new()));
    let kept = ids.clone();
    db.register_sink("outgoing", None, move |event| {
        kept.lock().unwrap().push(event.action_id);
        Ok(())
    })
    .unwrap();
    let batches = [0; 3].map(|_| db.run_maintenance_cycle().unwrap().delivered);
    assert_eq!(batches[..2], [Some(256); 2], "a cycle delivers one batch");
    assert_eq!(batches[2], Some(88));
    assert_eq!(*ids.lock().unwrap(), (1..=600).collect::<Vec<u64>>());
}

struct Veto;
impl DatabasePlugin for Veto {
    fn pre_commit(&self, _: &ChangeSet, _: CommitSource) -> contextdb::Result<()> {
        std::hint::black_box(vec![1_u8; 8 << 20]); // memory the refusal must give back
        Err(Error::PlanError("veto".into()))
    }
}

#[test]
fn a_commit_the_plug_in_or_the_disk_refuses_queues_and_delivers_nothing() {
    for outcome in ["plug-in", "disk"] {
        let path = folder(&format!("refused-{outcome}"));
        close(delivery_queue(&path, 8, 0));
        let (broken, mut options) = (
            Arc::new(AtomicBool::new(false)),
            DatabaseOpenOptions::default(),
        );
        let armed = broken.clone();
        options.storage = Some(failing_storage(move |call| match call {
            Call::Sync if armed.load(SeqCst) => Err(std::io::Error::other("disk")),
            _ => Ok(()),
        }));
        options.plugin = (outcome == "plug-in").then(|| Arc::new(Veto) as _);
        let db = open_with(&path, options).unwrap();
        broken.store(outcome == "disk", SeqCst);
        let sql = "INSERT INTO queued VALUES (1, 'x')";
        // A refusal leaves the statement's form in the capped record, once; used is read after it.
        let _ = (outcome == "plug-in").then(|| db.execute(sql, &params! {}));
        let used = memory_used(&db);
        let insert = db.execute(sql, &params! {});
        if outcome == "plug-in" {
            assert!(
                matches!(insert, Err(Error::PlanError(ref m)) if m == "veto"),
                "its own error"
            );
            assert_eq!(memory_used(&db), used, "the plug-in's memory is given back");
        }
        assert!(insert.is_err(), "{outcome}");
        // After the disk refused a sync the commit's outcome is unknown: once reopened the row and
        // its event are both there, delivered once, or both absent.
        // A close after the drive refused a sync returns that failure (section 7, C2).
        let closed = db.close();
        assert!(
            outcome == "plug-in" && closed.is_ok()
                || matches!(&closed, Err(Error::StoreIo { reason, .. }) if reason.contains("disk")),
            "{outcome}: {closed:?}"
        );
        drop(db);
        let db = open(&path);
        let heard = Arc::new(AtomicUsize::new(0));
        let counter = heard.clone();
        let hear = move |_: &SinkEvent| {
            counter.fetch_add(1, SeqCst);
            Ok(())
        };
        db.register_sink("outgoing", None, hear).unwrap();
        db.run_maintenance_cycle().unwrap();
        let metrics = db.sink_metrics("outgoing").unwrap();
        let rows = int(&db, "SELECT COUNT(*) FROM queued");
        assert!(
            rows <= 1 && (rows == 0 || outcome == "disk"),
            "{outcome}: {rows}"
        );
        assert_eq!(
            heard.load(SeqCst) as i64,
            rows,
            "{outcome}: the event goes with its row"
        );
        assert_eq!(metrics.delivered as i64, rows, "{outcome}: delivered once");
    }
}

#[test]
fn hidden_events_wait_while_later_visible_events_are_delivered_first() {
    let db = open(&folder("hidden-events"));
    let table = "CREATE TABLE notes (id INT PRIMARY KEY, c UUID CONTEXT_ID, label TEXT \
                 SCOPE_LABEL_READ ('public', 'secret') WRITE ('public', 'secret'))";
    for sql in [
        table,
        "CREATE EVENT TYPE added WHEN INSERT ON notes",
        "CREATE SINK outgoing TYPE callback",
        "CREATE ROUTE notify EVENT added TO outgoing",
    ] {
        run(&db, sql);
    }
    // Note 1 is hidden by its context, note 2 by its label, note 3 is visible.
    for (id, c, label) in [(1, 2, "public"), (2, 1, "secret"), (3, 1, "public")] {
        let values = params! { "id" => id, "c" => context(c), "label" => label };
        db.execute("INSERT INTO notes VALUES ($id, $c, $label)", &values)
            .unwrap();
    }
    let seen = Arc::new(Mutex::new(Vec::new()));
    let register = |handle: &contextdb::Database| {
        let seen = seen.clone();
        let heard = move |event: &SinkEvent| {
            seen.lock().unwrap().push(event.row_values["id"].clone());
            Ok(())
        };
        handle.register_sink("outgoing", None, heard).unwrap();
    };
    register(&with_labels(&in_contexts(&db, &[1]), &["public"]));
    assert_eq!(db.run_maintenance_cycle().unwrap().delivered, Some(1));
    assert_eq!(*seen.lock().unwrap(), [Value::Int64(3)]);
    assert_eq!(db.sink_metrics("outgoing").unwrap().queued, 2);
    register(&db);
    assert_eq!(db.run_maintenance_cycle().unwrap().delivered, Some(2));
    assert_eq!(
        seen.lock().unwrap()[1..],
        [Value::Int64(1), Value::Int64(2)]
    );
}
