use contextdb_core::{Result, read_contract::OwnerReadCancellation};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, OwnerRequestHandler, ReadSession,
    ReadSessionOptions,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};

struct HeldRead {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl OwnerRequestHandler for HeldRead {
    fn handle(
        &self,
        _namespace: &str,
        request: &[u8],
        cancellation: &OwnerReadCancellation,
    ) -> Result<Vec<u8>> {
        if request == b"held" || request == b"answer" {
            self.entered.send(()).expect("announce accepted request");
            self.release
                .lock()
                .expect("release gate")
                .recv()
                .expect("release accepted request");
        }
        assert!(
            !cancellation.is_cancelled(),
            "ordinary owner drop preserves accepted work"
        );
        Ok(request.to_vec())
    }
}

#[test]
fn graceful_close_publishes_quiescence_before_withdrawing_the_channel() {
    use contextdb_engine::{OwnerReadTestHooks, QuiescentStopPhase};

    let root = tempfile::tempdir().expect("task-scoped store directory");
    let path = root.path().join("owner-close.db");
    let runtime = root.path().join("runtime");
    std::fs::create_dir(&runtime).expect("create runtime directory");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
            .expect("secure runtime directory");
    }
    let (entered, accepted) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let (phase_sent, phase_seen) = mpsc::channel();
    let (resume_sent, resume) = mpsc::channel();
    let resume = Arc::new(Mutex::new(resume));
    let stop_observer = Arc::new(move |phase: QuiescentStopPhase| {
        phase_sent.send(phase).expect("announce stop phase");
        resume
            .lock()
            .expect("stop gate")
            .recv()
            .expect("resume stop");
    });
    let database = Arc::new(
        Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                owner_reads: OwnerReadConfig {
                    runtime_dir: Some(runtime.clone()),
                    handler: Some(Arc::new(HeldRead {
                        entered,
                        release: Mutex::new(released),
                    })),
                    test_hooks: Some(OwnerReadTestHooks {
                        clock: Arc::new(
                            contextdb_engine::local_transport::ManualDeadlineClock::default(),
                        ),
                        drain_started: mpsc::channel().0,
                        active_request_registered: None,
                        accept_thread_exited: None,
                        accept_thread_joined: None,
                        service_dropped: None,
                        quiescent_stop: Some(stop_observer),
                    }),
                    ..OwnerReadConfig::default()
                },
                ..DatabaseOpenOptions::default()
            },
        )
        .expect("open owner"),
    );
    database
        .execute(
            "CREATE TABLE retained (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create durable table");
    database
        .execute("INSERT INTO retained VALUES (7)", &HashMap::new())
        .expect("commit a row for the stop-boundary read");
    let reader = ReadSession::open_in_runtime_dir(
        &path,
        ReadSessionOptions::default(),
        Some(runtime.clone()),
    )
    .expect("open owner route");
    let reading = std::thread::spawn(move || reader.request_owner("proof", b"held"));
    accepted.recv().expect("accepted request enters");
    let closing = Arc::clone(&database);
    let closer = std::thread::spawn(move || closing.close());

    for expected in [
        QuiescentStopPhase::WritesStopped,
        QuiescentStopPhase::Published,
        QuiescentStopPhase::ChannelWithdrawn,
    ] {
        assert_eq!(phase_seen.recv().expect("stop phase"), expected);
        if expected == QuiescentStopPhase::WritesStopped {
            assert!(
                database
                    .execute("INSERT INTO retained VALUES (1)", &HashMap::new())
                    .is_err(),
                "the writer is frozen before quiescence is published"
            );
        }
        let fresh = ReadSession::open_in_runtime_dir(
            &path,
            ReadSessionOptions::default(),
            Some(runtime.clone()),
        )
        .expect("fresh reader opens at stop boundary");
        assert_eq!(
            fresh
                .execute("SELECT id FROM retained", &HashMap::new())
                .expect("complete committed row remains readable")
                .rows[0][0],
            contextdb_core::Value::Int64(7)
        );
        if expected == QuiescentStopPhase::WritesStopped {
            assert_eq!(fresh.request_owner("proof", b"probe").unwrap(), b"probe");
        } else {
            assert!(
                fresh.request_owner("proof", b"probe").is_err(),
                "a post-publication reader must select the file"
            );
        }
        resume_sent.send(()).expect("resume stop");
    }
    release.send(()).expect("complete accepted request");
    assert_eq!(
        reading
            .join()
            .expect("read joins")
            .expect("accepted read succeeds"),
        b"held"
    );
    assert_eq!(
        phase_seen.recv().expect("drained phase"),
        QuiescentStopPhase::AcceptedDrained
    );
    let fresh =
        ReadSession::open_in_runtime_dir(&path, ReadSessionOptions::default(), Some(runtime))
            .expect("fresh reader opens after drain");
    assert!(fresh.request_owner("proof", b"probe").is_err());
    assert_eq!(
        fresh
            .execute("SELECT id FROM retained", &HashMap::new())
            .expect("complete committed row after drain")
            .rows[0][0],
        contextdb_core::Value::Int64(7)
    );
    resume_sent.send(()).expect("release owner hold");
    closer.join().expect("close joins").expect("close succeeds");
}

#[test]
fn dropped_owner_finishes_an_accepted_request_before_releasing_the_store() {
    let root = tempfile::tempdir().expect("task-scoped store directory");
    let path = root.path().join("owner-drop.db");
    let runtime = root.path().join("runtime");
    std::fs::create_dir(&runtime).expect("create runtime directory");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
            .expect("secure runtime directory");
    }
    let (entered, accepted) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let database = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.clone()),
                handler: Some(Arc::new(HeldRead {
                    entered,
                    release: Mutex::new(released),
                })),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open owner");
    database
        .execute(
            "CREATE TABLE retained (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create durable table");
    let reader = ReadSession::open_in_runtime_dir(
        &path,
        ReadSessionOptions::default(),
        Some(runtime.clone()),
    )
    .expect("open owner route");
    let reading = std::thread::spawn(move || reader.request_owner("proof", b"answer"));
    accepted.recv().expect("owner accepted request");

    drop(database);
    release.send(()).expect("release request");
    assert_eq!(
        reading
            .join()
            .expect("reader joins")
            .expect("accepted read completes"),
        b"answer"
    );
    let idle =
        ReadSession::open_in_runtime_dir(&path, ReadSessionOptions::default(), Some(runtime))
            .expect("released store opens through its committed file");
    assert_eq!(
        idle.execute("SELECT COUNT(*) FROM retained", &HashMap::new())
            .expect("read committed table")
            .rows[0][0],
        contextdb_core::Value::Int64(0)
    );
}

#[test]
fn callback_close_refuses_its_pending_ack_and_redelivers_after_reopen() {
    let root = tempfile::tempdir().expect("task-scoped store directory");
    let path = root.path().join("callback-close.db");
    let database = Arc::new(Database::open(&path).expect("open owner"));
    for sql in [
        "CREATE TABLE events (id INTEGER PRIMARY KEY)",
        "CREATE EVENT TYPE inserted WHEN INSERT ON events",
        "CREATE SINK callback_sink TYPE callback",
        "CREATE ROUTE callback_route EVENT inserted TO callback_sink",
        "INSERT INTO events VALUES (1)",
    ] {
        database.execute(sql, &HashMap::new()).expect(sql);
    }
    assert_eq!(database.sink_metrics_for_test("callback_sink").queued, 1);

    let (closed, close_result) = mpsc::channel();
    let owner = Arc::downgrade(&database);
    database
        .register_sink("callback_sink", None, move |_| {
            let result = owner.upgrade().expect("owner lives").close();
            closed.send(result).expect("report callback close");
            Ok(())
        })
        .expect("register first delivery");
    close_result
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("callback initiated close")
        .expect("callback close succeeds");
    drop(database);

    let reopened = Database::open(&path).expect("reopen owner after dispatcher exit");
    assert_eq!(
        reopened.sink_metrics_for_test("callback_sink").queued,
        1,
        "the unacknowledged event remains durably queued"
    );
    let (delivered, received) = mpsc::channel();
    reopened
        .register_sink("callback_sink", None, move |_| {
            delivered.send(()).expect("report replay");
            Ok(())
        })
        .expect("register replay delivery");
    received
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("queued event is redelivered");
    reopened.close().expect("close replay owner");
    let final_open = Database::open(&path).expect("verify durable acknowledgement");
    assert_eq!(final_open.sink_metrics_for_test("callback_sink").queued, 0);
}

#[test]
fn concurrent_callback_and_ordinary_close_complete_one_shutdown_and_preserve_pending_delivery() {
    use contextdb_core::{Value, read_contract::ReadRoute};
    use contextdb_engine::{OwnerReadTestHooks, QuiescentStopPhase};

    let root = tempfile::tempdir().expect("task-scoped store directory");
    let path = root.path().join("concurrent-close.db");
    let phases = Arc::new(Mutex::new(Vec::new()));
    let writes_stopped = Arc::new(AtomicUsize::new(0));
    let (parked, reached_park) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let released = Arc::new(Mutex::new(released));
    let phases_for_hook = Arc::clone(&phases);
    let writes_stopped_for_hook = Arc::clone(&writes_stopped);
    let released_for_hook = Arc::clone(&released);
    let stop_observer = Arc::new(move |phase: QuiescentStopPhase| {
        phases_for_hook.lock().expect("phase ledger").push(phase);
        if phase == QuiescentStopPhase::WritesStopped
            && writes_stopped_for_hook.fetch_add(1, Ordering::SeqCst) == 0
        {
            parked.send(()).expect("announce first shutdown park");
            released_for_hook
                .lock()
                .expect("release gate")
                .recv()
                .expect("release first shutdown owner");
        }
    });
    let database = Arc::new(
        Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                owner_reads: OwnerReadConfig {
                    test_hooks: Some(OwnerReadTestHooks {
                        clock: Arc::new(
                            contextdb_engine::local_transport::ManualDeadlineClock::default(),
                        ),
                        drain_started: mpsc::channel().0,
                        active_request_registered: None,
                        accept_thread_exited: None,
                        accept_thread_joined: None,
                        service_dropped: None,
                        quiescent_stop: Some(stop_observer),
                    }),
                    ..OwnerReadConfig::default()
                },
                ..DatabaseOpenOptions::default()
            },
        )
        .expect("open owner"),
    );
    for sql in [
        "CREATE TABLE events (id INTEGER PRIMARY KEY)",
        "CREATE EVENT TYPE inserted WHEN INSERT ON events",
        "CREATE SINK callback_sink TYPE callback",
        "CREATE ROUTE callback_route EVENT inserted TO callback_sink",
        "INSERT INTO events VALUES (1)",
    ] {
        database.execute(sql, &HashMap::new()).expect(sql);
    }
    assert_eq!(database.sink_metrics_for_test("callback_sink").queued, 1);

    let (callback_result, callback_closed) = mpsc::channel();
    let callback_owner = Arc::downgrade(&database);
    database
        .register_sink("callback_sink", None, move |_| {
            callback_result
                .send(callback_owner.upgrade().expect("owner lives").close())
                .expect("report callback close");
            Ok(())
        })
        .expect("start pending delivery");
    reached_park.recv().expect("callback close stops writes");
    assert_eq!(
        phases.lock().expect("phase ledger").as_slice(),
        &[QuiescentStopPhase::WritesStopped],
        "quiescence has not been published while the shutdown owner is parked"
    );

    let ordinary_owner = Arc::clone(&database);
    let (ordinary_started, ordinary_start) = mpsc::channel();
    let ordinary = std::thread::spawn(move || {
        ordinary_started.send(()).expect("announce second closer");
        ordinary_owner.close()
    });
    ordinary_start.recv().expect("ordinary closer starts");
    release.send(()).expect("release first shutdown owner");
    callback_closed
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("callback close completes")
        .expect("callback close succeeds");
    ordinary
        .join()
        .expect("ordinary closer joins")
        .expect("ordinary close succeeds");
    assert_eq!(
        writes_stopped.load(Ordering::SeqCst),
        1,
        "one shutdown owner publishes the close boundary exactly once"
    );
    let phases = phases.lock().expect("phase ledger").clone();
    for phase in [
        QuiescentStopPhase::WritesStopped,
        QuiescentStopPhase::Published,
        QuiescentStopPhase::AcceptedDrained,
    ] {
        assert_eq!(
            phases.iter().filter(|observed| **observed == phase).count(),
            1,
            "shutdown phase {phase:?} is published once: {phases:?}"
        );
    }
    drop(database);

    let fresh = ReadSession::open(&path).expect("fresh bounded reader opens after both closes");
    assert_eq!(fresh.route(), ReadRoute::File);
    assert_eq!(
        fresh
            .execute("SELECT id FROM events", &HashMap::new())
            .expect("committed row remains readable")
            .rows,
        vec![vec![Value::Int64(1)]]
    );
    drop(fresh);
    let reopened = Database::open(&path).expect("reopen pending delivery");
    assert_eq!(
        reopened.sink_metrics_for_test("callback_sink").queued,
        1,
        "closing inside its delivery callback never acknowledges that delivery"
    );
    reopened.close().expect("close reopened owner");
}

#[test]
fn ordinary_close_waits_until_callback_dispatcher_releases_writer_resources() {
    let root = tempfile::tempdir().expect("task-scoped store directory");
    let path = root.path().join("callback-resource-close.db");
    let database = Arc::new(Database::open(&path).expect("open owner"));
    for sql in [
        "CREATE TABLE events (id INTEGER PRIMARY KEY)",
        "CREATE EVENT TYPE inserted WHEN INSERT ON events",
        "CREATE SINK callback_sink TYPE callback",
        "CREATE ROUTE callback_route EVENT inserted TO callback_sink",
        "INSERT INTO events VALUES (1)",
    ] {
        database.execute(sql, &HashMap::new()).expect(sql);
    }
    let (callback_closed_tx, callback_closed_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let owner = Arc::downgrade(&database);
    database
        .register_sink("callback_sink", None, move |_| {
            let result = owner.upgrade().expect("owner lives").close();
            callback_closed_tx
                .send(result)
                .expect("announce callback close");
            release_rx
                .lock()
                .expect("release gate")
                .recv()
                .expect("release dispatcher");
            Ok(())
        })
        .expect("start pending delivery");
    callback_closed_rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("self-close returns without waiting on dispatcher")
        .expect("callback close succeeds");
    let (milestone_tx, milestone_rx) = mpsc::channel();
    let follower_waiting_tx = milestone_tx.clone();
    database.__set_close_follower_observer_for_test(Some(Arc::new(move || {
        follower_waiting_tx
            .send("waiting")
            .expect("announce follower wait");
    })));
    let ordinary_owner = Arc::clone(&database);
    let (ordinary_done_tx, ordinary_done_rx) = mpsc::channel();
    let ordinary = std::thread::spawn(move || {
        let result = ordinary_owner.close();
        ordinary_done_tx
            .send(result)
            .expect("report ordinary close");
        milestone_tx
            .send("done")
            .expect("announce ordinary completion");
    });
    let first_milestone = milestone_rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("ordinary closer reaches a milestone");
    let ordinary_finished_while_parked = ordinary_done_rx.try_recv();
    release_tx.send(()).expect("release callback dispatcher");
    assert_eq!(
        first_milestone, "waiting",
        "ordinary closer must wait for cleanup"
    );
    assert!(
        matches!(
            ordinary_finished_while_parked,
            Err(mpsc::TryRecvError::Empty)
        ),
        "ordinary close cannot complete while dispatcher still owns writer resources"
    );
    ordinary_done_rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("ordinary closer finishes after cleanup")
        .expect("ordinary close succeeds");
    ordinary.join().expect("ordinary closer joins");
    drop(database);
    let reopened = Database::open(&path).expect("immediate writable reopen");
    assert_eq!(
        reopened.sink_metrics_for_test("callback_sink").queued,
        1,
        "self-close preserves pending delivery"
    );
    reopened.close().expect("close reopened owner");
}

#[test]
fn two_ordinary_close_callers_wait_for_one_complete_shutdown() {
    use contextdb_core::{Value, read_contract::ReadRoute};
    use contextdb_engine::{OwnerReadTestHooks, QuiescentStopPhase};

    let root = tempfile::tempdir().expect("task-scoped store directory");
    let path = root.path().join("two-closers.db");
    let phases = Arc::new(Mutex::new(Vec::new()));
    let (parked, reached_park) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let released = Arc::new(Mutex::new(released));
    let phases_for_hook = Arc::clone(&phases);
    let writes_stopped = Arc::new(AtomicUsize::new(0));
    let writes_stopped_for_hook = Arc::clone(&writes_stopped);
    let stop_observer = Arc::new(move |phase: QuiescentStopPhase| {
        phases_for_hook.lock().expect("phase ledger").push(phase);
        if phase == QuiescentStopPhase::WritesStopped
            && writes_stopped_for_hook.fetch_add(1, Ordering::SeqCst) == 0
        {
            parked.send(()).expect("announce stopped writes");
            released
                .lock()
                .expect("release gate")
                .recv()
                .expect("release shutdown owner");
        }
    });
    let database = Arc::new(
        Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                owner_reads: OwnerReadConfig {
                    test_hooks: Some(OwnerReadTestHooks {
                        clock: Arc::new(
                            contextdb_engine::local_transport::ManualDeadlineClock::default(),
                        ),
                        drain_started: mpsc::channel().0,
                        active_request_registered: None,
                        accept_thread_exited: None,
                        accept_thread_joined: None,
                        service_dropped: None,
                        quiescent_stop: Some(stop_observer),
                    }),
                    ..OwnerReadConfig::default()
                },
                ..DatabaseOpenOptions::default()
            },
        )
        .expect("open owner"),
    );
    database
        .execute(
            "CREATE TABLE retained (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create durable table");
    database
        .execute("INSERT INTO retained VALUES (7)", &HashMap::new())
        .expect("commit durable row");
    let (follower_entered, follower_observed) = mpsc::channel();
    database.__set_close_follower_observer_for_test(Some(Arc::new(move || {
        follower_entered.send(()).expect("announce close follower");
    })));

    let first = Arc::clone(&database);
    let first_close = std::thread::spawn(move || first.close());
    reached_park
        .recv()
        .expect("first closer parks after stopping writes");
    assert_eq!(
        phases.lock().expect("phase ledger").as_slice(),
        &[QuiescentStopPhase::WritesStopped],
        "no later shutdown phase occurs while the first closer is parked"
    );
    let second = Arc::clone(&database);
    let second_close = std::thread::spawn(move || second.close());
    follower_observed
        .recv()
        .expect("second caller reaches close while the first is parked");
    assert_eq!(
        phases.lock().expect("phase ledger").as_slice(),
        &[QuiescentStopPhase::WritesStopped],
        "the follower cannot publish quiescence or close resources early"
    );
    release.send(()).expect("release first closer");
    first_close
        .join()
        .expect("first close thread joins")
        .expect("first close succeeds");
    second_close
        .join()
        .expect("second close thread joins")
        .expect("second close succeeds");
    database.__set_close_follower_observer_for_test(None);
    let phases = phases.lock().expect("phase ledger").clone();
    for phase in [
        QuiescentStopPhase::WritesStopped,
        QuiescentStopPhase::Published,
        QuiescentStopPhase::AcceptedDrained,
    ] {
        assert_eq!(
            phases.iter().filter(|observed| **observed == phase).count(),
            1,
            "one completed close publishes {phase:?} once: {phases:?}"
        );
    }
    drop(database);
    let fresh = ReadSession::open(&path).expect("fresh reader opens after both close calls");
    assert_eq!(fresh.route(), ReadRoute::File);
    assert_eq!(
        fresh
            .execute("SELECT id FROM retained", &HashMap::new())
            .expect("read durable row")
            .rows,
        vec![vec![Value::Int64(7)]]
    );
}

/// Every owner socket standing anywhere under `runtime`.
#[cfg(unix)]
fn owner_sockets(runtime: &std::path::Path) -> Vec<std::path::PathBuf> {
    use std::os::unix::fs::FileTypeExt;
    let mut sockets = Vec::new();
    let mut pending = vec![runtime.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_socket() {
                sockets.push(entry.path());
            }
        }
    }
    sockets
}

/// Owner close stops admitting, drains what it admitted, and only then removes
/// the channel it created: a draining owner keeps its exact channel until
/// admitted work leaves, refusing new work over it, and a fresh reader still
/// reads the committed file.
#[cfg(unix)]
#[test]
fn a_draining_owner_keeps_its_exact_channel_until_admitted_work_leaves() {
    use contextdb_engine::OwnerReadTestHooks;
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().expect("task-scoped store directory");
    let path = root.path().join("owner-drain-channel.db");
    let runtime = root.path().join("runtime");
    std::fs::create_dir(&runtime).expect("create runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("secure runtime directory");
    let (entered, accepted) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let (drain_started, drain_seen) = mpsc::channel();
    let database = Arc::new(
        Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                owner_reads: OwnerReadConfig {
                    runtime_dir: Some(runtime.clone()),
                    handler: Some(Arc::new(HeldRead {
                        entered,
                        release: Mutex::new(released),
                    })),
                    test_hooks: Some(OwnerReadTestHooks {
                        clock: Arc::new(
                            contextdb_engine::local_transport::ManualDeadlineClock::default(),
                        ),
                        drain_started,
                        active_request_registered: None,
                        accept_thread_exited: None,
                        accept_thread_joined: None,
                        service_dropped: None,
                        quiescent_stop: None,
                    }),
                    ..OwnerReadConfig::default()
                },
                ..DatabaseOpenOptions::default()
            },
        )
        .expect("open owner"),
    );
    database
        .execute(
            "CREATE TABLE retained (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create durable table");
    database
        .execute("INSERT INTO retained VALUES (7)", &HashMap::new())
        .expect("commit a row");
    let created = owner_sockets(&runtime);
    assert_eq!(created.len(), 1, "a serving owner publishes one channel");

    let reader = ReadSession::open_in_runtime_dir(
        &path,
        ReadSessionOptions::default(),
        Some(runtime.clone()),
    )
    .expect("open owner route");
    let reading = std::thread::spawn(move || reader.request_owner("proof", b"held"));
    accepted.recv().expect("accepted request enters");
    let closing = Arc::clone(&database);
    let closer = std::thread::spawn(move || closing.close());
    drain_seen.recv().expect("owner close begins draining");

    assert_eq!(
        owner_sockets(&runtime),
        created,
        "a draining owner keeps its exact channel until admitted work leaves"
    );
    let fresh = ReadSession::open_in_runtime_dir(
        &path,
        ReadSessionOptions::default(),
        Some(runtime.clone()),
    )
    .expect("a fresh reader opens while the owner drains");
    assert!(
        fresh.request_owner("proof", b"probe").is_err(),
        "a draining owner admits no new work"
    );
    assert_eq!(
        fresh
            .execute("SELECT id FROM retained", &HashMap::new())
            .expect("the fresh reader reads the committed file")
            .rows[0][0],
        contextdb_core::Value::Int64(7)
    );
    drop(fresh);

    release.send(()).expect("complete accepted request");
    assert_eq!(
        reading
            .join()
            .expect("read joins")
            .expect("accepted read succeeds"),
        b"held"
    );
    closer.join().expect("close joins").expect("close succeeds");
    assert!(
        owner_sockets(&runtime).is_empty(),
        "once admitted work has left, close removes the channel it created"
    );
}
