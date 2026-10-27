//! Independent baseline guards. Each fixture runs in its own private runtime process.
use super::*;
use crate::local_transport::ManualDeadlineClock;
use crate::persistence::read_persistence_test_scaffold::install_reader_join_wait_hook_for_test;
use crate::read_session::{OwnerReadTestHooks, QuiescentStopPhase};
use contextdb_core::read_contract::{
    DeadlineClock, OwnerServingReason, OwnerServingState, ReadFailureDetail, ReadFailureKind,
    ReadFailureLimit, ReadRoute,
};
use std::os::unix::fs::PermissionsExt;
use std::sync::mpsc;
use std::time::Duration;

const WATCHDOG: Duration = Duration::from_secs(3);
const ACTOR: &str = "database::coexistence_guards::coexistence_actor";
fn launch(case: &str) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("runtime");
    std::fs::create_dir(&root).unwrap();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let supplied = root.join("contextdb");
    std::fs::create_dir(&supplied).unwrap();
    std::fs::set_permissions(&supplied, std::fs::Permissions::from_mode(0o700)).unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", ACTOR, "--ignored", "--nocapture"])
        .env("COEXISTENCE_CASE", case)
        .env("XDG_RUNTIME_DIR", &root)
        .env("COEXISTENCE_SCRATCH", dir.path())
        .output()
        .unwrap();
    eprintln!("{}", String::from_utf8_lossy(&output.stdout));
    eprintln!("{}", String::from_utf8_lossy(&output.stderr));
    assert!(
        output.status.success(),
        "{case}: independent actor failed: {}",
        output.status
    );
}
macro_rules! cases { ($($name:ident),+ $(,)?) => {$ (
    #[test] fn $name() { launch(stringify!($name)); }
)+}; }
cases!(
    red26_draining_open_deadline,
    red26_cancelled_open_cleanup,
    red26_serving_owner_held_root_control,
    red26_draining_uncontended_control,
    red26_draining_healthy_release_control,
    red26_exact_boundary_control,
    red27_turnover_deadline,
    red27_admission_precedes_publication,
    red27_turnover_healthy_release_control,
    red27_cursor_handoff_deadline,
    red27_cursor_handoff_cancel,
    red27_cursor_healthy_release_control,
    red27_cursor_finish_deadline,
    red27_cursor_finish_cancel,
    red28_writer_default_share,
    red28_writer_dirty_preserves_clean,
    red29_live_serving_status,
    red29_owner_serving_status,
    red29_file_status_control,
    red29_live_final_window,
    red29_owner_reader_charge,
    red29_empty_window_control,
    red29_policy_turnover_control
);

#[test]
#[ignore = "owned child entry; invoked by independent fixture launcher"]
fn coexistence_actor() {
    let case = std::env::var("COEXISTENCE_CASE").unwrap();
    if case == "red27_admission_precedes_publication" {
        registration_order();
    } else if case == "red26_cancelled_open_cleanup" {
        cancelled_cleanup();
    } else if case.starts_with("red26") || case.starts_with("red27") {
        coordination(&case);
    } else {
        cache(&case);
    }
}
fn runtime() -> std::path::PathBuf {
    PathBuf::from(std::env::var_os("XDG_RUNTIME_DIR").unwrap()).join("contextdb")
}
fn options() -> crate::DatabaseOpenOptions {
    crate::DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        owner_reads: crate::OwnerReadConfig {
            runtime_dir: Some(runtime()),
            ..Default::default()
        },
        ..Default::default()
    }
}
fn path() -> PathBuf {
    PathBuf::from(std::env::var_os("COEXISTENCE_SCRATCH").unwrap()).join("fixture.db")
}
fn typed(error: &Error, cancel: bool, turnover: bool) -> bool {
    if cancel {
        return matches!(error, Error::ReadCancelled);
    }
    matches!(error, Error::ReadFailure(f) if
        if turnover { f.kind() == ReadFailureKind::OwnerNotServing }
        else { matches!(f.detail(), ReadFailureDetail::OwnerLimitExceeded(d) if d.limit == ReadFailureLimit::ActiveMs && d.value == 5000) })
}
struct FinishHolder(Arc<std::fs::File>);
impl crate::read_session::ReadSessionTestObserver for FinishHolder {
    fn observe_event(&self, event: crate::read_session::ReadSessionEvent) {
        if matches!(
            event,
            crate::read_session::ReadSessionEvent::BeforeOwnerCursorHandoffRegistration
        ) {
            fs2::FileExt::try_lock_exclusive(self.0.as_ref())
                .expect("fixture acquires later finish root");
        }
    }
}
#[expect(
    clippy::needless_borrows_for_generic_args,
    reason = "Keep the explicit runtime-directory borrow at the fixture opener."
)]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
#[expect(
    clippy::unused_unit,
    reason = "Keep the explicit unit result at the operation observation boundary."
)]
fn coordination(case: &str) {
    let clock = Arc::new(ManualDeadlineClock::at(0));
    let owner_clock = Arc::new(ManualDeadlineClock::at(0));
    let (stopped_tx, stopped) = mpsc::channel();
    let release = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let gate = release.clone();
    let (drain_tx, _drain) = mpsc::channel();
    let mut config = options();
    config.owner_reads.test_hooks = Some(OwnerReadTestHooks {
        clock: owner_clock.clone(),
        drain_started: drain_tx,
        active_request_registered: None,
        accept_thread_exited: None,
        accept_thread_joined: None,
        service_dropped: None,
        quiescent_stop: Some(Arc::new(move |phase| {
            if phase == QuiescentStopPhase::AdmissionStopped {
                stopped_tx.send(()).unwrap();
                let mut ready = gate.0.lock().unwrap();
                while !*ready {
                    ready = gate.1.wait(ready).unwrap();
                }
            }
        })),
    });
    let db = Arc::new(Database::open_with_options(path(), config).unwrap());
    db.execute(
        "CREATE TABLE entries (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..8 {
        db.execute(
            &format!("INSERT INTO entries VALUES ({id})"),
            &HashMap::new(),
        )
        .unwrap();
    }
    assert_eq!(db.owner_read_status().state, OwnerServingState::Serving);
    let mut reader_options = crate::ReadSessionOptions::default();
    let finish_case = case.contains("finish");
    if case.contains("cursor") {
        reader_options.limits.cursor_page_rows = 1;
    }
    let root = PathBuf::from(std::env::var_os("XDG_RUNTIME_DIR").unwrap());
    let holder = Arc::new(std::fs::File::open(&root).unwrap());
    let cursor_case = case.contains("cursor");
    let turnover = case.contains("turnover");
    let serving = case.contains("serving_owner") || cursor_case;
    let retained = if cursor_case || turnover {
        let r = crate::ReadSession::with_runtime_directory_for_test(&runtime(), || {
            if finish_case {
                crate::ReadSession::open_with_clock_and_observer_for_test(
                    path(),
                    reader_options.clone(),
                    clock.clone(),
                    Arc::new(FinishHolder(holder.clone())),
                )
            } else {
                crate::ReadSession::open_with_clock_for_test(
                    path(),
                    reader_options.clone(),
                    clock.clone(),
                )
            }
        })
        .unwrap();
        assert_eq!(r.route(), ReadRoute::Owner);
        Some(r)
    } else {
        None
    };
    let close = if !serving {
        let d = db.clone();
        let t = std::thread::spawn(move || d.close());
        stopped
            .recv_timeout(WATCHDOG)
            .expect("real AdmissionStopped transition");
        assert_eq!(db.owner_read_status().state, OwnerServingState::NotServing);
        assert_eq!(
            db.owner_read_status().reason,
            Some(OwnerServingReason::ShutdownDraining)
        );
        let resources = db.read_route_resources_for_test().unwrap();
        eprintln!("REAL_DRAINING_OWNER {resources:?}");
        Some(t)
    } else {
        None
    };
    let held = !case.contains("uncontended");
    if held && !finish_case {
        fs2::FileExt::try_lock_exclusive(holder.as_ref()).expect("private root held by fixture");
    }
    let healthy = case.contains("control");
    let cancellation = contextdb_core::read_contract::OwnerReadCancellation::new();
    let token = cancellation.clone();
    let read_clock = clock.clone();
    let (entered_tx, entered) = mpsc::channel();
    let (drive_tx, drive) = mpsc::channel();
    let (driven_tx, driven) = mpsc::channel();
    let (answer_tx, answer) = mpsc::channel();
    let hook = held && !serving;
    let cancel = case.ends_with("cancel");
    let expire = if turnover {
        reader_options.timeouts.routing_retry_ms
    } else {
        5001
    };
    let worker = std::thread::spawn(move || {
        // Hook only schedules the existing first contended root path. It does not
        // replace acquisition, choose the result, or release the holder.
        if hook || cursor_case {
            let c = read_clock.clone();
            let t = token.clone();
            install_reader_join_wait_hook_for_test(Box::new(move || {
                entered_tx.send(()).unwrap();
                drive.recv().unwrap();
                if healthy
                    && std::env::var("COEXISTENCE_CASE")
                        .unwrap()
                        .contains("boundary")
                {
                    c.advance_to(5000);
                }
                if !healthy {
                    if cancel {
                        t.cancel();
                    } else {
                        c.advance_to(expire);
                    }
                }
                driven_tx.send(()).unwrap();
            }));
        }
        let result = if let Some(ref reader) = retained {
            if cursor_case {
                reader
                    .open_cursor_with_cancellation(
                        "SELECT id FROM entries ORDER BY id",
                        &HashMap::new(),
                        &token,
                    )
                    .map(|cursor| {
                        drop(cursor);
                        ()
                    })
            } else {
                reader
                    .execute_with_cancellation(
                        "SELECT id FROM entries ORDER BY id",
                        &HashMap::new(),
                        &token,
                    )
                    .map(|_| ())
            }
        } else {
            crate::ReadSession::with_runtime_directory_for_test(&runtime(), || {
                crate::ReadSession::open_with_clock_for_test(path(), reader_options, read_clock)
            })
            .map(|reader| {
                assert_eq!(
                    reader.route(),
                    if serving {
                        ReadRoute::Owner
                    } else {
                        ReadRoute::File
                    }
                );
                drop(reader);
            })
        };
        answer_tx.send((result, retained)).unwrap();
    });
    let wait_result = if hook || cursor_case {
        entered.recv_timeout(WATCHDOG).map(|()| {
            drive_tx.send(()).unwrap();
            driven.recv_timeout(WATCHDOG).unwrap();
            if healthy {
                fs2::FileExt::unlock(holder.as_ref()).unwrap();
            }
            answer.recv_timeout(WATCHDOG)
        })
    } else {
        Ok(answer.recv_timeout(WATCHDOG))
    };
    // Preserve the completion-under-hold verdict, then unconditionally release,
    // join opener and stop the genuine owner before any regression assertion.
    fs2::FileExt::unlock(holder.as_ref()).unwrap();
    drop(holder);
    let (before_release, result) = match wait_result {
        Ok(Ok(r)) => (true, Some(r)),
        _ => (false, answer.recv_timeout(WATCHDOG).ok()),
    };
    worker.join().unwrap();
    let (result, retained_reader) = match result {
        Some((r, session)) => (Some(r), session),
        None => (None, None),
    };
    *release.0.lock().unwrap() = true;
    release.1.notify_all();
    if let Some(close) = close {
        close.join().unwrap().unwrap();
    } else {
        db.close().unwrap();
    }
    assert_eq!(
        owner_clock.now_ms(),
        0,
        "owner drain deadline never advanced"
    );
    eprintln!(
        "COORDINATION_COMPLETION case={case} before_release={before_release} result={result:?} clock={} cancelled={}",
        clock.now_ms(),
        cancellation.is_cancelled()
    );
    assert!(
        before_release,
        "{case}: required operation did not return while fixture holder remained held"
    );
    let result = result.expect("completed operation");
    if healthy {
        result.unwrap();
    } else {
        assert!(
            result.as_ref().is_err_and(|e| typed(e, cancel, turnover)),
            "original typed refusal required: {result:?}"
        );
    }
    let recovered = retained_reader.map(|reader| {
        let fresh = reader
            .execute("SELECT id FROM entries ORDER BY id", &HashMap::new())
            .map(|rows| rows.rows().len());
        drop(reader);
        fresh
    });
    eprintln!("POST_RELEASE_SESSION case={case} result={recovered:?}");
    if let Some(fresh) = recovered {
        assert_eq!(
            fresh.expect("same session works after refusal and explicit release"),
            8
        );
    }
}
struct CarrierRetirement(std::sync::mpsc::Sender<()>);
impl crate::owner_read::OwnerBoundedExecutionObserver for CarrierRetirement {
    fn before_operation(
        &self,
        _operation: crate::owner_read::OwnerBoundedOperation,
        _limits: contextdb_core::read_contract::ReadLimits,
        _cancellation: contextdb_core::read_contract::OwnerReadCancellation,
    ) {
    }
    fn before_work(
        &self,
        _source: crate::executor::bounded_read_test_support::TestWorkSource,
        _completed: u64,
    ) {
    }
    fn cancellation_observed(&self, _completed: u64) {}
    fn carrier_request_retired(&self) {
        let _ = self.0.send(());
    }
}
#[expect(
    clippy::needless_borrows_for_generic_args,
    reason = "Keep the explicit runtime-directory borrow at the fixture opener."
)]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn cache(case: &str) {
    let mut config = options();
    config.memory_limit = Some(128 * 1024 * 1024);
    let (db, retired) = if case == "red29_owner_reader_charge" {
        let (sent, retired) = mpsc::channel();
        let db = crate::owner_read::OwnerServiceSpec::with_execution_observer_during_start_for_test(
            Arc::new(CarrierRetirement(sent)),
            || Database::open_with_options(path(), config).unwrap(),
        );
        (db, Some(retired))
    } else {
        (Database::open_with_options(path(), config).unwrap(), None)
    };
    db.execute(
        "CREATE TABLE entries (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("INSERT INTO entries VALUES (1)", &HashMap::new())
        .unwrap();
    let limits = contextdb_core::read_contract::ReadLimits::default();
    if case == "red29_live_final_window" || case == "red29_empty_window_control" {
        let reader = db.read_session(limits).unwrap();
        let persistence = db.persistence.as_ref().unwrap();
        let (answer, fraction) = persistence
            .with_db(|raw| {
                Ok(raw.with_read_cache_page_window_for_test(|| {
                    if case == "red29_empty_window_control" {
                        Ok(None)
                    } else {
                        reader
                            .execute("SELECT id FROM entries", &HashMap::new())
                            .map(Some)
                    }
                }))
            })
            .unwrap();
        let result = answer.unwrap();
        if let Some(rows) = &result {
            assert_eq!(rows.rows(), vec![vec![Value::Int64(1)]]);
        }
        drop(result);
        let shown = reader
            .execute("SHOW READ_CACHE_SHARE", &HashMap::new())
            .unwrap();
        let reported = shown.rows()[0][4].clone();
        drop(shown);
        drop(reader);
        db.close().unwrap();
        eprintln!("FINAL_WINDOW case={case} actual={fraction:?} reported={reported:?}");
        if case == "red29_empty_window_control" {
            assert_eq!(fraction, None);
            assert_eq!(reported, Value::Null);
        } else {
            let measured = fraction
                .expect("GUARD_BLOCKED: known query must produce a nonempty actual serving window");
            assert_eq!(reported, Value::Float64(measured * 100.0));
        }
    } else if case == "red29_owner_reader_charge" {
        let reader = crate::ReadSession::with_runtime_directory_for_test(&runtime(), || {
            crate::ReadSession::open(path())
        })
        .unwrap();
        let retired = retired
            .as_ref()
            .expect("owner charge fixture observes retirement");
        // Open authenticates with an initial OwnerStatus. A response frame
        // does not retire the carrier's original request invoices.
        retired
            .recv_timeout(WATCHDOG)
            .expect("open status carrier retired");
        let before = reader.charged_bytes().unwrap();
        retired
            .recv_timeout(WATCHDOG)
            .expect("baseline status carrier retired");
        let declared_before = db.declared_read_charged_bytes();
        let reservation = crate::memory_accounting::OwnedMemoryReservation::try_new_for_native(
            crate::memory_accounting::AccountantHandle::clone(&db.finalization.accountant),
            512 * 1024,
            "test",
            "unrelated writer reservation",
            "original writer",
        )
        .unwrap();
        let declared_after = db.declared_read_charged_bytes();
        let after = reader.charged_bytes().unwrap();
        retired
            .recv_timeout(WATCHDOG)
            .expect("final status carrier retired");
        drop(reservation);
        drop(reader);
        db.close().unwrap();
        eprintln!(
            "READER_CHARGE before={before} after={after} declared_before={declared_before} declared_after={declared_after} unrelated_writer=524288"
        );
        assert_eq!(
            declared_before, declared_after,
            "GUARD_BLOCKED: unrelated reservation must not charge reader owner"
        );
        assert_eq!(
            after, before,
            "owner reader reporting must exclude unrelated writer bytes"
        );
    } else if case == "red29_policy_turnover_control" {
        let reader = crate::ReadSession::with_runtime_directory_for_test(&runtime(), || {
            crate::ReadSession::open(path())
        })
        .unwrap();
        reader.set_read_cache_share(Some(20)).unwrap();
        assert_eq!(reader.options().read_cache_share, Some(20));
        db.close().unwrap();
        reader
            .execute("SELECT id FROM entries", &HashMap::new())
            .unwrap();
        assert_eq!(reader.route(), ReadRoute::File);
        let status = reader.read_cache_share_status().unwrap();
        assert_eq!(status.declared, Some(20));
        assert_eq!(
            status.effective_bytes,
            crate::persistence::redb_reader_cache_bytes(status.budget_bytes, Some(20))
        );
    } else if case == "red28_writer_default_share" {
        let actual = db.writer_cache_for_test();
        let budget = db
            .memory_limit()
            .unwrap_or_else(crate::memory_accounting::effective_process_ceiling);
        let expected = crate::persistence::redb_reader_cache_bytes(budget, None);
        db.close().unwrap();
        eprintln!("WRITER_SHARE actual={actual:?} budget={budget} expected={expected}");
        assert_eq!(
            actual.3, expected,
            "writer serving capacity must use declared reader-share derivation"
        );
    } else if case == "red28_writer_dirty_preserves_clean" {
        // Dirty buffers must not make a separate clean page cease to be resident.
        let before = db.__writer_cache_stats_for_test().unwrap();
        db.execute("SELECT id FROM entries", &HashMap::new())
            .unwrap();
        let warm = db.__writer_cache_stats_for_test().unwrap();
        db.execute("INSERT INTO entries VALUES (2)", &HashMap::new())
            .unwrap();
        let after = db.__writer_cache_stats_for_test().unwrap();
        db.close().unwrap();
        eprintln!("DIRTY_CACHE before={before:?} warm={warm:?} after={after:?}");
        assert!(
            after.3 >= warm.3,
            "dirty construction cannot evict the clean working set"
        );
    } else {
        let actual_capacity = db
            .persistence
            .as_ref()
            .unwrap()
            .with_db(|raw| Ok(raw.installed_cache_capacity_for_test()))
            .unwrap();
        let actual_index = db.__writer_cache_index_bytes_for_test().unwrap();
        let file = case == "red29_file_status_control";
        let live = case == "red29_live_serving_status";
        let reader = if live {
            db.read_session(limits).unwrap()
        } else {
            if file {
                db.close().unwrap();
            }
            crate::ReadSession::with_runtime_directory_for_test(&runtime(), || {
                crate::ReadSession::open(path())
            })
            .unwrap()
        };
        assert_eq!(
            reader.route(),
            if file {
                ReadRoute::File
            } else {
                ReadRoute::Owner
            }
        );
        let rows = reader
            .execute("SELECT id FROM entries", &HashMap::new())
            .unwrap();
        assert_eq!(rows.rows(), vec![vec![Value::Int64(1)]]);
        let status = reader.read_cache_share_status().unwrap();
        let owner_resident = if case == "red29_owner_serving_status" {
            let shown = reader
                .execute("SHOW READ_CACHE_SHARE", &HashMap::new())
                .unwrap();
            let resident = shown.rows()[0][4].clone();
            drop(shown);
            Some(resident)
        } else {
            None
        };
        drop(reader);
        if !file {
            db.close().unwrap();
        }
        eprintln!(
            "SERVING_STATUS case={case} returned={status:?} actual_capacity={actual_capacity} actual_index={actual_index}"
        );
        if let Some(resident) = owner_resident {
            eprintln!(
                "OWNER_FINAL_WINDOW case={case} reported={resident:?} expected=Float64(100.0)"
            );
            assert_eq!(
                resident,
                Value::Float64(100.0),
                "owner channel must transport final measured serving-cache residency"
            );
        }
        assert!(
            status.prepaid_index_bytes.is_some() && status.held_page_bytes.is_some(),
            "returned status must inspect actual serving cache"
        );
        if !file {
            assert_eq!(status.effective_bytes, actual_capacity);
            assert_eq!(status.prepaid_index_bytes, Some(actual_index));
        }
    }
}

fn plain_store() {
    let db = Database::open_with_options(path(), options()).unwrap();
    db.close().unwrap();
}
fn direct(clock: Arc<ManualDeadlineClock>) -> crate::direct_file_reader::DirectReaderConfig {
    let mut config = crate::direct_file_reader::DirectReaderConfig::new(
        contextdb_core::read_contract::ReadLimits::default(),
        clock,
    );
    config.runtime_dir = Some(runtime());
    config
}
fn records() -> Vec<PathBuf> {
    fn visit(p: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(p).unwrap() {
            let p = entry.unwrap().path();
            if p.is_dir() {
                visit(&p, out);
            } else if p
                .extension()
                .is_some_and(|e| e == "reader" || e == "snapshot")
            {
                out.push(p);
            }
        }
    }
    let mut found = Vec::new();
    visit(
        &PathBuf::from(std::env::var_os("XDG_RUNTIME_DIR").unwrap()),
        &mut found,
    );
    found.sort();
    found
}
fn physical_holds() -> (bool, bool) {
    use crate::persistence::physical_admission as p;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path())
        .unwrap();
    (
        p::held_against(&file, p::READER_HOLD_OFFSET, p::ByteLock::Exclusive).unwrap(),
        p::held_against(&file, p::DATA_OFFSET, p::ByteLock::Exclusive).unwrap(),
    )
}
fn registration_order() {
    use crate::persistence::read_persistence_test_scaffold::{
        ReaderPausePoint, pause_readers_at_for_test,
    };
    plain_store();
    assert_eq!(physical_holds(), (false, false));
    assert!(records().is_empty());
    let before = pause_readers_at_for_test(&path(), ReaderPausePoint::BeforeRegistration);
    let after = pause_readers_at_for_test(&path(), ReaderPausePoint::AfterProvisional);
    let worker = std::thread::spawn(|| {
        crate::direct_file_reader::DirectFileReader::open(
            path(),
            direct(Arc::new(ManualDeadlineClock::at(0))),
        )
        .map(drop)
    });
    let first = before
        .wait_for_paused_reader(WATCHDOG)
        .expect("GUARD_BLOCKED: actual first registry coordination");
    let before_holds = physical_holds();
    let before_records = records();
    before.release(first);
    drop(before);
    let published = after
        .wait_for_paused_reader(WATCHDOG)
        .expect("GUARD_BLOCKED: actual provisional publication");
    let after_holds = physical_holds();
    let after_records = records();
    after.release(published);
    drop(after);
    worker.join().unwrap().unwrap();
    let settled = physical_holds();
    let remaining = records();
    eprintln!(
        "ADMISSION_PUBLICATION_ORDER before_holds={before_holds:?} before_records={before_records:?} after_holds={after_holds:?} published_records={after_records:?} settled={settled:?} remaining={remaining:?}"
    );
    assert!(
        before_records.is_empty(),
        "GUARD_BLOCKED: first registration pause must precede publication"
    );
    assert!(
        !after_records.is_empty(),
        "GUARD_BLOCKED: publication pause must name real snapshot records"
    );
    assert_eq!(settled, (false, false));
    assert!(remaining.is_empty());
    assert_eq!(
        before_holds,
        (true, true),
        "OFD admission must precede registry/root coordination"
    );
    assert_eq!(
        after_holds,
        (true, true),
        "registration cannot be published before OFD admission"
    );
}
fn cancelled_cleanup() {
    plain_store();
    let original_records = records();
    assert!(original_records.is_empty());
    let root = PathBuf::from(std::env::var_os("XDG_RUNTIME_DIR").unwrap());
    let holder = std::fs::File::open(&root).unwrap();
    fs2::FileExt::try_lock_exclusive(&holder).unwrap();
    let (entered_tx, entered) = mpsc::channel();
    let (resume_tx, resume) = mpsc::channel();
    let (done_tx, done) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let token = contextdb_core::read_contract::OwnerReadCancellation::new();
        let t = token.clone();
        install_reader_join_wait_hook_for_test(Box::new(move || {
            t.cancel();
            entered_tx.send(()).unwrap();
            resume.recv().unwrap();
        }));
        let result = crate::direct_file_reader::DirectFileReader::open_with_cancellation(
            &path(),
            direct(Arc::new(ManualDeadlineClock::at(0))),
            &token,
        );
        let cancelled = matches!(
            result,
            Err(crate::direct_file_reader::DirectFileReaderError::Cancelled)
        );
        drop(result);
        done_tx.send(cancelled).unwrap();
    });
    entered
        .recv_timeout(WATCHDOG)
        .expect("GUARD_BLOCKED: exact original token cancelled at contention");
    resume_tx.send(()).unwrap();
    let held_result = done.recv_timeout(WATCHDOG);
    let before_records = records();
    let before_holds = physical_holds();
    let probe = std::fs::File::open(&root).unwrap();
    let fixture_still_holds = fs2::FileExt::try_lock_exclusive(&probe).is_err();
    drop(probe);
    fs2::FileExt::unlock(&holder).unwrap();
    drop(holder);
    let before_release = held_result.is_ok();
    let cancelled = held_result.unwrap_or_else(|_| done.recv_timeout(WATCHDOG).unwrap());
    worker.join().unwrap();
    let after_records = records();
    let after_holds = physical_holds();
    let healthy = crate::direct_file_reader::DirectFileReader::open(
        path(),
        direct(Arc::new(ManualDeadlineClock::at(0))),
    )
    .unwrap();
    drop(healthy);
    eprintln!(
        "CANCELLED_CLEANUP before_release={before_release} cancelled={cancelled} holder_still_held={fixture_still_holds} before_records={before_records:?} before_holds={before_holds:?} after_records={after_records:?} after_holds={after_holds:?}"
    );
    assert!(cancelled && fixture_still_holds);
    assert!(after_records.is_empty());
    assert_eq!(after_holds, (false, false));
    assert!(
        before_release,
        "cancelled OPEN must return cleaned while holder remains held"
    );
    assert!(
        before_records.is_empty(),
        "cancelled OPEN leaves no breadcrumb/snapshot before external release"
    );
    assert_eq!(
        before_holds,
        (false, false),
        "cancelled OPEN releases its original OFD admission"
    );
}
