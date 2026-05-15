use contextdb_core::Value;
use contextdb_core::types::ScopeLabel;
use contextdb_engine::{Database, SinkError, SinkEvent};
use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::env;
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

const CHILD_ENV: &str = "CONTEXTDB_T34_CHILD";
const RESULT_PREFIX: &str = "T34_RESULT ";

// The acceptance binary's AllocCounter is thread-local opt-in, so respawned child tests inherit it safely.
thread_local! {
    static DISPATCHER_THREAD_SENTINEL: RefCell<Option<DropSentinel>> = const { RefCell::new(None) };
}

#[derive(Clone, Copy)]
enum CallbackExit {
    Panic,
    Ok,
    Permanent,
    Transient,
}

#[derive(Clone, Copy)]
enum RegistrationPath {
    Unscoped,
    ScopedEdge,
}

struct ChildRun {
    output: Output,
    timed_out: bool,
}

struct DropSentinel {
    flag: Arc<AtomicBool>,
}

struct LastOwnerCallbackState {
    weak: Weak<Database>,
    exit: CallbackExit,
    callback_entered_tx: mpsc::SyncSender<()>,
    proceed_rx: Arc<Mutex<mpsc::Receiver<()>>>,
    last_strong_count: Arc<AtomicUsize>,
    after_drop: Arc<AtomicBool>,
    invocation_count: Arc<AtomicUsize>,
    first_event_id: Arc<AtomicU64>,
    dispatcher_released: Arc<AtomicBool>,
}

impl Drop for DropSentinel {
    fn drop(&mut self) {
        self.flag.store(true, Ordering::SeqCst);
    }
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn child_budget() -> Duration {
    if cfg!(debug_assertions) {
        Duration::from_secs(10)
    } else {
        Duration::from_secs(20)
    }
}

fn multi_iteration_budget() -> Duration {
    if cfg!(debug_assertions) {
        Duration::from_secs(30)
    } else {
        Duration::from_secs(60)
    }
}

fn current_test_name(name: &str) -> String {
    format!("event_bus_self_join::{name}")
}

fn is_child(test_id: &str) -> bool {
    env::var(CHILD_ENV).as_deref() == Ok(test_id)
}

fn run_child(test_name: &str, test_id: &str, timeout: Duration) -> ChildRun {
    let mut command = Command::new(env::current_exe().expect("current test executable"));
    command
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .env_clear()
        .env(CHILD_ENV, test_id)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for key in ["PATH", "HOME", "RUST_BACKTRACE"] {
        if let Ok(value) = env::var(key) {
            command.env(key, value);
        }
    }

    let mut child = command.spawn().expect("spawn child test process");
    let deadline = Instant::now() + timeout;
    let mut timed_out = false;
    loop {
        if child.try_wait().expect("poll child test process").is_some() {
            break;
        }
        if Instant::now() >= deadline {
            timed_out = true;
            let _ = child.kill();
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }

    let output = child.wait_with_output().expect("collect child test output");
    ChildRun { output, timed_out }
}

fn assert_child_success(run: &ChildRun) {
    let stdout = String::from_utf8_lossy(&run.output.stdout);
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    assert!(
        !run.timed_out,
        "child hung past budget; likely self-join deadlock\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    #[cfg(unix)]
    assert!(
        !matches!(run.output.status.signal(), Some(6 | 9 | 11)),
        "child terminated by signal {:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        run.output.status.signal()
    );
    assert!(
        run.output.status.success(),
        "child status: {:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        run.output.status
    );
    assert!(
        !stderr.contains("Resource deadlock avoided"),
        "child attempted a self-join\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("panic in a destructor"),
        "child aborted during destructor cleanup\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

fn result_json(run: &ChildRun) -> String {
    let stdout = String::from_utf8_lossy(&run.output.stdout);
    stdout
        .lines()
        .find_map(|line| line.strip_prefix(RESULT_PREFIX))
        .unwrap_or_else(|| panic!("missing {RESULT_PREFIX} line in child stdout:\n{stdout}"))
        .to_string()
}

fn json_u64(json: &str, key: &str) -> u64 {
    let marker = format!("\"{key}\":");
    let start = json
        .find(&marker)
        .unwrap_or_else(|| panic!("missing key {key} in {json}"))
        + marker.len();
    let digits = json[start..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    digits
        .parse()
        .unwrap_or_else(|_| panic!("invalid u64 for {key} in {json}"))
}

fn json_bool(json: &str, key: &str) -> bool {
    let marker = format!("\"{key}\":");
    let start = json
        .find(&marker)
        .unwrap_or_else(|| panic!("missing key {key} in {json}"))
        + marker.len();
    if json[start..].starts_with("true") {
        true
    } else if json[start..].starts_with("false") {
        false
    } else {
        panic!("invalid bool for {key} in {json}");
    }
}

fn assert_json_bool(json: &str, key: &str, expected: bool) {
    assert_eq!(
        json_bool(json, key),
        expected,
        "unexpected {key} in child result {json}"
    );
}

fn assert_json_u64(json: &str, key: &str, expected: u64) {
    assert_eq!(
        json_u64(json, key),
        expected,
        "unexpected {key} in child result {json}"
    );
}

fn declare_inv_schema(db: &Database, sink: &str, scoped: bool) {
    if scoped {
        db.execute(
            "CREATE TABLE invalidations (
                id UUID PRIMARY KEY,
                severity TEXT,
                reason TEXT,
                scope_label TEXT SCOPE_LABEL_READ ('edge', 'server') WRITE ('edge', 'server')
            )",
            &empty(),
        )
        .unwrap();
    } else {
        db.execute(
            "CREATE TABLE invalidations (id UUID PRIMARY KEY, severity TEXT, reason TEXT)",
            &empty(),
        )
        .unwrap();
    }
    db.execute(
        "CREATE EVENT TYPE inv_match WHEN INSERT ON invalidations",
        &empty(),
    )
    .unwrap();
    db.execute(&format!("CREATE SINK {sink} TYPE callback"), &empty())
        .unwrap();
    db.execute(
        &format!("CREATE ROUTE route_{sink} EVENT inv_match TO {sink}"),
        &empty(),
    )
    .unwrap();
}

fn insert_inv(db: &Database, id: u128, scoped: bool) {
    let mut params = HashMap::new();
    params.insert("id".into(), Value::Uuid(Uuid::from_u128(id)));
    params.insert("sev".into(), Value::Text("warning".into()));
    params.insert("rsn".into(), Value::Text(format!("event-{id}")));
    if scoped {
        params.insert("scope".into(), Value::Text("edge".into()));
        db.execute(
            "INSERT INTO invalidations (id, severity, reason, scope_label)
             VALUES ($id, $sev, $rsn, $scope)",
            &params,
        )
        .unwrap();
    } else {
        db.execute(
            "INSERT INTO invalidations (id, severity, reason) VALUES ($id, $sev, $rsn)",
            &params,
        )
        .unwrap();
    }
}

fn register_last_owner_callback(
    db: &Arc<Database>,
    sink: &str,
    path: RegistrationPath,
    state: LastOwnerCallbackState,
) -> u64 {
    let callback = move |_evt: &SinkEvent| {
        let invocation = state.invocation_count.fetch_add(1, Ordering::SeqCst);
        if invocation == 0
            && let Some(Value::Uuid(id)) = _evt.row_values.get("id")
        {
            state
                .first_event_id
                .store(id.as_u128() as u64, Ordering::SeqCst);
        }
        if invocation > 0 {
            return Ok(());
        }
        DISPATCHER_THREAD_SENTINEL.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot.is_none() {
                *slot = Some(DropSentinel {
                    flag: state.dispatcher_released.clone(),
                });
            }
        });

        let local_arc = state
            .weak
            .upgrade()
            .expect("resource-owner Arc must upgrade");
        let _ = state.callback_entered_tx.send(());
        state
            .proceed_rx
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(5))
            .expect("test thread must release callback");
        let count = Arc::strong_count(&local_arc);
        state.last_strong_count.store(count, Ordering::SeqCst);
        assert_eq!(
            count, 1,
            "callback-local arc must be last strong ref at drop time"
        );

        match state.exit {
            CallbackExit::Panic => {
                panic!("t34 self-join panic");
            }
            CallbackExit::Ok => {
                drop(local_arc);
                state.after_drop.store(true, Ordering::SeqCst);
                Ok(())
            }
            CallbackExit::Permanent => {
                drop(local_arc);
                state.after_drop.store(true, Ordering::SeqCst);
                Err(SinkError::Permanent("t34 permanent".into()))
            }
            CallbackExit::Transient => {
                drop(local_arc);
                state.after_drop.store(true, Ordering::SeqCst);
                Err(SinkError::Transient("t34 transient".into()))
            }
        }
    };

    match path {
        RegistrationPath::Unscoped => db.register_sink(sink, None, callback).unwrap(),
        RegistrationPath::ScopedEdge => {
            let scoped = db.scoped_with_constraints(
                None,
                Some(BTreeSet::from([ScopeLabel::new("edge")])),
                None,
            );
            scoped.register_sink(sink, None, callback).unwrap();
        }
    }
    db.runtime_starts_count_for_test(sink)
}

fn run_last_owner_drop_scenario(
    sink: &str,
    path: RegistrationPath,
    exit: CallbackExit,
    first_id: u128,
    insert_two_in_one_commit: bool,
) -> String {
    let scoped = matches!(path, RegistrationPath::ScopedEdge);
    let db = Arc::new(Database::open_memory());
    db.complete_initialization().unwrap();
    declare_inv_schema(&db, sink, scoped);

    let weak = Arc::downgrade(&db);
    let (callback_entered_tx, callback_entered_rx) = mpsc::sync_channel::<()>(1);
    let (proceed_tx, proceed_rx) = mpsc::sync_channel::<()>(1);
    let proceed_rx = Arc::new(Mutex::new(proceed_rx));
    let last_strong_count = Arc::new(AtomicUsize::new(0));
    let after_drop = Arc::new(AtomicBool::new(false));
    let invocation_count = Arc::new(AtomicUsize::new(0));
    let first_event_id = Arc::new(AtomicU64::new(0));
    let dispatcher_released = Arc::new(AtomicBool::new(false));

    let runtime_starts = register_last_owner_callback(
        &db,
        sink,
        path,
        LastOwnerCallbackState {
            weak,
            exit,
            callback_entered_tx,
            proceed_rx,
            last_strong_count: last_strong_count.clone(),
            after_drop: after_drop.clone(),
            invocation_count: invocation_count.clone(),
            first_event_id: first_event_id.clone(),
            dispatcher_released: dispatcher_released.clone(),
        },
    );

    let writer_db = db.clone();
    let (writer_done_tx, writer_done_rx) = mpsc::sync_channel::<()>(1);
    let writer = thread::spawn(move || {
        if insert_two_in_one_commit {
            writer_db.execute("BEGIN", &empty()).unwrap();
            insert_inv(&writer_db, first_id, scoped);
            insert_inv(&writer_db, first_id + 1, scoped);
            writer_db.execute("COMMIT", &empty()).unwrap();
        } else {
            insert_inv(&writer_db, first_id, scoped);
        }
        drop(writer_db);
        writer_done_tx.send(()).unwrap();
    });

    callback_entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("callback must acquire a local database Arc");
    writer_done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("writer must drop its Arc");
    drop(db);
    proceed_tx.send(()).unwrap();
    writer.join().unwrap();

    let release_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < release_deadline && !dispatcher_released.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(20));
    }

    let callback_panicked = matches!(exit, CallbackExit::Panic);
    let last_count = last_strong_count.load(Ordering::SeqCst);
    let after_drop = after_drop.load(Ordering::SeqCst);
    let invocations = invocation_count.load(Ordering::SeqCst);
    let first_event_id = first_event_id.load(Ordering::SeqCst);
    let dispatcher_released = dispatcher_released.load(Ordering::SeqCst);
    format!(
        "{{\"callback_panicked\":{},\"last_strong_count\":{},\"after_drop\":{},\"invocations\":{},\"first_event_id\":{},\"runtime_starts\":{},\"dispatcher_released\":{}}}",
        callback_panicked,
        last_count,
        after_drop,
        invocations,
        first_event_id,
        runtime_starts,
        dispatcher_released
    )
}

#[test]
fn t34_01_panic_callback_drops_resource_owner_arc_on_dispatcher_no_abort() {
    let name = "t34_01_panic_callback_drops_resource_owner_arc_on_dispatcher_no_abort";
    if is_child("t34_01") {
        let json = run_last_owner_drop_scenario(
            "s1",
            RegistrationPath::Unscoped,
            CallbackExit::Panic,
            1,
            false,
        );
        println!("{RESULT_PREFIX}{json}");
        return;
    }

    let run = run_child(&current_test_name(name), "t34_01", child_budget());
    assert_child_success(&run);
    let json = result_json(&run);
    assert_json_bool(&json, "callback_panicked", true);
    assert_json_u64(&json, "last_strong_count", 1);
    assert_json_u64(&json, "runtime_starts", 1);
    assert_json_bool(&json, "dispatcher_released", true);
}

#[test]
fn t34_02_panic_through_scoped_registration_drops_resource_owner_arc_no_abort() {
    let name = "t34_02_panic_through_scoped_registration_drops_resource_owner_arc_no_abort";
    if is_child("t34_02") {
        let json = run_last_owner_drop_scenario(
            "s2",
            RegistrationPath::ScopedEdge,
            CallbackExit::Panic,
            2,
            false,
        );
        println!("{RESULT_PREFIX}{json}");
        return;
    }

    let run = run_child(&current_test_name(name), "t34_02", child_budget());
    assert_child_success(&run);
    let json = result_json(&run);
    assert_json_bool(&json, "callback_panicked", true);
    assert_json_u64(&json, "last_strong_count", 1);
    assert_json_u64(&json, "runtime_starts", 1);
    assert_json_bool(&json, "dispatcher_released", true);
}

#[test]
fn t34_03_clean_drop_from_dispatcher_retires_sibling_within_bounded_budget() {
    let sibling_released = Arc::new(AtomicBool::new(false));
    let trigger_after_drop = Arc::new(AtomicBool::new(false));
    let trigger_last_strong = Arc::new(AtomicUsize::new(0));
    let trigger_released = Arc::new(AtomicBool::new(false));
    let sibling_released_worker = sibling_released.clone();
    let trigger_after_drop_worker = trigger_after_drop.clone();
    let trigger_last_strong_worker = trigger_last_strong.clone();
    let trigger_released_worker = trigger_released.clone();
    let (done_tx, done_rx) = mpsc::sync_channel::<()>(1);
    let (shutdown_started_tx, shutdown_started_rx) = mpsc::sync_channel::<()>(1);
    let (trigger_release_tx, trigger_release_rx) = mpsc::sync_channel::<()>(1);
    let (sibling_entered_tx, sibling_entered_rx) = mpsc::sync_channel::<()>(1);
    let (sibling_release_tx, sibling_release_rx) = mpsc::sync_channel::<()>(1);
    let sibling_release_rx = Arc::new(Mutex::new(sibling_release_rx));

    thread::spawn(move || {
        let db = Arc::new(Database::open_memory());
        db.complete_initialization().unwrap();
        db.execute(
            "CREATE TABLE invalidations (id UUID PRIMARY KEY, severity TEXT, reason TEXT)",
            &empty(),
        )
        .unwrap();
        db.execute(
            "CREATE EVENT TYPE inv_match WHEN INSERT ON invalidations",
            &empty(),
        )
        .unwrap();
        db.execute("CREATE SINK s_trigger TYPE callback", &empty())
            .unwrap();
        db.execute("CREATE SINK s_sibling TYPE callback", &empty())
            .unwrap();
        db.execute("CREATE SINK s_retry TYPE callback", &empty())
            .unwrap();
        db.execute(
            "CREATE ROUTE route_trigger EVENT inv_match TO s_trigger",
            &empty(),
        )
        .unwrap();
        db.execute(
            "CREATE ROUTE route_sibling EVENT inv_match TO s_sibling",
            &empty(),
        )
        .unwrap();
        db.execute(
            "CREATE ROUTE route_retry EVENT inv_match TO s_retry",
            &empty(),
        )
        .unwrap();

        let sibling_release_rx = sibling_release_rx.clone();
        let sibling_entered_once = Arc::new(AtomicBool::new(false));
        let sibling_entered_once_cb = sibling_entered_once.clone();
        db.register_sink("s_sibling", None, move |_| {
            DISPATCHER_THREAD_SENTINEL.with(|slot| {
                let mut slot = slot.borrow_mut();
                if slot.is_none() {
                    *slot = Some(DropSentinel {
                        flag: sibling_released_worker.clone(),
                    });
                }
            });
            if !sibling_entered_once_cb.swap(true, Ordering::SeqCst) {
                let _ = sibling_entered_tx.send(());
            }
            sibling_release_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .expect("test must release sibling callback");
            Ok(())
        })
        .unwrap();

        let retry_attempts = Arc::new(AtomicUsize::new(0));
        let retry_attempts_cb = retry_attempts.clone();
        db.register_sink("s_retry", None, move |_| {
            retry_attempts_cb.fetch_add(1, Ordering::SeqCst);
            Err(SinkError::Transient("park retry waiter".into()))
        })
        .unwrap();

        let weak = Arc::downgrade(&db);
        let (callback_entered_tx, callback_entered_rx) = mpsc::sync_channel::<()>(1);
        let (proceed_tx, proceed_rx) = mpsc::sync_channel::<()>(1);
        let proceed_rx = Arc::new(Mutex::new(proceed_rx));
        let invocation_count = Arc::new(AtomicUsize::new(0));
        let runtime_starts = register_last_owner_callback(
            &db,
            "s_trigger",
            RegistrationPath::Unscoped,
            LastOwnerCallbackState {
                weak,
                exit: CallbackExit::Ok,
                callback_entered_tx,
                proceed_rx,
                last_strong_count: trigger_last_strong_worker.clone(),
                after_drop: trigger_after_drop_worker.clone(),
                invocation_count,
                first_event_id: Arc::new(AtomicU64::new(0)),
                dispatcher_released: trigger_released_worker.clone(),
            },
        );
        assert_eq!(runtime_starts, 1, "trigger dispatcher starts once");

        let writer_db = db.clone();
        let (writer_done_tx, writer_done_rx) = mpsc::sync_channel::<()>(1);
        let writer = thread::spawn(move || {
            insert_inv(&writer_db, 3, false);
            drop(writer_db);
            writer_done_tx.send(()).unwrap();
        });

        callback_entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("trigger callback must start");
        sibling_entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("sibling callback must be in flight before shutdown");
        let retry_deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < retry_deadline && retry_attempts.load(Ordering::SeqCst) < 7 {
            thread::sleep(Duration::from_millis(20));
        }
        assert!(
            retry_attempts.load(Ordering::SeqCst) >= 7,
            "retry sibling must enter long backoff before shutdown"
        );
        thread::sleep(Duration::from_millis(100));
        writer_done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("writer must release its Arc");
        drop(db);
        shutdown_started_tx.send(()).unwrap();
        trigger_release_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("parent must release trigger callback into last-Arc drop");
        proceed_tx.send(()).unwrap();
        writer.join().unwrap();
        let observed_deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < observed_deadline
            && !trigger_after_drop_worker.load(Ordering::SeqCst)
        {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            trigger_after_drop_worker.load(Ordering::SeqCst),
            "trigger callback must continue after dropping the last Arc"
        );
        done_tx.send(()).unwrap();
    });

    shutdown_started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect(
            "worker must reach dispatcher-thread shutdown point before parent releases trigger",
        );
    assert!(
        done_rx.recv_timeout(Duration::from_millis(150)).is_err(),
        "worker must not finish before the trigger callback drops the last Arc"
    );
    trigger_release_tx
        .send(())
        .expect("release trigger callback");
    assert!(
        done_rx.recv_timeout(Duration::from_millis(750)).is_err(),
        "dispatcher-thread shutdown must wait for in-flight sibling callback before returning"
    );
    sibling_release_tx
        .send(())
        .expect("release sibling callback");
    assert!(
        done_rx.recv_timeout(Duration::from_millis(1500)).is_ok(),
        "clean dispatcher-thread drop did not finish promptly after sibling release; shutdown likely failed to notify waiters"
    );
    assert_eq!(
        trigger_last_strong.load(Ordering::SeqCst),
        1,
        "trigger callback must drop the last resource-owner Arc"
    );
    assert!(
        trigger_after_drop.load(Ordering::SeqCst),
        "clean-return callback must continue after dropping the last Arc"
    );

    let trigger_release_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < trigger_release_deadline && !trigger_released.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        trigger_released.load(Ordering::SeqCst),
        "dispatcher-thread shutdown must let the initiating dispatcher exit"
    );

    let release_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < release_deadline && !sibling_released.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        sibling_released.load(Ordering::SeqCst),
        "sibling dispatcher and callback registration must retire after shutdown"
    );
}

#[test]
fn t34_04_panic_isolation_counter_exact_and_same_event_delivered_to_sibling() {
    let db = Database::open_memory();
    db.complete_initialization().unwrap();
    db.execute(
        "CREATE TABLE invalidations (id UUID PRIMARY KEY, severity TEXT, reason TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE EVENT TYPE inv_match WHEN INSERT ON invalidations",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE SINK s_panic TYPE callback", &empty())
        .unwrap();
    db.execute("CREATE SINK s_count TYPE callback", &empty())
        .unwrap();
    db.execute(
        "CREATE ROUTE route_panic EVENT inv_match TO s_panic",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE ROUTE route_count EVENT inv_match TO s_count",
        &empty(),
    )
    .unwrap();

    db.register_sink("s_panic", None, |_| {
        panic!("t34 isolation panic");
    })
    .unwrap();

    let delivered_ids = Arc::new(Mutex::new(Vec::new()));
    let delivered_ids_cb = delivered_ids.clone();
    db.register_sink("s_count", None, move |evt| {
        let id = match evt.row_values.get("id") {
            Some(Value::Uuid(id)) => *id,
            other => panic!("expected UUID id in sink event, got {other:?}"),
        };
        delivered_ids_cb.lock().unwrap().push(id);
        Ok(())
    })
    .unwrap();

    for id in [11u128, 12, 13] {
        insert_inv(&db, id, false);
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let panic_metrics = db.sink_metrics_for_test("s_panic");
        let count_len = delivered_ids.lock().unwrap().len();
        if panic_metrics.permanent_failures == 3 && count_len == 3 {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }

    let panic_metrics = db.sink_metrics_for_test("s_panic");
    let count_metrics = db.sink_metrics_for_test("s_count");
    assert_eq!(panic_metrics.permanent_failures, 3);
    assert_eq!(panic_metrics.delivered, 0);
    assert_eq!(count_metrics.delivered, 3);
    assert_eq!(count_metrics.permanent_failures, 0);
    assert_eq!(
        *delivered_ids.lock().unwrap(),
        vec![
            Uuid::from_u128(11),
            Uuid::from_u128(12),
            Uuid::from_u128(13)
        ],
        "healthy sibling sink must receive the same committed event ids in order"
    );
}

#[test]
fn t34_05_dispatcher_thread_initiated_drop_is_deterministic_and_leak_free_across_iterations() {
    let name =
        "t34_05_dispatcher_thread_initiated_drop_is_deterministic_and_leak_free_across_iterations";
    if is_child("t34_05") {
        let mut first_ms = 0u128;
        let mut last_ms = 0u128;
        let mut max_ms = 0u128;
        for i in 0..5u128 {
            let started = Instant::now();
            let json = run_last_owner_drop_scenario(
                "s1",
                RegistrationPath::Unscoped,
                CallbackExit::Panic,
                100 + i,
                false,
            );
            assert_eq!(json_u64(&json, "last_strong_count"), 1);
            assert_eq!(json_u64(&json, "runtime_starts"), 1);
            assert_json_bool(&json, "dispatcher_released", true);
            let elapsed = started.elapsed().as_millis();
            if i == 0 {
                first_ms = elapsed;
            }
            last_ms = elapsed;
            max_ms = max_ms.max(elapsed);
        }
        println!(
            "{RESULT_PREFIX}{{\"iterations_completed\":5,\"all_clean\":true,\"first_ms\":{},\"last_ms\":{},\"max_ms\":{}}}",
            first_ms, last_ms, max_ms
        );
        return;
    }

    let run = run_child(&current_test_name(name), "t34_05", multi_iteration_budget());
    assert_child_success(&run);
    let json = result_json(&run);
    assert_json_u64(&json, "iterations_completed", 5);
    assert_json_bool(&json, "all_clean", true);
    let first = json_u64(&json, "first_ms");
    let last = json_u64(&json, "last_ms");
    let max = json_u64(&json, "max_ms");
    assert!(
        last <= first.saturating_mul(3).max(250),
        "last iteration grew suspiciously: first={first}ms last={last}ms result={json}"
    );
    assert!(
        max <= first.saturating_mul(5).max(500),
        "iteration duration grew suspiciously: first={first}ms max={max}ms result={json}"
    );
}

#[test]
fn t34_06_post_shutdown_batch_entries_are_not_delivered() {
    let name = "t34_06_post_shutdown_batch_entries_are_not_delivered";
    if is_child("t34_06") {
        let json = run_last_owner_drop_scenario(
            "s1",
            RegistrationPath::Unscoped,
            CallbackExit::Ok,
            200,
            true,
        );
        println!("{RESULT_PREFIX}{json}");
        return;
    }

    let run = run_child(&current_test_name(name), "t34_06", child_budget());
    assert_child_success(&run);
    let json = result_json(&run);
    assert_json_u64(&json, "last_strong_count", 1);
    assert_json_bool(&json, "after_drop", true);
    assert_json_u64(&json, "invocations", 1);
    assert_json_u64(&json, "first_event_id", 200);
    assert_json_u64(&json, "runtime_starts", 1);
    assert_json_bool(&json, "dispatcher_released", true);
}

#[test]
fn t34_07_clean_error_return_drops_resource_owner_arc_on_dispatcher_no_abort() {
    let name = "t34_07_clean_error_return_drops_resource_owner_arc_on_dispatcher_no_abort";
    if is_child("t34_07") {
        let json = run_last_owner_drop_scenario(
            "s1",
            RegistrationPath::Unscoped,
            CallbackExit::Permanent,
            300,
            false,
        );
        println!("{RESULT_PREFIX}{json}");
        return;
    }

    let run = run_child(&current_test_name(name), "t34_07", child_budget());
    assert_child_success(&run);
    let json = result_json(&run);
    assert_json_u64(&json, "last_strong_count", 1);
    assert_json_bool(&json, "after_drop", true);
    assert_json_u64(&json, "runtime_starts", 1);
    assert_json_bool(&json, "dispatcher_released", true);
}

#[test]
fn t34_08_explicit_close_then_drop_is_idempotent_no_double_join() {
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let delivered_cb = delivered.clone();
    let dispatcher_released = Arc::new(AtomicBool::new(false));
    let dispatcher_released_cb = dispatcher_released.clone();
    let db = Arc::new(Database::open_memory());
    db.complete_initialization().unwrap();
    declare_inv_schema(&db, "s1", false);
    db.register_sink("s1", None, move |evt| {
        DISPATCHER_THREAD_SENTINEL.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot.is_none() {
                *slot = Some(DropSentinel {
                    flag: dispatcher_released_cb.clone(),
                });
            }
        });
        delivered_cb.lock().unwrap().push(evt.clone());
        Ok(())
    })
    .unwrap();

    insert_inv(&db, 400, false);
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && delivered.lock().unwrap().is_empty() {
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        delivered.lock().unwrap().len(),
        1,
        "sink must receive the pre-close event before idempotency check"
    );

    let close_db = db.clone();
    let (close_tx, close_rx) = mpsc::sync_channel::<std::result::Result<(), String>>(1);
    thread::spawn(move || {
        let result = close_db.close().map_err(|err| err.to_string());
        close_tx.send(result).unwrap();
    });
    close_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("Database::close must return within budget")
        .expect("Database::close must succeed");
    let release_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < release_deadline && !dispatcher_released.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        dispatcher_released.load(Ordering::SeqCst),
        "Database::close from a non-dispatcher thread must retire the dispatcher thread"
    );
    db.close()
        .expect("second Database::close must be a clean public no-op");

    let before_drop_count = delivered.lock().unwrap().len();
    let (drop_tx, drop_rx) = mpsc::sync_channel::<()>(1);
    thread::spawn(move || {
        drop(db);
        drop_tx.send(()).unwrap();
    });
    drop_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("dropping after close must return within budget");
    assert_eq!(
        delivered.lock().unwrap().len(),
        before_drop_count,
        "drop after close must not trigger extra delivery"
    );
}

#[test]
fn t34_09_transient_error_return_drops_resource_owner_arc_on_dispatcher_no_abort() {
    let name = "t34_09_transient_error_return_drops_resource_owner_arc_on_dispatcher_no_abort";
    if is_child("t34_09") {
        let json = run_last_owner_drop_scenario(
            "s1",
            RegistrationPath::Unscoped,
            CallbackExit::Transient,
            500,
            false,
        );
        println!("{RESULT_PREFIX}{json}");
        return;
    }

    let run = run_child(&current_test_name(name), "t34_09", child_budget());
    assert_child_success(&run);
    let json = result_json(&run);
    assert_json_u64(&json, "last_strong_count", 1);
    assert_json_bool(&json, "after_drop", true);
    assert_json_u64(&json, "runtime_starts", 1);
    assert_json_bool(&json, "dispatcher_released", true);
}
