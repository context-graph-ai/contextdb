#![cfg(unix)]

//! An interrupt from another process stops the owner's work, and nothing else.
//!
//! The three in-process cancellation proofs share one `Arc` between the caller
//! and the kernel, which a second process cannot do. This is the journey the
//! reader-initiated cancellation frame exists for: a human presses Ctrl-C in
//! one process while the owner is inside a request it is running for them in
//! another. The interrupted request must end, the owner must keep serving, and
//! the cursor the caller opened before the interrupt must still hand back its
//! next page.

use contextdb_core::read_contract::{
    DeadlineClock, LocalUserIdentity, OwnerReadCancellation, OwnerRequestHandler,
    ReadClientTimeouts, ReadLimits,
};
use contextdb_engine::local_transport::{
    LocalHandshake, LocalRequest, LocalRequestEnvelope, LocalResponse, ManualDeadlineClock,
    channel_socket_path,
};
use contextdb_engine::owner_read::{OwnerClient, OwnerReadScaffoldError};
use contextdb_engine::persistence::read_persistence_test_scaffold::inspect_companion_record_for_test;
use contextdb_engine::read_contract::decode_cursor_page;
use contextdb_engine::{Database, DatabaseOpenOptions};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

const OWNER_ROLE: &str = "CONTEXTDB_CROSS_PROCESS_CANCEL_OWNER";
const OWNER_STORE: &str = "CONTEXTDB_CROSS_PROCESS_CANCEL_STORE";
const OWNER_RUNTIME: &str = "CONTEXTDB_CROSS_PROCESS_CANCEL_RUNTIME";

struct ThreadWake(std::thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match Pin::as_mut(&mut future).poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}

/// How much work the owner would do if nobody stopped it. The units are
/// deliberately slow so an interrupt lands in the middle of the work rather
/// than racing its start.
const OWNER_WORK_UNITS: u64 = 2_000;

/// The owner-side work being interrupted. It reports where it started and
/// where it stopped, so the proof rests on what the owner process actually
/// did, not on the reader getting its prompt back.
struct InterruptibleHandler;

impl OwnerRequestHandler for InterruptibleHandler {
    fn handle(
        &self,
        _namespace: &str,
        request: &[u8],
        cancellation: &OwnerReadCancellation,
    ) -> contextdb_core::Result<Vec<u8>> {
        if request != b"long-running" {
            return Ok(request.to_vec());
        }
        // The interrupt is WAITED for, not watched for. A loop that spun
        // through its units would finish long before any interrupt could
        // cross a process boundary, and a loop that slept between them would
        // be pacing the proof with a clock -- the outcome would then depend
        // on how the two processes were scheduled rather than on the
        // interrupt arriving. So the work does one unit and then blocks on
        // the cancellation itself, which tells it the moment somebody
        // cancels (and at once if somebody already has).
        let interrupted = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let told = Arc::clone(&interrupted);
        let _listener = cancellation.tell_on_cancel(move || {
            let (state, arrived) = &*told;
            let mut cancelled = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *cancelled = true;
            arrived.notify_all();
        });
        println!("owner-work-entered");
        std::io::stdout().flush().expect("flush owner work entry");
        let mut completed = 0_u64;
        completed = completed.saturating_add(1);
        {
            let (state, arrived) = &*interrupted;
            let mut cancelled = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            while !*cancelled {
                cancelled = arrived
                    .wait(cancelled)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }
        // The owner asserts this about itself: if it ran to the end of its
        // work, or ended for any reason other than the interrupt, this
        // process fails and the reader's side of the proof cannot pass.
        assert!(
            cancellation.is_cancelled(),
            "the owner's work ended without ever observing the interrupt",
        );
        assert!(
            completed < OWNER_WORK_UNITS,
            "the owner finished all {OWNER_WORK_UNITS} units of work instead of stopping",
        );
        println!("owner-work-stopped-at:{completed}");
        std::io::stdout()
            .flush()
            .expect("flush owner work interruption");
        Ok(request.to_vec())
    }
}

/// A runtime root short enough to hold the fixed-length channel name. The
/// address is 64 hexadecimal characters plus its extension, and a Unix socket
/// pathname is capped well below what a nested temporary directory produces.
fn short_runtime_root() -> tempfile::TempDir {
    let root = tempfile::Builder::new()
        .prefix("cdb")
        .tempdir()
        .expect("task-scoped owner runtime root");
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700))
        .expect("secure task-scoped owner runtime root");
    root
}

fn owner_channel(store: &Path, runtime_root: &Path) -> (PathBuf, LocalHandshake) {
    let record = inspect_companion_record_for_test(store).expect("published companion record");
    let channel = channel_socket_path(runtime_root, record.fields.channel_address)
        .expect("channel pathname fits this platform");
    let handshake = LocalHandshake::current(
        record.fields.database_identity,
        record.fields.writer_run_number,
        LocalUserIdentity(nix::unistd::Uid::effective().as_raw() as u64),
    );
    (channel, handshake)
}

/// What the owner says about itself, asked over the same channel.
fn owner_status_response(client: &Arc<Mutex<OwnerClient>>, limits: ReadLimits) -> String {
    let responses = block_on(
        client
            .lock()
            .expect("owner client")
            .request(LocalRequestEnvelope {
                limits,
                request: LocalRequest::OwnerStatus,
            }),
    )
    .expect("the owner answers for its own state after the interrupt");
    match responses.as_slice() {
        [LocalResponse::OwnerStatus { status }] => format!("{:?}", status.status),
        other => panic!("owner status returns one response, got {other:?}"),
    }
}

fn cursor_page(responses: &[LocalResponse]) -> Vec<Vec<contextdb_core::Value>> {
    match responses {
        [LocalResponse::CursorOpened { opened }] => {
            decode_cursor_page(&opened.payload)
                .expect("decode first cursor page")
                .rows
        }
        [LocalResponse::CursorPage { page }] => {
            decode_cursor_page(&page.payload)
                .expect("decode resumed cursor page")
                .rows
        }
        other => panic!("expected one cursor response, got {other:?}"),
    }
}

/// The owner half. It is the same binary, run as a second process, so the
/// interrupt genuinely crosses a process boundary.
fn run_owner_process() {
    let store = PathBuf::from(std::env::var_os(OWNER_STORE).expect("owner store path"));
    let runtime_root = PathBuf::from(std::env::var_os(OWNER_RUNTIME).expect("owner runtime root"));
    let mut options = DatabaseOpenOptions::default();
    options.owner_reads.runtime_dir = Some(runtime_root);
    options.owner_reads.handler = Some(Arc::new(InterruptibleHandler));
    let database = Database::open_with_options(&store, options).expect("child owns the store");
    database
        .execute(
            "CREATE TABLE interrupted_rows (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create the cursor fixture");
    for id in 1..=3_i64 {
        database
            .execute(
                "INSERT INTO interrupted_rows VALUES ($id)",
                &HashMap::from([("id".to_owned(), contextdb_core::Value::Int64(id))]),
            )
            .expect("insert the cursor fixture");
    }
    println!("owner-ready:{:?}", database.owner_read_status());
    std::io::stdout().flush().expect("flush owner readiness");
    let mut stop = String::new();
    std::io::stdin()
        .read_line(&mut stop)
        .expect("parent stop signal");
    database.close().expect("child releases the store");
}

#[test]
fn a_second_process_interrupt_ends_only_the_request_it_names() {
    if std::env::var_os(OWNER_ROLE).is_some() {
        run_owner_process();
        return;
    }

    let store_directory = tempfile::tempdir().expect("task-scoped store directory");
    let store = store_directory.path().join("interrupted.db");
    let runtime_root = short_runtime_root();
    let mut child = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg("a_second_process_interrupt_ends_only_the_request_it_names")
        .arg("--nocapture")
        .env(OWNER_ROLE, "1")
        .env(OWNER_STORE, &store)
        .env(OWNER_RUNTIME, runtime_root.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("start the independent owner process");
    let mut owner_output = BufReader::new(child.stdout.take().expect("owner stdout")).lines();
    let readiness = loop {
        let line = owner_output
            .next()
            .expect("owner writes a readiness line")
            .expect("read owner readiness");
        if let Some(state) = line.strip_prefix("owner-ready:") {
            break state.to_owned();
        }
    };
    assert!(
        readiness.contains("Serving") && !readiness.contains("NotServing"),
        "the independent owner must be serving before it can be interrupted: {readiness}",
    );

    let (channel, handshake) = owner_channel(&store, runtime_root.path());
    let clock: Arc<dyn DeadlineClock> = Arc::new(ManualDeadlineClock::at(0));
    let client = Arc::new(Mutex::new(
        block_on(OwnerClient::connect(
            &channel,
            handshake,
            ReadClientTimeouts {
                connect_ms: 60_000,
                routing_retry_ms: 60_000,
                response_ms: 120_000,
            },
            clock,
        ))
        .expect("reach the independent owner over its real channel"),
    ));

    let limits = ReadLimits {
        cursor_page_rows: 1,
        ..ReadLimits::default()
    };
    let opened = block_on(
        client
            .lock()
            .expect("owner client")
            .request(LocalRequestEnvelope {
                limits,
                request: LocalRequest::CursorOpen {
                    statement: "SELECT id FROM interrupted_rows ORDER BY id".to_owned(),
                    params: BTreeMap::new(),
                },
            }),
    )
    .expect("open a cursor before the interrupt");
    assert_eq!(
        cursor_page(&opened),
        vec![vec![contextdb_core::Value::Int64(1)]]
    );
    let cursor_id = match opened.as_slice() {
        [LocalResponse::CursorOpened { opened }] => opened.cursor_id,
        other => panic!("cursor open returns one response, got {other:?}"),
    };

    let (interrupt, interrupted_ordinal) = {
        let held = client.lock().expect("owner client");
        (
            held.cancel_handle()
                .expect("clone the carrier to interrupt"),
            held.next_request_ordinal(),
        )
    };
    let working_client = Arc::clone(&client);
    let interrupted = std::thread::spawn(move || {
        block_on(
            working_client
                .lock()
                .expect("owner client")
                .request(LocalRequestEnvelope {
                    limits,
                    request: LocalRequest::Custom {
                        namespace: "administrative".to_owned(),
                        payload: b"long-running".to_vec(),
                    },
                }),
        )
    });

    loop {
        let line = owner_output
            .next()
            .expect("owner announces the work it started")
            .expect("read owner work entry");
        if line == "owner-work-entered" {
            break;
        }
    }
    interrupt
        .cancel(limits, interrupted_ordinal)
        .expect("the interrupt reaches the owner over the same carrier");
    let stopped_at: u64 = loop {
        let line = owner_output
            .next()
            .expect("owner announces where its work stopped")
            .expect("read owner interruption");
        if let Some(units) = line.strip_prefix("owner-work-stopped-at:") {
            break units.parse().expect("owner reports completed work units");
        }
    };
    assert!(
        stopped_at < OWNER_WORK_UNITS,
        "the owner process must stop inside its work, not run to the end of it:          stopped at {stopped_at} of {OWNER_WORK_UNITS}",
    );

    let outcome = interrupted
        .join()
        .expect("interrupted request thread joins")
        .expect_err("an interrupted request publishes no result");
    assert!(
        matches!(
            outcome,
            OwnerReadScaffoldError::Database(contextdb_core::Error::ReadCancelled)
        ),
        "an interrupted request ends as a cancelled read, got {outcome:?}",
    );

    let resumed = block_on(
        client
            .lock()
            .expect("owner client")
            .request(LocalRequestEnvelope {
                limits,
                request: LocalRequest::CursorFetch {
                    cursor_id,
                    rows: None,
                },
            }),
    )
    .expect("the session and its cursor survive the interrupt");
    assert_eq!(
        cursor_page(&resumed),
        vec![vec![contextdb_core::Value::Int64(2)]],
        "the cursor resumes at the row the interrupt never reached",
    );

    let status_after = owner_status_response(&client, limits);
    assert!(
        status_after.contains("Serving") && !status_after.contains("NotServing"),
        "the owner keeps serving after an interrupted request: {status_after}",
    );

    child
        .stdin
        .take()
        .expect("owner stdin")
        .write_all(b"stop\n")
        .expect("release the independent owner");
    assert!(child.wait().expect("owner process exit").success());
}
