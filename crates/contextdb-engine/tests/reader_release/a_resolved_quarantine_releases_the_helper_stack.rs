//! A quarantine that resolves gives back everything the helper cost.
//!
//! Promise (contextdb-intent, Policies 21-23): a wait helper whose death could
//! not be confirmed is contained, and once its death is confirmed the store is
//! as if the helper had never run: its private stack mapping is released, so
//! a process that keeps meeting quarantines does not grow without bound.
//!
//! Test-estate reason (2026-10-15): no earlier test resolves a quarantine, so
//! nothing counts what the helper left mapped. The count is process-wide, so
//! the scenario runs in a child process of its own and reports the counts it
//! saw; the parent asserts them. One scenario, one child.

use crate::writer_acquisition_support::{
    Phase, WATCHDOG, is_quarantine_refusal, next_event, park_readers, seeded_store, watch,
};
use contextdb_core::read_contract::OwnerReadCancellation;
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    ReaderReleaseBlockForTest, WaitFault, WriterAdmissionEvent, inject_wait_fault_for_test,
    reader_release_blocks_of_thread_for_test, writer_wait_helper_stacks_mapped_for_test,
};
use contextdb_engine::{Database, DatabaseOpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};

const ROLE: &str = "CONTEXTDB_QUARANTINE_STACK_ROLE";
const STORE: &str = "CONTEXTDB_QUARANTINE_STACK_STORE";

/// Block until `pid` has exited, by the kernel's exit notification on a pidfd.
fn exits(pid: u32) -> bool {
    // SAFETY: plain syscalls on a pid and a descriptor this function owns.
    unsafe {
        let pidfd = libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0) as libc::c_int;
        if pidfd < 0 {
            return std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
        }
        let mut watched = libc::pollfd {
            fd: pidfd,
            events: libc::POLLIN,
            revents: 0,
        };
        let ready = libc::poll(&mut watched, 1, WATCHDOG.as_millis() as libc::c_int);
        libc::close(pidfd);
        ready == 1
    }
}

/// The child half: arm a quarantine, let it resolve, report the mapping count
/// at each step.
#[test]
fn quarantine_stack_child() {
    if std::env::var_os(ROLE).is_none() {
        return;
    }
    let path = std::env::var(STORE).expect("child store path");
    let path = Path::new(&path);
    let mut readers = park_readers(path, Phase::SourceStart, 1);
    let reader = readers.remove(0);
    let watch = watch(path);
    let fault = inject_wait_fault_for_test(path, WaitFault::KillFailsAndHelperUnconfirmed);

    // Both opens run on one thread that stays alive: the helper's parent-death
    // signal follows the thread that created it.
    let (commands, received) = std::sync::mpsc::channel::<(
        DatabaseOpenOptions,
        std::sync::mpsc::Sender<contextdb_core::Result<Database>>,
    )>();
    let opener_path = path.to_path_buf();
    let opener = std::thread::spawn(move || {
        for (options, reply) in received {
            let _ = reply.send(Database::open_with_options(&opener_path, options));
        }
    });
    let opener_thread = opener.thread().id();

    let stop = OwnerReadCancellation::new();
    let (reply, first) = std::sync::mpsc::channel();
    commands
        .send((
            DatabaseOpenOptions {
                reader_wait_stop: Some(stop.clone()),
                ..DatabaseOpenOptions::default()
            },
            reply,
        ))
        .expect("hand the first open over");
    assert_eq!(
        next_event(&watch, "while a reader is inside its acquisition"),
        WriterAdmissionEvent::Waiting
    );
    stop.cancel();
    match first
        .recv_timeout(WATCHDOG)
        .expect("the first open answered")
    {
        Err(error) if is_quarantine_refusal(&error) => {}
        other => panic!("an unconfirmed death is quarantined: {:?}", other.err()),
    }
    println!(
        "QUARANTINED mapped={}",
        writer_wait_helper_stacks_mapped_for_test()
    );
    std::io::stdout().flush().expect("flush");

    // The helper gets its locks once the reader leaves, and exits on its own.
    let helper = reader_release_blocks_of_thread_for_test(opener_thread)
        .into_iter()
        .filter_map(|block| match block {
            ReaderReleaseBlockForTest::HelperSpawned { pid } => Some(pid),
            _ => None,
        })
        .next_back()
        .expect("the wait's helper");
    reader.let_go();
    drop(reader.session());
    assert!(
        exits(helper),
        "the quarantined helper exits once it holds its locks"
    );
    drop(fault);

    // A later open makes its one non-blocking death check, resolves the
    // quarantine, and (already cancelled) answers without waiting.
    let cancelled = OwnerReadCancellation::new();
    cancelled.cancel();
    let (reply, later) = std::sync::mpsc::channel();
    commands
        .send((
            DatabaseOpenOptions {
                reader_wait_stop: Some(cancelled),
                ..DatabaseOpenOptions::default()
            },
            reply,
        ))
        .expect("hand the later open over");
    let _ = later
        .recv_timeout(WATCHDOG)
        .expect("the later open answered");
    println!(
        "RESOLVED mapped={}",
        writer_wait_helper_stacks_mapped_for_test()
    );
    std::io::stdout().flush().expect("flush");
    drop(commands);
    opener.join().expect("the opener thread finishes");
}

#[test]
fn a_quarantine_that_resolves_releases_the_helper_stack_mapping() {
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "quarantine-stack.db");
    let filter = module_path!()
        .split_once("::")
        .map(|(_, rest)| format!("{rest}::quarantine_stack_child"))
        .expect("a module path inside the test binary");
    let mut child = Command::new(std::env::current_exe().expect("test binary"))
        .arg("--exact")
        .arg(filter)
        .arg("--nocapture")
        .env(ROLE, "1")
        .env(STORE, &path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn the quarantine scenario");
    let output = BufReader::new(child.stdout.take().expect("child stdout"));
    let mut quarantined = None;
    let mut resolved = None;
    for line in output.lines() {
        let line = line.expect("read the child's output");
        if let Some(count) = line.strip_prefix("QUARANTINED mapped=") {
            quarantined = Some(count.trim().parse::<usize>().expect("a count"));
        }
        if let Some(count) = line.strip_prefix("RESOLVED mapped=") {
            resolved = Some(count.trim().parse::<usize>().expect("a count"));
        }
    }
    let status = child.wait().expect("the child ends");
    assert!(status.success(), "the quarantine scenario failed");
    assert!(
        quarantined.expect("the child reported its quarantine") >= 1,
        "an unresolved quarantine keeps the helper's stack mapped"
    );
    assert_eq!(
        resolved.expect("the child reported its resolution"),
        0,
        "a quarantine that resolved gave the helper's stack mapping back"
    );
}
