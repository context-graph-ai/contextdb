//! The helper a cancellable writer wait starts is contained: it can never be
//! left running after the wait that started it, it never holds the store
//! once its wait is over, and where the platform cannot guarantee that the
//! cancellable wait is refused up front instead of attempted.
//!
//! Promise (contextdb-intent, Policies 21-23, and the owner's rule that a
//! wait is bounded and cancellable): a caller that supplies a stop or a
//! deadline gets a wait that ends when asked and leaves nothing behind -- no
//! orphan process, no lock held by a process nobody owns. A default open,
//! which supplies neither, is never refused on this account and still waits.
//!
//! Test-estate reason, shared by every function here (2026-10-15, reader/writer
//! handoff): process-level containment cannot be shown in one process. The
//! killed-parent proof needs a real second process, and the kill and
//! quarantine schedules need the engine's own fault seams; no earlier test
//! starts or kills a wait helper because no helper existed. The seccomp case
//! is a separate function because it needs a thread-scoped syscall filter that
//! must not leak into any other proof.

use crate::writer_acquisition_support::{
    Phase, WATCHDOG, is_typed_io_refusal, is_unsupported_cancellable_wait, next_event, open_writer,
    park_readers, seeded_store, watch,
};
use contextdb_core::read_contract::OwnerReadCancellation;
use contextdb_engine::DatabaseOpenOptions;
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    ReaderReleaseBlockForTest, WaitFault, WriterAdmissionEvent, inject_wait_fault_for_test,
    reader_release_blocks_of_thread_for_test,
};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};

const ROLE: &str = "CONTEXTDB_WRITER_WAIT_CONTAINMENT_ROLE";
const STORE: &str = "CONTEXTDB_WRITER_WAIT_CONTAINMENT_STORE";

/// How many wait events carry this variant name, among those recorded by
/// waits that ran on `thread` (the thread that runs the writable open). The
/// record is process-wide and tests share it under `cargo test`, so every
/// read here is scoped to the proof's own open thread.
fn blocks_named(thread: std::thread::ThreadId, variant: &str) -> usize {
    reader_release_blocks_of_thread_for_test(thread)
        .iter()
        .filter(|block| format!("{block:?}").starts_with(variant))
        .count()
}

/// Helpers started and not yet reaped on `thread`. The per-thread preflight
/// starts and reaps one helper of its own, so a real wait is exactly one
/// unreaped helper.
fn unreaped_helpers(thread: std::thread::ThreadId) -> usize {
    blocks_named(thread, "HelperSpawned") - blocks_named(thread, "Reaped")
}

fn helper_pids(thread: std::thread::ThreadId) -> Vec<u32> {
    reader_release_blocks_of_thread_for_test(thread)
        .into_iter()
        .filter_map(|block| match block {
            ReaderReleaseBlockForTest::HelperSpawned { pid } => Some(pid),
            _ => None,
        })
        .collect()
}

fn cancellable() -> DatabaseOpenOptions {
    DatabaseOpenOptions {
        reader_wait_stop: Some(OwnerReadCancellation::new()),
        ..DatabaseOpenOptions::default()
    }
}

/// Block until the process `pid` has exited, by the kernel's own exit
/// notification on a pidfd; false when the failsafe bound passes first.
fn exits_within_failsafe(pid: u32) -> bool {
    // SAFETY: plain syscalls on a pid and a descriptor this function owns.
    unsafe {
        let pidfd = libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0) as libc::c_int;
        if pidfd < 0 {
            // No such process any more: it has already exited and been reaped.
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

/// Test-estate reason (2026-10-15): a wait helper killed from outside ends the wait with a typed
/// I/O refusal and holds nothing: the next writer opens without waiting.
#[test]
fn a_helper_killed_externally_is_a_typed_refusal_and_leaves_nothing_held() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "killed-helper.db");
    let mut readers = park_readers(&path, Phase::SourceStart, 1);
    let reader = readers.remove(0);
    let watch = watch(&path);

    let writer = open_writer(&path, cancellable());
    assert_eq!(
        next_event(&watch, "while a reader is inside its acquisition"),
        WriterAdmissionEvent::Waiting
    );
    let open_thread = writer.thread_id();
    assert_eq!(
        unreaped_helpers(open_thread),
        1,
        "a cancellable wait has exactly one helper running (the preflight's own is reaped)"
    );
    let pids = [*helper_pids(open_thread).last().expect("the wait's helper")];
    // SAFETY: signalling a process this proof started through the engine.
    let killed = unsafe { libc::kill(pids[0] as libc::pid_t, libc::SIGKILL) };
    assert_eq!(killed, 0, "the helper exists to be killed");

    match writer.finish() {
        Err(error) if is_typed_io_refusal(&error) => {}
        other => panic!("a helper killed from outside is a typed I/O refusal: {other:?}"),
    }
    assert!(exits_within_failsafe(pids[0]), "the killed helper is gone");

    reader.let_go();
    drop(reader.session());
    let next = open_writer(&path, DatabaseOpenOptions::default());
    assert_eq!(
        next_event(&watch, "for the writer after the killed helper"),
        WriterAdmissionEvent::Acquired,
        "the killed helper's locks died with it; nothing waits and nothing refuses"
    );
    next.finish()
        .expect("the next writer opens")
        .close()
        .expect("close");
}

/// The child half of the parent-killed test: a separate process that opens the store as a
/// cancellable writer, reports which helper it started, and waits to be killed.
#[test]
fn waiting_writer_child() {
    let Ok(role) = std::env::var(ROLE) else {
        return;
    };
    assert_eq!(role, "waiting-writer");
    let path = std::env::var(STORE).expect("child store path");
    let watch = watch(Path::new(&path));
    let writer = open_writer(Path::new(&path), cancellable());
    assert_eq!(
        next_event(&watch, "in the child"),
        WriterAdmissionEvent::Waiting
    );
    let pids = helper_pids(writer.thread_id());
    println!("WAITING {}", pids.last().expect("the wait's helper"));
    std::io::stdout().flush().expect("flush the marker");
    let _ = writer.finish();
}

/// Test-estate reason (2026-10-15): the wait helper never outlives its parent: after the waiting
/// writer's process is killed the helper is gone and the lock it waited on is free for an
/// independent process. A second process is the only way to kill the parent.
#[test]
fn a_parent_killed_mid_wait_leaves_no_helper_and_a_free_store() {
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "killed-parent.db");
    let mut readers = park_readers(&path, Phase::SourceStart, 1);
    let reader = readers.remove(0);

    let filter = module_path!()
        .split_once("::")
        .map(|(_, rest)| format!("{rest}::waiting_writer_child"))
        .expect("a module path inside the test binary");
    let mut child = Command::new(std::env::current_exe().expect("test binary"))
        .arg("--exact")
        .arg(filter)
        .arg("--nocapture")
        .env(ROLE, "waiting-writer")
        .env(STORE, &path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn the waiting-writer process");
    let mut output = BufReader::new(child.stdout.take().expect("child stdout"));
    let mut line = String::new();
    let helper = loop {
        line.clear();
        assert_ne!(
            output
                .read_line(&mut line)
                .expect("read the child's output"),
            0,
            "the child exited before it started waiting"
        );
        if let Some(pid) = line.trim().strip_prefix("WAITING ") {
            break pid.parse::<u32>().expect("the helper's process id");
        }
    };

    child.kill().expect("kill the waiting writer's process");
    child.wait().expect("reap the killed process");
    assert!(
        exits_within_failsafe(helper),
        "the helper of a killed parent must not outlive it"
    );

    reader.let_go();
    drop(reader.session());
    let watch = watch(&path);
    let next = open_writer(&path, DatabaseOpenOptions::default());
    assert_eq!(
        next_event(
            &watch,
            "for an independent writer after the parent was killed"
        ),
        WriterAdmissionEvent::Acquired,
        "the lock the dead parent's helper was waiting on is free"
    );
    next.finish()
        .expect("the independent writer opens")
        .close()
        .expect("close");
}

/// Test-estate reason (2026-10-15): where containment cannot be guaranteed (the real preflight runs
/// with SIGKILL denied) a cancellable open is refused with the typed unsupported refusal and leaves
/// no child, while a default open still waits and opens.
#[test]
fn a_failed_preflight_refuses_a_cancellable_open_but_a_default_open_still_waits() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "preflight.db");
    let mut readers = park_readers(&path, Phase::SourceStart, 1);
    let reader = readers.remove(0);
    let watch = watch(&path);
    let _fault = inject_wait_fault_for_test(&path, WaitFault::PreflightFails);

    let refused = open_writer(&path, cancellable());
    let open_thread = refused.thread_id();
    match refused.finish() {
        Err(error) if is_unsupported_cancellable_wait(&error) => {}
        other => panic!("a cancellable wait with a failed preflight is unsupported: {other:?}"),
    }
    assert_eq!(
        blocks_named(open_thread, "HelperSpawned"),
        blocks_named(open_thread, "Reaped"),
        "the preflight's helper was reaped: a wait the platform cannot contain \
         leaves no child behind"
    );

    let plain = open_writer(&path, DatabaseOpenOptions::default());
    assert_eq!(
        next_event(&watch, "for the default open"),
        WriterAdmissionEvent::Waiting,
        "a default open supplies no stop and no deadline, so it waits regardless"
    );
    reader.let_go();
    drop(reader.session());
    assert_eq!(
        next_event(&watch, "after the reader left"),
        WriterAdmissionEvent::Acquired
    );
    plain
        .finish()
        .expect("the default open waited and opened")
        .close()
        .expect("close");
}

/// Test-estate reason (2026-10-15): a helper whose death cannot be confirmed is quarantined: the
/// open that started it answers the typed quarantine refusal, and a later open of the same store
/// answers it again without starting a second wait helper.
#[test]
fn an_unconfirmed_helper_death_quarantines_the_store_and_starts_no_second_helper() {
    use crate::writer_acquisition_support::is_quarantine_refusal;

    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "quarantine.db");
    let mut readers = park_readers(&path, Phase::SourceStart, 1);
    let reader = readers.remove(0);
    let watch = watch(&path);
    let _fault = inject_wait_fault_for_test(&path, WaitFault::KillFailsAndHelperUnconfirmed);

    // Both opens run on ONE thread that outlives the second: the helper's
    // parent-death signal follows the thread that created it, so a first
    // open on a thread that has already exited would kill the quarantined
    // helper and dissolve the quarantine this proof is about.
    let opener = OpenerThread::start(&path);
    let stop = OwnerReadCancellation::new();
    let first = opener.open(DatabaseOpenOptions {
        reader_wait_stop: Some(stop.clone()),
        ..DatabaseOpenOptions::default()
    });
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
        other => panic!(
            "an unconfirmed helper death is the quarantine refusal: {:?}",
            other.as_ref().err()
        ),
    }
    let open_thread = opener.thread_id();
    assert_eq!(
        unreaped_helpers(open_thread),
        1,
        "the unconfirmed helper is still unresolved"
    );

    match opener
        .open(cancellable())
        .recv_timeout(WATCHDOG)
        .expect("the later open answered")
    {
        Err(error) if is_quarantine_refusal(&error) => {}
        other => panic!(
            "a later open of the quarantined store is refused too: {:?}",
            other.as_ref().err()
        ),
    }
    assert_eq!(
        unreaped_helpers(open_thread),
        1,
        "a quarantined store never gets a second wait helper while the first is unresolved"
    );

    reader.let_go();
    drop(reader.session());
    drop(opener);
}

/// A thread that runs every open it is given, one after another, and stays
/// alive until it is dropped.
struct OpenerThread {
    commands: Option<
        std::sync::mpsc::Sender<(
            DatabaseOpenOptions,
            std::sync::mpsc::Sender<contextdb_core::Result<contextdb_engine::Database>>,
        )>,
    >,
    thread: Option<std::thread::JoinHandle<()>>,
    thread_id: std::thread::ThreadId,
}

impl OpenerThread {
    fn thread_id(&self) -> std::thread::ThreadId {
        self.thread_id
    }

    fn start(path: &Path) -> Self {
        let path = path.to_path_buf();
        let (commands, received) = std::sync::mpsc::channel::<(
            DatabaseOpenOptions,
            std::sync::mpsc::Sender<contextdb_core::Result<contextdb_engine::Database>>,
        )>();
        let thread = std::thread::spawn(move || {
            for (options, reply) in received {
                let _ = reply.send(contextdb_engine::Database::open_with_options(
                    &path, options,
                ));
            }
        });
        Self {
            commands: Some(commands),
            thread_id: thread.thread().id(),
            thread: Some(thread),
        }
    }

    fn open(
        &self,
        options: DatabaseOpenOptions,
    ) -> std::sync::mpsc::Receiver<contextdb_core::Result<contextdb_engine::Database>> {
        let (reply, answer) = std::sync::mpsc::channel();
        self.commands
            .as_ref()
            .expect("the opener thread is running")
            .send((options, reply))
            .expect("hand an open to the opener thread");
        answer
    }
}

impl Drop for OpenerThread {
    fn drop(&mut self) {
        drop(self.commands.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Test-estate reason (2026-10-15): a thread whose syscall filter denies SIGKILL through a pidfd
/// gets the unsupported refusal for a cancellable open, and a default open on that thread still
/// waits. It runs on its own thread so the filter never leaks into another proof.
#[cfg(target_os = "linux")]
#[test]
fn a_thread_that_may_not_sigkill_gets_the_unsupported_refusal_and_default_opens_still_wait() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = seeded_store(directory.path(), "seccomp.db");
    let mut readers = park_readers(&path, Phase::SourceStart, 1);
    let reader = readers.remove(0);
    let watch = watch(&path);

    let (tell, heard) = std::sync::mpsc::channel();
    let thread_path = path.clone();
    let filtered = std::thread::spawn(move || {
        deny_sigkill_through_pidfd_send_signal_on_this_thread();
        let refused = contextdb_engine::Database::open_with_options(&thread_path, cancellable());
        let _ = tell.send(refused.err());
        // The default open on this same thread waits for the reader below.
        contextdb_engine::Database::open(&thread_path)
            .map(|database| database.close())
            .expect("a default open on a filtered thread still waits and opens")
            .expect("close");
    });
    match heard
        .recv_timeout(WATCHDOG)
        .expect("the cancellable open answered")
    {
        Some(error) if is_unsupported_cancellable_wait(&error) => {}
        other => panic!("a thread that cannot SIGKILL gets the unsupported refusal: {other:?}"),
    }
    assert_eq!(
        next_event(&watch, "for the default open on the filtered thread"),
        WriterAdmissionEvent::Waiting
    );
    reader.let_go();
    drop(reader.session());
    filtered.join().expect("the filtered thread finishes");
}

#[cfg(target_os = "linux")]
fn deny_sigkill_through_pidfd_send_signal_on_this_thread() {
    // Classic BPF over `seccomp_data`: `nr` is at offset 0, `args[1]` (the
    // signal of pidfd_send_signal) at offset 24. Only SIGKILL through
    // pidfd_send_signal is denied; everything else is allowed.
    const BPF_LD_W_ABS: u16 = 0x20;
    const BPF_JEQ_K: u16 = 0x15;
    const BPF_RET_K: u16 = 0x06;
    let program = [
        libc::sock_filter {
            code: BPF_LD_W_ABS,
            jt: 0,
            jf: 0,
            k: 0,
        },
        libc::sock_filter {
            code: BPF_JEQ_K,
            jt: 0,
            jf: 3,
            k: libc::SYS_pidfd_send_signal as u32,
        },
        libc::sock_filter {
            code: BPF_LD_W_ABS,
            jt: 0,
            jf: 0,
            k: 24,
        },
        libc::sock_filter {
            code: BPF_JEQ_K,
            jt: 0,
            jf: 1,
            k: libc::SIGKILL as u32,
        },
        libc::sock_filter {
            code: BPF_RET_K,
            jt: 0,
            jf: 0,
            k: 0x0005_0000 | libc::EPERM as u32,
        },
        libc::sock_filter {
            code: BPF_RET_K,
            jt: 0,
            jf: 0,
            k: 0x7fff_0000,
        },
    ];
    let filter = libc::sock_fprog {
        len: program.len() as u16,
        filter: program.as_ptr() as *mut libc::sock_filter,
    };
    // SAFETY: installs a filter on the calling thread only (no TSYNC flag).
    unsafe {
        assert_eq!(libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0), 0);
        assert_eq!(
            libc::syscall(
                libc::SYS_seccomp,
                1u32,
                0u32,
                &filter as *const libc::sock_fprog
            ),
            0,
            "install the thread-scoped syscall filter"
        );
    }
}
