#![allow(clippy::doc_lazy_continuation)] // Existing compatibility and test seam shape.
//! Writer file-ownership proofs across storage-handle recycle.
//!
//! A writable persistence keeps the same exclusively locked physical file
//! descriptor across the maintenance cycle's internal handle recycle
//! (`RedbPersistence::compact`'s close/reopen of its own `redb::Database`
//! handle). No reader or competing writer -- from this process or another,
//! through the original pathname, a hardlink, or a rename of either -- can
//! acquire the store while the recycle is in flight, and the store's own
//! (device, inode) identity never changes across it. An ordinary routed
//! reader keeps being served by the live owner throughout the recycle, with
//! no retry. Close, Drop-through-unwind, process death, a reopen fault, and
//! a refused competing construction all release real operating-system
//! resources without leaking a descriptor or disturbing the genuine holder.
//!
//! Every probe against a competing open goes through a genuinely separate
//! OS process using the raw `redb` primitive directly: the engine's own
//! self-reported owner state is never trusted as proof of exclusion.
//!
//! Unix-only ((device, inode) identity, hardlinks, and OS advisory locking
//! are POSIX concepts): gated at the `mod` declaration in
//! `tests/engine_seams.rs`, matching the existing `corrupt_store_refused_at_open`
//! and `writable_open_of_a_corrupt_file_names_the_next_step` modules there.

use contextdb_core::read_contract::ReadRoute;
use contextdb_core::{Error, Value};
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    WriterFileLifetime, WriterFileRecycleBoundary, WriterFileRecycleObservation,
    arm_handle_recycle_reopen_fault_for_test, observe_writer_file_recycle_for_test,
};
use contextdb_engine::read_session::{ReadSessionEvent, ReadSessionTestObserver};
use contextdb_engine::{Database, MaintenancePolicy, ReadSession};
use redb::{MultimapTableDefinition, ReadableDatabase, TableDefinition};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const PAYLOAD: TableDefinition<u64, &[u8]> = TableDefinition::new("payload");

/// The routed gap read queued at `AfterViewClose` and joined once
/// `drive_to_recycling_batch` returns: the row count it expected at the time
/// it was queued, and the join handle of the thread performing the read.
type GapReaderSlot = Option<(i64, std::thread::JoinHandle<(ReadRoute, Vec<Vec<Value>>)>)>;

struct CursorHandoffStarted(std::sync::mpsc::Sender<()>);

impl ReadSessionTestObserver for CursorHandoffStarted {
    fn observe_event(&self, event: ReadSessionEvent) {
        if event == ReadSessionEvent::OwnerCursorHandoffStarted {
            self.0
                .send(())
                .expect("signal the registered cursor handoff");
        }
    }
}
const MEMBERS: MultimapTableDefinition<u64, u64> = MultimapTableDefinition::new("members");

const CHILD_ROLE_ENV: &str = "CONTEXTDB_WRITER_OWNERSHIP_CHILD_ROLE";
const CHILD_PATH_ENV: &str = "CONTEXTDB_WRITER_OWNERSHIP_STORE_PATH";
const RAW_PROBE_MARKER: &str = "RAW_WRITER_OWNERSHIP_PROBE=";
const WRITER_OPENED_MARKER: &str = "WRITER_DIES_UNCLOSED_OPENED";

/// Ceiling on any one child process's lifetime from this file. Every helper
/// that spawns a child reaps it (by ordinary exit or by a forced kill once
/// this elapses) before returning, so a later assertion failure in the
/// caller can never leave a zombie or an unbounded wait behind.
const CHILD_BOUNDED_DEADLINE: Duration = Duration::from_secs(15);

/// Run a child process to completion within a bounded deadline, capturing
/// its combined stdout+stderr text. Both pipes are drained by their own
/// threads while the parent polls `try_wait` (never an unbounded
/// `output()`/`read_line`), so a chatty child can never block on a full pipe.
/// The child is always reaped -- on ordinary exit, or by a forced kill once
/// the deadline elapses -- before this function returns, and its text is
/// returned only after the process has actually finished, so a harness line
/// (for example the test binary's own "running N tests" preamble) can never
/// be mistaken for the child's own marker output. The returned flag is true
/// when the deadline forced the kill.
fn run_child_bounded(
    mut command: Command,
    deadline: Duration,
) -> (std::process::ExitStatus, String, bool) {
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("spawn bounded child process: {error}"));
    fn drain(pipe: Option<impl std::io::Read + Send + 'static>) -> std::thread::JoinHandle<String> {
        std::thread::spawn(move || {
            let mut text = String::new();
            if let Some(mut pipe) = pipe {
                let _ = pipe.read_to_string(&mut text);
            }
            text
        })
    }
    let stdout = drain(child.stdout.take());
    let stderr = drain(child.stderr.take());
    // Both pipes are closed once the child has exited, so the drain threads
    // finishing IS the exit event. Waiting on that, rather than polling
    // `try_wait` on a timer, means this returns the moment the child is done
    // and costs nothing while it runs.
    let (exited, child_exited) = std::sync::mpsc::channel::<()>();
    let closed = std::thread::spawn(move || {
        let out = stdout.join().unwrap_or_default();
        let err = stderr.join().unwrap_or_default();
        let _ = exited.send(());
        (out, err)
    });
    let mut timed_out = child_exited.recv_timeout(deadline).is_err();
    if timed_out {
        let _ = child.kill();
    }
    let status = child.wait().unwrap_or_else(|error| {
        panic!("reap bounded child: {error}");
    });
    let (out, err) = closed.join().unwrap_or_default();
    let stdout = out;
    let stderr = err;
    // A child that closed its pipes and then exited within the deadline is
    // not a timeout, whichever of the two the wait observed first.
    if timed_out && status.success() {
        timed_out = false;
    }
    let mut text = stdout;
    text.push_str(&stderr);
    (status, text, timed_out)
}

/// Harness filter for a child process of THIS test binary: `--exact` needs
/// the module-qualified test name. Self-contained so this file adds no
/// dependency on another test module's private helper.
fn child_test_filter(fn_name: &str) -> String {
    match module_path!().split_once("::") {
        Some((_, rest)) => format!("{rest}::{fn_name}"),
        None => fn_name.to_string(),
    }
}

fn child_store_path() -> PathBuf {
    PathBuf::from(std::env::var_os(CHILD_PATH_ENV).expect("child receives a store path"))
}

/// What a raw, engine-independent open attempt against a path found: the
/// exact redb-level primitive that is decisive proof of physical exclusion,
/// never the engine's own self-reported owner state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RawOpenProbeResult {
    Acquired,
    Refused,
    /// Nothing exists at the probed pathname: the open failed with
    /// `NotFound` and could neither observe nor acquire any lock.
    NotFound,
    /// The child completed normally but its open failed for another reason.
    Other,
}

/// The single outcome line a raw probe child prints for `result`.
fn raw_probe_outcome<T>(result: Result<T, redb::DatabaseError>) -> &'static str {
    match result {
        Ok(database) => {
            drop(database);
            "acquired"
        }
        Err(redb::DatabaseError::DatabaseAlreadyOpen) => "refused",
        Err(redb::DatabaseError::Storage(redb::StorageError::Io(error)))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            "not-found"
        }
        Err(error) => {
            eprintln!("raw probe open failed: {error:?}");
            "other"
        }
    }
}

#[test]
fn raw_redb_writer_probe_child() {
    if std::env::var(CHILD_ROLE_ENV).ok().as_deref() != Some("raw-writer") {
        return;
    }
    let outcome = raw_probe_outcome(redb::Builder::new().open(child_store_path()));
    println!("{RAW_PROBE_MARKER}{outcome}");
}

#[test]
fn raw_redb_reader_probe_child() {
    if std::env::var(CHILD_ROLE_ENV).ok().as_deref() != Some("raw-reader") {
        return;
    }
    let outcome = raw_probe_outcome(redb::Builder::new().open_read_only(child_store_path()));
    println!("{RAW_PROBE_MARKER}{outcome}");
}

/// A child that opens the store as a writer and then panics without ever
/// calling `close()` -- the OS process-death path, never an in-process
/// `catch_unwind`. Proves the exclusive lock is released by process exit,
/// not by any explicit engine cleanup code running.
#[test]
fn writer_opens_then_dies_without_closing_child() {
    if std::env::var(CHILD_ROLE_ENV).ok().as_deref() != Some("writer-dies-unclosed") {
        return;
    }
    let db = Database::open(child_store_path()).expect("writer child opens the real store");
    println!("{WRITER_OPENED_MARKER}");
    std::mem::forget(db);
    panic!("deliberate crash: writer never calls close() or lets Drop run");
}

fn spawn_probe(role: &str, fn_name: &str, path: &Path) -> RawOpenProbeResult {
    let mut command =
        Command::new(std::env::current_exe().expect("current integration-test binary"));
    command
        .arg("--exact")
        .arg(child_test_filter(fn_name))
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, role)
        .env(CHILD_PATH_ENV, path);
    let (status, text, timed_out) = run_child_bounded(command, CHILD_BOUNDED_DEADLINE);
    // A probe is decisive only when its child ran exactly the one probe test
    // to a successful exit and printed exactly one outcome. A crash, timeout,
    // filter miss, or garbled output is never read as a refusal. The marker
    // is searched within each line because a single-threaded harness prints
    // "test <name> ... " before the test's own output on the same line.
    let outcomes: Vec<&str> = text
        .lines()
        .filter_map(|line| {
            line.find(RAW_PROBE_MARKER).map(|start| {
                line[start + RAW_PROBE_MARKER.len()..]
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
            })
        })
        .collect();
    assert!(
        !timed_out && status.success() && text.contains("running 1 test") && outcomes.len() == 1,
        "{role} probe child did not complete decisively (status: {status:?}, timed out: \
         {timed_out}, outcome lines: {outcomes:?})\noutput:\n{text}"
    );
    match outcomes[0] {
        "refused" => RawOpenProbeResult::Refused,
        "acquired" => RawOpenProbeResult::Acquired,
        "not-found" => RawOpenProbeResult::NotFound,
        "other" => RawOpenProbeResult::Other,
        unknown => panic!("{role} probe printed an unknown outcome {unknown:?}\noutput:\n{text}"),
    }
}

fn probe_raw_writer(path: &Path) -> RawOpenProbeResult {
    spawn_probe("raw-writer", "raw_redb_writer_probe_child", path)
}

fn probe_raw_reader(path: &Path) -> RawOpenProbeResult {
    spawn_probe("raw-reader", "raw_redb_reader_probe_child", path)
}

/// This process's own device/inode identity for `path`, obtained by an
/// ordinary independent `stat` -- never supplied by, or trusted from, the
/// observation API under test.
fn file_identity(path: &Path) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt as _;
    let meta = std::fs::metadata(path)
        .unwrap_or_else(|error| panic!("stat {path:?} for its device/inode identity: {error}"));
    (meta.dev(), meta.ino())
}

/// Count this process's currently open file descriptors that reference the
/// given (device, inode) identity, via `/proc/self/fd`. This is a genuine
/// per-inode count (never a global, unrelated descriptor count): each entry
/// is itself `stat`-ed and only entries naming the exact identity count.
/// `None` where `/proc` does not exist (non-Linux Unix); every assertion
/// built on this treats `None` as "not checked on this platform", never as
/// a failure.
#[cfg(target_os = "linux")]
fn open_fd_count_for_identity(identity: (u64, u64)) -> Option<usize> {
    use std::os::unix::fs::MetadataExt as _;
    let entries = std::fs::read_dir("/proc/self/fd").ok()?;
    Some(
        entries
            .flatten()
            .filter(|entry| {
                std::fs::metadata(entry.path())
                    .map(|meta| (meta.dev(), meta.ino()) == identity)
                    .unwrap_or(false)
            })
            .count(),
    )
}

#[cfg(not(target_os = "linux"))]
fn open_fd_count_for_identity(_identity: (u64, u64)) -> Option<usize> {
    None
}

/// Count this process's open descriptors whose `/proc/self/fd` target lies
/// beneath `dir` -- the store, its companion files, and any renamed or
/// unlinked name of either -- so a descriptor retained per open on any file
/// of a test's own private directory is visible across repeated cycles.
/// Scoped to the test's directory, never a global descriptor count. `None`
/// where `/proc` does not exist.
#[cfg(target_os = "linux")]
fn open_fd_count_under_directory(dir: &Path) -> Option<usize> {
    let dir = std::fs::canonicalize(dir).ok()?;
    let entries = std::fs::read_dir("/proc/self/fd").ok()?;
    Some(
        entries
            .flatten()
            .filter(|entry| {
                std::fs::read_link(entry.path())
                    .map(|target| target.starts_with(&dir))
                    .unwrap_or(false)
            })
            .count(),
    )
}

#[cfg(not(target_os = "linux"))]
fn open_fd_count_under_directory(_dir: &Path) -> Option<usize> {
    None
}

/// Run `body` on its own thread and re-raise its panic unchanged. The
/// handle-recycle reopen fault is a one-shot thread-local flag consumed only
/// by an actual recycle, so every fault-arming control runs here: an armed
/// flag that a control failed to consume dies with this thread and can never
/// turn a later maintenance call, in this or any other test, into a fault.
fn on_isolated_fault_thread<T: Send>(body: impl FnOnce() -> T + Send) -> T {
    std::thread::scope(|scope| match scope.spawn(body).join() {
        Ok(value) => value,
        Err(payload) => std::panic::resume_unwind(payload),
    })
}

/// The same fixture `storage_compaction_online.rs` builds: enough
/// insert/delete churn on two tables that `redb`'s own fragmentation ratio
/// clears the compaction threshold, so `run_maintenance_cycle` has real,
/// finite work to do rather than an immediate no-op.
fn fragmented_store(path: &Path) {
    let db = redb::Database::create(path).expect("create fixture store");
    let write = db.begin_write().expect("begin fixture write");
    {
        let mut table = write.open_table(PAYLOAD).expect("open payload table");
        let bytes = vec![91_u8; 8192];
        for id in 0..1024 {
            table
                .insert(id, bytes.as_slice())
                .expect("insert fixture row");
        }
        let mut members = write
            .open_multimap_table(MEMBERS)
            .expect("open members table");
        for value in 0..2048 {
            members.insert(1, value).expect("insert fixture member");
        }
    }
    write.commit().expect("commit fixture write");
    let write = db.begin_write().expect("begin fixture delete");
    {
        let mut table = write.open_table(PAYLOAD).expect("open payload table");
        for id in 0..896 {
            table.remove(id).expect("remove fixture row");
        }
    }
    write.commit().expect("commit fixture delete");
    drop(db);
}

fn verify_payload_readable(path: &Path) {
    let db = redb::Database::open(path).expect("reopen fixture store for verification");
    let read = db.begin_read().expect("begin verification read");
    let table = read
        .open_table(PAYLOAD)
        .expect("open payload table for verification");
    for id in 896..1024 {
        assert_eq!(
            table.get(id).unwrap().unwrap().value(),
            vec![91_u8; 8192],
            "surviving payload row {id} unchanged after recycle"
        );
    }
    let members = read
        .open_multimap_table(MEMBERS)
        .expect("open members table for verification");
    assert_eq!(
        members
            .get(1)
            .unwrap()
            .map(|v| v.unwrap().value())
            .collect::<Vec<_>>(),
        (0..2048).collect::<Vec<_>>(),
        "members survive recycle unchanged"
    );
}

/// Drive `run_maintenance_cycle` to the batch that actually recycles the
/// redb handle, with every call wrapped in the observation seam so the
/// boundary that matters (the one where `handle_recycled` ends up true) is
/// caught. Each non-recycling batch commits one recording after its own
/// observations have run, so an observer sees precisely the count committed
/// by prior batches. The recycling batch returns before another recording
/// is appended: its caller can join any queued gap read before the next
/// writer commit, making the read's expected committed image deterministic.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn drive_to_recycling_batch(
    db: &Database,
    observer: Arc<dyn Fn(WriterFileRecycleObservation) + Send + Sync>,
    recordings_committed: &AtomicI64,
) -> contextdb_engine::CompactionReport {
    for id in 0..4096_i64 {
        let report =
            observe_writer_file_recycle_for_test(observer.clone(), || db.run_maintenance_cycle())
                .expect("maintenance cycle succeeds while draining fragmentation");
        if report.compaction.handle_recycled {
            return report.compaction;
        }
        db.execute(
            "INSERT INTO recordings VALUES ($id)",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .expect("recording insert commits between maintenance batches");
        recordings_committed.fetch_add(1, Ordering::SeqCst);
        assert!(
            report.compaction.ran,
            "the admitted maintenance sweep keeps advancing toward the recycling batch"
        );
    }
    panic!("maintenance cycle never reached its recycling batch within the bounded attempt budget");
}

/// The central deterministic-gap proof. At every actual close/reopen
/// boundary of a real `run_maintenance_cycle` call:
///   - the outer descriptor owner is alive and its physical identity is
///     unchanged (continuous ownership, not merely eventual re-acquisition),
///     confirmed by an independent `stat`, never by trusting the
///     observation's own reported identity alone;
///   - a genuinely separate OS process cannot acquire the store as a raw
///     shared (reader) or exclusive (writer) open, at any of the three
///     boundaries;
///   - at the boundary between the old view's close and the fresh view's
///     open (`AfterViewClose`, `live_views == 0`) -- the actual admission
///     window the unfixed recycle opens -- neither a hardlink alias nor a
///     rename of that alias admits a competing writer either;
///   - an ordinary routed reader (`ReadSession`, never a raw redb open) is
///     served by the live owner throughout the gap, with no retry and no
///     stale count;
///   - renaming the ORIGINAL pathname itself away for the rest of the gap,
///     and restoring it only once the fresh view is open, does not disturb
///     the recycle: the retained descriptor is not reacquired by pathname.
/// The writer's own next commit after the cycle completes, and a normal
/// close afterward, both still work.
#[test]
#[ignore = "A writer descriptor does not refuse a raw writer open by a separate process across its recycle boundaries."]
fn writer_descriptor_survives_all_three_recycle_boundaries_and_refuses_every_alias() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("writer_ownership.redb");

    // Seed the format the engine expects, then fragment it via the raw
    // backend exactly as the existing compaction tests do.
    let seed = Database::open(&path).expect("seed open");
    seed.close().expect("seed close");
    drop(seed);
    fragmented_store(&path);
    let before_len = std::fs::metadata(&path)
        .expect("stat fixture before recycle")
        .len();
    let baseline_identity = file_identity(&path);

    let db = Database::open(&path).expect("writer opens the real store for the whole test");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE recordings (id INT PRIMARY KEY)",
        &HashMap::new(),
    )
    .expect("create recordings table");

    let seen_boundaries: Arc<Mutex<Vec<WriterFileRecycleBoundary>>> =
        Arc::new(Mutex::new(Vec::new()));
    let recordings_committed = Arc::new(AtomicI64::new(0));
    let displaced_original: Arc<Mutex<Option<PathBuf>>> = Arc::new(Mutex::new(None));
    // Filled once, at `AfterViewClose`, with the expected row count and the
    // join handle of the routed reader spawned into the gap; joined and
    // asserted on below, once `drive_to_recycling_batch` returns.
    let gap_reader: Arc<Mutex<GapReaderSlot>> = Arc::new(Mutex::new(None));

    let seen_boundaries_for_observer = seen_boundaries.clone();
    let recordings_committed_for_observer = recordings_committed.clone();
    let displaced_original_for_observer = displaced_original.clone();
    let gap_reader_for_observer = gap_reader.clone();
    let path_for_observer = path.clone();
    let dir_for_observer = dir.path().to_path_buf();

    let observer: Arc<dyn Fn(WriterFileRecycleObservation) + Send + Sync> = Arc::new(
        move |observation: WriterFileRecycleObservation| {
            seen_boundaries_for_observer
                .lock()
                .unwrap()
                .push(observation.boundary);

            assert!(
                observation.owner.is_alive(),
                "the outer descriptor owner must remain alive at every recycle boundary \
                 ({:?}); a dropped owner here is exactly the bug this contract repairs",
                observation.boundary
            );
            assert_eq!(
                observation.identity, baseline_identity,
                "the physical (device, inode) identity reported at {:?} matches the identity \
                 stat-ed before the writer ever opened the store: continuous ownership, not \
                 eventual re-acquisition of an equal-looking file",
                observation.boundary
            );

            match observation.boundary {
                WriterFileRecycleBoundary::BeforeViewClose => {
                    assert_eq!(
                        observation.live_views, 1,
                        "the old backend view is still live immediately before its close"
                    );
                    assert_eq!(
                        file_identity(&path_for_observer),
                        baseline_identity,
                        "an independent stat of the original pathname matches, before the gap opens"
                    );
                    assert_eq!(
                        probe_raw_writer(&path_for_observer),
                        RawOpenProbeResult::Refused,
                        "a separate process's raw writer open is refused before the view closes"
                    );
                    assert_eq!(
                        probe_raw_reader(&path_for_observer),
                        RawOpenProbeResult::Refused,
                        "a separate process's raw reader open is refused before the view closes"
                    );
                }
                WriterFileRecycleBoundary::AfterViewClose => {
                    assert_eq!(
                        observation.live_views, 0,
                        "the old view is closed and the fresh view is not yet open: \
                         this is the exact admission window the recycle bug opens today"
                    );

                    // Original path: neither a competing writer nor a
                    // competing reader may slip in from a genuinely separate
                    // process while only the outer descriptor holds the lock.
                    assert_eq!(
                        probe_raw_writer(&path_for_observer),
                        RawOpenProbeResult::Refused,
                        "a separate process's raw writer open must be refused during the gap"
                    );
                    assert_eq!(
                        probe_raw_reader(&path_for_observer),
                        RawOpenProbeResult::Refused,
                        "a separate process's raw shared (reader) open must be refused during the gap"
                    );

                    // Hardlink alias: a second directory entry naming the
                    // same inode must find the same lock, not a fresh,
                    // unlocked file.
                    let hardlink_alias =
                        dir_for_observer.join("writer_ownership.redb.hardlink-alias");
                    std::fs::hard_link(&path_for_observer, &hardlink_alias)
                        .expect("create hardlink alias to the live store during the gap");
                    assert_eq!(
                        file_identity(&hardlink_alias),
                        baseline_identity,
                        "a hardlink alias independently stats to the identical (device, inode) pair"
                    );
                    assert_eq!(
                        probe_raw_writer(&hardlink_alias),
                        RawOpenProbeResult::Refused,
                        "a hardlink alias to the live store must not admit a competing writer"
                    );
                    assert_eq!(
                        probe_raw_reader(&hardlink_alias),
                        RawOpenProbeResult::Refused,
                        "a hardlink alias to the live store must not admit a competing shared reader"
                    );

                    // Renamed inode: rename that alias (never the original
                    // path yet) and probe the new name. The same physical
                    // file, reached by yet another name, must still refuse.
                    let renamed_alias =
                        dir_for_observer.join("writer_ownership.redb.renamed-alias");
                    std::fs::rename(&hardlink_alias, &renamed_alias)
                        .expect("rename the hardlink alias during the gap");
                    assert_eq!(
                        file_identity(&renamed_alias),
                        baseline_identity,
                        "renaming an alias never changes the underlying inode identity"
                    );
                    assert_eq!(
                        probe_raw_writer(&renamed_alias),
                        RawOpenProbeResult::Refused,
                        "a renamed alias of the live store's inode must not admit a competing writer"
                    );
                    assert_eq!(
                        probe_raw_reader(&renamed_alias),
                        RawOpenProbeResult::Refused,
                        "a renamed alias of the live store's inode must not admit a competing shared reader"
                    );
                    std::fs::remove_file(&renamed_alias)
                        .expect("clean up the renamed alias before the fresh view opens");

                    // Ordinary routed admission: an independent reader that
                    // knows nothing about `db` still reaches the live owner
                    // through the normal read-session route, with no retry
                    // and no stale count -- proof of NORMAL routing, which a
                    // raw redb `DatabaseAlreadyOpen` alone (above) does not
                    // establish, since that is only a physical-lock fact.
                    //
                    // This observer callback itself runs synchronously on the
                    // very thread performing the recycle, holding the
                    // store-handle lock for write across the whole gap.
                    // Opening the session is an ordinary routing decision and
                    // stays inline, before the path is displaced below; but
                    // `open_cursor` is the actual read, which needs that same
                    // lock and would deadlock waiting on it from inside the
                    // callback that alone can release it by returning -- a
                    // deadlock the owner never actually has in production,
                    // where a reader merely waits out the reopen. So only the
                    // cursor open/read is issued from a spawned thread. The
                    // handoff signal confirms it registered its file hold
                    // before this callback moves the pathname; the test body
                    // joins the read after the recycle releases the lock.
                    let expected_recordings =
                        recordings_committed_for_observer.load(Ordering::SeqCst);
                    let (handoff_started_tx, handoff_started_rx) = std::sync::mpsc::channel();
                    let gap_session = ReadSession::open_with_observer_for_test(
                        &path_for_observer,
                        Default::default(),
                        Arc::new(CursorHandoffStarted(handoff_started_tx)),
                    )
                    .expect(
                        "an ordinary reader opens a routed session while the owner is mid-recycle",
                    );
                    let gap_reader_handle = std::thread::spawn(move || {
                        let cursor = gap_session
                            .open_cursor("SELECT COUNT(*) FROM recordings", &HashMap::new())
                            .expect(
                            "the routed session executes a read during the recycle gap, with no retry",
                        );
                        let route = cursor.route();
                        let rows = cursor.first_page().rows().to_vec();
                        cursor.close().expect("close the routed cursor");
                        (route, rows)
                    });
                    *gap_reader_for_observer.lock().unwrap() =
                        Some((expected_recordings, gap_reader_handle));
                    handoff_started_rx
                        .recv()
                        .expect("the routed cursor registers its hold before the pathname moves");

                    // Prove recycle uses the RETAINED descriptor, never the
                    // pathname: rename the original path itself away for the
                    // rest of the gap. `AfterViewOpen` restores it before
                    // anything downstream needs it again.
                    let displaced =
                        dir_for_observer.join("writer_ownership.redb.displaced-during-gap");
                    std::fs::rename(&path_for_observer, &displaced)
                        .expect("rename the original path away while its descriptor is held");
                    assert!(
                        !path_for_observer.exists(),
                        "the original pathname is genuinely gone for the remainder of the gap"
                    );
                    assert_eq!(
                        probe_raw_writer(&path_for_observer),
                        RawOpenProbeResult::NotFound,
                        "nothing exists at the original pathname right now, so a raw open there can \
                         only fail outright -- it cannot observe or acquire any lock"
                    );
                    assert_eq!(
                        probe_raw_reader(&path_for_observer),
                        RawOpenProbeResult::NotFound,
                        "a raw shared open at the vacated original pathname finds no file at all"
                    );
                    // The displaced original is the same locked inode under a
                    // new name: still inside the gap, it admits neither a
                    // competing writer nor a competing shared reader.
                    assert_eq!(
                        file_identity(&displaced),
                        baseline_identity,
                        "the displaced original independently stats to the retained identity"
                    );
                    assert_eq!(
                        probe_raw_writer(&displaced),
                        RawOpenProbeResult::Refused,
                        "the displaced original must not admit a competing writer during the gap"
                    );
                    assert_eq!(
                        probe_raw_reader(&displaced),
                        RawOpenProbeResult::Refused,
                        "the displaced original must not admit a competing shared reader during the gap"
                    );
                    *displaced_original_for_observer.lock().unwrap() = Some(displaced);
                }
                WriterFileRecycleBoundary::AfterViewOpen => {
                    // Restore the original pathname before any subsequent
                    // ordinary operation (the writer's own next commit, the
                    // final close, or a raw verification reopen) depends on
                    // it again.
                    if let Some(displaced) = displaced_original_for_observer.lock().unwrap().take()
                    {
                        std::fs::rename(&displaced, &path_for_observer)
                            .expect("restore the original pathname once the fresh view is open");
                    }
                    assert_eq!(
                        observation.live_views, 1,
                        "the fresh backend view is live once the boundary fires"
                    );
                    assert_eq!(
                        file_identity(&path_for_observer),
                        baseline_identity,
                        "the restored original pathname independently stats to the same identity"
                    );
                    assert_eq!(
                        probe_raw_writer(&path_for_observer),
                        RawOpenProbeResult::Refused,
                        "the writer holds the store again immediately once the fresh view opens"
                    );
                    assert_eq!(
                        probe_raw_reader(&path_for_observer),
                        RawOpenProbeResult::Refused,
                        "a separate process's raw reader open is still refused once the fresh view opens"
                    );
                }
            }
        },
    );

    let compaction = drive_to_recycling_batch(&db, observer, &recordings_committed);
    assert!(
        compaction.handle_recycled,
        "the driven batch is the recycling one"
    );
    println!(
        "handle_recycle_micros={} bytes_before={:?} bytes_after={:?}",
        compaction.handle_recycle_micros, compaction.bytes_before, compaction.bytes_after
    );

    // The routed gap read was only queued inside the `AfterViewClose`
    // callback; the lock it waits on is not free until this call returns, so
    // it is only joined and checked here.
    let (expected_recordings, gap_reader_handle) = gap_reader
        .lock()
        .unwrap()
        .take()
        .expect("the AfterViewClose boundary fired and queued the routed gap read");
    let (gap_read_route, gap_read_rows) = gap_reader_handle
        .join()
        .expect("the routed gap read joins without panicking");
    assert_eq!(
        gap_read_route,
        ReadRoute::Owner,
        "an ordinary reader during live-owner maintenance is served by the owner's local channel, \
         never by falling through to the committed file"
    );
    assert_eq!(
        gap_read_rows,
        vec![vec![Value::Int64(expected_recordings)]],
        "the routed read sees exactly the recordings committed by prior maintenance batches, \
         snapshot-correct during the gap"
    );

    let boundaries = seen_boundaries.lock().unwrap().clone();
    assert_eq!(
        boundaries,
        vec![
            WriterFileRecycleBoundary::BeforeViewClose,
            WriterFileRecycleBoundary::AfterViewClose,
            WriterFileRecycleBoundary::AfterViewOpen,
        ],
        "all three boundaries fire, in order, exactly once for the recycling batch"
    );

    // The writer's own next commit after the cycle must succeed -- the
    // writer completes recycle and its next commit without a
    // reader-induced reopen failure, and without the pathname displacement
    // performed above leaving any lasting effect.
    db.execute("INSERT INTO recordings VALUES (999999)", &HashMap::new())
        .expect("writer's own commit position advances immediately after recycle");
    assert_eq!(
        db.execute(
            "SELECT id FROM recordings WHERE id = 999999",
            &HashMap::new()
        )
        .unwrap()
        .rows(),
        vec![vec![Value::Int64(999999)]]
    );

    assert!(
        std::fs::metadata(&path)
            .expect("stat fixture after recycle")
            .len()
            <= before_len,
        "steady-state compaction still shrinks or holds the file; ownership retention is additive, \
         not a regression on the existing recycle/shrink cost"
    );

    // Normal close/drop still fully releases the descriptor: a fresh raw
    // open at the original path succeeds only now.
    assert_eq!(
        probe_raw_writer(&path),
        RawOpenProbeResult::Refused,
        "the writer still holds the store immediately before its own close()"
    );
    db.close().expect("writer closes cleanly");
    drop(db);
    assert_eq!(
        probe_raw_writer(&path),
        RawOpenProbeResult::Acquired,
        "close()/Drop release the retained outer descriptor without a lock leak"
    );
    verify_payload_readable(&path);
}

#[test]
#[ignore = "An authenticated external page hold does not survive a storage handle recycle, so a row written after the recycle is not read back as expected."]
fn authenticated_external_page_holds_survive_an_actual_storage_handle_recycle() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("external_snapshot_recycle.redb");
    let seed = Database::open(&path).expect("seed engine store");
    seed.execute(
        "CREATE TABLE snapshot_items (id INTEGER PRIMARY KEY, value TEXT)",
        &HashMap::new(),
    )
    .expect("create snapshot table");
    seed.execute(
        "INSERT INTO snapshot_items VALUES (1, 'before-recycle')",
        &HashMap::new(),
    )
    .expect("seed snapshot row");
    seed.close().expect("close seed writer");
    drop(seed);
    fragmented_store(&path);

    let reader = ReadSession::open(&path).expect("register pre-writer file snapshot");
    let writer = Database::open(&path).expect("open overtaking writer");
    writer.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    writer
        .execute(
            "CREATE TABLE recordings (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create recycle progress table");
    writer
        .execute(
            "UPDATE snapshot_items SET value = 'after-recycle' WHERE id = 1",
            &HashMap::new(),
        )
        .expect("retire the reader-visible snapshot pages");

    let boundaries = Arc::new(Mutex::new(Vec::new()));
    let observed = boundaries.clone();
    let observer: Arc<dyn Fn(WriterFileRecycleObservation) + Send + Sync> =
        Arc::new(move |event| observed.lock().unwrap().push(event.boundary));
    let recordings = AtomicI64::new(0);
    let compaction = drive_to_recycling_batch(&writer, observer, &recordings);
    assert!(compaction.handle_recycled);
    assert_eq!(
        boundaries.lock().unwrap().as_slice(),
        [
            WriterFileRecycleBoundary::BeforeViewClose,
            WriterFileRecycleBoundary::AfterViewClose,
            WriterFileRecycleBoundary::AfterViewOpen,
        ]
    );

    let retained = writer
        .__external_snapshot_retention_for_test()
        .expect("reopened redb view re-registers the same external root");
    assert_eq!(retained.snapshots, 1);
    assert!(retained.retained_pages > 0);
    assert!(!retained.retained_page_allocations.is_empty());
    let old = reader
        .execute(
            "SELECT value FROM snapshot_items WHERE id = 1",
            &HashMap::new(),
        )
        .expect("hydrated route remains on its pre-recycle snapshot");
    assert_eq!(old.rows(), vec![vec![Value::Text("before-recycle".into())]]);

    drop(reader);
    for id in 100_000..100_003i64 {
        writer
            .execute(
                "INSERT INTO recordings VALUES ($id)",
                &HashMap::from([("id".into(), Value::Int64(id))]),
            )
            .expect("advance reclamation after recycled reader closes");
    }
    assert_eq!(
        writer
            .__external_snapshot_retention_for_test()
            .expect("last handle release reaches the recycled allocator"),
        Default::default()
    );
    writer.close().expect("close recycled writer");
}

/// The failure path at the reopen-fault seam: ownership must remain real
/// while the now-failed persistence is still alive, and release only once
/// its existing close path runs -- proven from the outside by a raw
/// subprocess and by this process's own per-inode descriptor count, rather
/// than by the same in-process handle asking itself whether it still thinks
/// it owns the file.
#[test]
#[ignore = "a failed reopen releases the outer file descriptor while its persistence is still alive"]
fn reopen_fault_holds_real_ownership_while_the_failed_persistence_is_alive_and_releases_after_close()
 {
    on_isolated_fault_thread(reopen_fault_holds_and_releases_after_close_body);
}

fn reopen_fault_holds_and_releases_after_close_body() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("reopen_fault.redb");

    let seed = Database::open(&path).expect("seed open");
    seed.close().expect("seed close");
    drop(seed);
    fragmented_store(&path);
    let identity = file_identity(&path);
    let baseline_fds = open_fd_count_for_identity(identity);

    let db = Database::open(&path).expect("writer opens the real store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE recordings (id INT PRIMARY KEY)",
        &HashMap::new(),
    )
    .expect("create recordings table");

    // Drive to the batch that would otherwise recycle, with the one-shot
    // fault re-armed before every attempt (it is only ever consumed inside
    // the recycle step, so re-arming a batch that never reaches that point
    // is a harmless no-op) so the recycling batch, whenever it lands, is
    // guaranteed to hit it. Track exactly how many recordings actually
    // committed before the fault fires, rather than assuming a specific
    // batch count the fixture's internal chunking does not otherwise
    // promise.
    let mut failed_here = false;
    let mut committed_recordings = 0_i64;
    for id in 0..4096_i64 {
        arm_handle_recycle_reopen_fault_for_test();
        match db.run_maintenance_cycle() {
            Ok(report) => {
                db.execute(
                    "INSERT INTO recordings VALUES ($id)",
                    &HashMap::from([("id".into(), Value::Int64(id))]),
                )
                .expect("recording insert commits between maintenance batches");
                committed_recordings += 1;
                if report.compaction.handle_recycled {
                    panic!(
                        "the armed reopen fault must turn the recycling batch into an error, \
                         not a silent successful recycle"
                    );
                }
            }
            Err(Error::StoreHandleRecycleFailed { .. }) => {
                failed_here = true;
                break;
            }
            Err(other) => panic!("unexpected maintenance failure before recycle: {other:?}"),
        }
    }
    assert!(
        failed_here,
        "the armed reopen fault must be exercised within the bounded attempt budget"
    );

    // Ownership remains real while the now-closed-for-writes persistence is
    // still alive: a raw subprocess cannot acquire either a shared or an
    // exclusive lock on the store, and this process's own per-inode
    // descriptor count is still above baseline.
    assert_eq!(
        probe_raw_writer(&path),
        RawOpenProbeResult::Refused,
        "a failed reopen must not release the outer descriptor while the persistence is alive"
    );
    assert_eq!(
        probe_raw_reader(&path),
        RawOpenProbeResult::Refused,
        "a failed reopen must not admit a competing reader either"
    );
    if let Some(baseline) = baseline_fds {
        assert!(
            open_fd_count_for_identity(identity).unwrap() > baseline,
            "the failed-but-still-alive persistence retains at least one descriptor on this inode"
        );
    }

    // The existing StoreHandleRecycleFailed contract: this handle is left
    // closed for further work, but the file itself is untouched and a
    // subsequent fresh open (after this handle's own close()) recovers
    // every row.
    db.close()
        .expect("existing close path releases ownership after a failed recycle");
    if let Some(baseline) = baseline_fds {
        assert_eq!(
            open_fd_count_for_identity(identity),
            Some(baseline),
            "close() after a failed recycle returns this inode's descriptor count to baseline"
        );
    }
    drop(db);
    assert_eq!(
        probe_raw_writer(&path),
        RawOpenProbeResult::Acquired,
        "close() after a failed recycle still fully releases the descriptor"
    );
    verify_payload_readable(&path);
    let reopened = Database::open(&path).expect("fresh open recovers every row");
    assert_eq!(
        reopened
            .execute("SELECT COUNT(*) FROM recordings", &HashMap::new())
            .unwrap()
            .rows(),
        vec![vec![Value::Int64(committed_recordings)]],
        "every recording committed before the failed recycle is durable"
    );
    reopened.close().expect("close the reopened handle");
}

/// A writer that dies (process exit via panic, never an explicit `close()`)
/// releases its exclusive lock through the OS itself, not through any
/// engine cleanup code. The child's marker and exit status are read only
/// after the child has fully, boundedly exited -- never by assuming the
/// FIRST line of its output is the marker, which a Rust test binary's own
/// harness preamble ("running N tests") can otherwise be mistaken for.
#[test]
fn a_writer_process_that_dies_unclosed_still_releases_its_lock() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("panic_release.redb");
    let seed = Database::open(&path).expect("seed open");
    seed.close().expect("seed close");
    drop(seed);

    assert_eq!(
        probe_raw_writer(&path),
        RawOpenProbeResult::Acquired,
        "the store is free before the crashing child ever opens it"
    );

    let mut command =
        Command::new(std::env::current_exe().expect("current integration-test binary"));
    command
        .arg("--exact")
        .arg(child_test_filter(
            "writer_opens_then_dies_without_closing_child",
        ))
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, "writer-dies-unclosed")
        .env(CHILD_PATH_ENV, &path);
    let (status, text, timed_out) = run_child_bounded(command, CHILD_BOUNDED_DEADLINE);
    assert!(
        !timed_out,
        "the crashing child must exit on its own within the bounded deadline\noutput:\n{text}"
    );
    assert!(
        !status.success(),
        "the child is expected to exit via a deliberate panic; got {status:?}\noutput:\n{text}"
    );
    assert!(
        text.contains(WRITER_OPENED_MARKER),
        "the crashing child must actually hold the lock before it dies\noutput:\n{text}"
    );

    assert_eq!(
        probe_raw_writer(&path),
        RawOpenProbeResult::Acquired,
        "process death (panic/unwind, no close()) must still release the exclusive lock \
         without leaving it held forever"
    );
}

/// Drop running through an in-process panic/unwind -- never a process exit
/// -- still releases the descriptor. This is the complement to the process-
/// death proof above: nothing here ever calls `std::process::exit`, so any
/// release observed is Rust's own unwind-time `Drop for Database` running,
/// not the OS reclaiming a dead process's file table.
#[test]
fn panic_unwind_through_a_scope_without_close_still_drops_and_releases_the_descriptor() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("panic_unwind.redb");
    let seed = Database::open(&path).expect("seed open");
    seed.close().expect("seed close");
    drop(seed);
    let identity = file_identity(&path);
    let baseline_fds = open_fd_count_for_identity(identity);

    // The panic hook is left untouched: tests in this binary run
    // concurrently, so the expected panic message is simply printed.
    let path_for_closure = path.clone();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let db = Database::open(&path_for_closure)
            .expect("writer opens the real store inside the scope");
        db.execute(
            "CREATE TABLE recordings (id INT PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create recordings table before the panic");
        // `db` is never explicitly closed here: only ordinary unwind-time
        // Drop may release it.
        panic!("deliberate in-process panic: unwinding must still drop `db`");
    }));
    assert!(outcome.is_err(), "the scope is expected to panic");

    if let Some(baseline) = baseline_fds {
        assert_eq!(
            open_fd_count_for_identity(identity),
            Some(baseline),
            "unwinding through the scope ran Drop for Database and released the descriptor, \
             back to baseline, with no OS process ever exiting"
        );
    }
    assert_eq!(
        probe_raw_writer(&path),
        RawOpenProbeResult::Acquired,
        "in-process panic/unwind (not process death) still released the exclusive lock via Drop"
    );
}

/// Explicit `close()` releases the descriptor while the `Database` value
/// itself is still alive and in scope -- proving the release happens inside
/// `close()`, never merely queued for a later `Drop` that has not run yet.
#[test]
#[ignore = "A live writer does not refuse a raw open of its store file by a separate process."]
fn explicit_close_releases_the_descriptor_while_the_database_value_is_still_alive() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("explicit_close.redb");
    let seed = Database::open(&path).expect("seed open");
    seed.close().expect("seed close");
    drop(seed);
    let identity = file_identity(&path);
    let baseline_fds = open_fd_count_for_identity(identity);

    let db = Database::open(&path).expect("writer opens the real store");
    db.execute(
        "CREATE TABLE recordings (id INT PRIMARY KEY)",
        &HashMap::new(),
    )
    .expect("create recordings table");
    if let Some(baseline) = baseline_fds {
        assert!(
            open_fd_count_for_identity(identity).unwrap() > baseline,
            "an open writer holds at least one more descriptor on this inode than baseline"
        );
    }
    assert_eq!(
        probe_raw_writer(&path),
        RawOpenProbeResult::Refused,
        "a live writer refuses a separate process's raw open"
    );

    // `db` is still a live, unmoved value across this call.
    db.close()
        .expect("explicit close releases the writer's descriptor");
    if let Some(baseline) = baseline_fds {
        assert_eq!(
            open_fd_count_for_identity(identity),
            Some(baseline),
            "close() alone returns this inode's descriptor count to baseline while `db` is \
             still alive and in scope"
        );
    }
    assert_eq!(
        probe_raw_writer(&path),
        RawOpenProbeResult::Acquired,
        "close() alone (the db value not yet dropped) already lets a separate process acquire \
         the store"
    );
    drop(db);
    if let Some(baseline) = baseline_fds {
        assert_eq!(
            open_fd_count_for_identity(identity),
            Some(baseline),
            "dropping an already-closed handle is a no-op on the descriptor count"
        );
    }
}

/// A writer that unwinds out of maintenance from inside the actual gap --
/// the old view closed, the fresh view not yet open -- is dropped during
/// that unwind and releases everything it held. The panic is raised by the
/// observer at `AfterViewClose` within an ordinary recycling
/// `run_maintenance_cycle`, and caught only outside the scope that owns the
/// `Database`, so the release observed here is the unwind-time `Drop` of a
/// writer whose recycle never completed. Per-inode and per-directory
/// descriptor counts and separate-process raw opens decide the release; the
/// store then reopens with every committed recording and keeps committing.
#[test]
fn panic_unwinding_through_the_after_view_close_boundary_releases_the_descriptor_and_reopens() {
    const UNWIND_MARKER: &str = "deliberate unwind through the after-view-close recycle boundary";

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("unwind_through_gap.redb");
    let seed = Database::open(&path).expect("seed open");
    seed.close().expect("seed close");
    drop(seed);
    fragmented_store(&path);
    let identity = file_identity(&path);
    let baseline_inode_fds = open_fd_count_for_identity(identity);
    let baseline_dir_fds = open_fd_count_under_directory(dir.path());

    let recordings_committed = Arc::new(AtomicI64::new(0));
    let reached_gap = Arc::new(AtomicBool::new(false));
    let gap_boundaries: Arc<Mutex<Vec<WriterFileRecycleBoundary>>> =
        Arc::new(Mutex::new(Vec::new()));

    // The panic hook is left untouched: tests in this binary run
    // concurrently, so the expected panic message is simply printed.
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let db = Database::open(&path).expect("writer opens the fragmented store");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.execute(
            "CREATE TABLE recordings (id INT PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create recordings table");

        let reached_gap_for_observer = reached_gap.clone();
        let gap_boundaries_for_observer = gap_boundaries.clone();
        let observer: Arc<dyn Fn(WriterFileRecycleObservation) + Send + Sync> =
            Arc::new(move |observation: WriterFileRecycleObservation| {
                gap_boundaries_for_observer
                    .lock()
                    .unwrap()
                    .push(observation.boundary);
                if observation.boundary != WriterFileRecycleBoundary::AfterViewClose {
                    return;
                }
                if let Some(baseline) = baseline_inode_fds {
                    assert!(
                        open_fd_count_for_identity(identity).unwrap() > baseline,
                        "inside the gap this process still holds a descriptor on the store inode"
                    );
                }
                reached_gap_for_observer.store(true, Ordering::SeqCst);
                std::panic::panic_any(UNWIND_MARKER);
            });
        drive_to_recycling_batch(&db, observer, &recordings_committed);
        unreachable!("the recycling batch unwinds from its after-view-close boundary");
    }));

    let payload = match outcome {
        Err(payload) => payload,
        Ok(()) => panic!("the recycling batch must unwind out of the Database scope"),
    };
    let payload_text = payload
        .downcast_ref::<&str>()
        .map(|text| text.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned());
    assert_eq!(
        payload_text.as_deref(),
        Some(UNWIND_MARKER),
        "the observer's own panic unwinds through maintenance unchanged, rather than being \
         converted into an error or replaced by a different panic"
    );
    assert!(
        reached_gap.load(Ordering::SeqCst),
        "the unwind started inside the actual recycle gap"
    );
    assert_eq!(
        gap_boundaries.lock().unwrap().clone(),
        vec![
            WriterFileRecycleBoundary::BeforeViewClose,
            WriterFileRecycleBoundary::AfterViewClose,
        ],
        "the unwind left the recycle before any fresh view opened"
    );

    if let Some(baseline) = baseline_inode_fds {
        assert_eq!(
            open_fd_count_for_identity(identity),
            Some(baseline),
            "unwinding out of the gap dropped the writer and returned this inode's descriptor \
             count to baseline"
        );
    }
    if let Some(baseline) = baseline_dir_fds {
        assert_eq!(
            open_fd_count_under_directory(dir.path()),
            Some(baseline),
            "no descriptor on the store or its companion files survives the unwind"
        );
    }
    assert_eq!(
        probe_raw_writer(&path),
        RawOpenProbeResult::Acquired,
        "after the unwind a separate process acquires the store exclusively"
    );
    assert_eq!(
        probe_raw_reader(&path),
        RawOpenProbeResult::Acquired,
        "after the unwind a separate process acquires the store shared"
    );

    let committed = recordings_committed.load(Ordering::SeqCst);
    let reopened = Database::open(&path)
        .expect("the same process reopens the store after the writer unwound out of the gap");
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        reopened
            .execute("SELECT COUNT(*) FROM recordings", &HashMap::new())
            .unwrap()
            .rows(),
        vec![vec![Value::Int64(committed)]],
        "every recording committed before the unwinding batch is durable"
    );
    reopened
        .execute("INSERT INTO recordings VALUES (999999)", &HashMap::new())
        .expect("the reopened writer commits");
    reopened
        .run_maintenance_cycle()
        .expect("the reopened writer runs ordinary maintenance");
    reopened.close().expect("close the reopened writer");
    drop(reopened);
    if let Some(baseline) = baseline_inode_fds {
        assert_eq!(
            open_fd_count_for_identity(identity),
            Some(baseline),
            "the reopened writer's close returns the inode's descriptor count to baseline"
        );
    }
    verify_payload_readable(&path);
}

/// Open a freshly fragmented store at `dir/name` for a descriptor-cycle
/// control, returning the writer, the store's independently stat-ed
/// identity, and that inode's descriptor count before the writer opened it.
fn open_fragmented_writer(
    dir: &Path,
    name: &str,
) -> (Database, PathBuf, (u64, u64), Option<usize>) {
    let path = dir.join(name);
    let seed = Database::open(&path).unwrap_or_else(|error| panic!("{name}: seed open: {error}"));
    seed.close()
        .unwrap_or_else(|error| panic!("{name}: seed close: {error}"));
    drop(seed);
    fragmented_store(&path);
    let identity = file_identity(&path);
    let baseline = open_fd_count_for_identity(identity);
    let db = Database::open(&path).unwrap_or_else(|error| panic!("{name}: open: {error}"));
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE recordings (id INT PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap_or_else(|error| panic!("{name}: create recordings table: {error}"));
    if let Some(baseline) = baseline {
        assert!(
            open_fd_count_for_identity(identity).unwrap() > baseline,
            "{name}: an open writer holds an extra descriptor on this inode"
        );
    }
    (db, path, identity, baseline)
}

fn assert_inode_fds(identity: (u64, u64), baseline: Option<usize>, context: &str) {
    if let Some(baseline) = baseline {
        assert_eq!(
            open_fd_count_for_identity(identity),
            Some(baseline),
            "{context}: this inode's descriptor count returns to baseline"
        );
    }
}

/// One ordinary cycle: an actual recycling maintenance batch (all three
/// boundaries, a descriptor held on the inode inside the gap), the next
/// commit, then an explicit close that returns the inode to baseline while
/// the `Database` value is still alive.
fn ordinary_recycle_cycle_releases(dir: &Path, name: &str) {
    let (db, path, identity, baseline) = open_fragmented_writer(dir, name);
    let boundaries: Arc<Mutex<Vec<WriterFileRecycleBoundary>>> = Arc::new(Mutex::new(Vec::new()));
    let boundaries_for_observer = boundaries.clone();
    let observer: Arc<dyn Fn(WriterFileRecycleObservation) + Send + Sync> =
        Arc::new(move |observation: WriterFileRecycleObservation| {
            boundaries_for_observer
                .lock()
                .unwrap()
                .push(observation.boundary);
            if observation.boundary == WriterFileRecycleBoundary::AfterViewClose
                && let Some(baseline) = baseline
            {
                assert!(
                    open_fd_count_for_identity(identity).unwrap() > baseline,
                    "inside the gap this process still holds a descriptor on the store inode"
                );
            }
        });
    let recordings = AtomicI64::new(0);
    let compaction = drive_to_recycling_batch(&db, observer, &recordings);
    assert!(
        compaction.handle_recycled,
        "{name}: the ordinary cycle performed an actual recycle"
    );
    assert_eq!(
        boundaries.lock().unwrap().clone(),
        vec![
            WriterFileRecycleBoundary::BeforeViewClose,
            WriterFileRecycleBoundary::AfterViewClose,
            WriterFileRecycleBoundary::AfterViewOpen,
        ],
        "{name}: the recycle crossed all three boundaries exactly once"
    );
    db.execute("INSERT INTO recordings VALUES (999999)", &HashMap::new())
        .unwrap_or_else(|error| panic!("{name}: commit after recycle: {error}"));
    db.close()
        .unwrap_or_else(|error| panic!("{name}: ordinary close: {error}"));
    assert_inode_fds(identity, baseline, &format!("{name}: ordinary close"));
    drop(db);
    assert_inode_fds(identity, baseline, &format!("{name}: drop after close"));
    assert_eq!(
        probe_raw_writer(&path),
        RawOpenProbeResult::Acquired,
        "{name}: a separate process acquires the store after the recycled writer closed"
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailedWriterRelease {
    ExplicitClose,
    DropWithoutClose,
}

/// One fault cycle: the recycling batch hits the reopen fault and returns
/// `StoreHandleRecycleFailed`; while the failed writer is alive it still
/// holds a descriptor and refuses separate-process opens; the chosen release
/// then returns the inode to baseline and frees the lock; a fresh open
/// recovers every committed recording.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn fault_cycle_releases(dir: &Path, name: &str, release: FailedWriterRelease) {
    let (db, path, identity, baseline) = open_fragmented_writer(dir, name);
    let boundaries: Arc<Mutex<Vec<WriterFileRecycleBoundary>>> = Arc::new(Mutex::new(Vec::new()));
    let owner: Arc<Mutex<Option<WriterFileLifetime>>> = Arc::new(Mutex::new(None));
    let boundaries_for_observer = boundaries.clone();
    let owner_for_observer = owner.clone();
    let observer: Arc<dyn Fn(WriterFileRecycleObservation) + Send + Sync> =
        Arc::new(move |observation: WriterFileRecycleObservation| {
            boundaries_for_observer
                .lock()
                .unwrap()
                .push(observation.boundary);
            *owner_for_observer.lock().unwrap() = Some(observation.owner.clone());
        });

    let mut committed = 0_i64;
    let mut faulted = false;
    for id in 0..4096_i64 {
        let outcome = observe_writer_file_recycle_for_test(observer.clone(), || {
            arm_handle_recycle_reopen_fault_for_test();
            db.run_maintenance_cycle()
        });
        match outcome {
            Ok(report) => {
                assert!(
                    !report.compaction.handle_recycled,
                    "{name}: the armed reopen fault must turn the recycling batch into an error"
                );
                assert!(
                    report.compaction.ran,
                    "{name}: the admitted maintenance sweep keeps advancing"
                );
                db.execute(
                    "INSERT INTO recordings VALUES ($id)",
                    &HashMap::from([("id".into(), Value::Int64(id))]),
                )
                .unwrap_or_else(|error| panic!("{name}: recording insert: {error}"));
                committed += 1;
            }
            Err(Error::StoreHandleRecycleFailed { .. }) => {
                faulted = true;
                break;
            }
            Err(other) => panic!("{name}: unexpected maintenance failure: {other:?}"),
        }
    }
    assert!(
        faulted,
        "{name}: the reopen fault was exercised within the bounded attempt budget"
    );
    let seen = boundaries.lock().unwrap().clone();
    assert_eq!(
        seen.first(),
        Some(&WriterFileRecycleBoundary::BeforeViewClose),
        "{name}: the failing batch reached the actual recycle, not an earlier error"
    );
    assert!(
        !seen.contains(&WriterFileRecycleBoundary::AfterViewOpen),
        "{name}: no fresh view opened in the faulted recycle: {seen:?}"
    );

    if let Some(baseline) = baseline {
        assert!(
            open_fd_count_for_identity(identity).unwrap() > baseline,
            "{name}: the failed writer still holds a descriptor on the inode while alive"
        );
    }
    assert_eq!(
        probe_raw_writer(&path),
        RawOpenProbeResult::Refused,
        "{name}: the failed writer still refuses a separate process's exclusive open"
    );
    assert_eq!(
        probe_raw_reader(&path),
        RawOpenProbeResult::Refused,
        "{name}: the failed writer still refuses a separate process's shared open"
    );

    match release {
        FailedWriterRelease::ExplicitClose => {
            db.close()
                .unwrap_or_else(|error| panic!("{name}: close after fault: {error}"));
            assert_inode_fds(identity, baseline, &format!("{name}: close after fault"));
            assert_eq!(
                probe_raw_writer(&path),
                RawOpenProbeResult::Acquired,
                "{name}: close after the fault frees the lock while the value is still alive"
            );
            drop(db);
        }
        FailedWriterRelease::DropWithoutClose => {
            drop(db);
            assert_inode_fds(identity, baseline, &format!("{name}: drop after fault"));
            assert_eq!(
                probe_raw_writer(&path),
                RawOpenProbeResult::Acquired,
                "{name}: dropping the failed writer without close frees the lock"
            );
        }
    }
    assert_inode_fds(identity, baseline, &format!("{name}: after release"));
    // Supplementary only: the descriptor counts and raw opens above decide
    // the release; a dead weak owner merely agrees with them.
    if let Some(owner) = owner.lock().unwrap().as_ref() {
        assert!(
            !owner.is_alive(),
            "{name}: no descriptor owner outlives the released writer"
        );
    }

    let reopened = Database::open(&path)
        .unwrap_or_else(|error| panic!("{name}: reopen after released fault: {error}"));
    assert_eq!(
        reopened
            .execute("SELECT COUNT(*) FROM recordings", &HashMap::new())
            .unwrap()
            .rows(),
        vec![vec![Value::Int64(committed)]],
        "{name}: every recording committed before the fault is durable"
    );
    reopened
        .close()
        .unwrap_or_else(|error| panic!("{name}: close reopened writer: {error}"));
    drop(reopened);
    assert_inode_fds(
        identity,
        baseline,
        &format!("{name}: after reopen and close"),
    );
}

/// Repeated ordinary recycle cycles and reopen-fault cycles each return
/// every descriptor to baseline. Each iteration seeds its own fragmented
/// store, so every ordinary cycle performs an actual recycle and every fault
/// cycle an actual `StoreHandleRecycleFailed`; a batch that neither recycles
/// nor faults fails the control. The failed writer is released by explicit
/// close in one control and by drop without close in another. Descriptor
/// counts per inode and across the test's whole directory, plus
/// separate-process raw opens, decide release -- a leak retained per open
/// accumulates in the directory count across rounds. Fault cycles run on
/// isolated threads so an armed fault never reaches another cycle or test.
#[test]
#[ignore = "a failed writer stops refusing a separate process exclusive open after a failed reopen cycle"]
fn repeated_close_recycle_and_reopen_fault_cycles_return_descriptor_count_to_baseline() {
    let dir = tempfile::tempdir().expect("tempdir");
    let baseline_dir_fds = open_fd_count_under_directory(dir.path());

    for round in 0..2 {
        ordinary_recycle_cycle_releases(dir.path(), &format!("round{round}_ordinary.redb"));
        on_isolated_fault_thread(|| {
            fault_cycle_releases(
                dir.path(),
                &format!("round{round}_fault_close.redb"),
                FailedWriterRelease::ExplicitClose,
            )
        });
        on_isolated_fault_thread(|| {
            fault_cycle_releases(
                dir.path(),
                &format!("round{round}_fault_drop.redb"),
                FailedWriterRelease::DropWithoutClose,
            )
        });
        // A cycle after the fault cycles still recycles normally on this
        // thread, which never armed the fault.
        ordinary_recycle_cycle_releases(dir.path(), &format!("round{round}_after_faults.redb"));
        if let Some(baseline) = baseline_dir_fds {
            assert_eq!(
                open_fd_count_under_directory(dir.path()),
                Some(baseline),
                "round {round}: no descriptor on any store or companion file accumulates"
            );
        }
    }
}

/// A writer refused because the store is already held by another writer
/// neither perturbs the real holder nor leaks anything: the real holder
/// keeps working, and a THIRD open only succeeds after the real holder's
/// own close().
///
/// A second `Database::open` of the SAME path from THIS SAME process is
/// refused by the engine's own in-process open registry before persistence
/// is even attempted (retrying for a bounded interval in case the first
/// holder is about to release, then failing): that is
/// [`Error::DatabaseLocked`], carrying this process's own id, never the
/// cross-process typed `HeldByWriter` read failure -- that class is what a
/// SEPARATE process observes when the physical companion lock refuses it.
#[test]
#[ignore = "A live writer does not refuse a raw open of its store file by a separate process, so a refused second construction does not leave the holder in sole ownership."]
fn a_refused_second_writer_construction_leaves_the_real_holder_and_its_lock_untouched() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("failed_construction.redb");

    let holder = Database::open(&path).expect("first writer opens the store");
    holder
        .execute(
            "CREATE TABLE recordings (id INT PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("first writer creates its table");

    let refused = Database::open(&path);
    match refused {
        Err(Error::DatabaseLocked { holder_pid, .. }) => {
            assert_eq!(
                holder_pid,
                std::process::id(),
                "a same-process second open names THIS process as the holder"
            );
        }
        Err(other) => panic!("expected a same-process DatabaseLocked refusal, got {other:?}"),
        Ok(_) => panic!("a second writer construction must be refused while the first is open"),
    }

    // The real holder is unperturbed by the failed construction attempt.
    holder
        .execute("INSERT INTO recordings VALUES (1)", &HashMap::new())
        .expect("the real holder keeps committing after a refused competitor");
    assert_eq!(
        holder
            .execute("SELECT id FROM recordings", &HashMap::new())
            .unwrap()
            .rows(),
        vec![vec![Value::Int64(1)]]
    );

    // The failed attempt leaked nothing: only after the real holder closes
    // does a fresh raw open succeed.
    assert_eq!(
        probe_raw_writer(&path),
        RawOpenProbeResult::Refused,
        "the real holder still owns the store after the refused competing construction"
    );
    holder.close().expect("real holder closes cleanly");
    drop(holder);
    assert_eq!(
        probe_raw_writer(&path),
        RawOpenProbeResult::Acquired,
        "the refused construction attempt left no descriptor behind to leak"
    );
}
