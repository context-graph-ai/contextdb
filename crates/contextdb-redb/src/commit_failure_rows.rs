//! A commit that failed past its point of no return refuses every later writer.
//!
//! Each sweep takes its positions from a successful control's event count, then
//! fails one backend call at each position, once, on a healing backend.

use crate::backends::FileBackend;
use crate::{
    CommitError, Database, ReadableDatabase, StorageBackend, StorageError, TableDefinition,
    TransactionError, WriteTransaction,
};
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

const KEYS: u64 = 200;
const REWRITTEN: TableDefinition<u64, &[u8]> = TableDefinition::new("rewritten");
const UNTOUCHED: TableDefinition<u64, &[u8]> = TableDefinition::new("untouched");
const LAST_COMMITTED: u8 = 2;
const FAILED_COMMIT: u8 = 200;
const UNTOUCHED_VALUE: u8 = 99;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Op {
    Read = 1,
    Write = 2,
    Sync = 3,
}

impl Op {
    const ALL: [Self; 3] = [Self::Read, Self::Write, Self::Sync];
    fn name(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Sync => "sync",
        }
    }
    fn parse(name: &str) -> Self {
        Self::ALL.into_iter().find(|op| op.name() == name).unwrap()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Route {
    Ordinary,
    Publishing,
    Prepared,
}

impl Route {
    const ALL: [Self; 3] = [Self::Ordinary, Self::Publishing, Self::Prepared];
    fn name(self) -> &'static str {
        match self {
            Self::Ordinary => "ordinary",
            Self::Publishing => "publishing",
            Self::Prepared => "prepared",
        }
    }
    fn parse(name: &str) -> Self {
        Self::ALL
            .into_iter()
            .find(|route| route.name() == name)
            .unwrap()
    }
}

#[derive(Debug, Default)]
pub(super) struct Gate {
    armed: AtomicU8,
    remaining: AtomicU64,
    fired: AtomicBool,
    unwind: AtomicBool,
    counting: AtomicBool,
    counts: [AtomicU64; 3],
}

impl Gate {
    fn arm(&self, op: Op, position: u64) {
        self.remaining.store(position, Ordering::SeqCst);
        self.armed.store(op as u8, Ordering::SeqCst);
    }
    pub(super) fn arm_unwind(&self, position: u64) {
        self.unwind.store(true, Ordering::SeqCst);
        self.arm(Op::Read, position);
    }
    fn count(&self) {
        self.counting.store(true, Ordering::SeqCst);
    }
    fn counted(&self, op: Op) -> u64 {
        self.counts[op as usize - 1].load(Ordering::SeqCst)
    }
    pub(super) fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }
    fn pass(&self, op: Op) -> Result<(), std::io::Error> {
        if self.counting.load(Ordering::SeqCst) {
            self.counts[op as usize - 1].fetch_add(1, Ordering::SeqCst);
        }
        if self.armed.load(Ordering::SeqCst) != op as u8 {
            return Ok(());
        }
        let remaining = self.remaining.load(Ordering::SeqCst);
        if remaining > 0 {
            self.remaining.store(remaining - 1, Ordering::SeqCst);
            return Ok(());
        }
        self.armed.store(0, Ordering::SeqCst);
        self.fired.store(true, Ordering::SeqCst);
        assert!(
            !self.unwind.load(Ordering::SeqCst),
            "injected backend unwind"
        );
        Err(std::io::Error::from(ErrorKind::Other))
    }
}

#[derive(Debug)]
struct OneShotFaultBackend {
    inner: FileBackend,
    gate: Arc<Gate>,
}

impl StorageBackend for OneShotFaultBackend {
    fn len(&self) -> Result<u64, std::io::Error> {
        self.inner.len()
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), std::io::Error> {
        self.gate.pass(Op::Read)?;
        self.inner.read(offset, out)
    }
    fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
        self.inner.set_len(len)
    }
    fn sync_data(&self) -> Result<(), std::io::Error> {
        self.gate.pass(Op::Sync)?;
        self.inner.sync_data()
    }
    fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
        self.gate.pass(Op::Write)?;
        self.inner.write(offset, data)
    }
}

fn value(round: u8) -> Vec<u8> {
    vec![round; 200]
}

/// Three rewrites leave freed pages the next commit reclaims, and one table no
/// later write touches, so an overwritten live page reads back as wrong data.
pub(super) fn seeded(path: &Path) -> (Database, Arc<Gate>) {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .unwrap();
    let gate = Arc::new(Gate::default());
    let backend = OneShotFaultBackend {
        inner: FileBackend::new(file).unwrap(),
        gate: Arc::clone(&gate),
    };
    let db = Database::builder()
        .set_cache_size(0)
        .create_with_backend(backend)
        .unwrap();
    let tx = db.begin_write().unwrap();
    {
        let mut untouched = tx.open_table(UNTOUCHED).unwrap();
        for key in 0..KEYS {
            untouched
                .insert(key, value(UNTOUCHED_VALUE).as_slice())
                .unwrap();
        }
    }
    tx.commit().unwrap();
    for round in 0..=LAST_COMMITTED {
        rewrite(&db, round).commit().unwrap();
    }
    (db, gate)
}

fn rewrite(db: &Database, round: u8) -> WriteTransaction {
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(REWRITTEN).unwrap();
        for key in 0..KEYS {
            table.insert(key, value(round).as_slice()).unwrap();
        }
    }
    tx
}

/// Prepared preparation reads before the boundary, so the fault arms after it.
pub(super) fn commit_through(
    tx: WriteTransaction,
    route: Route,
    arm: impl FnOnce(),
) -> Result<(), CommitError> {
    match route {
        Route::Ordinary => {
            arm();
            tx.commit()
        }
        Route::Publishing => {
            let marker = tx.durable_publication();
            arm();
            tx.commit_publishing(&marker)
        }
        Route::Prepared => {
            let mut prepared = tx
                .prepare_commit(|error| error, |_| Ok::<(), StorageError>(()))
                .unwrap();
            arm();
            prepared.commit()
        }
    }
}

/// A commit's read count differs between runs, because its page lists follow hash
/// order: over 200 controls each route saw three counts about equally often, 34 to
/// 36, or 11 to 13 for prepared, and shutdown saw 17 to 19. So a run can end short of
/// a position its control reached. Such a position runs again on a fresh store, until
/// it is reached. Every position the control saw must be reached. At 1 in 3 per try,
/// 64 tries all missing is not a thing a run will see.
const TRIES_FOR_A_POSITION_THE_CONTROL_SAW: u32 = 64;

/// A child spawned on another test thread holds a copy of every open file of
/// this process until its exec closes it, and with it that file's lock. A
/// reopen inside that window would see `DatabaseAlreadyOpen`. Spawning and
/// reopening take this in turn; a spawn returns only after the exec.
static SPAWN_OR_REOPEN: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// `case` runs one position on a fresh store and says whether its commit got there.
fn sweep_positions(control: u64, context: &str, mut case: impl FnMut(u64) -> bool) -> u64 {
    assert!(control > 0, "{context}: control reached no event");
    let mut position = 0;
    loop {
        let tries = if position < control {
            TRIES_FOR_A_POSITION_THE_CONTROL_SAW
        } else {
            1
        };
        if !(0..tries).any(|_| case(position)) {
            break;
        }
        position += 1;
    }
    assert!(
        position >= control,
        "{context}: position {position} of the control's {control} was never reached"
    );
    position
}

fn control_positions(route: Route, op: Op) -> u64 {
    let temp = crate::create_tempfile();
    let (db, gate) = seeded(temp.path());
    let tx = rewrite(&db, FAILED_COMMIT);
    commit_through(tx, route, || gate.count()).unwrap();
    gate.counted(op)
}

pub(super) fn assert_refused(db: &Database, context: &str) {
    let refused = db.begin_write().err();
    assert!(
        matches!(
            refused,
            Some(TransactionError::Storage(StorageError::PreviousIo))
        ),
        "{context}: the next writer must be refused with PreviousIo, got {refused:?}"
    );
}

fn recovery_flag_set(path: &Path) -> bool {
    let mut file = std::fs::File::open(path).unwrap();
    file.seek(SeekFrom::Start(9)).unwrap();
    let mut byte = [0u8];
    file.read_exact(&mut byte).unwrap();
    byte[0] & 2 != 0
}

/// Reopen repairs. Then the integrity check is clean, the rewritten table holds
/// one commit's value whole, and the untouched table is exact.
pub(super) fn assert_recovered(path: &Path, allowed: &[u8], context: &str) {
    let mut db = {
        let _turn = SPAWN_OR_REOPEN.lock().unwrap_or_else(|e| e.into_inner());
        Database::builder().create(path).unwrap()
    };
    let integrity = db.check_integrity();
    assert!(matches!(integrity, Ok(true)), "{context}: {integrity:?}");
    let read = db.begin_read().unwrap();
    let table = read.open_table(REWRITTEN).unwrap();
    let first = table.get(0).unwrap().unwrap().value().to_vec();
    assert!(
        allowed.iter().any(|round| first == value(*round)),
        "{context}: rewritten holds value {}",
        first[0]
    );
    for key in 0..KEYS {
        assert_eq!(
            table.get(key).unwrap().unwrap().value(),
            first.as_slice(),
            "{context}: key {key} is not from one commit"
        );
    }
    let untouched = read.open_table(UNTOUCHED).unwrap();
    for key in 0..KEYS {
        assert_eq!(
            untouched.get(key).unwrap().unwrap().value(),
            value(UNTOUCHED_VALUE).as_slice(),
            "{context}: untouched key {key}"
        );
    }
}

/// Every valid committed reader returns exact committed bytes, through a cold
/// point lookup and through a range iterator that holds the root page.
fn committed_reads(read: &crate::ReadTransaction, round: u8) -> Result<(), StorageError> {
    let table = read
        .open_table(REWRITTEN)
        .map_err(|e| e.into_storage_error_or_corrupted(""))?;
    let untouched = read
        .open_table(UNTOUCHED)
        .map_err(|e| e.into_storage_error_or_corrupted(""))?;
    for key in 0..KEYS {
        assert_eq!(
            table.get(key)?.unwrap().value(),
            value(round).as_slice(),
            "key {key} must keep committed round {round}"
        );
        assert_eq!(
            untouched.get(key)?.unwrap().value(),
            value(UNTOUCHED_VALUE).as_slice(),
            "untouched key {key}"
        );
    }
    let mut seen = 0;
    for entry in table.range(0..KEYS)? {
        let (_, found) = entry?;
        assert_eq!(found.value(), value(round).as_slice(), "range row");
        seen += 1;
    }
    assert_eq!(seen, KEYS);
    Ok(())
}

/// A failed commit that set only the commit latch keeps committed reads serving.
fn assert_reads_serve(db: &Database, context: &str) {
    let read = db.begin_read().unwrap();
    committed_reads(&read, LAST_COMMITTED).unwrap_or_else(|e| panic!("{context}: {e:?}"));
}

/// A genuine write or sync fault keeps its existing sticky refusal for reads.
/// Refusal is allowed here. Wrong bytes never are.
fn assert_reads_exact_or_refused(db: &Database, context: &str) {
    let Ok(read) = db.begin_read() else {
        return;
    };
    match committed_reads(&read, LAST_COMMITTED) {
        Ok(()) | Err(StorageError::PreviousIo) => {}
        Err(error) => panic!("{context}: {error:?}"),
    }
}

/// The handle's repair path mutates, so it refuses here. Repair is on reopen.
fn assert_integrity_refused(db: &mut Database, gate: &Gate, context: &str) {
    let before = Op::ALL.map(|op| gate.counted(op));
    gate.count();
    let refused = db.check_integrity();
    assert!(
        matches!(
            refused,
            Err(crate::DatabaseError::Storage(StorageError::PreviousIo))
        ),
        "{context}: check_integrity must refuse on this handle, got {refused:?}"
    );
    assert_eq!(
        gate.counted(Op::Write),
        before[1],
        "{context}: a refused check_integrity issued a write"
    );
    assert_eq!(
        gate.counted(Op::Sync),
        before[2],
        "{context}: a refused check_integrity issued a sync"
    );
}

fn sweep(route: Route, op: Op) {
    let label = format!("{route:?} {op:?}");
    sweep_positions(control_positions(route, op), &label, |position| {
        let context = format!("{label} position {position}");
        let temp = crate::create_tempfile();
        let (mut db, gate) = seeded(temp.path());
        let tx = rewrite(&db, FAILED_COMMIT);
        let result = commit_through(tx, route, || gate.arm(op, position));
        if !gate.fired() {
            assert!(
                result.is_ok(),
                "{context}: failed with no fault: {result:?}"
            );
            return false;
        }
        assert!(
            matches!(result, Err(CommitError::Storage(StorageError::Io(_)))),
            "{context}: the original error must return, got {result:?}"
        );
        assert_refused(&db, &context);
        assert_integrity_refused(&mut db, &gate, &context);
        if op == Op::Read {
            assert_reads_serve(&db, &context);
        } else {
            assert_reads_exact_or_refused(&db, &context);
        }
        drop(db);
        let allowed: &[u8] = if op == Op::Read {
            &[LAST_COMMITTED]
        } else {
            &[LAST_COMMITTED, FAILED_COMMIT]
        };
        assert_recovered(temp.path(), allowed, &context);
        true
    });
}

/// A snapshot older than the last commit is held across the failure and opened
/// before it, so its table and held root are read after the failure.
#[test]
fn an_older_held_snapshot_stays_exact_after_a_commit_that_failed_on_a_read() {
    for route in Route::ALL {
        let positions = {
            let temp = crate::create_tempfile();
            let (db, gate) = seeded(temp.path());
            let held = db.begin_read().unwrap();
            let held_table = held.open_table(REWRITTEN).unwrap();
            rewrite(&db, 3).commit().unwrap();
            let tx = rewrite(&db, FAILED_COMMIT);
            commit_through(tx, route, || gate.count()).unwrap();
            drop(held_table);
            drop(held);
            gate.counted(Op::Read)
        };
        sweep_positions(positions, &format!("{route:?} held"), |position| {
            let context = format!("{route:?} position {position}");
            let temp = crate::create_tempfile();
            let (db, gate) = seeded(temp.path());
            let held = db.begin_read().unwrap();
            let held_table = held.open_table(REWRITTEN).unwrap();
            rewrite(&db, 3).commit().unwrap();
            let tx = rewrite(&db, FAILED_COMMIT);
            let result = commit_through(tx, route, || gate.arm(Op::Read, position));
            if !gate.fired() {
                assert!(result.is_ok(), "{context}: {result:?}");
                return false;
            }
            assert!(result.is_err(), "{context}: the fault was swallowed");
            assert_refused(&db, &context);
            for key in 0..KEYS {
                assert_eq!(
                    held_table.get(key).unwrap().unwrap().value(),
                    value(LAST_COMMITTED).as_slice(),
                    "{context}: held key {key}"
                );
            }
            committed_reads(&held, LAST_COMMITTED).unwrap();
            let current = db.begin_read().unwrap();
            committed_reads(&current, 3).unwrap();
            drop(current);
            drop(held_table);
            drop(held);
            drop(db);
            assert_recovered(temp.path(), &[3], &context);
            true
        });
    }
}

/// A memory refusal past the boundary is contained here. Refusing it there at
/// all is the separate late-admission BUG. This row does not bless it.
#[test]
fn a_memory_refusal_inside_a_commit_is_contained_with_its_error_unchanged() {
    let run = |route: Route, refuse_at: Option<u64>, path: &Path| {
        let (db, _gate) = seeded(path);
        let tx = rewrite(&db, FAILED_COMMIT);
        let armed = Arc::new(AtomicBool::new(false));
        let seen = Arc::new(AtomicU64::new(0));
        let admit = {
            let armed = Arc::clone(&armed);
            let seen = Arc::clone(&seen);
            Arc::new(move |_: usize| -> Result<(), StorageError> {
                if !armed.load(Ordering::SeqCst) {
                    return Ok(());
                }
                let at = seen.fetch_add(1, Ordering::SeqCst);
                if Some(at) == refuse_at {
                    return Err(StorageError::Native(
                        crate::NativeColdReadError::AdmissionRefused,
                    ));
                }
                Ok(())
            })
        };
        let result = crate::read_admission::with_read_memory_admission_storage(admit, || {
            commit_through(tx, route, || armed.store(true, Ordering::SeqCst))
        });
        (db, result, seen.load(Ordering::SeqCst))
    };
    let refused = |refuse_at: u64, seen: u64| seen > refuse_at;
    for route in Route::ALL {
        let temp = crate::create_tempfile();
        let (_, control, positions) = run(route, None, temp.path());
        control.unwrap();
        sweep_positions(positions, &format!("{route:?} memory"), |position| {
            let context = format!("{route:?} position {position}");
            let temp = crate::create_tempfile();
            let (db, result, seen) = run(route, Some(position), temp.path());
            if !refused(position, seen) {
                assert!(result.is_ok(), "{context}: {result:?}");
                return false;
            }
            assert!(
                matches!(
                    result,
                    Err(CommitError::Storage(StorageError::Native(
                        crate::NativeColdReadError::AdmissionRefused
                    )))
                ),
                "{context}: {result:?}"
            );
            assert_refused(&db, &context);
            assert_reads_serve(&db, &context);
            drop(db);
            assert_recovered(temp.path(), &[LAST_COMMITTED], &context);
            true
        });
    }
}

#[test]
fn every_read_position_of_every_commit_route_refuses_the_next_writer() {
    for route in Route::ALL {
        sweep(route, Op::Read);
    }
}

#[test]
fn every_write_and_sync_position_of_every_commit_route_refuses_the_next_writer() {
    for route in Route::ALL {
        sweep(route, Op::Write);
        sweep(route, Op::Sync);
    }
}

/// The second writer has passed the first check and is observed waiting before
/// the first commit fails, so only a check after acquisition can refuse it.
#[cfg(feature = "test-seams")]
#[test]
fn a_writer_already_queued_when_the_commit_fails_is_refused() {
    let temp = crate::create_tempfile();
    let (db, gate) = seeded(temp.path());
    let db = Arc::new(db);
    let tx = rewrite(&db, FAILED_COMMIT);
    let (send_watch, take_watch) = std::sync::mpsc::channel();
    let queued = std::thread::spawn({
        let db = Arc::clone(&db);
        move || {
            let thread = std::thread::current().id();
            send_watch
                .send(crate::commit_work::watch_writer_queue_for_test(thread))
                .unwrap();
            db.begin_write().map(|_| ()).err()
        }
    });
    let watch = take_watch.recv().unwrap();
    assert!(watch.wait_untimed_for_test());
    let result = commit_through(tx, Route::Ordinary, || gate.arm(Op::Read, 0));
    assert!(result.is_err());
    let refused = queued.join().unwrap();
    assert!(
        matches!(
            refused,
            Some(TransactionError::Storage(StorageError::PreviousIo))
        ),
        "the queued writer must be refused, got {refused:?}"
    );
    drop(Arc::into_inner(db).unwrap());
    assert_recovered(temp.path(), &[LAST_COMMITTED], "queued writer");
}

#[test]
fn a_read_fault_outside_a_commit_stays_retryable() {
    let temp = crate::create_tempfile();
    let (db, gate) = seeded(temp.path());
    {
        let read = db.begin_read().unwrap();
        let table = read.open_table(REWRITTEN).unwrap();
        gate.arm(Op::Read, 0);
        assert!(table.get(0).is_err());
        assert!(gate.fired());
        assert_eq!(
            table.get(0).unwrap().unwrap().value(),
            value(LAST_COMMITTED).as_slice()
        );
    }
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(REWRITTEN).unwrap();
        gate.arm(Op::Read, 0);
        assert!(table.insert(0, value(50).as_slice()).is_err());
    }
    tx.abort().unwrap();
    rewrite(&db, FAILED_COMMIT).commit().unwrap();
    drop(db);
    assert_recovered(temp.path(), &[FAILED_COMMIT], "cold read retry");
}

#[test]
fn a_read_only_handoff_after_a_failed_commit_is_refused_and_keeps_recovery() {
    let temp = crate::create_tempfile();
    let (mut db, gate) = seeded(temp.path());
    let tx = rewrite(&db, FAILED_COMMIT);
    assert!(commit_through(tx, Route::Ordinary, || gate.arm(Op::Read, 0)).is_err());
    assert!(db.prepare_read_only_handoff().is_err());
    drop(db);
    assert!(recovery_flag_set(temp.path()));
    assert_recovered(temp.path(), &[LAST_COMMITTED], "read-only handoff");
}

const CHILD_ROW: &str = "commit_failure_rows::child_process_row";

const NOT_REACHED: i32 = 42;

#[derive(Debug, PartialEq, Eq)]
enum Child {
    Finished,
    NotReached,
    Aborted,
}

fn run_child(path: &Path, envs: &[(&str, String)]) -> Child {
    let mut child = Command::new(std::env::current_exe().unwrap());
    child
        .args(["--exact", CHILD_ROW, "--test-threads", "1"])
        .env("REDB_COMMIT_FAILURE_CHILD", path);
    for (name, value) in envs {
        child.env(name, value);
    }
    let spawned = {
        let _turn = SPAWN_OR_REOPEN.lock().unwrap_or_else(|e| e.into_inner());
        child
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    };
    let output = spawned.wait_with_output().unwrap();
    if output.status.success() {
        return Child::Finished;
    }
    if output.status.code() == Some(NOT_REACHED) {
        return Child::NotReached;
    }
    // No exit code: a signal ended it. Rust's abort on a second panic is SIGABRT.
    if output.status.code().is_none() {
        return Child::Aborted;
    }
    panic!(
        "child {envs:?} ended {:?}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}

fn reached_or_exit(fired: bool) {
    if !fired {
        std::process::exit(NOT_REACHED);
    }
}

/// A drop can abort the process, which must report as a failure of this case and
/// not end the whole sweep. So each case runs in its own process.
#[test]
fn child_process_row() {
    let Some(path) = std::env::var_os("REDB_COMMIT_FAILURE_CHILD") else {
        return;
    };
    let path = Path::new(&path);
    let var = |name: &str| std::env::var(name).unwrap();
    match var("CHILD_MODE").as_str() {
        "drop-after-failed-commit" => {
            let (db, gate) = seeded(path);
            let tx = rewrite(&db, FAILED_COMMIT);
            let position = var("CHILD_POSITION").parse().unwrap();
            let route = Route::parse(&var("CHILD_ROUTE"));
            let result = commit_through(tx, route, || gate.arm(Op::Read, position));
            reached_or_exit(gate.fired());
            assert!(result.is_err());
            drop(db);
        }
        "unwind-then-next-writer" => {
            let (db, gate) = seeded(path);
            let tx = rewrite(&db, FAILED_COMMIT);
            let position = var("CHILD_POSITION").parse().unwrap();
            let route = Route::parse(&var("CHILD_ROUTE"));
            let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                commit_through(tx, route, || gate.arm_unwind(position))
            }));
            reached_or_exit(gate.fired());
            assert!(unwound.is_err());
            assert_refused(&db, "after a caught unwind");
            drop(db);
        }
        #[cfg(feature = "test-seams")]
        "unwind-with-queued-writer" => {
            let (db, gate) = seeded(path);
            let db = Arc::new(db);
            let tx = rewrite(&db, FAILED_COMMIT);
            let (send_watch, take_watch) = std::sync::mpsc::channel();
            let queued = std::thread::spawn({
                let db = Arc::clone(&db);
                move || {
                    let thread = std::thread::current().id();
                    send_watch
                        .send(crate::commit_work::watch_writer_queue_for_test(thread))
                        .unwrap();
                    db.begin_write().map(|_| ()).err()
                }
            });
            let watch = take_watch.recv().unwrap();
            assert!(watch.wait_untimed_for_test());
            let position = var("CHILD_POSITION").parse().unwrap();
            let route = Route::parse(&var("CHILD_ROUTE"));
            crate::commit_work::take_writer_release_for_test();
            let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                commit_through(tx, route, || gate.arm_unwind(position))
            }));
            let latched_at_release = crate::commit_work::take_writer_release_for_test();
            if !gate.fired() {
                // The commit finished, and the queued writer may go on normally.
                drop(queued.join().unwrap());
                std::process::exit(NOT_REACHED);
            }
            assert!(unwound.is_err());
            // The race to the slot can hide a late latch; this cannot.
            assert_eq!(
                latched_at_release,
                Some(true),
                "the unwinding transaction gave up the writer slot before the latch was set"
            );
            let refused = queued.join().unwrap();
            assert!(
                matches!(
                    refused,
                    Some(TransactionError::Storage(StorageError::PreviousIo))
                ),
                "the queued writer saw an unset latch: {refused:?}"
            );
            drop(Arc::into_inner(db).unwrap());
        }
        "count-shutdown" => {
            let (db, gate) = seeded(path);
            gate.count();
            drop(db);
            let counts = Op::ALL.map(|op| gate.counted(op).to_string()).join(" ");
            std::fs::write(var("CHILD_COUNTS"), counts).unwrap();
        }
        "fault-in-shutdown" => {
            let (db, gate) = seeded(path);
            gate.arm(
                Op::parse(&var("CHILD_OP")),
                var("CHILD_POSITION").parse().unwrap(),
            );
            drop(db);
            reached_or_exit(gate.fired());
        }
        other => panic!("unknown child mode {other}"),
    }
}

#[test]
fn dropping_the_handle_right_after_a_failed_commit_starts_no_housekeeping_commit() {
    for route in Route::ALL {
        let control = control_positions(route, Op::Read);
        sweep_positions(control, route.name(), |position| {
            let context = format!("{route:?} position {position}");
            let temp = crate::create_tempfile();
            let outcome = run_child(
                temp.path(),
                &[
                    ("CHILD_MODE", "drop-after-failed-commit".into()),
                    ("CHILD_ROUTE", route.name().into()),
                    ("CHILD_POSITION", position.to_string()),
                ],
            );
            if outcome == Child::NotReached {
                return false;
            }
            assert_eq!(outcome, Child::Finished, "{context}: the drop aborted");
            assert!(recovery_flag_set(temp.path()), "{context}");
            assert_recovered(temp.path(), &[LAST_COMMITTED], &context);
            true
        });
    }
}

/// The first failure of this handle is inside the commit shutdown itself runs.
#[test]
fn a_fault_first_met_in_shutdown_keeps_committed_data_exact() {
    let temp = crate::create_tempfile();
    let counts_path = temp.path().with_extension("counts");
    let counted = run_child(
        temp.path(),
        &[
            ("CHILD_MODE", "count-shutdown".into()),
            ("CHILD_COUNTS", counts_path.display().to_string()),
        ],
    );
    assert_eq!(counted, Child::Finished);
    let counts = std::fs::read_to_string(&counts_path).unwrap();
    std::fs::remove_file(&counts_path).unwrap();
    let counts: Vec<u64> = counts.split(' ').map(|n| n.parse().unwrap()).collect();
    assert!(counts.iter().sum::<u64>() > 0, "shutdown reached no event");
    for (op, positions) in Op::ALL.into_iter().zip(counts) {
        if positions == 0 {
            continue;
        }
        sweep_positions(positions, op.name(), |position| {
            let context = format!("shutdown {op:?} position {position}");
            let temp = crate::create_tempfile();
            let outcome = run_child(
                temp.path(),
                &[
                    ("CHILD_MODE", "fault-in-shutdown".into()),
                    ("CHILD_OP", op.name().into()),
                    ("CHILD_POSITION", position.to_string()),
                ],
            );
            if outcome == Child::NotReached {
                return false;
            }
            assert_eq!(outcome, Child::Finished, "{context}: shutdown aborted");
            assert_recovered(temp.path(), &[LAST_COMMITTED], &context);
            true
        });
    }
}

/// An unwind through a held page lock can abort the process through a second
/// panic. A process that ended starts no later writer, so an abort is reported
/// here and committed data must still recover. A caught unwind must have run at
/// some position in each route, or this proves nothing about the latch.
fn unwind_sweep(mode: &str) {
    for route in Route::ALL {
        let (mut caught, mut aborted) = (0u64, 0u64);
        let control = control_positions(route, Op::Read);
        sweep_positions(control, route.name(), |position| {
            let context = format!("{mode} {route:?} position {position}");
            let temp = crate::create_tempfile();
            let outcome = run_child(
                temp.path(),
                &[
                    ("CHILD_MODE", mode.into()),
                    ("CHILD_ROUTE", route.name().into()),
                    ("CHILD_POSITION", position.to_string()),
                ],
            );
            match outcome {
                Child::NotReached => return false,
                Child::Finished => caught += 1,
                Child::Aborted => aborted += 1,
            }
            assert_recovered(temp.path(), &[LAST_COMMITTED], &context);
            true
        });
        eprintln!("UNWIND {mode} {route:?} caught={caught} aborted={aborted}");
        assert!(caught > 0, "{mode} {route:?}: no unwind was ever caught");
    }
}

/// Availability of reads after an unwind is not claimed: it can poison locks that
/// read paths unwrap. The writer refusal and the recovery are what is proven.
#[test]
fn a_caught_commit_unwind_refuses_the_next_writer_and_recovers_on_reopen() {
    unwind_sweep("unwind-then-next-writer");
}

/// The latch must be set before the unwinding transaction releases the writer
/// slot, including the prepared commit's moved transaction.
#[cfg(feature = "test-seams")]
#[test]
fn a_writer_queued_behind_a_commit_that_unwinds_sees_the_latch() {
    unwind_sweep("unwind-with-queued-writer");
}
