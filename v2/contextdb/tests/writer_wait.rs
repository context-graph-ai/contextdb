//! A second writer waits once for the store: until the writer that holds it lets go, or until
//! the caller's own deadline or cancel ends the wait. Nothing else ends it, a refused open
//! names what it waited on, and no caller needs to try the open again.

mod support;

use std::ops::RangeInclusive;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use contextdb::{Clock, Database, DatabaseOpenOptions, Error};

use support::Child;

/// The second at which the clocks of this file start.
fn second(n: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_700_000_000 + n)
}

/// Run by the clock while the open waits.
type Then = Box<dyn FnOnce() + Send>;

/// (case, deadline, cancel, run once the store refused the open, the readings of the clock the
/// refusal may come at, cancelled)
type Case = (
    &'static str,
    Option<SystemTime>,
    Option<Arc<AtomicBool>>,
    Then,
    RangeInclusive<u64>,
    bool,
);

/// A clock whose n-th reading is [`second`]`(n)`. A waiting open reads its clock for the
/// deadline after each try that found the store held, so a clock read twice has seen the store
/// refuse the open at least once: at its second reading this one runs `then`, once.
struct Ticking {
    reads: AtomicU64,
    then: Mutex<Option<Then>>,
}

impl Clock for Ticking {
    fn now(&self) -> SystemTime {
        let n = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        if n == 2 {
            let then = lock(&self.then).take();
            if let Some(then) = then {
                then();
            }
        }
        second(n)
    }
}

/// Opens the store with this deadline and cancel on a [`Ticking`] clock. Gives the outcome and
/// how many times the open read the clock.
fn open_waiting(
    folder: &Path,
    deadline: Option<SystemTime>,
    cancel: Option<Arc<AtomicBool>>,
    then: Then,
) -> (contextdb::Result<Database>, u64) {
    let clock = Arc::new(Ticking {
        reads: AtomicU64::new(0),
        then: Mutex::new(Some(then)),
    });
    let mut options = DatabaseOpenOptions::default();
    options.clock = Some(clock.clone());
    options.deadline = deadline;
    options.cancel = cancel;
    let outcome = support::open_with(folder, options);
    (outcome, clock.reads.load(Ordering::SeqCst))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Tells the holder `word` and waits for its `answer`.
fn ask(holder: &Mutex<Child>, word: &str, answer: &str) {
    let mut holder = lock(holder);
    holder.tell(word);
    holder.expect(answer);
}

#[test]
fn a_second_writer_waits_once_until_the_first_closes_or_the_callers_deadline_or_cancel() {
    const TEST: &str =
        "a_second_writer_waits_once_until_the_first_closes_or_the_callers_deadline_or_cancel";
    if let Some(role) = support::role() {
        if role.name == "lock-io" {
            let deadline = (role.number("deadline") != 0).then(|| second(5));
            let (result, _) = open_waiting(&role.folder, deadline, None, Box::new(|| {}));
            assert!(matches!(result, Err(Error::StoreIo { path, .. }) if path == role.folder));
            support::say("refused");
            return;
        }
        if let Some(how) = role.name.strip_prefix("gate-") {
            // The gate held whole as a writer does, or shared as readers do, or left at the deadline.
            let gate = Arc::new(std::fs::File::open(role.folder.join("readers")).unwrap());
            let (held, leaves) = (gate.clone(), how == "leaves");
            match how {
                "cancelled" | "writer" => gate.lock().unwrap(),
                _ => gate.lock_shared().unwrap(),
            }
            let cancel = Some(Arc::new(AtomicBool::new(how == "cancelled")));
            let then: Then = Box::new(move || drop(leaves.then(|| held.unlock())));
            let (answer, _) = open_waiting(&role.folder, Some(second(2)), cancel, then);
            let said = answer.map(support::close);
            let said = said.map_err(|e| format!("{e:?}: {e}"));
            support::say(format!("{said:?}"));
            return;
        }
        return holder(role);
    }
    let folder = support::folder("second-writer");
    // Each refusal is its typed holder and the store's path, and says this sentence of them; no
    // refusal names a process, as nothing the store has proves which one holds its lock.
    let (p, at) = (|| folder.clone(), folder.display());
    let (w, r) = (
        Error::DatabaseLocked { path: p() },
        Error::HeldByReaders { path: p() },
    );
    let c = Error::OpenCancelled { path: p() };
    let writer = format!("{w:?}: a writer holds the store at {at}; open a read session to read");
    let readers = format!("{r:?}: readers hold the store at {at}; retry in a moment");
    let stopped = format!("{c:?}: open cancelled while a writer holds {at}");
    let db = support::open(&folder);
    support::declare(&db);
    support::close(db);
    let holder = Arc::new(Mutex::new(Child::start(TEST, "holder", &folder)));
    lock(&holder).expect("holding");

    let options = DatabaseOpenOptions::default();
    assert!(
        options.deadline.is_none() && options.cancel.is_none(),
        "an open that names no deadline and no cancel waits until the store is free"
    );

    // Only the caller's deadline or cancel ends a wait while the holder keeps the store. The
    // refusal names what was waited on, a deadline's as the store held and a cancel's as
    // cancelled; the holder commits after each, so a refused open took nothing from it.
    let cancel_now = Arc::new(AtomicBool::new(false));
    let set = cancel_now.clone();
    let cases: [Case; 4] = [
        (
            "an open past its deadline is refused at once",
            Some(second(0)),
            None,
            Box::new(|| {}),
            0..=1,
            false,
        ),
        (
            "a deadline that passes while the holder keeps the store",
            Some(second(5)),
            None,
            Box::new(|| {}),
            5..=u64::MAX,
            false,
        ),
        (
            "an open cancelled before it began says cancelled",
            None,
            Some(Arc::new(AtomicBool::new(true))),
            Box::new(|| {}),
            0..=u64::MAX,
            true,
        ),
        (
            "an open cancelled while it waits says cancelled",
            Some(second(1_000_000)),
            Some(cancel_now),
            Box::new(move || set.store(true, Ordering::SeqCst)),
            2..=u64::MAX,
            true,
        ),
    ];
    for (case, deadline, cancel, then, readings, cancelled) in cases {
        let (outcome, reads) = open_waiting(&folder, deadline, cancel, then);
        let Err(refusal) = outcome else {
            panic!("{case}: a second writer was let in beside the first");
        };
        let said = format!("{refusal:?}: {refusal}");
        let wanted = if cancelled { &stopped } else { &writer };
        assert_eq!(&said, wanted, "{case}: a holder, never a process");
        assert!(
            readings.contains(&reads),
            "{case}: the open ended at the clock's reading {reads}, not in {readings:?}: at once \
             is one reading, and a wait ends no earlier than what ends it"
        );
        ask(&holder, "commit", "committed");
    }

    // A writer that waits opens in its one call once the holder lets go. The holder is told to
    // close only from inside the wait, after the store has refused the open once.
    let release = holder.clone();
    let (outcome, _) = open_waiting(
        &folder,
        Some(second(1_000_000)),
        None,
        Box::new(move || ask(&release, "close", "closed")),
    );
    let db = outcome.expect("the waiting writer opens when the first closes");
    support::assert_rows_are(&db, 1..=4);
    support::insert(&db, 5..=5);
    support::close(db);
    lock(&holder).end();
    for (role, wanted) in [
        ("cancelled", Err(&stopped)),
        ("writer", Err(&writer)),
        ("readers", Err(&readers)),
        ("leaves", Ok(())),
    ] {
        let mut child = Child::start(TEST, &format!("gate-{role}"), &folder);
        child.expect(&format!("{wanted:?}"));
        child.end();
    }
    #[cfg(target_os = "linux")]
    for deadline in [0, 1] {
        let role = format!("lock-io deadline={deadline}");
        let mut child = Child::start(TEST, &role, &folder);
        child.expect("refused");
        child.end();
        let trace = std::fs::read_to_string(folder.join(format!("{role}.trace"))).unwrap();
        assert!(
            trace.contains("ENOLCK")
                && trace.contains("INJECTED")
                && trace.contains(if deadline == 0 { "LOCK_EX" } else { "LOCK_NB" })
        );
        support::close(support::open(&folder));
    }
}

#[test]
fn a_reader_opens_at_once_while_a_second_writer_waits_for_the_store() {
    const TEST: &str = "a_reader_opens_at_once_while_a_second_writer_waits_for_the_store";
    if let Some(role) = support::role() {
        return match role.name.as_str() {
            "gate" => {
                let gate = std::fs::File::open(role.folder.join("readers")).unwrap();
                gate.lock_shared().unwrap();
                support::say("held");
                assert_eq!(support::hear().as_deref(), Some("release"));
                gate.unlock().unwrap();
            }
            "waiter" => waiter(role),
            "reader" => reader(role),
            _ => holder(role),
        };
    }
    let folder = support::folder("reader-beside-a-waiting-writer");
    let db = support::open(&folder);
    support::declare(&db);
    support::close(db);
    let mut holder = Child::start(TEST, "holder", &folder);
    holder.expect("holding");
    holder.ask("commit", "committed");

    // A second writer waiting for the store holds no reader back: a reader started in a third
    // process after the wait began opens and reads what the holder committed.
    let mut waiter = Child::start(TEST, "waiter", &folder);
    waiter.expect("waiting");
    let mut reader = Child::start(TEST, "reader", &folder);
    reader.expect("read");
    let mut second_reader = Child::start(TEST, "reader", &folder);
    second_reader.expect("read");
    reader.end();
    second_reader.end();

    holder.ask("close", "closed");
    waiter.expect("opened");
    waiter.end();
    holder.end();
    #[cfg(target_os = "linux")]
    {
        // A gate released before the open is no wait at all.
        let mut early = Child::start(TEST, "gate", &folder);
        early.expect("held");
        early.tell("release");
        early.end();
        let mut quick = Child::start(TEST, "holder", &folder);
        quick.expect("holding");
        quick.ask("close", "closed");
        quick.end();
        let mut gates = [(); 2].map(|()| Child::start(TEST, "gate", &folder));
        gates.iter_mut().for_each(|gate| gate.expect("held"));
        let mut writer = Child::start(TEST, "holder", &folder);
        writer.blocked_on(&folder.join("readers"));
        let mut reader = Child::start(TEST, "reader", &folder);
        reader.blocked_on(&folder.join("writer"));
        let [mut first, mut last] = gates;
        first.tell("release");
        first.end();
        writer.blocked_on(&folder.join("readers"));
        last.tell("release");
        last.end();
        writer.expect("holding");
        reader.expect("read");
        reader.end();
        writer.ask("close", "closed");
        writer.end();
    }
}

/// Holds the store for writing, commits the next row when told to, and closes the store when
/// told to.
fn holder(role: support::Role) {
    let db = support::open(&role.folder);
    support::say("holding");
    let mut next = 1;
    while let Some(word) = support::hear() {
        match word.as_str() {
            "commit" => {
                support::insert(&db, next..=next);
                next += 1;
                support::say("committed");
            }
            "close" => {
                db.close().expect("close");
                support::say("closed");
            }
            other => panic!("the holder was told {other:?}"),
        }
    }
}

/// Opens the store for reading, reads the one row the holder committed, and says so.
fn reader(role: support::Role) {
    let session = support::open_reader(&role.folder, None);
    support::assert_rows_are(&session, 1..=1);
    support::say("read");
    support::hear();
}

/// Opens the store for writing and waits for it, saying so once the store has refused it.
fn waiter(role: support::Role) {
    let say = Box::new(|| support::say("waiting"));
    let (outcome, _) = open_waiting(&role.folder, Some(second(1_000_000)), None, say);
    let db = outcome.expect("the waiting writer opens when the holder closes");
    support::say("opened");
    support::hear();
    support::close(db);
}

/// A copy of the descriptor this process holds a store's `lock` file open with: the same open
/// file, as every child process has it that is started while the store is open, until the
/// child replaces itself with its program. The descriptor is found among those the operating
/// system lists for this process, which is how Linux lists them.
#[cfg(target_os = "linux")]
fn copy_of_the_lock(folder: &std::path::Path) -> std::os::fd::OwnedFd {
    use std::fs;
    use std::os::fd::{BorrowedFd, RawFd};

    let lock = fs::canonicalize(folder.join("lock")).expect("the store's lock file");
    let mut found: Vec<RawFd> = Vec::new();
    for entry in fs::read_dir("/proc/self/fd").expect("the descriptors of this process") {
        let entry = entry.expect("a descriptor of this process");
        // Other tests open and close files meanwhile: a descriptor that has gone is not it.
        if fs::read_link(entry.path()).is_ok_and(|open| open == lock) {
            let number = entry.file_name().to_string_lossy().parse();
            found.push(number.expect("a descriptor is a number"));
        }
    }
    let [descriptor] = found.as_slice() else {
        panic!(
            "{} descriptors of this process are of {lock:?}, not one",
            found.len()
        );
    };
    // SAFETY: the caller has the store open, and the store keeps this descriptor open until
    // it is closed; the copy made here is a descriptor of its own.
    let held = unsafe { BorrowedFd::borrow_raw(*descriptor) };
    held.try_clone_to_owned()
        .expect("copy the descriptor of the lock")
}

/// A program that starts another process from one thread while another thread closes a store
/// and opens it again meets this for an instant: the child, until it has replaced itself with
/// its program, holds a copy of every file its parent had open, the store's lock among them.
/// Here a child is handed that copy to keep, so that the instant lasts.
#[cfg(target_os = "linux")]
#[test]
fn a_store_closed_here_opens_again_while_a_child_process_holds_a_copy_of_its_lock() {
    if support::role().is_some() {
        // The child opens nothing. It has the copy it was started with, and waits.
        support::say("holding");
        support::hear();
        return;
    }
    let folder = support::folder("lock-copied-to-a-child");
    let db = support::open(&folder);
    support::declare(&db);
    support::insert(&db, 1..=3);
    let mut heir = support::Child::start_holding(
        "a_store_closed_here_opens_again_while_a_child_process_holds_a_copy_of_its_lock",
        "heir",
        &folder,
        copy_of_the_lock(&folder),
    );
    heir.expect("holding");

    // Closing lets go of the store, so that the next writer can open it. No handle in this
    // process holds it now, and no other process ever opened it: the next open gets it. Its
    // deadline has already passed, so an open that found the store held is refused, not kept
    // waiting on a child that never lets go.
    support::close(db);
    let mut options = DatabaseOpenOptions::default();
    options.deadline = Some(UNIX_EPOCH);
    match support::open_with(&folder, options) {
        Ok(db) => {
            support::assert_rows_are(&db, 1..=3);
            support::insert(&db, 4..=4);
            support::close(db);
        }
        Err(Error::DatabaseLocked { .. }) => panic!(
            "the store this process closed was refused to it as held by a writer; this process \
             is {}, and the child that has a copy of the lock is {}",
            std::process::id(),
            heir.id()
        ),
        Err(other) => panic!("the store this process closed did not open again: {other:?}"),
    }
    heir.end();
    support::assert_rows_are(&support::open(&folder), 1..=4);
}

#[test]
fn a_second_open_in_the_same_process_past_its_deadline_names_a_writer_and_the_store() {
    let folder = support::folder("locked-same-process");
    let db = support::open(&folder);
    let mut options = DatabaseOpenOptions::default();
    options.deadline = Some(UNIX_EPOCH);
    let Err(refusal) = support::open_with(&folder, options) else {
        panic!("a store held in this process opened again");
    };
    let (path, at) = (folder.clone(), folder.display());
    let w = Error::DatabaseLocked { path };
    let said = format!("{w:?}: a writer holds the store at {at}; open a read session to read");
    assert_eq!(format!("{refusal:?}: {refusal}"), said, "no process");
    support::close(db);
    support::close(support::open(&folder));
}
