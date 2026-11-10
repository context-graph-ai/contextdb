//! A million rows are loaded and scanned by a writer whose limit is 64 MiB, and the process
//! never has more than that allocated on the engine's account.
//!
//! This test has a file of its own because its program counts every allocation. The count is
//! taken in a child, so that it is the count of one store and nothing else.

mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use contextdb::{DatabaseOpenOptions, params};

const LIMIT: usize = 64 << 20;
const ROWS: i64 = 1_000_000;
const ROWS_IN_A_COMMIT: i64 = 10_000;

/// The system allocator, counting the bytes allocated now and the most there have been.
struct Counting {
    now: AtomicUsize,
    peak: AtomicUsize,
}

// The allocator of a program has to be a static; it is the only one in the tests.
#[global_allocator]
static ALLOCATED: Counting = Counting {
    now: AtomicUsize::new(0),
    peak: AtomicUsize::new(0),
};

impl Counting {
    fn took(&self, bytes: usize) {
        let now = self.now.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.peak.fetch_max(now, Ordering::Relaxed);
    }

    fn gave_back(&self, bytes: usize) {
        self.now.fetch_sub(bytes, Ordering::Relaxed);
    }
}

// SAFETY: every call is handed to the system allocator unchanged; this only counts beside it.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract is passed on as it is.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            self.took(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract is passed on as it is.
        unsafe { System.dealloc(pointer, layout) };
        self.gave_back(layout.size());
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller's contract is passed on as it is.
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            if new_size >= layout.size() {
                self.took(new_size - layout.size());
            } else {
                self.gave_back(layout.size() - new_size);
            }
        }
        moved
    }
}

#[test]
fn a_million_rows_load_and_scan_under_64_mib() {
    if let Some(role) = support::role() {
        return load_and_scan(role);
    }
    let folder = support::folder("million-rows");
    let db = support::open(&folder);
    support::declare(&db);
    support::close(db);

    let lines = support::Child::start(
        "a_million_rows_load_and_scan_under_64_mib",
        "loader",
        &folder,
    )
    .end();
    assert_eq!(
        support::last_number(&lines, "committed"),
        Some(ROWS as u64),
        "the loader did not commit every row"
    );
    let before = support::last_number(&lines, "before").expect("the loader announces it");
    let peak = support::last_number(&lines, "peak").expect("the loader announces it");
    assert!(
        peak < before + LIMIT as u64,
        "with {before} bytes allocated before the store was opened, the process reached {peak}: \
         {} bytes on the store's account under a limit of {LIMIT}",
        peak - before
    );
}

/// Opens the store with the limit, loads the rows, scans them, and announces `before <bytes>`,
/// the bytes this process had allocated when it opened the store, and `peak <bytes>`, the most
/// it had allocated at any time after that, leaving out a buffer of its own that it holds for
/// a moment to show that the store does not count it. On the way it announces `committed <rows>` after
/// every commit and `scanned <what>` after every scan, so that the parent waits for one commit
/// or one scan at a time and never for the whole load.
fn load_and_scan(role: support::Role) {
    let before = ALLOCATED.now.load(Ordering::Relaxed);
    ALLOCATED.peak.store(before, Ordering::Relaxed);
    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(LIMIT);
    let db = support::open_with(&role.folder, options).expect("open the store under the limit");

    for first in (1..=ROWS).step_by(ROWS_IN_A_COMMIT as usize) {
        let tx = db.transaction().expect("begin");
        support::insert(&tx, first..first + ROWS_IN_A_COMMIT);
        tx.commit()
            .expect("commit ten thousand rows under the limit");
        support::say(format!("committed {}", first + ROWS_IN_A_COMMIT - 1));
    }

    // Three scans that visit every row and hold none: the kept count, every name, every tag.
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM t"), ROWS);
    assert_eq!(support::int(&db, "SELECT COUNT(name) FROM t"), ROWS);
    support::say("scanned names");
    assert_eq!(support::int(&db, "SELECT SUM(tag) FROM t"), ROWS / 10 * 45);
    support::say("scanned tags");
    // And one page from the far end of the table, each row whole.
    let last_page = db
        .execute(
            "SELECT id, name, tag FROM t WHERE id > $last ORDER BY id LIMIT 100",
            &params! { "last" => ROWS - 100 },
        )
        .expect("read the last page");
    let wanted: Vec<_> = (ROWS - 99..=ROWS).map(support::row).collect();
    support::assert_same_rows(last_page.rows(), &wanted, "the last hundred rows");

    // Memory the host takes for itself is not the store's: with twice the limit allocated
    // beside it, the store counts what it counted and still answers under its limit.
    let loaded = ALLOCATED.peak.load(Ordering::Relaxed);
    let counted = db.memory_used();
    {
        let beside = vec![1_u8; 2 * LIMIT];
        assert_eq!(
            db.memory_used(),
            counted,
            "memory the host allocated was counted against the store's limit"
        );
        assert_eq!(support::int(&db, "SELECT COUNT(name) FROM t"), ROWS);
        assert_eq!(std::hint::black_box(&beside).len(), 2 * LIMIT);
    }
    // The peak reported below is the store's, so the host's own buffer is taken out of it.
    ALLOCATED.peak.store(loaded, Ordering::Relaxed);
    support::close(db);

    support::say(format!("before {before}"));
    support::say(format!("peak {}", ALLOCATED.peak.load(Ordering::Relaxed)));
}
