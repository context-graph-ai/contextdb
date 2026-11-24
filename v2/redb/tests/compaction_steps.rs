fn fill(
    write: &redb::WriteTransaction,
    definition: TableDefinition<u64, &[u8]>,
    rows: impl IntoIterator<Item = u64>,
    body: &[u8],
) {
    let mut table = write.open_table(definition).unwrap();
    for row in rows {
        table.insert(row, body).unwrap();
    }
}
fn commit(db: &Database, change: impl FnOnce(&redb::WriteTransaction)) {
    let write = db.begin_write().unwrap();
    change(&write);
    write.commit().unwrap();
}
fn drain(db: &Database, count: usize) {
    for _ in 0..count {
        commit(db, |_| {});
    }
}
fn length(path: &Path) -> u64 {
    std::fs::metadata(path).unwrap().len()
}
fn manual_builder() -> Builder {
    let mut builder = Builder::new();
    builder.set_manual_compaction(true);
    builder.set_cache_size(64 * 1024);
    builder
}
fn page_store(name: &str) -> (PathBuf, Database, Arc<AtomicU64>, Arc<AtomicU64>) {
    let path = folder(name).join("data");
    let backend = PlainFile::open(&path, Vec::new());
    let counters = (backend.reads.clone(), backend.sent.clone());
    let db = manual_builder().create_with_backend(backend).unwrap();
    (path, db, counters.0, counters.1)
}
fn page_rows(read: &ReadTransaction, count: u64) {
    let table = read.open_table(ROWS).unwrap();
    assert_eq!(table.len().unwrap(), count);
    for row in 0..count {
        assert_eq!(table.get(row).unwrap().unwrap().value(), &[7; 3000][..]);
    }
}
fn fill_pages(write: &redb::WriteTransaction, count: u64) {
    fill(write, ROWS, 0..count, &[7; 3000]);
    let table = write.open_table(ROWS).unwrap();
    assert!(table.stats().unwrap().leaf_pages() >= count);
}
fn peek_and_reserve(
    db: &Database,
    path: &Path,
    sent: &AtomicU64,
    mut write: redb::WriteTransaction,
    count: u64,
) {
    let mut table = write.open_table(ROWS).unwrap();
    assert!(matches!(
        write.peek_table(ROWS),
        Err(redb::TableError::TableAlreadyOpen(..))
    ));
    table.insert(0, &[9][..]).unwrap();
    table.remove(1).unwrap();
    drop(table);
    assert!(
        write
            .peek_table(TableDefinition::<&str, &[u8]>::new("rows"))
            .is_err()
    );
    let table = write.peek_table(ROWS).unwrap();
    assert_eq!(table.get(0).unwrap().unwrap().value(), &[9]);
    assert!(table.get(1).unwrap().is_none());
    assert_eq!(
        table.get(count - 1).unwrap().unwrap().value(),
        &[7; 3000][..]
    );
    assert_eq!(table.len().unwrap(), count - 1);
    drop(table);
    assert!(sent.load(Relaxed) > 2 << 20, "uncommitted pages must spill");
    write.reserve(0).unwrap();
    write.reserve(128).unwrap();
    assert!(write.compaction_state().unwrap().0 >= 128);
    let reserved_length = length(path);
    db.set_file_size_limit(Some(reserved_length));
    write
        .open_table(ROWS)
        .unwrap()
        .insert(count + 1, &[9][..])
        .unwrap();
    write.commit().unwrap();
    assert_eq!(length(path), reserved_length);
    let mut write = db.begin_write().unwrap();
    assert!(matches!(write.reserve(100000), Err(StorageError::Io(ref e))
        if e.get_ref().is_some_and(|e| e.is::<redb::FileSizeLimit>())));
    write.abort().unwrap();
    db.set_file_size_limit(None);
    commit_rows(db, count + 2..=count + 2, 2);
    let table = db.begin_read().unwrap().open_table(ROWS).unwrap();
    assert_eq!(table.len().unwrap(), count + 1);
    commit(db, |write| assert!(write.delete_table(ROWS).unwrap()));
}

struct DeleteCounter;
thread_local! { static DELETE_BYTES: std::cell::Cell<(isize, isize)> = const { std::cell::Cell::new((0, 0)) }; }
fn delete_bytes(delta: isize) {
    let _ = DELETE_BYTES.try_with(|v| {
        let (now, peak) = v.get();
        v.set((now + delta, peak.max(now + delta)));
    });
}
#[global_allocator]
static DELETE_ALLOCATOR: DeleteCounter = DeleteCounter;
// SAFETY: each allocation and layout is forwarded unchanged to the system allocator.
unsafe impl std::alloc::GlobalAlloc for DeleteCounter {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let p = unsafe { std::alloc::System.alloc(layout) };
        if !p.is_null() {
            delete_bytes(layout.size() as isize);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: std::alloc::Layout) {
        unsafe { std::alloc::System.dealloc(p, layout) };
        delete_bytes(-(layout.size() as isize));
    }
    unsafe fn realloc(&self, p: *mut u8, layout: std::alloc::Layout, size: usize) -> *mut u8 {
        let p = unsafe { std::alloc::System.realloc(p, layout, size) };
        if !p.is_null() {
            delete_bytes(size as isize - layout.size() as isize);
        }
        p
    }
}
#[test]
fn native_page_contracts() {
    for committed in [false, true] {
        for count in [512, 5120, 51200] {
            let (path, db, reads, sent) = page_store(&format!("native-pages-{committed}-{count}"));
            let mut write = db.begin_write().unwrap();
            assert!(matches!(
                write.peek_table(ROWS),
                Err(redb::TableError::TableDoesNotExist(_))
            ));
            fill_pages(&write, count);
            if !committed && count == 51200 {
                peek_and_reserve(&db, &path, &sent, write, count);
                write = db.begin_write().unwrap();
                fill_pages(&write, count);
            }
            if committed {
                write.commit().unwrap();
                write = db.begin_write().unwrap();
            }
            let free = write.compaction_state().unwrap().0;
            reads.store(0, Relaxed);
            let before = DELETE_BYTES.with(|v| v.get().0);
            DELETE_BYTES.with(|v| v.set((before, before)));
            assert!(write.delete_table(ROWS).unwrap());
            let peak = DELETE_BYTES.with(|v| v.get().1 - before);
            assert!(
                reads.load(Relaxed) < count / 8,
                "read branches plus one discovery leaf"
            );
            // Committed pages need one pending-free address each; fresh pages need none.
            assert!(peak <= 64 * 1024 + if committed { 16 * count as isize } else { 0 });
            if !committed {
                assert!(write.compaction_state().unwrap().0 >= free + count);
            }
            write.abort().unwrap();
            drop(db);
            let db = manual_builder().open(&path).unwrap();
            assert_eq!(db.begin_read().unwrap().open_table(ROWS).is_ok(), committed);
            if committed {
                page_rows(&db.begin_read().unwrap(), count);
                commit(&db, |write| assert!(write.delete_table(ROWS).unwrap()));
                drop(db);
                let reopened = manual_builder().open(&path).unwrap();
                assert!(reopened.begin_read().unwrap().open_table(ROWS).is_err());
            }
        }
    }
}
#[test]
fn checkpoint_writes_before_commit_and_abort_restores_rows() {
    let (_, db, _, sent) = page_store("checkpoint-abort");
    commit(&db, |write| fill_pages(write, 13500));
    let write = db.begin_write().unwrap();
    assert!(write.delete_table(ROWS).unwrap());
    let before = DELETE_BYTES.with(|v| v.get().0);
    let written = sent.load(Relaxed);
    write.flush_freed_pages().unwrap();
    let released = before - DELETE_BYTES.with(|v| v.get().0);
    let written = sent.load(Relaxed) - written;
    eprintln!("checkpoint before commit: released={released}, written={written}");
    assert!(
        written > 0,
        "checkpoint must write pages before commit, even when memory release is already observable"
    );
    write.abort().unwrap();
    page_rows(&db.begin_read().unwrap(), 13500);
}
#[test]
fn whole_file_compaction_terminates_and_the_next_write_commits() {
    let path = folder("small-whole-compaction").join("data");
    let mut db = Database::create(&path).unwrap();
    commit_rows(&db, 0..=0, 0);
    drain(&db, 2);
    db.set_file_size_limit(Some(0));
    db.compact().expect("no relocation room must stop cleanly");
    db.set_file_size_limit(None);
    commit_rows(&db, 1..=1, 1);
}
#[test]
fn committed_pages_read_and_release_within_their_allowance() {
    // A small write releases bounded debt; large imports release enough to reuse their space.
    // The 1,000,000-page (4 GiB) step runs in Check/release; debug CI proves the 10x scaling step.
    let retired_counts: &[u64] = if cfg!(debug_assertions) {
        &[10_000, 100_000]
    } else {
        &[10_000, 100_000, 1_000_000]
    };
    for &retired in retired_counts {
        let import = (retired / 4).min(3 * 4096);
        let (path, db, reads, sent) = page_store(&format!("pending-release-import-{retired}"));
        let write = db.begin_write().unwrap();
        {
            fill(&write, ROWS, 0..retired, &[7; 3000]);
            let table = write.open_table(ROWS).unwrap();
            let stats = table.stats().unwrap();
            assert!(stats.leaf_pages() >= retired);
            assert_eq!(
                stats.stored_bytes() + stats.metadata_bytes() + stats.fragmented_bytes(),
                (stats.leaf_pages() + stats.branch_pages()) * 4096,
                "the pending addresses each name a single base page"
            );
        }
        write.commit().unwrap();
        commit(&db, |write| assert!(write.delete_table(ROWS).unwrap()));
        let write = db.begin_write().unwrap();
        let (free, total, pending, _, _) = write.compaction_space(&mut (retired as usize)).unwrap();
        assert!(
            pending >= retired,
            "only {pending} one-page addresses are pending"
        );
        drop(write);
        let (before, before_reads) = (sent.load(Relaxed), reads.load(Relaxed));
        commit(&db, |write| fill(write, ROWS, 0..1, &[9; 3000]));
        let (after, same_total, _, _) = db.begin_write().unwrap().compaction_state().unwrap();
        assert_eq!(same_total, total, "the small commit reuses pending space");
        // Full-page writes overestimate new allocations, including header and metadata writes.
        let written = (sent.load(Relaxed) - before).div_ceil(4096);
        let read_pages = (reads.load(Relaxed) - before_reads).div_ceil(4096);
        assert!(read_pages <= 4096 + 400);
        let released_upper = after.saturating_sub(free) + written;
        assert!(after > free, "the small commit must release pending space");
        assert!(
            released_upper <= 4096 + 400,
            "one-row commit released up to {released_upper} pages, including its metadata"
        );
        let before = length(&path);
        for first in (1..=4 * import).step_by(import as usize) {
            commit(&db, |write| {
                fill(write, ROWS, first..first + import, &[7; 3000])
            });
        }
        let growth = length(&path).saturating_sub(before);
        assert!(
            growth < 2 * import * 4096,
            "four imports grew by {growth} bytes: at least two commits of pages"
        );
        let table = db.begin_read().unwrap().open_table(ROWS).unwrap();
        assert_eq!(table.len().unwrap(), 1 + 4 * import);
        assert_eq!(table.get(0).unwrap().unwrap().value(), &[9u8; 3000][..]);
        assert_eq!(
            table.get(4 * import).unwrap().unwrap().value(),
            &[7u8; 3000][..]
        );
        eprintln!(
            "release/import witness: retired={retired}, pending={pending}, released_upper={released_upper}, read_pages={read_pages}, written={written}, four-import growth={growth}"
        );
    }
}
