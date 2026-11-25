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
fn remove(write: &redb::WriteTransaction, rows: impl IntoIterator<Item = u64>) {
    let mut table = write.open_table(ROWS).unwrap();
    for row in rows {
        table.remove(row).unwrap();
    }
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
#[test]
fn a_tidy_up_step_moves_pages_only_down_within_its_allowance() {
    // Design 4, S3: a step moves a page only to the lowest place wholly below it, is charged each
    // page it reads and writes once against one allowance, always moves the first movable page,
    // and a whole walk that moves nothing ends the pass. Each block is the one value of its own
    // table, so the file holds one copy of it until a step moves it.
    const BLOCKS: [TableDefinition<u64, &[u8]>; 6] = [
        TableDefinition::new("b0"),
        TableDefinition::new("b1"),
        TableDefinition::new("b2"),
        TableDefinition::new("b3"),
        TableDefinition::new("b4"),
        TableDefinition::new("b5"),
    ];
    let path = folder("tidy-up-step").join("data");
    let opened = || {
        let backend = PlainFile::open(&path, Vec::new());
        let counters = (backend.read_pages.clone(), backend.sent.clone());
        (
            manual_builder().create_with_backend(backend).unwrap(),
            counters,
        )
    };
    // The first and last pages of the file that end in each block's bytes: a page is copied
    // whole, so its copy ends in them too.
    let places = || {
        let file = std::fs::read(&path).unwrap();
        let tails = file.chunks_exact(4096).map(|page| &page[4032..]);
        let tails: Vec<&[u8]> = tails.collect();
        (0..6u8)
            .map(|b| {
                let at = |w: &&[u8]| w.iter().all(|&x| x == 0xB0 + b);
                (
                    tails.iter().position(at).unwrap(),
                    tails.iter().rposition(at).unwrap(),
                )
            })
            .collect::<Vec<_>>()
    };
    // One step as the engine takes it, from the stored place in its own commit, with the pages
    // the file was asked for during it.
    let step = |db: &Database, read_pages: &AtomicU64| {
        let was = places();
        let mut write = db.begin_write().unwrap();
        let place = write.compaction_position(false).unwrap().0;
        let before = read_pages.load(Relaxed);
        let (step, _) = write.compact_pages(256, place.as_deref()).unwrap();
        let file_reads = read_pages.load(Relaxed) - before;
        write.commit().unwrap();
        // Each moved page is read and written; past the allowance of 256 a step took only its
        // first movable block after discovery, with the block's copy and the pages above it.
        let (now, pages) = (places(), |b: usize| if b == 5 { 128 } else { 64 });
        // A block stays where it was or moves whole into a place wholly below the one it left.
        // The lowest copy is the block itself (a moved block's old pages keep its bytes until
        // they are written again); its value fills 50 pages, or 100 for the last.
        for (b, (now, was)) in now.iter().zip(&was).enumerate() {
            let last = now.0 + if b == 5 { 99 } else { 49 };
            assert!(
                now.0 == was.0 || last < was.0,
                "block {b} moved from {was:?} to pages {}..={last}, not wholly below where it was",
                now.0
            );
        }
        let lower: Vec<usize> = (0..6).filter(|&b| now[b].0 < was[b].0).collect();
        let known: u64 = lower.iter().map(|&b| pages(b)).sum();
        let over = (step.read + step.written).saturating_sub(256);
        assert!(
            step.read >= known
                && step.written >= known
                && (over == 0
                    || lower.len() == 1
                        && step.moved == pages(lower[0])
                        && over <= step.moved + step.written),
            "uncharged, or past 256 for more than its first block: {step:?} {lower:?}"
        );
        (step, file_reads)
    };
    let (db, (read_pages, _)) = opened();
    commit(&db, |write| fill(write, ROWS, 0..800, &[7; 3000]));
    for (b, table) in BLOCKS.iter().enumerate() {
        // Five blocks of 64 pages and one of 128 pages, above 800 one-page leaves.
        let bytes = if b == 5 { 400 << 10 } else { 200 << 10 };
        commit(&db, |write| {
            fill(write, *table, [0], &vec![0xB0 + b as u8; bytes])
        });
    }
    // More one-page leaves take every free page left between the blocks, beside the pages the
    // commits wrote for themselves: a step releases freed pages first (design 4, S3), so once
    // those pages are rewritten a place wholly free would otherwise open below a block.
    let free = db.begin_write().unwrap().compaction_state().unwrap().0;
    let leaves = 800 + free;
    commit(&db, |write| fill(write, ROWS, 800..leaves, &[7; 3000]));
    let start = places();
    // Every other leaf freed: the walk moves small pages down into the holes and a block into a
    // place they leave wholly below it, each step within its allowance, and reaches rest with
    // the file no longer than before it.
    commit(&db, |write| remove(write, (0..800).step_by(2)));
    drain(&db, 2);
    let before = length(&path);
    while !step(&db, &read_pages).0.ended {}
    assert!(
        length(&path) <= before,
        "a walk that moves pages only down grew the file"
    );
    // Every leaf freed: the pass moves all six blocks below where any of them began, across a
    // reopen that it goes on from.
    commit(&db, |write| remove(write, (1..800).step_by(2).chain(800..leaves)));
    drain(&db, 2);
    // Two small commits write the catalog's page again, now low in the file, so the first page
    // past the cut in tree order is a block.
    for row in 0..2 {
        commit(&db, |write| fill(write, TableDefinition::new("after"), [row], &[1]));
    }
    let (first, _) = step(&db, &read_pages);
    assert!(
        first.moved >= 64,
        "the first movable page always moves: {first:?}"
    );
    drop(db);
    let (db, (read_pages, sent)) = opened();
    let place = db
        .begin_write()
        .unwrap()
        .compaction_position(false)
        .unwrap()
        .0;
    assert!(
        place.is_some() || first.ended,
        "the pass's place is stored in the step's commit"
    );
    let (mut moved, mut ends) = (first.moved, u64::from(first.ended));
    while ends == 0 {
        let (taken, _) = step(&db, &read_pages);
        moved += taken.moved;
        ends += u64::from(taken.ended);
        for (b, (now, then)) in places().iter().zip(&start).enumerate() {
            assert!(
                now.1 <= then.1,
                "block {b} rose from {then:?} to {now:?} in {taken:?}"
            );
        }
    }
    let low = start.iter().map(|s| s.0).min().unwrap();
    for (b, now) in places().iter().enumerate() {
        assert!(
            now.0 < low,
            "block {b} of 64 or 128 pages did not move below {low}: {now:?}"
        );
    }
    let written = sent.load(Relaxed) / 4096;
    assert!(
        written <= 3 * moved,
        "the pass wrote {written} pages to move {moved}"
    );
    // A second pass moves nothing.
    loop {
        let (taken, file_reads) = step(&db, &read_pages);
        assert_eq!(taken.moved, 0, "a second pass moved pages: {taken:?}");
        let reads = "a step where nothing can move read";
        assert!(file_reads <= 256, "{reads} {file_reads} pages");
        if taken.ended {
            break;
        }
    }
    // One more commit cuts the file to its pages in use and at most one growth step, which is
    // never less than twice the largest allocation (`page_manager.rs` grow).
    drain(&db, 1);
    let (free, total, _, _) = db.begin_write().unwrap().compaction_state().unwrap();
    let used = (total - free) * 4096;
    let slack = length(&path) - used;
    assert!(
        slack <= (length(&path) / 16).max(2 * (512 << 10)),
        "{slack} bytes past {used} in use"
    );
    // The low mark (S4): the free pages at a commit's start less the most it had taken. A commit
    // that takes 200 pages while 200 that earlier commits freed come back at its end has a low
    // mark at least 200 below its end count; a commit that lengthened the file has none.
    commit(&db, |write| fill(write, ROWS, 0..600, &[7; 3000]));
    commit(&db, |write| remove(write, 200..600));
    drain(&db, 2);
    commit(&db, |write| remove(write, 0..200));
    commit(&db, |write| fill(write, ROWS, 1000..1200, &[7; 3000]));
    let next = db.begin_write().unwrap();
    let (end, _, _, _) = next.compaction_state().unwrap();
    let mark = next.last_commit_low_mark().unwrap();
    assert!(
        mark.is_some_and(|mark| end >= mark + 200),
        "end count {end}, low mark {mark:?}"
    );
    drop(next);
    let grown = length(&path);
    commit(&db, |write| fill(write, BLOCKS[0], [1], &vec![1; 8 << 20]));
    assert!(length(&path) > grown);
    let mark = db.begin_write().unwrap().last_commit_low_mark().unwrap();
    assert_eq!(
        mark, None,
        "a commit that lengthened the file has no low mark"
    );
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
