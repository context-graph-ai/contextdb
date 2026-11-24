use super::*;
fn enabled() -> Builder {
    let mut builder = Builder::new();
    builder.set_incremental_recovery(true);
    builder
}
fn writing(path: &Path) -> Database {
    let file = PlainFile::open(path, Vec::new());
    enabled().create_with_backend(file).unwrap()
}
fn reading(path: &Path) -> ReadOnlyDatabase {
    let file = PlainFile::read_only(path);
    enabled().open_read_only_with_backend(file).unwrap()
}

#[test]
fn checkpoints_append_freed_pages_with_a_user_table_open() {
    const N: u64 = 3 * 4096;
    let path = folder("checkpoint-append").join("data");
    let file = PlainFile::open(&path, Vec::new());
    let sent = file.sent.clone();
    let db = enabled()
        .set_cache_size(0)
        .create_with_backend(file)
        .unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(ROWS).unwrap();
        for n in 1..=N {
            table.insert(n, vec![7_u8; 4096].as_slice()).unwrap();
        }
    }
    write.commit().unwrap();
    let held = db.begin_read().unwrap();
    let free = db.begin_write().unwrap().compaction_state().unwrap().0;
    let initial_length = length(&path);
    for abort in [true, false] {
        let write = db.begin_write().unwrap();
        let mut table = write.open_table(ROWS).unwrap();
        let before = sent.load(Relaxed);
        write.flush_freed_pages().unwrap();
        assert_eq!(sent.load(Relaxed), before, "empty checkpoint wrote pages");
        for n in 1..=N {
            table.remove(n).unwrap();
            if n % 4096 == 0 {
                let before = sent.load(Relaxed);
                write.flush_freed_pages().unwrap();
                let written = sent.load(Relaxed) - before;
                assert!(written > 0, "checkpoint wrote no freed pages");
            }
        }
        drop(table);
        if abort {
            write.abort().unwrap();
        } else {
            write.commit().unwrap();
        }
        let read = db.begin_read().unwrap();
        let table = read.open_table(ROWS).unwrap();
        assert_eq!(table.len().unwrap(), if abort { N } else { 0 });
    }
    assert!(db.begin_write().unwrap().compaction_state().unwrap().2 >= N);
    for entry in held.open_table(ROWS).unwrap().iter().unwrap() {
        assert_eq!(entry.unwrap().1.value(), vec![7_u8; 4096]);
    }
    drop(held);
    for version in 0..8 {
        commit_rows(&db, 0..=0, version);
    }
    let after = db.begin_write().unwrap().compaction_state().unwrap().0;
    let trimmed = initial_length.saturating_sub(length(&path)) / 4096;
    assert!(after + trimmed >= free + N, "write-outs lost freed pages");
}

fn proof_page(bytes: &[u8]) -> usize {
    let slot = 64 + usize::from(bytes[9] & 1) * 128;
    let page = u64::from_le_bytes(bytes[slot + 96..slot + 104].try_into().unwrap());
    assert_ne!(page, u64::MAX, "the commit has no retained chain");
    assert_eq!(page >> 20, 0, "first-region order-zero fixture");
    let size = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    let header = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    (1 + header + page as usize) * size
}

// Extends the existing reader/header witness with dirty data and chained proof pages.
pub(super) fn proof_survives_stops() {
    const N: u64 = 12_288;
    let root = folder("chain-stops");
    let mut path = root.join("initial");
    let mut db = writing(&path);
    commit_rows(&db, 1..=N, 1);
    for version in 2..=3 {
        let stopped = root.join(format!("stop-{version}"));
        std::fs::write(&stopped, std::fs::read(&path).unwrap()).unwrap();
        let existing = reading(&stopped);
        assert_eq!(newest(&existing).unwrap().1, rows_at(1..=N, 1));
        commit_rows(&db, 1..=N, version);
        let whole = std::fs::read(&path).unwrap();
        let head = proof_page(&whole);
        assert_ne!(&whole[head..head + 8], &[255; 8], "multi-page proof");
        let body = value(N, version);
        let data = whole.windows(body.len()).position(|v| v == body).unwrap();
        for at in [data, head + 4095] {
            let mut torn = whole.clone();
            torn[at] ^= 1;
            std::fs::write(&stopped, torn).unwrap();
            assert_eq!(newest(&reading(&stopped)).unwrap().1, rows_at(1..=N, 1));
            assert_eq!(newest(&existing).unwrap().1, rows_at(1..=N, 1));
        }
        drop(existing);
        drop(db);
        db = writing(&stopped);
        assert_eq!(rows_of(&db.begin_read().unwrap()), rows_at(1..=N, 1));
        path = stopped;
    }
    let mut write = db.begin_write().unwrap();
    write.set_two_phase_commit(true);
    let mut table = write.open_table(ROWS).unwrap();
    table.insert(N + 1, b"sealed".as_slice()).unwrap();
    drop(table);
    write.commit().unwrap();
    for version in 4..=5 {
        commit_rows(&db, 0..=0, version);
    }
    let stopped = root.join("after-two-phase");
    std::fs::write(&stopped, std::fs::read(&path).unwrap()).unwrap();
    drop(db);
    let mut db = writing(&stopped);
    db.check_integrity().unwrap();
    let mut wanted = rows_at(0..=N, 1);
    wanted[0].1 = value(0, 5);
    wanted.push((N + 1, b"sealed".to_vec()));
    assert_eq!(rows_of(&db.begin_read().unwrap()), wanted);
    drop(db);
    assert!(writing(&stopped).check_integrity().unwrap());
}

#[test]
fn a_whole_writing_open_reads_only_the_last_commit_pages() {
    let mut reads = Vec::new();
    for n in [4096, 40_960] {
        let root = folder(&format!("whole-recovery-{n}"));
        let path = root.join("original");
        let db = writing(&path);
        commit_rows(&db, 1..=n, 1);
        commit_rows(&db, n..=n, 2);
        let stopped = root.join("stopped");
        std::fs::write(&stopped, std::fs::read(&path).unwrap()).unwrap();
        let file = PlainFile::open(&stopped, Vec::new());
        let counted = file.read_pages.clone();
        let recovered = enabled().create_with_backend(file).unwrap();
        reads.push(counted.load(Relaxed));
        let read = recovered.begin_read().unwrap();
        let table = read.open_table(ROWS).unwrap();
        assert_eq!(table.get(n).unwrap().unwrap().value(), value(n, 2));
    }
    let extra = reads[1].saturating_sub(reads[0]);
    assert!(extra <= 8, "whole-open reads follow store size: {reads:?}");
}

/// A reader of `file` with a cache of `bytes` beside its writer, and what the file is watched by.
fn cached(file: PlainFile, bytes: usize) -> (ReadOnlyDatabase, Arc<Watch>, Arc<AtomicU64>) {
    let (watch, pages) = (file.watch.clone(), file.read_pages.clone());
    let reader = enabled()
        .set_cache_size(bytes)
        .open_read_only_with_backend(file);
    (reader.unwrap(), watch, pages)
}
fn keep(list: &Mutex<Option<BTreeSet<u64>>>) {
    *list.lock().unwrap() = Some(BTreeSet::new());
}
fn kept(list: &Mutex<Option<BTreeSet<u64>>>) -> BTreeSet<u64> {
    list.lock().unwrap().take().unwrap()
}

// M8 rule 1: while the writer asks which commits readers hold, no page the two recoverable
// commits reach is reused but by its answer, bookkeeping and proof pages included. A reader
// opened at 3 that marks before (`asked_first` false) or just after the writer asked begins on
// 4, the newest whole commit or the one before a torn newest header, and checks it stopped past
// the header while the writer makes `MORE` commits (asked first, 5 is one): it is not refused,
// and the writer wrote none of the pages it read. With no mark held, freed pages are reused.
#[test]
fn a_commit_a_reader_has_marked_stays_whole_while_the_writer_commits_on() {
    const MORE: u64 = 2;
    let mut taken = rows_at(1..=50, 4);
    taken.extend(rows_at(51..=2000, 3));
    for (asked_first, torn) in [(false, false), (false, true), (true, false), (true, true)] {
        let case = format!("asked first {asked_first}, newest header torn {torn}");
        let path = folder(&format!("beside-{asked_first}-{torn}")).join("data");
        let (held, asked) = (Arc::new(Mutex::new(None)), Arc::<Watch>::default());
        let (answer, question) = (held.clone(), asked.clone());
        let file = PlainFile::open(&path, Vec::new());
        let written = file.watch.clone();
        let db = enabled()
            .set_held_commits_callback(move || {
                let oldest = *answer.lock().unwrap();
                pass(&question.gate);
                oldest
            })
            .create_with_backend(file)
            .unwrap();
        (1..=3).for_each(|version| commit_rows(&db, 1..=2000, version));
        let header = ServedHeader::default();
        let (reader, read, _) = cached(PlainFile::serving(&path, header.clone()), 0);
        // The newest header as a cut in its checksum would leave it, served to the reader.
        let tear = || {
            let mut bytes = std::fs::read(&path).unwrap()[..320].to_vec();
            let slot = 64 + usize::from(bytes[9] & 1) * 128;
            bytes[slot + 120] ^= 1;
            *header.lock().unwrap() = Some(bytes);
        };
        let (stop_read, stop_write) = (Arc::new(Barrier::new(2)), Arc::new(Barrier::new(2)));
        // The reader begins and stops past the header while `first` runs and `versions` commit.
        let beside = |first: &dyn Fn(), versions: RangeInclusive<u64>| {
            *read.gate.lock().unwrap() = Some(stop_read.clone());
            std::thread::scope(|scope| {
                let reading = scope.spawn(|| newest(&reader));
                stop_read.wait();
                first();
                versions.for_each(|v| commit_rows(&db, 1..=50, v));
                stop_read.wait();
                reading.join().unwrap()
            })
        };
        let mark = || {
            *held.lock().unwrap() = Some(0);
            keep(&written.written);
        };
        keep(&read.read);
        commit_rows(&db, 1..=50, 4);
        let four = db.begin_read().unwrap().commit_id();
        let given = if asked_first {
            *asked.gate.lock().unwrap() = Some(stop_write.clone());
            std::thread::scope(|scope| {
                let writing = scope.spawn(|| commit_rows(&db, 1..=50, 5));
                stop_write.wait();
                mark();
                if !torn {
                    return beside(
                        &|| {
                            stop_write.wait();
                        },
                        6..=4 + MORE,
                    );
                }
                stop_write.wait();
                writing.join().unwrap();
                tear();
                beside(&|| {}, 6..=5 + MORE)
            })
        } else {
            mark();
            if torn {
                commit_rows(&db, 1..=50, 5);
                tear();
            }
            let next = 5 + u64::from(torn);
            beside(&|| {}, next..=next + MORE - 1)
        };
        let (id, rows) = given.unwrap_or_else(|error| panic!("{case}: refused: {error}"));
        assert!((id, &rows) == (four, &taken), "{case}: not 4's commit");
        let (read, written) = (kept(&read.read), kept(&written.written));
        let under: Vec<_> = read.intersection(&written).filter(|at| **at > 0).collect();
        assert!(under.is_empty(), "{case}: read {under:?} was rewritten");
        *held.lock().unwrap() = None;
        (10..=13).for_each(|version| commit_rows(&db, 1..=50, version));
        let size = length(&path);
        (14..=33).for_each(|version| commit_rows(&db, 1..=50, version));
        assert!(length(&path) <= size, "with no mark freed pages are reused");
    }
}

// M2: what a reader's check allocates is set by the commit, never by the file: the same small
// commit costs the same at the top of a file of 3,000 pages and of 60,000, and the check of a
// commit of 60,000 pages stays inside the room of one check, 65,536 entries of 8 bytes and a bit.
#[test]
fn a_readers_check_allocates_by_its_commit_never_by_the_file() {
    const FILLER: TableDefinition<u64, &[u8]> = TableDefinition::new("filler");
    let checked = |reader: &ReadOnlyDatabase| {
        let before = DELETE_BYTES.with(|v| v.get().0);
        DELETE_BYTES.with(|v| v.set((before, before)));
        drop(reader.begin_read().unwrap());
        DELETE_BYTES.with(|v| v.get().1 - before)
    };
    let mut small = Vec::new();
    for pages in [3_000, 60_000] {
        let path = folder(&format!("check-room-{pages}")).join("data");
        let db = writing(&path);
        commit_rows(&db, 1..=1, 1);
        let (reader, _, _) = cached(PlainFile::read_only(&path), 0);
        commit(&db, |write| fill(write, FILLER, 0..pages, &[7; 3000]));
        let whole = checked(&reader);
        assert!(
            pages < 60_000 || whole <= 532_480,
            "60,000 pages took {whole} bytes"
        );
        commit_rows(&db, 2..=2, 1);
        small.push(checked(&reader));
    }
    assert_eq!(
        small[0], small[1],
        "the check of one small commit grew with the file"
    );
}

// M2: a commit past the longest list, 65,536 entries, is written with no list and its pages
// synced before the header naming them: a cut at any sync opens on the commit before or on it
// with every row, and a reader at the floor's cache beside it checks it by its roots. A close's
// commit, which writes no list either, cut at its first sync opens on the commit before it.
#[test]
fn a_commit_past_the_longest_list_has_none_and_every_cut_opens_whole() {
    let count = |reader: &ReadOnlyDatabase| -> Result<usize, redb::Error> {
        let read = reader.begin_read()?;
        let table = read.open_table(ROWS)?;
        Ok(table.iter()?.try_fold(0, |n, entry| entry.map(|_| n + 1))?)
    };
    for close in [false, true] {
        let path = folder(&format!("past-the-list-{close}")).join("data");
        let file = PlainFile::open(&path, Vec::new());
        let cuts = file.watch.clone();
        let db = enabled().create_with_backend(file).unwrap();
        commit_rows(&db, 1..=3, 1);
        commit_rows(&db, 1..=3, 2);
        let before = db.begin_read().unwrap().commit_id();
        let (reader, _, pages) = cached(PlainFile::read_only(&path), 8 << 20);
        std::fs::copy(&path, path.with_extension("safe")).unwrap();
        *cuts.cuts.lock().unwrap() = Some((path.clone(), Vec::new()));
        if close {
            drop(db);
        } else {
            commit(&db, |write| fill(write, ROWS, 10..70_010, &[7; 3000]));
        }
        let images = cuts.cuts.lock().unwrap().take().unwrap().1;
        assert!(!images.is_empty(), "a commit and a close sync");
        for (at, image) in images.iter().enumerate() {
            let opened = reading(image);
            let id = opened.begin_read().unwrap().commit_id();
            let opened_on = (id == before, count(&opened).ok());
            if at == 0 {
                assert_eq!(opened_on, (true, Some(3)), "cut before the pages are safe");
            } else if !close && at + 1 == images.len() {
                assert_eq!(
                    opened_on,
                    (false, Some(70_003)),
                    "cut after the header is safe"
                );
            }
            let either = [(true, Some(3)), (false, Some(3)), (false, Some(70_003))];
            assert!(
                either.contains(&opened_on),
                "cut at sync {at}: {opened_on:?}"
            );
            drop(opened);
            std::fs::remove_file(image).unwrap();
        }
        if !close {
            let mut header = [0; 320];
            File::open(&path)
                .unwrap()
                .read_exact_at(&mut header, 0)
                .unwrap();
            let slot = 64 + usize::from(header[9] & 1) * 128;
            assert_eq!(
                header[slot + 96..slot + 104],
                [255; 8],
                "a list past its longest"
            );
            let at = pages.load(Relaxed);
            drop(reader.begin_read().unwrap());
            let checked = pages.load(Relaxed) - at;
            assert!(
                checked <= 3,
                "its check read {checked} pages, past the header and roots"
            );
            assert_eq!(
                count(&reader).unwrap(),
                70_003,
                "the reader reads every row"
            );
        }
    }
}
