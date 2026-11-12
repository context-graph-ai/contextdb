//! What contextdb changes in this storage library: a file its writer holds opens at the newest
//! commit, also when the reader catches the header mid-commit; a commit somebody reads keeps its
//! pages; a page failing its checksum is refused; a multimap gives each value back whole. Every
//! row is a function of its number; each test has its own folder under the build's temporary one.
#![cfg(unix)]

use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io;
use std::ops::RangeInclusive;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

use redb::{
    AccessGuard, Builder, Database, MultimapTableDefinition, ReadOnlyDatabase, ReadTransaction,
    ReadableDatabase, ReadableTable, ReadableTableMetadata, StorageBackend, StorageError,
    TableDefinition,
};

const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("rows");

/// The data file the way contextdb's store hands it over: the plain file. The store keeps its
/// own lock on its folder, so this library's lock on the file is not what holds it for a writer.
///
/// `damaged` lists file offsets whose byte arrives with one bit flipped on every read. While
/// `header` holds bytes, they are what a read of the start of the file is answered with. The
/// file itself is never changed by either.
#[derive(Debug)]
struct PlainFile {
    file: File,
    damaged: Vec<u64>,
    header: ServedHeader,
    sent: Arc<AtomicU64>,
}

/// The start of the file as a reader is to be served it, or `None` for what the file holds.
type ServedHeader = Arc<Mutex<Option<Vec<u8>>>>;

impl PlainFile {
    fn open(path: &Path, damaged: Vec<u64>) -> PlainFile {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .unwrap();
        PlainFile {
            file,
            damaged,
            header: ServedHeader::default(),
            sent: Arc::default(),
        }
    }

    /// The file as a reader opens it: for reading only.
    fn read_only(path: &Path) -> PlainFile {
        PlainFile::serving(path, ServedHeader::default())
    }

    /// The file for reading only, its start answered from `header` whenever that holds bytes.
    fn serving(path: &Path, header: ServedHeader) -> PlainFile {
        PlainFile {
            file: File::open(path).unwrap(),
            damaged: Vec::new(),
            header,
            sent: Arc::default(),
        }
    }
}

impl StorageBackend for PlainFile {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.file.read_exact_at(out, offset)?;
        for at in &self.damaged {
            if (offset..offset + out.len() as u64).contains(at) {
                out[(at - offset) as usize] ^= 0x01;
            }
        }
        if let Some(header) = self.header.lock().unwrap().as_deref() {
            let served = header.iter().enumerate().skip(offset as usize);
            for (at, byte) in served.take(out.len()) {
                out[at - offset as usize] = *byte;
            }
        }
        Ok(())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.sent.fetch_add(data.len() as u64, Relaxed);
        self.file.write_all_at(data, offset)
    }
}

/// An empty folder of this test's own.
fn folder(test: &str) -> PathBuf {
    let folder = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("storage-{test}"));
    if folder.exists() {
        std::fs::remove_dir_all(&folder).unwrap();
    }
    std::fs::create_dir_all(&folder).unwrap();
    folder
}

/// Opens the file as its one writer, without this library's lock on it.
fn writer(path: &Path) -> Database {
    Database::builder()
        .create_with_backend(PlainFile::open(path, Vec::new()))
        .unwrap()
}

/// Opens the file as a reader beside its writer does: through its own handle on the file,
/// opened for reading only, with no lock on it.
fn reader(path: &Path) -> ReadOnlyDatabase {
    Builder::new()
        .open_read_only_with_backend(PlainFile::read_only(path))
        .expect("a file its writer holds must open for reading, with nothing to mend")
}

/// The 200 bytes row `row` holds after it was written for the `version`th time.
fn value(row: u64, version: u64) -> Vec<u8> {
    format!("row {row:06} version {version:06} ")
        .into_bytes()
        .into_iter()
        .cycle()
        .take(200)
        .collect()
}

/// Writes these rows at this version in one commit.
fn commit_rows(db: &Database, rows: RangeInclusive<u64>, version: u64) {
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(ROWS).unwrap();
        for row in rows {
            table.insert(row, value(row, version).as_slice()).unwrap();
        }
    }
    write.commit().unwrap();
}

/// Every row a reader sees, in key order.
fn rows_of(read: &ReadTransaction) -> Vec<(u64, Vec<u8>)> {
    let table = read.open_table(ROWS).unwrap();
    table
        .iter()
        .unwrap()
        .map(|entry| {
            let (key, value) = entry.unwrap();
            (key.value(), value.value().to_vec())
        })
        .collect()
}

/// These rows, all at one version.
fn rows_at(rows: RangeInclusive<u64>, version: u64) -> Vec<(u64, Vec<u8>)> {
    rows.map(|row| (row, value(row, version))).collect()
}

#[test]
fn a_file_a_writer_holds_opens_for_reading_at_the_newest_commit() {
    let path = folder("a-file-a-writer-holds-opens-for-reading").join("data");
    let writer = writer(&path);
    commit_rows(&writer, 1..=3, 1);

    let first = reader(&path);
    let earlier = first.begin_read().unwrap();
    assert_eq!(rows_of(&earlier), rows_at(1..=3, 1));
    let earlier = earlier.commit_id();

    // The writer goes on committing beside the reader, and is never made to wait or to fail.
    commit_rows(&writer, 4..=5, 1);
    commit_rows(&writer, 1..=1, 2);
    let mut newest = rows_at(1..=1, 2);
    newest.extend(rows_at(2..=5, 1));

    // The same reader, asked again, reads the commit that is newest now, and says it is a
    // later one.
    let later = first.begin_read().unwrap();
    assert_eq!(rows_of(&later), newest);
    assert!(later.commit_id() > earlier);

    // So does a reader that opens now.
    let second = reader(&path);
    assert_eq!(rows_of(&second.begin_read().unwrap()), newest);
}

/// The id and the rows of the commit a reader is given now, or why it was given none.
fn newest(reader: &ReadOnlyDatabase) -> Result<(u64, Vec<(u64, Vec<u8>)>), redb::Error> {
    let read = reader.begin_read()?;
    let table = read.open_table(ROWS)?;
    let mut rows = Vec::new();
    for entry in table.iter()? {
        let (key, value) = entry?;
        rows.push((key.value(), value.value().to_vec()));
    }
    Ok((read.commit_id(), rows))
}

#[test]
fn a_reader_that_catches_the_header_inside_a_commit_reads_the_last_whole_commit() {
    let path = folder("a-reader-that-catches-the-header-inside-a-commit").join("data");
    let writer = writer(&path);
    let header = || std::fs::read(&path).unwrap()[..320].to_vec();
    // What a reader was given, in words a failure can show.
    let told = |given: Result<(u64, Vec<(u64, Vec<u8>)>), redb::Error>| match given {
        Err(error) => format!("refused with {error:?}"),
        Ok((id, rows)) => match (1..=3).find(|version| rows == rows_at(1..=3, *version)) {
            Some(version) => format!("commit {id} with the rows of version {version}"),
            None => format!("commit {id} with rows no commit wrote"),
        },
    };

    // Two commits, and the header as it is between commits.
    commit_rows(&writer, 1..=3, 1);
    commit_rows(&writer, 1..=3, 2);
    let second = told(newest(&reader(&path)));
    let before = header();

    // A third commit. It rewrites the header: the byte that says which of the two commit slots
    // is the newer one, and that slot, which until now held the first commit. The second
    // commit's slot is not touched.
    commit_rows(&writer, 1..=3, 3);
    let third = told(newest(&reader(&path)));
    let after = header();
    let changed = |slot: &usize| before[*slot..slot + 128] != after[*slot..slot + 128];
    let slots: Vec<usize> = [64, 192].into_iter().filter(changed).collect();
    assert_eq!(slots.len(), 1, "one commit rewrites one of the two slots");

    // A read of the header that lands inside that rewrite gets its first bytes from after it
    // and the rest from before. Cut where the slot begins, the newer-slot byte already points
    // at a slot that still holds the first commit; cut in the middle of the slot, the slot is
    // half written. Either way the second commit is the newest whole one, and a reader is
    // given it: never a refusal, and never an older commit.
    let mut wrong = Vec::new();
    for cut in [slots[0], slots[0] + 64] {
        let mixed = [&after[..cut], &before[cut..]].concat();
        let mut expect = |reader: &str, expected: &String, given: String| {
            if given != *expected {
                wrong.push(format!(
                    "cut at byte {cut}, {reader}: expected {expected}, {given}"
                ));
            }
        };

        // A reader that opens on the mixed header.
        let served: ServedHeader = Arc::new(Mutex::new(Some(mixed.clone())));
        let opening = Builder::new()
            .open_read_only_with_backend(PlainFile::serving(&path, served))
            .map_err(redb::Error::from);
        let given = opening.and_then(|reader| newest(&reader));
        expect("a reader that opens now", &second, told(given));

        // A reader that opened on a whole header and is served the mixed one when it next
        // asks; once the header is whole again it is given the third commit.
        let served = ServedHeader::default();
        let open_already = Builder::new()
            .open_read_only_with_backend(PlainFile::serving(&path, Arc::clone(&served)))
            .unwrap();
        *served.lock().unwrap() = Some(mixed);
        let given = told(newest(&open_already));
        expect("a reader that was open already", &second, given);
        *served.lock().unwrap() = None;
        let given = told(newest(&open_already));
        expect("that reader once the header is whole", &third, given);
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn pages_a_held_commit_reaches_are_not_reused_until_it_is_released() {
    let path = folder("pages-a-held-commit-reaches-are-not-reused").join("data");
    // The writer cannot see a reader that opened the file for itself. It asks this, before it
    // uses a freed page again, which commit such a reader still holds.
    let held: Arc<Mutex<Option<u64>>> = Arc::new(Mutex::new(None));
    let asked = Arc::clone(&held);
    let mut builder = Database::builder();
    builder.set_held_commits_callback(move || *asked.lock().unwrap());
    let writer = builder
        .create_with_backend(PlainFile::open(&path, Vec::new()))
        .unwrap();
    commit_rows(&writer, 1..=200, 0);

    let mut allocated = Vec::new();
    for cycle in 1..=12 {
        // An outside reader holds the newest commit, and the writer is told which one that is.
        let outside = reader(&path);
        let outside_read = outside.begin_read().unwrap();
        *held.lock().unwrap() = Some(outside_read.commit_id());

        // Replace every row, twice, while that commit is held.
        commit_rows(&writer, 1..=200, 1000 + cycle);
        commit_rows(&writer, 1..=200, cycle);

        // The held commit still reads whole: none of its pages was written over.
        assert_eq!(
            rows_of(&outside_read),
            rows_at(1..=200, cycle - 1),
            "cycle {cycle}"
        );

        // Let go of it, and count the pages the file has in use.
        drop(outside_read);
        drop(outside);
        *held.lock().unwrap() = None;
        let write = writer.begin_write().unwrap();
        allocated.push(write.stats().unwrap().allocated_pages());
        write.abort().unwrap();
    }

    // Once released, the replaced pages are used again: the count stops growing.
    let settled = &allocated[4..];
    assert!(
        settled.iter().all(|pages| *pages == settled[0]),
        "pages in use after each hold, replace and release: {allocated:?}"
    );
}

/// Opens the file with these offsets damaged and reads one row. The damage is in the leaf that
/// holds the row, which nothing reads before the row is asked for.
fn read_row(path: &Path, damaged: Vec<u64>, row: u64) -> Result<Vec<u8>, StorageError> {
    let db = Database::builder()
        .create_with_backend(PlainFile::open(path, damaged))
        .unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    let value = table.get(row)?.expect("the row was committed");
    Ok(value.value().to_vec())
}

#[test]
fn a_page_with_a_wrong_checksum_is_refused_when_read() {
    let path = folder("a-page-with-a-wrong-checksum-is-refused").join("data");
    commit_rows(&writer(&path), 1..=500, 1);

    let stored = value(250, 1);
    assert_eq!(read_row(&path, Vec::new(), 250).unwrap(), stored);

    // Damage one byte in the middle of the row's value, wherever the file holds a copy of it.
    let file = std::fs::read(&path).unwrap();
    let damaged: Vec<u64> = file
        .windows(stored.len())
        .enumerate()
        .filter(|(_, window)| *window == stored.as_slice())
        .map(|(at, _)| (at + stored.len() / 2) as u64)
        .collect();
    assert!(
        !damaged.is_empty(),
        "the row's value is in the file as it was written"
    );

    match read_row(&path, damaged, 250) {
        Err(StorageError::Corrupted(_)) => {}
        Err(other) => {
            panic!("a damaged page must be refused as damage; it was refused as {other:?}")
        }
        Ok(answer) if answer == stored => panic!("the damage did not reach the read"),
        Ok(_) => panic!("a damaged page was answered with a wrong value"),
    }
}

/// Index entries, which compare as their bytes.
const SLICES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("slices");

/// The key of number `n`: 33 bytes that every key of one million numbers begins with.
fn key(n: u64) -> Vec<u8> {
    let (group, place) = (n / 1_000_000, n % 1_000_000);
    format!("g{group:02}/index-entry-with-a-long-text/{place:07}").into_bytes()
}

fn body(key: &[u8], len: usize) -> Vec<u8> {
    key.iter().copied().cycle().take(len).collect()
}

/// Writes `len` bytes under the key of each of these numbers in this order, 1,000 to a commit.
fn commit_slices(db: &Database, order: &[u64], len: usize) {
    for batch in order.chunks(1000) {
        let write = db.begin_write().unwrap();
        let mut table = write.open_table(SLICES).unwrap();
        for &n in batch {
            let key = key(n);
            table.insert(&*key, &*body(&key, len)).unwrap();
        }
        drop(table);
        write.commit().unwrap();
    }
}

type Guard<'a> = AccessGuard<'a, &'static [u8]>;
fn name(entry: redb::Result<(Guard<'_>, Guard<'_>)>) -> Vec<u8> {
    entry.unwrap().0.value().to_vec()
}

/// Asserts no page is over 4,096 bytes and the table holds exactly `want`, `len` bytes each.
fn assert_slices_are(db: &Database, want: &BTreeSet<Vec<u8>>, len: usize) {
    let read = db.begin_read().unwrap();
    let table = read.open_table(SLICES).unwrap();
    let s = table.stats().unwrap();
    let bytes = s.stored_bytes() + s.metadata_bytes() + s.fragmented_bytes();
    let pages = s.leaf_pages() + s.branch_pages();
    assert_eq!(bytes, pages * 4096, "a page over 4,096 bytes");
    assert_eq!(table.len().unwrap(), want.len() as u64, "pairs held");
    let (forward, back) = (table.iter().unwrap(), table.iter().unwrap().rev());
    assert!(forward.map(name).eq(want.iter().cloned()), "order");
    assert!(back.map(name).eq(want.iter().rev().cloned()), "reverse");
    for (got, edge) in [(table.first(), want.first()), (table.last(), want.last())] {
        let got = got.unwrap().map(|(k, _)| k.value().to_vec());
        assert_eq!(got.as_ref(), edge, "first or last");
    }
    let ends = |k: &Vec<u8>| [0, 1, 3, 4, 20].map(|n| k[..n].to_vec());
    for b in want.iter().flat_map(ends).collect::<BTreeSet<_>>() {
        let found = table.get(&*b).unwrap();
        assert!(found.is_none(), "lookup of the bound {b:?}");
        let up = table.range(&*b..).unwrap();
        let down = table.range(..&*b).unwrap().rev();
        let (up_want, down_want) = (want.range(b.clone()..), want.range(..b.clone()).rev());
        assert!(up.map(name).eq(up_want.cloned()), "from {b:?}");
        assert!(down.map(name).eq(down_want.cloned()), "below {b:?}");
    }
    for key in want {
        let found = table.get(&**key).unwrap().expect("a stored key");
        assert!(found.value() == body(key, len), "value of {key:?}");
        for probe in [[&**key, &[0]].concat(), key[..key.len() - 1].to_vec()] {
            let next = table.range(&*probe..).unwrap().next().map(name);
            let expected = want.range(probe.clone()..).next().cloned();
            assert_eq!(next, expected, "from {probe:?}");
        }
    }
}

#[test]
fn a_table_written_in_key_order_has_full_leaves() {
    // 16,000 keys of 40 bytes, 8 bytes each; a leaf has 4 header bytes and 8 a pair: 73 pairs.
    const PAIRS: u64 = 16_000;
    let whole = PAIRS.div_ceil(4092 / (40 + 8 + 8));
    let numbers = |f: fn(u64) -> u64| (0..PAIRS).map(f).collect::<Vec<_>>();
    let (ordered, random) = (numbers(|i| i), numbers(|i| i * 7919 % PAIRS));
    let runs = numbers(|i| i / 50 % 20 * 1_000_000 + i / 1000 * 50 + i % 50);
    let falling = numbers(|i| (19 - i / 800) * 1_000_000 + i % 800);
    // Runs: twenty of 800 keys, 50 a round. Shared bytes kept once take at most 67% of the
    // leaves whole keys would; random arrival (near 69% full under any cut): 145%.
    let names = ["ordered", "runs", "falling", "random"];
    for (name, order) in names.into_iter().zip([ordered, runs, falling, random]) {
        let most = whole * if name == "random" { 145 } else { 67 } / 100;
        let path = folder(&format!("a-table-written-{name}")).join("data");
        let db = writer(&path);
        commit_slices(&db, &order, 8);
        let mut want: BTreeSet<Vec<u8>> = order.iter().map(|&n| key(n)).collect();
        let leaves = db.begin_write().unwrap().stats().unwrap().leaf_pages();
        assert!(leaves <= most, "{name}: {leaves} leaves, at most {most}");
        assert_slices_are(&db, &want, 8);
        // Deletes run last to first; one block is cut to ten keys (`at % 100`), so its leaves
        // fall under a third and join others; new keys come in between.
        let block = want.iter().nth(want.len() / 2).unwrap()[..36].to_vec();
        let write = db.begin_write().unwrap();
        let mut table = write.open_table(SLICES).unwrap();
        for (at, key) in want.clone().iter().enumerate().rev() {
            if at % 7 == 3 {
                let added = [&**key, b"m"].concat();
                table.insert(&*added, &*body(&added, 8)).unwrap();
                want.insert(added);
            }
            if [at % 50 < 20, at % 100 != 0][key.starts_with(&block) as usize] {
                table.remove(&**key).unwrap();
                want.remove(key);
            }
        }
        drop(table);
        write.commit().unwrap();
        assert_slices_are(&db, &want, 8);
        // `extract_from_if` from a 4-byte bound, `retain_in` between 3 and 20 bytes.
        let pick = |n| want.iter().nth(want.len() / n).unwrap();
        let (low, high) = (pick(3)[..4].to_vec(), pick(2)[..20].to_vec());
        let (near, held) = (low[..3].to_vec(), want.clone());
        let gone = |k: &[u8]| k[k.len() - 1] % 3 == 0;
        let seen = |by: &str, k: &[u8]| assert!(held.contains(k), "{by} gave {k:?}, no stored key");
        let write = db.begin_write().unwrap();
        let mut table = write.open_table(SLICES).unwrap();
        let out = table.extract_from_if(&*low.., |k, _| (seen("extract_from_if", k), gone(k)).1);
        out.unwrap().for_each(|entry| drop(entry.unwrap()));
        want.retain(|k| *k < low || !gone(k));
        let kept = table.retain_in(&*near..&*high, |k, _| (seen("retain_in", k), !gone(k)).1);
        kept.unwrap();
        want.retain(|k| !(*k >= near && *k < high && gone(k)));
        drop(table);
        write.commit().unwrap();
        assert_slices_are(&db, &want, 8);
        drop(db);
        assert_slices_are(&writer(&path), &want, 8);
    }
    // 195 removals in key order beside a leaf of another run: no join may send pages again.
    let file = PlainFile::open(&folder("removals-in-key-order").join("data"), Vec::new());
    let sent = file.sent.clone();
    let db = Database::builder().create_with_backend(file).unwrap();
    let key = |c: u8, n: u32| [&[c; 57][..], format!("{n:03}").as_bytes()].concat();
    let write = db.begin_write().unwrap();
    let mut table = write.open_table(SLICES).unwrap();
    for (c, count) in [(b'p', 150), (b'q', 200)] {
        (0..count).for_each(|n| drop(table.insert(&*key(c, n), &[][..])));
    }
    drop(table);
    write.commit().unwrap();
    let leaves = db.begin_write().unwrap().stats().unwrap().leaf_pages();
    assert_eq!(leaves, 2, "one leaf for each run");
    let mut bytes = Vec::new();
    for n in 0..195 {
        let (write, before) = (db.begin_write().unwrap(), sent.load(Relaxed));
        let _ = write.open_table(SLICES).unwrap().remove(&*key(b'q', n));
        write.commit().unwrap();
        bytes.push(sent.load(Relaxed) - before);
    }
    let sum = |r: std::ops::Range<usize>| bytes[r].iter().sum::<u64>();
    let (control, thin, early, last) = (sum(10..50), sum(90..130), sum(10..30), sum(175..195));
    let said = format!("control {control}, thin {thin}, early {early}, last {last} bytes sent");
    assert!(
        thin <= control + 32_768 && last <= early + 32_768,
        "pages sent again: {said}"
    );
    // An update to 2,500 bytes in a leaf of 210-byte keys sharing 200: every pair back whole.
    for at in [0u32, 69, 137] {
        let db = writer(&folder(&format!("an-update-at-{at}")).join("data"));
        let k = |n: u32| [&[b'p'; 200][..], format!("{n:010}").as_bytes()].concat();
        for (n, len) in [(0..138, 10), (at..at + 1, 2500)] {
            let write = db.begin_write().unwrap();
            let mut t = write.open_table(SLICES).unwrap();
            n.for_each(|n| drop(t.insert(&*k(n), &*vec![b'v'; len]).unwrap()));
            drop(t);
            write.commit().unwrap();
        }
        let t = db.begin_read().unwrap().open_table(SLICES).unwrap();
        let s = t.stats().unwrap();
        let pages = s.leaf_pages() + s.branch_pages();
        let bytes = s.stored_bytes() + s.metadata_bytes() + s.fragmented_bytes();
        let wide = if at == 69 { 4096 } else { 0 }; // no one-page cut fits
        assert_eq!(
            bytes,
            pages * 4096 + wide,
            "update at {at}: a page over 4,096 bytes"
        );
        assert_eq!(t.len().unwrap(), 138, "update at {at}: pairs held");
        let got = t
            .iter()
            .unwrap()
            .map(|e| e.unwrap().1.value().len())
            .collect::<Vec<_>>();
        let want = (0..138)
            .map(|n| if n == at { 2500 } else { 10 })
            .collect::<Vec<_>>();
        assert_eq!(got, want, "update at {at}: every pair whole, in key order");
    }
}

#[test]
fn a_file_is_at_most_a_sixteenth_longer_than_its_pages() {
    let path = folder("a-file-is-at-most-a-sixteenth-longer").join("data");
    let db = writer(&path);
    let in_order: Vec<u64> = (0..40_000).collect();
    commit_slices(&db, &in_order, 243);
    let stats = db.begin_write().unwrap().stats().unwrap();
    let pages = stats.allocated_pages() * stats.page_size() as u64;
    assert!(pages > 40_000 * 243, "slices are in {pages} bytes of pages");
    // Seventeen sixteenths of the pages, and 64 KiB.
    let most = pages * 17 / 16 + (64 << 10);
    let check = |when: &str| {
        let length = std::fs::metadata(&path).unwrap().len();
        let said = format!("{when} the file is {length} bytes, its pages {pages}, at most {most}");
        assert!(length <= most, "{said}");
    };
    check("after the last commit");
    drop(db);
    check("after the database was dropped");
    let want = in_order.iter().map(|&n| key(n)).collect();
    assert_slices_are(&writer(&path), &want, 243);
}

#[test]
fn a_multimap_of_byte_string_values_gives_each_back_whole() {
    let long = |n: u64| [&[b'x'; 200][..], format!("{n:010}").as_bytes()].concat();
    let db = writer(&folder("a-multimap-of-byte-string-values").join("data"));
    let set: MultimapTableDefinition<&[u8], &[u8]> = MultimapTableDefinition::new("set");
    for adding in [true, false] {
        let write = db.begin_write().unwrap();
        let mut table = write.open_multimap_table(set).unwrap();
        for n in (0..60).filter(|n| adding || ![7, 31, 58].contains(n)) {
            let (k, v) = (&b"k"[..], long(n));
            let held = if adding {
                table.insert(k, &*v)
            } else {
                table.remove(k, &*v)
            };
            assert_eq!(
                held.unwrap(),
                !adding,
                "value {n}: an inserted value is held"
            );
        }
        drop(table);
        write.commit().unwrap();
    }
    let table = db.begin_read().unwrap().open_multimap_table(set).unwrap();
    let get = || {
        table
            .get(&b"k"[..])
            .unwrap()
            .map(|g| g.unwrap().value().to_vec())
    };
    assert!(get().eq([7, 31, 58].map(long)), "forwards, each whole");
    assert!(
        get().rev().eq([58, 31, 7].map(long)),
        "backwards, each whole"
    );
}
