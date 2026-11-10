//! Tests of what contextdb changes in this storage library: a file its writer holds opens for
//! reading at the newest commit, also when the reader catches the header in the middle of a
//! commit; a commit somebody still reads keeps its pages; and a page that fails its checksum is
//! refused when it is read.
//!
//! Every row's content is a function of its number, and each test has its own folder under the
//! build's temporary folder, left behind for inspection.
#![cfg(unix)]

use std::fs::{File, OpenOptions};
use std::io;
use std::ops::RangeInclusive;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use redb::{
    Builder, Database, ReadOnlyDatabase, ReadTransaction, ReadableDatabase, ReadableTable,
    StorageBackend, StorageError, TableDefinition,
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
