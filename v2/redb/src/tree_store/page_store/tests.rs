//! Page-store cases that need the crate's private checksum (rulings T2a).
use super::xxh3_checksum;
use crate::{Builder, Database, Error, ReadableTableMetadata, StorageBackend, TableDefinition};
use std::sync::atomic::Ordering::Relaxed;

#[allow(dead_code)]
#[path = "../../../tests/support/plain_file.rs"]
mod plain_file;
use plain_file::PlainFile;

const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("rows");

fn open(file: PlainFile) -> Database {
    Builder::new()
        .set_incremental_recovery(true)
        .create_with_backend(file)
        .unwrap()
}

/// Commits rows `rows` of `version`, one 512-byte value each.
fn commit(db: &Database, version: u8, rows: std::ops::RangeInclusive<u64>) {
    let write = db.begin_write().unwrap();
    let mut table = write.open_table(ROWS).unwrap();
    rows.for_each(|n| drop(table.insert(n, [version; 512].as_slice()).unwrap()));
    drop(table);
    write.commit().unwrap();
}

/// The file offset of a first-region, order-zero page, from the file's own header.
fn offset(bytes: &[u8], page: u64) -> usize {
    assert_eq!(page >> 20, 0, "a first-region, order-zero proof page");
    let size = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    let header = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    (1 + header + page as usize) * size
}

/// A close whose rebuild finds a chain of commit lists that loops, every file call sound, returns
/// that damage (section 7, C2): reading every page without a failed call is not enough.
#[test]
fn a_close_over_a_looping_chain_of_commit_lists_returns_the_damage() {
    let folder = tempfile::tempdir().unwrap();
    let (path, stopped) = (folder.path().join("data"), folder.path().join("stopped"));
    let db = open(PlainFile::open(&path, Vec::new()));
    commit(&db, 1, 1..=12_288);
    let bytes = std::fs::read(&path).unwrap(); // the large commit's chain of lists
    let slot = 64 + usize::from(bytes[9] & 1) * 128;
    let chain = u64::from_le_bytes(bytes[slot + 96..slot + 104].try_into().unwrap());
    commit(&db, 2, 1..=1);
    let mut bytes = std::fs::read(&path).unwrap(); // the file as a kill leaves it
    drop(db);
    let head = offset(&bytes, chain);
    let second = offset(
        &bytes,
        u64::from_le_bytes(bytes[head..head + 8].try_into().unwrap()),
    );
    let page = &mut bytes[second..second + 4096];
    let end = page.len() - 16;
    assert_eq!(
        xxh3_checksum(&page[..end]).to_le_bytes(),
        page[end..],
        "the chain is kept"
    );
    page[..8].copy_from_slice(&chain.to_le_bytes()); // its second list leads back to the first
    let checksum = xxh3_checksum(&page[..end]).to_le_bytes();
    page[end..].copy_from_slice(&checksum);
    std::fs::write(&stopped, bytes).unwrap();
    let closed = open(PlainFile::open(&stopped, Vec::new())).close();
    assert!(
        matches!(closed, Err(Error::Corrupted(_))),
        "the loop is what close returns: {closed:?}"
    );
}

/// A close refused room saves no map and succeeds, or returns the drive's failure when its flush
/// fails; either way the drive gets its sync and its close, the recovery mark stays and the next
/// open is the bounded one and loses nothing (section 7, C2).
#[test]
fn a_close_refused_for_room_keeps_the_recovery_mark_and_loses_nothing() {
    for fail in [false, true] {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("data");
        let file = PlainFile::open(&path, Vec::new());
        let (synced, closes) = (file.synced.clone(), file.closes.clone());
        let refuse = file.refuse_sync.clone();
        let db = open(file);
        commit(&db, 1, 1..=256);
        // Held across the last commit, it keeps that commit's freed pages from coming back.
        let reading = db.begin_read().unwrap();
        commit(&db, 2, 1..=256);
        db.set_file_size_limit(Some(0)); // the file stays at its length
        while db.get_memory().allocate_helper(4096, false, false).is_ok() {} // free pages used up
        let before = synced.load(Relaxed);
        refuse.store(fail, Relaxed);
        let refused = |e: Error| e.to_string().contains("sync refused");
        let closed = db.close().err().map(refused);
        drop(reading);
        let got = (closed, synced.load(Relaxed) > before, closes.load(Relaxed));
        let said = "room refused: success or the drive's failure; the sync and the close reach it";
        assert_eq!(got, (fail.then_some(true), true, 1), "{said}");
        let mark = std::fs::read(&path).unwrap()[9] & 2; // the god byte, after the magic number
        let db = open(PlainFile::open(&path, Vec::new()));
        let table = db.begin_read().unwrap().open_table(ROWS).unwrap();
        let last = table.get(256).unwrap().unwrap().value() == [2; 512].as_slice();
        let kept = db.recovery_state().unwrap().0.is_some();
        let after = (mark, kept, table.len().unwrap(), last);
        let said = "mark kept, no map saved: the next open is the bounded one and loses no row";
        assert_eq!(after, (2, true, 256, true), "{said}");
    }
}
