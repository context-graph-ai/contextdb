//! A compacted file grows back only as its data needs, at the caller's ratio.
//!
//! Upstream doubles a single-region file on growth, so the first write after
//! a compaction extended a 2.16 GB store to 4 GiB: larger than before it was
//! compacted. A caller-set growth step ratio caps every growth at that share
//! of the file (never below twice the allocation that needs it).

use redb::{Database, TableDefinition};

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("rows");
/// A twentieth of the file.
const RATIO: f64 = 0.05;

fn length(path: &std::path::Path) -> u64 {
    std::fs::metadata(path).unwrap().len()
}

/// Compact a store of `rows` 1 KiB values, then write 64 more rows one
/// commit at a time; returns the compacted and the final file length.
fn compact_then_write(path: &std::path::Path, ratio: f64) -> (u64, u64) {
    let mut db = Database::builder().set_cache_size(0).create(path).unwrap();
    db.set_growth_step_ratio(ratio);
    let value = vec![7u8; 1024];
    let rows = 8_192u64;
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(TABLE).unwrap();
        for key in 0..rows {
            table.insert(key, value.as_slice()).unwrap();
        }
    }
    write.commit().unwrap();
    while db.compact().unwrap() {}
    let compacted = length(path);
    for key in rows..rows + 64 {
        let write = db.begin_write().unwrap();
        {
            let mut table = write.open_table(TABLE).unwrap();
            table.insert(key, value.as_slice()).unwrap();
        }
        write.commit().unwrap();
    }
    (compacted, length(path))
}

#[test]
fn a_compacted_file_grows_by_the_declared_ratio_not_by_doubling() {
    let directory = tempfile::tempdir().unwrap();
    // Control: upstream's growth doubles the compacted file.
    let (compacted, grown) = compact_then_write(&directory.path().join("upstream.redb"), 0.0);
    assert!(
        grown > compacted + compacted / 2,
        "fixture: without a ratio the first growth doubles the file: compacted {compacted}, \
         grown {grown}"
    );
    let (compacted, grown) = compact_then_write(&directory.path().join("ratio.redb"), RATIO);
    assert!(
        grown > compacted,
        "fixture: the writes outgrew the compacted file"
    );
    assert!(
        grown <= compacted + compacted / 20 + 256 * 1024,
        "growth after compaction stays within the declared twentieth: compacted {compacted}, \
         grown {grown}"
    );
}
