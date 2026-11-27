//! The storage library's own view of a store's data file, for the tests that prove what is on
//! disk. It is the one test file that names the library.
use std::collections::BTreeMap;
use std::path::Path;

use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

/// The storage library's bounded freed-page workspace, which every write transaction holds.
pub const DRAIN_BYTES: usize = redb::WriteTransaction::DRAIN_BYTES;

/// The documented vector records of a closed store, by key.
pub fn vector_records(folder: &Path) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let db = redb::Builder::new()
        .open_read_only(folder.join("data"))
        .unwrap();
    let read = db.begin_read().unwrap();
    let mut records = BTreeMap::new();
    for name in ["vectors", "vector_blocks"] {
        let table = read
            .open_table(TableDefinition::<&[u8], &[u8]>::new(name))
            .unwrap();
        for entry in table.iter().unwrap() {
            let (key, value) = entry.unwrap();
            records.insert(key.value().to_vec(), value.value().to_vec());
        }
    }
    records
}

/// Whether a closed store keeps no large value apart from its row.
pub fn no_large_values(folder: &Path) -> bool {
    let db = redb::Builder::new()
        .open_read_only(folder.join("data"))
        .unwrap();
    let read = db.begin_read().unwrap();
    let beside = read
        .open_table(TableDefinition::<u64, &[u8]>::new("beside"))
        .unwrap();
    beside.is_empty().unwrap()
}

/// Cuts the last byte off the binary-code block `block` of a closed store's vectors.
pub fn damage_binary_block(folder: &Path, block: u32) {
    let file = redb::Database::open(folder.join("data")).unwrap();
    let write = file.begin_write().unwrap();
    {
        let mut blocks = write
            .open_table(TableDefinition::<&[u8], &[u8]>::new("vector_blocks"))
            .unwrap();
        let (key, mut bytes) = blocks
            .iter()
            .unwrap()
            .map(Result::unwrap)
            .find_map(|(k, v)| {
                let key = k.value();
                (key[0] == 7 && key[9..] == block.to_be_bytes())
                    .then(|| (key.to_vec(), v.value().to_vec()))
            })
            .expect("the target binary block exists");
        bytes.pop();
        blocks.insert(key.as_slice(), bytes.as_slice()).unwrap();
    }
    write.commit().unwrap();
}
