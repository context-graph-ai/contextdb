//! Read-only per-table census of a contextdb file: what is actually occupying
//! it, table by table, including the engine's own internal tables.
//!
//! This is the per-table size answer `Database::table_size_estimate` gives,
//! pointed at a real file on disk — the diagnostic that turns "the file grows
//! and we do not know why" into a closed accounting table.
//!
//! It **never opens the file through `Database`**: it goes straight to redb, so
//! nothing is reconciled on an open path, no maintenance loop starts, no
//! watermark moves, and nothing is written. Safe to point at a production file
//! or a preserved corpse. It reports redb's own stored / metadata / fragmented
//! accounting per table, which sums to the file size once free pages are
//! reclaimed.
//!
//! Usage: `cargo run -p contextdb-engine --example redb_table_census -- <path>`

use redb::{Database, ReadableDatabase, ReadableTableMetadata, TableHandle};

fn main() {
    let path = std::env::args().nth(1).expect("usage: <path-to-db-file>");
    let file_bytes = std::fs::metadata(&path).expect("stat db file").len();

    let db = Database::open(&path).expect("open redb file read-only");
    let txn = db.begin_read().expect("begin read txn");

    let mut rows = Vec::new();
    let mut total_stored = 0u64;
    let mut total_metadata = 0u64;
    let mut total_fragmented = 0u64;
    let mut total_entries = 0u64;

    for handle in txn.list_tables().expect("list tables") {
        let name = TableHandle::name(&handle).to_string();
        // Untyped open: every contextdb table is <&str,&[u8]>, <&[u8],&[u8]>
        // or <u64,u64>, so try each shape and report whichever opens.
        let (entries, stored, metadata, fragmented) = probe(&txn, &name);
        total_entries += entries;
        total_stored += stored;
        total_metadata += metadata;
        total_fragmented += fragmented;
        rows.push((name, entries, stored, metadata, fragmented));
    }

    rows.sort_by(|a, b| b.2.cmp(&a.2));

    println!("file_bytes={file_bytes}");
    println!(
        "{:<40} {:>10} {:>14} {:>12} {:>14}",
        "table", "entries", "stored_bytes", "meta_bytes", "fragmented"
    );
    for (name, entries, stored, metadata, fragmented) in &rows {
        println!("{name:<40} {entries:>10} {stored:>14} {metadata:>12} {fragmented:>14}");
    }
    println!(
        "{:<40} {:>10} {:>14} {:>12} {:>14}",
        "TOTAL", total_entries, total_stored, total_metadata, total_fragmented
    );
    let accounted = total_stored + total_metadata + total_fragmented;
    println!(
        "accounted={accounted} file={file_bytes} unaccounted={}",
        file_bytes as i64 - accounted as i64
    );
}

fn probe(txn: &redb::ReadTransaction, name: &str) -> (u64, u64, u64, u64) {
    use redb::TableDefinition;
    macro_rules! try_shape {
        ($k:ty, $v:ty) => {
            if let Ok(t) = txn.open_table(TableDefinition::<$k, $v>::new(name)) {
                let len = t.len().unwrap_or(0);
                let s = t.stats().expect("table stats");
                return (
                    len,
                    s.stored_bytes(),
                    s.metadata_bytes(),
                    s.fragmented_bytes(),
                );
            }
        };
    }
    try_shape!(&str, &[u8]);
    try_shape!(&[u8], &[u8]);
    try_shape!(u64, u64);
    try_shape!(u64, &[u8]);
    try_shape!(&str, u64);
    (0, 0, 0, 0)
}
