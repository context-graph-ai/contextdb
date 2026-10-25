//! Blob-store routes that run against the local store alone.

use super::fixture::{Store, existing_file};
use super::{Row, call};
use contextdb_engine::BlobStore;
use contextdb_engine::work_ledger::MovementPolicy;

fn blobs(store: &Store) -> BlobStore {
    contextdb_engine::work_ledger::install_work_ledger_schema(&store.db)
        .expect("fixture ledger schema");
    BlobStore::new(
        store.db.clone(),
        MovementPolicy::default(),
        store.sibling("blob.fabric-identity.key"),
    )
}

pub fn rows() -> Vec<Row> {
    vec![
        Row {
            route: "contextdb_engine::BlobStore::ingest_bytes",
            drive: || {
                let store = Store::open();
                let blobs = blobs(&store);
                call(|| blobs.ingest_bytes(b"route blob"))
            },
        },
        Row {
            route: "contextdb_engine::BlobStore::ingest_file",
            drive: || {
                let store = Store::open();
                let blobs = blobs(&store);
                let input = existing_file(store.dir.path(), "input.bin");
                call(|| blobs.ingest_file(&input))
            },
        },
        Row {
            route: "contextdb_engine::BlobStore::reclaim_unreferenced",
            drive: || {
                let store = Store::open();
                let blobs = blobs(&store);
                blobs.ingest_bytes(b"unreferenced").expect("fixture blob");
                call(|| blobs.reclaim_unreferenced(i64::MAX / 2, 0))
            },
        },
        Row {
            route: "contextdb_engine::BlobStore::reclaim_unreferenced_with_read_memory",
            drive: || {
                let store = Store::open();
                let blobs = blobs(&store);
                blobs.ingest_bytes(b"unreferenced").expect("fixture blob");
                call(|| {
                    blobs.reclaim_unreferenced_with_read_memory(
                        store.db.declared_reader_memory()?,
                        i64::MAX / 2,
                        0,
                    )
                })
            },
        },
    ]
}
