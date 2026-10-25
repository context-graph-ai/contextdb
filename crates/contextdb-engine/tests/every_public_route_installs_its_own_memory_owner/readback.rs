//! What a consumer gets back from a checkpoint export and from an
//! authenticated pull, read through ordinary queries: every seeded row and
//! the same declared columns. History occurrences are asserted by the
//! engine's own history suites, not here.

use super::fixture::{Store, memory_store, none};
use super::sync_routes::Pair;
use contextdb_core::Value;
use contextdb_engine::Database;

const READS: &[&str] = &[
    "SELECT id, bucket, body, embedding FROM notes ORDER BY id",
    "SELECT id, bucket, embedding, compressed FROM breadth_rows ORDER BY id",
];

/// Every seeded row and both tables' columns, as a consumer reads them.
fn contents(db: &Database) -> (Vec<Vec<Vec<Value>>>, Vec<serde_json::Value>) {
    let rows = READS
        .iter()
        .map(|sql| {
            db.execute(sql, &none())
                .unwrap_or_else(|error| panic!("`{sql}` must answer: {error:?}"))
                .rows()
                .to_vec()
        })
        .collect();
    let schema = ["notes", "breadth_rows"]
        .iter()
        .map(|table| {
            serde_json::to_value(
                db.table_meta(table)
                    .unwrap_or_else(|| panic!("{table} must be declared"))
                    .columns,
            )
            .expect("a column list serializes")
        })
        .collect();
    (rows, schema)
}

/// Run `body` on a newly spawned thread, which holds no memory owner.
fn on_fresh_thread<T: Send>(body: impl FnOnce() -> T + Send) -> T {
    std::thread::scope(|scope| scope.spawn(body).join().expect("the call returns"))
}

#[test]
fn a_checkpoint_export_reopens_with_every_seeded_row() {
    let file = Store::open();
    let memory = memory_store();
    for (name, source) in [("file-backed", file.db.clone()), ("in-memory", memory)] {
        let expected = contents(&source);
        assert_eq!(expected.0[0].len(), 3, "the fixture seeds three notes");
        assert_eq!(
            expected.0[1].len(),
            4,
            "the fixture seeds four breadth rows"
        );
        let artifact = file.sibling(&format!("{name}-export.db"));
        on_fresh_thread(|| source.export_snapshot(&artifact))
            .unwrap_or_else(|error| panic!("a {name} store exports its checkpoint: {error:?}"));
        let restored = on_fresh_thread(|| Database::open(&artifact))
            .unwrap_or_else(|error| panic!("the {name} export opens: {error:?}"));
        assert_eq!(
            on_fresh_thread(|| contents(&restored)),
            expected,
            "the {name} export reads back every seeded row under the same schema"
        );
        restored.close().expect("close the restored export");
    }
}

#[test]
fn an_authenticated_pull_delivers_every_seeded_row() {
    for (name, pair) in [
        ("file-backed", Pair::serve(false)),
        ("in-memory", Pair::serve_memory()),
    ] {
        let expected = contents(&pair.hub.db);
        on_fresh_thread(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a runtime for the calling thread")
                .block_on(async { pair.client()?.pull_default().await })
        })
        .unwrap_or_else(|error| panic!("a {name} edge pulls from its hub: {error:?}"));
        assert_eq!(
            on_fresh_thread(|| contents(&pair.edge.db)),
            expected,
            "the {name} edge reads every row the hub holds, under the same schema"
        );
    }
}
