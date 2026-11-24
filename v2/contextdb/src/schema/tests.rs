//! What `schema` makes of a schema record that disagrees with itself, and of the numbers a
//! vector column's index clauses give: no declaration writes such a record, so these tests write
//! one through the store itself. What a declaration is stored as and printed back as is proved
//! through the public calls in `tests/declarations.rs`.
//!
//! Every table is declared through the public calls, in a store of its own, and then read as
//! `schema` reads it: `Catalog::load` over the newest commit.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use super::{Catalog, Table, render};
use crate::memory::Budget;
use crate::store::{ChangeSet, Store};
use crate::types::{Error, Result};
use crate::{Database, DatabaseOpenOptions};

/// An empty folder for one test's store.
fn folder(test: &str) -> PathBuf {
    let folder =
        std::env::temp_dir().join(format!("contextdb-schema-{test}-{}", std::process::id()));
    if folder.exists() {
        std::fs::remove_dir_all(&folder).expect("clear the folder an earlier run left");
    }
    folder
}

/// Runs statements on the store in a folder, each of which must be accepted, with no
/// maintenance cycle beside them.
fn declare(folder: &Path, statements: &[&str]) {
    let options = crate::DatabaseOpenOptions {
        maintenance: crate::Maintenance::Caller,
        ..Default::default()
    };
    let db = Database::open_with_options(folder, options).expect("open the store");
    for sql in statements {
        if let Err(error) = db.execute(sql, &HashMap::new()) {
            panic!("{sql}\nwas refused: {error:?}");
        }
    }
    db.close().expect("close");
}

/// The tables of the store in a folder, as `schema` reads them from its newest commit.
fn load(folder: &Path) -> Result<Catalog> {
    let budget = Budget::new(None).expect("a budget");
    let store = Store::open_for_writing(folder, &budget, &DatabaseOpenOptions::default())
        .expect("open the store");
    let view = store.view().expect("the newest commit");
    Catalog::load(&view, &budget)
}

/// One table of the store in a folder.
fn table(folder: &Path, name: &str) -> Table {
    let catalog = load(folder).expect("the tables");
    let table = catalog.table(name);
    table.unwrap_or_else(|| panic!("no table {name}")).clone()
}

#[test]
fn every_damaged_schema_record_is_told_as_damage_or_read_as_tables_that_print() {
    // The second table declares rules of many kinds, so that its schema record holds most of
    // the parts a record may have.
    let folder = folder("damaged");
    declare(
        &folder,
        &[
            "CREATE TABLE a_parents (id UUID PRIMARY KEY, context_id UUID NOT NULL, \
             UNIQUE (context_id, id))",
            "CREATE TABLE z_rules (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID NOT NULL, \
             parent_id UUID REFERENCES a_parents(id) ON STATE archived PROPAGATE SET stale \
             MAX DEPTH 2 ABORT ON FAILURE, \
             kind TEXT NOT NULL IMMUTABLE, scope TEXT SCOPE_LABEL_READ ('r', 'w') WRITE ('w'), \
             status TEXT NOT NULL DEFAULT 'open', made_at TXID, doc JSON DEFAULT '{}', \
             confidence REAL, \
             v VECTOR(4) PARTITION_KEY (context_id) MAX_PARTITIONS 8 SEARCH_MODE EXACT, \
             UNIQUE (context_id, kind), \
             FOREIGN KEY (context_id, parent_id) REFERENCES a_parents(context_id, id)) \
             STATE MACHINE (status: open -> [stale, closed], stale -> [closed]) \
             PROPAGATE ON EDGE CITES INCOMING STATE stale SET stale MAX DEPTH 3 \
             PROPAGATE ON STATE closed EXCLUDE VECTOR \
             RETAIN 48 HOURS SYNC SAFE HISTORY ALL SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST \
             MAINTAIN COUNT (context_id)",
            "CREATE INDEX z_by_kind ON z_rules (kind DESC, status)",
            "CREATE INDEX z_by_owner ON z_rules ((doc -> 'owner'))",
        ],
    );
    let budget = Budget::new(None).expect("a budget");
    let store = Store::open_for_writing(&folder, &budget, &DatabaseOpenOptions::default())
        .expect("open the store");
    // The records come in the order of their tables' names.
    let record = {
        let view = store.view().expect("a view");
        let (mut at, mut record) = (0, Vec::new());
        view.each_schema(|bytes| {
            if at == 1 {
                record = bytes.to_vec();
            }
            at += 1;
            Ok(())
        })
        .expect("the records");
        record
    };
    assert!(record.len() > 200, "a record of {} bytes", record.len());
    // What `schema` reads once the store alone has written this record for `z_rules`.
    let load_with = |record: Vec<u8>| {
        let mut changes = ChangeSet::new(store.view().expect("a view"), &budget).expect("begin");
        changes.put_schema("z_rules", record).expect("stage");
        store.commit(changes).expect("commit the record");
        Catalog::load(&store.view().expect("the newest commit"), &budget)
    };
    let byte = |at: usize, to: u8| [&record[..at], &[to], &record[at + 1..]].concat();
    // The one place `from` stands in the record, its last byte made `to`.
    let last = |from: &[u8], to: u8| {
        let found: Vec<_> = record.windows(from.len()).map(|at| at == from).collect();
        let at = found.iter().position(|at| *at).expect("in it");
        assert!(!found[at + 1..].contains(&true), "{from:?} twice");
        byte(at + from.len() - 1, to)
    };
    // Each fault, whether every record it makes is damage, and the records. A record begins
    // with the byte of its version. A byte made 255 is a length past the record's end, a kind
    // byte that names no kind or a column number that names no column; inside a name or a
    // text it may leave a record that still agrees with itself, read as tables that print.
    // ColumnDefault literals never hold decoded JSON/vector values; missing cells may.
    let marker = b"doc\x04\0\0\0\0\x01\0\0\x01\x01\x04";
    let default = record
        .windows(marker.len())
        .position(|bytes| bytes == marker)
        .expect("doc default")
        + marker.len()
        - 1;
    let illegal = [
        [&[7][..], &7_u64.to_le_bytes(), br#"{"x":0}"#].concat(),
        [&[8][..], &4_u64.to_le_bytes(), &1_f32.to_le_bytes()].concat(),
        [
            &[0x88][..],
            &1_u64.to_le_bytes(),
            &1_f32.to_le_bytes(),
            &[1],
        ]
        .concat(),
    ];
    let forbidden = illegal
        .iter()
        .map(|value| {
            [
                &record[..default],
                value.as_slice(),
                &record[default + 11..],
            ]
            .concat()
        })
        .collect();
    let rows: [(&str, bool, Vec<Vec<u8>>); 6] = [
        (
            "with a JSON or vector ColumnDefault literal",
            true,
            forbidden,
        ),
        (
            "cut short",
            true,
            (0..record.len()).map(|n| record[..n].to_vec()).collect(),
        ),
        (
            "of another version",
            true,
            vec![byte(0, record[0] + 1), byte(0, 0xFF)],
        ),
        (
            "with a byte made 255",
            false,
            (0..record.len()).map(|at| byte(at, 0xFF)).collect(),
        ),
        // A name a vector's tally or a change of state would look its column up by, its last
        // letter made x.
        (
            "naming in its PARTITION_KEY or STATE MACHINE a column it lacks",
            true,
            vec![
                last(b"v\x01\0\0\0\x0a\0\0\0context_id", b'x'),
                last(b"\0\x01\x06\0\0\0status", b'x'),
            ],
        ),
        // The text 'open' made a TIMESTAMP of the same eight bytes: no literal a DEFAULT takes.
        (
            "with a DEFAULT of no literal",
            true,
            vec![last(b"status\x02\0\0\0\0\0\0\0\x01\x01\x04", 6)],
        ),
    ];
    for (what, always, records) in rows {
        let mut damaged = 0;
        for (at, changed) in records.into_iter().enumerate() {
            match load_with(changed) {
                Err(Error::StoreCorrupted { path, reason }) => {
                    assert!(
                        path == folder && !reason.is_empty(),
                        "{what} ({at}): {reason}"
                    );
                    damaged += 1;
                }
                Err(other) => panic!("a record {what} ({at}) was read as {other:?}"),
                Ok(catalog) => {
                    assert!(!always, "a record {what} ({at}) was read as tables");
                    for table in catalog.tables() {
                        assert!(
                            render::declaration(table).starts_with("CREATE TABLE "),
                            "{what} ({at})"
                        );
                    }
                }
            }
        }
        // A record holds lengths, kinds and column numbers, so some of the changes are damage.
        assert!(damaged > 0, "no record {what} was damage");
    }
    let view = store.view().unwrap();
    for bytes in [&[1, 255][..], &[0, 1][..]] {
        let mut held = budget
            .reserve(0, "schema damage fixture")
            .expect("a reservation");
        let mut cover = budget.passing("schema damage fixture").expect("a cover");
        let decoded = super::record::decode_event(bytes, &view, &mut held, &mut cover);
        assert!(matches!(decoded, Err(Error::StoreCorrupted { .. })));
    }
    drop(view);
    drop(store);
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

#[test]
fn the_index_clauses_of_a_vector_column_give_the_numbers_in_force() {
    use super::{Compaction, Quantization, VectorColumn};
    use crate::store::VectorId;
    let column = |id, indexing_threshold, compaction, quantization| VectorColumn {
        id: VectorId(id),
        column: format!("v{id}"),
        partition_key: Vec::new(),
        max_partitions: None,
        search_mode: None,
        indexing_threshold,
        probes: None,
        compaction,
        quantization,
    };
    let percent = |change, tombstone| Some(Compaction::Percent { change, tombstone });
    let vectors = [
        column(0, None, None, Quantization::F32),
        column(1, Some(1000), Some(Compaction::None), Quantization::Sq8),
        column(
            2,
            Some(u64::MAX),
            percent(Some(50), None),
            Quantization::F32,
        ),
        column(3, Some(1), percent(None, Some(100)), Quantization::Sq8),
        column(4, Some(50), percent(Some(1), Some(2)), Quantization::F32),
    ];
    // The size from which the column is grouped, the size from which a partition of it is,
    // and the two shares at which a partition is grouped again.
    let in_force: Vec<_> = vectors
        .iter()
        .map(|vector| {
            (
                vector.index_at(),
                vector.grouped_from(),
                vector.compaction(),
            )
        })
        .collect();
    assert_eq!(
        in_force,
        [
            (100_000, 1000, Some((20, 10))),
            (1000, 10, None),
            (u64::MAX, u64::MAX / 100, Some((50, 10))),
            (1, 1, Some((20, 100))),
            (50, 1, Some((1, 2))),
        ]
    );
}

#[test]
fn dropping_a_vector_column_leaves_no_record_of_any_kind_for_it() {
    use crate::store::VectorRecord::*;
    use crate::store::{Key, Read};
    let folder = folder("drop-vector-records");
    declare(
        &folder,
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(4) INDEXING_THRESHOLD 1000)",
            "INSERT INTO t (id, v) VALUES (1, [1.0, 0.0, 0.0, 0.0])",
        ],
    );
    let table_id = table(&folder, "t").id;
    let id = table(&folder, "t").vectors[0].id;
    // One record of every kind the vector part keeps for the column, as sealing and building
    // an index leave them; each is the place the check scans from. The row's insert wrote the
    // partition record as the engine encodes it, which every open reads; the rest are staged.
    let (tuple, row) = (Key::from_bytes(&[]), Key::from_bytes(&[1]));
    let records = [
        Partition(id, &tuple),
        Pending(id, 1, &row),
        Block(id, 1, 0),
        Ivf(id, 2),
        Centroids(id, 2, 0),
        Build(id, 1),
        Assigned(id, 2, 0),
        Binary(id, 1, 0),
        Mean(id, 1),
        Left(id, 1, &row),
    ];
    let budget = Budget::new(None).expect("a budget");
    {
        let store = Store::open_for_writing(&folder, &budget, &DatabaseOpenOptions::default())
            .expect("open the store");
        let mut changes = ChangeSet::new(store.view().expect("a view"), &budget).expect("begin");
        for record in &records[1..] {
            changes.put_vector(*record, vec![0; 8]).expect("stage");
        }
        store.commit(changes).expect("commit the records");
    }
    declare(&folder, &["ALTER TABLE t DROP COLUMN v"]);
    let store = Store::open_for_writing(&folder, &budget, &DatabaseOpenOptions::default())
        .expect("open the store");
    let view = store.view().expect("the newest commit");
    let mut left = Vec::new();
    for start in records {
        let kind = format!("{start:?}");
        let mine = format!("{}({id:?},", &kind[..kind.find('(').expect("a kind")]);
        view.vectors(start, &mut |record, _| {
            let name = format!("{record:?}");
            let ours = name.starts_with(&mine);
            if ours {
                left.push(name);
            }
            Ok(ours)
        })
        .expect("scan");
    }
    assert!(
        left.is_empty(),
        "a dropped column keeps nothing in the vector part, yet these records remain: {left:?}"
    );
    drop((view, store));
    // Removing a declaration retires its number, even after close and reopen.
    declare(&folder, &["DROP TABLE t"]);
    declare(&folder, &["CREATE TABLE next (id INTEGER PRIMARY KEY)"]);
    assert_ne!(
        table(&folder, "next").id,
        table_id,
        "a dropped table's number is never reused"
    );
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

#[test]
fn all_prefixes_keeps_a_list_that_two_indexes_share_once() {
    let folder = folder("prefix-lists");
    declare(
        &folder,
        &[
            "CREATE TABLE r (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER, \
             UNIQUE (a, b, id)) MAINTAIN COUNT ALL PREFIXES",
            "CREATE INDEX r_by_ac ON r (a, c)",
        ],
    );
    assert_eq!(
        table(&folder, "r").rules.counts.len(),
        3,
        "(a) once, (a, b), (a, c)"
    );
}
