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
use crate::Database;
use crate::memory::Budget;
use crate::store::{ChangeSet, Store};
use crate::types::{Error, Result};

/// An empty folder for one test's store.
fn folder(test: &str) -> PathBuf {
    let folder =
        std::env::temp_dir().join(format!("contextdb-schema-{test}-{}", std::process::id()));
    if folder.exists() {
        std::fs::remove_dir_all(&folder).expect("clear the folder an earlier run left");
    }
    folder
}

/// Runs statements on the store in a folder, each of which must be accepted.
fn declare(folder: &Path, statements: &[&str]) {
    let db = Database::open(folder).expect("open the store");
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
    let store = Store::open_for_writing(folder, &budget, None).expect("open the store");
    let view = store.view().expect("the newest commit");
    Catalog::load(&view)
}

/// One table of the store in a folder.
fn table(folder: &Path, name: &str) -> Table {
    let catalog = load(folder).expect("the tables");
    let table = catalog.table(name);
    table.unwrap_or_else(|| panic!("no table {name}")).clone()
}

/// A store with two tables, the second of which declares rules of many kinds, so that its
/// schema record holds most of the parts a record may have. Gives back the store and that
/// table's record, as it is stored.
fn store_with_every_rule(test: &str) -> (Rules, Vec<u8>) {
    let folder = folder(test);
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
    let store = Store::open_for_writing(&folder, &budget, None).expect("open the store");
    let view = store.view().expect("the newest commit");
    // The records come in the order of their tables' names.
    let mut records = view.schema_records().expect("the schema records");
    assert_eq!(records.len(), 2, "two tables have two schema records");
    let record = records.pop().expect("the record of the second table");
    assert!(record.len() > 200, "a record of {} bytes", record.len());
    (
        Rules {
            folder,
            budget,
            store,
        },
        record,
    )
}

/// The store of [`store_with_every_rule`], held open by the store part alone.
struct Rules {
    folder: PathBuf,
    budget: Budget,
    store: Store,
}

impl Rules {
    /// What `schema` reads once the store alone has written `record` as the schema record of
    /// the table `z_rules`.
    fn load_with(&self, record: Vec<u8>) -> Result<Catalog> {
        let view = self.store.view().expect("the newest commit");
        let mut changes = ChangeSet::new(view, &self.budget).expect("a change set");
        changes.put_schema("z_rules", record).expect("stage");
        self.store.commit(changes).expect("commit the record");
        Catalog::load(&self.store.view().expect("the newest commit"))
    }

    /// Lets go of the store and removes its folder.
    fn remove(self) {
        let Rules { folder, store, .. } = self;
        drop(store);
        std::fs::remove_dir_all(&folder).expect("remove the test's folder");
    }
}

/// Asserts that reading the tables told the store's damage, by name and naming the folder.
fn assert_damage(read: Result<Catalog>, folder: &Path, what: &str) {
    match read {
        Err(Error::StoreCorrupted { path, reason }) => {
            assert_eq!(path, folder, "{what}");
            assert!(!reason.is_empty(), "{what}");
        }
        Err(other) => panic!("{what} was read as {other:?}"),
        Ok(_) => panic!("{what} was read as a table"),
    }
}

#[test]
fn a_schema_record_cut_short_anywhere_is_damage() {
    let (rules, record) = store_with_every_rule("record-cut-short");
    // Every length from nothing to one byte short of the whole: each ends inside a number, a
    // text or a list that says it is longer than what is left.
    for length in 0..record.len() {
        let what = format!(
            "a schema record cut to {length} of its {} bytes",
            record.len()
        );
        assert_damage(
            rules.load_with(record[..length].to_vec()),
            &rules.folder,
            &what,
        );
    }
    rules.remove();
}

#[test]
fn a_schema_record_of_an_unknown_version_is_damage() {
    let (rules, record) = store_with_every_rule("record-unknown-version");
    // A record begins with one byte that says which version of the record follows.
    for version in [record[0].wrapping_add(1), 0xFF] {
        let mut changed = record.clone();
        changed[0] = version;
        let what = format!("a schema record that begins with the version byte {version}");
        assert_damage(rules.load_with(changed), &rules.folder, &what);
    }
    rules.remove();
}

#[test]
fn a_schema_record_with_one_byte_changed_is_a_whole_table_or_damage_and_never_a_panic() {
    let (rules, record) = store_with_every_rule("record-byte-changed");
    // Each byte in turn becomes 255: as part of a length it points past the record's end, as
    // the byte that says which kind follows it names no kind, and as the number of a column
    // it names no column of the table. Inside a name or a text it may leave a record that
    // still agrees with itself, and that record is read as tables that can be printed.
    let (mut damaged, mut read) = (0, 0);
    for position in 0..record.len() {
        if record[position] == 0xFF {
            continue;
        }
        let mut changed = record.clone();
        changed[position] = 0xFF;
        match rules.load_with(changed) {
            Err(Error::StoreCorrupted { path, .. }) => {
                assert_eq!(path, rules.folder, "byte {position}");
                damaged += 1;
            }
            Err(other) => panic!("with byte {position} changed the record was read as {other:?}"),
            Ok(catalog) => {
                for table in catalog.tables() {
                    assert!(
                        render(table).starts_with("CREATE TABLE "),
                        "byte {position}"
                    );
                }
                read += 1;
            }
        }
    }
    // A record holds lengths, kinds and column numbers, so some of the changes are damage.
    assert!(
        damaged > 0,
        "of the changed records none was damage and {read} were read as tables"
    );
    rules.remove();
}

#[test]
fn the_index_clauses_of_a_vector_column_give_the_numbers_in_force() {
    use super::{Consolidation, Quantization, VectorColumn};
    use crate::store::VectorId;
    let column = |id, auto_index_at, consolidation, quantization| VectorColumn {
        id: VectorId(id),
        column: format!("v{id}"),
        partition_key: Vec::new(),
        max_partitions: None,
        search_mode: None,
        auto_index_at,
        consolidation,
        quantization,
    };
    let percent = |change, tombstone| Some(Consolidation::Percent { change, tombstone });
    let vectors = [
        column(0, None, None, Quantization::F32),
        column(1, Some(1000), Some(Consolidation::None), Quantization::Sq8),
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
                vector.consolidation(),
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
        &["CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(4) AUTO_INDEX_AT 1000)"],
    );
    let id = table(&folder, "t").vectors[0].id;
    // One record of every kind the vector part keeps for the column, as sealing and building
    // an index leave them; each is both staged and the place the check scans from.
    let (tuple, row) = (Key::from_bytes(&[]), Key::from_bytes(&[1]));
    let records = [
        Partition(id, &tuple),
        Pending(id, 1, &row),
        Block(id, 1, 0),
        Grouping(id, 2),
        Centres(id, 2, 0),
        Build(id, 1),
        Assigned(id, 2, 0),
        Sketch(id, 1, 0),
        Mean(id, 1),
        Left(id, 1, &row),
    ];
    let budget = Budget::new(None).expect("a budget");
    {
        let store = Store::open_for_writing(&folder, &budget, None).expect("open the store");
        let mut changes = ChangeSet::new(store.view().expect("a view"), &budget).expect("begin");
        for record in records {
            changes.put_vector(record, vec![0; 8]).expect("stage");
        }
        store.commit(changes).expect("commit the records");
    }
    declare(&folder, &["ALTER TABLE t DROP COLUMN v"]);
    let store = Store::open_for_writing(&folder, &budget, None).expect("open the store");
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
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}
