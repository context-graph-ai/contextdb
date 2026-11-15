//! `Database::explain`: the path a statement would take, said in the documented words, without
//! running it.

mod support;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use contextdb::{ContextId, Database, Uuid, params};

/// A store with the usual table `t` and its index on `tag`, forty rows.
fn store(name: &str) -> (PathBuf, Database) {
    let folder = support::folder(name);
    let db = support::open(&folder);
    support::declare(&db);
    support::insert(&db, 1..=40);
    (folder, db)
}

/// What `explain` says of a statement. It must answer.
fn explained(db: &Database, sql: &str) -> String {
    db.explain(sql)
        .unwrap_or_else(|error| panic!("{sql}\nwas not explained: {error:?}"))
}

/// Fails unless the explanation holds each of these, naming the statement and the whole text.
fn assert_says(db: &Database, sql: &str, words: &[&str]) {
    let text = explained(db, sql);
    for word in words {
        assert!(text.contains(word), "{sql}\ndoes not say `{word}`:\n{text}");
    }
}

/// Every file under a folder with its bytes, in name order.
fn bytes_of(folder: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut files = Vec::new();
    for name in support::listing(folder) {
        let path = folder.join(name);
        if path.is_dir() {
            files.extend(bytes_of(&path));
        } else {
            let bytes = std::fs::read(&path).expect("read a file of the store");
            files.push((path, bytes));
        }
    }
    files
}

#[test]
fn a_search_is_explained_with_its_mode_and_partition_key() {
    let (_, db) = store("explain-search");
    support::declare_docs(&db, 3, "PARTITION_KEY (ctx) SEARCH_MODE EXACT");
    support::insert_docs(&db, 1..=8, 3, |n| format!("c{}", n % 2));
    assert_says(
        &db,
        "SELECT id FROM docs WHERE ctx = 'c1' ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 3",
        &["EXACT", "exact", "ctx"],
    );
    // A filter on a column no index leads with is said to take no path, and the statement that
    // would give it one; once the index is there the search says the path it takes.
    let by_tag = "SELECT id FROM docs WHERE tag = 3 ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 3";
    assert_says(
        &db,
        by_tag,
        &[
            "filter_path: none",
            "recovery: CREATE INDEX docs_tag_idx ON docs (tag)",
        ],
    );
    support::run(&db, "CREATE INDEX docs_tag_idx ON docs (tag)");
    assert_says(
        &db,
        by_tag,
        &[
            "filter_path: IndexScan { index: docs_tag_idx }",
            "small_set: yes",
        ],
    );
    // A vector read from a subquery leaves the filter path the search's own; the inner one is told once.
    let q = "SELECT id FROM docs WHERE tag = 3 ORDER BY v <=> (SELECT v FROM docs WHERE id = 1) LIMIT 3";
    let text = explained(&db, q);
    let path = text.lines().find(|line| line.contains("filter_path:"));
    assert_eq!(
        path.map(str::trim),
        Some("filter_path: IndexScan { index: docs_tag_idx }"),
        "{text}"
    );
    assert_eq!(text.matches("__pk_id").count(), 1, "{text}");
    // A filter that reaches more rows than the statement asks for is not a small set.
    assert_says(
        &db,
        "SELECT id FROM docs WHERE tag >= 0 ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 1",
        &["small_set: no"],
    );
    // The rows asked for are the limit, the offset and a second sort key's lookahead: tags 5 to
    // 8 are four rows, as many as LIMIT 2 OFFSET 2 asks for.
    assert_says(
        &db,
        "SELECT id FROM docs WHERE tag >= 5 ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 2 OFFSET 2",
        &["small_set: yes"],
    );
    // Refused as running is; an unbound $x is a PlanError, open only as a search's vector or key.
    let mut told = Vec::new();
    for (head, kind) in [
        ("SELECT nope FROM docs @ LIMIT 3", "ColumnNotFound"),
        ("SELECT id FROM docs WHERE 1 = 'x' @ LIMIT 3", "PlanError"),
        (
            "SELECT id FROM docs @ USE RANK nothing LIMIT 3",
            "RankPolicy",
        ),
        ("SELECT id FROM docs WHERE tag = $x @ LIMIT 3", "PlanError"),
        ("SELECT id, $x FROM docs @ LIMIT 3", "PlanError"),
        (
            "SELECT id FROM docs WHERE ctx LIKE $x @ LIMIT 3",
            "PlanError",
        ),
        ("SELECT id FROM docs WHERE id = $x", "PlanError"),
        ("DELETE FROM docs WHERE id = $x", "PlanError"),
    ] {
        let sql = head.replace('@', "ORDER BY v <=> $q");
        let error = format!("{:?}", db.explain(&sql).err());
        if !error.contains(kind) {
            told.push(format!("{sql}\ngave {error}"));
        }
    }
    let open = "SELECT id FROM docs WHERE ctx = $c ORDER BY v <=> $q LIMIT 3";
    explained(&db, open);
    // A bound value of the wrong type or width is refused as running refuses it.
    let sql = "SELECT id FROM docs WHERE tag = $x ORDER BY v <=> $q LIMIT 3";
    for bound in [
        params! { "x" => "text", "q" => support::vector(1, 3) },
        params! { "x" => 1, "q" => support::vector(1, 2) },
    ] {
        let ran = format!("{:?}", db.execute(sql, &bound).err());
        let said = format!("{:?}", db.execute(&format!("EXPLAIN {sql}"), &bound).err());
        if said != ran {
            told.push(format!("ran: {ran}\nexplained: {said}"));
        }
    }
    assert!(told.is_empty(), "{told:#?}");
}

#[test]
fn explaining_a_statement_changes_nothing_in_the_store() {
    let (folder, db) = store("explain-changes-nothing");
    let before = bytes_of(&folder);
    for sql in [
        "SELECT name FROM t WHERE id = 7",
        "DELETE FROM t WHERE id = 7",
        "UPDATE t SET name = 'gone' WHERE tag = 3",
        "INSERT INTO t (id, name, tag) VALUES (41, 'new', 1)",
    ] {
        explained(&db, sql);
        // No file is written, made or removed by the asking.
        assert!(
            bytes_of(&folder) == before,
            "{sql}\nchanged a file of the store"
        );
    }
    // Nothing the statements name was done.
    support::assert_rows_are(&db, 1..=40);
}

#[test]
fn a_narrowed_handle_is_told_no_row_or_count_of_what_it_may_not_see() {
    let (_, db) = store("explain-narrowed");
    support::run(
        &db,
        "CREATE TABLE passages (id INTEGER PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
         v VECTOR(3) PARTITION_KEY (context_id) SEARCH_MODE EXACT, UNIQUE (context_id, id)) \
         MAINTAIN COUNT (context_id)",
    );
    // Three passages in the context the handle is given and 137 in one it is not: 140 in all.
    let (own, hidden) = (Uuid::from_u128(1), Uuid::from_u128(2));
    let tx = db.transaction().expect("begin");
    for n in 1..=140_i64 {
        tx.execute(
            "INSERT INTO passages (id, context_id, v) VALUES ($id, $ctx, $v)",
            &params! {
                "id" => 1000 + n, "ctx" => if n <= 3 { own } else { hidden },
                "v" => support::vector(n, 3),
            },
        )
        .expect("insert a passage");
    }
    tx.commit().expect("commit");
    let narrowed = db.scoped_with_contexts(BTreeSet::from([ContextId::new(own)]));
    let count = support::int(&narrowed, "SELECT COUNT(*) AS n FROM passages");
    assert_eq!(count, 3, "the rows of its own Context");
    // Explaining a search through a WITH answers like any other statement.
    let with = "WITH x AS (SELECT id, v FROM passages) SELECT id FROM x ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 1";
    explained(&db, with);
    // It is told no key, route or refusal: each would show the state of rows it may not see.
    let own_search = "SELECT id FROM passages ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 2";
    let bare = |reader: &Database, sql: &str| {
        let text = explained(reader, sql);
        for line in ["key:", "route:", "refusal:"] {
            assert!(!text.contains(line), "told `{line}`:\n{text}");
        }
        text
    };
    bare(&narrowed, own_search);
    bare(&db.scoped_with_contexts(BTreeSet::new()), own_search);
    for sql in [
        own_search,
        "SELECT COUNT(*) AS n FROM passages",
        "SELECT context_id, COUNT(*) AS n FROM passages GROUP BY context_id",
    ] {
        let text = explained(&narrowed, sql);
        for secret in [hidden.to_string(), "137".to_owned(), "140".to_owned()] {
            assert!(!text.contains(&secret), "{sql}\nsays `{secret}`:\n{text}");
        }
    }
    // Writes to rows it may not see change neither what it is told nor what INDEXED search gives.
    for (t, key, n) in [("m", "", 1), ("d", "PARTITION_KEY (c)", 4)] {
        let make = format!(
            "CREATE TABLE {t} (id INTEGER PRIMARY KEY, c UUID NOT NULL CONTEXT_ID, v VECTOR(3) {key} INDEXING_THRESHOLD {n})"
        );
        support::run(&db, &make);
        let put = |ids: std::ops::Range<i64>, who: Uuid, v: &str| {
            for n in ids {
                support::run(&db, &format!("INSERT INTO {t} VALUES ({n}, '{who}', {v})"));
            }
        };
        let sql =
            format!("SELECT id FROM {t} ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR INDEXED LIMIT 2");
        let ask = || (bare(&narrowed, &sql), support::rows(&narrowed, &sql));
        put(1..4, own, "[1.0, 0.0, 0.0]");
        support::seal_all(&db);
        let before = ask();
        put(4..6, hidden, "[1.0, 0.0, 0.0]");
        assert_eq!(ask(), before, "{t}: unindexed hidden rows");
        support::seal_all(&db);
        assert_eq!(ask(), before, "{t}: indexed hidden rows");
        put(6..8, own, "[0.0, 1.0, 0.0]");
        assert_eq!(ask(), before, "{t}: own rows further away");
    }
}

#[test]
fn explaining_a_write_also_says_the_path_of_the_select_inside_it() {
    let (_, db) = store("explain-write-select");
    support::run(
        &db,
        "CREATE TABLE owners (id INTEGER PRIMARY KEY, t_id INTEGER NOT NULL)",
    );
    support::run(&db, "CREATE INDEX owners_t ON owners (t_id)");
    let inner = "(SELECT t_id FROM owners WHERE t_id = 3)";
    for write in [
        format!("DELETE FROM t WHERE id = 7 AND tag IN {inner}"),
        format!("UPDATE t SET name = 'x' WHERE id = 7 AND tag IN {inner}"),
    ] {
        // The write's own table by its key, and the inner table through its index.
        assert_says(&db, &write, &["index: __pk_id }", "index: owners_t }"]);
    }
}
