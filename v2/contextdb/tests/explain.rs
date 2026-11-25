//! `Database::explain`: the path a statement would take, said in the documented words, without
//! running it.

mod support;

use std::path::{Path, PathBuf};

use contextdb::{Database, Uuid, params};

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
        .to_string()
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
    // would make it countable for each partition: an index with the partition's column first
    // (section 6, the recovery line). An index on `tag` alone serves the filter but crosses the
    // partitions, so the find walks, prints no cost and the line still names that index (the
    // subquery case below asserts it).
    let by_tag = "SELECT id FROM docs WHERE tag = 3 ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 3";
    let recovery = "recovery: CREATE INDEX docs_ctx_tag_idx ON docs (ctx, tag)";
    assert_says(&db, by_tag, &["filter_path: none", recovery]);
    support::run(&db, "CREATE INDEX docs_tag_idx ON docs (tag)");
    assert_says(
        &db,
        "SELECT id FROM docs WHERE id <= 2 OR tag = 3 ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 3",
        &["OR arms reach the table by different paths"],
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
    let walked = text.contains("filter_route: walk") && text.contains(recovery);
    assert!(walked && !text.contains("_cost:"), "{text}");
    // With the partition's column first the filter is countable for each partition: one row of
    // tag 3 lies in its partition, and comparing it directly costs that one row.
    support::run(&db, "CREATE INDEX docs_ctx_tag_idx ON docs (ctx, tag)");
    let costs = ["filter_route: direct", "direct_cost: 1", "walk_cost: "];
    assert_says(&db, by_tag, &costs);
    // Every row passing and one asked for, the walk costs less than reading every row.
    assert_says(
        &db,
        "SELECT id FROM docs WHERE tag >= 0 ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 1",
        &["filter_route: walk"],
    );
    // The rows asked for are the limit, the offset and a second sort key's lookahead: tags 5 to
    // 8 are two rows of each partition's four and LIMIT 2 OFFSET 2 asks for four, so the walk
    // would cost more.
    assert_says(
        &db,
        "SELECT id FROM docs WHERE tag >= 5 ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 2 OFFSET 2",
        &["filter_route: direct"],
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
    // Context Graph's receipt read: UUID equality, a residual flag, then the ordinal order.
    for sql in [
        "CREATE TABLE event_search_generations (id UUID PRIMARY KEY)",
        "CREATE TABLE event_search_passages (id UUID PRIMARY KEY, generation_id UUID NOT NULL REFERENCES event_search_generations(id), passage_ordinal INTEGER NOT NULL, content_sha256 TEXT NOT NULL, is_current BOOLEAN NOT NULL)",
        "CREATE INDEX event_search_passages_generation_ordinal_idx ON event_search_passages (generation_id, passage_ordinal)",
        "INSERT INTO event_search_generations VALUES ('00000000-0000-0000-0000-000000000001'), ('00000000-0000-0000-0000-000000000002')",
        "INSERT INTO event_search_passages VALUES ('00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000001', 20, 'active-20', true), ('00000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000001', 10, 'active-10', true), ('00000000-0000-0000-0000-000000000005', '00000000-0000-0000-0000-000000000001', 30, 'retired', false), ('00000000-0000-0000-0000-000000000006', '00000000-0000-0000-0000-000000000002', 5, 'other', true)",
    ] {
        support::run(&db, sql);
    }
    let sql = "SELECT passage_ordinal, content_sha256 FROM event_search_passages WHERE generation_id = $generation_id AND is_current = $current ORDER BY generation_id ASC, passage_ordinal ASC";
    let bound = params! { "generation_id" => Uuid::from_u128(1), "current" => true };
    let rows = db.execute(sql, &bound).unwrap();
    assert_eq!(rows.rows().len(), 2);
    assert_eq!(rows.rows()[0], vec![10.into(), "active-10".into()]);
    assert_eq!(rows.rows()[1], vec![20.into(), "active-20".into()]);
    assert_eq!(rows.rows_examined(), 3);
    let plan = db.execute(&format!("EXPLAIN {sql}"), &bound).unwrap();
    assert_eq!(
        plan.rows()[0][0],
        "IndexScan { index: event_search_passages_generation_ordinal_idx }".into()
    );
    assert!(!format!("{:?}", plan.rows()).contains("Sort"));
    // A graph step is explained by the index its run reads: here the one its edge type leads.
    for sql in [
        "CREATE TABLE links (id INTEGER PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL)",
        "CREATE INDEX links_type ON links (edge_type, source_id)",
        "INSERT INTO links VALUES (1, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'A'), (2, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000003', 'A'), (3, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000004', 'B')",
    ] {
        support::run(&db, sql);
    }
    let one = "WHERE a.id = '00000000-0000-0000-0000-000000000001' COLUMNS (b.id AS b_id)";
    let step = |arrow| format!("SELECT b_id FROM GRAPH_TABLE(links MATCH (a){arrow}(b) {one})");
    let out = step("-[:B]->");
    let examined = support::run(&db, &out).rows_examined();
    assert_eq!(examined, 1, "only the B edge");
    let typed = ["index: links_type }", "[edge_type, source_id]"];
    assert_says(&db, &out, &typed);
    // An undirected step reads the edges at each end in turn: two head lines, one for each
    // index, which no line of an index it rejected holds.
    let target = "index: __graph_edge_target_source_type }";
    assert_says(&db, &step("-[:B]-"), &["index: links_type }", target]);
    // A kept count this read cannot use still prints its line.
    let kept = "CREATE TABLE kept (id INTEGER PRIMARY KEY, ctx TEXT, UNIQUE (ctx, id)) MAINTAIN COUNT (ctx)";
    support::run(&db, kept);
    let count = "SELECT COUNT(*) FROM kept WHERE ctx = 'a'";
    assert_says(
        &db.scoped_with_contexts(Default::default()),
        count,
        &["maintained count"],
    );
}
