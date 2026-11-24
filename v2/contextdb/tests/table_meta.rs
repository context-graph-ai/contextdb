//! What `Database::table_meta` says of a declared table: its columns, its UNIQUE rules, its
//! indexes and every rule it declares, the same after the store is closed and opened.

mod support;

use contextdb::{ColumnType, Error};

#[test]
fn table_meta_gives_each_tables_rules() {
    let folder = support::folder("table-meta");
    let db = support::open(&folder);
    // Parsing a declaration reports the same split scope rules as the stored table.
    let sql = "CREATE TABLE scoped (id INTEGER PRIMARY KEY, \
               reach TEXT SCOPE_LABEL_READ ('public', 'team') WRITE ('team'))";
    let declared: contextdb::TableMeta = contextdb::Database::declared_table(sql).unwrap().1;
    support::run(&db, sql);
    let sql = [
        "CREATE TABLE \"a b\" (i INT)",
        "/**/ CREATE TABLE IF NOT EXISTS c (i INT)",
    ];
    let names = sql.map(|t| contextdb::Database::declared_table(t).unwrap().0);
    assert_eq!(names, ["a b", "c"], "quoted names, IF NOT EXISTS, comments");
    assert!(declared.scope_label.is_some());
    assert_eq!(
        declared.scope_label,
        db.table_meta("scoped").unwrap().scope_label
    );
    let columns = "id UUID PRIMARY KEY, kind TEXT NOT NULL IMMUTABLE, slug TEXT UNIQUE, \
                   status TEXT NOT NULL DEFAULT 'active', source_id UUID NOT NULL, \
                   target_id UUID NOT NULL, edge_type TEXT NOT NULL, UNIQUE (kind, slug)";
    // One table that declares no option, and one for each option, the same in everything else.
    let options = [
        ("plain", ""),
        ("frozen", "IMMUTABLE"),
        ("local", "SYNC OFF"),
        ("pushed", "SYNC PUSH ONLY"),
        ("latest", "SYNC CONFLICT KEEP LATEST"),
        ("current", "SYNC CONFLICT KEEP LATEST HISTORY CURRENT ONLY"),
        ("kept", "RETAIN 7 DAYS"),
        ("kept_safe", "RETAIN 7 DAYS SYNC SAFE"),
        ("moving", "STATE MACHINE (status: active -> [done])"),
        ("acyclic", "DAG('NEEDS')"),
        ("counted", "MAINTAIN COUNT (kind)"),
        ("counted_all", "MAINTAIN COUNT ALL PREFIXES"),
    ];
    for (table, option) in options {
        support::run(&db, &format!("CREATE TABLE {table} ({columns}) {option}"));
        support::run(
            &db,
            &format!("CREATE INDEX by_status ON {table} (status DESC, id)"),
        );
    }

    // What every one of them says of its columns, its UNIQUE rules and its indexes.
    let plain = db
        .table_meta("plain")
        .expect("the description of a declared table");
    let names: Vec<&str> = plain.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "id",
            "kind",
            "slug",
            "status",
            "source_id",
            "target_id",
            "edge_type"
        ]
    );
    let id = &plain.columns[0];
    assert_eq!(id.column_type, ColumnType::Uuid);
    assert!(id.primary_key && !plain.columns[1].primary_key);
    assert!(!plain.columns[1].nullable && plain.columns[2].nullable);
    assert!(plain.columns[2].unique && !plain.columns[3].unique);
    assert!(plain.columns[3].default.is_some() && plain.columns[2].default.is_none());
    assert_eq!(plain.unique_constraints, [["kind", "slug"]]);
    let indexes: Vec<&str> = plain.indexes.iter().map(|i| i.name.as_str()).collect();
    assert!(
        indexes.contains(&"__pk_id") && indexes.contains(&"by_status"),
        "{indexes:?}"
    );

    // Rules whose columns spell one name stay separate rules, each with its own index.
    support::run(
        &db,
        "CREATE TABLE pairs (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, a_b INTEGER, \
         UNIQUE (a_b), UNIQUE (a, b), UNIQUE (a, b))",
    );
    let pairs = db.table_meta("pairs").expect("a description");
    let names: Vec<&str> = pairs.indexes.iter().map(|i| i.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "__pk_id",
            "__unique_a_b",
            "__unique_a_b_2",
            "__unique_a_b_3"
        ],
        "UNIQUE (a_b) and UNIQUE (a, b) are two rules with two indexes, and a repeat is a third"
    );

    // A key written as an element of the table describes its column as the column-level form
    // (`id` above) does: not nullable, as in Postgres.
    assert!(!id.nullable, "a PRIMARY KEY column is NOT NULL");
    support::run(&db, "CREATE TABLE by_element (a INTEGER, PRIMARY KEY (a))");
    let by_element = db.table_meta("by_element").expect("a description");
    assert!(
        !by_element.columns[0].nullable,
        "PRIMARY KEY (a) makes `a` NOT NULL exactly as `a INTEGER PRIMARY KEY` does"
    );

    // IMMUTABLE on a table, and no other table says it.
    for (table, _) in options {
        let meta = db
            .table_meta(table)
            .expect("the description of a declared table");
        assert_eq!(meta.immutable, table == "frozen", "{table}");
    }
    // Every other option shows in the description: a table that declares one is described
    // differently from the table that declares none, and from every other.
    let described: Vec<String> = options
        .iter()
        .map(|(table, _)| format!("{:?}", db.table_meta(table).expect("a description")))
        .collect();
    for (first, one) in described.iter().enumerate() {
        for (second, other) in described.iter().enumerate().skip(first + 1) {
            assert_ne!(
                one, other,
                "{} and {} are described alike",
                options[first].0, options[second].0
            );
        }
    }

    // Every other option is a field too, read from the stored declaration: tables differing in
    // one option, each then given a column, are not described alike.
    support::run(&db, "CREATE TABLE plans (id UUID PRIMARY KEY)");
    let flow = "(id UUID PRIMARY KEY, status TEXT NOT NULL, v VECTOR(4), plan_id UUID \
                REFERENCES plans(id)@, other_id UUID REFERENCES plans(id)";
    let rule = " ON STATE archived PROPAGATE SET invalidated";
    let plain = flow.replace('@', "");
    let states = "STATE MACHINE (status: active -> [invalidated])";
    let written = |options: &[&str], table: &str| -> Vec<String> {
        let id = "(id INTEGER PRIMARY KEY";
        let table = table.replacen('(', id, 1);
        options.iter().map(|o| table.replace('@', o)).collect()
    };
    let groups = [
        // EDGE DISCARD, unwritten and by two of its words.
        written(&["", "EDGE DISCARD NEVER", "EDGE DISCARD ALWAYS"], "() @"),
        // Who may see a row: no labels, one list for reading and writing, a list for each.
        written(
            &[
                "",
                "SCOPE_LABEL ('a', 'b')",
                "SCOPE_LABEL_READ ('a', 'b') WRITE ('a')",
                "SCOPE_LABEL_READ () WRITE ()",
            ],
            "(, reach TEXT @)",
        ),
        // A VECTOR column's partition key, and its partition limit unwritten and written.
        written(
            &[
                "",
                "PARTITION_KEY (t)",
                "PARTITION_KEY (t) MAX_PARTITIONS 4",
            ],
            "(, t TEXT NOT NULL, v VECTOR(4) @)",
        ),
        // Its quantization and its search mode.
        written(
            &[
                "",
                "WITH (quantization = 'SQ8')",
                "SEARCH_MODE EXACT",
                "SEARCH_MODE INDEXED",
            ],
            "(, v VECTOR(4) @)",
        ),
        // PROPAGATE: none, through the first or the second of two references to one table,
        // along an edge, and leaving vectors out.
        vec![
            format!("{plain}) {states}"),
            format!("{}) {states}", flow.replace('@', rule)),
            format!("{plain}{rule}) {states}"),
            format!(
                "{plain}) {states} PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated"
            ),
            format!("{plain}) {states} PROPAGATE ON STATE invalidated EXCLUDE VECTOR"),
        ],
    ];
    let mut fields = Vec::new();
    for (g, group) in groups.iter().enumerate() {
        let start = fields.len();
        for (t, columns) in group.iter().enumerate() {
            let table = format!("option_{g}_{t}");
            support::run(&db, &format!("CREATE TABLE {table} {columns}"));
            support::run(&db, &format!("ALTER TABLE {table} ADD COLUMN extra TEXT"));
            let meta = db.table_meta(&table).expect("a description");
            fields.push((table, format!("{meta:?}")));
        }
        for (i, (one, first)) in fields[start..].iter().enumerate() {
            for (other, second) in &fields[start + i + 1..] {
                assert_ne!(
                    first, second,
                    "{one} and {other} differ in one option, and TableMeta has no field for it"
                );
            }
        }
    }

    // A table that is not there, by name; and the same descriptions after close and open.
    assert!(matches!(db.table_meta("nope"), Err(Error::TableNotFound(name)) if name == "nope"));
    for (i, members) in ["b, a, b", "a, b", "b"].iter().enumerate() {
        support::run(
            &db,
            &format!("CREATE TABLE m{i} (id INT PRIMARY KEY) DELIVERY MANIFEST OVER {members}"),
        );
    }
    support::close(db);
    let db = support::open(&folder);
    let meta = |name| db.table_meta(name).expect("manifest metadata after reopen");
    assert!(format!("{:?}", meta("m0")).contains(r#"["a", "b"]"#));
    assert_eq!(*meta("m0"), *meta("m1"));
    assert_ne!(*meta("m0"), *meta("m2"));
    for ((table, _), before) in options.iter().zip(&described) {
        let after = format!("{:?}", db.table_meta(table).expect("a description"));
        assert_eq!(&after, before, "{table} after close and open");
    }
    let mut options = contextdb::ReadSessionOptions::default();
    options.limits.tables = Some(fields.iter().map(|(name, _)| name.clone()).collect());
    let session = contextdb::ReadSession::open_with_options(&folder, options).unwrap();
    assert!(matches!(
        session.table_meta("plain"),
        Err(Error::ReadLimitExceeded { .. })
    ));
    for (table, before) in &fields {
        assert_eq!(&format!("{:?}", session.table_meta(table).unwrap()), before);
        let after = format!("{:?}", db.table_meta(table).expect("a description"));
        assert_eq!(
            &after, before,
            "{table}'s option fields after close and open"
        );
    }
}
