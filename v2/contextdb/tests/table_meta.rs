//! What `Database::table_meta` says of a declared table: its columns, its UNIQUE rules, its
//! indexes and every rule it declares, the same after the store is closed and opened.

mod support;

use contextdb::{ColumnType, Error};

#[test]
fn table_meta_gives_each_tables_rules() {
    let folder = support::folder("table-meta");
    let db = support::open(&folder);
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

    // A table that is not there, by name; and the same descriptions after close and open.
    assert!(matches!(db.table_meta("nope"), Err(Error::TableNotFound(name)) if name == "nope"));
    support::close(db);
    let db = support::open(&folder);
    for ((table, _), before) in options.iter().zip(&described) {
        let after = format!("{:?}", db.table_meta(table).expect("a description"));
        assert_eq!(&after, before, "{table} after close and open");
    }
}
