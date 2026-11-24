//! `Database::declared_events` lists what is declared, so a host need not read history, and
//! `Database::declared_table` names the table a `CREATE TABLE` text declares, without running it.

mod support;

use contextdb::Database;
use std::mem::discriminant as kind;

#[test]
fn declared_events_and_a_create_tables_table_are_read_from_the_store() {
    let folder = support::folder("declared");
    let db = support::open(&folder);
    for sql in [
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
        "CREATE TABLE tags (id INTEGER PRIMARY KEY)",
        "CREATE TRIGGER on_note ON notes WHEN INSERT",
        "CREATE TRIGGER on_edit ON notes WHEN UPDATE INCLUDING SYNC",
        "CREATE EVENT TYPE Note_Added WHEN INSERT ON notes",
        "CREATE EVENT TYPE note_removed WHEN DELETE ON notes",
        "CREATE SINK audit TYPE callback",
        "CREATE SINK hook TYPE webhook URL 'https://example.invalid/hook'",
        "CREATE ROUTE to_audit EVENT note_added TO audit WHERE body IN ('urgent', 'late')",
        "CREATE ROUTE to_hook EVENT note_added TO hook",
    ] {
        support::run(&db, sql);
    }
    // Each kind's list with every field, in name order whatever order the store keeps.
    fn sorted(list: impl Iterator<Item = impl std::fmt::Debug>) -> String {
        let mut list: Vec<String> = list.map(|item| format!("{item:?}")).collect();
        list.sort();
        format!("[{}]", list.join(", "))
    }
    let listed = |db: &contextdb::Database| {
        let events = db.declared_events().expect("the declared events");
        let (triggers, types) = (events.triggers.iter(), events.event_types.iter());
        let routes = events.routes.iter();
        [
            sorted(triggers.map(|t| (&t.name, &t.table, &t.event, t.including_sync))),
            sorted(types.map(|e| (&e.name, &e.table, &e.event))),
            sorted(events.sinks.iter().map(|s| (&s.name, &s.sink_type, &s.url))),
            sorted(routes.map(|r| {
                let where_in = r.where_in.as_ref().map(|w| (&w.column, &w.values));
                (&r.name, &r.event_type, &r.sink, where_in)
            })),
        ]
        .join("\n")
    };
    let wanted = [
        r#"[("on_edit", "notes", Update, true), ("on_note", "notes", Insert, false)]"#,
        r#"[("note_added", "notes", Insert), ("note_removed", "notes", Delete)]"#,
        r#"[("audit", Callback, None), ("hook", Webhook, Some("https://example.invalid/hook"))]"#,
        concat!(
            r#"[("to_audit", "note_added", "audit", Some(("body", ["urgent", "late"]))), "#,
            r#"("to_hook", "note_added", "hook", None)]"#
        ),
    ];
    assert_eq!(
        listed(&db),
        wanted.join("\n"),
        "each declaration is listed by name with its fields, a name folded as a table's is, \
         and a webhook sink with the URL it was declared with"
    );

    // A name that is not declared is refused unless IF EXISTS is written (`events.sql`), in
    // words naming the kind as Postgres does; a trigger ON another table is not there for it.
    for (sql, words) in [
        ("DROP TRIGGER nope", ["trigger 'nope'", "does not exist"]),
        ("DROP ROUTE nope", ["route 'nope'", "does not exist"]),
        ("DROP SCHEDULE nope", ["schedule 'nope'", "does not exist"]),
        (
            "DROP TRIGGER on_edit ON tags",
            ["trigger 'on_edit' for table 'tags'", "does not exist"],
        ),
        (
            "CREATE TRIGGER on_edit ON notes WHEN UPDATE INCLUDING SYNC",
            ["trigger 'on_edit'", "already exists"],
        ),
    ] {
        let refusal = support::refusal(&db, sql).to_string();
        assert!(
            words.iter().all(|w| refusal.contains(w)),
            "{sql}\nwas refused as: {refusal}"
        );
    }

    // A drop takes the declaration out of the list, and the list is the same after a reopen.
    support::run(&db, "DROP ROUTE to_hook");
    support::run(&db, "DROP TRIGGER on_note");
    let after_drop = listed(&db);
    assert!(
        !after_drop.contains("to_hook")
            && !after_drop.contains("on_note")
            && after_drop.contains("to_audit"),
        "a dropped route and trigger are no longer listed:\n{after_drop}"
    );
    let db = support::reopen(db, &folder);
    assert_eq!(
        listed(&db),
        after_drop,
        "the declarations are schema records kept by the store"
    );

    // DROP TABLE takes the table's triggers and event types, and the routes from those event
    // types, out of the store, so a reopen brings none of them back; the sinks stay.
    support::run(&db, "DROP TABLE notes");
    let db = support::reopen(db, &folder);
    assert_eq!(listed(&db), ["[]", "[]", wanted[2], "[]"].join("\n"));

    // The table a CREATE TABLE text declares, named as the engine names it; the text is not run.
    for (sql, table) in [
        ("CREATE TABLE Mixed (id INTEGER)", "mixed"),
        ("CREATE TABLE \"Two Words\" (id INTEGER)", "Two Words"),
        ("create table if not exists kept (id INTEGER)", "kept"),
        ("/* a */ CREATE TABLE -- b\n z (id INTEGER) SYNC OFF", "z"),
        (
            "CREATE TABLE evidence (id INTEGER PRIMARY KEY, context_id UUID REFERENCES contexts(id))",
            "evidence",
        ),
    ] {
        let (name, meta): (_, contextdb::TableMeta) = Database::declared_table(sql).expect(sql);
        assert_eq!(name, table, "{sql}");
        assert_eq!(meta.columns[0].name, "id", "{sql}");
        assert!(db.table_meta(table).is_err(), "declared_table ran `{sql}`");
    }
    for sql in [
        "CREATE TABLE nodes (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES nodes(missing))",
        "CREATE TABLE ranked (id INTEGER PRIMARY KEY, tag_id INTEGER, embedding VECTOR(3) RANK_POLICY (JOIN tags ON id, FORMULA '(', SORT_KEY score))",
    ] {
        let declared = Database::declared_table(sql).unwrap_err();
        assert_eq!(kind(&declared), kind(&support::refusal(&db, sql)), "{sql}");
    }
    for sql in ["CREATE INDEX i ON t (b)", "SELECT 1", "CREATE TABLE ("] {
        assert!(
            Database::declared_table(sql).is_err(),
            "`{sql}` declares no table, and is refused rather than named"
        );
    }
}

#[test]
fn declared_table_refuses_oversized_text_and_still_reads_an_ordinary_one() {
    let (name, meta) =
        Database::declared_table("CREATE TABLE Notes (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("an ordinary CREATE TABLE gives its metadata");
    assert_eq!((name.as_str(), meta.columns.len()), ("notes", 2));
    let huge = format!(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY){}",
        " ".repeat(8 << 20)
    );
    assert!(
        matches!(
            Database::declared_table(&huge),
            Err(contextdb::Error::MemoryBudgetExceeded { .. })
        ),
        "text of 8 MiB was parsed without the bounded parse refusal"
    );
}
