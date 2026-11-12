//! The table source: the one place a statement's rows come from. A narrowed reader that names a
//! hidden row whole is told which rule hid it, a list of keys and an `IS NULL` over an index
//! read only the rows they return, and a large value no statement asked for is never loaded.

mod support;

use std::collections::BTreeSet;

use contextdb::{ContextId, Error, Principal, ScopeLabel, Uuid, Value, params};

#[test]
fn a_narrowed_reader_naming_a_hidden_row_by_its_key_is_refused_by_the_rule_that_hid_it() {
    let folder = support::folder("table-source-hidden-row-by-key");
    let db = support::open(&folder);
    let context = |n: u128| Uuid::from_u128(0x00C0_0000 + n);
    let acl = |n: u128| Uuid::from_u128(0x00AC_0000 + n);

    // Row 1 of each table is in the reader's view and row 2 is hidden by the table's rule.
    support::run(
        &db,
        "CREATE TABLE placed (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, payload TEXT)",
    );
    support::run(
        &db,
        "CREATE TABLE labelled (id INTEGER PRIMARY KEY, reach TEXT SCOPE_LABEL_READ ('public', \
         'internal') WRITE ('public', 'internal'), payload TEXT)",
    );
    support::run(
        &db,
        "CREATE TABLE acl_grants (id INTEGER PRIMARY KEY, principal_kind TEXT, \
         principal_id TEXT, acl_id UUID)",
    );
    support::run(
        &db,
        "CREATE TABLE guarded (id INTEGER PRIMARY KEY, \
         acl_id UUID ACL REFERENCES acl_grants(acl_id), payload TEXT)",
    );
    for (id, label) in [(1_i64, "public"), (2, "internal")] {
        let n = u128::try_from(id).expect("a small id");
        db.execute(
            "INSERT INTO placed (id, context_id, payload) VALUES ($id, $context, 'kept')",
            &params! { "id" => id, "context" => context(n) },
        )
        .expect("insert a placed row");
        db.execute(
            "INSERT INTO labelled (id, reach, payload) VALUES ($id, $reach, 'kept')",
            &params! { "id" => id, "reach" => label },
        )
        .expect("insert a labelled row");
        db.execute(
            "INSERT INTO guarded (id, acl_id, payload) VALUES ($id, $acl, 'kept')",
            &params! { "id" => id, "acl" => acl(n) },
        )
        .expect("insert a guarded row");
    }
    db.execute(
        "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) \
         VALUES (1, 'Agent', 'a', $acl)",
        &params! { "acl" => acl(1) },
    )
    .expect("grant access id 1");

    let contexts: BTreeSet<ContextId> = [ContextId::new(context(1))].into();
    let labels: BTreeSet<ScopeLabel> = [ScopeLabel::new("public")].into();
    let agent = Principal::Agent("a".to_owned());
    let in_one = db.scoped_with_constraints(Some(contexts), None, None);
    let public = db.scoped_with_constraints(None, Some(labels), None);
    let granted = db.scoped_with_constraints(None, None, Some(agent));

    let kept = vec![vec![Value::Text("kept".to_owned())]];
    for (reader, table) in [
        (&in_one, "placed"),
        (&public, "labelled"),
        (&granted, "guarded"),
    ] {
        // The row in view is returned, and a key no row has is an empty answer, not a refusal.
        let shown = format!("SELECT payload FROM {table} WHERE id = 1");
        assert_eq!(support::rows(reader, &shown), kept, "{shown}");
        let absent = format!("SELECT payload FROM {table} WHERE id = 99");
        assert_eq!(
            support::rows(reader, &absent),
            Vec::<Vec<Value>>::new(),
            "{absent}"
        );

        let hidden = format!("SELECT payload FROM {table} WHERE id = 2");
        let refused = support::refusal(reader, &hidden);
        let named = match (table, &refused) {
            ("placed", Error::ContextScopeViolation { table }) => table,
            ("labelled", Error::ScopeLabelViolation { table }) => table,
            ("guarded", Error::AclDenied { table }) => table,
            _ => panic!("{hidden}\nwas refused with {refused:?}, not by the rule of {table}"),
        };
        assert_eq!(named, table, "{hidden}");
        // To a reader not narrowed the row is there.
        assert_eq!(support::rows(&db, &hidden), kept, "{hidden}, not narrowed");
    }
}

#[test]
fn a_list_of_keys_and_an_is_null_over_an_index_read_only_the_rows_they_return() {
    let folder = support::folder("table-source-in-and-is-null");
    let db = support::open(&folder);
    support::run(&db, "CREATE TABLE n (id INTEGER PRIMARY KEY, c INTEGER)");
    support::run(&db, "CREATE INDEX n_c ON n (c)");
    // Of a thousand rows, the ten whose id is a multiple of 100 have no `c`.
    let tx = db.transaction().expect("begin");
    for id in 1..=1000_i64 {
        let c = if id % 100 == 0 {
            Value::Null
        } else {
            Value::Int64(id % 7)
        };
        tx.execute(
            "INSERT INTO n (id, c) VALUES ($id, $c)",
            &params! { "id" => id, "c" => c },
        )
        .expect("insert a row");
    }
    tx.commit().expect("commit");

    let listed = support::run(
        &db,
        "SELECT id FROM n WHERE id IN (3, 500, 997) ORDER BY id",
    );
    let wanted = [3_i64, 500, 997].map(|id| vec![Value::Int64(id)]);
    assert_eq!(listed.rows(), wanted, "the rows of the three keys");
    assert_eq!(listed.rows_examined(), 3, "rows read for three keys");

    // A key in the list that no row has is not a row read.
    let listed = support::run(&db, "SELECT id FROM n WHERE id IN (3, 5000) ORDER BY id");
    assert_eq!(
        listed.rows(),
        [[Value::Int64(3)]],
        "the one key that is there"
    );
    assert_eq!(
        listed.rows_examined(),
        1,
        "rows read for one key that is there"
    );

    let nulls = support::run(&db, "SELECT id FROM n WHERE c IS NULL ORDER BY id");
    let wanted: Vec<Vec<Value>> = (1..=10).map(|n| vec![Value::Int64(n * 100)]).collect();
    assert_eq!(nulls.rows(), wanted, "the rows with no `c`");
    assert_eq!(
        nulls.rows_examined(),
        10,
        "rows read for ten rows with no `c`"
    );
}

#[test]
fn a_large_value_no_statement_selects_is_not_loaded() {
    const TEXT: usize = 2 << 20;
    let folder = support::folder("table-source-unselected-large-value");
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE big (id INTEGER PRIMARY KEY, title TEXT, body TEXT)",
    );
    let body = support::letters(7, TEXT);
    db.execute(
        "INSERT INTO big (id, title, body) VALUES (1, 'first', $body)",
        &params! { "body" => body.clone() },
    )
    .expect("insert the row with the large text");
    support::close(db);

    // The limit is what the open store has in use after the same small read, and half the
    // text's size more: room for the row's small values and none for its text.
    let db = support::open(&folder);
    let title = "SELECT id, title FROM big WHERE id = 1";
    let small = vec![vec![Value::Int64(1), Value::Text("first".to_owned())]];
    assert_eq!(support::rows(&db, title), small, "before the limit");
    db.set_memory_limit(Some(db.memory_used() + TEXT / 2))
        .expect("set a limit above what is in use");

    assert_eq!(
        support::rows(&db, title),
        small,
        "the row read without its text, under the limit"
    );
    match support::refusal(&db, "SELECT body FROM big WHERE id = 1") {
        Error::MemoryBudgetExceeded { .. } => {}
        other => panic!("selecting the text under the limit was refused with {other:?}"),
    }
    assert_eq!(
        support::rows(&db, title),
        small,
        "the row read without its text, after the refusal"
    );

    // With the limit lifted the text is there, whole.
    db.set_memory_limit(None).expect("lift the limit");
    let found = support::rows(&db, "SELECT body FROM big WHERE id = 1");
    support::assert_text_is(&found[0][0], &body, "the text, with no limit");
}
