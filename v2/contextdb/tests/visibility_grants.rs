//! Which grants count. A row under an access id is shown to a principal only through a grant row
//! that the reader may itself see, in a grant table that is there and has the columns a grant is
//! read from; a grant row counts whatever its table's columns have become since it was written
//! and however long its principal's name is; and one statement sees one set of grants.

mod support;

use std::collections::{BTreeSet, HashMap};

use contextdb::{ContextId, Database, Error, Principal, ScopeLabel, Uuid, Value, params};

use support::Run;

// ---- who is asking ----

/// Context number `n`.
fn context(n: u128) -> Uuid {
    Uuid::from_u128(0x00C0_0000 + n)
}

/// Access-control id number `n`.
fn acl(n: u128) -> Uuid {
    Uuid::from_u128(0x00AC_0000 + n)
}

fn context_set(numbers: &[u128]) -> BTreeSet<ContextId> {
    numbers
        .iter()
        .map(|n| ContextId::new(context(*n)))
        .collect()
}

fn label_set(names: &[&str]) -> BTreeSet<ScopeLabel> {
    names.iter().map(|name| ScopeLabel::new(*name)).collect()
}

/// A handle that names this agent and nothing else.
fn agent(db: &Database, name: &str) -> Database {
    db.scoped_with_constraints(None, None, Some(Principal::Agent(name.to_owned())))
}

// ---- what a statement answered ----

/// Runs a statement that must not be refused.
fn put(on: &impl Run, sql: &str, values: &HashMap<String, Value>) {
    if let Err(error) = on.run(sql, values) {
        panic!("{sql}\nwas refused: {error:?}");
    }
}

/// The ids `SELECT id FROM <table> ORDER BY id` returns, which must be INTEGERs.
fn seen(on: &impl Run, table: &str) -> Vec<i64> {
    let sql = format!("SELECT id FROM {table} ORDER BY id");
    let first = |row: &Vec<Value>| match row.first() {
        Some(Value::Int64(id)) => *id,
        other => panic!("{sql}\nreturned {other:?} where an INTEGER was wanted"),
    };
    support::rows(on, &sql).iter().map(first).collect()
}

/// Asserts that a statement is refused for want of a grant on this table.
fn denied(on: &impl Run, sql: &str, values: &HashMap<String, Value>, table: &str) {
    match on.run(sql, values) {
        Err(Error::AclDenied { table: named }) => assert_eq!(named, table, "{sql}"),
        Err(other) => panic!("{sql}\nwas refused with {other:?}; AclDenied was wanted"),
        Ok(result) => panic!("{sql}\nwas not refused: {result:?}"),
    }
}

// ---- tables of grants and the rows they guard ----

/// Declares a grant table with these columns after the four every grant table here has.
fn declare_grants(db: &Database, table: &str, more: &str) {
    support::run(
        db,
        &format!(
            "CREATE TABLE {table} (id INTEGER PRIMARY KEY, principal_kind TEXT, \
             principal_id TEXT, acl_id UUID{more})"
        ),
    );
}

/// Declares a table whose rows are guarded by grants of `grants`, with a row under each of
/// these access ids, keyed by the id's number.
fn declare_guarded(db: &Database, table: &str, grants: &str, under: &[u128]) {
    support::run(
        db,
        &format!(
            "CREATE TABLE {table} (id INTEGER PRIMARY KEY, \
             acl_id UUID ACL REFERENCES {grants}(acl_id), payload TEXT)"
        ),
    );
    for n in under {
        let id = i64::try_from(*n).expect("a small access id number");
        put(
            db,
            &format!("INSERT INTO {table} (id, acl_id, payload) VALUES ($id, $acl, 'kept')"),
            &params! { "id" => id, "acl" => acl(*n) },
        );
    }
}

/// Writes grant row `id` of `table`: this agent holds access id number `n`. `more` names
/// further columns, each filled from the parameter of its own name in `values`.
fn grant(db: &Database, table: &str, id: i64, name: &str, n: u128, more: &[(&str, Value)]) {
    let mut columns = String::from("id, principal_kind, principal_id, acl_id");
    let mut places = String::from("$id, 'Agent', $name, $acl");
    let mut values = params! { "id" => id, "name" => name, "acl" => acl(n) };
    for (column, value) in more {
        columns.push_str(&format!(", {column}"));
        places.push_str(&format!(", ${column}"));
        values.insert((*column).to_owned(), value.clone());
    }
    put(
        db,
        &format!("INSERT INTO {table} ({columns}) VALUES ({places})"),
        &values,
    );
}

// ---- the five cases ----

#[test]
fn a_grant_row_the_reader_may_not_see_gives_no_access() {
    let folder = support::folder("grants-hidden-grant-row");
    let db = support::open(&folder);
    let a = Some(Principal::Agent("a".to_owned()));

    // Hidden by its Context: the grant of access id 2 is in a Context the reader is not in.
    declare_grants(&db, "placed_grants", ", context_id UUID CONTEXT_ID");
    declare_guarded(&db, "placed", "placed_grants", &[1, 2]);
    for (id, n) in [(1, 1), (2, 2)] {
        let place = [("context_id", Value::Uuid(context(n)))];
        grant(&db, "placed_grants", id, "a", n, &place);
    }
    let in_one = db.scoped_with_constraints(Some(context_set(&[1])), None, a.clone());
    let in_both = db.scoped_with_constraints(Some(context_set(&[1, 2])), None, a.clone());
    assert_eq!(seen(&in_one, "placed"), [1], "a grant in another Context");
    assert_eq!(seen(&in_both, "placed"), [1, 2], "both grants in view");

    // Hidden by its label: the grant of access id 2 is under a label the reader does not hold.
    declare_grants(
        &db,
        "labelled_grants",
        ", reach TEXT SCOPE_LABEL_READ ('public', 'internal') WRITE ('public', 'internal')",
    );
    declare_guarded(&db, "labelled", "labelled_grants", &[1, 2]);
    for (id, n, label) in [(1, 1, "public"), (2, 2, "internal")] {
        let reach = [("reach", Value::Text(label.to_owned()))];
        grant(&db, "labelled_grants", id, "a", n, &reach);
    }
    let public = db.scoped_with_constraints(None, Some(label_set(&["public"])), a.clone());
    let both = db.scoped_with_constraints(None, Some(label_set(&["public", "internal"])), a);
    assert_eq!(
        seen(&public, "labelled"),
        [1],
        "a grant under another label"
    );
    assert_eq!(seen(&both, "labelled"), [1, 2], "both grants in view");
}

#[test]
fn a_grant_table_that_is_missing_or_lacks_the_grant_column_grants_nobody() {
    let folder = support::folder("grants-no-grant-table");
    let db = support::open(&folder);
    let reader = agent(&db, "a");

    for (table, grants, remove) in [
        ("orphans", "gone_grants", "DROP TABLE gone_grants"),
        (
            "strays",
            "cut_grants",
            "ALTER TABLE cut_grants DROP COLUMN acl_id",
        ),
    ] {
        declare_grants(&db, grants, "");
        declare_guarded(&db, table, grants, &[1, 2]);
        grant(&db, grants, 1, "a", 1, &[]);
        assert_eq!(seen(&reader, table), [1], "{table}, while the grant stands");

        support::run(&db, remove);
        assert_eq!(seen(&reader, table), [0_i64; 0], "{table}, after {remove}");
        assert_eq!(
            seen(&db, table),
            [1, 2],
            "{table}, to a reader not narrowed"
        );
        let changed = support::run(&reader, &format!("UPDATE {table} SET payload = 'taken'"));
        assert_eq!(
            changed.rows_affected(),
            0,
            "{table}, an UPDATE of hidden rows"
        );
        denied(
            &reader,
            &format!("INSERT INTO {table} (id, acl_id, payload) VALUES (3, $acl, 'new')"),
            &params! { "acl" => acl(1) },
            table,
        );
        assert_eq!(
            seen(&db, table),
            [1, 2],
            "{table}, after the refused writes"
        );
    }
}

#[test]
fn a_grant_row_older_than_an_added_column_or_carrying_a_dropped_one_still_counts() {
    let folder = support::folder("grants-altered-grant-table");
    let db = support::open(&folder);
    let reader = agent(&db, "a");
    declare_grants(&db, "acl_grants", ", memo TEXT");
    declare_guarded(&db, "notes", "acl_grants", &[1, 2, 3, 4]);

    // Grant 1 carries a cell of `memo`, which is then dropped.
    let memo = [("memo", Value::Text("written before the drop".to_owned()))];
    grant(&db, "acl_grants", 1, "a", 1, &memo);
    support::run(&db, "ALTER TABLE acl_grants DROP COLUMN memo");
    assert_eq!(seen(&reader, "notes"), [1], "after the column was dropped");

    // Grants 1 and 2 are written before `reason` is added; grant 3 after, with a value in it.
    grant(&db, "acl_grants", 2, "a", 2, &[]);
    support::run(&db, "ALTER TABLE acl_grants ADD COLUMN reason TEXT");
    assert_eq!(seen(&reader, "notes"), [1, 2], "after the column was added");
    let reason = [("reason", Value::Text("written after the add".to_owned()))];
    grant(&db, "acl_grants", 3, "a", 3, &reason);
    assert_eq!(seen(&reader, "notes"), [1, 2, 3], "old and new grant rows");
}

#[test]
fn a_statement_that_writes_its_own_grant_table_sees_the_grants_it_first_looked_up() {
    let folder = support::folder("grants-self-written");
    let db = support::open(&folder);
    // A grant table whose own rows are guarded by its grants. Every row is under access id 1,
    // and only row 1, the first by key, grants that id to the reader.
    declare_grants(&db, "seats", ", guard UUID ACL REFERENCES seats(acl_id)");
    for (id, name) in [(1, "a"), (2, "b"), (3, "b")] {
        grant(&db, "seats", id, name, 1, &[("guard", Value::Uuid(acl(1)))]);
    }
    let reader = agent(&db, "a");
    assert_eq!(seen(&reader, "seats"), [1, 2, 3], "before the statement");

    // The statement takes the reader's only grant away in the first row it changes, and still
    // reaches every row that grant showed it.
    let changed = support::run(&reader, "UPDATE seats SET principal_id = 'nobody'");
    assert_eq!(changed.rows_affected(), 3, "rows the one statement changed");
    let names = support::rows(&db, "SELECT principal_id FROM seats ORDER BY id");
    let nobody = vec![Value::Text("nobody".to_owned())];
    assert_eq!(names, vec![nobody; 3], "every row was rewritten");

    // The next statement looks the grants up afresh and finds none.
    assert_eq!(seen(&reader, "seats"), [0_i64; 0], "after the statement");
}

#[test]
fn a_grant_to_a_principal_whose_name_is_2000_bytes_long_is_honoured() {
    let folder = support::folder("grants-long-principal-name");
    let db = support::open(&folder);
    declare_grants(&db, "acl_grants", "");
    declare_guarded(&db, "notes", "acl_grants", &[1, 2]);
    let name = "p".repeat(2_000);
    let near = format!("{}q", &name[1..]);
    grant(&db, "acl_grants", 1, &name, 1, &[]);

    assert_eq!(
        seen(&agent(&db, &name), "notes"),
        [1],
        "the principal granted"
    );
    assert_eq!(
        seen(&agent(&db, &near), "notes"),
        [0_i64; 0],
        "a principal whose name differs in its last byte"
    );
}
