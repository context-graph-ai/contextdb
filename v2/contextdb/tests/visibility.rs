//! Who may see and write a row. A handle narrowed to Contexts, to scope labels or to a principal
//! reads the rows it may and no others, changes none it may not see, is refused a row it names
//! by its key by the rule that hid it, and is told nothing of what it may not see: not by a
//! count, not by a search, not by the words of a refusal.

mod support;

use std::collections::{BTreeSet, HashMap};

use contextdb::{
    ContextId, Database, DatabaseOpenOptions, Error, Principal, ScopeLabel, Uuid, Value, params,
};

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

/// A handle narrowed to the Contexts with these numbers.
fn in_contexts(db: &Database, numbers: &[u128]) -> Database {
    db.scoped_with_contexts(context_set(numbers))
}

/// A handle narrowed to these scope labels.
fn with_labels(db: &Database, names: &[&str]) -> Database {
    db.scoped_with_constraints(None, Some(label_set(names)), None)
}

/// A handle that names this principal and nothing else.
fn named(db: &Database, principal: Principal) -> Database {
    db.scoped_with_constraints(None, None, Some(principal))
}

fn agent(name: &str) -> Principal {
    Principal::Agent(name.to_owned())
}

fn human(name: &str) -> Principal {
    Principal::Human(name.to_owned())
}

// ---- what a statement answered ----

/// The first column of every row a statement returns, which must be an INTEGER.
fn ids(on: &impl Run, sql: &str) -> Vec<i64> {
    ids_with(on, sql, &params! {})
}

fn ids_with(on: &impl Run, sql: &str, values: &HashMap<String, Value>) -> Vec<i64> {
    let result = match on.run(sql, values) {
        Ok(result) => result,
        Err(error) => panic!("{sql}\nwas refused: {error:?}"),
    };
    let first = |row: &Vec<Value>| match row.first() {
        Some(Value::Int64(id)) => *id,
        other => panic!("{sql}\nreturned {other:?} where an INTEGER was wanted"),
    };
    result.rows().iter().map(first).collect()
}

/// The ids `SELECT id FROM <table> <tail>` returns.
fn seen(on: &impl Run, table: &str, tail: &str) -> Vec<i64> {
    ids(on, &format!("SELECT id FROM {table} {tail}"))
}

/// The first column of every row a statement returns, which must be TEXT.
fn texts(on: &impl Run, sql: &str) -> Vec<String> {
    let first = |row: &Vec<Value>| match row.first() {
        Some(Value::Text(text)) => text.clone(),
        other => panic!("{sql}\nreturned {other:?} where a TEXT was wanted"),
    };
    support::rows(on, sql).iter().map(first).collect()
}

/// How many rows a write changed. The write must not be refused.
fn changed(on: &impl Run, sql: &str) -> u64 {
    support::run(on, sql).rows_affected()
}

/// The rule a narrowed handle was refused by.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Rule {
    Context,
    Label,
    Grant,
    NoPrincipal,
}

/// Asserts that a statement is refused by this rule, naming this table, and returns the
/// refusal.
fn refused_with(
    on: &impl Run,
    sql: &str,
    values: &HashMap<String, Value>,
    rule: Rule,
    table: &str,
) -> Error {
    let refusal = match on.run(sql, values) {
        Err(refusal) => refusal,
        Ok(result) => panic!("{sql}\nwas not refused ({rule:?} was wanted): {result:?}"),
    };
    let found = match &refusal {
        Error::ContextScopeViolation { table } => (Rule::Context, table.as_str()),
        Error::ScopeLabelViolation { table } => (Rule::Label, table.as_str()),
        Error::AclDenied { table } => (Rule::Grant, table.as_str()),
        Error::PrincipalRequired { table } => (Rule::NoPrincipal, table.as_str()),
        other => panic!("{sql}\nwas refused with {other:?}; {rule:?} was wanted"),
    };
    assert_eq!(found, (rule, table), "{sql}");
    refusal
}

fn refused(on: &impl Run, sql: &str, rule: Rule, table: &str) -> Error {
    refused_with(on, sql, &params! {}, rule, table)
}

// ---- the documented tables: a grant table and notes under a Context and an access id ----

const GRANTS: &str = "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, \
                      principal_id TEXT, acl_id UUID)";

const NOTES: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, \
                     acl_id UUID ACL REFERENCES acl_grants(acl_id), payload TEXT)";

/// Grants a principal one access-control id. `number` makes the grant row's own key.
fn grant(db: &Database, number: u128, kind: &str, name: &str, acl_id: Uuid) {
    let values = params! {
        "id" => Uuid::from_u128(0x0060_0000 + number), "kind" => kind, "name" => name,
        "acl" => acl_id,
    };
    let sql = "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) \
               VALUES ($id, $kind, $name, $acl)";
    if let Err(error) = db.execute(sql, &values) {
        panic!("granting {acl_id} to {kind} {name} was refused: {error:?}");
    }
}

/// Writes note `id`, with the payload `note-<id>`, under this Context and this access id, or
/// under none of either for `None`.
fn note(
    on: &impl Run,
    id: i64,
    context_number: Option<u128>,
    acl_number: Option<u128>,
) -> contextdb::Result<contextdb::QueryResult> {
    let uuid = |id: Option<Uuid>| id.map_or(Value::Null, Value::Uuid);
    on.run(
        "INSERT INTO notes (id, context_id, acl_id, payload) VALUES ($id, $context, $acl, $payload)",
        &params! {
            "id" => id, "context" => uuid(context_number.map(context)),
            "acl" => uuid(acl_number.map(acl)), "payload" => format!("note-{id}"),
        },
    )
}

/// A store with the documented tables. The agent `report-reader` holds access id 1 and the
/// human `ana` holds access id 2; nobody holds access id 3. The notes:
///
/// | id | Context | access id |
/// |---|---|---|
/// | 1 | 1 | 1 |
/// | 2 | 1 | 2 |
/// | 3 | 2 | 1 |
/// | 4 | 2 | 2 |
/// | 5 | 1 | 3 |
/// | 6 | 1 | none |
/// | 7 | none | 1 |
fn notes(name: &str) -> Database {
    let db = support::open(&support::folder(name));
    support::run(&db, GRANTS);
    support::run(&db, NOTES);
    grant(&db, 1, "Agent", "report-reader", acl(1));
    grant(&db, 2, "Human", "ana", acl(2));
    for (id, context_number, acl_number) in [
        (1, Some(1), Some(1)),
        (2, Some(1), Some(2)),
        (3, Some(2), Some(1)),
        (4, Some(2), Some(2)),
        (5, Some(1), Some(3)),
        (6, Some(1), None),
        (7, None, Some(1)),
    ] {
        if let Err(error) = note(&db, id, context_number, acl_number) {
            panic!("note {id} was refused: {error:?}");
        }
    }
    db
}

const ALL_NOTES: &str = "SELECT id FROM notes ORDER BY id";

#[test]
fn a_handle_that_declares_nothing_reads_every_row_of_an_access_controlled_table() {
    let db = notes("visibility-declares-nothing");
    assert_eq!(ids(&db, ALL_NOTES), [1, 2, 3, 4, 5, 6, 7]);
    // A handle made with no Context, no label and no principal declared nothing either.
    let unnarrowed = db.scoped_with_constraints(None, None, None);
    assert_eq!(ids(&unnarrowed, ALL_NOTES), [1, 2, 3, 4, 5, 6, 7]);
    assert_eq!(
        texts(&unnarrowed, "SELECT payload FROM notes WHERE id = 5"),
        ["note-5"]
    );
    assert_eq!(support::int(&unnarrowed, "SELECT COUNT(*) FROM notes"), 7);
}

#[test]
fn a_principal_reads_the_rows_it_holds_a_grant_for_and_no_others() {
    let db = notes("visibility-grants");
    let reader = named(&db, agent("report-reader"));
    assert_eq!(ids(&reader, ALL_NOTES), [1, 3, 7]);
    assert_eq!(
        texts(&reader, "SELECT payload FROM notes ORDER BY id"),
        ["note-1", "note-3", "note-7"]
    );
    assert_eq!(support::int(&reader, "SELECT COUNT(*) FROM notes"), 3);
    assert_eq!(ids(&named(&db, human("ana")), ALL_NOTES), [2, 4]);

    // The kind is part of who a principal is, and a principal nobody granted anything reads an
    // empty table, without a refusal.
    for nobody in [agent("ana"), human("report-reader"), agent("nobody")] {
        let handle = named(&db, nobody.clone());
        assert_eq!(ids(&handle, ALL_NOTES), [0_i64; 0], "{nobody:?}");
        assert_eq!(
            support::int(&handle, "SELECT COUNT(*) FROM notes"),
            0,
            "{nobody:?}"
        );
    }

    // A grant is read when a statement runs: one given later shows its rows to the handle that
    // was made before it, and one taken away hides them again.
    grant(&db, 3, "Agent", "report-reader", acl(3));
    assert_eq!(ids(&reader, ALL_NOTES), [1, 3, 5, 7]);
    let first_grant = params! { "id" => Uuid::from_u128(0x0060_0001) };
    let taken = db
        .execute("DELETE FROM acl_grants WHERE id = $id", &first_grant)
        .expect("take the first grant away");
    assert_eq!(taken.rows_affected(), 1);
    assert_eq!(ids(&reader, ALL_NOTES), [5]);
    assert_eq!(ids(&db, ALL_NOTES), [1, 2, 3, 4, 5, 6, 7]);
}

#[test]
fn a_handle_with_a_context_and_a_principal_sees_only_granted_rows_of_its_context() {
    let db = notes("visibility-context-and-principal");
    let both = |numbers: &[u128], principal: Principal| {
        db.scoped_with_constraints(Some(context_set(numbers)), None, Some(principal))
    };
    let reader = both(&[1], agent("report-reader"));
    assert_eq!(ids(&reader, ALL_NOTES), [1]);
    assert_eq!(support::int(&reader, "SELECT COUNT(*) FROM notes"), 1);
    assert_eq!(ids(&both(&[2], agent("report-reader")), ALL_NOTES), [3]);
    assert_eq!(ids(&both(&[1, 2], human("ana")), ALL_NOTES), [2, 4]);
    assert_eq!(ids(&both(&[1, 2], agent("nobody")), ALL_NOTES), [0_i64; 0]);

    // Named by its key, a hidden row is refused by the rule that hid it, and the Context is
    // asked before the grant: note 3 is granted and outside the Context, note 2 is inside it
    // and not granted, note 4 is neither.
    let by_key = |id: i64| format!("SELECT payload FROM notes WHERE id = {id}");
    assert_eq!(texts(&reader, &by_key(1)), ["note-1"]);
    refused(&reader, &by_key(3), Rule::Context, "notes");
    refused(&reader, &by_key(2), Rule::Grant, "notes");
    refused(&reader, &by_key(4), Rule::Context, "notes");
    // Note 7 is under no Context: it is outside every Context a handle can be narrowed to.
    refused(&reader, &by_key(7), Rule::Context, "notes");
    assert!(texts(&reader, &by_key(99)).is_empty());

    // All three rules on one table: a row that fails two is refused by the first asked, the
    // Context, then the label, then the grant.
    support::run(
        &db,
        "CREATE TABLE guarded (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, label TEXT \
         SCOPE_LABEL_READ ('public') WRITE ('public'), acl_id UUID ACL REFERENCES \
         acl_grants(acl_id))",
    );
    let triple = db.scoped_with_constraints(
        Some(context_set(&[1])),
        Some(label_set(&["public"])),
        Some(agent("report-reader")),
    );
    for (id, context_number, label, acl_number, wanted) in [
        (1, 1, "public", 1, None),
        (2, 2, "internal", 1, Some(Rule::Context)),
        (3, 2, "public", 2, Some(Rule::Context)),
        (4, 1, "internal", 2, Some(Rule::Label)),
        (5, 1, "public", 2, Some(Rule::Grant)),
    ] {
        support::run(
            &db,
            &format!(
                "INSERT INTO guarded VALUES ({id}, '{}', '{label}', '{}')",
                context(context_number),
                acl(acl_number)
            ),
        );
        let by_key = format!("SELECT id FROM guarded WHERE id = {id}");
        match wanted {
            None => assert_eq!(ids(&triple, &by_key), [id], "row {id}"),
            Some(rule) => {
                refused(&triple, &by_key, rule, "guarded");
            }
        }
    }
}

#[test]
fn a_narrowed_handle_with_no_principal_that_holds_grants_is_refused_an_access_controlled_table() {
    let db = notes("visibility-principal-required");
    // The same declaration with no row in it: the refusal is of the table, not of its rows.
    support::run(
        &db,
        "CREATE TABLE empty_notes (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, \
         acl_id UUID ACL REFERENCES acl_grants(acl_id), payload TEXT)",
    );
    let handles = [
        ("a Context", in_contexts(&db, &[1])),
        ("a label", with_labels(&db, &["public"])),
        (
            "a Context and a label",
            db.scoped_with_constraints(Some(context_set(&[1])), Some(label_set(&["public"])), None),
        ),
        ("the system principal", named(&db, Principal::System)),
        (
            "a Context and the system principal",
            db.scoped_with_constraints(Some(context_set(&[1])), None, Some(Principal::System)),
        ),
    ];
    let new_note = "INSERT INTO notes (id, context_id, acl_id, payload) \
                    VALUES (50, $context, $acl, 'new')";
    let new_values = params! { "context" => context(1), "acl" => acl(1) };
    for (what, handle) in &handles {
        for sql in [
            ALL_NOTES,
            "SELECT payload FROM notes WHERE id = 1",
            "SELECT payload FROM notes WHERE id = 99",
            "SELECT id FROM notes WHERE payload = 'note-1'",
            "SELECT COUNT(*) FROM notes",
            "UPDATE notes SET payload = 'changed' WHERE id = 1",
            "UPDATE notes SET payload = 'changed'",
            "DELETE FROM notes WHERE id = 1",
            "DELETE FROM notes",
        ] {
            let refusal = refused(handle, sql, Rule::NoPrincipal, "notes");
            let said = refusal.to_string();
            assert!(
                !said.chars().any(|c| c.is_ascii_digit()),
                "narrowed by {what}, {sql}\nwas refused with a number in its words: {said}"
            );
        }
        refused_with(handle, new_note, &new_values, Rule::NoPrincipal, "notes");
        refused(
            handle,
            "SELECT id FROM empty_notes ORDER BY id",
            Rule::NoPrincipal,
            "empty_notes",
        );
        refused(
            handle,
            "SELECT COUNT(*) FROM empty_notes",
            Rule::NoPrincipal,
            "empty_notes",
        );
    }
    // Nothing was changed by any of them.
    assert_eq!(
        texts(&db, "SELECT payload FROM notes ORDER BY id"),
        [
            "note-1", "note-2", "note-3", "note-4", "note-5", "note-6", "note-7"
        ]
    );
}

#[test]
fn a_table_is_narrowed_only_by_what_it_declares() {
    let db = support::open(&support::folder("visibility-undeclared"));
    support::run(
        &db,
        "CREATE TABLE plain (id INTEGER PRIMARY KEY, payload TEXT)",
    );
    support::run(
        &db,
        "CREATE TABLE scoped (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, payload TEXT)",
    );
    support::run(
        &db,
        "INSERT INTO plain (id, payload) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    );
    for (id, number) in [(1_i64, 1), (2, 2), (3, 1), (4, 2)] {
        db.execute(
            "INSERT INTO scoped (id, context_id, payload) VALUES ($id, $context, 'p')",
            &params! { "id" => id, "context" => context(number) },
        )
        .expect("insert a scoped row");
    }
    let in_context_one = in_contexts(&db, &[1]);
    let others = [
        ("a label", with_labels(&db, &["public"])),
        ("a principal", named(&db, agent("report-reader"))),
        ("the system principal", named(&db, Principal::System)),
    ];

    // A table that declares no Context column, no label and no access column is read whole by
    // every handle, and written by it.
    assert_eq!(
        ids(&in_context_one, "SELECT id FROM plain ORDER BY id"),
        [1, 2, 3]
    );
    for (what, handle) in &others {
        assert_eq!(
            ids(handle, "SELECT id FROM plain ORDER BY id"),
            [1, 2, 3],
            "{what}"
        );
        assert_eq!(
            support::int(handle, "SELECT COUNT(*) FROM plain"),
            3,
            "{what}"
        );
    }
    assert_eq!(
        changed(
            &in_context_one,
            "INSERT INTO plain (id, payload) VALUES (4, 'd')"
        ),
        1
    );
    assert_eq!(
        changed(&others[1].1, "UPDATE plain SET payload = 'e' WHERE id = 4"),
        1
    );
    assert_eq!(texts(&db, "SELECT payload FROM plain WHERE id = 4"), ["e"]);

    // A table that declares a Context column and no access column is narrowed by a handle's
    // Contexts and by nothing else it declares: no principal is asked for.
    assert_eq!(
        ids(&in_context_one, "SELECT id FROM scoped ORDER BY id"),
        [1, 3]
    );
    for (what, handle) in &others {
        assert_eq!(
            ids(handle, "SELECT id FROM scoped ORDER BY id"),
            [1, 2, 3, 4],
            "{what}"
        );
    }
    let principal_in_context =
        db.scoped_with_constraints(Some(context_set(&[2])), None, Some(Principal::System));
    assert_eq!(
        ids(&principal_in_context, "SELECT id FROM scoped ORDER BY id"),
        [2, 4]
    );
}

// ---- a table under a Context column ----

/// A store with `items`: item `n` of 1 to 12 is `(n, <Context 1 for odd n, 2 for even>,
/// "k<n % 3>", n * 10)`, and item 13 is under no Context. One index, over `(kind, n)`.
fn items(name: &str) -> Database {
    let db = support::open(&support::folder(name));
    support::run(
        &db,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, \
         kind TEXT NOT NULL, n INTEGER NOT NULL)",
    );
    support::run(&db, "CREATE INDEX items_kind ON items (kind, n)");
    for id in 1..=13_i64 {
        let of = match id {
            13 => Value::Null,
            id if id % 2 == 1 => Value::Uuid(context(1)),
            _ => Value::Uuid(context(2)),
        };
        if let Err(error) = item(&db, id, of) {
            panic!("item {id} was refused: {error:?}");
        }
    }
    db
}

fn item(on: &impl Run, id: i64, context_id: Value) -> contextdb::Result<contextdb::QueryResult> {
    on.run(
        "INSERT INTO items (id, context_id, kind, n) VALUES ($id, $context, $kind, $n)",
        &params! {
            "id" => id, "context" => context_id, "kind" => format!("k{}", id % 3), "n" => id * 10,
        },
    )
}

const ALL_ITEMS: &str = "SELECT id FROM items ORDER BY id";

#[test]
fn a_handle_narrowed_to_contexts_lists_only_the_rows_of_its_contexts() {
    let db = items("visibility-context-lists");
    let one = in_contexts(&db, &[1]);
    let two = in_contexts(&db, &[2]);
    let lists: [(&Database, &str, &[i64]); 12] = [
        (&one, "ORDER BY id", &[1, 3, 5, 7, 9, 11]),
        (&two, "ORDER BY id", &[2, 4, 6, 8, 10, 12]),
        // A filter that is no key.
        (&one, "WHERE kind = 'k0' ORDER BY id", &[3, 9]),
        (&one, "WHERE n >= 100 ORDER BY id", &[11]),
        // A range of the key, and a list of keys, hidden ones among them.
        (&one, "WHERE id >= 2 AND id <= 9 ORDER BY id", &[3, 5, 7, 9]),
        (&one, "WHERE id IN (1, 2, 3, 4, 13) ORDER BY id", &[1, 3]),
        (&one, "WHERE id IN (2)", &[]),
        // The first rows in order are hidden: the limit is filled with rows the handle sees.
        (&two, "ORDER BY id LIMIT 1", &[2]),
        (&one, "ORDER BY id DESC LIMIT 2", &[11, 9]),
        // In the order an index gives: of kind k1 are items 1, 4, 7, 10 and 13.
        (&two, "WHERE kind = 'k1' ORDER BY n LIMIT 2", &[4, 10]),
        (&one, "WHERE kind = 'k1' ORDER BY n LIMIT 1", &[1]),
        (&one, "WHERE kind = 'k1' ORDER BY n DESC LIMIT 1", &[7]),
    ];
    for (handle, sql, wanted) in lists {
        assert_eq!(seen(handle, "items", sql), wanted, "{sql}");
    }

    // The two ways to narrow to Contexts make the same handle.
    let also_one = db.scoped_with_constraints(Some(context_set(&[1])), None, None);
    assert_eq!(ids(&also_one, ALL_ITEMS), [1, 3, 5, 7, 9, 11]);
    // Several Contexts are read together; item 13, under none, is in no handle's Contexts.
    let both = in_contexts(&db, &[1, 2]);
    assert_eq!(ids(&both, ALL_ITEMS), (1..=12).collect::<Vec<i64>>());
    // A handle given no Context at all is narrowed to none: it sees no row.
    let none = in_contexts(&db, &[]);
    assert_eq!(ids(&none, ALL_ITEMS), [0_i64; 0]);
    assert_eq!(support::int(&none, "SELECT COUNT(*) FROM items"), 0);
    // A Context no row is under.
    assert_eq!(ids(&in_contexts(&db, &[3]), ALL_ITEMS), [0_i64; 0]);
    assert_eq!(ids(&db, ALL_ITEMS), (1..=13).collect::<Vec<i64>>());
}

#[test]
fn a_new_or_changed_row_outside_the_handles_contexts_is_refused_and_nothing_is_written() {
    let db = items("visibility-context-writes");
    let one = in_contexts(&db, &[1]);
    let before = support::rows(&db, "SELECT id, context_id, kind, n FROM items ORDER BY id");
    let unchanged = |what: &str| {
        let now = support::rows(&db, "SELECT id, context_id, kind, n FROM items ORDER BY id");
        support::assert_same_rows(&now, &before, what);
    };

    // A new row under another Context, or under none.
    for (id, of) in [(20_i64, Value::Uuid(context(2))), (21, Value::Null)] {
        match item(&one, id, of.clone()) {
            Err(Error::ContextScopeViolation { table }) => assert_eq!(table, "items"),
            other => panic!("a new row under {of:?} gave {other:?}"),
        }
        unchanged("after a refused insert");
    }
    // One statement with a row inside and a row outside writes neither.
    refused_with(
        &one,
        "INSERT INTO items (id, context_id, kind, n) VALUES (22, $mine, 'k', 1), (23, $other, 'k', 2)",
        &params! { "mine" => context(1), "other" => context(2) },
        Rule::Context,
        "items",
    );
    unchanged("after a refused insert of two rows");
    // A row the handle sees may not be moved out of its Contexts.
    let to_two = params! { "other" => context(2) };
    for sql in [
        "UPDATE items SET context_id = $other WHERE id = 1",
        "UPDATE items SET context_id = $other WHERE kind = 'k0'",
        "UPDATE items SET context_id = NULL WHERE id = 3",
    ] {
        let values = if sql.contains("$other") {
            to_two.clone()
        } else {
            params! {}
        };
        refused_with(&one, sql, &values, Rule::Context, "items");
        unchanged(sql);
    }

    // Inside its Contexts it writes, and the row is there for every handle that may see it.
    item(&one, 24, Value::Uuid(context(1))).expect("a new row inside the handle's Context");
    assert_eq!(ids(&one, "SELECT id FROM items WHERE id = 24"), [24]);
    assert_eq!(ids(&db, "SELECT id FROM items WHERE id = 24"), [24]);
    assert_eq!(
        ids(
            &in_contexts(&db, &[2]),
            "SELECT id FROM items WHERE id IN (24)"
        ),
        [0_i64; 0]
    );
    let both = in_contexts(&db, &[1, 2]);
    assert_eq!(
        both.execute(
            "UPDATE items SET context_id = $other WHERE id = 24",
            &to_two
        )
        .expect("move a row between two Contexts of the handle")
        .rows_affected(),
        1
    );
    assert_eq!(
        ids(&one, "SELECT id FROM items WHERE id IN (24)"),
        [0_i64; 0]
    );

    // A transaction of the handle reads and writes within the same reach, and a refused
    // statement leaves it usable.
    let tx = one.transaction().expect("begin");
    item(&tx, 30, Value::Uuid(context(1))).expect("a new row inside the handle's Context");
    match item(&tx, 31, Value::Uuid(context(2))) {
        Err(Error::ContextScopeViolation { table }) => assert_eq!(table, "items"),
        other => panic!("a new row outside the Context, in a transaction, gave {other:?}"),
    }
    assert_eq!(ids(&tx, ALL_ITEMS), [1, 3, 5, 7, 9, 11, 30]);
    assert_eq!(changed(&tx, "UPDATE items SET n = 0 WHERE id = 2"), 0);
    assert_eq!(changed(&tx, "DELETE FROM items WHERE kind = 'k2'"), 2);
    tx.commit().expect("commit");
    assert_eq!(
        ids(&db, ALL_ITEMS),
        [1, 2, 3, 4, 6, 7, 8, 9, 10, 12, 13, 24, 30]
    );
    assert_eq!(ids(&db, "SELECT n FROM items WHERE id = 2"), [20]);
}

// ---- one table under each rule that hides rows ----

/// `ledger`'s rows: id, slug, a, b, whether the handle of [`Gate::handle`] may see the row,
/// payload. `slug` is UNIQUE and so is `(a, b)`.
const LEDGER: [(i64, &str, i64, i64, bool, &str); 4] = [
    (1, "open-one", 1, 1, true, "seen-one"),
    (2, "shut-two", 1, 2, false, "secret-two"),
    (3, "open-three", 2, 1, true, "seen-three"),
    (4, "shut-four", 2, 2, false, "secret-four"),
];

const LEDGER_ROWS: &str = "SELECT id, payload FROM ledger ORDER BY id";

/// One rule that hides rows, with the table `ledger` declared under it. Rows 1 and 3 are
/// within the reach of the rule's handle; rows 2 and 4 are hidden from it by this rule and by
/// no other.
#[derive(Clone, Copy, Debug)]
enum Gate {
    Context,
    Label,
    Grant,
}

impl Gate {
    const ALL: [Gate; 3] = [Gate::Context, Gate::Label, Gate::Grant];

    /// The column `reach` as this rule declares it.
    fn column(self) -> &'static str {
        match self {
            Gate::Context => "reach UUID CONTEXT_ID",
            Gate::Label => {
                "reach TEXT SCOPE_LABEL_READ ('public', 'internal') WRITE ('public', 'internal')"
            }
            Gate::Grant => "reach UUID ACL REFERENCES acl_grants(acl_id)",
        }
    }

    /// What `reach` holds in a row the handle may see, and in one it may not.
    fn reach(self, seen: bool) -> Value {
        match (self, seen) {
            (Gate::Context, true) => Value::Uuid(context(1)),
            (Gate::Context, false) => Value::Uuid(context(2)),
            (Gate::Label, true) => support::text("public"),
            (Gate::Label, false) => support::text("internal"),
            (Gate::Grant, true) => Value::Uuid(acl(1)),
            (Gate::Grant, false) => Value::Uuid(acl(2)),
        }
    }

    fn handle(self, db: &Database) -> Database {
        match self {
            Gate::Context => in_contexts(db, &[1]),
            Gate::Label => with_labels(db, &["public"]),
            Gate::Grant => named(db, agent("report-reader")),
        }
    }

    /// The refusal of this rule.
    fn rule(self) -> Rule {
        match self {
            Gate::Context => Rule::Context,
            Gate::Label => Rule::Label,
            Gate::Grant => Rule::Grant,
        }
    }

    /// A store with `ledger` under this rule and its four rows.
    fn store(self, name: &str) -> Database {
        let db = support::open(&support::folder(&format!("{name}-{self:?}")));
        if let Gate::Grant = self {
            support::run(&db, GRANTS);
            grant(&db, 1, "Agent", "report-reader", acl(1));
            grant(&db, 2, "Human", "someone-else", acl(2));
        }
        support::run(
            &db,
            &format!(
                "CREATE TABLE ledger (id INTEGER PRIMARY KEY, slug TEXT UNIQUE, a INTEGER, \
                 b INTEGER, {}, payload TEXT, UNIQUE (a, b))",
                self.column()
            ),
        );
        for (id, slug, a, b, seen, payload) in LEDGER {
            db.execute(
                "INSERT INTO ledger (id, slug, a, b, reach, payload) \
                 VALUES ($id, $slug, $a, $b, $reach, $payload)",
                &params! {
                    "id" => id, "slug" => slug, "a" => a, "b" => b, "reach" => self.reach(seen),
                    "payload" => payload,
                },
            )
            .unwrap_or_else(|error| panic!("{self:?}: ledger row {id} was refused: {error:?}"));
        }
        db
    }
}

/// The `(id, payload)` rows of `ledger` with these ids, as they were first written.
fn ledger_rows(of: &[i64]) -> Vec<Vec<Value>> {
    LEDGER
        .iter()
        .filter(|row| of.contains(&row.0))
        .map(|row| vec![Value::Int64(row.0), support::text(row.5)])
        .collect()
}

/// A hidden row named whole (by its key, by its one UNIQUE column, by every column of the
/// composite UNIQUE rule) is refused by the rule that hid it; every other read leaves it out
/// and says nothing.
fn a_hidden_row_named_whole_is_refused_and_a_list_read_leaves_it_out(gate: Gate) {
    let db = gate.store("visibility-anchor");
    let handle = gate.handle(&db);
    let rule = gate.rule();

    for sql in [
        "SELECT payload FROM ledger WHERE id = 2",
        "SELECT payload FROM ledger WHERE slug = 'shut-two'",
        "SELECT payload FROM ledger WHERE a = 1 AND b = 2",
        "SELECT payload FROM ledger WHERE b = 2 AND a = 2",
        "SELECT id, slug, a, b, payload FROM ledger WHERE id = 4",
        "SELECT id FROM ledger WHERE slug = 'shut-four'",
    ] {
        refused(&handle, sql, rule, "ledger");
    }
    // The same when the key is a bound value.
    let bound: [(&str, HashMap<String, Value>); 3] = [
        (
            "SELECT payload FROM ledger WHERE id = $id",
            params! { "id" => 2_i64 },
        ),
        (
            "SELECT payload FROM ledger WHERE slug = $slug",
            params! { "slug" => "shut-four" },
        ),
        (
            "SELECT payload FROM ledger WHERE a = $a AND b = $b",
            params! { "a" => 2_i64, "b" => 2_i64 },
        ),
    ];
    for (sql, values) in &bound {
        refused_with(&handle, sql, values, rule, "ledger");
    }

    // The same shapes over a row within reach answer with it, and over no row with nothing.
    for (sql, wanted) in [
        ("SELECT payload FROM ledger WHERE id = 1", "seen-one"),
        (
            "SELECT payload FROM ledger WHERE slug = 'open-three'",
            "seen-three",
        ),
        (
            "SELECT payload FROM ledger WHERE a = 2 AND b = 1",
            "seen-three",
        ),
    ] {
        assert_eq!(texts(&handle, sql), [wanted], "{gate:?}: {sql}");
    }
    for sql in [
        "SELECT payload FROM ledger WHERE id = 99",
        "SELECT payload FROM ledger WHERE slug = 'no-such'",
        "SELECT payload FROM ledger WHERE a = 9 AND b = 9",
    ] {
        assert!(texts(&handle, sql).is_empty(), "{gate:?}: {sql}");
    }

    // Every other shape is a list, a key in a list and a key in a range among them.
    let lists: [(&str, &[i64]); 12] = [
        ("ORDER BY id", &[1, 3]),
        ("WHERE a = 1 ORDER BY id", &[1]),
        ("WHERE b = 2 ORDER BY id", &[]),
        ("WHERE id IN (2)", &[]),
        ("WHERE id IN (1, 2, 4) ORDER BY id", &[1]),
        ("WHERE id >= 2 AND id <= 2", &[]),
        ("WHERE id > 1 ORDER BY id", &[3]),
        ("WHERE payload = 'secret-two'", &[]),
        ("WHERE slug IN ('shut-two', 'open-one') ORDER BY id", &[1]),
        ("ORDER BY id DESC LIMIT 1", &[3]),
        ("ORDER BY slug DESC LIMIT 1", &[3]),
        ("ORDER BY slug LIMIT 4", &[1, 3]),
    ];
    for (sql, wanted) in lists {
        assert_eq!(seen(&handle, "ledger", sql), wanted, "{gate:?}: {sql}");
    }

    // The handle that declares nothing reads all four, however it names them.
    assert_eq!(ids(&db, "SELECT id FROM ledger ORDER BY id"), [1, 2, 3, 4]);
    assert_eq!(
        texts(&db, "SELECT payload FROM ledger WHERE id = 2"),
        ["secret-two"]
    );
    assert_eq!(
        texts(&db, "SELECT payload FROM ledger WHERE a = 2 AND b = 2"),
        ["secret-four"]
    );
}

#[test]
fn a_row_of_another_context_named_by_key_is_a_context_refusal_and_a_list_read_leaves_it_out() {
    a_hidden_row_named_whole_is_refused_and_a_list_read_leaves_it_out(Gate::Context);
}

#[test]
fn a_row_under_another_label_named_by_key_is_a_label_refusal_and_a_list_read_leaves_it_out() {
    a_hidden_row_named_whole_is_refused_and_a_list_read_leaves_it_out(Gate::Label);
}

#[test]
fn a_row_with_no_grant_named_by_key_is_an_access_refusal_and_a_list_read_leaves_it_out() {
    a_hidden_row_named_whole_is_refused_and_a_list_read_leaves_it_out(Gate::Grant);
}

#[test]
fn an_update_or_a_delete_changes_no_row_the_handle_may_not_see() {
    for gate in Gate::ALL {
        let db = gate.store("visibility-hidden-writes");
        let handle = gate.handle(&db);
        let ledger_is = |wanted: &[Vec<Value>], what: &str| {
            let now = support::rows(&db, LEDGER_ROWS);
            support::assert_same_rows(&now, wanted, &format!("{gate:?}: the ledger {what}"));
        };

        // By its key, by a whole UNIQUE rule, by a filter that only hidden rows pass: no row
        // is changed, and the statement is not refused.
        for sql in [
            "UPDATE ledger SET payload = 'changed' WHERE id = 2",
            "UPDATE ledger SET payload = 'changed' WHERE slug = 'shut-four'",
            "UPDATE ledger SET payload = 'changed' WHERE a = 1 AND b = 2",
            "UPDATE ledger SET payload = 'changed' WHERE b = 2",
            "UPDATE ledger SET payload = 'changed' WHERE payload = 'secret-two'",
            "UPDATE ledger SET payload = 'changed' WHERE id IN (2, 4)",
            "DELETE FROM ledger WHERE id = 2",
            "DELETE FROM ledger WHERE slug = 'shut-four'",
            "DELETE FROM ledger WHERE a = 2 AND b = 2",
            "DELETE FROM ledger WHERE b = 2",
            "DELETE FROM ledger WHERE payload = 'secret-four'",
            "DELETE FROM ledger WHERE id >= 2 AND id <= 2",
        ] {
            assert_eq!(changed(&handle, sql), 0, "{gate:?}: {sql}");
            ledger_is(&ledger_rows(&[1, 2, 3, 4]), &format!("after {sql}"));
        }

        // A filter that hidden rows and seen rows pass changes the seen ones only.
        assert_eq!(
            changed(&handle, "UPDATE ledger SET payload = 'mine' WHERE a = 1"),
            1,
            "{gate:?}"
        );
        assert_eq!(
            changed(&handle, "UPDATE ledger SET payload = 'mine'"),
            2,
            "{gate:?}"
        );
        let mine = |id: i64| vec![Value::Int64(id), support::text("mine")];
        let hidden = ledger_rows(&[2, 4]);
        ledger_is(
            &[mine(1), hidden[0].clone(), mine(3), hidden[1].clone()],
            "after the handle updated every row it sees",
        );
        assert_eq!(
            changed(&handle, "DELETE FROM ledger WHERE a = 2"),
            1,
            "{gate:?}"
        );
        assert_eq!(changed(&handle, "DELETE FROM ledger"), 1, "{gate:?}");
        ledger_is(&hidden, "after the handle deleted every row it sees");
        assert_eq!(
            support::int(&db, "SELECT COUNT(*) FROM ledger"),
            2,
            "{gate:?}"
        );
    }
}

#[test]
fn a_row_guard_answers_false_for_a_row_the_handle_may_not_see() {
    for gate in Gate::ALL {
        let db = gate.store("visibility-guard");
        let handle = gate.handle(&db);
        let holds = |payload: &str| vec![("payload".to_owned(), support::text(payload))];

        // Row 2 is there and holds this value: the handle that declares nothing is told so.
        let tx = db.transaction().expect("begin");
        let asked = tx.guard_row_conditions("ledger", "id", &Value::Int64(2), &holds("secret-two"));
        assert!(
            asked.expect("a guard through the handle that declares nothing"),
            "{gate:?}: row 2 does not hold what it was written with"
        );
        tx.rollback();

        let tx = handle.transaction().expect("begin");
        let guard = |key_column: &str, key: Value, predicates: &[(String, Value)]| match tx
            .guard_row_conditions("ledger", key_column, &key, predicates)
        {
            Ok(answer) => answer,
            Err(error) => {
                panic!("{gate:?}: a guard on {key_column} = {key:?} was refused: {error:?}")
            }
        };
        // A row within reach: the guard answers by what the row holds.
        assert!(guard("id", Value::Int64(1), &holds("seen-one")), "{gate:?}");
        assert!(guard("id", Value::Int64(1), &[]), "{gate:?}");
        assert!(!guard("id", Value::Int64(1), &holds("other")), "{gate:?}");
        // A hidden row is to the handle a row that is not there: false whatever is asked of
        // it, by its key or by another column that names it.
        assert!(
            !guard("id", Value::Int64(2), &holds("secret-two")),
            "{gate:?}"
        );
        assert!(!guard("id", Value::Int64(2), &[]), "{gate:?}");
        assert!(
            !guard("slug", support::text("shut-four"), &holds("secret-four")),
            "{gate:?}"
        );
        assert!(!guard("slug", support::text("shut-four"), &[]), "{gate:?}");
        assert!(!guard("id", Value::Int64(99), &[]), "{gate:?}");
        tx.rollback();
        let now = support::rows(&db, LEDGER_ROWS);
        support::assert_same_rows(
            &now,
            &ledger_rows(&[1, 2, 3, 4]),
            "the ledger after the guards",
        );
    }
}

#[test]
fn no_refusal_holds_a_value_or_a_number_of_a_row_the_handle_may_not_see() {
    for gate in Gate::ALL {
        let db = gate.store("visibility-refusal-words");
        let handle = gate.handle(&db);
        let rule = gate.rule();
        let mut refusals = Vec::new();
        for sql in [
            "SELECT payload FROM ledger WHERE id = 2",
            "SELECT payload FROM ledger WHERE slug = 'shut-two'",
            "SELECT payload FROM ledger WHERE a = 2 AND b = 2",
        ] {
            refusals.push((sql, refused(&handle, sql, rule, "ledger")));
        }
        let by_key = "SELECT payload FROM ledger WHERE id = $id";
        let key = params! { "id" => 4_i64 };
        refusals.push((by_key, refused_with(&handle, by_key, &key, rule, "ledger")));
        // A write outside the handle's reach: a new row, and a seen row moved out.
        let outside = params! { "reach" => gate.reach(false) };
        for sql in [
            "INSERT INTO ledger (id, slug, a, b, reach, payload) \
             VALUES (5, 'shut-five', 3, 3, $reach, 'secret-five')",
            "UPDATE ledger SET reach = $reach WHERE id = 1",
        ] {
            refusals.push((sql, refused_with(&handle, sql, &outside, rule, "ledger")));
        }

        // The table's name has no digit in it, so a digit in a refusal is a key, a count, a
        // Context or an access id; and no word of a hidden row's values is in one.
        for (sql, refusal) in &refusals {
            for said in [refusal.to_string(), format!("{refusal:?}")] {
                assert!(
                    !said.chars().any(|c| c.is_ascii_digit()),
                    "{gate:?}: {sql}\nwas refused with a number in its words: {said}"
                );
                for hidden in ["secret", "shut", "internal", "someone-else"] {
                    assert!(
                        !said.contains(hidden),
                        "{gate:?}: {sql}\nwas refused with `{hidden}` in its words: {said}"
                    );
                }
            }
        }
        let now = support::rows(&db, LEDGER_ROWS);
        support::assert_same_rows(
            &now,
            &ledger_rows(&[1, 2, 3, 4]),
            "the ledger after refusals",
        );
    }
}

// ---- scope labels ----

#[test]
fn a_simple_label_is_checked_when_a_row_is_written_and_hides_no_row() {
    let db = support::open(&support::folder("visibility-simple-label"));
    support::run(
        &db,
        "CREATE TABLE memos (id INTEGER PRIMARY KEY, label TEXT SCOPE_LABEL ('public', \
         'internal'), payload TEXT)",
    );
    let new = |id: i64, label: &str| {
        format!("INSERT INTO memos (id, label, payload) VALUES ({id}, {label}, 'new')")
    };
    support::run(&db, &new(1, "'public'"));
    support::run(&db, &new(2, "'internal'"));
    let memos = "SELECT id, label, payload FROM memos ORDER BY id";
    let public = with_labels(&db, &["public"]);
    let other = with_labels(&db, &["other"]);

    // The simple form has no read side: a handle with labels reads every row, by key too.
    assert_eq!(seen(&public, "memos", "ORDER BY id"), [1, 2]);
    assert_eq!(seen(&public, "memos", "WHERE id = 2"), [2]);

    // It writes rows under a label the table lists and the handle holds.
    assert_eq!(changed(&public, &new(3, "'public'")), 1);
    let renamed = "UPDATE memos SET payload = 'uno' WHERE id = 1";
    assert_eq!(changed(&public, renamed), 1);
    let before = support::rows(&db, memos);
    // Refused: a label the table lists and the handle does not hold, a label the handle
    // holds and the table does not list, and no label.
    for (handle, id, label) in [
        (&public, 4, "'internal'"),
        (&other, 5, "'other'"),
        (&public, 6, "NULL"),
    ] {
        refused(handle, &new(id, label), Rule::Label, "memos");
        support::assert_same_rows(&support::rows(&db, memos), &before, label);
    }
    // Refused: a row it reads and may not write, changed, relabelled or removed.
    for sql in [
        "UPDATE memos SET payload = 'dos' WHERE id = 2",
        // Setting the value the row holds is the same write to the same row.
        "UPDATE memos SET payload = 'new' WHERE id = 2",
        "INSERT INTO memos (id, label, payload) VALUES (2, 'public', 'x') \
         ON CONFLICT (id) DO UPDATE SET payload = 'dos'",
        "INSERT INTO memos (id, label, payload) VALUES (2, 'public', 'x') \
         ON CONFLICT (id) DO UPDATE SET payload = 'new'",
        "UPDATE memos SET label = 'internal' WHERE id = 1",
        "DELETE FROM memos WHERE id = 2",
        "DELETE FROM memos",
    ] {
        refused(&public, sql, Rule::Label, "memos");
        support::assert_same_rows(&support::rows(&db, memos), &before, sql);
    }
    assert_eq!(seen(&db, "memos", "ORDER BY id"), [1, 2, 3]);
}

#[test]
fn a_label_with_a_read_side_hides_rows_and_is_checked_when_a_row_is_written() {
    let db = support::open(&support::folder("visibility-split-label"));
    // Read under 'public' and 'internal'; written under 'internal' and 'draft'.
    support::run(
        &db,
        "CREATE TABLE briefs (id INTEGER PRIMARY KEY, label TEXT SCOPE_LABEL_READ ('public', \
         'internal') WRITE ('internal', 'draft'), payload TEXT)",
    );
    let new = |id: i64, label: &str| {
        format!("INSERT INTO briefs (id, label, payload) VALUES ({id}, {label}, 'new')")
    };
    for (id, label) in [
        (1, "'public'"),
        (2, "'internal'"),
        (3, "'draft'"),
        (4, "NULL"),
    ] {
        support::run(&db, &new(id, label));
    }
    let all = "ORDER BY id";
    let briefs = "SELECT id, label, payload FROM briefs ORDER BY id";

    // A row is read under a label the table lists for reading and the handle holds: 'draft'
    // is no label the table is read under, whoever holds it.
    let reads: [(&[&str], &[i64]); 7] = [
        (&["public"], &[1]),
        (&["internal"], &[2]),
        (&["public", "internal"], &[1, 2]),
        (&["draft"], &[]),
        (&["public", "internal", "draft"], &[1, 2]),
        (&["other"], &[]),
        (&[], &[]),
    ];
    for (names, wanted) in reads {
        let handle = with_labels(&db, names);
        assert_eq!(seen(&handle, "briefs", all), wanted, "labels {names:?}");
    }
    // A handle that declares no label is not narrowed by a table's labels.
    assert_eq!(seen(&in_contexts(&db, &[1]), "briefs", all), [1, 2, 3, 4]);
    assert_eq!(seen(&db, "briefs", all), [1, 2, 3, 4]);

    // Named by key, a row under a label the handle may not read is the label's refusal: one
    // it does not hold, one the table is not read under, and a row with no label.
    let every = with_labels(&db, &["public", "internal", "draft"]);
    let public = with_labels(&db, &["public"]);
    let by_key = |id: i64| format!("SELECT payload FROM briefs WHERE id = {id}");
    refused(&public, &by_key(2), Rule::Label, "briefs");
    refused(&every, &by_key(3), Rule::Label, "briefs");
    refused(&every, &by_key(4), Rule::Label, "briefs");
    assert_eq!(seen(&every, "briefs", "WHERE id = 2"), [2]);

    // Written under a label the table lists for writing and the handle holds, though a row
    // under 'draft' cannot be read back.
    assert_eq!(changed(&every, &new(5, "'internal'")), 1);
    assert_eq!(changed(&every, &new(6, "'draft'")), 1);
    let renamed = "UPDATE briefs SET payload = 'dos' WHERE id = 2";
    assert_eq!(changed(&every, renamed), 1);
    assert_eq!(seen(&every, "briefs", all), [1, 2, 5]);
    // A hidden row is not matched, whatever label it is under.
    let hidden = "UPDATE briefs SET payload = 'tres' WHERE id = 3";
    assert_eq!(changed(&every, hidden), 0);
    assert_eq!(changed(&every, "DELETE FROM briefs WHERE id = 6"), 0);

    let before = support::rows(&db, briefs);
    // Refused: 'public' is read and not written; 'internal' is written and `public` does not
    // hold it; and no label.
    for (handle, id, label) in [
        (&every, 7, "'public'"),
        (&public, 8, "'internal'"),
        (&public, 9, "NULL"),
    ] {
        refused(handle, &new(id, label), Rule::Label, "briefs");
        support::assert_same_rows(&support::rows(&db, briefs), &before, label);
    }
    for sql in [
        "UPDATE briefs SET payload = 'uno' WHERE id = 1",
        "DELETE FROM briefs WHERE id = 1",
        "UPDATE briefs SET label = 'public' WHERE id = 2",
    ] {
        refused(&every, sql, Rule::Label, "briefs");
        support::assert_same_rows(&support::rows(&db, briefs), &before, sql);
    }
    assert_eq!(seen(&db, "briefs", all), [1, 2, 3, 4, 5, 6]);
}

// ---- access-controlled rows, written ----

#[test]
fn a_principal_writes_only_rows_under_an_id_it_holds_a_grant_for() {
    let db = notes("visibility-grant-writes");
    let reader = named(&db, agent("report-reader"));
    let payloads = "SELECT id, acl_id, payload FROM notes ORDER BY id";

    note(&reader, 60, Some(1), Some(1)).expect("a new row under an id the principal holds");
    assert_eq!(ids(&reader, ALL_NOTES), [1, 3, 7, 60]);
    assert_eq!(
        changed(&reader, "UPDATE notes SET payload = 'mine' WHERE id = 60"),
        1
    );

    let before = support::rows(&db, payloads);
    // A new row under an id granted to somebody else, to nobody, and under no id.
    for (id, acl_number) in [(61_i64, Some(2)), (62, Some(3)), (63, None)] {
        match note(&reader, id, Some(1), acl_number) {
            Err(Error::AclDenied { table }) => assert_eq!(table, "notes"),
            other => panic!("a new row under access id {acl_number:?} gave {other:?}"),
        }
        support::assert_same_rows(
            &support::rows(&db, payloads),
            &before,
            "after a refused insert",
        );
    }
    // A row it sees may not be moved under an id it does not hold.
    refused_with(
        &reader,
        "UPDATE notes SET acl_id = $acl WHERE id = 1",
        &params! { "acl" => acl(2) },
        Rule::Grant,
        "notes",
    );
    support::assert_same_rows(
        &support::rows(&db, payloads),
        &before,
        "after a refused update",
    );

    // Rows under other ids are not matched by its writes.
    assert_eq!(
        changed(&reader, "UPDATE notes SET payload = 'mine' WHERE id = 2"),
        0
    );
    assert_eq!(
        changed(&reader, "DELETE FROM notes WHERE id IN (2, 4, 5, 6)"),
        0
    );
    assert_eq!(changed(&reader, "DELETE FROM notes WHERE id >= 1"), 4);
    assert_eq!(ids(&db, ALL_NOTES), [2, 4, 5, 6]);
    assert_eq!(
        texts(&db, "SELECT payload FROM notes WHERE id = 2"),
        ["note-2"]
    );
}

// ---- what a narrowed handle is told by a count ----

#[test]
fn a_narrowed_handle_counts_its_own_rows_where_the_whole_table_is_counted_with_no_row_read() {
    let db = support::open(&support::folder("visibility-counts"));
    support::run(
        &db,
        "CREATE TABLE events (id INTEGER PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
         kind TEXT NOT NULL, UNIQUE (context_id, id)) MAINTAIN COUNT (context_id)",
    );
    support::run(&db, "CREATE INDEX events_kind ON events (kind)");
    // Events 1 to 10 are under Context 1 and 11 to 15 under Context 2; event n is of kind
    // k<n % 2>. Of kind k1 are five events of Context 1 and three of Context 2.
    let tx = db.transaction().expect("begin");
    for id in 1..=15_i64 {
        tx.execute(
            "INSERT INTO events (id, context_id, kind) VALUES ($id, $context, $kind)",
            &params! {
                "id" => id, "context" => context(if id <= 10 { 1 } else { 2 }),
                "kind" => format!("k{}", id % 2),
            },
        )
        .expect("insert an event");
    }
    tx.commit().expect("commit");

    // A count through a handle is the number of its own rows, by the table's count, by the
    // kept count of a Context and by an index range: never the table's 15, the other Context's
    // 5 or the index range's 8. (That the whole table is counted with no row read is proved in
    // `counted_work.rs` and `kept_counts.rs`; that a narrowed count reads its rows, in `kept_counts_narrowed.rs`.)
    let count = |on: &Database, filter: &str, of: Option<u128>| {
        let sql = format!("SELECT COUNT(*) FROM events{filter}");
        let values = of.map_or_else(|| params! {}, |n| params! { "context" => context(n) });
        match on.execute(&sql, &values).expect("a count").rows() {
            [row] => match row.as_slice() {
                [Value::Int64(count)] => *count,
                other => panic!("{sql}\nreturned {other:?}, not one count"),
            },
            other => panic!("{sql}\nreturned {} rows, not one", other.len()),
        }
    };
    let in_context = " WHERE context_id = $context";
    let one = in_contexts(&db, &[1]);
    assert_eq!(count(&one, "", None), 10);
    assert_eq!(count(&one, in_context, Some(1)), 10);
    assert_eq!(count(&one, in_context, Some(2)), 0);
    assert_eq!(count(&one, " WHERE kind = 'k1'", None), 5);
    assert_eq!(count(&one, " WHERE id > 8", None), 2);
    assert_eq!(
        count(&in_contexts(&db, &[2]), " WHERE kind = 'k1'", None),
        3
    );
    assert_eq!(count(&in_contexts(&db, &[3]), "", None), 0);

    // A count that names a hidden row by its key says the row is not the handle's, by the
    // refusal or by 0, and never counts it.
    let hidden = "SELECT COUNT(*) FROM events WHERE id = 11";
    match one.execute(hidden, &params! {}) {
        Ok(result) => assert_eq!(result.rows(), [[Value::Int64(0)]], "{hidden}"),
        Err(Error::ContextScopeViolation { table }) => assert_eq!(table, "events"),
        Err(other) => panic!("{hidden}\nwas refused with {other:?}"),
    }

    // The counts follow the rows the handle writes, for it and for the whole table.
    assert_eq!(changed(&one, "DELETE FROM events WHERE kind = 'k1'"), 5);
    assert_eq!(count(&one, "", None), 5);
    assert_eq!(count(&db, "", None), 10);
    assert_eq!(count(&db, in_context, Some(2)), 5);
    assert_eq!(count(&db, " WHERE kind = 'k1'", None), 3);
}

// ---- what a narrowed handle is told by a search ----

#[test]
fn a_search_returns_the_nearest_rows_the_handle_may_see_and_no_hidden_row() {
    const DIMENSION: usize = 8;
    let db = support::open(&support::folder("visibility-search"));
    support::run(&db, GRANTS);
    grant(&db, 1, "Agent", "report-reader", acl(1));
    grant(&db, 2, "Human", "someone-else", acl(2));
    support::run(
        &db,
        &format!(
            "CREATE TABLE passages (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, \
             acl_id UUID ACL REFERENCES acl_grants(acl_id), v VECTOR({DIMENSION}))"
        ),
    );
    let query = support::vector(1_000, DIMENSION);
    let passage = |id: i64, context_number: u128, acl_number: u128, v: Vec<f32>| {
        db.execute(
            "INSERT INTO passages (id, context_id, acl_id, v) VALUES ($id, $context, $acl, $v)",
            &params! {
                "id" => id, "context" => context(context_number), "acl" => acl(acl_number),
                "v" => v,
            },
        )
        .unwrap_or_else(|error| panic!("passage {id} was refused: {error:?}"));
    };
    // Passage n of 1 to 40: Context 1 for odd n and 2 for even; access id 2 when n is a
    // multiple of 3 and 1 otherwise. Three more hold the very vector searched with, so that
    // each would be the first answer: 101 is hidden by its Context, 102 by its access id,
    // 103 by both.
    for n in 1..=40_i64 {
        let context_number = if n % 2 == 1 { 1 } else { 2 };
        let acl_number = if n % 3 == 0 { 2 } else { 1 };
        passage(n, context_number, acl_number, support::vector(n, DIMENSION));
    }
    passage(101, 2, 1, query.clone());
    passage(102, 1, 2, query.clone());
    passage(103, 2, 2, query.clone());

    let reader = named(&db, agent("report-reader"));
    let reader_in_one =
        db.scoped_with_constraints(Some(context_set(&[1])), None, Some(agent("report-reader")));
    let in_context_one = |row: &[Value]| row[1] == Value::Uuid(context(1));
    let granted = |row: &[Value]| row[2] == Value::Uuid(acl(1));

    // Twice: with every vector waiting on its list, and with every vector in a block.
    for sealed in [false, true] {
        if sealed {
            assert_eq!(support::seal_all(&db), 43, "vectors sealed");
        }
        let stored = support::stored(&db, "SELECT id, context_id, acl_id, v FROM passages");
        for k in [1, 5, 43] {
            for filter in [None, Some("id > 10")] {
                let sql = support::nearest_sql("passages", "v", filter, k);
                let passes = |row: &[Value]| {
                    filter.is_none() || matches!(row[0], Value::Int64(id) if id > 10)
                };
                let what = |who: &str| format!("{who}, sealed: {sealed}");

                // The handle that declares nothing finds the planted rows first.
                let wanted = stored.nearest(passes, &query, k);
                support::assert_nearest(&db, &sql, &query, &wanted, &what("declaring nothing"));
                assert!(
                    matches!(wanted[0].key, Value::Int64(101)),
                    "the planted rows are not the nearest: {wanted:?}"
                );

                // A narrowed handle is answered with the nearest rows it may see, each at its
                // true distance; a hidden row takes no place among them.
                let wanted = stored.nearest(|row| passes(row) && granted(row), &query, k);
                support::assert_nearest(&reader, &sql, &query, &wanted, &what("the principal"));
                let wanted = stored.nearest(
                    |row| passes(row) && granted(row) && in_context_one(row),
                    &query,
                    k,
                );
                let what = what("the principal in Context 1");
                support::assert_nearest(&reader_in_one, &sql, &query, &wanted, &what);
            }
        }
    }

    // With no principal that holds grants the search is refused like any read of the table.
    let sql = support::nearest_sql("passages", "v", None, 5);
    match support::search(&in_contexts(&db, &[1]), &sql, &query) {
        Err(Error::PrincipalRequired { table }) => assert_eq!(table, "passages"),
        other => panic!("{sql}\nthrough a handle with no principal gave {other:?}"),
    }
}

// ---- a handle made from a narrowed handle ----

#[test]
fn a_handle_made_from_a_narrowed_handle_sees_no_more_than_it() {
    let db = items("visibility-narrowed-again");
    let one = in_contexts(&db, &[1]);
    // Asking for more Contexts, or for none, gives no more than the handle already has.
    let widened = one.scoped_with_contexts(context_set(&[1, 2]));
    assert_eq!(ids(&widened, ALL_ITEMS), [1, 3, 5, 7, 9, 11]);
    let undeclared = one.scoped_with_constraints(None, None, None);
    assert_eq!(ids(&undeclared, ALL_ITEMS), [1, 3, 5, 7, 9, 11]);
    refused(
        &undeclared,
        "SELECT kind FROM items WHERE id = 2",
        Rule::Context,
        "items",
    );
    // Asking for other Contexts leaves none.
    let other = one.scoped_with_contexts(context_set(&[2]));
    assert_eq!(ids(&other, ALL_ITEMS), [0_i64; 0]);
    // Narrowing a handle of two Contexts to one of them keeps that one.
    let both = in_contexts(&db, &[1, 2]);
    assert_eq!(
        ids(&both.scoped_with_contexts(context_set(&[2, 3])), ALL_ITEMS),
        [2, 4, 6, 8, 10, 12]
    );

    // A handle that names a principal keeps it: another name asked of it does not replace it.
    let store = notes("visibility-narrowed-again-notes");
    let reader = named(&store, agent("report-reader"));
    let renamed = reader.scoped_with_constraints(None, None, Some(human("ana")));
    assert_eq!(ids(&renamed, ALL_NOTES), [1, 3, 7]);
    let unnamed = reader.scoped_with_constraints(Some(context_set(&[1])), None, None);
    assert_eq!(ids(&unnamed, ALL_NOTES), [1]);
}

// ---- what the filter holds in memory ----

// Its size is fixed by the room: 24,000 ids are 23 times the 1,024 a statement remembers, and
// their 384,000 bytes are three times the 128 KiB the limit leaves.
#[test]
fn twenty_four_thousand_access_ids_are_read_under_a_limit_with_no_room_to_remember_them() {
    const ROWS: i64 = 24_000;
    const BATCH: i64 = 500;
    const KIB: usize = 1 << 10;
    const MIB: usize = 1 << 20;
    let paper_acl = |n: i64| Uuid::from_u128(0x0A00_0000_0000 + n as u128);

    // Paper n of 24,000 is under an access id of its own, and the agent holds the id of
    // every even paper. Loaded under no limit, 500 rows a statement.
    let folder = support::folder("visibility-many-access-ids");
    let db = support::open(&folder);
    support::run(&db, GRANTS);
    // Whichever way a grant is looked up, an index that leads with the id serves it.
    support::run(&db, "CREATE INDEX acl_grants_acl ON acl_grants (acl_id)");
    support::run(
        &db,
        "CREATE TABLE papers (id INTEGER PRIMARY KEY, acl_id UUID ACL REFERENCES \
         acl_grants(acl_id), n INTEGER NOT NULL)",
    );
    for first in (1..=ROWS).step_by((BATCH * 8) as usize) {
        let tx = db.transaction().expect("begin");
        for start in (first..first + BATCH * 8).step_by(BATCH as usize) {
            let batch = start..start + BATCH;
            let papers: Vec<String> = batch
                .clone()
                .map(|n| format!("({n}, '{}', {n})", paper_acl(n)))
                .collect();
            let grants: Vec<String> = batch
                .filter(|n| n % 2 == 0)
                .map(|n| {
                    let id = Uuid::from_u128(0x0B00_0000_0000 + n as u128);
                    format!("('{id}', 'Agent', 'reader', '{}')", paper_acl(n))
                })
                .collect();
            for sql in [
                format!(
                    "INSERT INTO papers (id, acl_id, n) VALUES {}",
                    papers.join(", ")
                ),
                format!(
                    "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) VALUES {}",
                    grants.join(", ")
                ),
            ] {
                if let Err(error) = tx.execute(&sql, &params! {}) {
                    panic!("loading the rows from {start} was refused: {error:?}");
                }
            }
        }
        tx.commit().expect("commit a part of the load");
    }
    support::close(db);

    // 9 MiB is what a store needs to open at all. The 128 KiB above it hold the filter's
    // 64 KiB and a row eight times over, and do not hold an entry for each id: 24,000 ids of
    // 16 bytes alone are 384,000 bytes, and the 12,000 that are granted are 192,000.
    let limit = 9 * MIB + 128 * KIB;
    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(limit);
    let db = support::open_with(&folder, options).expect("open the store under the limit");
    let reader = named(&db, agent("reader"));
    let in_use = db.memory_used();
    let granted = ROWS / 2;
    assert_eq!(
        support::int(&reader, "SELECT COUNT(*) FROM papers"),
        granted
    );
    // The even numbers up to 24,000: which rows were counted, not only how many.
    assert_eq!(
        support::int(&reader, "SELECT SUM(n) FROM papers"),
        granted * (granted + 1)
    );
    assert_eq!(
        db.memory_used(),
        in_use,
        "memory in use after the statements"
    );
    // A principal that holds none of the ids reads the table whole as well, and sees no row.
    let stranger = named(&db, agent("stranger"));
    assert_eq!(support::int(&stranger, "SELECT COUNT(*) FROM papers"), 0);
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM papers"), ROWS);
}

// ---- a narrowed handle declares nothing ----

#[test]
fn a_narrowed_handle_is_refused_every_declaration_and_setting_and_changes_nothing() {
    // The statements that declare a table or an index or set the store's limit apply to every
    // Context, label and principal at once, so only the handle that declared no narrowing may
    // run them. The refusal names the statement as a person writes it. The order lets each
    // statement run, unnarrowed, after the ones before it.
    let statements = [
        ("CREATE INDEX notes_by_tag ON notes (tag)", "CREATE INDEX"),
        ("ALTER TABLE notes ADD COLUMN extra TEXT", "ALTER TABLE"),
        ("SET MEMORY_LIMIT 64M", "SET MEMORY_LIMIT"),
        ("DROP INDEX notes_by_s ON notes", "DROP INDEX"),
        ("ALTER TABLE notes DROP COLUMN s CASCADE", "ALTER TABLE"),
        ("DROP TABLE notes", "DROP TABLE"),
    ];
    // The two readings of the vector part's state are the same: for every Context at once.
    let readings = [
        ("SHOW VECTOR_INDEXES", "SHOW VECTOR_INDEXES"),
        ("SHOW VECTOR_PARTITIONS", "SHOW VECTOR_PARTITIONS"),
    ];
    type Narrow = fn(&Database) -> Database;
    let handles: [(&str, Narrow); 3] = [
        ("a Context", |db| in_contexts(db, &[1])),
        ("a scope label", |db| with_labels(db, &["public"])),
        ("a principal", |db| named(db, agent("someone"))),
    ];
    let db = support::open(&support::folder("visibility-declare"));
    support::run(
        &db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, s TEXT, tag TEXT)",
    );
    support::run(&db, "CREATE INDEX notes_by_s ON notes (s)");
    for (id, number) in [(1_i64, 1_u128), (2, 2)] {
        let values = params! { "id" => id, "context" => context(number) };
        db.execute(
            "INSERT INTO notes (id, context_id, s, tag) VALUES ($id, $context, 'a', 't')",
            &values,
        )
        .expect("insert a note");
    }
    let limit = |db: &Database| support::rows(db, "SHOW MEMORY_LIMIT")[0][0].clone();
    let (declared, limit_before) = (support::rows(&db, "SELECT * FROM notes"), limit(&db));
    for (sql, name) in statements.iter().chain(&readings) {
        for (narrowed_by, narrow) in &handles {
            match support::refusal(&narrow(&db), sql) {
                Error::AdminRequired { statement } => assert_eq!(
                    statement, *name,
                    "{sql}\nrefused to a handle narrowed by {narrowed_by} under another name"
                ),
                other => panic!(
                    "{sql}\nto a handle narrowed by {narrowed_by}, which may not change what \
                     every Context's rows are kept in, was refused as {other:?}"
                ),
            }
        }
    }
    // The Rust call that sets the same limit is refused as the statement is: a program holding
    // only a narrowed handle must not lower the limit of the whole store through the other door.
    let bytes_before = db.memory_limit();
    for (narrowed_by, narrow) in &handles {
        match narrow(&db).set_memory_limit(Some(64 << 20)) {
            Err(Error::AdminRequired { statement }) => assert_eq!(
                statement, "SET MEMORY_LIMIT",
                "set_memory_limit on a handle narrowed by {narrowed_by} named another statement"
            ),
            other => panic!(
                "set_memory_limit on a handle narrowed by {narrowed_by} sets the limit of every \
                 Context, so it must be AdminRequired, got {other:?}"
            ),
        }
    }
    assert_eq!(
        db.memory_limit(),
        bytes_before,
        "a refused call changed the limit"
    );
    // The unnarrowed handle still sees the table, its column, its index and the row of the
    // Context the narrowed handles could not see, and the limit is what it was.
    assert_eq!(
        support::rows(&db, "SELECT * FROM notes"),
        declared,
        "the table changed"
    );
    let text = db.declaration("notes").expect("the table is still there");
    assert!(
        text.contains("notes_by_s") && !text.contains("notes_by_tag") && !text.contains("extra"),
        "a refused statement changed the declaration:\n{text}"
    );
    assert_eq!(limit(&db), limit_before, "the memory limit");
    assert_eq!(seen(&db, "notes", "ORDER BY id"), [1, 2]);
    // Each is a statement the unnarrowed handle may run.
    for (sql, _) in statements.iter().chain(&readings) {
        support::run(&db, sql);
    }
}
