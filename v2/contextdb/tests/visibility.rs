//! Who may see and write a row. A handle narrowed to Contexts, to scope labels or to a principal
//! reads the rows it may and no others, changes none it may not see, is refused a row it names
//! by its key by the rule that hid it, and is told nothing of what it may not see: not by a
//! count, not by a search, not by the words of a refusal.

mod support;

use std::collections::HashMap;

use contextdb::{Database, Error, Principal, Uuid, Value, params};

use support::{GRANTS, Run, acl, agent, context, context_set, grant, human, ids, in_contexts};
use support::{label_set, named, with_labels};

// ---- what a statement answered ----

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

const NOTES: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, \
                     acl_id UUID ACL REFERENCES acl_grants(acl_id), payload TEXT)";

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
    let source = support::rows(&unnarrowed, "SELECT acl_id FROM notes WHERE id = 5");
    assert_eq!(source, [[Value::Uuid(acl(3))]], "the row's recorded source");
}

#[test]
fn a_principal_reads_the_rows_it_holds_a_grant_for_and_no_others() {
    let db = notes("visibility-grants");
    let reader = named(&db, agent("report-reader"));
    assert_eq!(ids(&reader, ALL_NOTES), [1, 3, 7]);
    let session = reader.read_session(Default::default()).unwrap();
    assert!(ids(&session, ALL_NOTES) == [1, 3, 7], "as the handle");
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
        "SELECT payload FROM ledger WHERE id = (SELECT 2)",
        "SELECT id FROM ledger WHERE slug = 'shut-four'",
        // Each names the one hidden row whatever the form it is written in: a sum, a row value,
        // the same key twice, a key beside alternatives that each name the same row.
        "SELECT payload FROM ledger WHERE id = 1 + 1",
        "SELECT payload FROM ledger WHERE (a, b) = (1, 2)",
        "SELECT payload FROM ledger WHERE id = 2 OR id = 2",
        "SELECT payload FROM ledger WHERE id = 2 AND (payload = 'a' OR slug = 'b')",
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
    let lists: [(&str, &[i64]); 13] = [
        ("ORDER BY id", &[1, 3]),
        ("WHERE a = 1 ORDER BY id", &[1]),
        ("WHERE b = 2 ORDER BY id", &[]),
        ("WHERE id IN (2)", &[]),
        ("WHERE id = 2 AND id = 3", &[]),
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

// ---- one world under each rule, and what is done to the rows it hides ----

type Sections = HashMap<String, Vec<String>>;

/// The steps done to rows a handle may not see, by the sections of the world that do them.
const STEPS: [&str; 6] = [
    "hidden rows added",
    "seal",
    "build",
    "hidden rows changed",
    "hidden rows taken away",
    "build",
];

/// The sections of `tests/fixtures/narrowed.sql` under this rule: each a list of statements,
/// with `{reach}`, `{own}` and `{hid}` put in.
fn world(gate: Gate) -> Sections {
    let literal = |seen| match gate.reach(seen) {
        Value::Uuid(id) => format!("'{id}'"),
        Value::Text(text) => format!("'{text}'"),
        other => unreachable!("{other:?}"),
    };
    let (mut sections, mut at) = (Sections::new(), "");
    for line in include_str!("fixtures/narrowed.sql").lines().chain([
        "## world",
        "DELETE FROM vo WHERE id >= 100",
        "INSERT INTO vo VALUES (3, {own}, [0.0, 0.0, 1.0])",
        "CREATE INDEX vo_reach ON vo (reach)",
        "## reads",
        "SELECT id FROM vo WHERE id >= 100 ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR AUTO LIMIT 2",
        "SELECT id FROM vo WHERE id >= 100 ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR INDEXED LIMIT 2",
        "SELECT id FROM vo WHERE reach > {own} ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR AUTO LIMIT 2",
        "SELECT id FROM vo WHERE reach > {own} ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR INDEXED LIMIT 2",
        "SELECT id FROM vo WHERE id IN (1, 100) ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 2",
        "SELECT id FROM vo WHERE reach = {own} ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 3",
        "## hidden rows added",
        "INSERT INTO vo VALUES (100, {hid}, [1.0, 0.0, 0.0]), (101, {hid}, [1.0, 0.0, 0.0]), (102, {hid}, [1.0, 0.0, 0.0])",
    ]) {
        if let Some(name) = line.strip_prefix("## ") {
            at = name;
        } else if !line.starts_with('#') {
            let sql = line
                .replace("{reach}", gate.column())
                .replace("{own}", &literal(true));
            let sql = sql.replace("{hid}", &literal(false));
            sections.entry(at.to_owned()).or_default().push(sql);
        }
    }
    sections
}

/// Everything a handle is told by the world's reads, asked plainly and through EXPLAIN, and by
/// its writes, each rolled back. `leaks` gets every EXPLAIN that holds one of `words.1`.
fn told(
    on: &Database,
    world: &Sections,
    words: &[Vec<String>; 2],
    leaks: &mut Vec<String>,
) -> Vec<String> {
    let session = on.read_session(contextdb::ReadLimits::default()).unwrap();
    assert_eq!(
        support::run(&session, "SHOW MEMORY_LIMIT").peak_bytes(),
        None
    );
    let empty = support::ids(on, "SELECT id FROM vo ORDER BY id").is_empty();
    let mut told = Vec::new();
    let session = on.read_session(Default::default()).unwrap();
    assert_eq!(
        support::run(&session, "SHOW MEMORY_LIMIT").peak_bytes(),
        None
    );
    for sql in ["SHOW VECTOR_INDEXES", "SHOW VECTOR_PARTITIONS"] {
        let r = session.execute(sql, &params! {});
        assert!(matches!(r, Err(Error::AdminRequired { .. })), "{r:?}");
    }
    for sql in &world["reads"] {
        let plan = on.explain(sql);
        assert!(
            plan.is_ok() || sql.contains("INDEXED"),
            "{sql} is explained"
        );
        let text = match plan {
            Ok(text) => text.to_string(),
            Err(refusal) => said(Err(refusal), sql, &words[0], leaks),
        };
        assert!(!text.is_empty(), "{sql} is told something");
        for word in words[1].iter().filter(|word| text.contains(*word)) {
            leaks.push(format!("{sql}\nis explained with `{word}`:\n{text}"));
        }
        told.push(format!("{sql}\n{text}"));
        let result = on.execute(sql, &params! {});
        if sql.contains("FROM vo ")
            && (sql.contains("WHERE id IN (1, 100)") || sql.contains("WHERE reach ="))
        {
            let rows = result.as_ref().unwrap().rows();
            let keys = vec![vec![Value::Int64(1)]; usize::from(!empty)];
            if sql.contains("WHERE id IN") {
                assert_eq!(rows, keys);
            } else {
                assert_eq!(rows.len(), if empty { 0 } else { 3 });
            }
        }
        let answer = said(result, sql, &words[0], leaks);
        told.push(format!("{sql}\n{answer}"));
    }
    for sql in &world["writes"] {
        let tx = on.transaction().expect("begin");
        let answer = said(tx.execute(sql, &params! {}), sql, &words[0], leaks);
        told.push(format!("{sql}\n{answer}"));
        tx.rollback();
    }
    told
}

/// The `Debug` of a result, or a refusal's `Debug` and words. `leaks` gets a refusal that holds
/// a digit (no name in the world has one: it is a key, a count or an id) or one of `hidden`.
fn said(
    answer: contextdb::Result<contextdb::QueryResult>,
    sql: &str,
    hidden: &[String],
    leaks: &mut Vec<String>,
) -> String {
    let refusal = match answer {
        Ok(result) => return format!("{result:?}"),
        Err(refusal) => refusal,
    };
    let words = format!("{refusal:?} {refusal}");
    if words.contains(|c: char| c.is_ascii_digit()) || hidden.iter().any(|w| words.contains(w)) {
        leaks.push(format!(
            "{sql}\nwas refused with a digit or a hidden word: {words}"
        ));
    }
    words
}

#[test]
fn a_narrowed_reader_is_told_the_same_whatever_is_done_to_rows_it_may_not_see() {
    let (mut leaks, mut moved) = (Vec::new(), Vec::new());
    for gate in Gate::ALL {
        let world = world(gate);
        let db = support::open(&support::folder(&format!("narrowed-world-{gate:?}")));
        if let Gate::Grant = gate {
            support::run(&db, GRANTS);
            grant(&db, 1, "Agent", "report-reader", acl(1));
            grant(&db, 2, "Human", "someone-else", acl(2));
        }
        world["world"]
            .iter()
            .for_each(|sql| drop(support::run(&db, sql)));
        let _ = support::cycle(&db);
        // The rule's handle, and for Contexts one given none as well.
        let mut handles = vec![gate.handle(&db)];
        handles.extend(matches!(gate, Gate::Context).then(|| in_contexts(&db, &[])));
        let id = match gate.reach(false) {
            Value::Uuid(id) => id.to_string(),
            Value::Text(label) => label,
            other => unreachable!("{other:?}"),
        };
        // A refusal names no hidden value. An EXPLAIN says no key, route or refusal, no hidden
        // id, and neither the 140 rows of `vd` there are nor the 137 the handle may not see.
        let refusal = ["secret", "shut", "internal", "someone-else", &id];
        let explain = ["key:", "route:", "refusal:", &id, "140", "137"];
        let words = [
            refusal.map(String::from).to_vec(),
            explain.map(String::from).to_vec(),
        ];
        for table in ["vm", "vd", "vo"] {
            let sql = format!(
                "SELECT id FROM {table} ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR INDEXED LIMIT 2"
            );
            let answer = gate.handle(&db).execute(&sql, &params! {});
            if table == "vo" && matches!(gate, Gate::Context) {
                assert_eq!(ids(&gate.handle(&db), &sql), [1, 2]);
            } else if !format!("{answer:?}").starts_with("Err(VectorIndexedNarrowed") {
                leaks.push(format!(
                    "{gate:?}, {table}: expected VectorIndexedNarrowed, got {answer:?}"
                ));
            }
        }
        let before: Vec<_> = handles
            .iter()
            .map(|on| told(on, &world, &words, &mut leaks))
            .collect();
        for step in STEPS {
            match step {
                "seal" => drop(support::cycle(&db)),
                "build" => drop(support::maintenance(&db)),
                _ => world[step]
                    .iter()
                    .for_each(|sql| drop(support::run(&db, sql))),
            }
            for (on, before) in handles.iter().zip(&before) {
                let now = told(on, &world, &words, &mut Vec::new());
                let pairs = before.iter().zip(now).filter(|(was, now)| *was != now);
                moved.extend(
                    pairs.map(|(was, now)| format!("{gate:?} after {step}:\n{was}\n{now}")),
                );
            }
        }
        // Hidden rows fill both partitions: a third key is refused alike, naming nothing hidden.
        let full = |on: &Database| on.execute(&world["limit"][0], &params! {}).unwrap_err();
        let (narrowed, plain) = (full(&handles[0]), full(&db));
        assert!(matches!(
            narrowed,
            Error::VectorPartitionLimitExceeded { .. }
        ));
        assert_eq!(format!("{narrowed:?}"), format!("{plain:?}"));
        assert!(!format!("{narrowed:?}").contains(&id), "{gate:?}");
        let empty = in_contexts(&db, &[])
            .read_session(contextdb::ReadLimits::default())
            .unwrap();
        let shown = ["SHOW VECTOR_INDEXES", "SHOW VECTOR_PARTITIONS"]
            .map(|sql| empty.execute(sql, &params! {}));
        assert!(
            shown
                .iter()
                .all(|result| matches!(result, Err(Error::AdminRequired { .. })))
        );
        for on in &handles {
            assert!(
                matches!(on.event_bus_status(), Err(Error::AdminRequired { statement }) if statement == "event_bus_status")
            );
            assert!(
                matches!(on.sink_metrics("hidden"), Err(Error::AdminRequired { statement }) if statement == "sink_metrics")
            );
            assert!(
                matches!(on.subscribe(), Err(Error::AdminRequired { statement }) if statement == "subscribe")
            );
            assert!(
                matches!(on.subscribe_with_capacity(1), Err(Error::AdminRequired { statement }) if statement == "subscribe_with_capacity")
            );
            let report = on.work_done().map(|_| ());
            if !matches!(report, Err(Error::AdminRequired { .. })) {
                leaks.push(format!("{gate:?}: {report:?}"));
            }
            let report = on
                .run_maintenance_cycle()
                .expect("narrowed maintenance runs");
            let counts: [Option<u64>; 8] = [
                report.delivered,
                report.expired_rows,
                report.compacted_pages,
                report.deferred,
                report.vector.maintenance_vectors_done,
                report.vector.maintenance_vectors_remaining,
                report.vector.remaining_partitions,
                report.vector.stalled_partitions,
            ];
            if counts != [None; 8] {
                leaks.push(format!("{gate:?}: maintenance counts: {counts:?}"));
            }
            for sql in ["SELECT id FROM vm", "SHOW MEMORY_LIMIT"] {
                let answer = on.execute(sql, &params! {}).expect("narrowed statement");
                assert_eq!(answer.peak_bytes(), None, "{gate:?}: {sql}");
                if sql == "SHOW MEMORY_LIMIT" {
                    let limit = Value::Int64(support::limit(on).expect("finite limit") as i64);
                    assert_eq!(answer.rows()[0][0], limit);
                }
            }
        }
    }
    assert!(
        leaks.is_empty() && moved.is_empty(),
        "told of rows it may not see:\n\n{}\n\nchanged by what was done to them:\n\n{}",
        leaks.join("\n\n"),
        moved.join("\n\n")
    );
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
    // The handle with no labels writes a row with none.
    assert_eq!(changed(&db, &new(7, "NULL")), 1);
    assert_eq!(seen(&db, "memos", "ORDER BY id"), [1, 2, 3, 7]);
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

    // What a store needs to open at all, and 128 KiB above it that hold the filter's
    // 64 KiB and a row eight times over, and do not hold an entry for each id: 24,000 ids of
    // 16 bytes alone are 384,000 bytes, and the 12,000 that are granted are 192,000.
    let limit = support::opening::open_floor() + 128 * KIB;
    let db = support::open_limited(&folder, limit);
    let reader = named(&db, agent("reader"));
    let in_use = support::used(&db);
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
        support::used(&db),
        in_use
            + support::form_charge("SELECT COUNT(*) FROM papers")
            + support::form_charge("SELECT SUM(n) FROM papers"),
        "memory in use after the statements: the record's line for each of the two forms"
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
        ("SET DISK_LIMIT 16M", "SET DISK_LIMIT"),
        (
            "SET MAINTENANCE_POLL_INTERVAL 2 SECONDS",
            "SET MAINTENANCE_POLL_INTERVAL",
        ),
        (
            "SET STORAGE_COMPACTION START_RATIO 0.2",
            "SET STORAGE_COMPACTION START_RATIO",
        ),
        (
            "SET STORAGE_COMPACTION STOP_RATIO 0.01",
            "SET STORAGE_COMPACTION STOP_RATIO",
        ),
        (
            "SET UNINDEXED_VECTOR_LIMIT 400",
            "SET UNINDEXED_VECTOR_LIMIT",
        ),
        ("DROP INDEX notes_by_s ON notes", "DROP INDEX"),
        ("ALTER TABLE notes DROP COLUMN s CASCADE", "ALTER TABLE"),
        ("DROP TABLE notes", "DROP TABLE"),
    ];
    // The two readings of the vector part's state are the same: for every Context at once.
    let readings = [
        ("SHOW CONTENT_SIZE", "SHOW CONTENT_SIZE"),
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
    let bytes_before = support::limit(&db);
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
        support::limit(&db),
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
        "a refused statement changed the declaration:\n{}",
        text.as_str()
    );
    assert_eq!(limit(&db), limit_before, "the memory limit");
    assert_eq!(seen(&db, "notes", "ORDER BY id"), [1, 2]);
    // Each is a statement the unnarrowed handle may run.
    for (sql, _) in statements.iter().chain(&readings) {
        support::run(&db, sql);
    }
}

/// A count through an index on a narrowed handle counts the rows the handle may see and no
/// others: of three rows under one indexed value, one is hidden by the rule, and the count is
/// the two the handle sees, the same as the table without the index gives, with no more rows
/// examined than those two.
fn a_count_through_an_index_counts_only_what_the_handle_sees(gate: Gate) {
    let db = gate.store("visibility-count");
    for twin in ["tagged", "plain"] {
        support::run(
            &db,
            &format!(
                "CREATE TABLE {twin} (id INTEGER PRIMARY KEY, tag INTEGER NOT NULL, {})",
                gate.column()
            ),
        );
        for (id, seen) in [(1_i64, true), (2, false), (3, true)] {
            db.execute(
                &format!("INSERT INTO {twin} (id, tag, reach) VALUES ($id, 7, $reach)"),
                &params! { "id" => id, "reach" => gate.reach(seen) },
            )
            .expect("a row of the table");
        }
    }
    support::run(&db, "CREATE INDEX tagged_tag ON tagged (tag)");
    let handle = gate.handle(&db);
    let (indexed, plain) = (
        support::run(&handle, "SELECT COUNT(*) FROM tagged WHERE tag = 7"),
        support::run(&handle, "SELECT COUNT(*) FROM plain WHERE tag = 7"),
    );
    assert_eq!(
        indexed.rows(),
        plain.rows(),
        "{gate:?}: the index changes no count"
    );
    assert_eq!(
        indexed.rows()[0][0],
        Value::Int64(2),
        "{gate:?}: the rows the handle sees"
    );
    assert!(
        indexed.rows_examined() <= 2,
        "{gate:?}: the hidden row is not examined, nor counted from its entry: {} examined",
        indexed.rows_examined()
    );
}

#[test]
fn a_count_through_an_index_counts_only_what_a_context_handle_sees() {
    a_count_through_an_index_counts_only_what_the_handle_sees(Gate::Context);
}

#[test]
fn a_count_through_an_index_counts_only_what_a_label_handle_sees() {
    a_count_through_an_index_counts_only_what_the_handle_sees(Gate::Label);
}

#[test]
fn a_count_through_an_index_counts_only_what_a_grant_handle_sees() {
    a_count_through_an_index_counts_only_what_the_handle_sees(Gate::Grant);
}

/// A key and thirteen two-way alternatives still name the one row: a whole handle reads that
/// row alone, and a handle that may not see it is refused as it is for the key alone.
fn thirteen_alternatives_beside_a_key_still_name_one_row(gate: Gate) {
    let db = gate.store("visibility-thirteen");
    let ors = (0..13)
        .map(|_| "(a = 1 OR a = 9)")
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!("SELECT payload FROM ledger WHERE id = 2 AND {ors}");
    let whole = support::run(&db, &sql);
    assert_eq!(
        whole.rows().len(),
        1,
        "{gate:?}: the row passes every alternative"
    );
    assert_eq!(
        whole.rows_examined(),
        1,
        "{gate:?}: the key names one row, and one row is read"
    );
    refused(&gate.handle(&db), &sql, gate.rule(), "ledger");
}

#[test]
fn thirteen_alternatives_beside_a_key_name_one_row_for_every_rule() {
    for gate in Gate::ALL {
        thirteen_alternatives_beside_a_key_still_name_one_row(gate);
    }
}

/// Two nodes and an edge between them, all under Context 1 but the far node's row, which is
/// under Context 2, and a vector row the same way. A reader of Context 1 may not see them.
fn hidden_far_node() -> Database {
    let db = support::open(&support::folder("visibility-hidden-far-node"));
    for sql in [
        "CREATE TABLE edges (id INTEGER PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, \
         edge_type TEXT NOT NULL, context_id UUID CONTEXT_ID)",
        "CREATE TABLE things (id UUID PRIMARY KEY, label TEXT, context_id UUID CONTEXT_ID)",
        "CREATE TABLE vecs (id INTEGER PRIMARY KEY, v VECTOR(2), context_id UUID CONTEXT_ID)",
    ] {
        support::run(&db, sql);
    }
    let (near, far) = (Uuid::from_u128(1), Uuid::from_u128(2));
    for (id, label, of) in [(near, "near", 1), (far, "far", 2)] {
        let values = params! { "id" => id, "label" => label, "context" => context(of) };
        db.execute(
            "INSERT INTO things (id, label, context_id) VALUES ($id, $label, $context)",
            &values,
        )
        .unwrap();
    }
    let values = params! { "from" => near, "to" => far, "context" => context(1) };
    db.execute(
        "INSERT INTO edges VALUES (1, $from, $to, 'E', $context)",
        &values,
    )
    .unwrap();
    for (id, of) in [(1, 1), (2, 2)] {
        let values = params! { "id" => id, "v" => Value::Vector(vec![1.0, id as f32]), "context" => context(of) };
        db.execute("INSERT INTO vecs VALUES ($id, $v, $context)", &values)
            .unwrap();
    }
    db
}

#[test]
fn a_node_whose_row_the_reader_may_not_see_gives_no_property_to_columns() {
    let db = hidden_far_node();
    let walk = "SELECT label FROM GRAPH_TABLE(edges MATCH (a)-[e]->(b) COLUMNS (b.label AS label))";
    let all = support::rows(&db, walk);
    assert_eq!(
        all,
        [[Value::Text("far".into())]],
        "a handle that sees the far node reads its property"
    );
    let one = in_contexts(&db, &[1]);
    assert!(
        support::rows(&one, walk).is_empty(),
        "a hidden node's property is never returned, not as NULL either"
    );
}

#[test]
fn a_row_vector_subquery_naming_a_hidden_row_is_refused_by_the_rule_that_hid_it() {
    let db = hidden_far_node();
    let nearest = |id: i64| {
        format!("SELECT id FROM vecs ORDER BY v <=> (SELECT v FROM vecs WHERE id = {id}) LIMIT 5")
    };
    assert_eq!(
        ids(&db, &nearest(2)),
        [2, 1],
        "a handle that sees row 2 searches by its vector, nearest first: row 2 itself at 0"
    );
    let one = in_contexts(&db, &[1]);
    refused(&one, &nearest(2), Rule::Context, "vecs");
    assert_eq!(
        ids(&one, &nearest(1)),
        [1],
        "the reader's own row still serves"
    );
}
