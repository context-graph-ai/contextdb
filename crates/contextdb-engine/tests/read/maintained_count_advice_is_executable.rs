//! The `ALTER TABLE` that `.explain` prints can be run as printed.
//!
//! `SET MAINTAIN COUNT` replaces a table's whole declared set — that is its
//! contract, the same one `SET IVF DEFAULT` has. So advice that names only the
//! missing column list is advice that silently drops every count the table
//! already declares: a consumer that follows the engine's own suggestion to
//! make one read O(1) makes its other reads O(rows). The printed statement
//! therefore carries the existing declarations plus the missing one.
//!
//! Checked on both read routes, because the disclosure crosses the owner
//! channel as its own carried field and a route that dropped it would leave a
//! reader of someone else's store with no advice at all. The two routes do not
//! answer the same way and are not asked to: the stored count is reachable
//! only through a committed read image, so the file route serves it while a
//! writable handle and the owner route count the same postings from the index
//! range. Both are exact; only the file route is O(1). What matters here is
//! that neither ever reports a DECLARED count as undeclared.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadRoute;
use contextdb_engine::cli_render::{render_explain_trace, render_table_meta};
use contextdb_engine::{Database, MaintenancePolicy, ReadSession, ReadSessionOptions};
use std::collections::HashMap;

const OVER_A: &str = "SELECT COUNT(*) FROM placements WHERE a = $a";
const OVER_B: &str = "SELECT COUNT(*) FROM placements WHERE b = $b";

fn over_a() -> HashMap<String, Value> {
    HashMap::from([("a".to_string(), Value::Text("left".into()))])
}

fn over_b() -> HashMap<String, Value> {
    HashMap::from([("b".to_string(), Value::Text("up".into()))])
}

fn execute(db: &Database, sql: &str, params: HashMap<String, Value>) {
    db.execute(sql, &params)
        .unwrap_or_else(|error| panic!("fixture statement `{sql}`: {error}"));
}

/// The `ALTER TABLE ...` the trace line advises, lifted out exactly as printed.
fn advised_statement(rendered: &str) -> String {
    let line = rendered
        .lines()
        .find(|line| line.contains("maintained count: not declared"))
        .unwrap_or_else(|| panic!("the trace advises a declaration:\n{rendered}"));
    let start = line
        .find("ALTER TABLE")
        .unwrap_or_else(|| panic!("the advice names a statement: {line}"));
    let rest = &line[start..];
    let end = rest
        .find(" makes this O(1)")
        .unwrap_or_else(|| panic!("the advice ends with what it buys: {line}"));
    rest[..end].to_string()
}

#[test]
#[expect(
    clippy::needless_borrow,
    reason = "The test keeps explicit borrows of query-result accessors while inspecting the retained answer."
)]
fn the_declaration_explain_advises_keeps_the_declarations_already_made() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let path = root.path().join("advice.db");
    let db = Database::open(&path).expect("open the writable fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    execute(
        &db,
        "CREATE TABLE placements (id UUID PRIMARY KEY, a TEXT NOT NULL, b TEXT NOT NULL)",
        HashMap::new(),
    );
    execute(
        &db,
        "CREATE INDEX placements_by_a ON placements (a)",
        HashMap::new(),
    );
    execute(
        &db,
        "CREATE INDEX placements_by_b ON placements (b)",
        HashMap::new(),
    );
    for n in 0..12u128 {
        execute(
            &db,
            "INSERT INTO placements (id, a, b) VALUES ($id, $a, $b)",
            HashMap::from([
                ("id".into(), Value::Uuid(uuid::Uuid::from_u128(n))),
                (
                    "a".into(),
                    Value::Text(if n % 2 == 0 {
                        "left".into()
                    } else {
                        "right".into()
                    }),
                ),
                (
                    "b".into(),
                    Value::Text(if n % 3 == 0 {
                        "up".into()
                    } else {
                        "down".into()
                    }),
                ),
            ]),
        );
    }
    execute(
        &db,
        "ALTER TABLE placements SET MAINTAIN COUNT (a)",
        HashMap::new(),
    );

    // The count over `b` is the one that is not declared, so it is the one the
    // engine advises about.
    let undeclared = db.execute(OVER_B, &over_b()).expect("count over b");
    let advice = advised_statement(&render_explain_trace(&undeclared.trace()));
    eprintln!("the engine advises: {advice}");

    // Run it exactly as printed — that is the whole point of printing it.
    db.execute(&advice, &HashMap::new())
        .unwrap_or_else(|error| panic!("the advised statement `{advice}`: {error}"));

    let schema = render_table_meta(
        "placements",
        &db.table_meta("placements").expect("the table exists"),
    );
    assert!(
        schema.contains("MAINTAIN COUNT (a), (b)"),
        "following the advice keeps the declaration that was already there and adds the one it \
         was about: {schema}"
    );

    for (sql, params, column, what) in [
        (OVER_A, over_a(), "a", "the count that was already declared"),
        (OVER_B, over_b(), "b", "the count the advice was about"),
    ] {
        let answered = db.execute(sql, &params).expect("count after the advice");
        let rendered = render_explain_trace(&answered.trace());
        assert!(
            matches!(answered.trace().maintained_count.as_deref(),
                Some(contextdb_engine::maintained_counts::MaintainedCountDisclosure::Used { table, columns })
                    if table == "placements" && columns == &vec![(column.to_owned(), contextdb_core::SortDirection::Asc)]),
            "{what} is reported as declared after the advice ran: {rendered}"
        );
        assert!(
            !rendered.contains("not declared"),
            "{what} is never advised to declare what it already declares: {rendered}"
        );
    }
    assert_eq!(
        db.execute(OVER_A, &over_a()).unwrap().rows()[0][0],
        Value::Int64(6)
    );
    assert_eq!(
        db.execute(OVER_B, &over_b()).unwrap().rows()[0][0],
        Value::Int64(4)
    );

    // The owner route: the writer still holds the store, so a reading session
    // is answered by the owner and the disclosure has to cross that channel.
    let owner_session = ReadSession::open_with_options(&path, ReadSessionOptions::default())
        .expect("open a reading session while the writer holds the store");
    assert_eq!(owner_session.route(), ReadRoute::Owner);
    for (sql, params, column) in [(OVER_A, over_a(), "a"), (OVER_B, over_b(), "b")] {
        let answered = owner_session
            .execute(sql, &params)
            .expect("owner-route count");
        let rendered = render_explain_trace(&answered.trace());
        assert!(
            matches!(answered.trace().maintained_count.as_deref(),
                Some(contextdb_engine::maintained_counts::MaintainedCountDisclosure::Used { table, columns })
                    if table == "placements" && columns == &vec![(column.to_owned(), contextdb_core::SortDirection::Asc)])
                && !rendered.contains("not declared"),
            "the owner route carries the same disclosure the owner made: {rendered}"
        );
    }
    drop(owner_session);
    db.close().expect("close the writer");

    // The file route: nobody holds the store, so the session reads the
    // committed image itself -- and that is the route the stored count serves.
    let file_session = ReadSession::open_with_options(&path, ReadSessionOptions::default())
        .expect("open a reading session on an unheld store");
    assert_eq!(file_session.route(), ReadRoute::File);
    for (sql, params, expected) in [
        (OVER_A, over_a(), Value::Int64(6)),
        (OVER_B, over_b(), Value::Int64(4)),
    ] {
        let answered = file_session
            .execute(sql, &params)
            .expect("file-route count");
        assert_eq!(answered.rows()[0][0], expected);
        let rendered = render_explain_trace(&answered.trace());
        assert!(
            rendered.contains("maintained count: used"),
            "the file route serves both counts from the declaration the advice produced: \
             {rendered}"
        );
    }
}
