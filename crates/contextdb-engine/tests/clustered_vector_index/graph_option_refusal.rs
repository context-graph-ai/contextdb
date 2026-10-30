//! The graph index's standalone tuning clauses and its search-radius member
//! are no longer declarations. FLAT_BELOW and AUTO_EXPANSION_CEILING are
//! refused on CREATE, ADD COLUMN and ALTER (including SET ... DEFAULT), and
//! SEARCH_RADIUS is refused when written as a SEARCH or IVF member; each
//! refusal is a parse error naming the offending keyword in the one removal
//! sentence, which also says how to declare a target, and nothing is
//! published.

use super::support::empty;
use contextdb_core::Error;
use contextdb_engine::Database;
use contextdb_engine::cli_render::render_table_meta;

fn schema(db: &Database, table: &str) -> String {
    let meta = db
        .table_meta(table)
        .unwrap_or_else(|| panic!("table metadata exists for {table}"));
    render_table_meta(table, &meta)
}

fn assert_names_the_removed_keyword(error: &Error, keyword: &str, sql: &str) {
    let sentence = format!(
        "Remove the {keyword} clause: the default search policy applies; to declare a target use SEARCH (TARGET_ACCURACY = n, EXPECTED_VECTORS = n)"
    );
    match error {
        Error::ParseError(message) => assert!(
            message.contains(&sentence),
            "the refusal of {sql} names {keyword}: expected {sentence:?} in {message}"
        ),
        other => panic!("{sql} is refused with a parse error, got {other:?}"),
    }
}

#[test]
fn standalone_graph_tuning_clauses_are_refused_naming_the_keyword() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE add_target (id INTEGER PRIMARY KEY)", &empty())
        .expect("create the add-column target");
    db.execute(
        "CREATE TABLE alter_target (id INTEGER PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .expect("create the alter target");
    let add_before = schema(&db, "add_target");
    let alter_before = schema(&db, "alter_target");

    for (keyword, clause) in [
        ("FLAT_BELOW", "FLAT_BELOW 100"),
        ("AUTO_EXPANSION_CEILING", "AUTO_EXPANSION_CEILING 50"),
    ] {
        let create = format!(
            "CREATE TABLE graph_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) {clause})"
        );
        let error = db
            .execute(&create, &empty())
            .expect_err("the clause is refused on CREATE");
        assert_names_the_removed_keyword(&error, keyword, &create);
        assert!(
            db.table_meta("graph_items").is_none(),
            "{create} publishes no table"
        );

        let add = format!("ALTER TABLE add_target ADD COLUMN embedding VECTOR(3) {clause}");
        let error = db
            .execute(&add, &empty())
            .expect_err("the clause is refused on ADD COLUMN");
        assert_names_the_removed_keyword(&error, keyword, &add);
        assert_eq!(schema(&db, "add_target"), add_before, "{add} adds nothing");

        for alter in [
            format!("ALTER TABLE alter_target ALTER COLUMN embedding SET {clause}"),
            format!("ALTER TABLE alter_target ALTER COLUMN embedding SET {keyword} DEFAULT"),
        ] {
            let error = db
                .execute(&alter, &empty())
                .expect_err("the clause is refused on ALTER");
            assert_names_the_removed_keyword(&error, keyword, &alter);
            assert_eq!(
                schema(&db, "alter_target"),
                alter_before,
                "{alter} changes nothing"
            );
        }
    }
}

#[test]
fn search_radius_is_refused_as_a_search_or_ivf_member() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE listed_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) IVF (LISTS = 8))",
        &empty(),
    )
    .expect("a valid IVF member is accepted, so the refusals below are about the member");
    let before = schema(&db, "listed_items");
    for sql in [
        "CREATE TABLE radius_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) IVF (SEARCH_RADIUS = 0.5))",
        "CREATE TABLE radius_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) IVF (LISTS = 8, SEARCH_RADIUS = 0.5))",
        "CREATE TABLE radius_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) SEARCH (SEARCH_RADIUS = 0.5))",
        "ALTER TABLE listed_items ALTER COLUMN embedding SET IVF (SEARCH_RADIUS = 0.5)",
        "ALTER TABLE listed_items ALTER COLUMN embedding SET SEARCH (SEARCH_RADIUS = 0.5)",
    ] {
        let error = db
            .execute(sql, &empty())
            .expect_err("SEARCH_RADIUS is refused as a member");
        assert_names_the_removed_keyword(&error, "SEARCH_RADIUS", sql);
        assert!(
            db.table_meta("radius_items").is_none(),
            "{sql} publishes no table"
        );
        assert_eq!(schema(&db, "listed_items"), before, "{sql} changes nothing");
    }
}
