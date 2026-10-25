//! A schema author declares the clustered vector index as
//! `IVF (LISTS, PROBES, RERANK)`. The graph index spelling `HNSW (...)` is no
//! longer a storage choice: it is refused with a parse error that names the
//! replacement, and nothing is published. The IVF group renders canonically,
//! round-trips through its own rendering, and refuses invalid members.

use super::support::empty;
use contextdb_core::{Error, VectorIndexRef};
use contextdb_engine::Database;
use contextdb_engine::cli_render::render_table_meta;

fn schema(db: &Database, table: &str) -> String {
    let meta = db
        .table_meta(table)
        .unwrap_or_else(|| panic!("table metadata exists for {table}"));
    render_table_meta(table, &meta)
}

fn assert_names_the_ivf_form(error: &Error, sql: &str) {
    match error {
        Error::ParseError(message) => assert!(
            ["IVF", "LISTS", "PROBES", "RERANK"]
                .iter()
                .all(|word| message.contains(word)),
            "the refusal of {sql} names IVF (LISTS = ..., PROBES = ..., RERANK = ...): {message}"
        ),
        other => panic!("{sql} is refused with a parse error, got {other:?}"),
    }
}

#[test]
fn an_hnsw_declaration_is_refused_with_a_parse_error_naming_ivf() {
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

    let create = "CREATE TABLE graph_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE INDEXED HNSW (M = 16, EF_CONSTRUCTION = 200, EF_SEARCH = 64))";
    let error = db
        .execute(create, &empty())
        .expect_err("CREATE with HNSW is refused");
    assert_names_the_ivf_form(&error, create);
    assert!(
        db.table_meta("graph_items").is_none(),
        "a refused CREATE publishes no table"
    );

    let add = "ALTER TABLE add_target ADD COLUMN embedding VECTOR(3) HNSW (M = 12)";
    let error = db
        .execute(add, &empty())
        .expect_err("ADD COLUMN with HNSW is refused");
    assert_names_the_ivf_form(&error, add);
    assert_eq!(schema(&db, "add_target"), add_before, "{add} adds nothing");

    for alter in [
        "ALTER TABLE alter_target ALTER COLUMN embedding SET HNSW (EF_SEARCH = 64)",
        "ALTER TABLE alter_target ALTER COLUMN embedding SET HNSW DEFAULT",
    ] {
        let error = db
            .execute(alter, &empty())
            .expect_err("ALTER with HNSW is refused");
        assert_names_the_ivf_form(&error, alter);
        assert_eq!(
            schema(&db, "alter_target"),
            alter_before,
            "{alter} changes nothing"
        );
    }
}

#[test]
fn an_all_default_ivf_group_is_the_same_declaration_as_silence() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE silent_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE INDEXED)",
        &empty(),
    )
    .expect("create the silent declaration");
    db.execute(
        "CREATE TABLE default_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE INDEXED IVF (LISTS = DEFAULT, PROBES = DEFAULT, RERANK = DEFAULT))",
        &empty(),
    )
    .expect("an all-DEFAULT IVF group parses");
    assert_eq!(
        schema(&db, "default_items"),
        schema(&db, "silent_items").replace("silent_items", "default_items"),
        "DEFAULT members declare no override, so the group renders as silence"
    );
}

#[test]
fn explicit_ivf_members_render_canonically_and_round_trip() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE declared_items (id INTEGER PRIMARY KEY, scope_id TEXT NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_id) SEARCH_MODE AUTO IVF (RERANK = 200, LISTS = 64, PROBES = 8))",
        &empty(),
    )
    .expect("CREATE accepts explicit IVF members in any order");
    let declared = schema(&db, "declared_items");
    assert!(
        declared.contains("IVF (LISTS = 64, PROBES = 8, RERANK = 200)"),
        "explicit members render in canonical order: {declared}"
    );

    let replay = Database::open_memory();
    replay
        .execute(&declared, &empty())
        .expect("the rendered declaration is executable SQL");
    assert_eq!(
        schema(&replay, "declared_items"),
        declared,
        "the rendering is a fixpoint"
    );

    db.execute("CREATE TABLE add_items (id INTEGER PRIMARY KEY)", &empty())
        .expect("create the add-column target");
    db.execute(
        "ALTER TABLE add_items ADD COLUMN embedding VECTOR(3) WITH (quantization = 'SQ8') IVF (PROBES = 4)",
        &empty(),
    )
    .expect("ADD COLUMN accepts a partial IVF group");
    let added = schema(&db, "add_items");
    assert!(
        added.contains("IVF (PROBES = 4)") && !added.contains("LISTS") && !added.contains("RERANK"),
        "a partial group renders only its declared members: {added}"
    );
}

#[test]
fn ivf_alters_change_only_the_members_they_name() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE policy_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) IVF (LISTS = 32, PROBES = 4, RERANK = 100))",
        &empty(),
    )
    .expect("declare an explicit complete group");

    db.execute(
        "ALTER TABLE policy_items ALTER COLUMN embedding SET IVF (PROBES = 16, RERANK = 300)",
        &empty(),
    )
    .expect("a partial ALTER keeps omitted members");
    let partial = schema(&db, "policy_items");
    assert!(
        partial.contains("IVF (LISTS = 32, PROBES = 16, RERANK = 300)"),
        "omitted members retain their declaration: {partial}"
    );

    db.execute(
        "ALTER TABLE policy_items ALTER COLUMN embedding SET IVF (PROBES = DEFAULT)",
        &empty(),
    )
    .expect("a member DEFAULT clears only that member");
    let member_default = schema(&db, "policy_items");
    assert!(
        member_default.contains("IVF (LISTS = 32, RERANK = 300)")
            && !member_default.contains("PROBES"),
        "member DEFAULT leaves the other members intact: {member_default}"
    );

    db.execute(
        "ALTER TABLE policy_items ALTER COLUMN embedding SET IVF DEFAULT",
        &empty(),
    )
    .expect("IVF DEFAULT clears the whole group");
    assert!(
        !schema(&db, "policy_items").contains("IVF"),
        "a cleared group renders as silence"
    );
}

#[test]
fn duplicate_zero_and_negative_ivf_members_are_refused_without_changing_the_schema() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE policy_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) IVF (LISTS = 32, PROBES = 4, RERANK = 100))",
        &empty(),
    )
    .expect("declare the group every refusal must preserve");
    let original = schema(&db, "policy_items");

    for member in ["LISTS", "PROBES", "RERANK"] {
        let duplicate = format!(
            "ALTER TABLE policy_items ALTER COLUMN embedding SET IVF ({member} = 8, {member} = 9)"
        );
        match db.execute(&duplicate, &empty()) {
            Err(Error::ParseError(message)) => assert!(
                message.contains(member),
                "a duplicate member is named in the refusal of {duplicate}: {message}"
            ),
            other => panic!("{duplicate} is refused as a duplicate member, got {other:?}"),
        }
        assert_eq!(
            schema(&db, "policy_items"),
            original,
            "{duplicate} is atomic"
        );

        for value in ["0", "-1"] {
            for sql in [
                format!(
                    "ALTER TABLE policy_items ALTER COLUMN embedding SET IVF ({member} = {value})"
                ),
                format!(
                    "CREATE TABLE refused_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) IVF ({member} = {value}))"
                ),
            ] {
                let error = db
                    .execute(&sql, &empty())
                    .expect_err("a non-positive member is refused");
                let index = if sql.starts_with("CREATE") {
                    VectorIndexRef::new("refused_items", "embedding")
                } else {
                    VectorIndexRef::new("policy_items", "embedding")
                };
                assert!(
                    matches!(
                        &error,
                        Error::InvalidVectorPolicyDeclaration { index: refused, issue }
                            if refused == &index && issue.to_string().contains("IVF")
                    ),
                    "{sql} is a typed vector policy refusal naming IVF, got {error:?}"
                );
                assert_eq!(schema(&db, "policy_items"), original, "{sql} is atomic");
                assert!(
                    db.table_meta("refused_items").is_none(),
                    "{sql} publishes no table"
                );
            }
        }
    }
}
