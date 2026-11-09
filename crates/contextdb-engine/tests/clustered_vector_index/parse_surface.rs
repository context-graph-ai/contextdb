//! A schema author declares a vector column's search target as
//! `SEARCH (TARGET_ACCURACY, EXPECTED_VECTORS)`, the accuracy a decimal
//! percent with at most two fractional digits, and may override the clustered index's layout with
//! `IVF (LISTS, PROBES, REFINE_FACTOR)`, the refine factor a decimal
//! multiplier of LIMIT of at least 1.00; a query may raise its own target
//! with `USE VECTOR INDEXED (TARGET_ACCURACY = n)`. Both groups render
//! canonically, round-trip through their own rendering, and refuse invalid
//! members.

use super::support::{empty, open_caller_driven, params, unit_vector};
use contextdb_core::{Error, Value, VectorIndexRef};
use contextdb_engine::Database;
use contextdb_engine::cli_render::render_table_meta;

fn schema(db: &Database, table: &str) -> String {
    let meta = db
        .table_meta(table)
        .unwrap_or_else(|| panic!("table metadata exists for {table}"));
    render_table_meta(table, &meta)
}

#[test]
fn all_default_groups_are_the_same_declaration_as_silence() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE silent_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE INDEXED)",
        &empty(),
    )
    .expect("create the silent declaration");
    db.execute(
        "CREATE TABLE default_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE INDEXED SEARCH (TARGET_ACCURACY = DEFAULT, EXPECTED_VECTORS = DEFAULT) IVF (LISTS = DEFAULT, PROBES = DEFAULT, REFINE_FACTOR = DEFAULT))",
        &empty(),
    )
    .expect("all-DEFAULT SEARCH and IVF groups parse");
    assert_eq!(
        schema(&db, "default_items"),
        schema(&db, "silent_items").replace("silent_items", "default_items"),
        "DEFAULT members declare nothing, so both groups render as silence"
    );
}

#[test]
fn explicit_members_render_canonically_and_round_trip() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE declared_items (id INTEGER PRIMARY KEY, scope_id TEXT NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_id) SEARCH_MODE AUTO SEARCH (EXPECTED_VECTORS = 1000000, TARGET_ACCURACY = 99) IVF (REFINE_FACTOR = 2.5, LISTS = 64, PROBES = 8))",
        &empty(),
    )
    .expect("CREATE accepts explicit members in any order");
    let declared = schema(&db, "declared_items");
    assert!(
        declared.contains("SEARCH (TARGET_ACCURACY = 99, EXPECTED_VECTORS = 1000000)"),
        "SEARCH renders its members in the fixed order: {declared}"
    );
    assert!(
        declared.contains("IVF (LISTS = 64, PROBES = 8, REFINE_FACTOR = 2.5"),
        "IVF renders its members in canonical order, the factor as decimal text: {declared}"
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

    db.execute(
        "CREATE TABLE partial_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) SEARCH (EXPECTED_VECTORS = 5000) IVF (REFINE_FACTOR = 1.00))",
        &empty(),
    )
    .expect("a partial SEARCH group and the smallest refine factor parse");
    let partial = schema(&db, "partial_items");
    assert!(
        partial.contains("SEARCH (TARGET_ACCURACY = DEFAULT, EXPECTED_VECTORS = 5000)"),
        "once any member is explicit, an omitted member renders as DEFAULT: {partial}"
    );
    assert!(
        partial.contains("IVF (REFINE_FACTOR = 1") && !partial.contains("LISTS"),
        "a partial IVF group renders only its declared member: {partial}"
    );
    let replay = Database::open_memory();
    replay
        .execute(&partial, &empty())
        .expect("the partial rendering is executable");
    assert_eq!(
        schema(&replay, "partial_items"),
        partial,
        "the partial rendering is a fixpoint"
    );

    for (index, (declared, rendered)) in [("95.5", "95.5"), ("99.50", "99.5"), ("99.05", "99.05")]
        .into_iter()
        .enumerate()
    {
        let table = format!("decimal_items_{index}");
        db.execute(
            &format!(
                "CREATE TABLE {table} (id INTEGER PRIMARY KEY, embedding VECTOR(3) SEARCH (TARGET_ACCURACY = {declared}))"
            ),
            &empty(),
        )
        .expect("a decimal percent with up to two fractional digits is accepted");
        let schema_text = schema(&db, &table);
        assert!(
            schema_text.contains(&format!(
                "SEARCH (TARGET_ACCURACY = {rendered}, EXPECTED_VECTORS = DEFAULT)"
            )),
            "TARGET_ACCURACY = {declared} renders without trailing zeros as {rendered}: {schema_text}"
        );
        let replay = Database::open_memory();
        replay
            .execute(&schema_text, &empty())
            .expect("the decimal rendering is executable");
        assert_eq!(
            schema(&replay, &table),
            schema_text,
            "the decimal rendering is a fixpoint"
        );
    }

    db.execute("CREATE TABLE add_items (id INTEGER PRIMARY KEY)", &empty())
        .expect("create the add-column target");
    db.execute(
        "ALTER TABLE add_items ADD COLUMN embedding VECTOR(3) WITH (quantization = 'SQ8') IVF (PROBES = 4)",
        &empty(),
    )
    .expect("ADD COLUMN accepts a partial IVF group");
    let added = schema(&db, "add_items");
    assert!(
        added.contains("IVF (PROBES = 4)")
            && !added.contains("LISTS")
            && !added.contains("REFINE_FACTOR"),
        "a partial IVF group renders only its declared members: {added}"
    );
}

#[test]
fn the_calibration_depth_is_not_an_ivf_member() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE listed_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) IVF (LISTS = 8))",
        &empty(),
    )
    .expect("a valid IVF member is accepted, so the refusals below are about the member");
    for sql in [
        "CREATE TABLE depth_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) IVF (CALIBRATION_MAX_K = 100))",
        "CREATE TABLE depth_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) IVF (LISTS = 8, MAX_K = 100))",
    ] {
        assert!(
            matches!(db.execute(sql, &empty()), Err(Error::ParseError(_))),
            "{sql}: the calibration depth is a policy value, not an IVF member"
        );
        assert!(
            db.table_meta("depth_items").is_none(),
            "{sql} publishes no table"
        );
    }
}

#[test]
fn alters_change_only_the_members_they_name() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE policy_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) SEARCH (TARGET_ACCURACY = 95, EXPECTED_VECTORS = 20000) IVF (LISTS = 32, PROBES = 4, REFINE_FACTOR = 2.0))",
        &empty(),
    )
    .expect("declare explicit groups");

    db.execute(
        "ALTER TABLE policy_items ALTER COLUMN embedding SET IVF (PROBES = 16, REFINE_FACTOR = 3.5)",
        &empty(),
    )
    .expect("a partial IVF ALTER keeps omitted members");
    let partial = schema(&db, "policy_items");
    assert!(
        partial.contains("IVF (LISTS = 32, PROBES = 16, REFINE_FACTOR = 3.5"),
        "omitted IVF members retain their declaration: {partial}"
    );
    db.execute(
        "ALTER TABLE policy_items ALTER COLUMN embedding SET IVF (PROBES = DEFAULT)",
        &empty(),
    )
    .expect("an IVF member DEFAULT clears only that member");
    let member_default = schema(&db, "policy_items");
    assert!(
        member_default.contains("IVF (LISTS = 32, REFINE_FACTOR = 3.5")
            && !member_default.contains("PROBES"),
        "member DEFAULT leaves the other IVF members intact: {member_default}"
    );

    db.execute(
        "ALTER TABLE policy_items ALTER COLUMN embedding SET SEARCH (TARGET_ACCURACY = 90)",
        &empty(),
    )
    .expect("a partial SEARCH ALTER keeps omitted members");
    let searched = schema(&db, "policy_items");
    assert!(
        searched.contains("SEARCH (TARGET_ACCURACY = 90, EXPECTED_VECTORS = 20000)"),
        "an omitted SEARCH member retains its declaration: {searched}"
    );

    db.execute(
        "ALTER TABLE policy_items ALTER COLUMN embedding SET SEARCH DEFAULT",
        &empty(),
    )
    .expect("SET SEARCH DEFAULT clears the whole group");
    db.execute(
        "ALTER TABLE policy_items ALTER COLUMN embedding SET IVF DEFAULT",
        &empty(),
    )
    .expect("SET IVF DEFAULT clears the whole group");
    let cleared = schema(&db, "policy_items");
    assert!(
        !cleared.contains("SEARCH (") && !cleared.contains("IVF"),
        "cleared groups render as silence: {cleared}"
    );
}

/// A typed vector policy refusal naming the member; a value the grammar
/// cannot read as that member's type may instead be a parse error naming it.
fn assert_member_refused(
    result: contextdb_core::Result<contextdb_engine::QueryResult>,
    index: &VectorIndexRef,
    member: &str,
    sql: &str,
    parse_allowed: bool,
) {
    match result {
        Err(Error::InvalidVectorPolicyDeclaration {
            index: refused,
            issue,
        }) if &refused == index && issue.to_string().contains(member) => {}
        Err(Error::ParseError(message)) if parse_allowed && message.contains(member) => {}
        other => panic!("{sql} is a typed refusal naming {member}, got {other:?}"),
    }
}

/// (group, member, two valid values, out-of-range values, values of the wrong type)
type MemberCases<'a> = (&'a str, &'a str, [&'a str; 2], &'a [&'a str], &'a [&'a str]);

#[test]
fn duplicate_and_out_of_range_members_are_refused_without_changing_the_schema() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE policy_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) SEARCH (TARGET_ACCURACY = 95, EXPECTED_VECTORS = 20000) IVF (LISTS = 32, PROBES = 4, REFINE_FACTOR = 2.0))",
        &empty(),
    )
    .expect("declare the groups every refusal must preserve");
    let original = schema(&db, "policy_items");
    let altered = VectorIndexRef::new("policy_items", "embedding");
    let created = VectorIndexRef::new("refused_items", "embedding");

    let members: [MemberCases; 5] = [
        ("IVF", "LISTS", ["8", "9"], &["0", "-1"], &[]),
        ("IVF", "PROBES", ["2", "3"], &["0", "-1"], &[]),
        (
            "IVF",
            "REFINE_FACTOR",
            ["1.5", "2.5"],
            &["0.99", "0", "-1.5"],
            &[],
        ),
        (
            "SEARCH",
            "EXPECTED_VECTORS",
            ["1000", "2000"],
            &["0", "-1"],
            &[],
        ),
        (
            "SEARCH",
            "TARGET_ACCURACY",
            ["90", "80.5"],
            &["0", "-1", "101"],
            &["95.555"],
        ),
    ];
    for (group, member, [first, second], out_of_range, wrong_type) in members {
        let duplicate = format!(
            "ALTER TABLE policy_items ALTER COLUMN embedding SET {group} ({member} = {first}, {member} = {second})"
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

        for (values, parse_allowed) in [(out_of_range, false), (wrong_type, true)] {
            for value in values {
                for (sql, index) in [
                    (
                        format!(
                            "ALTER TABLE policy_items ALTER COLUMN embedding SET {group} ({member} = {value})"
                        ),
                        &altered,
                    ),
                    (
                        format!(
                            "CREATE TABLE refused_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) {group} ({member} = {value}))"
                        ),
                        &created,
                    ),
                ] {
                    assert_member_refused(
                        db.execute(&sql, &empty()),
                        index,
                        member,
                        &sql,
                        parse_allowed,
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
}

/// 200 committed rows under the derived policy, with no index build.
fn target_items(db: &Database) {
    db.execute(
        "CREATE TABLE target_items (id INTEGER PRIMARY KEY, embedding VECTOR(8))",
        &empty(),
    )
    .expect("declare a vector column with the derived policy");
    for id in 0..200_i64 {
        db.execute(
            "INSERT INTO target_items (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                ("embedding", Value::Vector(unit_vector(id as u64, 8))),
            ]),
        )
        .expect("commit one row");
    }
}

#[test]
fn a_query_raises_its_own_target_accuracy_and_an_out_of_range_target_is_refused() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_caller_driven(&root.path().join("query-target.db"));
    target_items(&db);
    let query = params([("query", Value::Vector(unit_vector(7, 8)))]);
    for value in ["0", "101", "-1", "95.555"] {
        let sql = format!(
            "SELECT id FROM target_items ORDER BY embedding <=> $query USE VECTOR INDEXED (TARGET_ACCURACY = {value}) LIMIT 5"
        );
        let error = db
            .execute(&sql, &query)
            .expect_err("an out-of-range query target is refused");
        assert!(
            !matches!(error, Error::Other(_)) && error.to_string().contains("TARGET_ACCURACY"),
            "{sql} is refused with a typed error naming TARGET_ACCURACY, got {error:?}"
        );
    }
}
