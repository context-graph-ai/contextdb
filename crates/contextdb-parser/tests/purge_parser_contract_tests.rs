use contextdb_parser::parse;

#[test]
fn purge_from_where_parses_as_distinct_statement() {
    let statement = parse("PURGE FROM notes WHERE id = $id")
        .expect("PURGE FROM with a DELETE-shaped predicate must parse");
    let rendered = format!("{statement:?}");

    assert!(
        rendered.starts_with("Purge("),
        "PURGE must remain a distinct statement, not ordinary DELETE: {rendered}"
    );
    assert!(
        rendered.contains("notes"),
        "PURGE statement must retain its selected table: {rendered}"
    );
    assert!(
        rendered.contains("where_clause: Some"),
        "PURGE statement must retain its WHERE predicate: {rendered}"
    );
}
