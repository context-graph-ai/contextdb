use contextdb_parser::ast::{FromItem, JoinType, SelectBody, Statement};
use contextdb_parser::parse;

fn select_body(sql: &str) -> SelectBody {
    match parse(sql) {
        Ok(Statement::Select(select)) => select.body,
        other => panic!("expected a SELECT for {sql:?}, got {other:?}"),
    }
}

#[test]
fn bare_join_parses_to_the_same_ast_as_inner_join() {
    let bare = select_body("SELECT a.id FROM a JOIN b ON a.id = b.id");
    let inner = select_body("SELECT a.id FROM a INNER JOIN b ON a.id = b.id");

    assert_eq!(format!("{bare:?}"), format!("{inner:?}"));
    assert_eq!(bare.joins.len(), 1);
    assert_eq!(bare.joins[0].join_type, JoinType::Inner);
}

#[test]
fn bare_join_keyword_is_case_insensitive() {
    let lower = select_body("select a.id from a join b on a.id = b.id");
    let inner = select_body("SELECT a.id FROM a INNER JOIN b ON a.id = b.id");

    assert_eq!(format!("{lower:?}"), format!("{inner:?}"));
}

#[test]
fn bare_join_keeps_the_from_alias_and_the_join_alias() {
    let body = select_body("SELECT x.id FROM a x JOIN b y ON x.id = y.id");

    assert_eq!(body.joins.len(), 1);
    assert_eq!(body.joins[0].join_type, JoinType::Inner);
    assert_eq!(body.joins[0].table, "b");
    assert_eq!(body.joins[0].alias.as_deref(), Some("y"));
    match &body.from[0] {
        FromItem::Table { name, alias } => {
            assert_eq!(name, "a");
            assert_eq!(alias.as_deref(), Some("x"));
        }
        other => panic!("expected a table FROM item, got {other:?}"),
    }
}

#[test]
fn bare_join_without_any_alias_parses() {
    let body = select_body("SELECT a.id FROM a JOIN b ON a.id = b.id");

    assert_eq!(body.joins[0].table, "b");
    assert_eq!(body.joins[0].alias, None);
    match &body.from[0] {
        FromItem::Table { name, alias } => {
            assert_eq!(name, "a");
            assert_eq!(alias, &None);
        }
        other => panic!("expected a table FROM item, got {other:?}"),
    }
}

#[test]
fn join_keyword_prefix_is_still_a_valid_table_alias() {
    let body = select_body("SELECT joiner.id FROM a joiner JOIN b ON joiner.id = b.id");

    assert_eq!(body.joins[0].join_type, JoinType::Inner);
    match &body.from[0] {
        FromItem::Table { name, alias } => {
            assert_eq!(name, "a");
            assert_eq!(alias.as_deref(), Some("joiner"));
        }
        other => panic!("expected a table FROM item, got {other:?}"),
    }
}

#[test]
fn left_join_still_parses_as_left() {
    let body = select_body("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id");

    assert_eq!(body.joins[0].join_type, JoinType::Left);
}
