use std::path::PathBuf;

use super::Error;

#[test]
fn a_locked_store_names_the_holder_and_the_path() {
    let refusal = Error::DatabaseLocked {
        path: PathBuf::from("./hub"),
    };
    assert_eq!(
        refusal.to_string(),
        "a writer holds the store at ./hub; open a read session to read",
        "the refusal names a writer, no process, and the store, and what a reader does instead"
    );
}

#[test]
fn a_unique_refusal_names_the_table_and_every_column_of_the_rule() {
    let refusal = Error::UniqueViolation {
        table: "t".to_owned(),
        column: "a, b, c".to_owned(),
        key: "1, x, 1".to_owned(),
    };
    assert_eq!(
        refusal.to_string(),
        "unique constraint violation: t (a, b, c) = (1, x, 1)"
    );
}
