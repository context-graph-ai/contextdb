use std::path::PathBuf;

use super::Error;

#[test]
fn a_locked_store_names_the_holder_the_path_and_how_to_read() {
    let refusal = Error::DatabaseLocked {
        holder_pid: 3_138_758,
        path: PathBuf::from("./hub"),
    };
    assert_eq!(
        refusal.to_string(),
        "another session holds this store for writing: process 3138758 holds ./hub; open a \
         read session to read it"
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
