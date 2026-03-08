use super::helpers::*;
use contextdb_core::Error;

#[test]
fn anti_recursive_cte_rejected() {
    let db = setup_ontology_db();
    let err = db.execute("WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t", &std::collections::HashMap::new()).unwrap_err();
    assert!(matches!(err, Error::RecursiveCteNotSupported));
}

#[test]
fn anti_window_rejected() {
    let db = setup_ontology_db();
    let err = db.execute("SELECT ROW_NUMBER() OVER (PARTITION BY x) FROM t", &std::collections::HashMap::new()).unwrap_err();
    assert!(matches!(err, Error::WindowFunctionNotSupported));
}
