//! The serving-cache size a consumer declares is one the engine keeps.
//!
//! `SET VECTOR_SERVING_CACHE` shipped with its grammar, planner and executor
//! arms proven by the parser census and nothing else, so two defects reached a
//! tip: the reader the setting governs refused `SHOW VECTOR_SERVING_CACHE`
//! outright, and an absent setting was read as a declaration of zero bytes,
//! which collapsed every serving cache to one page. Both are leg failures a
//! single test of each leg would have caught, so each leg has one here.

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn shown(db: &Database) -> Value {
    let result = db
        .execute("SHOW VECTOR_SERVING_CACHE", &empty())
        .expect("the declared serving cache size is inspectable");
    assert_eq!(result.columns(), vec!["bytes"]);
    result
        .rows()
        .first()
        .and_then(|row| row.first())
        .cloned()
        .expect("one row, one column")
}

/// Declare, Persist, Inspect and Default, through the surface a consumer uses.
#[test]
fn a_declared_serving_cache_survives_a_reopen_and_clears_on_none() {
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("serving-cache.db");

    let db = Database::open(&path).expect("open the store");
    // Default: nothing declared reads as nothing declared, not as zero.
    assert_eq!(
        shown(&db),
        Value::Null,
        "an undeclared serving cache size reports as absent"
    );
    assert_eq!(db.vector_serving_cache_bytes(), None);

    db.execute("SET VECTOR_SERVING_CACHE '64M'", &empty())
        .expect("declare a serving cache size");
    assert_eq!(shown(&db), Value::Int64(64 * 1024 * 1024));
    db.close().expect("seal the store");
    drop(db);

    let reopened = Database::open(&path).expect("reopen the store");
    assert_eq!(
        shown(&reopened),
        Value::Int64(64 * 1024 * 1024),
        "a declared serving cache size survives a reopen"
    );
    assert_eq!(
        reopened.vector_serving_cache_bytes(),
        Some(64 * 1024 * 1024)
    );

    reopened
        .execute("SET VECTOR_SERVING_CACHE NONE", &empty())
        .expect("clear the declaration");
    assert_eq!(shown(&reopened), Value::Null);
    reopened.close().expect("seal the store");
    drop(reopened);

    let cleared = Database::open(&path).expect("reopen after clearing");
    assert_eq!(
        shown(&cleared),
        Value::Null,
        "clearing the declaration removes the persisted key rather than storing a zero"
    );
    assert_eq!(cleared.vector_serving_cache_bytes(), None);
}

/// A size that cannot serve anything is refused in plain words, whether or not
/// it carries a unit — a bare `0` used to die as a grammar error instead.
#[test]
fn a_serving_cache_of_zero_is_refused_in_plain_words() {
    let root = tempfile::tempdir().expect("scratch directory");
    let db = Database::open(root.path().join("zero.db")).expect("open the store");
    for statement in [
        "SET VECTOR_SERVING_CACHE 0",
        "SET VECTOR_SERVING_CACHE '0K'",
    ] {
        let refusal = db
            .execute(statement, &empty())
            .expect_err("a zero-byte serving cache is refused");
        let text = format!("{refusal}");
        assert!(
            text.contains("at least one byte"),
            "{statement} must say what is wrong in plain words, got {text}"
        );
    }
    assert_eq!(
        shown(&db),
        Value::Null,
        "a refused declaration leaves the setting absent"
    );
}
