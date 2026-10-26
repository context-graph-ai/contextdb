use super::reader_proof_support as fixture;
use contextdb_core::Error;
use contextdb_core::read_contract::{OwnerReadCancellation, ReadLimits};
use contextdb_engine::{Database, ReadSessionOptions};
use std::{collections::HashMap, sync::Arc};

#[test]
fn a_cancelled_cache_statement_keeps_the_previous_declaration() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cancelled-cache-setting.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE small (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.close().unwrap();
    let reader = fixture::open_file(
        &path,
        ReadSessionOptions {
            memory_limit: Some(64 * 1024 * 1024),
            read_cache_share: Some(40),
            limits: ReadLimits {
                memory: 64 * 1024 * 1024,
                ..fixture::limits()
            },
            ..Default::default()
        },
        Arc::new(fixture::Routes::default()),
    );
    let cancellation = OwnerReadCancellation::new();
    cancellation.cancel();
    for sql in ["SET READ_CACHE_SHARE = 0", "SHOW READ_CACHE_SHARE"] {
        assert!(matches!(
            reader.execute_with_cancellation(sql, &HashMap::new(), &cancellation),
            Err(Error::ReadCancelled)
        ));
    }
    assert_eq!(reader.options().read_cache_share, Some(40));
}

#[test]
fn cache_inspection_honours_the_declared_result_byte_limit() {
    use contextdb_core::read_contract::{ReadFailureDetail, ReadFailureLimit};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("bounded-cache-inspection.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE small (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.close().unwrap();
    let reader = fixture::open_file(
        &path,
        ReadSessionOptions {
            memory_limit: Some(64 * 1024 * 1024),
            limits: ReadLimits {
                memory: 64 * 1024 * 1024,
                result_bytes: 1,
                cursor_page_bytes: 1,
                ..fixture::limits()
            },
            ..Default::default()
        },
        Arc::new(fixture::Routes::default()),
    );
    let error = reader
        .execute("SHOW READ_CACHE_SHARE", &HashMap::new())
        .unwrap_err();
    match error {
        Error::ReadFailure(failure) => assert!(
            matches!(failure.detail(), ReadFailureDetail::OwnerLimitExceeded(detail) if detail.limit == ReadFailureLimit::ResultBytes && detail.value == 1)
        ),
        other => panic!("inspection keeps the typed result-byte refusal: {other:?}"),
    }
}

#[test]
fn a_non_integer_unicode_share_keeps_a_valid_inspection_prefix() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unicode-cache-setting.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE small (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.close().unwrap();
    let reader = fixture::open_file(
        &path,
        ReadSessionOptions {
            memory_limit: Some(64 * 1024 * 1024),
            read_cache_share: Some(40),
            limits: ReadLimits {
                memory: 64 * 1024 * 1024,
                ..fixture::limits()
            },
            ..Default::default()
        },
        Arc::new(fixture::Routes::default()),
    );
    let token = format!("'{}'", "🙂".repeat(24));
    let error = reader
        .execute(&format!("SET READ_CACHE_SHARE {token}"), &HashMap::new())
        .unwrap_err();
    assert!(
        matches!(error, Error::InvalidSetting { name: "READ_CACHE_SHARE", value, .. } if value == token)
    );
    let shown = reader
        .execute("SHOW READ_CACHE_SHARE", &HashMap::new())
        .unwrap();
    let column = shown
        .columns()
        .iter()
        .position(|name| name == "last_refusal")
        .unwrap();
    let contextdb_core::Value::Text(prefix) = &shown.rows()[0][column] else {
        panic!("the refused value is text")
    };
    assert!(!prefix.is_empty() && token.starts_with(prefix));
    assert_eq!(reader.options().read_cache_share, Some(40));
}
