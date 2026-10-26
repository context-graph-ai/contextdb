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
