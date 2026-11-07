use contextdb_core::read_contract::ReadFailureKind;
use contextdb_core::{Error, Value};
use contextdb_engine::{Database, ReadSession};
use std::collections::HashMap;

fn assert_erasure(error: Error) {
    assert!(
        matches!(error, Error::ReadFailure(failure) if failure.kind() == ReadFailureKind::AuthoritativeErasure)
    );
}

#[test]
fn an_erased_reader_keeps_its_erasure_answer_when_asking_for_its_owner() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("erased-owner.db");
    let writer = Database::open(&path).unwrap();
    for sql in [
        "CREATE TABLE erased (id INTEGER PRIMARY KEY, payload TEXT)",
        "CREATE TABLE unrelated (id INTEGER PRIMARY KEY, payload TEXT)",
        "INSERT INTO erased VALUES (1, 'secret')",
        "INSERT INTO unrelated VALUES (1, 'kept')",
    ] {
        writer.execute(sql, &HashMap::new()).unwrap();
    }
    writer.close().unwrap();
    let session = ReadSession::open(&path).unwrap();
    session
        .execute("SELECT payload FROM erased", &HashMap::new())
        .unwrap();
    let writer = Database::open(&path).unwrap();
    writer
        .execute(
            "PURGE FROM erased WHERE payload = 'secret'",
            &HashMap::new(),
        )
        .unwrap();
    assert_erasure(
        session
            .execute("SELECT payload FROM erased", &HashMap::new())
            .unwrap_err(),
    );
    assert_eq!(
        session
            .execute("SELECT payload FROM unrelated", &HashMap::new())
            .unwrap()
            .rows(),
        &[vec![Value::Text("kept".to_owned())]]
    );
    assert_erasure(session.request_owner("unused", b"").unwrap_err());
    drop(session);
    writer.close().unwrap();
}
