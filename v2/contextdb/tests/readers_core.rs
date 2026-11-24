//! The shared reader core refuses closed writer handles. Extends store.rs's close case.
mod support;
use contextdb::Error;
#[test]
fn every_writer_read_call_refuses_a_closed_handle() {
    let db = support::open(&support::folder("closed-read-core"));
    support::declare(&db);
    db.close().unwrap();
    for error in [
        db.execute("SELECT 1", &contextdb::params! {}).err(),
        db.memory_limit().err(),
        db.memory_used().err(),
        db.table_names().err(),
        db.committed_watermark().err(),
        db.declaration("t").err(),
    ] {
        assert!(matches!(error, Some(Error::DatabaseClosed)));
    }
    let error = support::refusal(&db, "SELEC");
    assert!(
        matches!(error, Error::DatabaseClosed),
        "a closed handle answers DatabaseClosed before it parses: {error:?}"
    );
}
