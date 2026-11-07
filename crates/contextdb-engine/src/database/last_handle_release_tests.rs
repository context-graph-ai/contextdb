use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::Database;

#[test]
fn last_handle_release_waits_for_retained_readers_and_identifies_one_store_life() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("release.db");
    let database = Database::open(&path).unwrap();
    let release = database.last_handle_release_for_test();
    let retained_reader = database.read_session(ReadLimits::default()).unwrap();
    assert!(!release.is_released_for_test());
    drop(database);
    assert!(
        !release.is_released_for_test(),
        "a retained reader still owns the store after the writable handle drops"
    );
    drop(retained_reader);
    let first = release.wait_for_release_for_test().unwrap();
    assert!(release.is_released_for_test());

    let replacement = Database::open(&path).expect("one reopen after actual resource release");
    let replacement_release = replacement.last_handle_release_for_test();
    assert!(!replacement_release.is_released_for_test());
    assert_eq!(release.wait_for_release_for_test().unwrap(), first);
    drop(replacement);
    let second = replacement_release.wait_for_release_for_test().unwrap();
    assert_ne!(first.generation, second.generation);
}

#[test]
fn explicit_close_completes_the_same_release_receipt_without_dropping_the_handle() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("close.db");
    let database = Database::open(&path).unwrap();
    let release = database.last_handle_release_for_test();
    database.close().unwrap();
    release.wait_for_release_for_test().unwrap();
    let reopened = Database::open(&path).expect("close receipt follows persistence release");
    drop(reopened);
    drop(database);
}
