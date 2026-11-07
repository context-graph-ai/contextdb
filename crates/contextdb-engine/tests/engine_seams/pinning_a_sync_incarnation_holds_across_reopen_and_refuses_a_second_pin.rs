//! A fixture can pin a store's sync incarnation before the first sync, and
//! only then: the pin survives a reopen and a second pin is refused.
use contextdb_core::{Incarnation, TenantId};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::path::Path;

fn open(path: &Path) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            memory_limit: Some(64 << 20),
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap()
}

#[test]
fn pinning_a_sync_incarnation_holds_across_reopen_and_refuses_a_second_pin() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pinned.redb");
    let tenant = TenantId::from("pinned-tenant");
    let pinned = Incarnation(0x5eed_0000_0000_0000_0000_0000_0000_00aa);

    let db = open(&path);
    db.pin_sync_incarnation_for_test(&tenant, pinned).unwrap();
    assert_eq!(db.sync_incarnation_for_test(&tenant).unwrap(), pinned);
    let refused = db
        .pin_sync_incarnation_for_test(&tenant, Incarnation(1))
        .expect_err("a second pin must not replace a live incarnation");
    assert!(
        refused.to_string().contains("already exists"),
        "refusal names the existing incarnation: {refused}"
    );
    assert_eq!(db.sync_incarnation_for_test(&tenant).unwrap(), pinned);
    db.close().unwrap();
    drop(db);

    let db = open(&path);
    assert_eq!(
        db.sync_incarnation_for_test(&tenant).unwrap(),
        pinned,
        "the pin is the persisted incarnation, not a handle cache entry"
    );
    assert!(
        db.pin_sync_incarnation_for_test(&tenant, Incarnation(2))
            .is_err(),
        "a reopened store already holds an incarnation"
    );
    db.close().unwrap();

    let minted = open(&directory.path().join("minted.redb"));
    let first = minted.sync_incarnation_for_test(&tenant).unwrap();
    assert!(
        minted
            .pin_sync_incarnation_for_test(&tenant, pinned)
            .is_err(),
        "a minted incarnation cannot be pinned over"
    );
    assert_eq!(minted.sync_incarnation_for_test(&tenant).unwrap(), first);
    minted.close().unwrap();
}
