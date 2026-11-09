use super::*;

fn many_table_commit(
    count: usize,
) -> (
    tempfile::TempDir,
    crate::Database,
    contextdb_core::TxId,
    Vec<String>,
) {
    let root = tempfile::tempdir().expect("fixture directory");
    let db = crate::Database::open(root.path().join("held.db")).expect("open the database");
    db.set_maintenance_policy(crate::MaintenancePolicy::CallerDriven);
    let tables: Vec<String> = (0..count)
        .map(|n| format!("held_{n:03}_{}", "x".repeat(60)))
        .collect();
    for table in &tables {
        db.execute(
            &format!("CREATE TABLE {table} (id UUID PRIMARY KEY, payload TEXT NOT NULL)"),
            &std::collections::HashMap::new(),
        )
        .expect("declare one table");
    }
    let tx = db.begin().expect("begin");
    for (n, table) in tables.iter().enumerate() {
        db.execute_in_tx(
            tx,
            &format!(
                "INSERT INTO {table} VALUES ('{}', 'one row')",
                uuid::Uuid::from_u128(0x6e1d << 112 | n as u128)
            ),
            &std::collections::HashMap::new(),
        )
        .expect("stage one row");
    }
    (root, db, tx, tables)
}

const COMMIT_TABLES: usize = 40;

/// A detached Legacy write set has no active transaction left to clean it up.
/// Its panic must refund the same staged rows as an ordinary failed apply,
/// after the real persistence/cache release rather than before cached pages die.
#[cfg(feature = "test-seams")]
#[test]
#[expect(
    clippy::result_large_err,
    reason = "The injected commit returns the allocation-free inline public error."
)]
fn detached_staged_rows_refund_after_panic_and_physical_store_release() {
    let mut ordinary_remaining = None;
    for fault in [HoldFault::Error, HoldFault::Panic] {
        let (_root, db, tx, _) = many_table_commit(COMMIT_TABLES);
        let accountant = db.__accountant_handle_for_test();
        let release = db.last_handle_release_for_test();
        inject_hold_fault(HoldStep::AdmitName, fault, 0);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| db.commit(tx)));
        assert!(!clear_hold_fault(), "the controlled failure fired");
        match fault {
            HoldFault::Error => assert!(matches!(outcome, Ok(Err(_)))),
            HoldFault::Panic => assert!(outcome.is_err()),
            _ => unreachable!(),
        }
        drop(db);
        release
            .wait_for_release_for_test()
            .expect("physical store release");
        let remaining = accountant.usage().used;
        eprintln!("HOLDER_PHYSICAL_RELEASE fault={fault:?} remaining={remaining}");
        if let Some(ordinary) = ordinary_remaining {
            assert_eq!(
                remaining, ordinary,
                "panic retires every detached row invoice"
            );
        } else {
            ordinary_remaining = Some(remaining);
        }
    }
}
