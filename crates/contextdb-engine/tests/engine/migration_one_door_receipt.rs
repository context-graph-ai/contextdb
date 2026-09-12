//! The migration door hands back a receipt, never a way back into the store.
//!
//! The defect this pins: the door used to return the temporary `Database` it
//! had built, publicly and writably, and leave the caller to perform the
//! atomic swap. A caller holding that handle could mutate the staged store
//! after its safety fingerprint had been recorded, and the sequence that makes
//! the swap crash-safe lived outside the engine that owns it. What comes back
//! now is a statement of what happened, after everything is published and
//! every handle is released.

use contextdb_engine::database::{
    LegacyMigrationSource, MigrationError, MigrationFailureStage, MigrationReceipt,
};
use std::path::PathBuf;

fn engine_source() -> String {
    std::fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/database.rs"))
        .expect("read the engine source that declares the migration door")
}

/// The receipt is plain data: no field of it can be a handle onto a store.
#[test]
fn the_receipt_carries_counts_and_nothing_that_can_be_written_through() {
    fn assert_plain_data<T: Send + Sync + Clone + std::fmt::Debug + PartialEq + 'static>() {}
    assert_plain_data::<MigrationReceipt>();

    let receipt = MigrationReceipt {
        applied_rows: 3,
        keyless_rows_copied: 1,
        keyless_table_receipts: vec![("notes".to_owned(), 1)],
    };
    // A receipt can be cloned and compared, which a live store handle never
    // could be; this is what makes it safe to hand to a caller.
    assert_eq!(receipt.clone(), receipt);
    assert_eq!(receipt.applied_rows, 3);
    assert_eq!(receipt.keyless_rows_copied, 1);
    assert_eq!(
        receipt.keyless_table_receipts,
        vec![("notes".to_owned(), 1)]
    );

    let source = engine_source();
    let declaration = source
        .split("pub struct MigrationReceipt {")
        .nth(1)
        .and_then(|rest| rest.split('}').next())
        .expect("the receipt is declared in the engine source");
    assert!(
        !declaration.contains("Database"),
        "the receipt must not carry a store handle: {declaration}",
    );
}

/// The door's own signature is the guarantee: it takes the validated source
/// and nothing else, and returns a receipt, so neither the swap nor what gets
/// published can be somebody else's job.
#[test]
fn the_door_takes_only_the_validated_source_and_returns_a_receipt() {
    let source = engine_source();
    let signature = source
        .split("pub fn migrate_in_place(")
        .nth(1)
        .and_then(|rest| rest.split('{').next())
        .expect("the migration door is declared in the engine source");
    assert!(
        signature.contains("MigrationReceipt"),
        "the one door must return a receipt: {signature}",
    );
    for forbidden in [
        "tmp_path",
        "destination",
        "keyless_table_rows",
        "observe",
        "Path",
    ] {
        assert!(
            !signature.contains(forbidden),
            "a caller must not be able to supply {forbidden}: {signature}",
        );
    }

    // What is published, where it is published, and the rows that go with it
    // all come out of the source the door already holds.
    let derived = source
        .split("pub fn migrate_in_place(")
        .nth(1)
        .and_then(|rest| rest.split("\n    /// Close the migration source").next())
        .expect("the migration door body is readable");
    for required in [
        "let destination = self.path.as_path();",
        "self.keyless_table_rows()",
        "migration_temp_path(destination)",
    ] {
        assert!(
            derived.contains(required),
            "the door must derive {required} itself",
        );
    }
    assert!(
        !source.contains("pub struct StagedMigrationReplacement"),
        "the staged replacement handed the caller a writable store and is gone",
    );
    assert!(
        !source.contains("pub fn stage_migration_replacement"),
        "a staging-only door would leave the swap outside the engine",
    );

    // The steps that publish a migration belong to the door, not its caller.
    let door = source
        .split("pub fn migrate_in_place(")
        .nth(1)
        .and_then(|rest| rest.split("\n    /// Close the migration source").next())
        .expect("the migration door body is readable");
    for required in [
        "rename_migration_target(tmp_path, destination)",
        "remove_migration_temporary_companion(&generated_companion)",
        "let publication = self.close();",
        "let released = tmp_db.close();",
    ] {
        assert!(
            door.contains(required),
            "the door must perform {required} itself",
        );
    }

    // The compile-time half of the same statement: this coercion only type-checks
    // while the door's one parameter is the source itself, so a caller has no
    // place to hand in a destination, a target name, rows, or a callback.
    let _: fn(&LegacyMigrationSource) -> Result<MigrationReceipt, MigrationError> =
        LegacyMigrationSource::migrate_in_place;
}

/// Every boundary the door passes, in the order a crash proof walks them.
#[test]
fn the_boundaries_cover_the_whole_migration_in_order() {
    let source = engine_source();
    let declaration = source
        .split("pub enum MigrationBoundary {")
        .nth(1)
        .and_then(|rest| rest.split('}').next())
        .expect("the boundaries are declared in the engine source");
    let ordered: Vec<_> = declaration
        .lines()
        .map(str::trim)
        .filter(|line| line.ends_with(','))
        .map(|line| line.trim_end_matches(',').to_owned())
        .collect();
    assert_eq!(
        ordered,
        vec![
            "TemporaryStoreOpened",
            "TemporaryStoreImported",
            "SourceStoreSealed",
            "TemporaryStoreBuilt",
            "TemporaryStoreDurablyPreparedAndOwned",
            "BeforeAtomicSwap",
            "AfterAtomicSwap",
            "TemporaryCompanionCleaned",
            "BeforeFinalGuardRelease",
        ],
        "the door must reach the swap and its cleanup, not stop at staging",
    );
}

/// A caller renders three different things to an operator depending on what a
/// failure left them holding, so the door says which of the three this is.
#[test]
fn a_failure_says_what_it_left_the_operator_holding() {
    let before_swap = [
        MigrationError::TempStoreOpen(contextdb_core::Error::Other("open".to_owned())),
        MigrationError::RecordReplacementTarget(contextdb_core::Error::Other("record".to_owned())),
        MigrationError::ImportLegacyData(contextdb_core::Error::Other("import".to_owned())),
        MigrationError::KeylessRowsCopy(contextdb_core::Error::Other("keyless".to_owned())),
        MigrationError::DurablyPrepare(contextdb_core::Error::Other("prepare".to_owned())),
    ];
    for failure in before_swap {
        assert_eq!(
            failure.stage(),
            MigrationFailureStage::BeforeSwap,
            "everything before the swap leaves the original store usable and no residue: \
             {failure:?}",
        );
    }
    assert_eq!(
        MigrationError::AtomicSwap(std::io::Error::other("swap")).stage(),
        MigrationFailureStage::AtSwap,
    );
    assert_eq!(
        MigrationError::PublishReplacement(contextdb_core::Error::Other("publish".to_owned()))
            .stage(),
        MigrationFailureStage::AfterSwap,
    );
}
