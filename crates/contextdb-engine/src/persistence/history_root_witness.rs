//! Producer-authored historical roots. No history or catalog is enumerated.
use super::*;

const WITNESSES: TableDefinition<&str, &[u8]> = TableDefinition::new("__history_root_witness_v1");
const RECORD_BYTES: usize = 50;

fn protected(name: &str) -> bool {
    matches!(name, "row_graph_fwd" | "row_graph_rev")
        || name == "__authored_row_bodies"
        || name == "ddl_directions_by_table"
        || name.starts_with("rel_")
        || name.strip_prefix("__rel_index_").is_some_and(|suffix| {
            suffix.len() == 64 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
        || name == READ_LINEAGE_CONFIG_TABLE.name()
}
fn corruption(name: &str, reason: &str) -> Error {
    Error::StoreCorrupted {
        path: name.strip_prefix("rel_").unwrap_or(name).to_owned(),
        reason: format!(
            "historical root witness {reason}; recreate a store without producer witnesses"
        ),
    }
}
fn record(root: Option<[u8; 32]>, incarnation: [u8; 16]) -> [u8; RECORD_BYTES] {
    let mut bytes = [0; RECORD_BYTES];
    bytes[0] = 1;
    bytes[1] = u8::from(root.is_some());
    bytes[2..18].copy_from_slice(&incarnation);
    if let Some(root) = root {
        bytes[18..].copy_from_slice(&root);
    }
    bytes
}
// Authenticate every byte before using or replacing an existing witness.
fn valid_record(bytes: &[u8]) -> bool {
    if bytes.len() != RECORD_BYTES || bytes[0] != 1 || bytes[2..18].iter().all(|b| *b == 0) {
        return false;
    }
    match bytes[1] {
        0 => bytes[18..].iter().all(|b| *b == 0),
        1 => {
            // A nonempty root needs a physical page and a positive entry count.
            bytes[18..26].iter().any(|b| *b != 0)
                && u64::from_le_bytes(bytes[42..50].try_into().unwrap()) != 0
        }
        _ => false,
    }
}
fn witnessed_length(bytes: &[u8]) -> Option<u64> {
    valid_record(bytes).then(|| {
        if bytes[1] == 0 {
            0
        } else {
            u64::from_le_bytes(bytes[42..50].try_into().unwrap())
        }
    })
}
fn matches(bytes: &[u8], root: Option<[u8; 32]>) -> bool {
    valid_record(bytes)
        && bytes[1] == u8::from(root.is_some())
        && match root {
            Some(root) => bytes[18..] == root,
            None => bytes[18..].iter().all(|byte| *byte == 0),
        }
}
impl RedbPersistence {
    pub(super) fn begin_history_write(db: &redb::Database) -> Result<redb::WriteTransaction> {
        let write = db.begin_write().map_err(Self::storage_error)?;
        if !write.table_exists(WITNESSES).map_err(Self::storage_error)? {
            return Err(corruption(WITNESSES.name(), "metadata is absent"));
        }
        super::erasure_frontiers::verify_in_write(&write)?;
        Ok(write.with_table_root_hook(Self::historical_root_event))
    }
    pub(super) fn begin_history_write_funded(
        original: &crate::memory_accounting::OwnedMemoryReservation, db: &redb::Database,
    ) -> crate::funded_error::OriginalResult<redb::WriteTransaction> {
        let write = db.begin_write().map_err(|error| Self::storage_error_funded(original, error))?;
        if !write.table_exists(WITNESSES).map_err(|error| Self::storage_error_funded(original, error))? {
            // The witness formatter and erasure verifier remain explicit
            // producer gaps; neither is retrospectively charged to Original.
            return Err(corruption(WITNESSES.name(), "metadata is absent").into());
        }
        super::erasure_frontiers::verify_in_write(&write)?;
        Ok(write.with_table_root_hook(Self::historical_root_event))
    }
    pub(super) fn historical_root_event(
        write: &redb::WriteTransaction,
        name: &str,
        event: redb::TableRootEvent,
    ) -> std::result::Result<(), redb::StorageError> {
        if !protected(name) {
            return Ok(());
        }
        #[cfg(any(test, feature = "test-seams"))]
        content_identity::version_manifest::commit_row_work::count_protected_root_event();
        Self::historical_root_event_inner(write, name, event)
            .map_err(|error| redb::StorageError::AdmissionRefused(Box::new(error)))
    }
    fn historical_root_event_inner(
        write: &redb::WriteTransaction,
        name: &str,
        event: redb::TableRootEvent,
    ) -> Result<()> {
        use redb::TableRootEvent::*;
        let physical = match event {
            AfterClose | AfterRelocate => write.normal_table_root(name),
            _ => write.normal_table_root_current(name),
        }
        .map_err(Self::storage_error)?;
        let mut witnesses = write.open_table(WITNESSES).map_err(Self::storage_error)?;
        let previous = witnesses.get(name).map_err(Self::storage_error)?;
        match event {
            BeforeOpen | BeforeDelete | BeforeRelocate => {
                match (physical, previous.as_ref()) {
                    (None, None) => (), // A new physical table, never compatibility for an existing one.
                    (Some(root), Some(previous)) if matches(previous.value(), root) => (),
                    _ => {
                        return Err(corruption(
                            name,
                            "does not match the pre-mutation committed root",
                        ));
                    }
                }
            }
            AfterClose | AfterRelocate => {
                let root = physical.ok_or_else(|| {
                    corruption(name, "physical table is absent after publication")
                })?;
                let incarnation = if let Some(previous) = previous.as_ref() {
                    if !valid_record(previous.value()) {
                        return Err(corruption(name, "record is malformed"));
                    }
                    previous.value()[2..18]
                        .try_into()
                        .expect("validated fixed record")
                } else {
                    *uuid::Uuid::new_v4().as_bytes()
                };
                let encoded = record(root, incarnation);
                if previous.as_ref().is_some_and(|old| old.value() == encoded) {
                    return Ok(());
                }
                drop(previous);
                witnesses
                    .insert(name, encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            AfterDelete => {
                drop(previous);
                witnesses.remove(name).map_err(Self::storage_error)?;
            }
        }
        Ok(())
    }
    /// A relation's committed row count, as its producer witness captured it
    /// when the relation last closed: one witness lookup, nothing enumerated.
    /// `None` when the relation has never been witnessed.
    pub(super) fn witnessed_row_count_in_read(
        read: &redb::ReadTransaction,
        name: &str,
    ) -> Result<Option<u64>> {
        let witnesses = read.open_table(WITNESSES).map_err(|error| match error {
            redb::TableError::TableDoesNotExist(_) => {
                corruption(WITNESSES.name(), "metadata is absent")
            }
            other => Self::storage_error(other),
        })?;
        let Some(record) = witnesses.get(name).map_err(Self::storage_error)? else {
            return Ok(None);
        };
        witnessed_length(record.value())
            .map(Some)
            .ok_or_else(|| corruption(name, "record is malformed"))
    }
    /// [`Self::witnessed_row_count_in_read`] inside a write transaction: the
    /// count as of the relation's last close in this transaction or before.
    pub(super) fn witnessed_row_count_in_write(
        write: &redb::WriteTransaction,
        name: &str,
    ) -> Result<Option<u64>> {
        let witnesses = write.open_table(WITNESSES).map_err(Self::storage_error)?;
        let Some(record) = witnesses.get(name).map_err(Self::storage_error)? else {
            return Ok(None);
        };
        witnessed_length(record.value())
            .map(Some)
            .ok_or_else(|| corruption(name, "record is malformed"))
    }
    pub(super) fn initialize_history_witnesses(write: &redb::WriteTransaction) -> Result<()> {
        write.open_table(WITNESSES).map_err(Self::storage_error)?;
        Ok(())
    }
    pub(super) fn check_history_witness_schema(read: &redb::ReadTransaction) -> Result<()> {
        read.open_table(WITNESSES).map_err(|error| match error {
            redb::TableError::TableDoesNotExist(_) => {
                corruption(WITNESSES.name(), "metadata is absent")
            }
            other => Self::storage_source_error(WITNESSES.name(), other),
        })?;
        Ok(())
    }
    /// For a read that asks only the canonical tree of an index: whether the
    /// index table's physical root still matches its producer witness. An
    /// absent or malformed witness, and a storage failure, still refuse.
    pub(super) fn verify_history_root_canonical(
        read: &redb::ReadTransaction,
        name: &str,
    ) -> Result<bool> {
        if !protected(name) {
            return Ok(true);
        }
        let map_error = |error: redb::Error| match Self::storage_error(error) {
            Error::StoreCorrupted { reason, .. } => corruption(name, &reason),
            other => other,
        };
        let physical = read
            .normal_table_root(name)
            .map_err(|error| map_error(error.into()))?;
        let witnesses = read.open_table(WITNESSES).map_err(|error| match error {
            redb::TableError::TableDoesNotExist(_) => corruption(name, "metadata is absent"),
            other => map_error(other.into()),
        })?;
        let witness = witnesses
            .get(name)
            .map_err(|error| map_error(error.into()))?;
        match (physical, witness) {
            (Some(root), Some(witness)) => {
                let bytes = witness.value();
                if !valid_record(bytes) {
                    return Err(corruption(name, "record is malformed"));
                }
                Ok(matches(bytes, root))
            }
            _ => Err(corruption(
                name,
                "does not match the captured historical root",
            )),
        }
    }

    pub(super) fn verify_history_root(read: &redb::ReadTransaction, name: &str) -> Result<()> {
        if !protected(name) {
            return Ok(());
        }
        #[cfg(any(test, feature = "test-seams"))]
        if name.starts_with("__rel_index_") {
            STRICT_INDEX_ROOT_CHECKS.with(|count| count.set(count.get() + 1));
        }
        let map_error = |error: redb::Error| match Self::storage_error(error) {
            Error::StoreCorrupted { reason, .. } => corruption(name, &reason),
            other => other,
        };
        let physical = read
            .normal_table_root(name)
            .map_err(|error| map_error(error.into()))?;
        let witnesses = read.open_table(WITNESSES).map_err(|error| match error {
            redb::TableError::TableDoesNotExist(_) => corruption(name, "metadata is absent"),
            other => map_error(other.into()),
        })?;
        let witness = witnesses
            .get(name)
            .map_err(|error| map_error(error.into()))?;
        match (physical, witness) {
            (Some(root), Some(witness)) if matches(witness.value(), root) => Ok(()),
            _ => Err(corruption(
                name,
                "does not match the captured historical root",
            )),
        }
    }
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    /// Strict index-table root checks on this thread. Every physical index
    /// cursor, ordered window and maintained group passes one.
    static STRICT_INDEX_ROOT_CHECKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn strict_index_root_checks_for_test() -> u64 {
    STRICT_INDEX_ROOT_CHECKS.with(std::cell::Cell::get)
}

/// Index tables whose physical root was found to differ from its producer
/// witness by a canonical-only read, per store. Bounded: one entry per
/// damaged index, cleared when a later canonical check of it matches.
static OBSERVED_INDEX_ROOT_DAMAGE: parking_lot::Mutex<
    std::collections::BTreeMap<std::path::PathBuf, std::collections::BTreeSet<(String, String)>>,
> = parking_lot::Mutex::new(std::collections::BTreeMap::new());

pub(crate) fn note_index_root_damage(
    path: &std::path::Path,
    table: &str,
    index: &str,
    damaged: bool,
) {
    let mut observed = OBSERVED_INDEX_ROOT_DAMAGE.lock();
    if damaged {
        observed
            .entry(path.to_path_buf())
            .or_default()
            .insert((table.to_owned(), index.to_owned()));
    } else if let Some(indexes) = observed.get_mut(path) {
        indexes.remove(&(table.to_owned(), index.to_owned()));
        if indexes.is_empty() {
            observed.remove(path);
        }
    }
}

pub(crate) fn observed_index_root_damage(path: &std::path::Path) -> Vec<(String, String)> {
    OBSERVED_INDEX_ROOT_DAMAGE
        .lock()
        .get(path)
        .map(|indexes| indexes.iter().cloned().collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn seeded(path: &std::path::Path) -> redb::Database {
        let db = redb::Database::create(path).unwrap();
        RedbPersistence::write_format_marker(
            &db,
            &mut super::super::content_identity::ProofAdmission::unaccounted(),
        )
        .unwrap();
        let write = db.begin_write().unwrap();
        let write = write.with_table_root_hook(RedbPersistence::historical_root_event);
        write
            .open_table(TableDefinition::<&[u8], &[u8]>::new("rel_fixture"))
            .unwrap()
            .insert(&b"key"[..], &b"old"[..])
            .unwrap();
        write.commit().unwrap();
        db
    }
    fn verify(db: &redb::Database) -> Result<()> {
        RedbPersistence::verify_history_root(&db.begin_read().unwrap(), "rel_fixture")
    }
    #[test]
    fn committed_bypass_is_refused_and_later_producer_cannot_bless_it() {
        let dir = tempfile::tempdir().unwrap();
        let db = seeded(&dir.path().join("witness.db"));
        {
            let write = RedbPersistence::begin_history_write(&db).unwrap();
            write
                .open_table(TableDefinition::<u64, &[u8]>::new("pending_metadata"))
                .unwrap()
                .insert(1, [4u8; 8192].as_slice())
                .unwrap();
            let touched = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let sink = touched.clone();
            let table = redb::commit_work::observe_root_work_for_test(
                std::sync::Arc::new(move |event| {
                    if matches!(
                        event.kind,
                        redb::commit_work::RootWorkKindForTest::FinalizeBegin
                            | redb::commit_work::RootWorkKindForTest::ChecksumPage
                            | redb::commit_work::RootWorkKindForTest::PublishBegin
                    ) {
                        sink.lock()
                            .unwrap()
                            .push((event.table.to_owned(), event.kind));
                    }
                    Ok(())
                }),
                || write.open_table(TableDefinition::<&[u8], &[u8]>::new("rel_fixture")),
            )
            .unwrap();
            let work = touched.lock().unwrap().clone();
            assert!(
                work.is_empty(),
                "BeforeOpen must only inspect the existing header: {work:?}"
            );
            drop(table);
            write.commit().unwrap();
        }
        let held = db.begin_read().unwrap();
        let raw = db.begin_write().unwrap();
        raw.open_table(TableDefinition::<&[u8], &[u8]>::new("rel_fixture"))
            .unwrap()
            .insert(&b"extra"[..], &b"untrusted"[..])
            .unwrap();
        raw.commit().unwrap();
        assert!(matches!(verify(&db), Err(Error::StoreCorrupted { .. })));
        assert!(RedbPersistence::verify_history_root(&held, "rel_fixture").is_ok());
        let producer = RedbPersistence::begin_history_write(&db).unwrap();
        assert!(
            producer
                .open_table(TableDefinition::<&[u8], &[u8]>::new("rel_fixture"))
                .is_err()
        );
        drop(producer);
        assert!(matches!(verify(&db), Err(Error::StoreCorrupted { .. })));
    }
    #[test]
    fn aborted_publication_keeps_old_root_and_recreation_changes_incarnation() {
        let dir = tempfile::tempdir().unwrap();
        let db = seeded(&dir.path().join("witness.db"));
        let inserts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = inserts.clone();
        let guard = db.observe_table_writes_for_test(std::sync::Arc::new(move |event| {
            if event.table == WITNESSES.name()
                && event.operation == redb::TableWriteOperationForTest::Insert
            {
                observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }));
        let write = RedbPersistence::begin_history_write(&db).unwrap();
        for _ in 0..3 {
            drop(
                write
                    .open_table(TableDefinition::<&[u8], &[u8]>::new("rel_fixture"))
                    .unwrap(),
            );
        }
        write.commit().unwrap();
        drop(guard);
        assert_eq!(
            inserts.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "unchanged protected-table reopens must not reauthor producer witnesses"
        );
        let incarnation = {
            let read = db.begin_read().unwrap();
            read.open_table(WITNESSES)
                .unwrap()
                .get("rel_fixture")
                .unwrap()
                .unwrap()
                .value()[2..18]
                .to_vec()
        };
        {
            let write = RedbPersistence::begin_history_write(&db).unwrap();
            write
                .open_table(TableDefinition::<&[u8], &[u8]>::new("rel_fixture"))
                .unwrap()
                .insert(&b"key"[..], &b"aborted"[..])
                .unwrap();
        }
        assert!(verify(&db).is_ok());
        let write = RedbPersistence::begin_history_write(&db).unwrap();
        write
            .delete_table(TableDefinition::<&[u8], &[u8]>::new("rel_fixture"))
            .unwrap();
        write
            .open_table(TableDefinition::<&[u8], &[u8]>::new("rel_fixture"))
            .unwrap();
        write.commit().unwrap();
        assert!(verify(&db).is_ok());
        let read = db.begin_read().unwrap();
        let witnesses = read.open_table(WITNESSES).unwrap();
        let recreated = witnesses.get("rel_fixture").unwrap().unwrap();
        assert_ne!(recreated.value()[2..18], incarnation);
        assert_eq!(
            recreated.value()[1],
            0,
            "empty is an explicit valid root state"
        );
    }
    #[test]
    fn absent_or_malformed_witness_is_never_bootstrapped() {
        let dir = tempfile::tempdir().unwrap();
        let db = seeded(&dir.path().join("witness.db"));
        for malformed in [None, Some(&b"bad"[..])] {
            let raw = db.begin_write().unwrap();
            {
                let mut witnesses = raw.open_table(WITNESSES).unwrap();
                if let Some(value) = malformed {
                    witnesses.insert("rel_fixture", value).unwrap();
                } else {
                    witnesses.remove("rel_fixture").unwrap();
                }
            }
            raw.commit().unwrap();
            assert!(matches!(verify(&db), Err(Error::StoreCorrupted { .. })));
            let producer = RedbPersistence::begin_history_write(&db).unwrap();
            assert!(
                producer
                    .open_table(TableDefinition::<&[u8], &[u8]>::new("rel_fixture"))
                    .is_err()
            );
        }
        let raw = db.begin_write().unwrap();
        raw.delete_table(WITNESSES).unwrap();
        raw.commit().unwrap();
        assert!(RedbPersistence::begin_history_write(&db).is_err());
        assert!(verify(&db).is_err());
        // A producer that began against valid evidence must not bless a
        // malformed existence flag introduced before its close callback.
        let mut blessed = Vec::new();
        for (flag, clear_root) in [(0u8, false), (2, false), (255, false), (1, true)] {
            let db = seeded(&dir.path().join(format!("flag-{flag}-{clear_root}.db")));
            let write = RedbPersistence::begin_history_write(&db).unwrap();
            let table = write
                .open_table(TableDefinition::<&[u8], &[u8]>::new("rel_fixture"))
                .unwrap();
            {
                let mut witnesses = write.open_table(WITNESSES).unwrap();
                let mut bytes = witnesses
                    .get("rel_fixture")
                    .unwrap()
                    .unwrap()
                    .value()
                    .to_vec();
                bytes[1] = flag;
                if clear_root {
                    bytes[18..].fill(0);
                }
                witnesses.insert("rel_fixture", bytes.as_slice()).unwrap();
            }
            drop(table);
            if write.commit().is_ok() {
                blessed.push((flag, clear_root));
            }
            assert!(
                verify(&db).is_ok(),
                "refusal preserves the old valid witness"
            );
        }
        assert!(
            blessed.is_empty(),
            "malformed witness records were blessed (flag, clear_root): {blessed:?}"
        );
    }
    thread_local! {
        static RELOCATION_EVENTS: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
    }
    fn observed_hook(
        write: &redb::WriteTransaction,
        name: &str,
        event: redb::TableRootEvent,
    ) -> std::result::Result<(), redb::StorageError> {
        RedbPersistence::historical_root_event(write, name, event)?;
        if protected(name) {
            RELOCATION_EVENTS.with(|count| {
                let (before, after) = count.get();
                count.set(match event {
                    redb::TableRootEvent::BeforeRelocate => (before + 1, after),
                    redb::TableRootEvent::AfterRelocate => (before, after + 1),
                    _ => (before, after),
                });
            });
        }
        Ok(())
    }
    #[test]
    fn bounded_compaction_updates_only_selected_root_and_abort_keeps_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let db = redb::Database::create(dir.path().join("compaction.db")).unwrap();
        RedbPersistence::write_format_marker(
            &db,
            &mut super::super::content_identity::ProofAdmission::unaccounted(),
        )
        .unwrap();
        let write = db.begin_write().unwrap();
        let write = write.with_table_root_hook(RedbPersistence::historical_root_event);
        {
            let mut scratch = write
                .open_table(TableDefinition::<u64, &[u8]>::new("00_scratch"))
                .unwrap();
            for key in 0..128 {
                scratch.insert(key, &[0u8; 4096][..]).unwrap();
            }
        }
        for name in ["rel_fixture", "rel_second"] {
            let mut table = write
                .open_table(TableDefinition::<u64, &[u8]>::new(name))
                .unwrap();
            for key in 0..64 {
                table.insert(key, &[1u8; 4096][..]).unwrap();
            }
        }
        write.commit().unwrap();
        let write = RedbPersistence::begin_history_write(&db).unwrap();
        write
            .delete_table(TableDefinition::<u64, &[u8]>::new("00_scratch"))
            .unwrap();
        write.commit().unwrap();
        let held = db.begin_read().unwrap();
        let mut cursor = redb::CompactionCursor::default();
        let original = format!("{cursor:?}");
        let write = RedbPersistence::begin_history_write(&db).unwrap();
        let refused = write.compact_step_with_cursor_hook_storage(
            &mut cursor,
            2,
            |_| Ok(()),
            |_, _| {
                Err(redb::StorageError::Corrupted(
                    "injected publication cut".into(),
                ))
            },
        );
        assert!(refused.is_err());
        assert_eq!(
            format!("{cursor:?}"),
            original,
            "refusal publishes no successor cursor"
        );
        let mut relocated = 0;
        let mut complete = false;
        for _ in 0..4096 {
            RELOCATION_EVENTS.with(|count| count.set((0, 0)));
            let write = RedbPersistence::begin_history_write(&db)
                .unwrap()
                .with_table_root_hook(observed_hook);
            let progress = write
                .compact_step_with_cursor_hook_storage(
                    &mut cursor,
                    2,
                    |_| Ok(()),
                    |txn, _| {
                        RedbPersistence::rebind_ddl_selector_roots_in_write(txn)
                            .map_err(|error| redb::StorageError::Corrupted(error.to_string()))
                    },
                )
                .unwrap();
            RELOCATION_EVENTS.with(|count| {
                let (before, after) = count.get();
                assert!(
                    before <= 1 && after <= 1,
                    "one selected target, no catalog witness sweep"
                );
                assert!(after <= before, "validate before relocation publication");
            });
            let read = db.begin_read().unwrap();
            for name in ["rel_fixture", "rel_second"] {
                RedbPersistence::verify_history_root(&read, name).unwrap();
                RedbPersistence::verify_history_root(&held, name).unwrap();
            }
            relocated += progress.pages_relocated;
            if progress.complete {
                complete = true;
                break;
            }
        }
        assert!(complete, "finite native sweep completes");
        assert!(relocated > 0, "fixture exercised physical relocation");
    }
    #[test]
    fn root_witness_work_does_not_enumerate_n_2n_4n_history() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        for count in [1024u64, 2048, 4096] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("history.db");
            let db = redb::Database::create(&path).unwrap();
            RedbPersistence::write_format_marker(
                &db,
                &mut super::super::content_identity::ProofAdmission::unaccounted(),
            )
            .unwrap();
            let write = db.begin_write().unwrap();
            let write = write.with_table_root_hook(RedbPersistence::historical_root_event);
            {
                let mut table = write
                    .open_table(TableDefinition::<u64, &[u8]>::new("rel_fixture"))
                    .unwrap();
                for key in 0..count {
                    table.insert(key, &[1u8; 1024][..]).unwrap();
                }
            }
            write.commit().unwrap();
            drop(db);
            let db = redb::ReadOnlyDatabase::open(&path).unwrap();
            let read = db.begin_read().unwrap();
            let bytes = Arc::new(AtomicUsize::new(0));
            let observed = bytes.clone();
            let guard = redb::observe_backend_reads(Arc::new(move |n| {
                observed.fetch_add(n, Ordering::Relaxed);
            }));
            RedbPersistence::verify_history_root(&read, "rel_fixture").unwrap();
            drop(guard);
            assert!(
                bytes.load(Ordering::Relaxed) <= 64 * 1024,
                "fixed metadata/header paths, no historical value scan: count={count}"
            );
        }
    }
    #[test]
    fn unvisited_leaf_damage_is_out_of_scope_but_consumed_leaf_refuses() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("physical.db");
        let db = redb::Database::create(&path).unwrap();
        RedbPersistence::write_format_marker(
            &db,
            &mut super::super::content_identity::ProofAdmission::unaccounted(),
        )
        .unwrap();
        let write = db.begin_write().unwrap();
        let write = write.with_table_root_hook(RedbPersistence::historical_root_event);
        const MARKER: &[u8] = b"CDB141-LAST-UNVISITED-LEAF";
        {
            let mut table = write
                .open_table(TableDefinition::<u64, &[u8]>::new("rel_fixture"))
                .unwrap();
            for key in 0..64u64 {
                let mut value = [1u8; 4096];
                if key == 63 {
                    value[..MARKER.len()].copy_from_slice(MARKER);
                }
                table.insert(key, &value[..]).unwrap();
            }
        }
        write.commit().unwrap();
        drop(db);
        let db = redb::ReadOnlyDatabase::open(&path).unwrap();
        let snapshot = db.begin_read().unwrap().storage_snapshot_identity();
        drop(db);
        let mut bytes = std::fs::read(&path).unwrap();
        let positions = bytes
            .windows(MARKER.len())
            .enumerate()
            .filter_map(|(i, window)| (window == MARKER).then_some(i))
            .collect::<Vec<_>>();
        assert!(
            !positions.is_empty(),
            "fixture locates the physical leaf payload"
        );
        for i in positions {
            bytes[i + 12] ^= 1;
        }
        std::fs::write(&path, bytes).unwrap();
        let db = redb::ReadOnlyDatabase::open(&path).unwrap();
        let read = db.begin_external_read(snapshot).unwrap();
        RedbPersistence::verify_history_root(&read, "rel_fixture").unwrap();
        let table = read
            .open_table(TableDefinition::<u64, &[u8]>::new("rel_fixture"))
            .unwrap();
        assert_eq!(table.get(0).unwrap().unwrap().value(), &[1u8; 4096][..]);
        assert!(
            table.get(63).is_err(),
            "the consumed damaged leaf must fail native checksum validation"
        );
    }
    fn failing_close_hook(
        write: &redb::WriteTransaction,
        name: &str,
        event: redb::TableRootEvent,
    ) -> std::result::Result<(), redb::StorageError> {
        RedbPersistence::historical_root_event(write, name, event)?;
        if protected(name) && matches!(event, redb::TableRootEvent::AfterClose) {
            return Err(redb::StorageError::Corrupted(
                "injected close-hook failure after witness staging".into(),
            ));
        }
        Ok(())
    }
    #[test]
    fn close_hook_failure_refuses_commit_and_reopen_keeps_complete_old_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("close-failure.db");
        let db = seeded(&path);
        let write = RedbPersistence::begin_history_write(&db)
            .unwrap()
            .with_table_root_hook(failing_close_hook);
        write
            .open_table(TableDefinition::<&[u8], &[u8]>::new("rel_fixture"))
            .unwrap()
            .insert(&b"key"[..], &b"must-not-publish"[..])
            .unwrap();
        assert!(
            write.commit().is_err(),
            "latched Drop callback error refuses before publication"
        );
        drop(db);
        let db = redb::Database::open(&path).unwrap();
        verify(&db).unwrap();
        let read = db.begin_read().unwrap();
        let table = read
            .open_table(TableDefinition::<&[u8], &[u8]>::new("rel_fixture"))
            .unwrap();
        assert_eq!(
            table.get(&b"key"[..]).unwrap().unwrap().value(),
            &b"old"[..]
        );
    }

    fn generated_present_record(db: &redb::Database) -> Vec<u8> {
        let read = db.begin_read().unwrap();
        let physical = read
            .normal_table_root("rel_fixture")
            .unwrap()
            .expect("fixture table")
            .expect("published root");
        let stored = read
            .open_table(WITNESSES)
            .unwrap()
            .get("rel_fixture")
            .unwrap()
            .unwrap()
            .value()
            .to_vec();
        assert_eq!(stored.len(), RECORD_BYTES);
        assert_eq!(stored[0], 1);
        let incarnation: [u8; 16] = stored[2..18].try_into().unwrap();
        let root: [u8; 32] = stored[18..].try_into().unwrap();
        assert_eq!(stored.as_slice(), record(Some(root), incarnation).as_slice());
        assert_eq!(root, physical);
        let length = RedbPersistence::witnessed_row_count_in_read(&read, "rel_fixture")
            .unwrap()
            .unwrap();
        const PAGE: usize = 8;
        const CHECKSUM: usize = 16;
        const LENGTH: usize = 8;
        assert_eq!(PAGE + CHECKSUM + LENGTH, root.len());
        assert_eq!(&root[PAGE + CHECKSUM..], &length.to_le_bytes());
        assert!(root[..PAGE].iter().any(|byte| *byte != 0));
        assert_ne!(length, 0);
        stored
    }

    fn plant(
        path: &std::path::Path,
        mutate: impl FnOnce(&mut [u8]),
    ) -> (redb::Database, Vec<u8>, Vec<u8>) {
        let db = seeded(path);
        let original = generated_present_record(&db);
        let mut bytes = original.clone();
        mutate(&mut bytes);
        let raw = db.begin_write().unwrap();
        {
            let mut witnesses = raw.open_table(WITNESSES).unwrap();
            witnesses.insert("rel_fixture", bytes.as_slice()).unwrap();
        }
        raw.commit().unwrap();
        (db, original, bytes)
    }

    fn assert_one_span(original: &[u8], mutated: &[u8], start: usize, end: usize) {
        assert_ne!(&original[start..end], &mutated[start..end]);
        assert!(original.iter().zip(mutated).enumerate().all(|(index, (before, after))| {
            (index >= start && index < end) || before == after
        }));
    }

    fn assert_canonical_malformed(db: &redb::Database, bytes: &[u8]) {
        assert!(!valid_record(bytes), "producer predicate must reject this one-field record");
        let read = db.begin_read().unwrap();
        match RedbPersistence::verify_history_root_canonical(&read, "rel_fixture") {
            Err(Error::StoreCorrupted { reason, .. }) => {
                assert!(reason.contains("record is malformed"), "{reason}");
            }
            other => panic!("malformed witness must refuse, got {other:?}"),
        }
    }

    #[test]
    fn canonical_zero_incarnation_witness_is_typed_malformed() {
        let dir = tempfile::tempdir().unwrap();
        let (db, original, bytes) = plant(&dir.path().join("witness.db"), |bytes| {
            bytes[2..18].fill(0);
        });
        assert_one_span(&original, &bytes, 2, 18);
        assert_canonical_malformed(&db, &bytes);
    }

    #[test]
    fn canonical_invalid_presence_witness_is_typed_malformed() {
        let dir = tempfile::tempdir().unwrap();
        let (db, original, bytes) = plant(&dir.path().join("witness.db"), |bytes| {
            bytes[1] = 2;
        });
        assert_one_span(&original, &bytes, 1, 2);
        assert_canonical_malformed(&db, &bytes);
    }

    #[test]
    fn canonical_absent_flag_nonzero_tail_is_typed_malformed() {
        let dir = tempfile::tempdir().unwrap();
        let (db, original, bytes) = plant(&dir.path().join("witness.db"), |bytes| {
            bytes[1] = 0;
        });
        assert_one_span(&original, &bytes, 1, 2);
        assert!(bytes[18..].iter().any(|byte| *byte != 0));
        assert_canonical_malformed(&db, &bytes);
    }

    #[test]
    fn canonical_present_zero_page_witness_is_typed_malformed() {
        let dir = tempfile::tempdir().unwrap();
        let (db, original, bytes) = plant(&dir.path().join("witness.db"), |bytes| {
            bytes[18..26].fill(0);
        });
        assert_one_span(&original, &bytes, 18, 26);
        assert_canonical_malformed(&db, &bytes);
    }

    #[test]
    fn canonical_present_zero_length_witness_is_typed_malformed() {
        let dir = tempfile::tempdir().unwrap();
        let (db, original, bytes) = plant(&dir.path().join("witness.db"), |bytes| {
            bytes[42..50].fill(0);
        });
        assert_one_span(&original, &bytes, 42, 50);
        assert_canonical_malformed(&db, &bytes);
    }

    #[test]
    fn canonical_valid_root_mismatch_stays_disclosed_false() {
        let dir = tempfile::tempdir().unwrap();
        let (db, original, bytes) = plant(&dir.path().join("witness.db"), |bytes| {
            bytes[26] ^= 1;
        });
        assert_one_span(&original, &bytes, 26, 27);
        assert!(valid_record(&bytes));
        let read = db.begin_read().unwrap();
        let physical = read
            .normal_table_root("rel_fixture")
            .unwrap()
            .unwrap()
            .unwrap();
        assert_ne!(&bytes[18..], &physical[..]);
        assert_eq!(
            RedbPersistence::verify_history_root_canonical(&read, "rel_fixture").unwrap(),
            false
        );
    }
}
