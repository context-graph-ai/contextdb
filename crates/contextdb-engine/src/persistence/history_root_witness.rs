//! Producer-authored historical roots. No history or catalog is enumerated.
use super::*;

const WITNESSES: TableDefinition<&str, &[u8]> = TableDefinition::new("__history_root_witness_v1");
const RECORD_BYTES: usize = 50;

fn protected(name: &str) -> bool {
    name.starts_with("rel_")
        || name.strip_prefix("__rel_index_").is_some_and(|suffix| {
            suffix.len() == 64 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
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
fn matches(bytes: &[u8], root: Option<[u8; 32]>) -> bool {
    bytes.len() == RECORD_BYTES
        && bytes[0] == 1
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
        Self::historical_root_event_inner(write, name, event)
            .map_err(|error| redb::StorageError::AdmissionRefused(Box::new(error)))
    }
    fn historical_root_event_inner(
        write: &redb::WriteTransaction,
        name: &str,
        event: redb::TableRootEvent,
    ) -> Result<()> {
        use redb::TableRootEvent::*;
        let physical = write.normal_table_root(name).map_err(Self::storage_error)?;
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
                    if previous.value().len() != RECORD_BYTES || previous.value()[0] != 1 {
                        return Err(corruption(name, "record is malformed"));
                    }
                    previous.value()[2..18]
                        .try_into()
                        .expect("validated fixed record")
                } else {
                    *uuid::Uuid::new_v4().as_bytes()
                };
                let encoded = record(root, incarnation);
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
                if bytes.len() != RECORD_BYTES || bytes[0] != 1 {
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
        let write = db.begin_write().unwrap();
        RedbPersistence::initialize_history_witnesses(&write).unwrap();
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
        let write = db.begin_write().unwrap();
        RedbPersistence::initialize_history_witnesses(&write).unwrap();
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
                .compact_step_with_cursor_hook_storage(&mut cursor, 2, |_| Ok(()), |_, _| Ok(()))
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
            let write = db.begin_write().unwrap();
            RedbPersistence::initialize_history_witnesses(&write).unwrap();
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
        let write = db.begin_write().unwrap();
        RedbPersistence::initialize_history_witnesses(&write).unwrap();
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
}
