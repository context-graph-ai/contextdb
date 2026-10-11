//! CURRENT identity is a canonical exact-delta witness, never a history scan.
//! The count and XOR of complete source identities share one atomic record.
//! With count one the XOR is the sole identity. Keeping the XOR at other counts
//! makes signed duplicate fixtures reversible without selecting a first source.
use super::*;
use contextdb_vector::VectorCopyAdmission;

pub(super) const CURRENT: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_current_counted_identity");
pub(super) const ROOT: &str = "vector_current_counted_identity_root";
const MAGIC: &[u8; 8] = b"CDBVCI01";

fn damage(reason: &str) -> Error {
    Error::StoreCorrupted {
        path: "vector_current_counted_identity".into(),
        reason: reason.into(),
    }
}

fn decode<'a>(key: &[u8], bytes: &'a [u8]) -> Result<(u64, &'a [u8])> {
    if bytes.len() < 48 || !bytes.starts_with(MAGIC) {
        return Err(damage("CURRENT identity envelope is truncated"));
    }
    let end = bytes.len() - 32;
    let mut hash = blake3::Hasher::new();
    hash.update(key);
    hash.update(&bytes[..end]);
    if hash.finalize().as_bytes().as_slice() != &bytes[end..] {
        return Err(damage("CURRENT identity checksum or key is invalid"));
    }
    let count = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
    let identity = &bytes[16..end];
    if count == 0 && !identity.is_empty() {
        return Err(damage("empty CURRENT count retains a source identity"));
    }
    Ok((count, identity))
}

fn encode(key: &[u8], count: u64, identity: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(48 + identity.len());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&count.to_be_bytes());
    bytes.extend_from_slice(identity);
    let mut hash = blake3::Hasher::new();
    hash.update(key);
    hash.update(&bytes);
    bytes.extend_from_slice(hash.finalize().as_bytes());
    bytes
}

/// Complete reverse identity, including owning and vector versions and route.
/// Trailing zero padding is discarded only in the XOR accumulator; the encoded
/// identity's length is itself part of the XOR, so count one is reversible.
fn contribution(record: &VectorPartitionMembershipRecord) -> Vec<u8> {
    let identity = vector_membership_pages::reverse_key(record);
    // The row/index already own the witness key; the value needs only the
    // owning stamps, canonical partition and vector stamps from the suffix.
    let prefix = 24 + record.index.table.len() + record.index.column.len();
    let identity = &identity[prefix..];
    let mut bytes = Vec::with_capacity(8 + identity.len());
    bytes.extend_from_slice(&(identity.len() as u64).to_be_bytes());
    bytes.extend_from_slice(identity);
    bytes
}

pub(super) fn transition<const CHECK: bool>(
    write: &redb::WriteTransaction,
    previous: Option<&VectorPartitionMembershipRecord>,
    next: Option<&VectorPartitionMembershipRecord>,
    accountant: Option<&crate::memory_accounting::AccountantHandle>,
) -> Result<()> {
    let Some(route) = previous.or(next) else {
        return Ok(());
    };
    // Both records occupy one exact primary key; renames delete then insert.
    if let Some(next) = next
        && (next.index != route.index || next.row_id != route.row_id)
    {
        return Err(damage("CURRENT delta changes its row key"));
    }
    let _key_memory = accountant
        .map(|owner| {
            crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                owner.clone(),
                route
                    .index
                    .table
                    .len()
                    .saturating_add(route.index.column.len())
                    .saturating_add(route.partition_key.estimated_bytes())
                    .saturating_mul(8)
                    .saturating_add(1024),
                "vector",
                "current_identity_delta",
                "Raise MEMORY_LIMIT to publish this vector identity.",
            )
        })
        .transpose()?;
    let key = vector_membership_pages::reverse_row_prefix(&route.index, route.row_id);
    let mut table = write
        .open_table(CURRENT)
        .map_err(vector_membership_pages::metadata_error)?;
    let stored = table
        .get_bounded(key.as_slice())
        .map_err(vector_membership_pages::metadata_error)?;
    if stored.is_none() && previous.is_some() {
        return Err(damage(
            "required CURRENT witness is missing during a membership delta",
        ));
    }
    let _value_memory = accountant
        .map(|owner| {
            crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                owner.clone(),
                stored
                    .as_ref()
                    .map_or(0, |value| value.value().len())
                    .saturating_mul(4),
                "vector",
                "current_identity_delta",
                "Raise MEMORY_LIMIT to publish this vector identity.",
            )
        })
        .transpose()?;
    let (mut count, mut xor) = match stored.as_ref() {
        Some(value) => {
            let (count, identity) = decode(&key, value.value())?;
            (count, identity.to_vec())
        }
        None => (0, Vec::new()),
    };
    if CHECK && count > 1 {
        return Err(damage("CURRENT row has multiple live source identities"));
    }
    if let Some(record) = previous.filter(|record| record.deleted_tx.is_none()) {
        let bytes = contribution(record);
        if count == 1 && xor != trimmed(&bytes) {
            return Err(damage(
                "CURRENT witness differs from the exact previous source",
            ));
        }
        count = count
            .checked_sub(1)
            .ok_or_else(|| damage("CURRENT source count underflow"))?;
        fold(&mut xor, &bytes);
    }
    if let Some(record) = next.filter(|record| record.deleted_tx.is_none()) {
        count = count
            .checked_add(1)
            .ok_or_else(|| damage("CURRENT source count overflow"))?;
        fold(&mut xor, &contribution(record));
    }
    if CHECK && count > 1 {
        return Err(damage("CURRENT row has multiple live source identities"));
    }
    if count == 0 && !xor.is_empty() {
        return Err(damage(
            "CURRENT source removal leaves a mismatched identity",
        ));
    }
    drop(stored);
    table
        .insert(key.as_slice(), encode(&key, count, &xor).as_slice())
        .map_err(vector_membership_pages::metadata_error)?;
    Ok(())
}

fn trimmed(bytes: &[u8]) -> &[u8] {
    &bytes[..bytes
        .iter()
        .rposition(|byte| *byte != 0)
        .map_or(0, |last| last + 1)]
}
fn fold(xor: &mut Vec<u8>, bytes: &[u8]) {
    xor.resize(xor.len().max(bytes.len()), 0);
    for (target, byte) in xor.iter_mut().zip(bytes) {
        *target ^= byte;
    }
    xor.truncate(trimmed(xor).len());
}

/// The accepted row and witness are read from the existing precommit capture.
/// Native page admission uses that operation's reader, separate from the fixed
/// number of logical metadata probes and decodes.
pub(crate) fn accepted_identity(
    root: &WriterSnapshot,
    index: &VectorIndexRef,
    partition: &VectorPartitionKey,
    accepted: &VersionedRow,
    copies: &mut VectorCopyAdmission,
    memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Option<(TxId, Lsn)>> {
    vector_membership_source::operation_checkpoint()?;
    let mut scratch = copies.scratch();
    scratch.reserve(
        index
            .table
            .len()
            .saturating_add(index.column.len())
            .saturating_mul(8)
            .saturating_add(4096),
    )?;
    graph_read::with_pages(memory, || {
        vector_membership_source::operation_checkpoint()?;
        root.check_table_scope(&index.table)?;
        let read = root.owner_read()?;
        vector_membership_pages::validate_in_read(&read)?;
        let key = vector_membership_pages::reverse_row_prefix(index, accepted.row_id);
        let table = match read.open_byte_table_bounded(CURRENT) {
            Ok(table) => Some(table),
            Err(redb::TableError::TableDoesNotExist(_)) => None,
            Err(error) => return Err(vector_membership_pages::metadata_error(error)),
        };
        observe_probe();
        let value = table
            .as_ref()
            .map(|table| {
                table
                    .get_bounded(key.as_slice())
                    .map_err(vector_membership_pages::metadata_error)
            })
            .transpose()?
            .flatten();
        let Some(value) = value else {
            // Absence is legal only for a row with no membership history at all.
            match read.open_byte_table_bounded(vector_membership_pages::REVERSE) {
                Ok(reverse) => {
                    if reverse
                        .prefix_bounded(key.as_slice())
                        .map_err(vector_membership_pages::metadata_error)?
                        .next()
                        .transpose()
                        .map_err(vector_membership_pages::metadata_error)?
                        .is_some()
                    {
                        return Err(damage("required CURRENT witness is missing"));
                    }
                }
                Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(error) => return Err(vector_membership_pages::metadata_error(error)),
            }
            return Ok(None);
        };
        scratch.reserve(value.value().len().saturating_mul(8))?;
        let (count, xor) = decode(&key, value.value())?;
        if count == 0 {
            return Ok(None);
        }
        if count != 1 {
            return Err(damage("CURRENT row has multiple live source identities"));
        }
        let length = xor
            .get(..8)
            .ok_or_else(|| damage("CURRENT sole identity has no length"))?;
        let length = usize::try_from(u64::from_be_bytes(length.try_into().unwrap()))
            .map_err(|_| damage("CURRENT sole identity length overflow"))?;
        scratch.reserve(partition.estimated_bytes().saturating_mul(8))?;
        let partition_bytes = partition.canonical_bytes();
        if length != 40usize.saturating_add(partition_bytes.len())
            || xor.len() > length.saturating_add(8)
        {
            return Err(damage("CURRENT sole identity length is invalid"));
        }
        let mut identity = Vec::with_capacity(key.len().saturating_add(length));
        identity.extend_from_slice(&key);
        identity.extend_from_slice(&xor[8..]);
        identity.resize(key.len().saturating_add(length), 0);
        let reverse = read
            .open_byte_table_bounded(vector_membership_pages::REVERSE)
            .map_err(vector_membership_pages::metadata_error)?;
        observe_probe();
        let source = reverse
            .get_bounded(identity.as_slice())
            .map_err(vector_membership_pages::metadata_error)?
            .ok_or_else(|| damage("CURRENT source has no exact reverse companion"))?;
        scratch.reserve(source.value().len().saturating_mul(8))?;
        let payload =
            vector_partition_record_payload(VectorPartitionRecordKind::Membership, source.value())
                .map_err(|reason| damage(&reason))?;
        prevalidate_vector_partition_metadata_routes(
            VectorPartitionRecordKind::Membership,
            payload,
        )
        .map_err(|reason| damage(&reason))?;
        let native =
            vector_partition_route_native_capacity(payload).map_err(|reason| damage(&reason))?;
        scratch.reserve(native.saturating_mul(8))?;
        // All decoded fields are transient; only two scalar identities leave
        // this lookup. Release their codec charges after the record and keys
        // are freed, rather than retaining them until the enclosing operation.
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        let _record_scope = crate::read_image_memory::active_owner()?
            .row_owner()?
            .enter();
        let record = vector_membership_pages::decode_reverse_member(&identity, source.value())?;
        if record.index != *index
            || record.row_id != accepted.row_id
            || record.partition_key != *partition
            || record.row_created_tx != accepted.created_tx
            || record.row_lsn != accepted.lsn
            || record.deleted_tx.is_some()
            || record.visible_from < record.row_created_tx.max(record.vector_created_tx)
            || accepted.deleted_tx.is_some()
        {
            return Err(damage(
                "CURRENT source differs from its accepted owning row",
            ));
        }
        for (definition, exact_key) in [
            (
                VECTOR_PARTITION_MEMBERSHIP_TABLE,
                RedbPersistence::vector_partition_membership_key(&record),
            ),
            (
                vector_membership_pages::SOURCE,
                vector_membership_pages::source_key(&record),
            ),
        ] {
            vector_membership_source::operation_checkpoint()?;
            let companion = read
                .open_byte_table_bounded(definition)
                .map_err(vector_membership_pages::metadata_error)?;
            observe_probe();
            let bytes = companion
                .get_bounded(exact_key.as_slice())
                .map_err(vector_membership_pages::metadata_error)?
                .ok_or_else(|| damage("CURRENT source has no exact primary/source companion"))?;
            if bytes.value() != source.value() {
                return Err(damage("CURRENT primary/source/reverse companions disagree"));
            }
        }
        Ok(Some((record.vector_created_tx, record.vector_lsn)))
    })
}

fn observe_probe() {
    #[cfg(any(test, feature = "test-seams"))]
    crate::database::resident_history_test_seams::note(|work| work.membership_keys_examined += 1);
}

#[cfg(all(test, feature = "test-seams"))]
mod tests {
    use super::*;
    use crate::memory_accounting::MemoryAccountant;

    #[derive(Debug)]
    struct ReadBudget {
        accountant: Arc<MemoryAccountant>,
        native_pages: std::sync::atomic::AtomicUsize,
        native_bytes: std::sync::atomic::AtomicUsize,
    }
    impl ReadBudget {
        fn new(accountant: Arc<MemoryAccountant>) -> Self {
            Self {
                accountant,
                native_pages: Default::default(),
                native_bytes: Default::default(),
            }
        }
    }
    impl contextdb_core::read_memory::ReadMemoryBudget for ReadBudget {
        fn try_reserve(
            &self,
            bytes: usize,
            subsystem: &'static str,
            operation: &'static str,
            hint: &'static str,
        ) -> Result<()> {
            self.accountant
                .try_allocate_for(bytes, subsystem, operation, hint)?;
            if subsystem == "graph_source" && operation == "read_window" {
                self.native_pages
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.native_bytes
                    .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
            }
            Ok(())
        }
        fn release(&self, bytes: usize) {
            self.accountant.release(bytes);
        }
    }

    fn open_fixture(path: &Path) -> (Arc<RedbPersistence>, crate::memory_accounting::AccountantHandle) {
        let persistence = Arc::new(RedbPersistence::open_with_cache(path, 0).unwrap());
        // Writable recovery may already have installed the source's accountant.
        // Qualify the actual installed funder, never a rejected later attachment.
        let writer = persistence
            .proof_accountant()
            .unwrap_or_else(|| Arc::new(MemoryAccountant::with_budget(64 * 1024 * 1024)).into());
        writer.set_budget(Some(64 * 1024 * 1024)).unwrap();
        persistence.attach_memory_accountant(writer.clone());
        assert_eq!(persistence.proof_accountant().unwrap().as_ptr(), writer.as_ptr());
        (persistence, writer)
    }

    fn fixture(
        history: u64,
    ) -> (
        tempfile::TempDir,
        Arc<RedbPersistence>,
        crate::memory_accounting::AccountantHandle,
        VectorPartitionMembershipRecord,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("current.db");
        let db = crate::Database::open(&path).unwrap();
        db.execute("CREATE TABLE points (id INTEGER PRIMARY KEY, scope TEXT NOT NULL, v VECTOR(1) PARTITION_KEY(scope) MAX_PARTITIONS 4)", &HashMap::new()).unwrap();
        db.close().unwrap();
        let (persistence, writer) = open_fixture(&path);
        let index = VectorIndexRef::new("points", "v");
        persistence
            .append_vector_entries_batch(
                &[VectorEntry {
                    index: index.clone(),
                    row_id: RowId(1),
                    vector: vec![1.0],
                    created_tx: TxId(1),
                    deleted_tx: None,
                    lsn: Lsn(1),
                }],
                &HashMap::new(),
            )
            .unwrap();
        let records: Vec<_> = (1..=history)
            .map(|tx| VectorPartitionMembershipRecord {
                index: index.clone(),
                partition_key: VectorPartitionKey::from_components(vec![
                    contextdb_core::VectorPartitionComponent::Text("one".into()),
                ]),
                row_id: RowId(1),
                row_created_tx: TxId(tx),
                row_lsn: Lsn(tx),
                vector_created_tx: TxId(1),
                vector_lsn: Lsn(1),
                visible_from: TxId(tx),
                deleted_tx: (tx != history).then_some(TxId(tx + 1)),
            })
            .collect();
        for chunk in records.chunks(8) {
            let _writer_decode = persistence.writer_decode_scope().unwrap();
            persistence
                .append_vector_partition_memberships_batch(chunk)
                .unwrap();
        }
        drop(persistence);
        assert_eq!(writer.usage().used, 0);
        let (persistence, writer) = open_fixture(&path);
        persistence.settle_writer_cache_budget(0).unwrap();
        assert_eq!(persistence.resident_cache_stats_for_test().unwrap().3, 0);
        (
            directory,
            persistence,
            writer,
            records.last().unwrap().clone(),
        )
    }

    fn row(record: &VectorPartitionMembershipRecord) -> VersionedRow {
        VersionedRow {
            row_id: record.row_id,
            values: HashMap::new(),
            created_tx: record.row_created_tx,
            deleted_tx: None,
            lsn: record.row_lsn,
            created_at: None,
        }
    }
    fn read(
        root: &WriterSnapshot,
        record: &VectorPartitionMembershipRecord,
        accepted: &VersionedRow,
        limit: usize,
    ) -> (Result<Option<(TxId, Lsn)>>, usize) {
        let accountant = Arc::new(MemoryAccountant::with_budget(limit));
        let payer = Arc::new(ReadBudget::new(accountant.clone()));
        let memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget> = payer.clone();
        let mut copies = crate::vector_read_memory::admission(memory.clone());
        let (result, work) = crate::database::resident_history_test_seams::observe(|| {
            accepted_identity(
                root,
                &record.index,
                &record.partition_key,
                accepted,
                &mut copies,
                memory,
            )
        });
        eprintln!(
            "CURRENT metadata probes={} envelope validations={} native admitted pages={} bytes={}",
            work.membership_keys_examined,
            work.membership_envelopes_verified,
            payer
                .native_pages
                .load(std::sync::atomic::Ordering::Relaxed),
            payer
                .native_bytes
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        if result.as_ref().is_ok_and(|answer| answer.is_some()) {
            assert_eq!(work.membership_envelopes_verified, 3);
        }
        drop(copies);
        assert_eq!(
            accountant.usage().used,
            0,
            "last CURRENT metadata owner refunds the executing reader"
        );
        (result, work.membership_keys_examined)
    }

    #[test]
    fn repeated_current_probes_release_transient_decode_before_the_next_row() {
        let (_directory, persistence, writer, record) = fixture(1);
        let root = persistence
            .initialize_writer_snapshot(SnapshotId(1), Lsn(1))
            .unwrap();
        let decode = persistence.writer_decode_scope().unwrap();
        assert_eq!(
            read(&root, &record, &row(&record), 8 * 1024 * 1024)
                .0
                .unwrap(),
            Some((TxId(1), Lsn(1)))
        );
        let used = writer.usage().used;
        writer.set_budget(Some(used + 16 * 1024)).unwrap();
        for _ in 0..256 {
            assert_eq!(
                read(&root, &record, &row(&record), 8 * 1024 * 1024)
                    .0
                    .unwrap(),
                Some((TxId(1), Lsn(1)))
            );
            assert_eq!(writer.usage().used, used);
        }
        drop(decode);
        drop(root);
        drop(persistence);
        assert_eq!(writer.usage().used, 0);
        assert_eq!(writer.underflow_count_for_test(), 0);
    }

    #[test]
    fn current_identity_probe_work_is_independent_of_retained_history() {
        for history in [1, 256, 4096] {
            let (_directory, persistence, writer, record) = fixture(history);
            eprintln!("CURRENT retained history H={history}");
            let root = persistence
                .initialize_writer_snapshot(SnapshotId(history), Lsn(history))
                .unwrap();
            let (result, probes) = read(&root, &record, &row(&record), 8 * 1024 * 1024);
            assert_eq!(result.unwrap(), Some((TxId(1), Lsn(1))));
            assert_eq!(
                probes, 4,
                "H={history}: witness plus exact reverse/primary/source, no history stream"
            );
            let mut wrong = row(&record);
            wrong.lsn = Lsn(history + 1);
            assert!(matches!(
                read(&root, &record, &wrong, 8 * 1024 * 1024).0,
                Err(Error::StoreCorrupted { .. })
            ));
            assert!(matches!(
                read(&root, &record, &row(&record), 1).0,
                Err(Error::MemoryBudgetExceeded { .. })
            ));
            drop(root);
            drop(persistence);
            assert_eq!(
                writer.usage().used,
                0,
                "final root/persistence owner refunds writer pages and controls"
            );
        }
    }

    #[test]
    fn signed_duplicates_are_counted_atomically_and_do_not_change_old_captures() {
        for variant in 0..4 {
            let (directory, mut persistence, mut writer, clean) = fixture(2);
            let old = persistence
                .initialize_writer_snapshot(SnapshotId(2), Lsn(2))
                .unwrap();
            let mut duplicate = clean.clone();
            match variant {
                0 => {
                    duplicate.row_created_tx = TxId(1);
                    duplicate.row_lsn = Lsn(1);
                    duplicate.visible_from = TxId(1);
                }
                1 => {
                    duplicate.vector_created_tx = TxId(2);
                    duplicate.vector_lsn = Lsn(2);
                }
                2 => {
                    duplicate.partition_key = VectorPartitionKey::from_components(vec![
                        contextdb_core::VectorPartitionComponent::Text("two".into()),
                    ]);
                }
                _ => {
                    duplicate.row_created_tx = TxId(3);
                    duplicate.row_lsn = Lsn(3);
                    duplicate.vector_created_tx = TxId(3);
                    duplicate.vector_lsn = Lsn(3);
                    duplicate.visible_from = TxId(3);
                }
            }
            if variant == 1 || variant == 3 {
                persistence
                    .append_vector_entries_batch(
                        &[VectorEntry {
                            index: clean.index.clone(),
                            row_id: clean.row_id,
                            vector: vec![2.0],
                            created_tx: duplicate.vector_created_tx,
                            deleted_tx: None,
                            lsn: duplicate.vector_lsn,
                        }],
                        &HashMap::new(),
                    )
                    .unwrap();
            }
            let before = durable_records(&persistence);
            let refusal = persistence
                .append_vector_partition_memberships_batch(std::slice::from_ref(&duplicate))
                .unwrap_err();
            assert!(
                matches!(refusal, Error::StoreCorrupted { .. }),
                "production duplicate variant={variant}: {refusal:?}"
            );
            assert_eq!(
                durable_records(&persistence),
                before,
                "refused production delta is atomically unchanged"
            );
            persistence
                .write_corrupt_store_vector_memberships_for_test(&[duplicate.clone()])
                .unwrap();
            let damaged = persistence
                .initialize_writer_snapshot(SnapshotId(3), Lsn(3))
                .unwrap();
            assert!(
                matches!(
                    read(&damaged, &clean, &row(&clean), 8 * 1024 * 1024).0,
                    Err(Error::StoreCorrupted { .. })
                ),
                "signed duplicate variant={variant}"
            );
            assert_eq!(
                read(&old, &clean, &row(&clean), 8 * 1024 * 1024).0.unwrap(),
                Some((TxId(1), Lsn(1))),
                "old COW capture stays complete"
            );
            drop((old, damaged));
            drop(persistence);
            assert_eq!(writer.usage().used, 0);
            (persistence, writer) = open_fixture(&directory.path().join("current.db"));
            let reopened = persistence
                .initialize_writer_snapshot(SnapshotId(3), Lsn(3))
                .unwrap();
            assert!(
                matches!(
                    read(&reopened, &clean, &row(&clean), 8 * 1024 * 1024).0,
                    Err(Error::StoreCorrupted { .. })
                ),
                "signed duplicates refuse after close/open"
            );
            // Semantic bypass still applies exact deltas, including idempotent replay.
            persistence
                .write_corrupt_store_vector_memberships_for_test(&[duplicate.clone()])
                .unwrap();
            duplicate.deleted_tx = Some(TxId(4));
            persistence
                .write_corrupt_store_vector_memberships_for_test(&[duplicate])
                .unwrap();
            let recovered = persistence
                .initialize_writer_snapshot(SnapshotId(4), Lsn(4))
                .unwrap();
            assert_eq!(
                read(&recovered, &clean, &row(&clean), 8 * 1024 * 1024)
                    .0
                    .unwrap(),
                Some((TxId(1), Lsn(1))),
                "subtracting the exact duplicate recovers the sole source without scanning"
            );
            drop((reopened, recovered));
            drop(persistence);
            assert_eq!(writer.usage().used, 0);
        }
    }

    #[test]
    fn current_moves_replay_and_refusals_preserve_exact_owner_and_captured_roots() {
        let (directory, persistence, writer, current) = fixture(256);
        let old = persistence
            .initialize_writer_snapshot(SnapshotId(256), Lsn(256))
            .unwrap();
        let mut moved = current.clone();
        moved.row_created_tx = TxId(257);
        moved.row_lsn = Lsn(257);
        moved.visible_from = TxId(257);
        moved.partition_key = VectorPartitionKey::from_components(vec![
            contextdb_core::VectorPartitionComponent::Text("two".into()),
        ]);
        let publish_move = || -> Result<()> {
            let _decode_scope = persistence.writer_decode_scope()?;
            persistence.prepare_proof_owner()?;
            persistence.with_db(|db| {
                let write = RedbPersistence::begin_history_write(db)?;
                {
                    let mut memberships = write
                        .open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE)
                        .map_err(RedbPersistence::storage_error)?;
                    let mut tombstones = write
                        .open_table(VECTOR_PARTITION_TOMBSTONE_TABLE)
                        .map_err(RedbPersistence::storage_error)?;
                    persistence.tombstone_vector_partition_membership(
                        &write,
                        &mut memberships,
                        &mut tombstones,
                        current.clone(),
                        TxId(257),
                        Lsn(257),
                        VectorPartitionTombstoneCause::Move,
                    )?;
                    persistence.insert_vector_partition_membership(
                        &write,
                        &mut memberships,
                        &moved,
                    )?;
                }
                RedbPersistence::apply_content_identity_mutations_in_write(
                    &write,
                    &[],
                    &mut persistence.proof_admission(),
                )?;
                RedbPersistence::write_writer_recovery_bounds_in_write(&write, None)?;
                write.commit().map_err(RedbPersistence::storage_error)
            })
        };
        let before = durable_records(&persistence);
        writer.set_budget(Some(writer.usage().used + 1)).unwrap();
        let refused = publish_move();
        assert!(
            matches!(refused, Err(Error::MemoryBudgetExceeded { .. })),
            "{refused:?}"
        );
        assert_eq!(durable_records(&persistence), before);
        writer.set_budget(Some(64 * 1024 * 1024)).unwrap();
        publish_move().unwrap();
        let after = durable_records(&persistence);
        publish_move().unwrap();
        assert_eq!(
            durable_records(&persistence),
            after,
            "exact replay is idempotent"
        );
        let next = persistence
            .initialize_writer_snapshot(SnapshotId(257), Lsn(257))
            .unwrap();
        assert_eq!(
            read(&next, &moved, &row(&moved), 8 * 1024 * 1024)
                .0
                .unwrap(),
            Some((TxId(1), Lsn(1)))
        );
        assert_eq!(
            read(&old, &current, &row(&current), 8 * 1024 * 1024)
                .0
                .unwrap(),
            Some((TxId(1), Lsn(1)))
        );
        assert!(matches!(
            read(&next, &current, &row(&current), 8 * 1024 * 1024).0,
            Err(Error::StoreCorrupted { .. })
        ));
        drop((old, next, persistence));
        assert_eq!(writer.usage().used, 0);
        let (persistence, writer) = open_fixture(&directory.path().join("current.db"));
        let reopened = persistence
            .initialize_writer_snapshot(SnapshotId(257), Lsn(257))
            .unwrap();
        assert_eq!(
            read(&reopened, &moved, &row(&moved), 8 * 1024 * 1024)
                .0
                .unwrap(),
            Some((TxId(1), Lsn(1)))
        );
        assert_eq!(durable_records(&persistence), after);
        drop((reopened, persistence));
        assert_eq!(writer.usage().used, 0);
    }

    #[test]
    fn missing_and_mismatched_current_witnesses_refuse_without_reconstruction() {
        for mode in 0..3 {
            let (directory, persistence, writer, clean) = fixture(1);
            persistence
                .with_db(|db| {
                    let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
                    let key =
                        vector_membership_pages::reverse_row_prefix(&clean.index, clean.row_id);
                    match mode {
                        0 => {
                            write
                                .open_table(CURRENT)
                                .map_err(RedbPersistence::storage_error)?
                                .remove(key.as_slice())
                                .map_err(RedbPersistence::storage_error)?;
                        }
                        1 => {
                            write
                                .delete_table(CURRENT)
                                .map_err(RedbPersistence::storage_error)?;
                        }
                        _ => {
                            let mut wrong = clean.clone();
                            wrong.row_lsn = Lsn(777);
                            write
                                .open_table(CURRENT)
                                .map_err(RedbPersistence::storage_error)?
                                .insert(
                                    key.as_slice(),
                                    encode(&key, 1, trimmed(&contribution(&wrong))).as_slice(),
                                )
                                .map_err(RedbPersistence::storage_error)?;
                        }
                    }
                    // Sign the changed physical root; missing/mismatched records still refuse.
                    if mode != 1 {
                        vector_membership_pages::write_fingerprints(&write)?;
                    }
                    write.commit().map_err(RedbPersistence::storage_error)
                })
                .unwrap();
            let root = persistence
                .initialize_writer_snapshot(SnapshotId(1), Lsn(1))
                .unwrap();
            assert!(matches!(
                read(&root, &clean, &row(&clean), 8 * 1024 * 1024).0,
                Err(Error::StoreCorrupted { .. })
            ));
            let damaged = durable_records(&persistence);
            drop((root, persistence));
            assert_eq!(writer.usage().used, 0);
            let (persistence, writer) = open_fixture(&directory.path().join("current.db"));
            assert_eq!(
                durable_records(&persistence),
                damaged,
                "open never reconstructs the witness"
            );
            let reopened = persistence
                .initialize_writer_snapshot(SnapshotId(1), Lsn(1))
                .unwrap();
            assert!(matches!(
                read(&reopened, &clean, &row(&clean), 8 * 1024 * 1024).0,
                Err(Error::StoreCorrupted { .. })
            ));
            drop((reopened, persistence));
            assert_eq!(writer.usage().used, 0);
        }
    }
    fn durable_records(persistence: &RedbPersistence) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
        persistence
            .with_db(|db| {
                let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
                [
                    CURRENT,
                    VECTOR_PARTITION_MEMBERSHIP_TABLE,
                    vector_membership_pages::SOURCE,
                    vector_membership_pages::REVERSE,
                    vector_membership_pages::SUMMARY,
                    VECTOR_PARTITION_JOURNAL_TABLE,
                    VECTOR_PARTITION_TOMBSTONE_TABLE,
                ]
                .into_iter()
                .map(|definition| match read.open_table(definition) {
                    Ok(table) => table
                        .iter()
                        .map_err(RedbPersistence::storage_error)?
                        .map(|item| {
                            let (key, value) = item.map_err(RedbPersistence::storage_error)?;
                            Ok((key.value().to_vec(), value.value().to_vec()))
                        })
                        .collect(),
                    Err(redb::TableError::TableDoesNotExist(_)) => Ok(Vec::new()),
                    Err(error) => Err(RedbPersistence::storage_error(error)),
                })
                .collect()
            })
            .unwrap()
    }

    #[test]
    fn cancelled_current_identity_refuses_before_probes_and_refunds_its_owner() {
        let (_directory, persistence, _writer, clean) = fixture(1);
        let root = persistence
            .initialize_writer_snapshot(SnapshotId(1), Lsn(1))
            .unwrap();
        let accountant = Arc::new(MemoryAccountant::with_budget(8 * 1024 * 1024));
        let payer = Arc::new(ReadBudget::new(accountant.clone()));
        let memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget> = payer.clone();
        let mut copies = crate::vector_read_memory::admission(memory.clone());
        let cancellation = crate::OwnerReadCancellation::new();
        let checkpoint = crate::executor::bounded::ReadOpenContext::new(
            memory.clone(),
            crate::read_session::ReadSessionOptions::default().limits,
            crate::read_session::read_clock(),
            cancellation.clone(),
        )
        .unwrap();
        let scope = vector_membership_source::enter_read_operation(
            vector_membership_source::MembershipOwner::Writable(persistence.clone()),
            SnapshotId(1),
            memory.clone(),
            &mut copies,
            &mut |_| Ok(()),
            Some(checkpoint.clone()),
        )
        .unwrap();
        let before = durable_records(&persistence);
        cancellation.cancel();
        let (result, work) = crate::database::resident_history_test_seams::observe(|| {
            accepted_identity(
                &root,
                &clean.index,
                &clean.partition_key,
                &row(&clean),
                &mut copies,
                memory.clone(),
            )
        });
        assert!(matches!(result, Err(Error::ReadCancelled)));
        assert_eq!(work.membership_keys_examined, 0);
        assert_eq!(durable_records(&persistence), before);
        drop((scope, checkpoint, copies, memory));
        assert_eq!(accountant.usage().used, 0);
    }
}
