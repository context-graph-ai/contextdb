//! Store-local erasure publication watermarks, independent of row-life history.
use super::*;
use contextdb_core::read_memory::{ReadCredit, ReadMemoryBudget};

pub(super) const TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("read_erasure_frontiers");
// No UTF-8 table name can alias this key; named probes borrow the admitted name.
const GLOBAL: &[u8] = &[0xff];

fn corrupt(reason: &str) -> Error {
    Error::StoreCorrupted {
        path: "read_erasure_frontiers".into(),
        reason: reason.into(),
    }
}
fn decode(bytes: &[u8]) -> Result<Lsn> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| corrupt("an erasure frontier has invalid width"))?;
    Ok(Lsn(u64::from_le_bytes(bytes)))
}
pub(super) fn initialize(write: &redb::WriteTransaction) -> Result<()> {
    write
        .open_table(TABLE)
        .map_err(RedbPersistence::storage_error)?
        .insert(GLOBAL, 0_u64.to_le_bytes().as_slice())
        .map_err(RedbPersistence::storage_error)?;
    Ok(())
}
/// Both independent physical summaries are authenticated by the existing
/// Configuration tail. No encoded root shape or public format marker changes.
fn binding(totals: [u8; 32], erasures: [u8; 32]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"contextdb-content-and-erasure-summaries");
    hash.update(&totals);
    hash.update(&erasures);
    *hash.finalize().as_bytes()
}
pub(super) fn binding_in_write(write: &redb::WriteTransaction) -> Result<[u8; 32]> {
    let totals = write
        .table_root_fingerprint(content_size::TOTALS)
        .map_err(RedbPersistence::storage_error)?
        .unwrap_or([0; 32]);
    let erasures = write
        .table_root_fingerprint(TABLE)
        .map_err(RedbPersistence::storage_error)?
        .ok_or_else(|| corrupt("canonical erasure controls are missing"))?;
    Ok(binding(totals, erasures))
}
pub(super) fn binding_in_read(read: &redb::ReadTransaction) -> Result<[u8; 32]> {
    let totals = read
        .table_root_fingerprint(content_size::TOTALS)
        .map_err(RedbPersistence::storage_error)?
        .unwrap_or([0; 32]);
    let erasures = read
        .table_root_fingerprint(TABLE)
        .map_err(RedbPersistence::storage_error)?
        .ok_or_else(|| corrupt("canonical erasure controls are missing"))?;
    Ok(binding(totals, erasures))
}
pub(super) fn verify_in_write(write: &redb::WriteTransaction) -> Result<()> {
    if binding_in_write(write)? != content_identity::content_size_root_in_write(write)? {
        return Err(corrupt(
            "canonical erasure controls changed outside publication",
        ));
    }
    let table = write
        .open_table(TABLE)
        .map_err(RedbPersistence::storage_error)?;
    table
        .get(GLOBAL)
        .map_err(RedbPersistence::storage_error)?
        .map(|value| decode(value.value()))
        .transpose()?
        .ok_or_else(|| corrupt("the required global erasure frontier is missing"))?;
    Ok(())
}
pub(super) fn raise(write: &redb::WriteTransaction, table: &str, lsn: Lsn) -> Result<()> {
    let mut summaries = write
        .open_table(TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let global = summaries
        .get(GLOBAL)
        .map_err(RedbPersistence::storage_error)?
        .map(|value| decode(value.value()))
        .transpose()?
        .ok_or_else(|| corrupt("the required global erasure frontier is missing"))?;
    if lsn > global {
        summaries
            .insert(GLOBAL, lsn.0.to_le_bytes().as_slice())
            .map_err(RedbPersistence::storage_error)?;
    }
    let prior = summaries
        .get(table.as_bytes())
        .map_err(RedbPersistence::storage_error)?
        .map(|value| decode(value.value()))
        .transpose()?
        .unwrap_or(Lsn(0));
    if lsn > prior {
        summaries
            .insert(table.as_bytes(), lsn.0.to_le_bytes().as_slice())
            .map_err(RedbPersistence::storage_error)?;
    }
    Ok(())
}
pub(super) fn fold_lineage(
    write: &redb::WriteTransaction,
    key: &str,
    value: Option<&[u8]>,
) -> Result<()> {
    if let Some(value) = value
        && let Some((table, frontier)) =
            crate::database::Database::erasure_frontier_for_write(key, value)?
    {
        raise(write, &table, frontier)?;
    }
    Ok(())
}

// The same captured view authenticates descriptors, Configuration and selected frontiers.
fn native_binding(read: &redb::NativeReadView) -> Result<[u8; 32]> {
    let totals = read
        .table_root_fingerprint(content_size::TOTALS)
        .map_err(RedbPersistence::storage_error)?
        .unwrap_or([0; 32]);
    let erasures = read
        .table_root_fingerprint(TABLE)
        .map_err(RedbPersistence::storage_error)?
        .ok_or_else(|| corrupt("canonical erasure controls are missing"))?;
    Ok(binding(totals, erasures))
}
fn native_global(read: &redb::NativeReadView) -> Result<Lsn> {
    if native_binding(read)? != content_identity::content_size_root_in_native_read(read)? {
        return Err(corrupt(
            "erasure controls are not bound to the committed root",
        ));
    }
    read.get_native_byte_value(TABLE.name(), GLOBAL)
        .map_err(RedbPersistence::storage_error)?
        .map(|value| decode(value.value()))
        .transpose()?
        .ok_or_else(|| corrupt("the required global erasure frontier is missing"))
}
fn native_failure(failure: native_storage_admission::NativeStorageFailure) -> Error {
    use contextdb_core::read_memory::NativeRefusal;
    match failure {
        native_storage_admission::NativeStorageFailure::Unqualified(error) => error,
        native_storage_admission::NativeStorageFailure::Refused(refusal) => match refusal {
            NativeRefusal::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
            } => Error::MemoryBudgetExceeded {
                subsystem: subsystem.into(),
                operation: operation.into(),
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint: hint.into(),
            },
            NativeRefusal::ProcessMemoryBudgetExceeded {
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
            } => Error::MemoryBudgetExceeded {
                subsystem: "process_native_ceiling".into(),
                operation: operation.into(),
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint: hint.into(),
            },
            NativeRefusal::NativeUnsupported {
                subsystem,
                operation,
            } => Error::ReadFailure(
                contextdb_core::read_contract::ReadFailure::owner_route_unsupported(
                    contextdb_core::read_memory::native_unsupported_inspection(
                        subsystem, operation,
                    ),
                ),
            ),
            NativeRefusal::Other(reason) => Error::Other(reason.into()),
        },
    }
}

impl RedbPersistence {
    pub(crate) fn global_erasure_frontier(&self, memory: Arc<dyn ReadMemoryBudget>) -> Result<Lsn> {
        let admission = native_storage_admission::StorageAdmissionScope::reader_operation(memory)
            .map_err(native_failure)?;
        admission
            .run_native(|| {
                self.with_db_handle(|db| {
                    let read = db
                        .begin_native_publication_read()
                        .map_err(Self::storage_error)?;
                    native_global(&read)
                })
            })
            .map_err(native_failure)
    }
    pub(crate) fn decode_erasure_frontier_control(bytes: &[u8]) -> Result<Lsn> {
        decode(bytes)
    }
    /// Read one fresh committed view, including its authenticated absence
    /// proofs. The caller holds the table-scoped erasure publication guard.
    pub(crate) fn validate_erasure_frontiers(
        &self,
        scope: &crate::executor::ReadPublicationScope,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<()> {
        let admission = native_storage_admission::StorageAdmissionScope::reader_operation(memory)
            .map_err(native_failure)?;
        admission
            .run_native(|| {
                self.with_db_handle(|db| {
                    scope.check_publication_budget()?;
                    let read = db
                        .begin_native_publication_read()
                        .map_err(Self::storage_error)?;
                    let global = native_global(&read)?;
                    if scope.tables.contains("*") {
                        #[cfg(any(test, feature = "test-seams"))]
                        crate::writer_query_test_seams::note_erasure_frontier_probe();
                        scope.check_erasure_frontier("*", global)?;
                    } else {
                        for table in &scope.tables {
                            scope.check_publication_budget()?;
                            #[cfg(any(test, feature = "test-seams"))]
                            crate::writer_query_test_seams::note_erasure_frontier_probe();
                            let frontier = read
                                .get_native_byte_value(TABLE.name(), table.as_bytes())
                                .map_err(Self::storage_error)?
                                .map(|value| decode(value.value()))
                                .transpose()?
                                .unwrap_or(Lsn(0));
                            if frontier > global {
                                return Err(corrupt(
                                    "a named erasure frontier exceeds the global frontier",
                                ));
                            }
                            scope.check_erasure_frontier(table, frontier)?;
                        }
                    }
                    scope.check_publication_budget()
                })
            })
            .map_err(native_failure)
    }
    pub(crate) fn copy_erasure_frontier(&self, table: &str, lsn: Lsn) -> Result<()> {
        self.with_db(|db| {
            let write = Self::begin_history_write(db)?;
            raise(&write, table, lsn)?;
            Self::raise_noncanonical_writer_floors_in_write(&write, TxId(0), lsn, RowId(0))?;
            Self::apply_content_identity_mutations_in_write(
                &write,
                &[],
                &mut self.proof_admission(),
            )?;
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(Self::storage_error)
        })
    }
}
impl WriterSnapshot {
    /// The source and destination preserve local LSN coordinates. Copy one
    /// admitted table name at a time, never reconstruct DISCARD from lineage.
    pub(crate) fn copy_erasure_frontiers_into(
        &self,
        destination: &RedbPersistence,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<()> {
        graph_read::with_pages(memory.clone(), || {
            let _control = ReadCredit::try_new(
                memory.clone(),
                TABLE.name().len() + CONTENT_IDENTITY_ROOTS_TABLE.name().len(),
                "reader",
                "export_erasure_frontiers",
                "Raise the reader memory limit to export local erasure controls.",
            )?;
            let read = self.owner_read()?;
            if binding_in_read(&read)? != content_identity::content_size_root_in_read(&read)? {
                return Err(corrupt("snapshot erasure controls are not authenticated"));
            }
            let summaries = read
                .open_table(TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let global = summaries
                .get(GLOBAL)
                .map_err(RedbPersistence::storage_error)?
                .map(|value| decode(value.value()))
                .transpose()?
                .ok_or_else(|| corrupt("snapshot global erasure frontier is missing"))?;
            for entry in summaries.iter().map_err(RedbPersistence::storage_error)? {
                let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                if key.value() == GLOBAL {
                    continue;
                }
                let frontier = decode(value.value())?;
                if frontier > global {
                    return Err(corrupt("snapshot erasure maxima disagree"));
                }
                let table = std::str::from_utf8(key.value())
                    .map_err(|_| corrupt("invalid erasure table name"))?;
                destination.copy_erasure_frontier(table, frontier)?;
            }
            Ok(())
        })
    }
}
