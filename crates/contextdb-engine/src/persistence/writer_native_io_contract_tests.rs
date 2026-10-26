//! Independent producer proofs. Runtime and mutation receipts are still owed.
//! Each family and decisive mutant is mapped in the accompanying evidence.
use super::*;
use crate::funded_error::{OriginalError, OriginalStoragePhase};
use crate::memory_accounting::MemoryAccountant;
use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
use crate::persistence::{RedbPersistence, native_storage_admission::StorageAdmissionScope};
use redb::{CheckedWriterBackendForTest as Checked, NativeStorageBackend, StorageBackend};
use serial_test::serial;

#[derive(Clone, Copy, Debug)]
enum Fault {
    Closed,
    Poisoned,
    Quota,
}
impl Fault {
    fn make(
        self,
    ) -> (
        tempfile::NamedTempFile,
        Option<Arc<LockedWriterFile>>,
        WriterFileView,
    ) {
        let file = tempfile::NamedTempFile::new().unwrap();
        file.as_file().set_len(8).unwrap();
        let owner = LockedWriterFile::lock(file.reopen().unwrap(), None).unwrap();
        let view = owner.view();
        match self {
            Self::Closed => {
                drop(owner);
                (file, None, view)
            }
            Self::Poisoned => {
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let _guard = owner.projected_growth_bytes.lock().unwrap();
                        panic!("poison the actual writer growth mutex");
                    }))
                    .is_err()
                );
                (file, Some(owner), view)
            }
            Self::Quota => {
                owner.set_disk_limit(Some(8)).unwrap();
                (file, Some(owner), view)
            }
        }
    }
    fn expected(self) -> &'static str {
        match self {
            Self::Closed => "writer file ownership is closed",
            Self::Poisoned => "projected file-growth reservation was poisoned",
            Self::Quota => "projected storage growth from 8 to 9 bytes exceeds the 8 byte limit",
        }
    }
    fn assert_cause(self, error: &redb::StorageError) {
        match (self, error) {
            (
                Self::Closed,
                redb::StorageError::Native(redb::NativeColdReadError::BackendIo(
                    redb::NativeIoCause::WriterOwnerClosed,
                )),
            ) => {}
            (
                Self::Poisoned,
                redb::StorageError::Native(redb::NativeColdReadError::BackendIo(
                    redb::NativeIoCause::ProjectedGrowthPoisoned,
                )),
            ) => {}
            (
                Self::Quota,
                redb::StorageError::Native(redb::NativeColdReadError::BackendIo(
                    redb::NativeIoCause::DiskGrowthRefused {
                        current_bytes: 8,
                        projected_bytes: 9,
                        limit_bytes: 8,
                    },
                )),
            ) => {}
            _ => panic!(
                "the actual producer must retain its exact scalar cause: {self:?}, {error:?}"
            ),
        }
    }
}

// A decorator records actual growth calls without allocating. The standard
// door must stay opaque even when its concrete type also has a native capability.
type GrowthLog = std::sync::Mutex<(usize, [u64; 8])>;
#[derive(Debug)]
struct ObservedWriter(WriterFileView, Option<Arc<GrowthLog>>);
impl ObservedWriter {
    fn note_growth(&self, len: u64) {
        if let Some(log) = &self.1 {
            let mut log = log.lock().unwrap();
            let index = log.0;
            if index < log.1.len() {
                log.1[index] = len;
            }
            log.0 += 1;
        }
    }
}
impl StorageBackend for ObservedWriter {
    fn len(&self) -> io::Result<u64> {
        StorageBackend::len(&self.0)
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        StorageBackend::read(&self.0, offset, out)
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        StorageBackend::write(&self.0, offset, data)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.note_growth(len);
        StorageBackend::set_len(&self.0, len)
    }
    fn sync_data(&self) -> io::Result<()> {
        StorageBackend::sync_data(&self.0)
    }
    fn close(&self) -> io::Result<()> {
        StorageBackend::close(&self.0)
    }
    fn scratch_backend(&self) -> io::Result<Option<Box<dyn redb::ScratchBackend>>> {
        StorageBackend::scratch_backend(&self.0)
    }
}

impl NativeStorageBackend for ObservedWriter {
    fn native_len(&self) -> Result<u64, redb::NativeBackendError> {
        self.0.native_len()
    }
    fn native_read(&self, offset: u64, out: &mut [u8]) -> Result<(), redb::NativeBackendError> {
        self.0.native_read(offset, out)
    }
    fn native_write(&self, offset: u64, data: &[u8]) -> Result<(), redb::NativeBackendError> {
        self.0.native_write(offset, data)
    }
    fn native_set_len(&self, len: u64) -> Result<(), redb::NativeBackendError> {
        self.note_growth(len);
        self.0.native_set_len(len)
    }
    fn native_sync_data(&self) -> Result<(), redb::NativeBackendError> {
        self.0.native_sync_data()
    }
}

#[derive(Clone, Copy)]
enum Op {
    Len,
    Read,
    Write,
    Grow,
    Sync,
}
impl Op {
    fn checked(self, backend: &Checked) -> Result<(), redb::StorageError> {
        match self {
            Self::Len => backend.len().map(|_| ()),
            Self::Read => backend.read(0, &mut [0]),
            Self::Write => backend.write(0, &[1]),
            Self::Grow => backend.set_len(9),
            Self::Sync => backend.sync_data(),
        }
    }
    fn legacy(self, view: &WriterFileView) -> io::Result<()> {
        match self {
            Self::Len => StorageBackend::len(view).map(|_| ()),
            Self::Read => StorageBackend::read(view, 0, &mut [0]),
            Self::Write => StorageBackend::write(view, 0, &[1]),
            Self::Grow => StorageBackend::set_len(view, 9),
            Self::Sync => StorageBackend::sync_data(view),
        }
    }
}

// Original construction, not a hand-made NativeIoCause. A wrong Backend arm,
// legacy allocation, lost quota fields or eager failure latch is decisive here.
#[test]
#[serial]
fn actual_writer_causes_are_scalar_and_legacy_payloads_stay_exact() {
    for fault in [Fault::Closed, Fault::Poisoned, Fault::Quota] {
        let operations: &[Op] = if matches!(fault, Fault::Closed) {
            &[Op::Len, Op::Read, Op::Write, Op::Grow, Op::Sync]
        } else {
            &[Op::Grow]
        };
        for &op in operations {
            let (_file, _owner, view) = fault.make();
            let checked = Checked::native(view);
            let (error, _, count) =
                oracle::native_requests_for_test(|| op.checked(&checked).unwrap_err());
            assert_eq!(
                count, 0,
                "finite producer must fail before allocating diagnostic storage"
            );
            fault.assert_cause(&error);
            let (_legacy_file, _legacy_owner, legacy) = fault.make();
            let error = op.legacy(&legacy).unwrap_err();
            let kind = if matches!(fault, Fault::Quota) {
                io::ErrorKind::QuotaExceeded
            } else {
                io::ErrorKind::Other
            };
            assert_eq!(error.kind(), kind);
            assert_eq!(error.to_string(), fault.expected());
            if matches!(fault, Fault::Quota) {
                let source = error
                    .get_ref()
                    .unwrap()
                    .downcast_ref::<DiskGrowthRefusal>()
                    .unwrap();
                assert_eq!(
                    (
                        source.current_bytes,
                        source.projected_bytes,
                        source.limit_bytes
                    ),
                    (8, 9, 8)
                );
            }
            let _ = take_disk_growth_refusal();
        }
    }
    // Scratch is still an ordinary io::Error even on the concrete native type.
    let (_file, _owner, closed) = Fault::Closed.make();
    let error = closed.scratch_backend().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert_eq!(error.to_string(), Fault::Closed.expected());
}

#[test]
#[serial]
fn actual_writer_funded_diagnosis_prepays_and_keeps_original_invoice_through_free() {
    for fault in [Fault::Closed, Fault::Poisoned, Fault::Quota] {
        let (_file, _owner, view) = fault.make();
        let checked = Checked::native(view); // Original backend Box is outside the measured claim.
        let writer = MemoryAccountant::writer_no_limit().unwrap();
        let owner = writer.accountant();
        let funder = writer.prepare_source_funder().unwrap();
        let original = writer.reserve_assembly_original(&funder).unwrap();
        let baseline = owner.usage().used;
        let counter = owner.writer_registration_observation_for_test().4;
        oracle::last_request_for_test(true);
        let (error, gap) = oracle::funded_requests(&owner, || {
            let _observation = oracle::declaration_observation();
            let prepared =
                OriginalError::prepare(&original, "redb", "exact_byte_read", "test").unwrap();
            let prepaid = oracle::last_request_for_test(false);
            assert_eq!(prepaid.2, 1);
            let request = (
                oracle::last_request_pointer_for_test(),
                prepaid.0,
                prepaid.1,
            );
            oracle::watch_exact_free_for_test(request, counter);
            let cause = checked.set_len(9).unwrap_err();
            fault.assert_cause(&cause);
            let error = RedbPersistence::storage_error_funded(prepared, &original, cause);
            assert!(error.source_is_qualified());
            assert!(!error.is_memory_refusal());
            assert_eq!(
                oracle::last_request_for_test(false),
                prepaid,
                "native producer and adapter allocate no text or replacement carrier"
            );
            let retained = owner.usage().used;
            owner.set_budget(Some(retained)).unwrap();
            let public = error.into_public_prepared();
            assert!(
                matches!(&public, contextdb_core::Error::FundedEngine(source) if source.source_is_qualified())
            );
            assert_eq!(
                oracle::last_request_for_test(false),
                prepaid,
                "public transfer allocates nothing"
            );
            assert_eq!(owner.usage().used, retained);
            public
        });
        assert_eq!(gap, 0);
        let bytes = oracle::last_request_for_test(false).0;
        assert_eq!(owner.usage().used, baseline + bytes);
        // Compare owning Display outside the funding measurement.
        let expected = if matches!(fault, Fault::Quota) {
            "disk budget exceeded: allocate storage pages - file is 8 bytes, limit is 8 bytes. Hint: Reduce retained file-backed data or raise DISK_LIMIT before writing more data.".to_owned()
        } else {
            format!(
                "contextdb-engine-storage-io:I/O error: {}",
                fault.expected()
            )
        };
        assert_eq!(error.to_string(), expected);
        drop(error);
        let free = oracle::source_free_for_test(1)[0];
        assert_eq!(free, (baseline + bytes, baseline + bytes, 1));
        oracle::disarm_source_free_watch();
        assert_eq!(owner.usage().used, baseline);
        owner.set_budget(None).unwrap();
        // The actual scope's pre-entry carrier refusal must not invoke the producer.
        let scope = StorageAdmissionScope::writer_original_native(&original).unwrap();
        owner.set_budget(Some(owner.usage().used)).unwrap();
        let entered = std::cell::Cell::new(false);
        let (result, _, requests) = oracle::native_requests_for_test(|| {
            scope.run_original(OriginalStoragePhase::Database, || {
                entered.set(true);
                let prepared =
                    OriginalError::prepare(&original, "redb", "exact_byte_read", "test").unwrap();
                Err::<(), _>(RedbPersistence::storage_error_funded(
                    prepared,
                    &original,
                    checked.set_len(9).unwrap_err(),
                ))
            })
        });
        assert!(!entered.get());
        assert_eq!(requests, 0);
        let refusal = result.unwrap_err();
        assert!(refusal.is_memory_refusal());
        assert!(refusal.source_is_qualified());
        drop(refusal);
        assert_eq!(owner.usage().used, baseline);
        owner.set_budget(None).unwrap();
        // If the producer already returned its scalar, failed diagnostic admission
        // cannot consume or replace that source, including its quota fields.
        let (_again_file, _again_owner, again) = fault.make();
        let again = Checked::native(again);
        let scalar = again.set_len(9).unwrap_err();
        owner.set_budget(Some(baseline)).unwrap();
        let (prepared, _, requests) = oracle::native_requests_for_test(|| {
            OriginalError::prepare(&original, "redb", "exact_byte_read", "test")
        });
        assert!(
            matches!(prepared.unwrap_err(), contextdb_core::read_memory::NativeRefusal::MemoryBudgetExceeded {
            subsystem: "redb", operation: "exact_byte_read", requested_bytes,
            available_bytes: 0, budget_limit_bytes, hint: "test",
        } if requested_bytes == bytes && budget_limit_bytes == baseline)
        );
        assert_eq!(requests, 0);
        fault.assert_cause(&scalar);
        let _ = take_disk_growth_refusal();
    }
}

#[test]
#[serial]
fn actual_legacy_and_os_sources_cannot_borrow_native_or_disk_tls_qualification() {
    let writer = MemoryAccountant::writer_no_limit().unwrap();
    let owner = writer.accountant();
    let funder = writer.prepare_source_funder().unwrap();
    let original = writer.reserve_assembly_original(&funder).unwrap();
    // Each legacy view has the same concrete producer and matching kind/text,
    // but came through the standard door. No wrapper can confer native authority.
    for stale_disk in [false, true] {
        let (_file, _held, view) = Fault::Closed.make();
        let legacy = Checked::legacy(ObservedWriter(view, None));
        let prepared =
            OriginalError::prepare(&original, "redb", "exact_byte_read", "test").unwrap();
        if stale_disk {
            let (_quota_file, _quota_owner, quota) = Fault::Quota.make();
            assert!(quota.native_set_len(9).is_err()); // swallowed actual finite failure
        }
        let cause = legacy.len().unwrap_err();
        assert!(matches!(&cause, redb::StorageError::Io(error)
            if error.kind() == io::ErrorKind::Other && error.to_string() == Fault::Closed.expected()));
        let error = RedbPersistence::storage_error_funded(prepared, &original, cause);
        assert!(
            !error.source_is_qualified(),
            "matching text or a prior quota signal is no certificate"
        );
        assert!(!error.is_memory_refusal());
        let text = error.to_string();
        assert_eq!(
            text,
            if stale_disk {
                "disk budget exceeded: allocate storage pages - file is 8 bytes, limit is 8 bytes. Hint: Reduce retained file-backed data or raise DISK_LIMIT before writing more data.".to_owned()
            } else {
                "contextdb-engine-storage-io:I/O error: writer file ownership is closed".to_owned()
            }
        );
        assert!(take_disk_growth_refusal().is_none());
        let public = error.into_public_prepared();
        assert!(
            matches!(&public, contextdb_core::Error::FundedEngine(source) if !source.source_is_qualified())
        );
    }
    for fault in [Fault::Poisoned, Fault::Quota] {
        let (_file, _held, view) = fault.make();
        let legacy = Checked::legacy(ObservedWriter(view, None));
        let prepared =
            OriginalError::prepare(&original, "redb", "exact_byte_read", "test").unwrap();
        let cause = legacy.set_len(9).unwrap_err();
        assert!(
            matches!(&cause, redb::StorageError::Io(error) if error.to_string() == fault.expected())
        );
        let error = RedbPersistence::storage_error_funded(prepared, &original, cause);
        assert!(
            !error.source_is_qualified(),
            "the legacy producer with identical text remains unqualified"
        );
        assert!(!error.is_memory_refusal());
        assert!(take_disk_growth_refusal().is_none());
    }
    // Obtain the baseline from the same OS operation. No errno/message-size or
    // private io::Error representation is used to qualify it.
    for stale_disk in [false, true] {
        let file = tempfile::NamedTempFile::new().unwrap();
        let baseline = File::open(file.path()).unwrap().set_len(9).unwrap_err();
        let held = LockedWriterFile::from_locked(File::open(file.path()).unwrap(), None).unwrap();
        let native = Checked::native(held.view());
        if stale_disk {
            let (_quota_file, _quota_owner, quota) = Fault::Quota.make();
            assert!(quota.native_set_len(9).is_err());
        }
        let cause = native.set_len(9).unwrap_err();
        assert!(matches!(&cause, redb::StorageError::Io(error)
            if error.kind() == baseline.kind() && error.raw_os_error() == baseline.raw_os_error()
            && error.to_string() == baseline.to_string()));
        let prepared =
            OriginalError::prepare(&original, "redb", "exact_byte_read", "test").unwrap();
        let error = RedbPersistence::storage_error_funded(prepared, &original, cause);
        assert!(
            !error.source_is_qualified(),
            "a raw OS failure is never certified"
        );
        assert!(take_disk_growth_refusal().is_none());
        if !stale_disk {
            let ordinary = RedbPersistence::storage_error(redb::StorageError::Io(baseline));
            assert_eq!(
                error.to_string(),
                ordinary.to_string(),
                "opaque OS Display stays exact"
            );
        }
        // Non-quota I/O poisons the checked handle; it does not retry silently.
        assert!(matches!(native.len(), Err(redb::StorageError::PreviousIo)));
    }
    let (_scratch_file, _scratch_owner, scratch) = Fault::Closed.make();
    let cause = scratch.scratch_backend().unwrap_err();
    let prepared = OriginalError::prepare(&original, "redb", "exact_byte_read", "test").unwrap();
    let error =
        RedbPersistence::storage_error_funded(prepared, &original, redb::StorageError::Io(cause));
    assert!(
        !error.source_is_qualified(),
        "scratch never inherits the ordinary native file certificate"
    );
    drop(error);
    let (_file, _owner, closed) = Fault::Closed.make();
    let native = Checked::native(closed);
    assert!(native.len().is_err()); // swallowed native finite failure, no disk signal
    let prepared = OriginalError::prepare(&original, "redb", "exact_byte_read", "test").unwrap();
    let foreign = redb::StorageError::Io(io::Error::other("writer file ownership is closed"));
    let error = RedbPersistence::storage_error_funded(prepared, &original, foreign);
    assert!(!error.source_is_qualified());
}

#[test]
#[serial]
fn actual_native_and_legacy_quota_refusal_leave_checked_handles_usable() {
    for native in [false, true] {
        let (file, owner, view) = Fault::Quota.make();
        let owner = owner.unwrap();
        let backend = if native {
            Checked::native(view)
        } else {
            Checked::legacy(view)
        };
        let cause = backend.set_len(9).unwrap_err();
        if native {
            Fault::Quota.assert_cause(&cause);
        } else {
            assert!(
                matches!(cause, redb::StorageError::Io(error) if error.kind() == io::ErrorKind::QuotaExceeded)
            );
        }
        assert_eq!(backend.len().unwrap(), 8);
        assert_eq!(file.as_file().metadata().unwrap().len(), 8);
        assert_eq!(owner.projected_growth_bytes(), 0);
        owner.set_disk_limit(Some(9)).unwrap();
        backend.set_len(9).unwrap();
        assert_eq!(backend.len().unwrap(), 9);
        assert_eq!(owner.projected_growth_bytes(), 0);
        backend.write(8, &[7]).unwrap();
        let mut byte = [0];
        backend.read(8, &mut byte).unwrap();
        assert_eq!(byte, [7]);
        let _ = take_disk_growth_refusal();
    }
}

#[test]
#[serial]
fn real_builder_routes_refuse_growth_without_publication_and_retry_the_same_payload() {
    use redb::ReadableDatabase;
    const TABLE: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("writer_io");
    for native in [false, true] {
        let file = tempfile::NamedTempFile::new().unwrap();
        let owner = LockedWriterFile::lock(file.reopen().unwrap(), None).unwrap();
        let provider: redb::ExternalSnapshotProvider = Arc::new(|| Ok(Vec::new()));
        let mut builder = redb::Database::builder();
        builder.set_cache_size(0);
        let growth = Arc::new(std::sync::Mutex::new((0, [0; 8])));
        let backend = ObservedWriter(owner.view(), Some(Arc::clone(&growth)));
        let database = if native {
            builder
                .create_with_native_backend_and_external_snapshots(backend, provider)
                .unwrap()
        } else {
            builder
                .create_with_backend_and_external_snapshots(backend, provider)
                .unwrap()
        };
        let write = database.begin_write().unwrap();
        {
            let mut table = write.open_table(TABLE).unwrap();
            table.insert(1, &[3][..]).unwrap();
        }
        write.commit().unwrap();
        let pinned = database.begin_read().unwrap();
        let before = pinned.storage_snapshot_identity();
        let size = file.as_file().metadata().unwrap().len();
        let payload = [7u8; 128];
        *growth.lock().unwrap() = (0, [0; 8]);
        owner.set_disk_limit(Some(size)).unwrap();
        let write = database.begin_write().unwrap();
        // Crossing the existing file with small records forces a single-page
        // request: preferred geometric growth exceeds its minimum retry.
        let mut failed_key = None;
        let mut source = None;
        {
            let mut table = write.open_table(TABLE).unwrap();
            for key in 2..=size / payload.len() as u64 + 2 {
                match table.insert(key, payload.as_slice()) {
                    Ok(_) => {}
                    Err(error) => {
                        failed_key = Some(key);
                        source = Some(error);
                        break;
                    }
                }
            }
        }
        drop(write); // The complete staged batch is refused, never published.
        let failed_key = failed_key.expect("payload bytes alone exceed the file capacity");
        let source: redb::Error = source.unwrap().into();
        let attempts = *growth.lock().unwrap();
        assert_eq!(
            attempts.0, 2,
            "one preferred growth and exactly one minimum retry"
        );
        assert!(attempts.1[0] > attempts.1[1] && attempts.1[1] > size);
        if native {
            assert!(
                matches!(&source, redb::Error::Native(redb::NativeColdReadError::BackendIo(
                redb::NativeIoCause::DiskGrowthRefused { current_bytes, projected_bytes, limit_bytes }))
                if *current_bytes == size && *limit_bytes == size && *projected_bytes == attempts.1[1])
            );
        } else {
            let redb::Error::Io(error) = &source else {
                panic!("legacy quota remains I/O");
            };
            assert_eq!(error.kind(), io::ErrorKind::QuotaExceeded);
            let payload = error
                .get_ref()
                .unwrap()
                .downcast_ref::<DiskGrowthRefusal>()
                .unwrap();
            assert_eq!(
                (
                    payload.current_bytes,
                    payload.projected_bytes,
                    payload.limit_bytes
                ),
                (size, attempts.1[1], size)
            );
        }
        assert_eq!(file.as_file().metadata().unwrap().len(), size);
        assert_eq!(
            database.begin_read().unwrap().storage_snapshot_identity(),
            before
        );
        assert!(
            database
                .begin_read()
                .unwrap()
                .open_table(TABLE)
                .unwrap()
                .get(2)
                .unwrap()
                .is_none()
        );
        owner.set_disk_limit(None).unwrap();
        let write = database.begin_write().unwrap();
        {
            let mut table = write.open_table(TABLE).unwrap();
            for key in 2..=failed_key {
                table.insert(key, payload.as_slice()).unwrap();
            }
        }
        write.commit().unwrap();
        let read = database.begin_read().unwrap();
        let table = read.open_table(TABLE).unwrap();
        assert_eq!(table.get(1).unwrap().unwrap().value(), &[3]);
        assert_eq!(
            table.get(failed_key).unwrap().unwrap().value(),
            payload.as_slice()
        );
        assert!(pinned.open_table(TABLE).unwrap().get(2).unwrap().is_none());
        drop(table);
        drop(read);
        drop(pinned);
        drop(database);
        drop(owner);
        let reopened = redb::Database::open(file.path()).unwrap();
        let read = reopened.begin_read().unwrap();
        let table = read.open_table(TABLE).unwrap();
        assert_eq!(
            table.get(failed_key).unwrap().unwrap().value(),
            payload.as_slice()
        );
        let _ = take_disk_growth_refusal();
    }
}
