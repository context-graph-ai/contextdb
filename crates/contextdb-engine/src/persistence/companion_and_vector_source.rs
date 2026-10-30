use super::*;

impl Drop for CompanionGuard {
    fn drop(&mut self) {
        if let Ok(state) = self.state.lock() {
            release_companion_range_lock(&state.file, COMPANION_OWNER_LOCK_OFFSET);
            release_companion_range_lock(&state.file, COMPANION_CLAIM_WINDOW_LOCK_OFFSET);
            unlock_file(&state.file);
        }
    }
}

pub(super) fn write_companion_region(
    file: &mut File,
    offset: u64,
    bytes: &[u8],
) -> std::io::Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(bytes)
}

/// Durable companion updates use this primitive for the inactive slot's
/// payload. The crash checkpoint cannot be moved away from the write without
/// replacing the operation the companion writer is required to call.
pub(super) fn write_companion_slot_payload(
    file: &mut File,
    offset: u64,
    payload: &[u8],
) -> std::io::Result<()> {
    write_companion_region(file, offset, payload)?;
    #[cfg(any(test, feature = "test-seams"))]
    read_persistence_test_scaffold::companion_write_checkpoint(
        read_persistence_test_scaffold::CompanionWriteBoundary::SlotPayloadWrite,
    );
    Ok(())
}

pub(super) fn sync_companion_slot_payload(file: &File) -> std::io::Result<()> {
    file.sync_data()?;
    #[cfg(any(test, feature = "test-seams"))]
    read_persistence_test_scaffold::companion_write_checkpoint(
        read_persistence_test_scaffold::CompanionWriteBoundary::SlotPayloadSync,
    );
    Ok(())
}

pub(super) fn write_companion_slot_checksum(
    file: &mut File,
    offset: u64,
    checksum: &[u8; 32],
) -> std::io::Result<()> {
    write_companion_region(file, offset, checksum)?;
    #[cfg(any(test, feature = "test-seams"))]
    read_persistence_test_scaffold::companion_write_checkpoint(
        read_persistence_test_scaffold::CompanionWriteBoundary::SlotChecksumWrite,
    );
    Ok(())
}

pub(super) fn sync_companion_slot_checksum(file: &File) -> std::io::Result<()> {
    file.sync_data()?;
    #[cfg(any(test, feature = "test-seams"))]
    read_persistence_test_scaffold::companion_write_checkpoint(
        read_persistence_test_scaffold::CompanionWriteBoundary::SlotChecksumSync,
    );
    Ok(())
}

pub(super) fn write_companion_active_slot(
    file: &mut File,
    offset: u64,
    active_slot: &[u8],
) -> std::io::Result<()> {
    write_companion_region(file, offset, active_slot)?;
    #[cfg(any(test, feature = "test-seams"))]
    read_persistence_test_scaffold::companion_write_checkpoint(
        read_persistence_test_scaffold::CompanionWriteBoundary::ActiveSlotWrite,
    );
    Ok(())
}

pub(super) fn sync_companion_active_slot(file: &File) -> std::io::Result<()> {
    file.sync_data()?;
    #[cfg(any(test, feature = "test-seams"))]
    read_persistence_test_scaffold::companion_write_checkpoint(
        read_persistence_test_scaffold::CompanionWriteBoundary::ActiveSlotSync,
    );
    Ok(())
}

pub(super) fn write_companion_record(
    file: &mut File,
    slot: usize,
    record: &EncodedCompanionRecord,
) -> std::io::Result<()> {
    #[cfg(any(test, feature = "test-seams"))]
    crate::read_probe::note_persistence_companion_mutation();
    let slot_offset = companion_slot_offset(slot)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let payload_length = u32::try_from(record.checksummed_payload.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "companion payload is too large",
        )
    })?;
    if record.checksummed_payload.len() > COMPANION_SLOT_PAYLOAD_CAPACITY {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "companion payload exceeds its fixed slot",
        ));
    }
    let mut slot_payload = vec![0_u8; 4 + COMPANION_SLOT_PAYLOAD_CAPACITY];
    slot_payload[..4].copy_from_slice(&payload_length.to_le_bytes());
    slot_payload[4..4 + record.checksummed_payload.len()]
        .copy_from_slice(&record.checksummed_payload);
    write_companion_slot_payload(file, slot_offset as u64, &slot_payload)?;
    sync_companion_slot_payload(file)?;
    write_companion_slot_checksum(
        file,
        (slot_offset + 4 + COMPANION_SLOT_PAYLOAD_CAPACITY) as u64,
        &record.stored_checksum,
    )?;
    sync_companion_slot_checksum(file)?;
    let selector = companion_selector_bytes(slot, record.fields.generation)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    write_companion_active_slot(file, COMPANION_SELECTOR_OFFSET as u64, &selector)?;
    sync_companion_active_slot(file)
}

pub(super) fn write_companion_slot_without_selector(
    file: &mut File,
    slot: usize,
    payload: &[u8],
    checksum: &[u8; 32],
) -> std::io::Result<()> {
    let slot_offset = companion_slot_offset(slot)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let payload_length = u32::try_from(payload.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "companion payload is too large",
        )
    })?;
    if payload.len() > COMPANION_SLOT_PAYLOAD_CAPACITY {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "companion payload exceeds its fixed slot",
        ));
    }
    let mut slot_payload = vec![0_u8; 4 + COMPANION_SLOT_PAYLOAD_CAPACITY];
    slot_payload[..4].copy_from_slice(&payload_length.to_le_bytes());
    slot_payload[4..4 + payload.len()].copy_from_slice(payload);
    write_companion_region(file, slot_offset as u64, &slot_payload)?;
    file.sync_data()?;
    write_companion_region(
        file,
        (slot_offset + 4 + COMPANION_SLOT_PAYLOAD_CAPACITY) as u64,
        checksum,
    )?;
    file.sync_data()
}

pub(super) fn write_companion_pending_slot(
    file: &mut File,
    slot: usize,
    pending: &PendingReplacement,
) -> std::io::Result<()> {
    write_companion_slot_without_selector(
        file,
        slot,
        &pending.checksummed_payload,
        &pending.stored_checksum,
    )
}

pub(super) fn write_companion_record_copy(
    file: &mut File,
    slot: usize,
    record: &EncodedCompanionRecord,
) -> std::io::Result<()> {
    write_companion_slot_without_selector(
        file,
        slot,
        &record.checksummed_payload,
        &record.stored_checksum,
    )
}

impl CacheInstallation {
    pub(super) fn record(&self, capacity: usize, reserve: usize) {
        self.reserve
            .store(reserve, std::sync::atomic::Ordering::Release);
        self.capacity
            .store(capacity, std::sync::atomic::Ordering::Release);
    }

    pub(super) fn capacity(&self) -> Option<usize> {
        Some(self.capacity.load(std::sync::atomic::Ordering::Acquire)).filter(|bytes| *bytes != 0)
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature"
        )
    )]
    pub(super) fn reserve(&self) -> usize {
        self.reserve.load(std::sync::atomic::Ordering::Acquire)
    }
}

/// The exact fixed source reservation, independent of the page-cache declaration.
pub(crate) fn read_source_fixed_reservation_bytes() -> usize {
    64 * 1024
        + content_identity::READ_VERIFIER_MEMORY_BYTES
        + std::mem::size_of::<Lsn>()
        + std::mem::size_of::<content_identity::ValidatedHistoryOpenControl>()
}

/// File-reader cache policy. The displayed default is 14%; its exact capacity
/// is one seventh of the budget left after fixed source reservations. Explicit
/// percentages select a share of that same remainder, without a fixed byte cap.
pub(crate) fn redb_reader_cache_bytes(budget: usize, percent: Option<u8>) -> usize {
    let available = budget.saturating_sub(read_source_fixed_reservation_bytes());
    match percent {
        None => available / 7,
        Some(percent) => {
            // Decompose before multiplying so valid percentages cannot overflow.
            // Invalid declarations are refused before any storage open.
            let percent = usize::from(percent.min(100));
            available / 100 * percent + available % 100 * percent / 100
        }
    }
}

pub(crate) fn redb_cache_bytes_for_limit(limit: Option<usize>) -> usize {
    let budget = limit.unwrap_or_else(|| {
        usize::try_from(
            crate::read_session::ReadSessionOptions::default()
                .limits
                .memory,
        )
        .unwrap_or(usize::MAX)
    });
    redb_reader_cache_bytes(budget, None)
}

/// The writer's serving cache uses the reader default over its declared budget.
/// Dirty source pages keep their separate original writer payer.
pub(crate) fn redb_writer_cache_bytes_for_limit(limit: Option<usize>) -> usize {
    redb_reader_cache_bytes(
        limit.unwrap_or_else(crate::memory_accounting::effective_process_ceiling),
        None,
    )
}

/// Read-only test seam exposing the writer's page-cache reserve for a
/// declared `MEMORY_LIMIT`, so a test that opens two databases under
/// different budgets can account for their different reserves instead of
/// assuming they cancel. Production code never calls this; it calls
/// `redb_writer_cache_bytes_for_limit` directly.
#[cfg(any(test, feature = "test-seams"))]
pub fn redb_writer_cache_bytes_for_limit_for_test(limit: Option<usize>) -> usize {
    redb_writer_cache_bytes_for_limit(limit)
}

/// Exact existing page-cap and fixed source-reservation components.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn read_source_cache_reservation_for_limit_for_test(limit: Option<usize>) -> (usize, usize) {
    (
        redb_cache_bytes_for_limit(limit),
        read_source_fixed_reservation_bytes(),
    )
}

pub(super) fn redb_builder() -> redb::Builder {
    redb_builder_with_cache(REDB_CACHE_BYTES)
}

pub(super) fn redb_builder_with_cache(cache_bytes: usize) -> redb::Builder {
    let mut builder = redb::Builder::new();
    builder.set_cache_size(cache_bytes);
    builder.set_deferred_free_budget(
        STORAGE_MAINTENANCE_PAGE_BATCH,
        STORAGE_MAINTENANCE_PAGE_BATCH,
    );
    // An active writer reclaims at least what each commit frees, so a long-lived writer with no
    // maintenance worker cannot accumulate an ever-growing deferred-free backlog.
    builder.set_keep_pace_reclaim(true);
    builder
}

/// Arm the one-shot test injection seam: the NEXT `compact()` call on THIS
/// thread fails to reopen after closing the handle. Production-dead —
/// nothing but a test calls it.
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn arm_handle_recycle_reopen_fault_for_test() {
    HANDLE_RECYCLE_REOPEN_FAULT.with(|f| f.set(true));
}

/// Consume the armed injection flag, if any. Reads a thread-local that is
/// never set outside a test, so the production path takes the `false` arm.
pub(super) fn take_handle_recycle_reopen_fault_for_test() -> bool {
    HANDLE_RECYCLE_REOPEN_FAULT.with(|f| f.replace(false))
}

#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn arm_received_schema_pre_commit_fault_for_test() {
    RECEIVED_SCHEMA_PRE_COMMIT_FAULT.with(|fault| fault.set(true));
}

pub(super) fn take_received_schema_pre_commit_fault_for_test() -> bool {
    RECEIVED_SCHEMA_PRE_COMMIT_FAULT.with(|fault| fault.replace(false))
}

/// Arm the one-shot received-schema side-effect checkpoint fault for this
/// test thread. This seam is absent from production builds.
#[cfg(test)]
pub(crate) fn arm_received_schema_side_effect_persistence_fault_for_test() {
    RECEIVED_SCHEMA_SIDE_EFFECT_PERSISTENCE_FAULT.with(|fault| fault.set(true));
}

#[cfg(test)]
pub(super) fn take_received_schema_side_effect_persistence_fault_for_test() -> bool {
    RECEIVED_SCHEMA_SIDE_EFFECT_PERSISTENCE_FAULT.with(|fault| fault.replace(false))
}

#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn arm_authoritative_purge_point_remove_persistence_failure_for_test() {
    AUTHORITATIVE_PURGE_PRE_COMMIT_FAULT.with(|fault| fault.set(true));
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn take_authoritative_purge_point_remove_persistence_failure_for_test() -> bool {
    AUTHORITATIVE_PURGE_PRE_COMMIT_FAULT.with(|fault| fault.replace(false))
}

#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from fixtures.rs"
    )
)]
pub(crate) fn arm_erasure_first_table_fault_for_test() {
    ERASURE_FIRST_TABLE_FAULT.with(|state| state.set((true, false)));
}
#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from fixtures.rs"
    )
)]
pub(crate) fn erasure_first_table_fault_reached_for_test() -> bool {
    ERASURE_FIRST_TABLE_FAULT.with(|state| state.get().1)
}
#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
pub(super) fn erasure_first_table_staged_for_test() -> Result<()> {
    let armed = ERASURE_FIRST_TABLE_FAULT.with(|state| {
        let (armed, reached) = state.get();
        if armed {
            state.set((false, true));
        } else {
            state.set((false, reached));
        }
        armed
    });
    if armed {
        Err(Error::Other(
            "storage error: erasure first table staged fault injected".into(),
        ))
    } else {
        Ok(())
    }
}

pub(super) fn policy_hold_index_key(key: &str) -> bool {
    key.starts_with("sync_policy_hold_select.v1.")
        || key.starts_with("sync_policy_hold_indexed.v1.")
        || key.starts_with("sync_policy_hold_released.v1.")
}

pub(super) fn sync_source_kind_to_u8(kind: SyncSourceKind) -> u8 {
    match kind {
        SyncSourceKind::Pulled => 0,
        SyncSourceKind::AcceptedLocal => 1,
        SyncSourceKind::AcceptedLocalPending => 2,
    }
}
impl PersistenceBytes {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn try_new_for(
        accountant: crate::memory_accounting::AccountantHandle,
        bytes: usize,
        component: &str,
        operation: &str,
        hint: &str,
    ) -> Result<Self> {
        accountant.try_allocate_for(bytes, component, operation, hint)?;
        Ok(Self { accountant, bytes })
    }
    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }
}
impl Drop for PersistenceBytes {
    fn drop(&mut self) {
        self.accountant.release(self.bytes);
    }
}
impl PersistenceProofOwner {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare(
        accountant: crate::memory_accounting::StoreAccountantHandle,
    ) -> Result<Arc<Self>> {
        debug_assert!(accountant.is_writer(), "only raw writers prepare an owner");
        let mut owner = Self {
            // The original tagged handle is the sole counted custody. Keep
            // the existing field/layout without a duplicate writer use.
            writer_use: None,
            accountant: Some(accountant),
            control: None,
        };
        let bytes = crate::memory_accounting::writer_adapter_layout(&owner).size();
        owner.control = Some(crate::memory_accounting::ControlCredit::admit_from_handle(
            owner
                .accountant
                .as_ref()
                .expect("prepared owner has its funder"),
            bytes,
            "vector_maintenance",
            "persistence_proof_control",
            "Raise MEMORY_LIMIT for this fixed writer control.",
        )?);
        Ok(Arc::new(owner))
    }
}
impl Drop for PersistenceProofOwner {
    fn drop(&mut self) {
        struct Cleanup<'a> {
            owner: &'a mut PersistenceProofOwner,
            next: u8,
            refund: Option<crate::memory_accounting::PendingControlRefund>,
        }
        impl Cleanup<'_> {
            fn finish(&mut self) {
                while self.next < 3 {
                    let step = self.next;
                    self.next += 1;
                    match step {
                        0 => {
                            drop(self.owner.accountant.take());
                            #[cfg(test)]
                            RETURNED_LOADER_OWNER_DROP_PANIC_FOR_TEST.with(|flag| {
                                if flag.replace(false) {
                                    panic!("CDB127 original-owner cleanup unwind");
                                }
                            });
                        }
                        1 => drop(self.owner.writer_use.take()),
                        2 => drop(self.refund.take()),
                        _ => unreachable!(),
                    }
                }
            }
        }
        impl Drop for Cleanup<'_> {
            fn drop(&mut self) {
                self.finish();
            }
        }
        let refund = self
            .control
            .take()
            .map(crate::memory_accounting::ControlCredit::prepare);
        let mut cleanup = Cleanup {
            owner: self,
            next: 0,
            refund,
        };
        cleanup.finish();
    }
}
impl ProofAccountantSlot {
    pub(super) const MASK: usize = 0b11;
    pub(super) const RAW_READER: usize = 0;
    pub(super) const RAW_WRITER: usize = 1;
    pub(super) const READY_WRITER: usize = 2;

    pub(super) fn attached(accountant: crate::memory_accounting::AccountantHandle) -> Self {
        // Role discovery and checked retention are one exclusive-domain-gate
        // snapshot. Into-raw moves that exact Arc/use into the slot. A rejected
        // OnceLock set drops only this attempted slot after the gate is gone.
        let handle = crate::memory_accounting::StoreAccountantHandle::new(accountant);
        Self(std::sync::atomic::AtomicPtr::new(
            handle.into_owned_raw().cast(),
        ))
    }

    pub(super) fn raw_handle_pointer(
        pointer: *mut (),
    ) -> *mut crate::memory_accounting::MemoryAccountant {
        debug_assert!(matches!(
            pointer.addr() & Self::MASK,
            Self::RAW_READER | Self::RAW_WRITER
        ));
        // Bits 0/1 already match StoreAccountantHandle's immutable role tag.
        pointer.cast()
    }

    pub(super) fn ready_pointer(pointer: *mut ()) -> *mut PersistenceProofOwner {
        debug_assert_eq!(pointer.addr() & Self::MASK, Self::READY_WRITER);
        pointer.map_addr(|address| address & !Self::MASK).cast()
    }

    pub(super) fn accountant(&self) -> crate::memory_accounting::AccountantHandle {
        let pointer = self.0.load(Ordering::Acquire);
        match pointer.addr() & Self::MASK {
            Self::READY_WRITER => {
                // Ready is terminal. Borrowing self keeps this owner live.
                unsafe {
                    (&*Self::ready_pointer(pointer))
                        .accountant
                        .as_ref()
                        .expect("ready owner has its funder")
                        .as_handle()
                }
            }
            Self::RAW_READER | Self::RAW_WRITER => {
                // A writer winner owns this same accountant before publishing
                // and consuming raw custody. A reader never transitions.
                let original = std::mem::ManuallyDrop::new(unsafe {
                    crate::memory_accounting::StoreAccountantHandle::from_owned_raw(
                        Self::raw_handle_pointer(pointer),
                    )
                });
                original.as_handle()
            }
            _ => unreachable!("invalid proof-accountant slot state"),
        }
    }

    pub(super) fn original_handle_for(
        &self,
        accountant: &crate::memory_accounting::AccountantHandle,
    ) -> Option<crate::memory_accounting::StoreAccountantHandle> {
        let pointer = self.0.load(Ordering::Acquire);
        match pointer.addr() & Self::MASK {
            Self::READY_WRITER => {
                let handle = unsafe {
                    (&*Self::ready_pointer(pointer))
                        .accountant
                        .as_ref()
                        .expect("ready owner has its funder")
                };
                handle.same_accountant(accountant).then(|| handle.clone())
            }
            Self::RAW_READER | Self::RAW_WRITER => {
                let decoded = pointer
                    .map_addr(|address| address & !0b111)
                    .cast::<crate::memory_accounting::MemoryAccountant>();
                if !std::ptr::eq(decoded, accountant.as_ptr()) {
                    return None;
                }
                // Even a delayed observation after publication is pinned by
                // the winner's same-accountant, same-original-epoch chain.
                Some(unsafe {
                    crate::memory_accounting::StoreAccountantHandle::borrow_owned_raw(
                        Self::raw_handle_pointer(pointer),
                    )
                })
            }
            _ => unreachable!("invalid proof-accountant slot state"),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare(&self) -> Result<()> {
        let old = self.0.load(Ordering::Acquire);
        match old.addr() & Self::MASK {
            Self::RAW_READER | Self::READY_WRITER => return Ok(()),
            Self::RAW_WRITER => {}
            _ => unreachable!("invalid proof-accountant slot state"),
        }
        #[cfg(test)]
        RETURNED_LOADER_AFTER_RAW_OBSERVATION_FOR_TEST.with(|hook| {
            let callback = hook.borrow_mut().take();
            if let Some(callback) = callback {
                callback();
            }
        });
        // Clone precisely the observed role; never rediscover a current
        // writer association. The domain gate ends before control admission.
        let attempt = unsafe {
            crate::memory_accounting::StoreAccountantHandle::borrow_owned_raw(
                Self::raw_handle_pointer(old),
            )
        };
        let owner = PersistenceProofOwner::prepare(attempt)?;
        let raw = Arc::into_raw(owner).cast_mut().cast::<()>();
        let tagged = raw.map_addr(|address| address | Self::READY_WRITER);
        match self
            .0
            .compare_exchange(old, tagged, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                // The published winner already owns the same original Arc
                // and counted epoch. Consume exactly the former raw ownership.
                unsafe {
                    drop(
                        crate::memory_accounting::StoreAccountantHandle::from_owned_raw(
                            Self::raw_handle_pointer(old),
                        ),
                    );
                }
            }
            Err(_) => {
                // Lose only the admitted attempt. The old raw ownership was
                // consumed by its winner and is never consumed by a loser.
                unsafe {
                    drop(Arc::from_raw(raw.cast::<PersistenceProofOwner>()));
                }
            }
        }
        Ok(())
    }
}
impl Drop for ProofAccountantSlot {
    fn drop(&mut self) {
        let pointer = *self.0.get_mut();
        match pointer.addr() & Self::MASK {
            Self::READY_WRITER => unsafe {
                drop(Arc::from_raw(Self::ready_pointer(pointer)));
            },
            Self::RAW_READER | Self::RAW_WRITER => unsafe {
                drop(
                    crate::memory_accounting::StoreAccountantHandle::from_owned_raw(
                        Self::raw_handle_pointer(pointer),
                    ),
                );
            },
            _ => unreachable!("invalid proof-accountant slot state"),
        }
    }
}
impl std::fmt::Display for DiagnosticIndexName<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut hash = blake3::Hasher::new();
        hash.update(self.table.as_bytes());
        hash.update(&[0]);
        hash.update(self.index.as_bytes());
        write!(f, "__rel_index_{}", hash.finalize().to_hex())
    }
}

pub(super) fn digest_field(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn digest_redb_table<K: RedbKey + 'static, V: RedbValue + 'static>(
    read_txn: &redb::ReadTransaction,
    definition: TableDefinition<K, V>,
    label: &str,
    hasher: &mut blake3::Hasher,
) -> Result<u64> {
    digest_field(hasher, label.as_bytes());
    let table = match read_txn.open_table(definition) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
        Err(err) => {
            return Err(Error::Other(
                (format!("snapshot digest table open failed: {err}")).into(),
            ));
        }
    };
    let mut count = 0_u64;
    for entry in table.iter().map_err(|err| {
        Error::Other((format!("snapshot digest table iteration failed: {err}")).into())
    })? {
        let (key, value) = entry.map_err(|err| {
            Error::Other((format!("snapshot digest entry read failed: {err}")).into())
        })?;
        let key_value = key.value();
        let value_value = value.value();
        let key_bytes = K::as_bytes(&key_value);
        let value_bytes = V::as_bytes(&value_value);
        digest_field(hasher, key_bytes.as_ref());
        digest_field(hasher, value_bytes.as_ref());
        count = count.saturating_add(1);
    }
    Ok(count)
}

impl BorrowedVectorPartitionBaseGenerationRecord<'_> {
    pub(super) fn into_owned(self) -> VectorPartitionBaseGenerationRecord {
        VectorPartitionBaseGenerationRecord {
            index: self.index,
            partition_key: self.partition_key,
            generation_id: self.generation_id,
            base_tx: self.base_tx,
            base_lsn: self.base_lsn,
            highest_included_tx: self.highest_included_tx,
            highest_included_lsn: self.highest_included_lsn,
            dimension: self.dimension,
            quantization: self.quantization,
            algorithm_version: self.algorithm_version,
            graph_format_version: self.graph_format_version,
            graph: VectorPartitionGraphBytes {
                // Copy once at the final length. Serde's Vec decoder grows
                // geometrically beyond its initial capped reservation,
                // retaining excess capacity and overlapping reallocations.
                bytes: self.graph.bytes.to_vec(),
                checksum: self.graph.checksum,
            },
            state_digest: self.state_digest,
        }
    }
}

impl BorrowedVectorPartitionChangeGenerationRecord<'_> {
    pub(super) fn into_owned(self) -> VectorPartitionChangeGenerationRecord {
        VectorPartitionChangeGenerationRecord {
            index: self.index,
            partition_key: self.partition_key,
            base_generation_id: self.base_generation_id,
            change_generation_id: self.change_generation_id,
            covered_through_tx: self.covered_through_tx,
            covered_through_lsn: self.covered_through_lsn,
            fresh_tail_from_tx: self.fresh_tail_from_tx,
            fresh_tail_from_lsn: self.fresh_tail_from_lsn,
            dimension: self.dimension,
            quantization: self.quantization,
            algorithm_version: self.algorithm_version,
            graph_format_version: self.graph_format_version,
            graph: VectorPartitionGraphBytes {
                bytes: self.graph.bytes.to_vec(),
                checksum: self.graph.checksum,
            },
        }
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn borrow_vector_partition_base<'a>(
    encoded: &'a [u8],
    index: &VectorIndexRef,
    partition_key: &VectorPartitionKey,
    generation_id: u64,
    corruption: impl Fn(contextdb_core::error::DiagnosticText) -> Error,
) -> Result<BorrowedVectorPartitionBaseGenerationRecord<'a>> {
    let payload =
        vector_partition_record_payload(VectorPartitionRecordKind::BaseGeneration, encoded)
            .map_err(&corruption)?;
    vector_partition_expected_route_prefix(payload, index, partition_key).map_err(|_| corruption("base generation identity, dimension, or inclusion boundary is invalid: generation route prefix does not match its physical partition".into()))?;
    let (record, consumed): (BorrowedVectorPartitionBaseGenerationRecord<'_>, usize) =
        bincode::serde::borrow_decode_from_slice(payload, bincode::config::standard()).map_err(
            |error| {
                crate::funded_error::map_diagnostic(
                    crate::read_image_memory::active_owner()
                        .and_then(|owner| owner.diagnostic_budget()),
                    format_args!("payload decode failed: {error}"),
                    &corruption,
                )
            },
        )?;
    if consumed != payload.len() {
        return Err(corruption(
            "payload decode failed: bincode exact decode left trailing bytes".into(),
        ));
    }
    if record.generation_id != generation_id
        || record.dimension == 0
        || record.highest_included_tx.0 < record.base_tx.0
        || record.highest_included_lsn.0 < record.base_lsn.0
    {
        return Err(corruption(
            "base generation identity, dimension, or inclusion boundary is invalid".into(),
        ));
    }
    if *blake3::hash(record.graph.bytes).as_bytes() != record.graph.checksum {
        return Err(corruption("immutable graph byte checksum mismatch".into()));
    }
    Ok(record)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn borrow_vector_partition_change<'a>(
    encoded: &'a [u8],
    index: &VectorIndexRef,
    partition_key: &VectorPartitionKey,
    base: &BorrowedVectorPartitionBaseGenerationRecord<'_>,
    generation_id: u64,
    corruption: impl Fn(contextdb_core::error::DiagnosticText) -> Error,
) -> Result<BorrowedVectorPartitionChangeGenerationRecord<'a>> {
    let payload =
        vector_partition_record_payload(VectorPartitionRecordKind::ChangeGeneration, encoded)
            .map_err(&corruption)?;
    vector_partition_expected_route_prefix(payload, index, partition_key).map_err(|_| corruption("base generation identity, dimension, or inclusion boundary is invalid: generation route prefix does not match its physical partition".into()))?;
    let (record, consumed): (BorrowedVectorPartitionChangeGenerationRecord<'_>, usize) =
        bincode::serde::borrow_decode_from_slice(payload, bincode::config::standard()).map_err(
            |error| {
                crate::funded_error::map_diagnostic(
                    crate::read_image_memory::active_owner()
                        .and_then(|owner| owner.diagnostic_budget()),
                    format_args!("payload decode failed: {error}"),
                    &corruption,
                )
            },
        )?;
    if consumed != payload.len() {
        return Err(corruption(
            "payload decode failed: bincode exact decode left trailing bytes".into(),
        ));
    }
    if record.base_generation_id != base.generation_id
        || record.change_generation_id != generation_id
        || record.dimension != base.dimension
        || record.quantization != base.quantization
        || record.algorithm_version != base.algorithm_version
        || !vector_paged::change_format_compatible(
            record.graph_format_version,
            base.graph_format_version,
        )
        || record.covered_through_tx.0 < base.highest_included_tx.0
        || record.covered_through_lsn.0 < base.highest_included_lsn.0
        || record.fresh_tail_from_tx.0 < record.covered_through_tx.0
        || record.fresh_tail_from_lsn.0 < record.covered_through_lsn.0
    {
        return Err(corruption(
            "sealed change identity, format, or replay boundary is invalid".into(),
        ));
    }
    if *blake3::hash(record.graph.bytes).as_bytes() != record.graph.checksum {
        return Err(corruption("immutable graph byte checksum mismatch".into()));
    }
    Ok(record)
}

impl VectorPartitionGraphBytes {
    pub(crate) fn verified(bytes: Vec<u8>) -> Self {
        let checksum = *blake3::hash(&bytes).as_bytes();
        Self { bytes, checksum }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn verify(
        &self,
        corruption: impl FnOnce(contextdb_core::error::DiagnosticText) -> Error,
    ) -> Result<()> {
        if *blake3::hash(&self.bytes).as_bytes() != self.checksum {
            return Err(corruption("immutable graph byte checksum mismatch".into()));
        }
        Ok(())
    }
}

impl ReadOnlyVectorGraphPageSource for RetainedVectorGraphPageSource {
    fn reader(&self) -> Result<Box<dyn ReadOnlyVectorGraphPageReader>> {
        let transaction = self.source.begin_read()?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::writer_query_test_seams::note_physical_source(
            crate::writer_query_test_seams::PhysicalFamily::Graph,
            &transaction,
        );
        Ok(Box::new(RetainedVectorGraphPageReader {
            transaction,
            key: Arc::clone(&self.key),
            is_base: self.is_base,
            graph_offset: self.graph_offset,
            graph_len: self.graph_len,
            paged: self.paged.as_ref().map(Arc::clone),
            database_path: self.source.database_path.clone(),
            read_memory: self.source.read_memory.clone(),
        }))
    }

    fn retained_bytes(&self) -> usize {
        let own = std::mem::size_of::<Self>().saturating_add(self.key.capacity());
        self.paged.as_ref().map_or(own, |paged| {
            own.saturating_add(std::mem::size_of::<RetainedVectorGraphPages>())
                .saturating_add(paged.root_key.capacity())
        })
    }

    fn page_bytes(&self) -> usize {
        self.paged
            .as_ref()
            .map_or(LEGACY_VECTOR_PARTITION_GRAPH_PAGE_BYTES, |paged| {
                paged.page_bytes
            })
    }

    fn total_page_bytes(&self) -> Option<usize> {
        self.paged
            .as_ref()
            .map(|paged| paged.page_bytes.saturating_mul(paged.page_count))
    }
}

impl ReadOnlyVectorGraphPageReader for RetainedVectorGraphPageReader {
    fn read_range(&mut self, offset: usize, len: usize) -> Result<Vec<u8>> {
        let _inline_phase = contextdb_core::find_attribution::inline_phases::enter(
            contextdb_core::find_attribution::inline_phases::Phase::PageRead,
        );
        let relative_end = offset
            .checked_add(len)
            .ok_or_else(|| Error::Other(("saved graph page range overflow".to_string()).into()))?;
        if relative_end > self.graph_len {
            return Err(Error::Other(
                ("saved graph page range exceeds its generation".to_string()).into(),
            ));
        }
        #[cfg(any(test, feature = "test-seams"))]
        let _graph_reads = {
            // This key was constructed from the validated generation route.
            let table_len = usize::try_from(u64::from_be_bytes(
                self.key[..8].try_into().expect("generation key component"),
            ))
            .expect("generation table length fits its owned key");
            let table = std::str::from_utf8(&self.key[8..8 + table_len])
                .expect("generation table key was constructed from UTF-8");
            crate::writer_query_test_seams::physical_operation(
                crate::writer_query_test_seams::PhysicalFamily::Graph,
                table,
            )
        };
        if let Some(paged) = &self.paged {
            if len == 0 {
                return Ok(Vec::new());
            }
            let first = offset / paged.page_bytes;
            let last = (relative_end - 1) / paged.page_bytes;
            let table = self
                .transaction
                .open_table(VECTOR_PARTITION_GRAPH_PAGE_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let mut encoded = Vec::with_capacity(len);
            let mut physical_page_lookups = 0usize;
            let mut authenticated_body_bytes = 0usize;
            for page in first..=last {
                let page_number = u32::try_from(page).map_err(|_| {
                    Error::Other(("saved graph physical page index overflow".to_string()).into())
                })?;
                let key =
                    RedbPersistence::vector_partition_graph_page_key(&paged.root_key, page_number);
                let value = table
                    .get(key.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| {
                        crate::funded_error::corruption_text(
                            Ok(self.read_memory.clone()),
                            self.database_path.display(),
                            "vector partition graph page manifest points to a missing page".into(),
                        )
                    })?;
                let stored = value.value();
                let expected_len = paged.page_bytes.min(
                    self.graph_len
                        .saturating_sub(page.saturating_mul(paged.page_bytes)),
                );
                if page >= paged.page_count
                    || stored.len()
                        != expected_len.saturating_add(VECTOR_PARTITION_GRAPH_PAGE_HEADER_BYTES)
                {
                    return Err(crate::funded_error::corruption_text(
                        Ok(self.read_memory.clone()),
                        self.database_path.display(),
                        "vector partition graph physical page is corrupt".into(),
                    ));
                }
                let stored_page = u32::from_le_bytes(
                    stored[..4]
                        .try_into()
                        .expect("the saved graph page header length was checked"),
                );
                let expected_checksum = &stored[4..VECTOR_PARTITION_GRAPH_PAGE_HEADER_BYTES];
                let bytes = &stored[VECTOR_PARTITION_GRAPH_PAGE_HEADER_BYTES..];
                if stored_page != page_number || {
                    let _inline_auth = contextdb_core::find_attribution::inline_phases::enter(
                        contextdb_core::find_attribution::inline_phases::Phase::PageAuth,
                    );
                    contextdb_core::find_attribution::inline_phases::authenticated(bytes.len());
                    blake3::hash(bytes).as_bytes() != expected_checksum
                } {
                    return Err(crate::funded_error::corruption_text(
                        Ok(self.read_memory.clone()),
                        self.database_path.display(),
                        "vector partition graph physical page is corrupt".into(),
                    ));
                }
                physical_page_lookups = physical_page_lookups.saturating_add(1);
                authenticated_body_bytes = authenticated_body_bytes.saturating_add(bytes.len());
                let page_start = page.saturating_mul(paged.page_bytes);
                let copy_start = offset.saturating_sub(page_start);
                let copy_end =
                    relative_end.min(page_start.saturating_add(bytes.len())) - page_start;
                encoded.extend_from_slice(&bytes[copy_start..copy_end]);
            }
            if encoded.len() != len {
                return Err(crate::funded_error::corruption_text(
                    Ok(self.read_memory.clone()),
                    self.database_path.display(),
                    "vector partition graph physical page range is incomplete".into(),
                ));
            }
            #[cfg(any(test, feature = "test-seams"))]
            read_persistence_test_scaffold::note_retained_graph_page_read_for_test(
                read_persistence_test_scaffold::RetainedGraphPageReadObservation {
                    offset,
                    returned_bytes: encoded.len(),
                    source_page_bytes: paged.page_bytes,
                    physical_page_lookups,
                    authenticated_body_bytes,
                },
            );
            return Ok(encoded);
        }
        let encoded = if self.is_base {
            let table = self
                .transaction
                .open_table(VECTOR_PARTITION_BASE_GENERATION_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let value = table
                .get(self.key.as_slice())
                .map_err(RedbPersistence::storage_error)?
                .ok_or_else(|| {
                    Error::Other(("saved base generation disappeared".to_string()).into())
                })?;
            value
                .value()
                .get(self.graph_offset + offset..self.graph_offset + relative_end)
                .ok_or_else(|| {
                    Error::Other(("saved base graph page is truncated".to_string()).into())
                })?
                .to_vec()
        } else {
            let table = self
                .transaction
                .open_table(VECTOR_PARTITION_CHANGE_GENERATION_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let value = table
                .get(self.key.as_slice())
                .map_err(RedbPersistence::storage_error)?
                .ok_or_else(|| {
                    Error::Other(("saved change generation disappeared".to_string()).into())
                })?;
            value
                .value()
                .get(self.graph_offset + offset..self.graph_offset + relative_end)
                .ok_or_else(|| {
                    Error::Other(("saved change graph page is truncated".to_string()).into())
                })?
                .to_vec()
        };
        Ok(encoded)
    }
}

impl RetainedReadSource {
    /// Committed-image twin of the writable bridge: the same read of this
    /// pinned source, retained together with the source itself.
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn captured_vector_serving_selection_in_read(
        &self,
        read: Arc<redb::ReadTransaction>,
        writer: Option<WriterSnapshotHandle>,
        retained: Option<RetainedReadSource>,
        partition: &VectorPartitionRef,
        generations: Option<(
            VectorPartitionGenerationDescriptor,
            Option<VectorPartitionGenerationDescriptor>,
        )>,
        inspection: contextdb_vector::store::RawVectorInspectionSnapshot,
        snapshot: SnapshotId,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<contextdb_vector::store::CapturedVectorServingSelection> {
        if writer.is_some()
            || !retained
                .as_ref()
                .is_some_and(|retained| RetainedReadSource::ptr_eq(retained, self))
        {
            return Err(vector_membership_source::damage(
                "committed-image vector serving escaped its retained source",
            ));
        }
        crate::database::vector_captured_serving::captured_serving_selection(
            crate::database::vector_captured_serving::CapturedVectorRootLease::retained(
                read,
                self.clone(),
            )?,
            partition,
            generations,
            inspection,
            snapshot,
            owner,
        )
    }
}
impl std::fmt::Display for IndexValidationFailure<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "durable index {} posting ", self.declaration.name)?;
        if let Some(parts) = self.parts {
            write!(
                f,
                "has {parts} key parts where the declaration has {}",
                self.declaration.columns.len()
            )
        } else {
            let (column, prefix) = self.column.expect("column validation detail");
            write!(f, "{prefix}{column}{}", self.suffix)
        }
    }
}

impl ReadOnlyVectorGenerationSource {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn new(source: RetainedReadSource, snapshot_lsn: Lsn) -> Result<Self> {
        let read = Arc::new(source.begin_read()?);
        Ok(Self {
            read,
            source,
            snapshot_lsn,
        })
    }

    pub(super) fn corruption(&self, reason: impl std::fmt::Display) -> Error {
        crate::funded_error::corruption_display(
            Ok(self.source.read_memory.clone()),
            self.source.database_path.display(),
            format_args!("vector partition durability record is corrupt: {reason}"),
        )
    }

    pub(super) fn generation_table_error(&self, error: redb::TableError, message: &str) -> Error {
        let memory_refusal = match &error {
            redb::TableError::Storage(redb::StorageError::AdmissionRefused(error)) =>
                matches!(error.downcast_ref::<Error>(), Some(Error::MemoryBudgetExceeded { .. })),
            redb::TableError::Storage(redb::StorageError::Native(redb::NativeColdReadError::AdmissionRefusedAt(owner))) =>
                owner.inspect_failure(|cause| cause.and_then(|cause| cause.downcast_ref::<native_storage_admission::NativeStorageFailure>())
                    .is_some_and(|cause| matches!(cause, native_storage_admission::NativeStorageFailure::Refused(contextdb_core::read_memory::NativeRefusal::MemoryBudgetExceeded { .. })))),
            _ => false,
        };
        if memory_refusal && let redb::TableError::Storage(storage) = error {
            return RedbPersistence::storage_error(storage);
        }
        self.corruption(format_args!("{message}: {error}"))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_read<T>(
        &self,
        read: impl FnOnce(&redb::ReadTransaction) -> Result<T>,
    ) -> Result<T> {
        read(&self.read)
    }

    /// Paged generation readers of a committed image read its pinned root.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn paged_root_capture(&self) -> vector_paged::PagedRootCapture {
        let source = self.source.clone();
        let read = Arc::clone(&self.read);
        Arc::new(move || {
            vector_paged::CapturedPagedRoot::from_image(Arc::clone(&read), source.clone())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn generation_record_bytes(
        &self,
        index: &VectorIndexRef,
        key: &VectorPartitionKey,
        base: u64,
        change: Option<u64>,
    ) -> Result<usize> {
        self.with_read(|read| {
            RedbPersistence::generation_record_bytes_in_read(read, index, key, base, change)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retained_graph_pages(
        &self,
        read: &redb::ReadTransaction,
        generation_key: &[u8],
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        base_generation_id: u64,
        generation_id: u64,
        is_base: bool,
        graph: &BorrowedVectorPartitionGraphBytes<'_>,
    ) -> Result<Option<Arc<RetainedVectorGraphPages>>> {
        let table = match read.open_table(VECTOR_PARTITION_GRAPH_PAGE_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(RedbPersistence::storage_error(error)),
        };
        let root_key =
            RedbPersistence::vector_partition_graph_page_root_key(generation_key, is_base);
        let Some(value) = table
            .get(root_key.as_slice())
            .map_err(RedbPersistence::storage_error)?
        else {
            return Ok(None);
        };
        let payload = vector_partition_record_payload(
            VectorPartitionRecordKind::GraphPageManifest,
            value.value(),
        )
        .map_err(|reason| self.corruption(reason))?;
        vector_partition_expected_route_prefix(payload, index, partition_key)
            .map_err(|reason| self.corruption(reason))?;
        let manifest: VectorPartitionGraphPageManifestRecord =
            decode_vector_partition_record_payload(
                VectorPartitionRecordKind::GraphPageManifest,
                value.value(),
            )
            .map_err(|reason| self.corruption(reason))?;
        RedbPersistence::validate_vector_schema_binding_in_read(
            read,
            index,
            manifest.schema_binding,
        )?;
        let page_bytes = manifest.page_bytes as usize;
        let bounded_page_bytes = VECTOR_PARTITION_GRAPH_PAGE_RECORD_BYTES.checked_sub(
            VECTOR_PARTITION_GRAPH_PAGE_LEAF_FRAME_BYTES
                .saturating_add(root_key.len())
                .saturating_add(VECTOR_PARTITION_GRAPH_PAGE_KEY_SUFFIX_BYTES)
                .saturating_add(VECTOR_PARTITION_GRAPH_PAGE_HEADER_BYTES),
        );
        let supported_page_bytes = page_bytes != 0
            && (Some(page_bytes) == bounded_page_bytes
                || page_bytes == LEGACY_VECTOR_PARTITION_GRAPH_PAGE_BYTES);
        let expected_pages = if supported_page_bytes {
            graph.bytes.len().div_ceil(page_bytes)
        } else {
            Default::default()
        };
        if manifest.index != *index
            || manifest.partition_key != *partition_key
            || manifest.base_generation_id != base_generation_id
            || manifest.generation_id != generation_id
            || manifest.is_base != is_base
            || !supported_page_bytes
            || manifest.graph_bytes != graph.bytes.len() as u64
            || manifest.graph_checksum != graph.checksum
            || manifest.page_count as usize != expected_pages
        {
            return Err(self
                .corruption("graph page manifest identity, extent, or checksum root is invalid"));
        }
        Ok(Some(Arc::new(RetainedVectorGraphPages {
            root_key,
            page_bytes,
            page_count: manifest.page_count as usize,
        })))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn with_base_graph<T>(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        generation_id: u64,
        consume: impl FnOnce(ReadOnlyVectorGenerationGraph<'_>) -> Result<T>,
    ) -> Result<T> {
        let key = RedbPersistence::vector_partition_base_generation_key_from_parts(
            index,
            partition_key,
            generation_id,
        );
        self.with_read(|read| {
            #[cfg(any(test, feature = "test-seams"))]
            crate::writer_query_test_seams::note_physical_source(
                crate::writer_query_test_seams::PhysicalFamily::Graph,
                read,
            );
            #[cfg(any(test, feature = "test-seams"))]
            let _graph_reads = crate::writer_query_test_seams::physical_operation(
                crate::writer_query_test_seams::PhysicalFamily::Graph,
                &index.table,
            );
            let table = read
                .open_table(VECTOR_PARTITION_BASE_GENERATION_TABLE)
                .map_err(|error| {
                    self.generation_table_error(
                        error,
                        "catalog points to a missing base-generation table",
                    )
                })?;
            let value = {
                let _reading = contextdb_core::find_attribution::stage("graph_load_record_read");
                table
                    .get(key.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| self.corruption("catalog points to a missing base generation"))?
            };
            let encoded = value.value();
            let payload =
                vector_partition_record_payload(VectorPartitionRecordKind::BaseGeneration, encoded)
                    .map_err(|reason| self.corruption(reason))?;
            vector_partition_expected_route_prefix(payload, index, partition_key)
                .map_err(|reason| self.corruption(reason))?;
            let (record, consumed): (BorrowedVectorPartitionBaseGenerationRecord<'_>, usize) =
                bincode::serde::borrow_decode_from_slice(payload, bincode::config::standard())
                    .map_err(|error| {
                        self.corruption(format_args!("payload decode failed: {error}"))
                    })?;
            if consumed != payload.len()
                || record.index != *index
                || record.partition_key != *partition_key
                || record.generation_id != generation_id
                || record.dimension == 0
                || record.highest_included_tx.0 < record.base_tx.0
                || record.highest_included_lsn.0 < record.base_lsn.0
            {
                return Err(self.corruption(
                    "base generation identity, dimension, or inclusion boundary is invalid",
                ));
            }
            let paged = self.retained_graph_pages(
                read,
                &key,
                index,
                partition_key,
                generation_id,
                generation_id,
                true,
                &record.graph,
            )?;
            let graph_offset = record.graph.bytes.as_ptr() as usize - encoded.as_ptr() as usize;
            let source: Arc<dyn ReadOnlyVectorGraphPageSource> =
                Arc::new(RetainedVectorGraphPageSource {
                    source: self.source.clone(),
                    key: Arc::new(key.clone()),
                    is_base: true,
                    graph_offset,
                    graph_len: record.graph.bytes.len(),
                    paged,
                });
            let _state_digest = record.state_digest;
            consume(ReadOnlyVectorGenerationGraph {
                bytes: record.graph.bytes,
                covered_tx: record.highest_included_tx,
                covered_lsn: record.highest_included_lsn,
                dimension: record.dimension,
                quantization: record.quantization,
                algorithm_version: record.algorithm_version,
                graph_format_version: record.graph_format_version,
                source,
            })
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn with_change_graph<T>(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        base_generation_id: u64,
        change_generation_id: u64,
        base: &ChangeGenerationBase,
        consume: impl FnOnce(ReadOnlyVectorGenerationGraph<'_>) -> Result<T>,
    ) -> Result<T> {
        let key = RedbPersistence::vector_partition_change_generation_key_from_parts(
            index,
            partition_key,
            base_generation_id,
            change_generation_id,
        );
        self.with_read(|read| {
            #[cfg(any(test, feature = "test-seams"))]
            crate::writer_query_test_seams::note_physical_source(
                crate::writer_query_test_seams::PhysicalFamily::Graph,
                read,
            );
            #[cfg(any(test, feature = "test-seams"))]
            let _graph_reads = crate::writer_query_test_seams::physical_operation(
                crate::writer_query_test_seams::PhysicalFamily::Graph,
                &index.table,
            );
            let table = read
                .open_table(VECTOR_PARTITION_CHANGE_GENERATION_TABLE)
                .map_err(|error| {
                    self.generation_table_error(
                        error,
                        "catalog points to a missing change-generation table",
                    )
                })?;
            let value = {
                let _reading = contextdb_core::find_attribution::stage("graph_load_record_read");
                table
                    .get(key.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| {
                        self.corruption("catalog points to a missing sealed change generation")
                    })?
            };
            let encoded = value.value();
            let payload = vector_partition_record_payload(
                VectorPartitionRecordKind::ChangeGeneration,
                encoded,
            )
            .map_err(|reason| self.corruption(reason))?;
            vector_partition_expected_route_prefix(payload, index, partition_key)
                .map_err(|reason| self.corruption(reason))?;
            let (record, consumed): (BorrowedVectorPartitionChangeGenerationRecord<'_>, usize) =
                bincode::serde::borrow_decode_from_slice(payload, bincode::config::standard())
                    .map_err(|error| {
                        self.corruption(format_args!("payload decode failed: {error}"))
                    })?;
            if consumed != payload.len()
                || record.index != *index
                || record.partition_key != *partition_key
                || record.base_generation_id != base_generation_id
                || record.change_generation_id != change_generation_id
                || record.dimension != base.dimension
                || record.quantization != base.quantization
                || record.algorithm_version != base.algorithm_version
                || !vector_paged::change_format_compatible(
                    record.graph_format_version,
                    base.graph_format_version,
                )
                || record.covered_through_tx.0 < base.covered_tx.0
                || record.covered_through_lsn.0 < base.covered_lsn.0
                || record.fresh_tail_from_tx.0 < record.covered_through_tx.0
                || record.fresh_tail_from_lsn.0 < record.covered_through_lsn.0
            {
                return Err(self
                    .corruption("sealed change identity, format, or replay boundary is invalid"));
            }
            let paged = self.retained_graph_pages(
                read,
                &key,
                index,
                partition_key,
                base_generation_id,
                change_generation_id,
                false,
                &record.graph,
            )?;
            let graph_offset = record.graph.bytes.as_ptr() as usize - encoded.as_ptr() as usize;
            let source: Arc<dyn ReadOnlyVectorGraphPageSource> =
                Arc::new(RetainedVectorGraphPageSource {
                    source: self.source.clone(),
                    key: Arc::new(key.clone()),
                    is_base: false,
                    graph_offset,
                    graph_len: record.graph.bytes.len(),
                    paged,
                });
            consume(ReadOnlyVectorGenerationGraph {
                bytes: record.graph.bytes,
                covered_tx: record.covered_through_tx,
                covered_lsn: record.covered_through_lsn,
                dimension: record.dimension,
                quantization: record.quantization,
                algorithm_version: record.algorithm_version,
                graph_format_version: record.graph_format_version,
                source,
            })
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_base(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        generation_id: u64,
    ) -> Result<VectorPartitionBaseGenerationRecord> {
        let key = RedbPersistence::vector_partition_base_generation_key_from_parts(
            index,
            partition_key,
            generation_id,
        );
        self.with_read(|read| {
            #[cfg(any(test, feature = "test-seams"))]
            crate::writer_query_test_seams::note_physical_source(
                crate::writer_query_test_seams::PhysicalFamily::Graph,
                read,
            );
            #[cfg(any(test, feature = "test-seams"))]
            let _graph_reads = crate::writer_query_test_seams::physical_operation(
                crate::writer_query_test_seams::PhysicalFamily::Graph,
                &index.table,
            );
            let table = read
                .open_table(VECTOR_PARTITION_BASE_GENERATION_TABLE)
                .map_err(|error| {
                    self.generation_table_error(
                        error,
                        "catalog points to a missing base-generation table",
                    )
                })?;
            let value = {
                let _reading = contextdb_core::find_attribution::stage("graph_load_record_read");
                table
                    .get(key.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| self.corruption("catalog points to a missing base generation"))?
            };
            let record = borrow_vector_partition_base(
                value.value(),
                index,
                partition_key,
                generation_id,
                |reason| self.corruption(reason),
            )?;
            let record = record.into_owned();
            Ok(record)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_change(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        base_generation_id: u64,
        change_generation_id: u64,
    ) -> Result<VectorPartitionChangeGenerationRecord> {
        let key = RedbPersistence::vector_partition_change_generation_key_from_parts(
            index,
            partition_key,
            base_generation_id,
            change_generation_id,
        );
        self.with_read(|read| {
            #[cfg(any(test, feature = "test-seams"))]
            crate::writer_query_test_seams::note_physical_source(
                crate::writer_query_test_seams::PhysicalFamily::Graph,
                read,
            );
            #[cfg(any(test, feature = "test-seams"))]
            let _graph_reads = crate::writer_query_test_seams::physical_operation(
                crate::writer_query_test_seams::PhysicalFamily::Graph,
                &index.table,
            );
            let base_key = RedbPersistence::vector_partition_base_generation_key_from_parts(
                index,
                partition_key,
                base_generation_id,
            );
            let base_table = read
                .open_table(VECTOR_PARTITION_BASE_GENERATION_TABLE)
                .map_err(|error| {
                    self.generation_table_error(
                        error,
                        "catalog points to a missing base-generation table",
                    )
                })?;
            let base_value = base_table
                .get(base_key.as_slice())
                .map_err(RedbPersistence::storage_error)?
                .ok_or_else(|| {
                    self.corruption("change generation points to a missing base generation")
                })?;
            let base = borrow_vector_partition_base(
                base_value.value(),
                index,
                partition_key,
                base_generation_id,
                |reason| self.corruption(reason),
            )?;

            let table = read
                .open_table(VECTOR_PARTITION_CHANGE_GENERATION_TABLE)
                .map_err(|error| {
                    self.generation_table_error(
                        error,
                        "catalog points to a missing change-generation table",
                    )
                })?;
            let value = {
                let _reading = contextdb_core::find_attribution::stage("graph_load_record_read");
                table
                    .get(key.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| {
                        self.corruption("catalog points to a missing sealed change generation")
                    })?
            };
            let record = borrow_vector_partition_change(
                value.value(),
                index,
                partition_key,
                &base,
                change_generation_id,
                |reason| self.corruption(reason),
            )?;
            let record = record.into_owned();
            Ok(record)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn tail_entry_count(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        covered_lsn: Lsn,
    ) -> Result<usize> {
        let prefix = RedbPersistence::vector_partition_prefix(index, partition_key);
        self.with_read(|read| {
            let table = match read.open_table(VECTOR_PARTITION_JOURNAL_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
                Err(error) => return Err(RedbPersistence::storage_error(error)),
            };
            let mut count = 0usize;
            for entry in table
                .range(prefix.as_slice()..)
                .map_err(RedbPersistence::storage_error)?
            {
                let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                if !key.value().starts_with(&prefix) {
                    break;
                }
                let record: VectorPartitionJournalRecord = decode_vector_partition_record_payload(
                    VectorPartitionRecordKind::Journal,
                    value.value(),
                )
                .map_err(|reason| self.corruption(reason))?;
                if record.index != *index
                    || record.partition_key != *partition_key
                    || RedbPersistence::vector_partition_journal_key(&record).as_slice()
                        != key.value()
                {
                    return Err(self.corruption(
                        "journal selection encountered a mismatched route or physical key",
                    ));
                }
                if record.lsn > covered_lsn
                    && record.lsn <= self.snapshot_lsn
                    && matches!(record.change, VectorPartitionJournalChange::Upsert(_))
                {
                    count = count.checked_add(1).ok_or_else(|| {
                        self.corruption("journal tail entry count exceeds the process range")
                    })?;
                }
            }
            Ok(count)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_tail(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        covered_lsn: Lsn,
    ) -> Result<(Vec<VectorEntry>, Lsn)> {
        let prefix = RedbPersistence::vector_partition_prefix(index, partition_key);
        self.with_read(|read| {
            let table = match read.open_table(VECTOR_PARTITION_JOURNAL_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => {
                    return Ok((Vec::new(), covered_lsn));
                }
                Err(error) => return Err(RedbPersistence::storage_error(error)),
            };
            let mut identities = BTreeSet::new();
            let mut replayed_through_lsn = covered_lsn;
            for entry in table
                .range(prefix.as_slice()..)
                .map_err(RedbPersistence::storage_error)?
            {
                let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                if !key.value().starts_with(&prefix) {
                    break;
                }
                let record: VectorPartitionJournalRecord = decode_vector_partition_record_payload(
                    VectorPartitionRecordKind::Journal,
                    value.value(),
                )
                .map_err(|reason| self.corruption(reason))?;
                if record.index != *index
                    || record.partition_key != *partition_key
                    || RedbPersistence::vector_partition_journal_key(&record).as_slice()
                        != key.value()
                {
                    return Err(self.corruption(
                        "journal selection encountered a mismatched route or physical key",
                    ));
                }
                if record.lsn <= covered_lsn || record.lsn > self.snapshot_lsn {
                    continue;
                }
                #[cfg(any(test, feature = "test-seams"))]
                crate::vector_observations::emit(
                    index,
                    partition_key,
                    covered_lsn,
                    crate::vector_observations::VectorJournalReplayWork::Record { lsn: record.lsn },
                );
                replayed_through_lsn = replayed_through_lsn.max(record.lsn);
                if let VectorPartitionJournalChange::Upsert(membership) = record.change {
                    identities.insert((
                        membership.row_id,
                        membership.vector_created_tx,
                        membership.vector_lsn,
                    ));
                }
            }
            if identities.is_empty() {
                return Ok((Vec::new(), replayed_through_lsn));
            }
            let vectors = read
                .open_table(VECTORS_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let mut tail = Vec::with_capacity(identities.len());
            for (row_id, created_tx, lsn) in identities {
                let key = RedbPersistence::vector_identity_key(index, row_id, created_tx, lsn);
                let value = vectors
                    .get(key.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| {
                        self.corruption("journal tail points to a missing immutable vector")
                    })?;
                let entry = RedbPersistence::decode_vector_entry(value.value())?;
                if entry.index != *index
                    || entry.row_id != row_id
                    || entry.created_tx != created_tx
                    || entry.lsn != lsn
                {
                    return Err(
                        self.corruption("journal tail vector key disagrees with its payload")
                    );
                }
                #[cfg(any(test, feature = "test-seams"))]
                crate::vector_observations::emit(
                    index,
                    partition_key,
                    covered_lsn,
                    crate::vector_observations::VectorJournalReplayWork::Vector {
                        row_id,
                        created_tx,
                        lsn,
                    },
                );
                tail.push(entry);
            }
            Ok((tail, replayed_through_lsn))
        })
    }
}

impl PreencodedVectorPartitionGenerationCandidate {
    pub(crate) fn stale_metadata_bytes(&self) -> usize {
        self.attributable_stale_catalog_keys
            .capacity()
            .saturating_mul(std::mem::size_of::<Vec<u8>>())
            .saturating_add(
                self.attributable_stale_catalog_keys
                    .iter()
                    .fold(0usize, |bytes, key| bytes.saturating_add(key.capacity())),
            )
    }

    pub(crate) fn write_reservation_bytes(&self) -> usize {
        let payload = self
            .base_key
            .len()
            .saturating_add(self.base_bytes.len())
            .saturating_add(self.catalog_key.len())
            .saturating_add(self.catalog_bytes.len())
            .saturating_add(self.base_pages.write_bytes())
            .saturating_add(
                self.change
                    .as_ref()
                    .map_or(0, |(key, value)| key.len().saturating_add(value.len())),
            );
        let payload = payload.saturating_add(
            self.change_pages
                .as_ref()
                .map_or(0, PreencodedVectorPartitionGraphPages::write_bytes),
        );
        // Account for copy-on-write pages and record/page rounding as well as
        // the exact encoded records; never admit a nominal one-kilobyte write.
        payload.saturating_mul(2).saturating_add(64 * 1024)
    }
}

impl PreencodedVectorPartitionGraphPages {
    pub(super) fn write_bytes(&self) -> usize {
        let page_count = self.graph_bytes.len().div_ceil(self.page_bytes);
        self.root_key
            .len()
            .saturating_add(self.manifest_bytes.len())
            .saturating_add(self.graph_bytes.len())
            .saturating_add(
                page_count.saturating_mul(
                    self.root_key
                        .len()
                        .saturating_add(VECTOR_PARTITION_GRAPH_PAGE_KEY_SUFFIX_BYTES)
                        .saturating_add(VECTOR_PARTITION_GRAPH_PAGE_HEADER_BYTES),
                ),
            )
    }
}

impl VectorPartitionRecordIndex for VectorPartitionMembershipRecord {
    fn vector_partition_record_index(&self) -> &VectorIndexRef {
        &self.index
    }
}
impl VectorPartitionRecordIndex for VectorPartitionTombstoneRecord {
    fn vector_partition_record_index(&self) -> &VectorIndexRef {
        &self.membership.index
    }
}
impl VectorPartitionRecordIndex for VectorPartitionJournalRecord {
    fn vector_partition_record_index(&self) -> &VectorIndexRef {
        &self.index
    }
}
impl VectorPartitionRecordIndex for VectorPartitionBaseGenerationRecord {
    fn vector_partition_record_index(&self) -> &VectorIndexRef {
        &self.index
    }
    fn native_paged_graph(&self) -> Option<&[u8]> {
        (self.graph_format_version == vector_paged::VECTOR_GRAPH_PAGED_FORMAT_VERSION
            || (self.graph_format_version == 1
                && self.graph.bytes.starts_with(&vector_paged::META_MAGIC)))
        .then_some(self.graph.bytes.as_slice())
    }
}
impl VectorPartitionRecordIndex for VectorPartitionChangeGenerationRecord {
    fn vector_partition_record_index(&self) -> &VectorIndexRef {
        &self.index
    }
    fn native_paged_graph(&self) -> Option<&[u8]> {
        (self.graph_format_version == vector_paged::VECTOR_GRAPH_PAGED_FORMAT_VERSION
            || (self.graph_format_version == 1
                && self.graph.bytes.starts_with(&vector_paged::META_MAGIC)))
        .then_some(self.graph.bytes.as_slice())
    }
}
impl VectorPartitionRecordIndex for VectorPartitionGenerationCatalogRecord {
    fn vector_partition_record_index(&self) -> &VectorIndexRef {
        &self.index
    }
}
impl std::ops::Deref for ReceivedRowsGuard<'_> {
    type Target = HashMap<String, Vec<VersionedRow>>;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(rows) => rows,
            Self::Private(rows) => rows,
        }
    }
}
impl ReceivedSchemaPersistenceProjection {
    /// Consumers that need one record do not retain an array position into
    /// the private image. This borrow never creates a second row/body list.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn visit_rows(
        &self,
        mut visit: impl FnMut(&str, &VersionedRow) -> Result<()>,
    ) -> Result<()> {
        if let Some(records) = &self.row_records {
            return records.visit(&self.vector_image_memory, visit);
        }
        let image = self.rows_image();
        for (table, rows) in &*image {
            if !self.affected_tables.contains(table) {
                continue;
            }
            for row in rows {
                visit(table, row)?;
            }
        }
        Ok(())
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn visit_rows_original(
        &self,
        original: &OriginalPersistenceFrame<'_>,
        mut visit: impl FnMut(&str, &VersionedRow) -> Result<()>,
    ) -> Result<()> {
        if let Some(records) = &self.row_records {
            records.visit_original(original, visit)
        } else {
            let image = self.rows_image();
            for (table, rows) in &*image {
                if self.affected_tables.contains(table) {
                    for row in rows {
                        visit(table, row)?;
                    }
                }
            }
            Ok(())
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn visit_table_rows_original(
        &self,
        original: &OriginalPersistenceFrame<'_>,
        table: &str,
        mut visit: impl FnMut(&VersionedRow) -> Result<()>,
    ) -> Result<()> {
        if let Some(records) = &self.row_records {
            records.visit_table_original(original, table, visit)
        } else {
            if let Some(rows) = self.rows_image().get(table) {
                for row in rows {
                    visit(row)?;
                }
            }
            Ok(())
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn materialize_rows_compatibility(
        &self,
    ) -> Result<HashMap<String, Vec<VersionedRow>>> {
        let mut rows = HashMap::<String, Vec<VersionedRow>>::new();
        self.visit_rows(|table, row| {
            self.vector_image_memory.reserve(
                row.estimated_bytes()
                    .saturating_mul(8)
                    .saturating_add(table.len())
                    .saturating_add(std::mem::size_of::<VersionedRow>()),
            )?;
            rows.entry(table.to_owned()).or_default().push(row.clone());
            Ok(())
        })?;
        Ok(rows)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn visit_vector_bodies(
        &self,
        mut visit: impl FnMut(&VectorEntry) -> Result<()>,
    ) -> Result<()> {
        if let Some(records) = &self.vector_records {
            records.bodies.visit(&self.vector_image_memory, visit)
        } else {
            for body in &self.vectors {
                visit(body)?;
            }
            Ok(())
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn visit_vector_bodies_original(
        &self,
        original: &OriginalPersistenceFrame<'_>,
        mut visit: impl FnMut(&VectorEntry) -> Result<()>,
    ) -> Result<()> {
        if let Some(records) = &self.vector_records {
            records.bodies.visit_original(original, visit)
        } else {
            for body in &self.vectors {
                visit(body)?;
            }
            Ok(())
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn visit_vector_memberships(
        &self,
        mut visit: impl FnMut(&VectorPartitionMembershipRecord) -> Result<()>,
    ) -> Result<()> {
        if let Some(records) = &self.vector_records {
            records.memberships.visit(&self.vector_image_memory, visit)
        } else {
            for member in &self.vector_memberships {
                visit(member)?;
            }
            Ok(())
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn visit_vector_memberships_original(
        &self,
        original: &OriginalPersistenceFrame<'_>,
        mut visit: impl FnMut(&VectorPartitionMembershipRecord) -> Result<()>,
    ) -> Result<()> {
        if let Some(records) = &self.vector_records {
            records.memberships.visit_original(original, visit)
        } else {
            for member in &self.vector_memberships {
                visit(member)?;
            }
            Ok(())
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn visit_sync_sources(
        &self,
        mut visit: impl FnMut(&str, RowId, Lsn, u8) -> Result<()>,
    ) -> Result<()> {
        if let Some(records) = &self.sync_source_records {
            records.visit(&self.vector_image_memory, visit)
        } else {
            for (table, row, lsn, kind) in &self.sync_sources {
                visit(table, *row, *lsn, sync_source_kind_to_u8(*kind))?;
            }
            Ok(())
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn visit_sync_sources_original(
        &self,
        frame: &OriginalPersistenceFrame<'_>,
        mut visit: impl FnMut(&str, RowId, Lsn, u8) -> Result<()>,
    ) -> Result<()> {
        if let Some(records) = &self.sync_source_records {
            records.visit_original(frame, visit)
        } else {
            for (table, row, lsn, kind) in &self.sync_sources {
                visit(table, *row, *lsn, sync_source_kind_to_u8(*kind))?;
            }
            Ok(())
        }
    }
    pub(crate) fn rows_image(&self) -> ReceivedRowsGuard<'_> {
        match &self.private_rows {
            Some(store) => ReceivedRowsGuard::Private(store.tables.read()),
            None => ReceivedRowsGuard::Owned(&self.rows),
        }
    }
}

/// One SQ8 component as `PersistedVector::to_f32` reconstructs it; the
/// verification projection streams the same values, so both call this.
#[inline]
pub(crate) fn sq8_component(min: f32, max: f32, byte: u8) -> f32 {
    let range = max - min;
    if range <= f32::EPSILON {
        min
    } else {
        min + ((byte as f32) / 255.0) * range
    }
}

/// One SQ4 component (a nibble, high first) as `PersistedVector::to_f32`
/// reconstructs it.
#[inline]
pub(crate) fn sq4_component(min: f32, max: f32, nibble: u8) -> f32 {
    let range = max - min;
    if range <= f32::EPSILON {
        min
    } else {
        min + ((nibble as f32) / 15.0) * range
    }
}

impl PersistedVector {
    pub(super) fn quantization(&self) -> VectorQuantization {
        match self {
            Self::F32(_) => VectorQuantization::F32,
            Self::SQ8 { .. } => VectorQuantization::SQ8,
            Self::SQ4 { .. } => VectorQuantization::SQ4,
        }
    }

    pub(super) fn decoded_bytes(&self) -> usize {
        let len = match self {
            Self::F32(values) => values.len(),
            Self::SQ8 { len, payload, .. } => (*len as usize).min(payload.len()),
            Self::SQ4 { len, payload, .. } => (*len as usize).min(payload.len().saturating_mul(2)),
        };
        len.saturating_mul(std::mem::size_of::<f32>())
    }
    pub(super) fn from_f32(vector: &[f32], quantization: VectorQuantization) -> Self {
        match quantization {
            VectorQuantization::F32 => PersistedVector::F32(vector.to_vec()),
            VectorQuantization::SQ8 => {
                let (min, max) = vector_min_max(vector);
                let range = max - min;
                let payload = if range <= f32::EPSILON {
                    vec![0; vector.len()]
                } else {
                    vector
                        .iter()
                        .map(|value| {
                            (((*value - min) / range) * 255.0).round().clamp(0.0, 255.0) as u8
                        })
                        .collect()
                };
                PersistedVector::SQ8 {
                    min,
                    max,
                    len: vector.len() as u32,
                    payload,
                }
            }
            VectorQuantization::SQ4 => {
                let (min, max) = vector_min_max(vector);
                let range = max - min;
                let mut payload = Vec::with_capacity(vector.len().div_ceil(2));
                let quantized = if range <= f32::EPSILON {
                    vec![0; vector.len()]
                } else {
                    vector
                        .iter()
                        .map(|value| {
                            (((*value - min) / range) * 15.0).round().clamp(0.0, 15.0) as u8
                        })
                        .collect::<Vec<_>>()
                };
                for pair in quantized.chunks(2) {
                    let hi = pair[0] & 0x0f;
                    let lo = pair.get(1).copied().unwrap_or(0) & 0x0f;
                    payload.push((hi << 4) | lo);
                }
                PersistedVector::SQ4 {
                    min,
                    max,
                    len: vector.len() as u32,
                    payload,
                }
            }
        }
    }

    pub(super) fn to_f32(&self) -> Vec<f32> {
        match self {
            PersistedVector::F32(vector) => vector.clone(),
            PersistedVector::SQ8 {
                min,
                max,
                len,
                payload,
            } => payload
                .iter()
                .take(*len as usize)
                .map(|byte| sq8_component(*min, *max, *byte))
                .collect(),
            PersistedVector::SQ4 {
                min,
                max,
                len,
                payload,
            } => {
                // A malformed durable length must not reserve independently
                // of the bytes that can actually decode. Callers validate
                // the reconstructed dimension and classify a short payload.
                let mut values =
                    Vec::with_capacity(payload.len().saturating_mul(2).min(*len as usize));
                for byte in payload {
                    for q in [byte >> 4, byte & 0x0f] {
                        if values.len() == *len as usize {
                            break;
                        }
                        values.push(sq4_component(*min, *max, q));
                    }
                }
                values
            }
        }
    }
}

pub(super) fn vector_min_max(vector: &[f32]) -> (f32, f32) {
    let Some((first, rest)) = vector.split_first() else {
        return (0.0, 0.0);
    };
    rest.iter()
        .copied()
        .fold((*first, *first), |(min, max), value| {
            (min.min(value), max.max(value))
        })
}

impl From<LegacyColumnDefV1> for contextdb_core::ColumnDef {
    fn from(legacy: LegacyColumnDefV1) -> Self {
        contextdb_core::ColumnDef {
            search_target_accuracy_hundredths: None,
            search_expected_vectors: None,
            ivf_lists: None,
            ivf_probes: None,
            ivf_refine_factor_hundredths: None,
            vector_engine_policy: Default::default(),
            name: legacy.name,
            column_type: legacy.column_type,
            nullable: legacy.nullable,
            primary_key: legacy.primary_key,
            unique: legacy.unique,
            default: legacy.default,
            references: legacy.references,
            expires: legacy.expires,
            immutable: legacy.immutable,
            quantization: legacy.quantization,
            rank_policy: legacy.rank_policy,
            // Fields v1.0.0 never wrote: honest, unset defaults.
            context_id: false,
            scope_label: None,
            acl_ref: None,
            partition_key_columns: None,
            max_partitions: None,
            search_mode: contextdb_core::VectorSearchMode::Auto,
            auto_index_at: None,
            flat_below: None,
            auto_expansion_ceiling: None,
            hnsw_m: None,
            hnsw_ef_construction: None,
            hnsw_ef_search: None,
            hnsw_search_radius_millionths: None,
            vector_policy_revision: contextdb_core::DEFAULT_VECTOR_POLICY_REVISION,
            consolidation_change_percent: None,
            consolidation_tombstone_percent: None,
            consolidation_disabled: false,
        }
    }
}

impl From<LegacyTableMetaV1> for TableMeta {
    fn from(legacy: LegacyTableMetaV1) -> Self {
        TableMeta {
            columns: legacy.columns.into_iter().map(Into::into).collect(),
            immutable: legacy.immutable,
            state_machine: legacy.state_machine,
            dag_edge_types: legacy.dag_edge_types,
            unique_constraints: legacy.unique_constraints,
            natural_key_column: legacy.natural_key_column,
            propagation_rules: legacy.propagation_rules,
            default_ttl_seconds: legacy.default_ttl_seconds,
            sync_safe: legacy.sync_safe,
            expires_column: legacy.expires_column,
            indexes: legacy.indexes,
            // Fields v1.0.0 never wrote: honest, unset defaults.
            composite_foreign_keys: Vec::new(),
            sync_direction: None,
            retain_declared_unit: None,
            primary_key_columns: Vec::new(),
            conflict_policy: None,
            history_policy: None,
            delivery_manifest_tables: None,
            edge_discard: None,
            maintained_counts: None,
        }
    }
}

pub(super) fn replacement_temp_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(format!(
        ".replacement-{}.tmp",
        uuid::Uuid::new_v4().simple()
    ));
    PathBuf::from(name)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn create_replacement_placeholder(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    options.open(path).map_err(RedbPersistence::storage_error)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn sync_database_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(RedbPersistence::storage_error)
}

impl RedbPersistence {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn f32_witness_index<'a>(
        ws: &'a WriteSet,
        batch: Option<&'a PreparedVectorPartitionMutationBatch>,
        quantization: &HashMap<VectorIndexRef, VectorQuantization>,
        mut admit: impl FnMut(usize) -> Result<()>,
    ) -> Result<F32WitnessIndex<'a>> {
        let carried = batch.map_or(0, |batch| {
            batch
                .mutations
                .iter()
                .map(|mutation| match mutation {
                    PreparedVectorPartitionMutation::Delete { .. } => 1usize,
                    PreparedVectorPartitionMutation::Move { .. } => 2,
                    PreparedVectorPartitionMutation::Insert { .. } => 0,
                })
                .sum::<usize>()
        });
        let capacity = ws
            .vector_inserts
            .len()
            .checked_add(carried)
            .ok_or_else(|| Error::Other("F32 witness directory overflow".into()))?;
        admit(
            crate::memory_accounting::native_map_plan::<F32WitnessKey<'_>, F32Witness<'_>>(
                capacity,
            )?
            .layout
            .size(),
        )?;
        let mut witnesses = HashMap::with_capacity(capacity);
        for entry in &ws.vector_inserts {
            if quantization.get(&entry.index) == Some(&VectorQuantization::F32) {
                // Preserve the existing last-entry rule for this exact occurrence/column.
                witnesses.insert(
                    (
                        entry.index.table.as_str(),
                        entry.row_id,
                        entry.created_tx,
                        entry.lsn,
                        entry.index.column.as_str(),
                    ),
                    F32Witness::Staged(&entry.vector),
                );
            }
        }
        if let Some(batch) = batch {
            for mutation in &batch.mutations {
                let (index, before, after) = match mutation {
                    PreparedVectorPartitionMutation::Delete { index, before, .. } => {
                        (index, before, None)
                    }
                    PreparedVectorPartitionMutation::Move {
                        index,
                        before,
                        after,
                        ..
                    } => (index, before, Some(after)),
                    PreparedVectorPartitionMutation::Insert { .. } => continue,
                };
                if quantization.get(index) != Some(&VectorQuantization::F32) {
                    continue;
                }
                let witness = F32Witness::Durable(
                    index,
                    before.row.row_id,
                    before.vector_created_tx,
                    before.vector_lsn,
                );
                witnesses.insert(
                    (
                        index.table.as_str(),
                        before.row.row_id,
                        before.row.created_tx,
                        before.row.lsn,
                        index.column.as_str(),
                    ),
                    witness,
                );
                if let Some(after) = after {
                    witnesses.insert(
                        (
                            index.table.as_str(),
                            after.row.row_id,
                            after.row.created_tx,
                            after.row.lsn,
                            index.column.as_str(),
                        ),
                        witness,
                    );
                }
            }
        }
        Ok(witnesses)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn overlay_f32_witnesses<'a>(
        &self,
        write: &redb::WriteTransaction,
        witnesses: &F32WitnessIndex<'a>,
        table: &str,
        row: &VersionedRow,
        overlay: &mut F32RowOverlay<'a>,
        mut admit: impl FnMut(usize) -> Result<()>,
        mut decode: impl FnMut(&[u8]) -> Result<VectorEntry>,
    ) -> Result<()> {
        // Grow the destination only after its whole native table and copied keys fit.
        let count = overlay
            .len()
            .checked_add(row.values.len())
            .ok_or_else(|| Error::Other("row F32 overlay overflow".into()))?;
        admit(
            crate::memory_accounting::native_map_plan::<String, std::borrow::Cow<'_, [f32]>>(
                count,
            )?
            .layout
            .size()
            .saturating_add(row.values.keys().map(String::len).sum::<usize>()),
        )?;
        overlay.reserve(row.values.len());
        for column in row.values.keys() {
            let Some(witness) =
                witnesses.get(&(table, row.row_id, row.created_tx, row.lsn, column.as_str()))
            else {
                continue;
            };
            let value = match witness {
                F32Witness::Staged(vector) => std::borrow::Cow::Borrowed(*vector),
                F32Witness::Durable(index, row_id, created_tx, lsn) => {
                    admit(
                        index
                            .table
                            .len()
                            .saturating_add(index.column.len())
                            .saturating_add(32),
                    )?;
                    let key = Self::vector_identity_key(index, *row_id, *created_tx, *lsn);
                    let vectors = content_size::open_vectors(write).map_err(Self::storage_error)?;
                    let stored = vectors
                        .get(key.as_slice())
                        .map_err(Self::storage_error)?
                        .ok_or_else(|| {
                            self.vector_partition_corruption(
                                "a prepared row publication has no durable source vector",
                            )
                        })?;
                    let entry = decode(stored.value())?;
                    if entry.index != **index
                        || entry.row_id != *row_id
                        || entry.created_tx != *created_tx
                        || entry.lsn != *lsn
                    {
                        return Err(self.vector_partition_corruption(
                            "a prepared row witness disagrees with its exact durable source",
                        ));
                    }
                    std::borrow::Cow::Owned(entry.vector)
                }
            };
            overlay.insert(column.clone(), value);
        }
        Ok(())
    }
}
