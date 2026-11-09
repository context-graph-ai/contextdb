use super::*;

impl PersistenceConstruction {
    pub(super) fn finish(&mut self) {
        for step in (0..15).chain([16, 17, 18, 15]) {
            if !std::mem::replace(&mut self.initialized[step], false) {
                continue;
            }
            // Advancing before drop prevents repeating an unwinding field.
            unsafe {
                match step {
                    0 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).path));
                    }
                    1 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).runtime_dir));
                    }
                    2 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).db));
                    }
                    3 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).writer_file));
                    }
                    4 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).lock_file));
                    }
                    5 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).storage_compaction
                        ));
                    }
                    6 => {
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                                (*self.source).handoff_prepare_io_fault
                            ));
                        }
                    }
                    7 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).writer_snapshots
                        ));
                    }
                    8 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).writer_schema
                        ));
                    }
                    9 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).cache_charge_bytes
                        ));
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).cache_installation
                        ));
                    }
                    18 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).cache_stripe_residue
                        ));
                    }
                    10 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).used_legacy_table_meta_layout
                        ));
                    }
                    11 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).native_ivf_cache
                        ));
                    }
                    12 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).paged_root_capture
                        ));
                    }
                    13 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).scoped_vector_root_capture
                        ));
                    }
                    15 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).proof_accountant
                        ));
                    }
                    16 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).saved_export_recovery_cutoff
                        ));
                    }
                    17 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).saved_export_abandonment_error
                        ));
                    }
                    _ => unreachable!(),
                }
            }
            #[cfg(test)]
            RETURNED_LOADER_PARTIAL_PANIC_FOR_TEST.with(|flag| {
                if flag.get() == Some(step) {
                    flag.set(None);
                    panic!("partial persistence cleanup unwind");
                }
            });
        }
    }
}
impl Drop for PersistenceConstruction {
    fn drop(&mut self) {
        // If one initialized child unwinds, this stack guard resumes at the
        // next field. finish disarms each field before its destructor runs.
        struct Resume<'a>(&'a mut PersistenceConstruction);
        impl Drop for Resume<'_> {
            fn drop(&mut self) {
                self.0.finish();
            }
        }
        let resume = Resume(self);
        resume.0.finish();
    }
}
impl Drop for RedbPersistence {
    fn drop(&mut self) {
        self.close();
        drop(self.runtime_dir.take());
        // Automatic field drop frees every payload before the final proof slot.
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn try_lock_exclusive(file: &File) -> Result<bool> {
    match companion_ownership_lock(file, CompanionOwnershipLock::Exclusive) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => Ok(false),
        Err(err) => Err(Error::Other(
            (format!("companion lock error: {err}")).into(),
        )),
    }
}

pub(super) fn unlock_file(file: &File) {
    let _ = companion_ownership_lock(file, CompanionOwnershipLock::Unlock);
}

#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn arm_table_meta_rewrite_crash_for_test(boundary: u8) {
    assert!(
        (1..=3).contains(&boundary),
        "the table-metadata rewrite has three crash boundaries"
    );
    TABLE_META_REWRITE_CRASH.with(|armed| armed.set(boundary));
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn table_meta_rewrite_checkpoint_for_test(boundary: u8) {
    crate::read_work_probe::note_graph(
        "publication",
        "checkpoint",
        &[boundary],
        None,
        Some("table_meta_rewrite_checkpoint_for_test"),
    );
    if TABLE_META_REWRITE_CRASH.with(|armed| armed.get()) == boundary {
        eprintln!("TABLE_META_REWRITE_CRASH={boundary}");
        std::process::abort();
    }
}

#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn arm_retention_reclaim_crash_for_test(boundary: u8) {
    RETENTION_RECLAIM_CRASH.with(|armed| armed.set(boundary));
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn retention_reclaim_checkpoint_for_test(boundary: u8) {
    crate::read_work_probe::note_graph(
        "publication",
        "checkpoint",
        &[boundary],
        None,
        Some("retention_reclaim_checkpoint_for_test"),
    );
    if RETENTION_RECLAIM_CRASH.with(|armed| armed.get() == boundary) {
        eprintln!("RETENTION_RECLAIM_CRASH={boundary}");
        std::process::abort();
    }
}

#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from consolidation_metadata_contract_tests.rs"
    )
)]
#[allow(
    dead_code,
    reason = "KEEP-SEAM: test helper behind a test cfg; unit tests or the test-seams build use it"
)]
pub(crate) fn arm_policy_metadata_fault_for_test(boundary: u8, crash: bool) {
    assert!(matches!(boundary, 10 | 11));
    POLICY_METADATA_FAULT.with(|armed| armed.set((boundary, crash)));
}

#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
pub(super) fn policy_metadata_checkpoint_for_test(boundary: u8) -> Result<()> {
    let crash = POLICY_METADATA_FAULT.with(|armed| {
        let (selected, crash) = armed.get();
        if selected != boundary {
            return None;
        }
        armed.set((0, false));
        Some(crash)
    });
    match crash {
        Some(true) => {
            eprintln!("POLICY_METADATA_CRASH={boundary}");
            std::process::abort()
        }
        Some(false) => Err(Error::Other(
            "injected policy metadata publication failure".into(),
        )),
        None => Ok(()),
    }
}

#[cfg(test)]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn stored_relational_row_bytes_fault_for_test() -> Result<()> {
    if STORED_ROW_BYTES_FAULT.with(|armed| armed.replace(false)) {
        return Err(Error::Other(
            "injected stored relational row size read failure".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn arm_stored_relational_row_bytes_fault_for_test() {
    STORED_ROW_BYTES_FAULT.with(|armed| armed.set(true));
}

#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from empty_declared_index_readiness_tests.rs"
    )
)]
#[allow(
    dead_code,
    reason = "KEEP-SEAM: test helper behind a test cfg; unit tests or the test-seams build use it"
)]
pub(crate) fn arm_currency_reclaim_fault_for_test(boundary: u8, crash: bool) {
    assert!((1..=34).contains(&boundary));
    assert!(
        crash || boundary != 3,
        "legacy boundary 3 remains crash-only"
    );
    CURRENCY_RECLAIM_FAULT.with(|armed| armed.set((boundary, crash)));
}

#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
pub(super) fn currency_reclaim_checkpoint_for_test(boundary: u8) -> Result<()> {
    crate::read_work_probe::note_graph(
        "publication",
        "checkpoint",
        &[boundary],
        None,
        Some("currency_reclaim_checkpoint_for_test"),
    );
    let crash = CURRENCY_RECLAIM_FAULT.with(|armed| {
        let (selected, crash) = armed.get();
        if selected != boundary {
            return None;
        }
        armed.set((0, false));
        Some(crash)
    });
    match crash {
        Some(true) => {
            eprintln!("CURRENCY_RECLAIM_CRASH={boundary}");
            std::process::abort();
        }
        Some(false) => Err(Error::Other(
            ("injected currency cleanup failure".to_string()).into(),
        )),
        None => Ok(()),
    }
}

#[cfg(any(test, feature = "test-seams"))]
impl RedbPersistence {
    /// Count physical durable keys without decoding bodies or building a mirror.
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(crate) fn durable_version_count_for_test(&self, name: &str) -> Result<usize> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let name = Self::rel_table_name(name);
            let definition = TableDefinition::<&[u8], &[u8]>::new(&name);
            let table = read.open_table(definition).map_err(Self::storage_error)?;
            Ok(table.len().map_err(Self::storage_error)? as usize)
        })
    }
}
// Extends the existing retention/currency fault cells; no independent fault index.
// 4 repack, 5 projection, 6 lifecycle, 7 location, 8 history, 9 bounds,
// 10 final precommit, 11 postcommit reporting (publication has already succeeded).
// 12 mixed-retention progress; 13 scalar bounds; 14 commit directory; 15 row heads;
// 16 relational counts; 17 scoped maxima; 18 vector fingerprints;
// 19 recovery fingerprint; 20 scoped fingerprint; 21..=25 graph tags 1..=5;
// 26 final metadata binding.
// 27..=31 compaction progress/vector/commit-directory/row-head/final binding;
// 32..=34 imported scalar bounds/commit-directory/final binding.
#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
pub(super) fn graph_observation_checkpoint(boundary: u8) -> Result<()> {
    retention_reclaim_checkpoint_for_test(boundary);
    currency_reclaim_checkpoint_for_test(boundary)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn admitted_relational_index_endpoint(
    key: &[DirectedValue],
    row_id: RowId,
    created_tx: TxId,
    memory: &crate::read_image_memory::ImageMemoryHandle,
) -> Result<Vec<u8>> {
    admitted_relational_index_endpoint_with_checkpoint(key, row_id, created_tx, memory, &mut || {
        Ok(())
    })
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn admitted_relational_index_endpoint_with_checkpoint(
    key: &[DirectedValue],
    row_id: RowId,
    created_tx: TxId,
    memory: &crate::read_image_memory::ImageMemoryHandle,
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<Vec<u8>> {
    admitted_relational_index_parts_with_checkpoint(
        key.len(),
        key.iter().map(|part| match part {
            DirectedValue::Asc(TotalOrdAsc(value)) => (false, value),
            DirectedValue::Desc(TotalOrdDesc(value)) => (true, value),
        }),
        row_id,
        created_tx,
        memory,
        checkpoint,
    )
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn admitted_relational_row_index_endpoint_with_checkpoint(
    declaration: &IndexDecl,
    row: &VersionedRow,
    created_tx: TxId,
    memory: &crate::read_image_memory::ImageMemoryHandle,
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<Vec<u8>> {
    // Borrow values from the already admitted row instead of deep cloning its key.
    admitted_relational_index_parts_with_checkpoint(
        declaration.columns.len(),
        declaration.columns.iter().map(|(column, direction)| {
            (
                matches!(direction, contextdb_core::SortDirection::Desc),
                row.values.get(column).unwrap_or(&Value::Null),
            )
        }),
        row.row_id,
        created_tx,
        memory,
        checkpoint,
    )
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn admitted_relational_index_parts_with_checkpoint<'v, I>(
    key_len: usize,
    key: I,
    row_id: RowId,
    created_tx: TxId,
    memory: &crate::read_image_memory::ImageMemoryHandle,
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<Vec<u8>>
where
    I: Clone + Iterator<Item = (bool, &'v Value)>,
{
    // Emit the existing bincode tuple layout directly. Borrowed Value's JSON
    // serializer otherwise creates an unowned whole-document String twice.
    struct EndpointWriter<'a> {
        encoded: Option<&'a mut Vec<u8>>,
        bytes: usize,
        checkpoint: &'a mut dyn FnMut() -> Result<()>,
        failure: Option<Error>,
    }
    impl std::io::Write for EndpointWriter<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if let Err(error) = (self.checkpoint)() {
                self.failure = Some(error);
                return Err(std::io::Error::other("index endpoint checkpoint refused"));
            }
            self.bytes = self
                .bytes
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("index endpoint size overflow"))?;
            if let Some(encoded) = self.encoded.as_deref_mut() {
                if self.bytes > encoded.capacity() {
                    return Err(std::io::Error::other(
                        "index endpoint exceeded admitted capacity",
                    ));
                }
                for window in bytes.chunks(64 * 1024) {
                    if let Err(error) = (self.checkpoint)() {
                        self.failure = Some(error);
                        return Err(std::io::Error::other("index endpoint checkpoint refused"));
                    }
                    encoded.extend_from_slice(window);
                }
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn encode<T: serde::Serialize>(value: T, writer: &mut EndpointWriter<'_>) -> Result<()> {
        let result =
            bincode::serde::encode_into_std_write(value, &mut *writer, bincode::config::standard());
        if let Some(error) = writer.failure.take() {
            return Err(error);
        }
        result
            .map(|_| ())
            .map_err(|error| Error::Other((format!("index endpoint encode: {error}")).into()))
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn json(value: &serde_json::Value, writer: &mut EndpointWriter<'_>) -> Result<()> {
        let result = serde_json::to_writer(&mut *writer, value);
        if let Some(error) = writer.failure.take() {
            return Err(error);
        }
        result
            .map_err(|error| Error::Other((format!("index endpoint JSON encode: {error}")).into()))
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn emit<'v>(
        key_len: usize,
        key: impl Iterator<Item = (bool, &'v Value)>,
        row_id: RowId,
        created_tx: TxId,
        writer: &mut EndpointWriter<'_>,
    ) -> Result<()> {
        let length = u64::try_from(key_len)
            .map_err(|_| Error::Other("index endpoint length overflow".into()))?;
        encode(length, writer)?;
        for (descending, value) in key {
            encode(descending, writer)?;
            if let Value::Json(value) = value {
                let mut measured = EndpointWriter {
                    encoded: None,
                    bytes: 0,
                    checkpoint: &mut *writer.checkpoint,
                    failure: None,
                };
                json(value, &mut measured)?;
                let length = u64::try_from(measured.bytes)
                    .map_err(|_| Error::Other("index JSON length overflow".into()))?;
                drop(measured);
                // Value::Json is variant seven and its payload is a string.
                encode(7u32, writer)?;
                encode(length, writer)?;
                json(value, writer)?;
            } else {
                encode(value, writer)?;
            }
        }
        encode(row_id, writer)?;
        encode(created_tx, writer)
    }
    checkpoint()?;
    let controls = std::mem::size_of::<EndpointWriter<'_>>()
        .checked_mul(2)
        .and_then(|n| {
            n.checked_add(std::mem::size_of::<
                serde_json::Serializer<&mut EndpointWriter<'_>>,
            >())
        })
        .ok_or_else(|| Error::Other("index endpoint controls overflow".into()))?;
    memory.reserve(controls)?;
    let result = (|| {
        let mut measured = EndpointWriter {
            encoded: None,
            bytes: 0,
            checkpoint,
            failure: None,
        };
        emit(key_len, key.clone(), row_id, created_tx, &mut measured)?;
        let capacity = measured.bytes;
        drop(measured);
        checkpoint()?;
        memory.reserve(capacity)?;
        let mut encoded = Vec::with_capacity(capacity);
        let mut output = EndpointWriter {
            encoded: Some(&mut encoded),
            bytes: 0,
            checkpoint,
            failure: None,
        };
        emit(key_len, key, row_id, created_tx, &mut output)?;
        if output.bytes != capacity {
            return Err(Error::Other("index endpoint size changed".into()));
        }
        drop(output);
        Ok(encoded)
    })();
    memory.release(controls);
    result
}

impl OwnedColdEventSource<'_> {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn extent(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        sink: &str,
    ) -> Result<(u64, u64, redb::StorageSnapshotIdentity)> {
        self.extent_in(original, sink, RedbPersistence::storage_error, |error| {
            error
        })
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn visit_range(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        sink: &str,
        after: Option<u64>,
        upper: u64,
        visit: impl for<'e> FnMut(u64, &'e [u8]) -> Result<bool>,
    ) -> Result<bool> {
        self.visit_range_in(
            original,
            sink,
            after,
            upper,
            visit,
            RedbPersistence::storage_error,
            |error| error,
        )
    }

    pub(crate) fn extent_in<E>(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        sink: &str,
        storage: impl Fn(redb::Error) -> E,
        existing: impl Fn(Error) -> E,
    ) -> std::result::Result<(u64, u64, redb::StorageSnapshotIdentity), E> {
        let key =
            OwnedEventKey::concatenated(original, "__sink_queue_", sink).map_err(&existing)?;
        let definition: TableDefinition<u64, &[u8]> = TableDefinition::new(&key.value);
        let root = self.read.storage_snapshot_identity();
        let table = match self.read.open_table(definition) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok((0, 0, root)),
            Err(error) => return Err(storage(error.into())),
        };
        let depth = table.len().map_err(|error| storage(error.into()))?;
        let newest = table
            .last()
            .map_err(|error| storage(error.into()))?
            .map_or(0, |(id, _)| id.value());
        Ok((depth, newest, root))
    }

    /// Return true only when the selected interval is exhausted. A false
    /// callback stops immediately while the current value guard is live;
    /// there is no encoded-body staging vector or backlog count traversal.
    pub(crate) fn visit_range_in<E>(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        sink: &str,
        after: Option<u64>,
        upper: u64,
        mut visit: impl for<'e> FnMut(u64, &'e [u8]) -> std::result::Result<bool, E>,
        storage: impl Fn(redb::Error) -> E,
        existing: impl Fn(Error) -> E,
    ) -> std::result::Result<bool, E> {
        let start = match after {
            Some(id) => match id.checked_add(1) {
                Some(id) => id,
                None => return Ok(true),
            },
            None => 0,
        };
        if start > upper {
            return Ok(true);
        }
        let key =
            OwnedEventKey::concatenated(original, "__sink_queue_", sink).map_err(&existing)?;
        let definition: TableDefinition<u64, &[u8]> = TableDefinition::new(&key.value);
        let table = match self.read.open_table(definition) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(true),
            Err(error) => return Err(storage(error.into())),
        };
        for entry in table
            .range(start..=upper)
            .map_err(|error| storage(error.into()))?
        {
            let (id, bytes) = entry.map_err(|error| storage(error.into()))?;
            if !visit(id.value(), bytes.value())? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

impl OwnedEventReadKey {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn queue_table(
        sink: &str,
        memory: &contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<Self> {
        let bytes = "__sink_queue_"
            .len()
            .checked_add(sink.len())
            .ok_or_else(|| Error::Other("sink queue table name size overflow".into()))?;
        memory.try_reserve(
            bytes,
            "reader",
            "sink_queue_source",
            "Reduce concurrent queue reads or raise the declared reader memory limit.",
        )?;
        // Install the refund before any fallible construction or validation.
        let mut key = Self {
            value: String::new(),
            memory: memory.clone(),
            bytes,
        };
        key.value = String::with_capacity(bytes);
        if key.value.capacity() != bytes {
            return Err(Error::Other(
                "sink queue source key capacity differs from its admission".into(),
            ));
        }
        key.value.push_str("__sink_queue_");
        key.value.push_str(sink);
        Ok(key)
    }
}

impl Drop for OwnedEventReadKey {
    fn drop(&mut self) {
        // The concrete String allocation must disappear before its invoice.
        drop(std::mem::take(&mut self.value));
        self.memory.release(self.bytes);
    }
}

impl OwnedEncodedEventSource<'_> {
    pub(crate) fn cold_queue_source(&self) -> OwnedColdEventSource<'_> {
        OwnedColdEventSource { read: self.read }
    }

    /// Decode while the value guard and the one captured event root are live.
    /// No current-root config lookup or encoded-body copy is involved.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn decode_owned_config<T: serde::de::DeserializeOwned>(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        key: &str,
    ) -> Result<Option<OriginalDecoded<T>>> {
        self.decode_owned_config_in(
            original,
            key,
            RedbPersistence::decode_owned_config_value,
            RedbPersistence::storage_error,
        )
    }

    pub(crate) fn decode_owned_config_funded<T: serde::de::DeserializeOwned>(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        key: &str,
    ) -> crate::funded_error::OriginalResult<Option<OriginalDecoded<T>>> {
        let diagnostic = crate::funded_error::OriginalError::prepare_route(original)?;
        self.decode_owned_config_in(
            original,
            key,
            RedbPersistence::decode_owned_config_value_funded,
            |error| diagnostic.storage(original, error),
        )
    }

    pub(super) fn decode_owned_config_in<T, E>(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        key: &str,
        decode: impl FnOnce(
            &crate::memory_accounting::OwnedMemoryReservation,
            &[u8],
        ) -> std::result::Result<OriginalDecoded<T>, E>,
        storage: impl Fn(redb::Error) -> E,
    ) -> std::result::Result<Option<OriginalDecoded<T>>, E> {
        let table = match self.read.open_table(CONFIG_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(storage(error.into())),
        };
        let encoded = table.get(key).map_err(|error| storage(error.into()))?;
        encoded
            .map(|encoded| decode(original, encoded.value()))
            .transpose()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn decode_owned_counter(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        prefix: &str,
        sink: &str,
    ) -> Result<u64> {
        let key = OwnedEventKey::concatenated(original, prefix, sink)?;
        Ok(self
            .decode_owned_config::<u64>(original, &key.value)?
            .map_or(0, |decoded| decoded.value))
    }

    pub(crate) fn decode_owned_counter_funded(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        prefix: &str,
        sink: &str,
    ) -> crate::funded_error::OriginalResult<u64> {
        let diagnostic = crate::funded_error::OriginalError::prepare_route(original)?;
        let key = OwnedEventKey::concatenated(original, prefix, sink)
            .map_err(|error| diagnostic.error(error))?;
        Ok(self
            .decode_owned_config_funded::<u64>(original, &key.value)?
            .map_or(0, |decoded| decoded.value))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn visit(
        &self,
        sink: &str,
        visit: impl for<'e> FnMut(u64, &'e [u8]) -> Result<()>,
    ) -> Result<()> {
        self.visit_in(sink, visit, RedbPersistence::storage_error, |error| error)
    }
    pub(crate) fn visit_funded(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        sink: &str,
        visit: impl for<'e> FnMut(u64, &'e [u8]) -> crate::funded_error::OriginalResult<()>,
    ) -> crate::funded_error::OriginalResult<()> {
        let diagnostic = crate::funded_error::OriginalError::prepare_route(original)?;
        self.visit_in(
            sink,
            visit,
            |error| diagnostic.storage(original, error),
            |error| diagnostic.error(error),
        )
    }
    pub(super) fn visit_in<E>(
        &self,
        sink: &str,
        mut visit: impl for<'e> FnMut(u64, &'e [u8]) -> std::result::Result<(), E>,
        storage: impl Fn(redb::Error) -> E,
        existing: impl Fn(Error) -> E,
    ) -> std::result::Result<(), E> {
        if let Some(entries) = self.loaded.and_then(|loaded| loaded.get(sink)) {
            for (id, bytes) in entries {
                visit(*id, bytes)?;
            }
            return Ok(());
        }
        let name = OwnedEventReadKey::queue_table(sink, self.read_memory).map_err(&existing)?;
        let definition: TableDefinition<u64, &[u8]> = TableDefinition::new(&name.value);
        let table = match self.read.open_table(definition) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
            Err(error) => return Err(storage(error.into())),
        };
        for entry in table.iter().map_err(|error| storage(error.into()))? {
            let (id, encoded) = entry.map_err(|error| storage(error.into()))?;
            // No encoded buffer is copied. The storage guard and captured
            // transaction own these bytes until the synchronous visit ends.
            visit(id.value(), encoded.value())?;
        }
        Ok(())
    }
}

impl RedbPersistence {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn with_owned_cold_event_source<R>(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        f: impl for<'s> FnOnce(&OwnedColdEventSource<'s>) -> Result<R>,
    ) -> Result<R> {
        // Admit our bounded source/transaction control before opening the pin.
        // Storage pages retain their existing persistence admission owner.
        let _control = crate::memory_accounting::reserve_preparation_from_original(
            original,
            std::mem::size_of::<redb::ReadTransaction>()
                .checked_add(std::mem::size_of::<OwnedColdEventSource<'_>>())
                .ok_or_else(|| Error::Other("cold event source control overflow".into()))?,
            "events",
            "sink_queue_source",
            "Reduce concurrent deliveries or raise MEMORY_LIMIT.",
        )?;
        let storage_scope =
            native_storage_admission::StorageAdmissionScope::writer_original(original)?;
        storage_scope.run_original_unqualified(|| {
            self.with_db_handle(|db| {
                let read = db.begin_read().map_err(Self::storage_error)?;
                f(&OwnedColdEventSource { read: &read })
            })
        })
    }

    pub(crate) fn with_owned_cold_event_source_funded<R>(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        f: impl for<'s> FnOnce(&OwnedColdEventSource<'s>) -> crate::funded_error::OriginalResult<R>,
    ) -> crate::funded_error::OriginalResult<R> {
        let diagnostic = crate::funded_error::OriginalError::prepare_route(original)?;
        let _control = crate::memory_accounting::reserve_preparation_original_native(
            original,
            std::mem::size_of::<redb::ReadTransaction>()
                .checked_add(std::mem::size_of::<OwnedColdEventSource<'_>>())
                .ok_or_else(|| {
                    crate::funded_error::OriginalError::native(
                        contextdb_core::read_memory::NativeRefusal::Other(
                            "cold event source control overflow",
                        ),
                    )
                })?,
            "events",
            "sink_queue_source",
            "Reduce concurrent deliveries or raise MEMORY_LIMIT.",
        )
        .map_err(crate::funded_error::OriginalError::native)?;
        let storage_scope =
            native_storage_admission::StorageAdmissionScope::writer_original_native(original)
                .map_err(crate::funded_error::OriginalError::native)?;
        storage_scope.run_original(
            crate::funded_error::OriginalStoragePhase::ColdSource,
            || {
                self.with_db_handle_funded(original, |db| {
                    let read = db
                        .begin_read()
                        .map_err(|error| diagnostic.storage(original, error))?;
                    f(&OwnedColdEventSource { read: &read })
                })
            },
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn with_owned_encoded_event_source<R>(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        f: impl for<'s> FnOnce(&'s OwnedEncodedEventSource<'s>) -> Result<R>,
    ) -> Result<R> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let source = OwnedEncodedEventSource {
                read: &read,
                loaded: None,
                read_memory: &read_memory,
            };
            f(&source)
        })
    }
}

impl std::io::Write for OwnedEventEncodedLength {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn owned_event_decimal_width(mut value: u64) -> usize {
    let mut width = 1;
    while value >= 10 {
        value /= 10;
        width += 1;
    }
    width
}

impl OwnedEventKey {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn queue_identity(
        original: &crate::memory_accounting::OwnedMemoryReservation,
        sink: &str,
        id: u64,
    ) -> Result<Self> {
        use std::fmt::Write;
        let sink_length = u64::try_from(sink.len())
            .map_err(|_| Error::Other("sink name size overflow".into()))?;
        let bytes = owned_event_decimal_width(sink_length)
            .checked_add(sink.len())
            .and_then(|n| n.checked_add(2))
            .and_then(|n| n.checked_add(owned_event_decimal_width(id)))
            .ok_or_else(|| Error::Other("sink queue identity size overflow".into()))?;
        let allocation = crate::memory_accounting::reserve_preparation_from_original(
            original,
            bytes,
            "events",
            "sink_queue_identity",
            "Reduce pending deliveries or raise MEMORY_LIMIT.",
        )?;
        let mut value = String::with_capacity(bytes);
        if value.capacity() != bytes {
            drop(value);
            return Err(Error::Other(
                "sink queue identity capacity differs from its admission".into(),
            ));
        }
        if write!(&mut value, "{}:{sink}:{id}", sink.len()).is_err() {
            drop(value);
            return Err(Error::Other("cannot format sink queue identity".into()));
        }
        if value.len() != bytes || value.capacity() != bytes {
            drop(value);
            return Err(Error::Other(
                "sink queue identity differs from its measured size".into(),
            ));
        }
        Ok(Self { value, allocation })
    }
}

impl RedbPersistence {
    /// Encode into one concretely priced buffer; a length mismatch refuses
    /// before any durable write rather than growing a naked Vec.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn encode_owned_event<T: serde::Serialize>(
        original: &crate::memory_accounting::OwnedMemoryReservation,
        value: &T,
    ) -> Result<OwnedEncodedBytes> {
        let mut diagnostic = Some(
            crate::funded_error::OriginalError::prepare(
                original,
                "relational",
                "statement_overlay",
                "Reduce prepared event bodies or raise MEMORY_LIMIT.",
            )
            .map_err(|failure| {
                native_storage_admission::original_storage_error(
                    native_storage_admission::NativeStorageFailure::Refused(failure),
                )
            })?,
        );
        Self::encode_owned_event_in(
            original,
            value,
            |error| {
                diagnostic
                    .take()
                    .expect("one failed encoding pass")
                    .encode(
                        error,
                        crate::memory_accounting::retain_preparation_original(original),
                    )
                    .into_public_prepared()
            },
            |error| error,
        )
    }
    pub(crate) fn encode_owned_event_funded<T: serde::Serialize>(
        original: &crate::memory_accounting::OwnedMemoryReservation,
        value: &T,
    ) -> crate::funded_error::OriginalResult<OwnedEncodedBytes> {
        let diagnostic = crate::funded_error::OriginalError::prepare_route(original)?;
        Self::encode_owned_event_in(
            original,
            value,
            |error| {
                diagnostic.encode(
                    error,
                    crate::memory_accounting::retain_preparation_original(original),
                )
            },
            |error| diagnostic.error(error),
        )
    }
    pub(super) fn encode_owned_event_in<T: serde::Serialize, E>(
        original: &crate::memory_accounting::OwnedMemoryReservation,
        value: &T,
        mut codec: impl FnMut(bincode::error::EncodeError) -> E,
        existing: impl Fn(Error) -> E,
    ) -> std::result::Result<OwnedEncodedBytes, E> {
        #[cfg(test)]
        let _purge_producer =
            content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::new::<T>(
                "RedbPersistence::encode_owned_event_in",
            );
        let mut measured = OwnedEventEncodedLength(0);
        bincode::serde::encode_into_std_write(value, &mut measured, bincode::config::standard())
            .map_err(&mut codec)?;
        let buffer = crate::memory_accounting::new_owned_native_vec::<u8>(original, measured.0)
            .map_err(&existing)?;
        // Establish body-before-invoice custody before invoking Serialize,
        // which can return an error or unwind on its second pass.
        let mut encoded = OwnedEncodedBytes {
            bytes: buffer.0,
            allocation: buffer.1,
        };
        encoded.bytes.resize(measured.0, 0);
        let written = bincode::serde::encode_into_slice(
            value,
            encoded.bytes.as_mut_slice(),
            bincode::config::standard(),
        )
        .map_err(codec)?;
        if written != measured.0 || encoded.bytes.capacity() != measured.0 {
            return Err(existing(Error::Other(
                "sink event encoding differs from its measured size".into(),
            )));
        }
        Ok(encoded)
    }

    /// Prepare the exact original status-removal wire keys without invoking
    /// the Legacy key constructors or an unowned temporary mutation vector.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn prepare_owned_sink_queue_removals(
        original: &crate::memory_accounting::OwnedMemoryReservation,
        sink: &str,
        ids: &[u64],
    ) -> Result<OwnedSinkQueueRemovals> {
        // Keep each factory's body-first tuple intact across the next
        // fallible admission; split locals would refund before buffer Drop.
        let mutations = crate::memory_accounting::new_owned_native_vec(original, ids.len())?;
        let credits = crate::memory_accounting::new_owned_native_vec(original, ids.len())?;
        // Aggregate ownership is established before the first key is copied,
        // including all error/unwind paths through this loop.
        let mut prepared = OwnedSinkQueueRemovals {
            mutations: mutations.0,
            credits: credits.0,
            mutations_allocation: mutations.1,
            credits_allocation: credits.1,
        };
        for id in ids {
            let key = OwnedEventKey::queue_identity(original, sink, *id)?;
            let namespace = b"sink-queue";
            let length = 16_usize
                .checked_add(namespace.len())
                .and_then(|n| n.checked_add(key.value.len()))
                .ok_or_else(|| Error::Other("sink queue logical key size overflow".into()))?;
            let (mut logical_key, allocation) =
                crate::memory_accounting::new_owned_native_vec::<u8>(original, length)?;
            logical_key.extend_from_slice(&(namespace.len() as u64).to_le_bytes());
            logical_key.extend_from_slice(namespace);
            logical_key.extend_from_slice(&(key.value.len() as u64).to_le_bytes());
            logical_key.extend_from_slice(key.value.as_bytes());
            prepared
                .mutations
                .push(content_identity::ContentIdentityMutation {
                    family: content_identity::LogicalFamily::Status,
                    logical_key,
                    logical_value: None,
                    row_head: None,
                });
            prepared.credits.push(allocation);
        }
        Ok(prepared)
    }
}

impl OwnedEventKey {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn concatenated(
        original: &crate::memory_accounting::OwnedMemoryReservation,
        prefix: &str,
        suffix: &str,
    ) -> Result<Self> {
        let bytes = prefix
            .len()
            .checked_add(suffix.len())
            .ok_or_else(|| Error::Other("sink queue key size overflow".into()))?;
        let allocation = crate::memory_accounting::reserve_preparation_from_original(
            original,
            bytes,
            "events",
            "sink_queue_identity",
            "Reduce pending deliveries or raise MEMORY_LIMIT.",
        )?;
        let mut value = String::with_capacity(bytes);
        value.push_str(prefix);
        value.push_str(suffix);
        if value.capacity() != bytes {
            return Err(Error::Other(
                "sink queue key capacity differs from admission".into(),
            ));
        }
        Ok(Self { value, allocation })
    }
}

impl RedbPersistence {
    /// Keep the ordinary config decoder's prefix compatibility while lending
    /// the caller's authentic original to every decoded native allocation.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn decode_owned_config_value<T: serde::de::DeserializeOwned>(
        original: &crate::memory_accounting::OwnedMemoryReservation,
        bytes: &[u8],
    ) -> Result<OriginalDecoded<T>> {
        original_event_decode::decode_config_compatible(original, bytes)
    }

    /// The streamed blob projection uses exactly the same diagnostic adapter
    /// as the slice path; its reader keeps the original storage failure.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn decode_owned_blob_reader<T: StreamedBlobProjection, R: std::io::Read>(
        original: &crate::memory_accounting::OwnedMemoryReservation,
        reader: &mut R,
    ) -> Result<OriginalDecoded<T>> {
        original_event_decode::decode_blob_reader(original, reader)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn decode_owned_event<T: serde::de::DeserializeOwned>(
        original: &crate::memory_accounting::OwnedMemoryReservation,
        bytes: &[u8],
    ) -> Result<OriginalDecoded<T>> {
        original_event_decode::decode(original, bytes)
    }

    pub(crate) fn decode_owned_event_funded<T: serde::de::DeserializeOwned>(
        original: &crate::memory_accounting::OwnedMemoryReservation,
        bytes: &[u8],
    ) -> crate::funded_error::OriginalResult<OriginalDecoded<T>> {
        original_event_decode::decode_funded(original, bytes, true)
    }
    pub(crate) fn decode_owned_config_value_funded<T: serde::de::DeserializeOwned>(
        original: &crate::memory_accounting::OwnedMemoryReservation,
        bytes: &[u8],
    ) -> crate::funded_error::OriginalResult<OriginalDecoded<T>> {
        original_event_decode::decode_funded(original, bytes, false)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn update_owned_sink_queue_entry<T: serde::Serialize>(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        sink: &str,
        remove_id: Option<u64>,
        put: Option<(u64, &T)>,
        uncertain: bool,
        unconfirmed_delta: i8,
    ) -> Result<()> {
        if remove_id.is_none() && put.is_none() {
            return Ok(());
        }
        // All escaping serialization/key controls are paid by this immutable
        // seed before the transaction can publish their corresponding writes.
        let table_name = OwnedEventKey::concatenated(original, "__sink_queue_", sink)?;
        let remove_key = remove_id
            .map(|id| OwnedEventKey::queue_identity(original, sink, id))
            .transpose()?;
        let put_key = put
            .map(|(id, _)| OwnedEventKey::queue_identity(original, sink, id))
            .transpose()?;
        let encoded_put = put
            .map(|(_, entry)| Self::encode_owned_event(original, entry))
            .transpose()?;
        let uncertain_key = uncertain
            .then(|| {
                OwnedEventKey::concatenated(
                    original,
                    crate::database::event_bus::UNCERTAIN_COUNT_CONFIG_PREFIX,
                    sink,
                )
            })
            .transpose()?;
        let unconfirmed_key = (unconfirmed_delta != 0)
            .then(|| {
                OwnedEventKey::concatenated(
                    original,
                    crate::database::event_bus::UNCONFIRMED_COUNT_CONFIG_PREFIX,
                    sink,
                )
            })
            .transpose()?;
        let mut identity = content_identity::OriginalIdentityMutations::new(original, 4)?;
        let mut proof = content_identity::original_writer::ProofAdmission::new(
            original,
            self.proof_scratch_owner(),
        )?;
        if let Some(key) = &remove_key {
            identity.push_status::<u64>(b"sink-queue", key.value.as_bytes(), None)?;
        }
        if let (Some(key), Some((_, entry))) = (&put_key, put) {
            identity.push_status(b"sink-queue", key.value.as_bytes(), Some(entry))?;
        }
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            {
                let definition: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(&table_name.value);
                let mut table = write_txn
                    .open_table(definition)
                    .map_err(Self::storage_error)?;
                if let Some(id) = remove_id {
                    table.remove(id).map_err(Self::storage_error)?;
                }
                if let (Some((id, _)), Some(encoded)) = (put, &encoded_put) {
                    table
                        .insert(id, encoded.bytes.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            if let Some(key) = &uncertain_key {
                let mut table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                let previous: u64 = match table
                    .get(key.value.as_str())
                    .map_err(Self::storage_error)?
                {
                    Some(value) => Self::decode_owned_event::<u64>(original, value.value())?.value,
                    None => 0,
                };
                let encoded = Self::encode_owned_event(original, &previous.saturating_add(1))?;
                table
                    .insert(key.value.as_str(), encoded.bytes.as_slice())
                    .map_err(Self::storage_error)?;
                identity.push_config(&key.value, Some(&encoded.bytes))?;
            }
            if let Some(key) = &unconfirmed_key {
                let mut table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                let previous: u64 = match table
                    .get(key.value.as_str())
                    .map_err(Self::storage_error)?
                {
                    Some(value) => Self::decode_owned_event::<u64>(original, value.value())?.value,
                    None => 0,
                };
                let next = if unconfirmed_delta > 0 {
                    previous.checked_add(1)
                } else {
                    previous.checked_sub(1)
                }
                .ok_or_else(|| Error::Other(("sink attempt summary overflow".to_owned()).into()))?;
                let encoded = Self::encode_owned_event(original, &next)?;
                table
                    .insert(key.value.as_str(), encoded.bytes.as_slice())
                    .map_err(Self::storage_error)?;
                identity.push_config(&key.value, Some(&encoded.bytes))?;
            }
            Self::apply_owned_content_identity_with_manifests_in_write(
                &write_txn,
                &identity.mutations,
                &[],
                &mut proof,
            )?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
            {
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10)?;
                let graph_commit_result = write_txn.commit();
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)?;
            Ok(())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn remove_owned_sink_queue_entries(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        sink: &str,
        remove_ids: &[u64],
        unconfirmed: u64,
    ) -> Result<()> {
        if remove_ids.is_empty() {
            return Ok(());
        }
        let table_name = OwnedEventKey::concatenated(original, "__sink_queue_", sink)?;
        let removals = Self::prepare_owned_sink_queue_removals(original, sink, remove_ids)?;
        let summary_key = (unconfirmed > 0)
            .then(|| {
                OwnedEventKey::concatenated(
                    original,
                    crate::database::event_bus::UNCONFIRMED_COUNT_CONFIG_PREFIX,
                    sink,
                )
            })
            .transpose()?;
        let mut identity = content_identity::OriginalIdentityMutations::new(original, 1)?;
        let mut proof = content_identity::original_writer::ProofAdmission::new(
            original,
            self.proof_scratch_owner(),
        )?;
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            {
                let definition: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(&table_name.value);
                let mut table = write_txn
                    .open_table(definition)
                    .map_err(Self::storage_error)?;
                for id in remove_ids {
                    table.remove(*id).map_err(Self::storage_error)?;
                }
            }
            if let Some(key) = &summary_key {
                let mut table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                let previous: u64 = match table
                    .get(key.value.as_str())
                    .map_err(Self::storage_error)?
                {
                    Some(value) => Self::decode_owned_event::<u64>(original, value.value())?.value,
                    None => 0,
                };
                let next = previous.checked_sub(unconfirmed).ok_or_else(|| {
                    Error::Other(("sink attempt summary overflow".to_owned()).into())
                })?;
                let encoded = Self::encode_owned_event(original, &next)?;
                table
                    .insert(key.value.as_str(), encoded.bytes.as_slice())
                    .map_err(Self::storage_error)?;
                identity.push_config(&key.value, Some(&encoded.bytes))?;
            }
            Self::apply_owned_content_identity_with_manifests_in_write(
                &write_txn,
                &removals.mutations,
                &identity.mutations,
                &mut proof,
            )?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
            {
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10)?;
                let graph_commit_result = write_txn.commit();
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)?;
            Ok(())
        })
    }

    // Dormant private queue companions retain the original methods above.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn update_owned_sink_queue_entry_at_boundary<T: serde::Serialize>(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        sink: &str,
        remove_id: Option<u64>,
        put: Option<(u64, &T)>,
        uncertain: bool,
        unconfirmed_delta: i8,
        completion: &crate::database::event_bus::OwnedQueueCommitToken<'_>,
        cleanup: &mut crate::database::event_bus::OwnedQueueCommitCleanup,
    ) -> crate::funded_error::OriginalResult<()> {
        if remove_id.is_none() && put.is_none() {
            return Ok(());
        }
        let diagnostic = crate::funded_error::OriginalError::prepare_route(original)?;
        let prepared = (|| -> crate::funded_error::OriginalResult<_> {
            // All escaping serialization/key controls are paid by this immutable
            // seed before the transaction can publish their corresponding writes.
            let table_name = OwnedEventKey::concatenated(original, "__sink_queue_", sink)
                .map_err(|error| diagnostic.error(error))?;
            let remove_key = remove_id
                .map(|id| OwnedEventKey::queue_identity(original, sink, id))
                .transpose()
                .map_err(|error| diagnostic.error(error))?;
            let put_key = put
                .map(|(id, _)| OwnedEventKey::queue_identity(original, sink, id))
                .transpose()
                .map_err(|error| diagnostic.error(error))?;
            let encoded_put = put
                .map(|(_, entry)| Self::encode_owned_event_funded(original, entry))
                .transpose()?;
            let uncertain_key = uncertain
                .then(|| {
                    OwnedEventKey::concatenated(
                        original,
                        crate::database::event_bus::UNCERTAIN_COUNT_CONFIG_PREFIX,
                        sink,
                    )
                })
                .transpose()
                .map_err(|error| diagnostic.error(error))?;
            let unconfirmed_key = (unconfirmed_delta != 0)
                .then(|| {
                    OwnedEventKey::concatenated(
                        original,
                        crate::database::event_bus::UNCONFIRMED_COUNT_CONFIG_PREFIX,
                        sink,
                    )
                })
                .transpose()
                .map_err(|error| diagnostic.error(error))?;
            let mut identity = content_identity::OriginalIdentityMutations::new(original, 4)
                .map_err(|error| diagnostic.error(error))?;
            let proof = content_identity::original_writer::ProofAdmission::new(
                original,
                self.proof_scratch_owner(),
            )
            .map_err(|error| diagnostic.error(error))?;
            if let Some(key) = &remove_key {
                identity
                    .push_status::<u64>(b"sink-queue", key.value.as_bytes(), None)
                    .map_err(|error| diagnostic.error(error))?;
            }
            if let (Some(key), Some((_, entry))) = (&put_key, put) {
                identity
                    .push_status(b"sink-queue", key.value.as_bytes(), Some(entry))
                    .map_err(|error| diagnostic.error(error))?;
            }
            Ok((
                table_name,
                remove_key,
                put_key,
                encoded_put,
                uncertain_key,
                unconfirmed_key,
                identity,
                proof,
            ))
        })();
        let (
            table_name,
            _remove_key,
            _put_key,
            encoded_put,
            uncertain_key,
            unconfirmed_key,
            mut identity,
            mut proof,
        ) = prepared?;
        // Preparation owners remain live through the result return.
        self.with_db_queue_original(original, cleanup, |db, cleanup| {
            let write_txn = Self::begin_history_write_funded(original, db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            {
                let definition: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(&table_name.value);
                let mut table = write_txn
                    .open_table(definition)
                    .map_err(|error| diagnostic.storage(original, error))?;
                if let Some(id) = remove_id {
                    table
                        .remove(id)
                        .map_err(|error| diagnostic.storage(original, error))?;
                }
                if let (Some((id, _)), Some(encoded)) = (put, &encoded_put) {
                    table
                        .insert(id, encoded.bytes.as_slice())
                        .map_err(|error| diagnostic.storage(original, error))?;
                }
            }
            if let Some(key) = &uncertain_key {
                let mut table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(|error| diagnostic.storage(original, error))?;
                let previous: u64 = match table
                    .get(key.value.as_str())
                    .map_err(|error| diagnostic.storage(original, error))?
                {
                    Some(value) => {
                        Self::decode_owned_event_funded::<u64>(original, value.value())?.value
                    }
                    None => 0,
                };
                let encoded =
                    Self::encode_owned_event_funded(original, &previous.saturating_add(1))?;
                table
                    .insert(key.value.as_str(), encoded.bytes.as_slice())
                    .map_err(|error| diagnostic.storage(original, error))?;
                identity
                    .push_config(&key.value, Some(&encoded.bytes))
                    .map_err(|error| diagnostic.error(error))?;
            }
            if let Some(key) = &unconfirmed_key {
                let mut table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(|error| diagnostic.storage(original, error))?;
                let previous: u64 = match table
                    .get(key.value.as_str())
                    .map_err(|error| diagnostic.storage(original, error))?
                {
                    Some(value) => {
                        Self::decode_owned_event_funded::<u64>(original, value.value())?.value
                    }
                    None => 0,
                };
                let next = if unconfirmed_delta > 0 {
                    previous.checked_add(1)
                } else {
                    previous.checked_sub(1)
                }
                .ok_or_else(|| Error::Other("sink attempt summary overflow".into()))
                .map_err(|error| diagnostic.error(error))?;
                let encoded = Self::encode_owned_event_funded(original, &next)?;
                table
                    .insert(key.value.as_str(), encoded.bytes.as_slice())
                    .map_err(|error| diagnostic.storage(original, error))?;
                identity
                    .push_config(&key.value, Some(&encoded.bytes))
                    .map_err(|error| diagnostic.error(error))?;
            }
            Self::apply_owned_content_identity_with_manifests_in_write(
                &write_txn,
                &identity.mutations,
                &[],
                &mut proof,
            )
            .map_err(|error| diagnostic.error(error))?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, None)
                .map_err(|error| diagnostic.error(error))?;
            {
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10).map_err(|error| diagnostic.error(error))?;
                self.commit_exact_queue_root(write_txn, original, completion, cleanup)?;
                #[cfg(any(test, feature = "test-seams"))]
                {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11).map_err(|error| diagnostic.error(error))?;
                }
            }
            Ok(())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn remove_owned_sink_queue_entries_at_boundary(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        sink: &str,
        remove_ids: &[u64],
        unconfirmed: u64,
        completion: &crate::database::event_bus::OwnedQueueCommitToken<'_>,
        cleanup: &mut crate::database::event_bus::OwnedQueueCommitCleanup,
    ) -> crate::funded_error::OriginalResult<()> {
        if remove_ids.is_empty() {
            return Ok(());
        }
        let diagnostic = crate::funded_error::OriginalError::prepare_route(original)?;
        let prepared = (|| -> crate::funded_error::OriginalResult<_> {
            let table_name = OwnedEventKey::concatenated(original, "__sink_queue_", sink)
                .map_err(|error| diagnostic.error(error))?;
            let removals = Self::prepare_owned_sink_queue_removals(original, sink, remove_ids)
                .map_err(|error| diagnostic.error(error))?;
            let summary_key = (unconfirmed > 0)
                .then(|| {
                    OwnedEventKey::concatenated(
                        original,
                        crate::database::event_bus::UNCONFIRMED_COUNT_CONFIG_PREFIX,
                        sink,
                    )
                })
                .transpose()
                .map_err(|error| diagnostic.error(error))?;
            let identity = content_identity::OriginalIdentityMutations::new(original, 1)
                .map_err(|error| diagnostic.error(error))?;
            let proof = content_identity::original_writer::ProofAdmission::new(
                original,
                self.proof_scratch_owner(),
            )
            .map_err(|error| diagnostic.error(error))?;
            Ok((table_name, removals, summary_key, identity, proof))
        })();
        let (table_name, removals, summary_key, mut identity, mut proof) = prepared?;
        self.with_db_queue_original(original, cleanup, |db, cleanup| {
            let write_txn = Self::begin_history_write_funded(original, db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            {
                let definition: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(&table_name.value);
                let mut table = write_txn
                    .open_table(definition)
                    .map_err(|error| diagnostic.storage(original, error))?;
                for id in remove_ids {
                    table
                        .remove(*id)
                        .map_err(|error| diagnostic.storage(original, error))?;
                }
            }
            if let Some(key) = &summary_key {
                let mut table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(|error| diagnostic.storage(original, error))?;
                let previous: u64 = match table
                    .get(key.value.as_str())
                    .map_err(|error| diagnostic.storage(original, error))?
                {
                    Some(value) => {
                        Self::decode_owned_event_funded::<u64>(original, value.value())?.value
                    }
                    None => 0,
                };
                let next = previous
                    .checked_sub(unconfirmed)
                    .ok_or_else(|| Error::Other("sink attempt summary overflow".into()))
                    .map_err(|error| diagnostic.error(error))?;
                let encoded = Self::encode_owned_event_funded(original, &next)?;
                table
                    .insert(key.value.as_str(), encoded.bytes.as_slice())
                    .map_err(|error| diagnostic.storage(original, error))?;
                identity
                    .push_config(&key.value, Some(&encoded.bytes))
                    .map_err(|error| diagnostic.error(error))?;
            }
            Self::apply_owned_content_identity_with_manifests_in_write(
                &write_txn,
                &removals.mutations,
                &identity.mutations,
                &mut proof,
            )
            .map_err(|error| diagnostic.error(error))?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, None)
                .map_err(|error| diagnostic.error(error))?;
            {
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10).map_err(|error| diagnostic.error(error))?;
                self.commit_exact_queue_root(write_txn, original, completion, cleanup)?;
                #[cfg(any(test, feature = "test-seams"))]
                {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11).map_err(|error| diagnostic.error(error))?;
                }
            }
            Ok(())
        })
    }
}

#[cfg(target_os = "macos")]
pub(super) fn companion_ownership_lock(
    file: &File,
    operation: CompanionOwnershipLock,
) -> std::io::Result<()> {
    let lock_type = match operation {
        CompanionOwnershipLock::Exclusive => RangeLockMode::Exclusive.lock_type(),
        CompanionOwnershipLock::Unlock => libc::F_UNLCK as libc::c_short,
    };
    match companion_range_lock(file, COMPANION_OWNER_LOCK_OFFSET, lock_type, false)? {
        RangeLockOutcome::Taken => Ok(()),
        RangeLockOutcome::HeldByAnother => Err(std::io::ErrorKind::WouldBlock.into()),
    }
}

#[cfg(not(target_os = "macos"))]
pub(super) fn companion_ownership_lock(
    file: &File,
    operation: CompanionOwnershipLock,
) -> std::io::Result<()> {
    match operation {
        CompanionOwnershipLock::Exclusive => fs2::FileExt::try_lock_exclusive(file),
        CompanionOwnershipLock::Unlock => fs2::FileExt::unlock(file),
    }
}
