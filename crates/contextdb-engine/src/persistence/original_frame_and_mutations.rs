use super::*;

impl<'o> OriginalPersistenceFrame<'o> {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn new(
        original: &'o crate::memory_accounting::OwnedMemoryReservation,
    ) -> Result<Self> {
        Ok(Self {
            original,
            work: std::cell::RefCell::new(
                crate::memory_accounting::reserve_preparation_from_original(
                    original,
                    0,
                    "persistence",
                    "prepare_commit",
                    "Raise MEMORY_LIMIT to admit commit preparation.",
                )?,
            ),
        })
    }
    pub(crate) fn into_reservation(self) -> crate::memory_accounting::OwnedMemoryReservation {
        self.work.into_inner()
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn reserve(&self, bytes: usize) -> Result<()> {
        crate::memory_accounting::grow_preparation_from_original(
            &mut self.work.borrow_mut(),
            bytes,
            "persistence",
            "prepare_commit",
            "Raise MEMORY_LIMIT to admit commit preparation.",
        )
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn retain(
        &self,
        credit: &mut crate::memory_accounting::OwnedMemoryReservation,
    ) -> Result<()> {
        crate::memory_accounting::merge_preparation_original(&mut self.work.borrow_mut(), credit)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn take_mutation(
        &self,
        mut owned: content_identity::OriginalIdentityMutation,
    ) -> Result<content_identity::ContentIdentityMutation> {
        content_identity::retain_original_identity_mutation(self, &mut owned)?;
        Ok(owned.mutation)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn canonical<T: serde::Serialize + ?Sized>(&self, value: &T) -> Result<Vec<u8>> {
        content_identity::retain_original_canonical(self, value)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn json_scratch<'a>(
        &self,
        values: impl IntoIterator<Item = &'a Value>,
    ) -> Result<Option<crate::memory_accounting::OwnedMemoryReservation>> {
        use contextdb_core::read_memory::NativeRefusal;
        struct Count(Option<usize>);
        impl std::io::Write for Count {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0 = self.0.and_then(|n| n.checked_add(bytes.len()));
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut bytes = 0usize;
        let mut diagnostic = None;
        for value in values {
            let Value::Json(json) = value else {
                continue;
            };
            if diagnostic.is_none() {
                diagnostic = Some(
                    crate::funded_error::OriginalError::prepare(
                        self.original,
                        "persistence",
                        "prepare_commit",
                        "Raise MEMORY_LIMIT to admit commit preparation.",
                    )
                    .map_err(Error::NativeEngineOther)?,
                );
            }
            let mut count = Count(Some(0));
            serde_json::to_writer(&mut count, json).map_err(|error| {
                diagnostic
                    .take()
                    .expect("measurement has its prepared unqualified cell")
                    .json_measure(error)
                    .into_public_prepared()
            })?;
            let measured = count
                .0
                .ok_or(Error::NativeEngineOther(NativeRefusal::Other(
                    "JSON serialization length overflow",
                )))?;
            bytes = bytes.max(measured);
        }
        drop(diagnostic);
        if bytes == 0 {
            return Ok(None);
        }
        // Selected Value fields and both outer passes serialize sequentially.
        // Their exact native temporaries die before reuse of this workspace;
        // destination buffers and outer diagnostic sources remain additive.
        let layout = std::alloc::Layout::array::<u8>(bytes).map_err(|_| {
            Error::NativeEngineOther(NativeRefusal::Other(
                "JSON serialization buffer layout overflow",
            ))
        })?;
        Ok(Some(
            crate::memory_accounting::reserve_preparation_original_native(
                self.original,
                layout.size(),
                "persistence",
                "prepare_commit",
                "Raise MEMORY_LIMIT to admit commit preparation.",
            )
            .map_err(Error::NativeEngineOther)?,
        ))
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn record<T: serde::Serialize + ?Sized>(&self, value: &T) -> Result<()> {
        self.reserve(content_identity::original_record_work_bytes(value)?)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn encode<T: serde::Serialize>(&self, value: &T) -> Result<Vec<u8>> {
        let mut encoded = RedbPersistence::encode_owned_event(self.original, value)?;
        self.retain(&mut encoded.allocation)?;
        Ok(encoded.bytes)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn decode<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T> {
        let mut decoded = RedbPersistence::decode_owned_event(self.original, bytes)?;
        self.retain(&mut decoded.allocation)?;
        Ok(decoded.value)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn encode_blob<T: serde::Serialize>(&self, value: &T) -> Result<Vec<u8>> {
        struct Counter(usize);
        impl std::io::Write for Counter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0 = self
                    .0
                    .checked_add(bytes.len())
                    .ok_or(std::io::ErrorKind::OutOfMemory)?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut counter = Counter(0);
        value
            .serialize(&mut rmp_serde::Serializer::new(&mut counter).with_struct_map())
            .map_err(|err| Error::Other(format!("encode blob repository record: {err}").into()))?;
        self.reserve(counter.0)?;
        let mut bytes = Vec::with_capacity(counter.0);
        value
            .serialize(&mut rmp_serde::Serializer::new(&mut bytes).with_struct_map())
            .map_err(|err| Error::Other(format!("encode blob repository record: {err}").into()))?;
        Ok(bytes)
    }
    /// MessagePack uses the same allocation-before-copy seed and exact
    /// Original as terminal event decoding. No reader or global payer is found.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn decode_blob<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T> {
        let mut decoded = original_event_decode::decode_blob(self.original, bytes)?;
        self.retain(&mut decoded.allocation)?;
        Ok(decoded.value)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn copy_bytes(&self, bytes: &[u8]) -> Result<Vec<u8>> {
        self.reserve(bytes.len())?;
        Ok(bytes.to_vec())
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn copy_text(&self, text: &str) -> Result<String> {
        self.reserve(text.len())?;
        Ok(text.to_owned())
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn array<T>(&self, count: usize) -> Result<()> {
        self.reserve(
            std::alloc::Layout::array::<T>(count)
                .map_err(|_| Error::Other("commit preparation array overflow".into()))?
                .size(),
        )
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn map<K, V>(&self, count: usize) -> Result<()> {
        self.reserve(
            crate::memory_accounting::native_map_plan::<K, V>(count)?
                .layout
                .size(),
        )
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn tree<K, V>(&self, count: usize) -> Result<()> {
        let node = std::mem::size_of::<(K, V)>()
            .checked_mul(11)
            .and_then(|n| n.checked_add(128))
            .ok_or_else(|| Error::Other("commit preparation tree overflow".into()))?;
        self.reserve(
            node.checked_mul(count)
                .ok_or_else(|| Error::Other("commit preparation tree overflow".into()))?,
        )
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn plan_config(&self, key: &str, value: &[u8]) -> Result<()> {
        let derived = custody_groups::plan_bytes(key, value)?;
        self.reserve(
            key.len()
                .checked_add(value.len())
                .and_then(|n| n.checked_add(derived))
                .ok_or_else(|| Error::Other("configuration copy overflow".into()))?,
        )
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn plan_write_set(
        &self,
        ws: &WriteSet,
        sink_events: &[PreparedSinkEvent],
        trigger_audits: &[(u64, TriggerAuditEntry)],
    ) -> Result<()> {
        // Concrete per-branch directories coexist with their input bodies.
        let rows = ws
            .relational_inserts
            .len()
            .checked_add(ws.relational_deletes.len())
            .ok_or_else(|| Error::Other("commit row count overflow".into()))?;
        self.tree::<&str, Vec<&VersionedRow>>(rows)?;
        self.array::<Option<(Vec<u8>, TxId, u64)>>(ws.adj_deletes.len())?;
        self.array::<&DdlChange>(
            sink_events
                .len()
                .checked_add(trigger_audits.len())
                .ok_or_else(|| Error::Other("commit event count overflow".into()))?,
        )?;
        self.tree::<&str, Vec<(usize, &SinkQueueEntry)>>(sink_events.len())?;
        self.array::<(usize, &SinkQueueEntry)>(
            sink_events
                .len()
                .checked_mul(4)
                .ok_or_else(|| Error::Other("sink event directory overflow".into()))?,
        )?;
        for (table, row) in &ws.relational_inserts {
            self.reserve(table.len())?;
            let _json_scratch = self.json_scratch(row.values.values())?;
            self.record(row)?;
        }
        for (table, _, _) in &ws.relational_deletes {
            self.reserve(
                table
                    .len()
                    .checked_mul(4)
                    .ok_or_else(|| Error::Other("row delete key overflow".into()))?,
            )?;
        }
        for edge in &ws.adj_inserts {
            let _json_scratch = self.json_scratch(edge.properties.values())?;
            self.record(edge)?;
        }
        for (_, kind, _, _) in &ws.adj_deletes {
            self.reserve(
                kind.len()
                    .checked_mul(4)
                    .and_then(|n| n.checked_add(256))
                    .ok_or_else(|| Error::Other("edge delete key overflow".into()))?,
            )?;
        }
        for (sink, entry) in sink_events {
            self.reserve(sink.len())?;
            self.record(entry)?;
        }
        for (_, audit) in trigger_audits {
            self.record(audit)?;
        }
        for (key, value) in &ws.config_writes {
            self.plan_config(key, value)?;
        }
        Ok(())
    }
}
/// Arm (or disarm with `None`) a refusal at one named identity-mutation site
/// for the calling thread.
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn refuse_identity_growth_at_for_test(site: Option<&'static str>) {
    REFUSED_IDENTITY_GROWTH_SITE.with(|armed| armed.set(site));
}
impl WriterMutationVector {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_capacity(capacity: usize) -> Result<Self> {
        let owner = crate::read_image_memory::active_owner()?;
        owner.reserve(
            std::alloc::Layout::array::<content_identity::ContentIdentityMutation>(capacity)
                .map_err(|_| Error::Other("logical identity mutation layout overflow".into()))?
                .size(),
        )?;
        Ok(Self {
            values: Vec::with_capacity(capacity),
            owner,
        })
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn push(&mut self, value: content_identity::ContentIdentityMutation) -> Result<()> {
        if self.values.len() == self.values.capacity() {
            let next = self
                .values
                .capacity()
                .checked_add((self.values.capacity() / 8).max(4))
                .ok_or_else(|| {
                    Error::Other("logical identity mutation capacity overflow".into())
                })?;
            self.owner.reserve(
                std::alloc::Layout::array::<content_identity::ContentIdentityMutation>(next)
                    .map_err(|_| Error::Other("logical identity mutation layout overflow".into()))?
                    .size(),
            )?;
            let mut successor = Vec::with_capacity(next);
            successor.append(&mut self.values);
            let old = std::mem::replace(&mut self.values, successor);
            let old_bytes =
                old.capacity() * std::mem::size_of::<content_identity::ContentIdentityMutation>();
            drop(old);
            self.owner.release(old_bytes);
        }
        self.values.push(value);
        Ok(())
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn extend(
        &mut self,
        values: impl IntoIterator<Item = content_identity::ContentIdentityMutation>,
    ) -> Result<()> {
        for value in values {
            self.push(value)?;
        }
        Ok(())
    }
}
impl std::ops::Deref for WriterMutationVector {
    type Target = Vec<content_identity::ContentIdentityMutation>;
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}
impl<'f, 'o> OriginalMutationVector<'f, 'o> {
    pub(super) fn new(frame: &'f OriginalPersistenceFrame<'o>) -> Self {
        Self {
            values: Vec::new(),
            frame,
            funded_capacity: 0,
        }
    }
    pub(super) fn from_vec(
        frame: &'f OriginalPersistenceFrame<'o>,
        values: Vec<content_identity::ContentIdentityMutation>,
    ) -> Self {
        Self {
            values,
            frame,
            funded_capacity: 0,
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn push(&mut self, value: content_identity::ContentIdentityMutation) -> Result<()> {
        if self.values.len() == self.values.capacity() {
            let next = self
                .values
                .capacity()
                .checked_mul(2)
                .map(|n| n.max(4))
                .ok_or_else(|| {
                    Error::Other("logical identity mutation capacity overflow".into())
                })?;
            self.frame
                .array::<content_identity::ContentIdentityMutation>(next)?;
            let mut successor = Vec::with_capacity(next);
            successor.append(&mut self.values);
            let prior = std::mem::replace(&mut self.values, successor);
            drop(prior);
            self.frame.work.borrow_mut().try_shrink(
                self.funded_capacity
                    * std::mem::size_of::<content_identity::ContentIdentityMutation>(),
            )?;
            self.funded_capacity = next;
        }
        self.values.push(value);
        Ok(())
    }
    /// `push` at a named preparation site. A test may arm one site so the
    /// growth admission refuses there exactly as an exhausted MEMORY_LIMIT does.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn push_at(
        &mut self,
        _site: &'static str,
        value: content_identity::ContentIdentityMutation,
    ) -> Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        if self.values.len() == self.values.capacity()
            && REFUSED_IDENTITY_GROWTH_SITE.with(|armed| armed.get() == Some(_site))
        {
            return Err(Error::MemoryBudgetExceeded {
                subsystem: "persistence".into(),
                operation: "prepare_commit".into(),
                requested_bytes: std::mem::size_of::<content_identity::ContentIdentityMutation>()
                    * self.values.capacity().saturating_mul(2).max(4),
                available_bytes: 0,
                budget_limit_bytes: 0,
                hint: "Raise MEMORY_LIMIT to admit commit preparation.".into(),
            });
        }
        self.push(value)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn extend(
        &mut self,
        values: impl IntoIterator<Item = content_identity::ContentIdentityMutation>,
    ) -> Result<()> {
        for value in values {
            self.push(value)?;
        }
        Ok(())
    }
    pub(super) fn into_vec(self) -> Vec<content_identity::ContentIdentityMutation> {
        self.values
    }
}
impl std::ops::Deref for OriginalMutationVector<'_, '_> {
    type Target = Vec<content_identity::ContentIdentityMutation>;
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}
impl IntoIterator for OriginalMutationVector<'_, '_> {
    type Item = content_identity::ContentIdentityMutation;
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}
impl OriginalWindowMemory {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn new(original: &crate::memory_accounting::OwnedMemoryReservation) -> Result<Self> {
        Ok(Self(std::cell::RefCell::new(
            crate::memory_accounting::reserve_preparation_from_original(
                original,
                0,
                "writer_source",
                "graph_delete_window",
                "Raise MEMORY_LIMIT to decode the selected edge version.",
            )?,
        )))
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reserve(&self, bytes: usize) -> Result<()> {
        crate::memory_accounting::grow_preparation_from_original(
            &mut self.0.borrow_mut(),
            bytes,
            "writer_source",
            "graph_delete_window",
            "Raise MEMORY_LIMIT to decode the selected edge version.",
        )
    }
    pub(super) fn scope<T>(&self, f: impl FnOnce() -> T) -> T {
        f()
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn original_schema_config_values<'a>(
    non_event: &'a [(String, Vec<u8>)],
    events: Option<&'a [(String, Vec<u8>)]>,
    cut: usize,
) -> Result<impl Iterator<Item = &'a (String, Vec<u8>)> + Clone> {
    if cut > non_event.len() {
        return Err(Error::Other(
            "native schema config insertion point exceeds projection".into(),
        ));
    }
    Ok(non_event[..cut]
        .iter()
        .chain(events.into_iter().flatten())
        .chain(non_event[cut..].iter()))
}

pub(crate) fn original_schema_event_config_position(non_event: &[(String, Vec<u8>)]) -> usize {
    // These are the existing private wire keys, not a consumer policy list.
    // The original builder appends the event config immediately before its
    // optional trigger projection; source-order and duplicate-key precedence
    // are therefore preserved without regrouping either projection.
    non_event
        .iter()
        .position(|(key, _)| key == "__trigger_declarations" || key == "__trigger_including_sync")
        .unwrap_or(non_event.len())
}

impl RedbPersistence {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn encode_into_original<T: serde::Serialize>(
        frame: &OriginalPersistenceFrame<'_>,
        value: &T,
        encoded: &mut Vec<u8>,
    ) -> Result<()> {
        let next = frame.encode(value)?;
        let old = std::mem::replace(encoded, next);
        drop(old);
        Ok(())
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn reserve_owned_writer_snapshot(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
    ) -> Result<crate::memory_accounting::OwnedMemoryReservation> {
        self.prepare_proof_owner()?;
        let _schema = self.prepared_writer_schema()?;
        crate::memory_accounting::reserve_preparation_from_original(
            original,
            writer_source::native_snapshot_control_bytes()?
                .checked_add(
                    contextdb_tx::staged_source::source_arc_layout::<redb::ReadTransaction>()
                        .size(),
                )
                .and_then(|bytes| {
                    bytes.checked_add(
                        self.writer_snapshots
                            .lock()
                            .len()
                            .checked_add(1)?
                            .checked_mul(std::mem::size_of::<WriterSnapshotWeak>())?,
                    )
                })
                .ok_or_else(|| Error::Other("writer publication control size overflow".into()))?,
            "writer_source",
            "published_root",
            "Raise MEMORY_LIMIT to admit the published writer root.",
        )
    }
}
impl OwnedEncodedEventSource<'_> {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn decode_owned_sink_queue(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        sink: &str,
    ) -> Result<OriginalLoadedSinkQueue> {
        let mut count = 0usize;
        self.visit(sink, |_, _| {
            count = count
                .checked_add(1)
                .ok_or_else(|| Error::Other("sink queue startup count overflow".into()))?;
            Ok(())
        })?;
        let (entries, allocation) = crate::memory_accounting::new_owned_native_vec::<(
            u64,
            SinkQueueEntry,
        )>(original, count)?;
        let mut loaded = OriginalLoadedSinkQueue {
            entries,
            _allocation: allocation,
        };
        self.visit(sink, |id, bytes| {
            let mut entry = original_event_decode::decode_sink_entry(original, bytes)?;
            crate::memory_accounting::merge_preparation_original(
                &mut loaded._allocation,
                &mut entry.allocation,
            )?;
            loaded.entries.push((id, entry.value));
            Ok(())
        })?;
        if loaded.entries.len() != count {
            return Err(Error::Other(
                "captured sink queue changed between count and decode".into(),
            ));
        }
        Ok(loaded)
    }
}

impl RedbPersistence {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn flush_owned_data_with_logs_and_sink_events(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        ws: &WriteSet,
        change_log: &[ChangeLogEntry],
        options: FlushDataOptions<'_>,
        schema_config: Option<&[(String, Vec<u8>)]>,
        config_cut: usize,
    ) -> Result<()> {
        let storage_scope =
            native_storage_admission::StorageAdmissionScope::writer_original(original)?;
        storage_scope.run_original_unqualified(|| {
        let frame = OriginalPersistenceFrame::new(original)?;
        frame.plan_write_set(ws, options.sink_events, options.trigger_audits)?;

        let deleted_rows_snapshot = options.snapshots.deleted_rows;
        let sink_events = options.sink_events;
        let trigger_audits = options.trigger_audits;
        let schema_ddl = options.schema_ddl;
        let max_sink_queue_depth = options.max_sink_queue_depth;
        let partition_mutations = options.partition_mutations;
        if let Some(received_schema) = options.received_schema {
            return self.flush_owned_received_schema_stage(
                original,
                ws,
                change_log,
                received_schema,
                sink_events,
                trigger_audits,
                max_sink_queue_depth,
                schema_config,
                config_cut,
            );
        }
        let loaded_table_meta;
        let table_meta = match options.snapshots.table_meta {
            Some(table_meta) => table_meta,
            None => {
                loaded_table_meta = self.load_all_table_meta()?;
                &loaded_table_meta
            }
        };
        let write_set_has_vector_changes = !ws.vector_deletes.is_empty()
            || !ws.vector_inserts.is_empty()
            || !ws.vector_moves.is_empty();
        if write_set_has_vector_changes
            && partition_mutations.is_none_or(PreparedVectorPartitionMutationBatch::is_empty)
        {
            return Err(Error::Other((
                "vector persistence requires a prepared partition mutation batch".to_string()).into()));
        }
        let has_vector_changes = write_set_has_vector_changes
            || partition_mutations.is_some_and(|batch| !batch.is_empty());
        let vector_quantization = if has_vector_changes {
            Self::admitted_vector_quantization_map(table_meta, |bytes| frame.reserve(bytes))?
        } else {
            HashMap::new()
        };
        // Runtime rows deliberately retain no vector body, but the durable
        // F32 row value is the independent half of the direct-reader
        // divergence proof. Rejoin the staged vector with the exact row
        // occurrence while both are still inside this commit; quantized
        // columns continue to persist only their vector-store body.
        let f32_row_vectors =
            Self::f32_witness_index(ws, partition_mutations, &vector_quantization, |bytes| {
                frame.reserve(bytes)
            })?;
        let prepared_local_erasure_vector_generations = options
            .local_erasure
            .as_ref()
            .map(|erasure| self.prepare_authoritative_purge_vector_generations(erasure.projection))
            .transpose()?
            .unwrap_or_default();
        for (key, value) in schema_ddl
            .event_bus
            .into_iter()
            .flat_map(|ddl| ddl.config_values.iter())
            .chain(
                schema_ddl
                    .trigger
                    .into_iter()
                    .flat_map(|ddl| ddl.config_values.iter()),
            )
        {
            frame.plan_config(key, value)?;
        }
        let group_plan = custody_groups::GroupPlan::prepaid(
            ws.config_writes
                .iter()
                .chain(
                    schema_ddl
                        .event_bus
                        .into_iter()
                        .flat_map(|ddl| ddl.config_values.iter()),
                )
                .chain(
                    schema_ddl
                        .trigger
                        .into_iter()
                        .flat_map(|ddl| ddl.config_values.iter()),
                ),
        )?;
        self.with_db(|db| {
            let prepared_discard = crate::database::discard::take_original_discard_write(self, original, ws)?;
            let has_discard_history = prepared_discard.is_some();
            if has_discard_history && options.local_erasure.is_none() {
                return Err(Error::Other("discard history has no matching local erasure projection".into()));
            }
            let write_txn = match prepared_discard {
                Some(write) => discard_history::TerminalWrite::Prepared(write),
                None => discard_history::TerminalWrite::Fresh(Self::begin_history_write(db)?),
            };
#[cfg(any(test, feature = "test-seams"))]
let mut graph_publication_attempt = crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let graph_commit_support=Self::capture_graph_commit_support(&write_txn,ws.commit_lsn)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut stage_started = std::time::Instant::now();
            let mut pending_manifests = original_relational_manifest::OriginalPendingManifestPublication::new(content_identity::original_writer::ProofAdmission::new(frame.original,self.proof_scratch_owner())?);
            // Declared first, so dropped last: these charges outlive the mutations they pay for.
            frame.array::<historical_index_commitment::CommitmentCredit>(1)?;
            let mut identity_mutations_commitment_credits: Vec<historical_index_commitment::CommitmentCredit> = Vec::with_capacity(1);
            let mut identity_mutations = OriginalMutationVector::new(&frame);
            for (table, row_id, _) in &ws.relational_deletes {
                identity_mutations.push(content_identity::original_producers::remove_row_mutation(&frame,table, *row_id)?)?;
            }
            for (table, row) in &ws.relational_inserts {
                identity_mutations.push(content_identity::original_producers::row_mutation(&frame,
                    table,
                    row,
                    table_meta.get(table),
                )?)?;
            }
            for (source, edge_type, target, _) in &ws.adj_deletes {
                identity_mutations.push(content_identity::original_producers::remove_edge_mutation(&frame,
                    *source, edge_type, *target,
                )?)?;
            }
            for edge in &ws.adj_inserts {
                identity_mutations.push(content_identity::original_producers::edge_mutation(&frame,edge)?)?;
            }
            if let Some(batch) = partition_mutations {
                for mutation in &batch.mutations {
                    match mutation {
                        PreparedVectorPartitionMutation::Delete { index, before, .. } => {
                            let quantization = vector_quantization
                                .get(index)
                                .copied()
                                .unwrap_or_default();
                            identity_mutations.push(content_identity::original_producers::remove_vector_mutation(&frame,
                                index,
                                before.row.row_id,
                                quantization,
                            )?)?;
                        }
                        PreparedVectorPartitionMutation::Insert { index, after, entry } => {
                            let entry = batch.resolve_insert(ws, *entry, index, after)?;
                            let quantization = vector_quantization
                                .get(index)
                                .copied()
                                .unwrap_or_default();
                            let [current, historical] = content_identity::original_producers::vector_mutation_pair(&frame, entry, quantization)?;
                            identity_mutations.push_at("vector_insert", current)?;
                            identity_mutations.push(historical)?;
                        }
                        PreparedVectorPartitionMutation::Move {
                            index,
                            before,
                            after,
                            moved_tx,
                        } => {
                            let quantization = vector_quantization
                                .get(index)
                                .copied()
                                .unwrap_or_default();
                            identity_mutations.push(content_identity::original_producers::remove_vector_mutation(&frame,
                                index,
                                before.row.row_id,
                                quantization,
                            )?)?;
                            let source_frame = OriginalPersistenceFrame::new(frame.original)?;
                            source_frame.reserve(index.table.len().saturating_add(index.column.len()).saturating_mul(2).saturating_add(32))?;
                            let mut moved = VectorEntry {
                                index: index.clone(),
                                row_id: after.row.row_id,
                                vector: Vec::new(),
                                created_tx: *moved_tx,
                                deleted_tx: None,
                                lsn: batch.commit_lsn,
                            };
                            if let PreparedVectorPartitionMutation::Move { before, .. } = mutation {
                                let vectors = content_size::open_vectors(&write_txn)
                                    .map_err(Self::storage_error)?;
                                let source_key = Self::vector_identity_key(
                                    index,
                                    before.row.row_id,
                                    before.vector_created_tx,
                                    before.vector_lsn,
                                );
                                let source = vectors
                                    .get(source_key.as_slice())
                                    .map_err(Self::storage_error)?
                                    .ok_or_else(|| {
                                        self.vector_partition_corruption(
                                            "a prepared move has no durable source vector",
                                        )
                                    })?;
                                moved.vector = Self::decode_vector_entry_original(&source_frame,source.value())?.vector;
                            }
                            let [current, historical] = content_identity::original_producers::vector_mutation_pair(&frame, &moved, quantization)?;
                            identity_mutations.push_at("vector_move", current)?;
                            identity_mutations.push(historical)?;
                        }
                    }
                }
            }

            // Exact durable F32 sources are held as identities and decoded row by row.

            // Postings and the row versions they project are committed to
            // separately and must agree: see `historical_index_commitment`.
            let mut commitments = historical_index_commitment::CommitmentBatch::for_original(frame.original)?;
            let mut relational_deletes_by_table = BTreeMap::<&str, Vec<(RowId, TxId)>>::new();
            for (table, row_id, deleted_tx) in &ws.relational_deletes {
                relational_deletes_by_table
                    .entry(table.as_str())
                    .or_default()
                    .push((*row_id, *deleted_tx));
            }
            for (table, deletes) in relational_deletes_by_table {
                let table_name = Self::rel_table_name(table);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut redb_table =
                    content_size::open_rows(&write_txn, table_def, table_meta.get(table))
                        .map_err(Self::storage_error)?;
                for (row_id, deleted_tx) in deletes {
                    let frame = OriginalPersistenceFrame::new(frame.original)?;
                    let mut encoded = Vec::new();
                    self.verify_relational_group_pending_in_write_original(frame.original,&write_txn, table, row_id, &*redb_table, &pending_manifests.mutations)?;
                    let _row_scope = crate::read_image_memory::active_owner()?.row_owner()?.enter();
                    if let Some(row) = deleted_rows_snapshot
                        .as_ref()
                        .and_then(|by_table| by_table.get(table))
                        .and_then(|by_row| by_row.get(&row_id))
                    {
                        if row.deleted_tx.is_some() {
                            continue;
                        }
                        let key = Self::rel_row_key(row);
                        let mut overlay = redb_table
                            .get(key.as_slice())
                            .map_err(Self::storage_error)?
                            .map(|stored| {
                                Self::decode_versioned_row_original(&frame,stored.value(), table_meta.get(table))
                            })
                            .transpose()?
                            .map(|stored| {
                                Self::f32_vectors_from_row_original(&frame,&stored, table_meta.get(table))
                            }).transpose()?.unwrap_or_default();
                        self.overlay_f32_witnesses(&write_txn, &f32_row_vectors, table, row, &mut overlay, |bytes| frame.reserve(bytes), |bytes| Self::decode_vector_entry_original(&frame, bytes))?;
                        Self::encode_versioned_row_with_f32_overlay_into_original(&frame,
                            row,
                            Some(deleted_tx),
                            table_meta.get(table),
                            Some(&overlay),
                            &mut encoded,
                        )?;
                        {
                            let predecessor = redb_table.get(key.as_slice()).map_err(Self::storage_error)?;
                            self.replace_relational_manifest_pending_in_write_original(frame.original,&write_txn, table, key.as_slice(), predecessor.as_ref().map(|body| body.value()), Some(encoded.as_slice()), Some(&mut pending_manifests))?;
                        }
                        let keys = content_size::VersionLife::keys_of(redb_table.indexes(), row)?;
                        redb_table.insert_version(
                            &mut commitments,
                            key.as_slice(),
                            encoded.as_slice(),
                            &content_size::VersionLife {
                                keys: &keys,
                                created: row.created_tx,
                                deleted: Some(deleted_tx),
                            },
                        )?;
                        continue;
                    }
                    let (lower, upper) = Self::rel_row_key_range(row_id);
                    let latest_version = {
                        let mut range = redb_table
                            .range(lower.as_slice()..upper.as_slice())
                            .map_err(Self::storage_error)?;
                        range
                            .next_back()
                            .transpose()
                            .map_err(Self::storage_error)?
                            .map(|(key, value)| {
                                let row = Self::decode_versioned_row_original(&frame,
                                    value.value(),
                                    table_meta.get(table),
                                )?;
                                Ok::<_, Error>((key.value().to_vec(), row))
                            })
                            .transpose()?
                    };
                    let Some((key, mut row)) = latest_version else {
                        continue;
                    };
                    if row.row_id != row_id || row.deleted_tx.is_some() {
                        continue;
                    }
                    row.deleted_tx = Some(deleted_tx);
                    Self::encode_versioned_row_into_original(&frame,&row, table_meta.get(table), &mut encoded)?;
                    {
                        let predecessor = redb_table.get(key.as_slice()).map_err(Self::storage_error)?;
                        self.replace_relational_manifest_pending_in_write_original(frame.original,&write_txn, table, key.as_slice(), predecessor.as_ref().map(|body| body.value()), Some(encoded.as_slice()), Some(&mut pending_manifests))?;
                    }
                    let keys = content_size::VersionLife::keys_of(redb_table.indexes(), &row)?;
                    redb_table.insert_version(
                        &mut commitments,
                        key.as_slice(),
                        encoded.as_slice(),
                        &content_size::VersionLife {
                            keys: &keys,
                            created: row.created_tx,
                            deleted: Some(deleted_tx),
                        },
                    )?;
                }
            }

            let _replacement_lookup_charge = Some(crate::memory_accounting::persistence_original_projection(frame.original)?.as_handle()).map(|_accountant| {
                crate::memory_accounting::reserve_preparation_from_original(frame.original, ws.relational_deletes.len().saturating_mul(128).saturating_add(64),
                    "writer_source", "replacement_lookup", "Raise MEMORY_LIMIT to prepare row replacement identities.",
                )
            }).transpose()?;
            let replaced_rows = ws.relational_deletes.iter()
                .map(|(table, row_id, _)| (table.as_str(), *row_id))
                .collect::<HashSet<_>>();
            let mut relational_inserts_by_table = BTreeMap::<&str, Vec<&VersionedRow>>::new();
            for (table, row) in &ws.relational_inserts {
                relational_inserts_by_table
                    .entry(table.as_str())
                    .or_default()
                    .push(row);
            }
            for (table, rows) in relational_inserts_by_table {
                let table_name = Self::rel_table_name(table);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut redb_table =
                    content_size::open_rows(&write_txn, table_def, table_meta.get(table))
                        .map_err(Self::storage_error)?;
                for row in rows {
                    let frame = OriginalPersistenceFrame::new(frame.original)?;
                    let _row_scope = crate::read_image_memory::active_owner()?.row_owner()?.enter();
                    let mut encoded = Vec::new();
                    // An ordinary update reopens with body-free runtime rows
                    // and need not stage an unchanged vector again. Carry the
                    // independent F32 witness from the exact prior row id into
                    // its successor; explicitly staged vectors win when the
                    // update did change a vector.
                    let replaces_same_row = replaced_rows.contains(&(table, row.row_id));
                    let mut overlay = if replaces_same_row {
                        let (lower, upper) = Self::rel_row_key_range(row.row_id);
                        let mut range = redb_table
                            .range(lower.as_slice()..upper.as_slice())
                            .map_err(Self::storage_error)?;
                        range
                            .next_back()
                            .transpose()
                            .map_err(Self::storage_error)?
                            .map(|(_, stored)| {
                                Self::decode_versioned_row_original(&frame,
                                    stored.value(),
                                    table_meta.get(table),
                                )
                            })
                            .transpose()?
                            .map(|stored| {
                                Self::f32_vectors_from_row_original(&frame,&stored, table_meta.get(table))
                            }).transpose()?.unwrap_or_default()
                    } else {
                        HashMap::new()
                    };
                    // A clear removes the prior body's witness from this
                    // successor. A replacement may stage a new body below.
                    overlay.retain(|column, _| {
                        !ws.vector_deletes.iter().any(|(index, deleted_row, _)| {
                            index.table == table
                                && *deleted_row == row.row_id
                                && index.column == *column
                        })
                    });
                    self.overlay_f32_witnesses(&write_txn, &f32_row_vectors, table, row, &mut overlay, |bytes| frame.reserve(bytes), |bytes| Self::decode_vector_entry_original(&frame, bytes))?;
                    Self::encode_versioned_row_with_f32_overlay_into_original(&frame,
                        row,
                        row.deleted_tx,
                        table_meta.get(table),
                        Some(&overlay),
                        &mut encoded,
                    )?;
                    let key = Self::rel_row_key(row);
                    {
                        let predecessor = redb_table.get(key.as_slice()).map_err(Self::storage_error)?;
                        self.replace_relational_manifest_pending_in_write_original(frame.original,&write_txn, table, key.as_slice(), predecessor.as_ref().map(|body| body.value()), Some(encoded.as_slice()), Some(&mut pending_manifests))?;
                    }
                    let keys = content_size::VersionLife::keys_of(redb_table.indexes(), row)?;
                    redb_table.insert_version(
                        &mut commitments,
                        key.as_slice(),
                        encoded.as_slice(),
                        &content_size::VersionLife {
                            keys: &keys,
                            created: row.created_tx,
                            deleted: row.deleted_tx,
                        },
                    )?;
                }
            }

            identity_mutations.extend(Self::flush_relational_index_changes_in_write_original(&frame,
                &write_txn,
                ws,
                table_meta,
                deleted_rows_snapshot.as_ref(),
                &mut commitments,
            )?)?;
            let (published, credit) = commitments.finish(&write_txn)?.into_parts();
            identity_mutations_commitment_credits.push(credit);
            identity_mutations.extend(published)?;

            let (clear_source_lsns, set_source_lsns) = sync_source_lsn_updates(ws);
            if !clear_source_lsns.is_empty() || !set_source_lsns.is_empty() {
                let mut source_lsn_table = write_txn
                    .open_table(SYNC_ROW_SOURCE_LSN_TABLE)
                    .map_err(Self::storage_error)?;
                let mut source_kind_table = write_txn
                    .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
                    .map_err(Self::storage_error)?;
                for (table, row_id) in clear_source_lsns {
                    let key = Self::sync_row_source_lsn_key(&table, row_id);
                    source_lsn_table
                        .remove(key.as_slice())
                        .map_err(Self::storage_error)?;
                    source_kind_table
                        .remove(key.as_slice())
                        .map_err(Self::storage_error)?;
                    identity_mutations.push(content_identity::original_producers::remove(&frame,
                        content_identity::LogicalFamily::Sync,
                        key,
                    )?)?;
                }
                for (table, row_id, source_lsn, kind) in set_source_lsns {
                    let key = Self::sync_row_source_lsn_key(&table, row_id);
                    source_lsn_table
                        .insert(key.as_slice(), source_lsn.0)
                        .map_err(Self::storage_error)?;
                    source_kind_table
                        .insert(key.as_slice(), kind)
                        .map_err(Self::storage_error)?;
                    let mut value = source_lsn.0.to_le_bytes().to_vec();
                    value.push(kind);
                    identity_mutations.push(content_identity::original_producers::put(&frame,
                        content_identity::LogicalFamily::Sync,
                        key,
                        value,
                    )?)?;
                }
            }

            // Retain only this write set's actually tombstoned predecessors,
            // independently of older imported tombstones or retired receipts.
            let selected_deletion_memory = OriginalWindowMemory::new(frame.original)?;
            type SelectedDeletion = Option<(Vec<u8>, TxId, (Lsn, TxId))>;
            selected_deletion_memory.reserve(
                ws.adj_deletes.len().saturating_mul(std::mem::size_of::<SelectedDeletion>()),
            )?;
            let mut selected_deletions: Vec<SelectedDeletion> =
                (0..ws.adj_deletes.len()).map(|_| None).collect();
            if !ws.adj_deletes.is_empty() || !ws.adj_inserts.is_empty() {
                let mut fwd_table = write_txn
                    .open_table(GRAPH_FWD_TABLE)
                    .map_err(Self::storage_error)?;
                let mut rev_table = write_txn
                    .open_table(GRAPH_REV_TABLE)
                    .map_err(Self::storage_error)?;

                let mut encoded = Vec::new();
                for (delete_index, (source, edge_type, target, deleted_tx)) in
                    ws.adj_deletes.iter().enumerate()
                {
                    let mut key_charge = Some(crate::memory_accounting::persistence_original_projection(frame.original)?.as_handle()).map(|_accountant| {
                        crate::memory_accounting::reserve_preparation_from_original(frame.original, (33 + edge_type.len()).saturating_mul(2), "writer_source",
                            "graph_delete_keys", "Raise MEMORY_LIMIT to prepare the selected edge identity.",
                        )
                    }).transpose()?;
                    let mut prefix = Vec::with_capacity(33 + edge_type.len());
                    prefix.extend_from_slice(source.as_bytes());
                    prefix.extend_from_slice(target.as_bytes());
                    prefix.extend_from_slice(edge_type.as_bytes());
                    prefix.push(0);
                    let upper = writer_source::prefix_end(&prefix);
                    let mut after = None::<Vec<u8>>;
                    let mut found = false;
                    loop {
                        let lower = after.as_deref().map_or(
                            std::ops::Bound::Included(prefix.as_slice()),
                            std::ops::Bound::Excluded,
                        );
                        let upper = upper.as_deref().map_or(
                            std::ops::Bound::Unbounded,
                            std::ops::Bound::Excluded,
                        );
                        // End the range borrow before changing either direction.
                        // Only this version's key/body is live in scratch.
                        let memory = Some(OriginalWindowMemory::new(frame.original)?);
                        let next = {
                            let mut range = fwd_table.range::<&[u8]>((lower, upper))
                                .map_err(Self::storage_error)?.inspect(|_entry| { #[cfg(any(test, feature = "test-seams"))] if let Ok((key, _)) = _entry { crate::read_work_probe::graph_storage_key("graph_fwd", "iteration_step", key.value()); } });
                            range.next().transpose().map_err(Self::storage_error)?
                                .map(|(key, value)| {
                                    if let Some(memory) = &memory {
                                        memory.reserve(key.value().len().saturating_mul(2)
                                            .saturating_add(value.value().len().saturating_mul(2)).saturating_add(64))?;
                                    }
                                    #[cfg(any(test, feature = "test-seams"))]
                                    crate::writer_query_test_seams::note_graph_delete_decode();
                                    #[cfg(any(test, feature = "test-seams"))]
                                    crate::read_work_probe::note_graph("graph_payload", "payload_decode", key.value(), None, Some("ordinary_deletion"));
                                    let edge: AdjEntry = match &memory {
                                        Some(memory) => memory.scope(|| frame.decode(value.value())),
                                        None => frame.decode(value.value()),
                                    }?;
                                    Ok::<_, Error>((key.value().to_vec(), edge))
                                }).transpose()?
                        };
                        let Some((key, mut edge)) = next else { break; };
                        if edge.source != *source || edge.target != *target
                            || edge.edge_type != *edge_type {
                            return Err(crate::funded_error::corruption_text(crate::funded_error::original_budget(original), self.path.display(), "graph posting is outside its edge identity".into()));
                        }
                        if edge.deleted_tx.is_none() {
                            found = true;
                            let order = (edge.lsn, edge.created_tx);
                            let selected = &mut selected_deletions[delete_index];
                            if selected.as_ref().is_none_or(|(_, _, prior)| order > *prior) {
                                if let Some((p, d, prior)) = selected {
                                    if p.len() != key.len() {
                                        return Err(crate::funded_error::corruption_text(crate::funded_error::original_budget(original), self.path.display(), "selected graph predecessor width changed".into()));
                                    }
                                    p.copy_from_slice(&key);
                                    *d = *deleted_tx;
                                    *prior = order;
                                } else {
                                    selected_deletion_memory.reserve(key.len())?;
                                    *selected = Some((key.clone(), *deleted_tx, order));
                                }
                            }
                            edge.deleted_tx = Some(*deleted_tx);
                            let _json_scratch = frame.json_scratch(edge.properties.values())?;
                            let mut encoded = Vec::new();
                            Self::encode_into_original(&frame,&edge, &mut encoded)?;
                            let rev_key = Self::graph_rev_key(&edge);
                            fwd_table.insert(observed_graph_key!("graph_fwd", "insert", key.as_slice(), 5), encoded.as_slice())
                                .map_err(Self::storage_error)?;
                            rev_table.insert(observed_graph_key!("graph_rev", "insert", rev_key.as_slice(), 5), encoded.as_slice())
                                .map_err(Self::storage_error)?;
                            self.graph_writer_window(||Self::install_graph_creation_in_write(&write_txn, &edge, &encoded))?;
                        }
                        if let Some(charge) = &mut key_charge {
                            crate::memory_accounting::grow_preparation_from_original(charge,key.capacity(), "writer_source", "graph_delete_keys",
                                "Raise MEMORY_LIMIT to retain the selected edge resume key.")?;
                            if let Some(prior) = after.take() { let bytes=prior.capacity();drop(prior);crate::memory_accounting::release_preparation_original(charge,bytes); }
                        }
                        after = Some(key);
                    }
                    if !found {
                        return Err(Error::NotFound(format!(
                            "edge {source} -[{edge_type}]-> {target} in graph_fwd"
                        )));
                    }
                }

                for entry in &ws.adj_inserts {
                    let _json_scratch = frame.json_scratch(entry.properties.values())?;
                    Self::encode_into_original(&frame,entry, &mut encoded)?;
                    let fwd_key = Self::graph_fwd_key(entry);
                    let rev_key = Self::graph_rev_key(entry);
                    fwd_table
                        .insert(observed_graph_key!("graph_fwd", "insert", fwd_key.as_slice(), 5), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    self.graph_writer_window(||Self::install_graph_creation_in_write(&write_txn, entry, &encoded))?;
                    rev_table
                        .insert(observed_graph_key!("graph_rev", "insert", rev_key.as_slice(), 5), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            self.graph_writer_window(|| Self::apply_row_graph_in_write(&write_txn, ws))?;

            if has_vector_changes {
                let mut vectors_table = content_size::open_vectors(&write_txn)
                    .map_err(Self::storage_error)?;

                if let Some(batch) = partition_mutations {
                    for mutation in &batch.mutations {
                        let PreparedVectorPartitionMutation::Delete {
                            index,
                            before,
                            deleted_tx,
                        } = mutation
                        else {
                            continue;
                        };
                        let frame = OriginalPersistenceFrame::new(frame.original)?;
                        let key = Self::vector_identity_key(
                            index,
                            before.row.row_id,
                            before.vector_created_tx,
                            before.vector_lsn,
                        );
                        let mut entry = {
                            let stored = vectors_table
                                .get(key.as_slice())
                                .map_err(Self::storage_error)?
                                .ok_or_else(|| {
                                    self.vector_partition_corruption(
                                        "a prepared delete has no durable source vector",
                                    )
                                })?;
                            Self::decode_vector_entry_original(&frame,stored.value())?
                        };
                        if entry.index != *index
                            || entry.row_id != before.row.row_id
                            || entry.created_tx != before.vector_created_tx
                            || entry.lsn != before.vector_lsn
                        {
                            return Err(self.vector_partition_corruption(
                                "a prepared delete source key does not match its vector payload",
                            ));
                        }
                        if let Some(existing_deleted_tx) = entry.deleted_tx
                            && existing_deleted_tx != *deleted_tx
                        {
                            return Err(self.vector_partition_corruption(
                                "a prepared delete source has a conflicting deletion transaction",
                            ));
                        }
                        entry.deleted_tx = Some(*deleted_tx);
                        let quantization = vector_quantization
                            .get(index)
                            .copied()
                            .unwrap_or_default();
                        let encoded = Self::encode_vector_entry_original(&frame,&entry, quantization)?;
                        vectors_table
                            .insert(key.as_slice(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }

                if let Some(batch) = partition_mutations {
                    for mutation in &batch.mutations {
                        let PreparedVectorPartitionMutation::Insert {
                            index,
                            after,
                            entry,
                        } = mutation
                        else {
                            continue;
                        };
                        let frame = OriginalPersistenceFrame::new(frame.original)?;
                        let entry = batch.resolve_insert(ws, *entry, index, after)?;
                        if entry.index != *index
                            || entry.row_id != after.row.row_id
                            || entry.created_tx != after.vector_created_tx
                            || entry.lsn != after.vector_lsn
                            || entry.deleted_tx.is_some()
                        {
                            return Err(Error::Other((format!(
                                "prepared vector insert has inconsistent identity on {}.{}",
                                index.table, index.column
                            )).into()));
                        }
                        let quantization = vector_quantization
                            .get(index)
                            .copied()
                            .unwrap_or_default();
                        let encoded = Self::encode_vector_entry_original(&frame,entry, quantization)?;
                        let key = Self::vector_key(entry);
                        if let Some(existing) = vectors_table
                            .get(key.as_slice())
                            .map_err(Self::storage_error)?
                            && existing.value() != encoded.as_slice()
                        {
                            return Err(self.vector_partition_corruption(
                                "a prepared insert destination has conflicting vector bytes",
                            ));
                        }
                        vectors_table
                            .insert(key.as_slice(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }

                if let Some(batch) = partition_mutations {
                    for mutation in &batch.mutations {
                        let PreparedVectorPartitionMutation::Move {
                            index,
                            before,
                            after,
                            moved_tx,
                        } = mutation
                        else {
                            continue;
                        };
                        let frame = OriginalPersistenceFrame::new(frame.original)?;
                        if after.vector_created_tx != *moved_tx
                            || after.vector_lsn != batch.commit_lsn
                        {
                            return Err(Error::Other((format!(
                                "prepared vector move has inconsistent destination identity on {}.{}",
                                index.table, index.column
                            )).into()));
                        }
                        let old_key = Self::vector_identity_key(
                            index,
                            before.row.row_id,
                            before.vector_created_tx,
                            before.vector_lsn,
                        );
                        let mut old_entry = {
                            let stored = vectors_table
                                .get(old_key.as_slice())
                                .map_err(Self::storage_error)?
                                .ok_or_else(|| {
                                    self.vector_partition_corruption(
                                        "a prepared move has no durable source vector",
                                    )
                                })?;
                            Self::decode_vector_entry_original(&frame,stored.value())?
                        };
                        if old_entry.index != *index
                            || old_entry.row_id != before.row.row_id
                            || old_entry.created_tx != before.vector_created_tx
                            || old_entry.lsn != before.vector_lsn
                        {
                            return Err(self.vector_partition_corruption(
                                "a prepared move source key does not match its vector payload",
                            ));
                        }
                        if let Some(existing_deleted_tx) = old_entry.deleted_tx
                            && existing_deleted_tx != *moved_tx
                        {
                            return Err(self.vector_partition_corruption(
                                "a prepared move source has a conflicting deletion transaction",
                            ));
                        }
                        old_entry.deleted_tx = Some(*moved_tx);
                        let quantization = vector_quantization
                            .get(index)
                            .copied()
                            .unwrap_or_default();
                        let old_encoded = Self::encode_vector_entry_original(&frame,&old_entry, quantization)?;
                        vectors_table
                            .insert(old_key.as_slice(), old_encoded.as_slice())
                            .map_err(Self::storage_error)?;

                        let mut new_entry = old_entry;
                        new_entry.row_id = after.row.row_id;
                        new_entry.created_tx = *moved_tx;
                        new_entry.deleted_tx = None;
                        new_entry.lsn = batch.commit_lsn;
                        let new_key = Self::vector_key(&new_entry);
                        let new_encoded = Self::encode_vector_entry_original(&frame,&new_entry, quantization)?;
                        if let Some(existing) = vectors_table
                            .get(new_key.as_slice())
                            .map_err(Self::storage_error)?
                            && existing.value() != new_encoded.as_slice()
                        {
                            return Err(self.vector_partition_corruption(
                                "a prepared move destination has conflicting vector bytes",
                            ));
                        }
                        vectors_table
                            .insert(new_key.as_slice(), new_encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }
            }

            if let Some(partition_mutations) = partition_mutations {
                self.write_vector_partition_mutations_in_write(
                    &write_txn,
                    partition_mutations,
                )?;
            }

            if let (Some(lsn), Some(tx)) = (ws.commit_lsn, Self::write_set_visibility_tx(ws)) {
                let mut table = write_txn
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                table.insert(lsn.0, tx.0).map_err(Self::storage_error)?;
                identity_mutations.push(content_identity::original_producers::commit_index_mutation(&frame,lsn, tx)?)?;
            }

            Self::stage_graph_commit_support(&write_txn,graph_commit_support)?;
            if !change_log.is_empty() {
                let mut table = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                let mut encoded = Vec::new();
                let mut key = String::with_capacity(Self::change_log_entry_key_len());
                for (index, entry) in change_log.iter().enumerate() {
                    Self::write_change_log_entry_key(lsn, index, &mut key);
                    if let ChangeLogEntry::EdgeDelete { source, target, edge_type, .. } = entry {
                        let selected_index = ws.adj_deletes.iter().position(|(s, k, t, _)|
                            s == source && t == target && k == edge_type
                        ).ok_or_else(|| crate::funded_error::corruption_text(crate::funded_error::original_budget(original), self.path.display(), "ordinary graph deletion has no selected write-set predecessor".into()))?;
                        let (p, d, _) = selected_deletions[selected_index].as_ref()
                            .ok_or_else(|| crate::funded_error::corruption_text(crate::funded_error::original_budget(original), self.path.display(), "ordinary graph deletion selected predecessor is missing".into()))?;
                        self.encode_change_log_with_selected_predecessor_in_write(
                            &write_txn, entry, Some((p.as_slice(), *d)), &mut encoded,
                        )?;
                    } else {
                        self.encode_change_log_into_in_write(&write_txn, entry, &mut encoded)?;
                    }
                    table
                        .insert(observed_graph_key!("history", "insert", key.as_str(), 8), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    self.graph_writer_window(||Self::write_change_log_selectors_in_write(&write_txn, key.as_str(), encoded.as_slice()))?;
                    identity_mutations.push(Self::stored_change_log_mutation_in_write(&write_txn, &key)?)?;
                }
            }

            if !sink_events.is_empty() {
                let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                let mut overflow_audits = Vec::new();
                let mut by_sink: BTreeMap<&str, Vec<(usize, &SinkQueueEntry)>> = BTreeMap::new();
                for (index, (sink, entry)) in sink_events.iter().enumerate() {
                    by_sink
                        .entry(sink.as_str())
                        .or_default()
                        .push((index, entry));
                }
                for (sink, entries) in by_sink {
                    let table_name = Self::sink_queue_table_name(sink);
                    let table_def: TableDefinition<u64, &[u8]> =
                        TableDefinition::new(table_name.as_str());
                    let mut table = write_txn
                        .open_table(table_def)
                        .map_err(Self::storage_error)?;
                    let mut existing = table.len().map_err(Self::storage_error)? as usize;
                    for (index, entry) in entries {
                        if existing >= max_sink_queue_depth {
                            return Err(Error::Other((format!("sink queue is full: {sink}")).into()));
                        }
                        let overflow_id = if existing >= max_sink_queue_depth {
                            table
                                .first()
                                .map_err(Self::storage_error)?
                                .map(|(key, _)| key.value())
                        } else {
                            None
                        };
                        if let Some(id) = overflow_id {
                            table.remove(id).map_err(Self::storage_error)?;
                            existing = existing.saturating_sub(1);
                            overflow_audits.push((index, sink.to_string()));
                            let logical_key = format!("{}:{sink}:{id}", sink.len());
                            identity_mutations.push(content_identity::original_producers::remove_status_mutation(&frame,
                                b"sink-queue",
                                logical_key.as_bytes(),
                            )?)?;
                        }
                        let encoded = frame.encode(entry)?;
                        table
                            .insert(entry.id, encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        let logical_key = format!("{}:{sink}:{}", sink.len(), entry.id);
                        identity_mutations.push(content_identity::original_producers::encoded_status_mutation(&frame,
                            b"sink-queue",
                            logical_key.as_bytes(),
                            entry,
                        )?)?;
                        existing = existing.saturating_add(1);
                    }
                }
                if !overflow_audits.is_empty() {
                    let mut audit_table = write_txn
                        .open_table(SINK_AUDIT_TABLE)
                        .map_err(Self::storage_error)?;
                    for (index, sink) in overflow_audits {
                        let key = Self::sink_audit_key(lsn, index, &sink);
                        let audit = SinkAuditEntry {
                            lsn,
                            kind: SinkAuditKind::QueueOverflow {
                                sink,
                                dropped_count: 1,
                            },
                        };
                        let encoded = frame.encode(&audit)?;
                        audit_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        identity_mutations.push(content_identity::original_producers::status_mutation(&frame,
                            b"sink-audit",
                            key.as_bytes(),
                            &encoded,
                        )?)?;
                    }
                }
            }

            if !trigger_audits.is_empty() {
                {
                    let mut audit_table = write_txn
                        .open_table(TRIGGER_AUDIT_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut stamps = write_txn
                        .open_table(TRIGGER_AUDIT_STAMPS_TABLE)
                        .map_err(Self::storage_error)?;
                    let stamped_at = Wallclock::now().0;
                    for (index, entry) in trigger_audits {
                        let key = Self::trigger_audit_key(*index, &entry.trigger_name);
                        let encoded = frame.encode(entry)?;
                        audit_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        stamps
                            .insert(key.as_str(), stamped_at)
                            .map_err(Self::storage_error)?;
                        identity_mutations.push(content_identity::original_producers::status_mutation(&frame,
                            b"trigger-audit",
                            key.as_bytes(),
                            &encoded,
                        )?)?;
                        identity_mutations.push(content_identity::original_producers::status_mutation(&frame,
                            b"trigger-stamp",
                            key.as_bytes(),
                            &stamped_at.to_le_bytes(),
                        )?)?;
                    }
                }
                {
                    let mut config_table = write_txn
                        .open_table(CONFIG_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut ring: Vec<TriggerAuditEntry> = config_table
                        .get(TRIGGER_AUDIT_RING_CONFIG_KEY)
                        .map_err(Self::storage_error)?
                        .map(|value| frame.decode(value.value()))
                        .transpose()?
                        .unwrap_or_default();
                    frame.array::<TriggerAuditEntry>(ring.len().checked_add(trigger_audits.len()).and_then(|n|n.checked_mul(4)).ok_or_else(||Error::Other("trigger audit ring preparation overflow".into()))?)?;
                    ring.extend(trigger_audits.iter().map(|(_, entry)| entry.clone()));
                    let overflow = ring
                        .len()
                        .saturating_sub(crate::database::trigger::TRIGGER_AUDIT_RING_CAPACITY);
                    if overflow > 0 {
                        ring.drain(0..overflow);
                    }
                    let current_next_index = config_table
                        .get(TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY)
                        .map_err(Self::storage_error)?
                        .map(|value| frame.decode::<u64>(value.value()))
                        .transpose()?
                        .unwrap_or(0);
                    let next_index = trigger_audits
                        .iter()
                        .map(|(index, _)| index.saturating_add(1))
                        .max()
                        .unwrap_or(0)
                        .max(current_next_index);
                    let encoded_ring = frame.encode(&ring)?;
                    config_table
                        .insert(TRIGGER_AUDIT_RING_CONFIG_KEY, encoded_ring.as_slice())
                        .map_err(Self::storage_error)?;
                    identity_mutations.extend(content_identity::original_producers::config_mutations(&frame,
                        TRIGGER_AUDIT_RING_CONFIG_KEY,
                        Some(&encoded_ring),
                    )?)?;
                    let encoded_next_index = frame.encode(&next_index)?;
                    config_table
                        .insert(
                            TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY,
                            encoded_next_index.as_slice(),
                        )
                        .map_err(Self::storage_error)?;
                    identity_mutations.extend(content_identity::original_producers::config_mutations(&frame,
                        TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY,
                        Some(&encoded_next_index),
                    )?)?;
                }
            }

            if schema_ddl.event_bus.is_some() || schema_ddl.trigger.is_some() {
                {
                    let mut config_table = write_txn
                        .open_table(CONFIG_TABLE)
                        .map_err(Self::storage_error)?;
                    if let Some(event_bus_ddl) = schema_ddl.event_bus {
                        for (key, encoded) in &event_bus_ddl.config_values {
                            config_table
                                .insert(key.as_str(), encoded.as_slice())
                                .map_err(Self::storage_error)?;
                            identity_mutations.extend(content_identity::original_producers::config_mutations(&frame,
                                key,
                                Some(encoded),
                            )?)?;
                        }
                    }
                    if let Some(trigger_ddl) = schema_ddl.trigger {
                        for (key, encoded) in &trigger_ddl.config_values {
                            config_table
                                .insert(key.as_str(), encoded.as_slice())
                                .map_err(Self::storage_error)?;
                            identity_mutations.extend(content_identity::original_producers::config_mutations(&frame,
                                key,
                                Some(encoded),
                            )?)?;
                        }
                    }
                }
                frame.array::<&DdlChange>(schema_ddl.event_bus.map_or(0,|ddl|ddl.ddl.len()).checked_add(schema_ddl.trigger.map_or(0,|ddl|ddl.ddl.len())).ok_or_else(||Error::Other("event DDL ordinal count overflow".into()))?)?;
                let ddl_entries = schema_ddl
                    .event_bus
                    .into_iter()
                    .flat_map(|commit| commit.ddl.iter())
                    .chain(
                        schema_ddl
                            .trigger
                            .into_iter()
                            .flat_map(|commit| commit.ddl.iter()),
                    )
                    .collect::<Vec<_>>();
                if !ddl_entries.is_empty() {
                    let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                    let mut ddl_table = write_txn
                        .open_table(DDL_LOG_TABLE)
                        .map_err(Self::storage_error)?;
                    for (index, change) in ddl_entries.iter().enumerate() {
                        let key = Self::ddl_log_key_for_index(lsn, index, ddl_entries.len());
                        self.append_ddl_summary_in_write(&write_txn, &key, change)?;
                        let encoded = frame.encode(change)?;
                        ddl_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        identity_mutations.push(content_identity::original_producers::encoded_history_mutation(&frame,
                            &key, change,
                        )?)?;
                    }
                }
            }

            if !ws.config_writes.is_empty() || !ws.config_deletes.is_empty() {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for key in &ws.config_deletes {
                    Self::update_policy_hold_release_index(&write_txn, key, false)?;
                    config_table
                        .remove(key.as_str())
                        .map_err(Self::storage_error)?;
                    identity_mutations.extend(content_identity::original_producers::config_mutations(&frame,key, None)?)?;
                }
                for (key, encoded) in &ws.config_writes {
                    Self::update_policy_hold_release_index(&write_txn, key, true)?;
                    let encoded = if ws.config_max_u64_keys.iter().any(|max_key| max_key == key) {
                        let incoming = frame.decode::<u64>(encoded)?;
                        let current = config_table
                            .get(key.as_str())
                            .map_err(Self::storage_error)?
                            .map(|value| frame.decode::<u64>(value.value()))
                            .transpose()?
                            .unwrap_or(0);
                        frame.encode(&current.max(incoming))?
                    } else {
                        encoded.clone()
                    };
                    config_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    identity_mutations.extend(content_identity::original_producers::config_mutations(&frame,
                        key,
                        Some(&encoded),
                    )?)?;
                }
            }

            if let Some(erasure) = options.local_erasure.as_ref() {
                self.apply_local_erasure(
                    &write_txn,
                    erasure.projection,
                    &prepared_local_erasure_vector_generations,
                    None,
                    erasure.accountant.clone(),
                    erasure.operation,
                    has_discard_history.then_some(&frame),
                )?;
            }
            #[cfg(any(test, feature = "test-seams"))]
            {
                observe_append_stage(AppendStage::RowsIndexesAndLogs, &mut stage_started);
            }
            Self::apply_owned_content_identity_with_manifests_and_groups_in_write(
                &write_txn,
                &identity_mutations,
                &pending_manifests.mutations,
                &mut content_identity::original_writer::ProofAdmission::new(frame.original,self.proof_scratch_owner())?, Some(&group_plan))?;
            drop(pending_manifests);
            #[cfg(any(test, feature = "test-seams"))]
            {
                observe_append_stage(AppendStage::ContentIdentity, &mut stage_started);
            }
            // Failure is reached after the unit and its outcome are staged.
            #[cfg(any(test, feature = "test-seams"))]
            if ws
                .config_writes
                .iter()
                .any(|(key, _)| key.starts_with("delivery_hub_outcome.v1."))
            {
                crate::custody::durability::before_custody_commit(&write_txn)?;
            }
            #[cfg(any(test, feature = "test-seams"))]
            {
                stage_started = std::time::Instant::now();
            }
            self.preserve_graph_commit_support(&write_txn,graph_commit_support,ws.commit_lsn.zip(Self::write_set_visibility_tx(ws)))?;
            // The grouping that decides the retention clocks is admitted first.
            let _clock_grouping = crate::memory_accounting::reserve_preparation_from_original(
                frame.original, Self::retention_clock_grouping_bytes(ws),
                "writer_source", "retention_clock_grouping", "Raise MEMORY_LIMIT to decide this commit's retention clocks.",
            )?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, Some(ws))?;
            #[cfg(feature = "in-process-test-seams")]
            crate::writer_commit_test_seams::reached(crate::writer_commit_test_seams::CommitBoundary::BeforeRedb);
            { #[cfg(any(test, feature = "test-seams"))]
            graph_observation_checkpoint(10)?;
            let graph_commit_result = write_txn.commit();
#[cfg(any(test, feature = "test-seams"))]
if graph_commit_result.is_ok() { graph_publication_attempt.committed(); graph_observation_checkpoint(11)?; }
graph_commit_result }.map_err(Self::storage_error)?;
            #[cfg(feature = "in-process-test-seams")]
            crate::writer_commit_test_seams::reached(crate::writer_commit_test_seams::CommitBoundary::AfterRedb);
            #[cfg(any(test, feature = "test-seams"))]
            {
                observe_append_stage(AppendStage::Commit, &mut stage_started);
            }
            Ok(())
        })
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn flush_owned_received_schema_stage(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        ws: &WriteSet,
        change_log: &[ChangeLogEntry],
        stage: &ReceivedSchemaPersistenceProjection,
        sink_events: &[PreparedSinkEvent],
        trigger_audits: &[(u64, TriggerAuditEntry)],
        max_sink_queue_depth: usize,
        schema_config: Option<&[(String, Vec<u8>)]>,
        config_cut: usize,
    ) -> Result<()> {
        let storage_scope =
            native_storage_admission::StorageAdmissionScope::writer_original(original)?;
        storage_scope.run_original_unqualified(|| {
            let frame = OriginalPersistenceFrame::new(original)?;
            frame.plan_write_set(ws, sink_events, trigger_audits)?;
            for meta in stage.table_meta.values() {
                frame.record(meta)?;
            }
            // Encoding and the received index rebuild retain original-funded
            // per-record windows, rather than the sum of every row's scratch.

            if stage.schema_noop {
                return self.flush_owned_received_schema_noop(
                    original,
                    ws,
                    change_log,
                    stage,
                    schema_config,
                    config_cut,
                );
            }
            let vector_quantization = Self::vector_quantization_map(&stage.table_meta);
            let sink_queue_names = sink_events
                .iter()
                .map(|(sink, _)| sink.clone())
                .collect::<BTreeSet<_>>();
            let schema = self.prepare_writer_schema_change(
                stage.affected_tables.iter().map(String::as_str),
                stage
                    .table_meta
                    .iter()
                    .map(|(name, meta)| (name.as_str(), meta)),
            )?;
            for (key, value) in
                original_schema_config_values(&stage.config_values, schema_config, config_cut)?
            {
                frame.plan_config(key, value)?;
            }
            let group_plan = custody_groups::GroupPlan::prepaid(
                original_schema_config_values(&stage.config_values, schema_config, config_cut)?
                    .chain(&ws.config_writes),
            )?;
            self.with_db(|db| {
                // A received working source and terminal installation share ONE
                // public write. No prefix commit or second writer acquisition.
                // Unjoined terminal: this flush still returns bare Error;
                // take_or_begin_funded cannot be flattened at this internal call.
                let write_txn = received_working::ReceivedWorkingWrite::take_or_begin(
                    self, db, original, stage,
                )?;
                let partition_image = stage
                    .replace_vectors
                    .then(|| {
                        self.prepare_complete_vector_partition_image_paged_original(
                            &frame, &write_txn, stage,
                        )
                    })
                    .transpose()?;
                #[cfg(any(test, feature = "test-seams"))]
                let mut graph_publication_attempt =
                    crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
                let graph_commit_support =
                    Self::capture_graph_commit_support(&write_txn, ws.commit_lsn)?;
                self.graph_writer_window(|| {
                    Self::apply_received_row_graph_parts_in_write(
                        &write_txn,
                        &stage.row_adj_inserts,
                        &stage.row_adj_deletes,
                        &stage.dropped_row_graph,
                    )?;
                    self.retire_received_shape_losses_in_write(&write_txn, stage, Some(original))
                })?;
                let indexes_to_rebuild = {
                    let stored_meta = write_txn
                        .open_table(META_TABLE)
                        .map_err(Self::storage_error)?;
                    let ready = write_txn
                        .open_table(RELATIONAL_INDEX_READY_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut rebuild = HashSet::new();
                    for (table, next) in &stage.table_meta {
                        let metadata_window = OriginalPersistenceFrame::new(frame.original)?;
                        metadata_window
                            .reserve(table.len().saturating_mul(4).saturating_add(512))?;
                        let previous = stored_meta
                            .get(Self::meta_key(table).as_str())
                            .map_err(Self::storage_error)?
                            .map(|encoded| {
                                Self::decode_table_meta_versioned_original(
                                    &metadata_window,
                                    encoded.value(),
                                )
                                .map(|(meta, _)| meta)
                            })
                            .transpose()?;
                        let unchanged_declarations = previous.as_ref().is_some_and(|previous| {
                            if previous.maintained_counts != next.maintained_counts {
                                return false;
                            }
                            let before = Self::relational_index_declarations(previous);
                            if before != Self::relational_index_declarations(next) {
                                return false;
                            }
                            let indexed_columns = before
                                .iter()
                                .flat_map(|index| {
                                    index.columns.iter().map(|(name, _)| name.as_str())
                                })
                                .collect::<HashSet<_>>();
                            previous
                                .columns
                                .iter()
                                .filter(|column| indexed_columns.contains(column.name.as_str()))
                                .eq(next.columns.iter().filter(|column| {
                                    indexed_columns.contains(column.name.as_str())
                                }))
                        });
                        let ready_before = ready
                            .get(table.as_str())
                            .map_err(Self::storage_error)?
                            .is_some_and(|version| {
                                version.value() == RELATIONAL_INDEX_READY_VERSION
                            });
                        if !unchanged_declarations
                            || !ready_before
                            || stage.row_changed_tables.contains(table)
                            || stage.structurally_dropped_tables.contains(table)
                        {
                            rebuild.insert(table.clone());
                        }
                    }
                    rebuild
                };

                // Read before anything is replaced: every member the replaced
                // generation stored for an affected table leaves the committed
                // identity in this transaction, ahead of the replacement's own
                // members, so the later mutation of one logical key wins.
                // Postings and the row versions they project are committed to
                // separately and must agree: see `historical_index_commitment`.
                let mut commitments =
                    historical_index_commitment::CommitmentBatch::for_original(frame.original)?;
                let retired_identity = self.retire_received_schema_generation_in_write_original(
                    &frame,
                    &write_txn,
                    stage,
                    &mut commitments,
                    Some(&write_txn),
                )?;

                {
                    let mut meta_table = write_txn
                        .open_table(META_TABLE)
                        .map_err(Self::storage_error)?;
                    // Only retired names are removed first. A surviving name's
                    // previous incarnation record must remain until its own encode.
                    for table in &stage.affected_tables {
                        if !stage.table_meta.contains_key(table) {
                            let key = Self::meta_key(table);
                            meta_table
                                .remove(key.as_str())
                                .map_err(Self::storage_error)?;
                        }
                    }
                    for (table, meta) in &stage.table_meta {
                        // Retain one complete encoding owner, not a HashMap of
                        // every table's bytes and conservative scratch reservation.
                        // The owner stays alive through insertion and route setup;
                        // its bytes drop before its reservation at this iteration's end.
                        let encoded = self.schema_update_in_table(
                            &meta_table,
                            table,
                            meta,
                            stage.ddl_log.iter().map(|(_, change)| change),
                        )?;
                        let key = Self::meta_key(table);
                        meta_table
                            .insert(key.as_str(), encoded.bytes.as_slice())
                            .map_err(Self::storage_error)?;
                        vector_membership_pages::ensure_declared_routes(
                            &write_txn,
                            table,
                            meta,
                            Some(
                                crate::memory_accounting::persistence_original_projection(
                                    frame.original,
                                )?
                                .as_handle(),
                            ),
                        )?;
                    }
                }

                self.retain_pending_authored_rows_in_write_original(
                    &frame,
                    &write_txn,
                    &stage.authored_pending_rows,
                )?;
                let mut relational_tables =
                    stage.affected_tables.iter().cloned().collect::<Vec<_>>();
                relational_tables.sort();
                relational_tables.dedup();
                for table in relational_tables {
                    #[cfg(any(test, feature = "test-seams"))]
                    crate::database::record_received_schema_durable_table_rewrite_for_test(&table);
                    let table_name = Self::rel_table_name(&table);
                    let table_def: TableDefinition<&[u8], &[u8]> =
                        TableDefinition::new(table_name.as_str());
                    self.retain_received_authored_rows_in_write(&write_txn, &table, stage)?;
                    self.retire_relational_manifests_in_write(&write_txn, &table)?;
                    content_size::reset_rows(&write_txn, &table)?;
                    match write_txn.delete_table(table_def) {
                        Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                        Err(redb::TableError::TableTypeMismatch { .. }) => {}
                        Err(error) => return Err(Self::storage_error(error)),
                    }
                    let legacy_table_def: TableDefinition<u64, &[u8]> =
                        TableDefinition::new(table_name.as_str());
                    match write_txn.delete_table(legacy_table_def) {
                        Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                        Err(redb::TableError::TableTypeMismatch { .. }) => {}
                        Err(error) => return Err(Self::storage_error(error)),
                    }
                    // The rows are written again below: the version side of
                    // every index is folded afresh from them.
                    if let Some(meta) = stage.table_meta.get(&table) {
                        for declaration in Self::relational_index_declarations(meta) {
                            commitments.reset(
                                &write_txn,
                                &table,
                                &declaration,
                                historical_index_commitment::Side::Versions,
                            )?;
                        }
                    }
                }
                frame.map::<&str, Option<Lsn>>(stage.table_meta.len())?;
                let mut table_read_floors =
                    HashMap::<&str, Option<Lsn>>::with_capacity(stage.table_meta.len());
                for table in stage.table_meta.keys() {
                    table_read_floors.insert(table.as_str(), None);
                }
                let mut recovery_floors: Option<(TxId, Lsn, RowId)> = None;
                fn fold_floor(
                    floors: &mut Option<(TxId, Lsn, RowId)>,
                    tx: TxId,
                    lsn: Lsn,
                    row: RowId,
                ) {
                    *floors = Some(floors.map_or((tx, lsn, row), |(t, l, r)| {
                        (t.max(tx), l.max(lsn), r.max(row))
                    }));
                }
                let row_identity_heads =
                    schema_projection::ReceivedLatestMutations::new(&write_txn)?;
                for (table, meta) in &stage.table_meta {
                    let table_window = OriginalPersistenceFrame::new(frame.original)?;
                    table_window.reserve(table.len().saturating_add(256))?;
                    let table_name = Self::rel_table_name(table);
                    let table_def: TableDefinition<&[u8], &[u8]> =
                        TableDefinition::new(table_name.as_str());
                    let mut indexes = if indexes_to_rebuild.contains(table) {
                        Some(schema_projection::OriginalProjectedIndexes::prepare(
                            self,
                            &frame,
                            &write_txn,
                            table,
                            meta,
                            &mut commitments,
                        )?)
                    } else {
                        None
                    };
                    let mut prior = None;
                    stage.visit_table_rows_original(&frame, table, |row| {
                        let rank = (row.row_id, row.created_tx, row.lsn);
                        if prior.is_some_and(|previous| previous > rank) {
                            return Err(Error::Other(
                                "received index input is not in prepared durable order".into(),
                            ));
                        }
                        prior = Some(rank);
                        // The physical encoding owns a shorter window than the
                        // logical identity; no dead encoded buffer stays billed.
                        {
                            // Close the row table before opening index/proof tables.
                            let frame = OriginalPersistenceFrame::new(frame.original)?;
                            frame.record(meta)?;
                            let mut redb_table =
                                content_size::open_rows(&write_txn, table_def, Some(meta))
                                    .map_err(Self::storage_error)?;
                            #[cfg(any(test, feature = "test-seams"))]
                            crate::database::record_received_schema_durable_row_rewrite_for_test();
                            let encoded = Self::encode_versioned_row_original(
                                &frame,
                                row,
                                stage.table_meta.get(table),
                            )?;
                            let key = Self::rel_row_key(row);
                            {
                                let predecessor = redb_table
                                    .get(key.as_slice())
                                    .map_err(Self::storage_error)?;
                                self.replace_relational_manifest_in_write_original(
                                    frame.original,
                                    &write_txn,
                                    table,
                                    key.as_slice(),
                                    predecessor.as_ref().map(|body| body.value()),
                                    Some(encoded.as_slice()),
                                )?;
                            }
                            let keys =
                                content_size::VersionLife::keys_of(redb_table.indexes(), row)?;
                            redb_table.insert_version(
                                &mut commitments,
                                key.as_slice(),
                                encoded.as_slice(),
                                &content_size::VersionLife {
                                    keys: &keys,
                                    created: row.created_tx,
                                    deleted: row.deleted_tx,
                                },
                            )?;
                            drop(redb_table);
                        }
                        if let Some(indexes) = indexes.as_mut() {
                            indexes.push(self, &write_txn, table, meta, row, &mut commitments)?;
                        }
                        if let Some(floor) = table_read_floors.get_mut(table.as_str()) {
                            *floor = Some(floor.map_or(row.lsn, |lsn| lsn.max(row.lsn)));
                        }
                        fold_floor(
                            &mut recovery_floors,
                            row.created_tx.max(row.deleted_tx.unwrap_or(TxId(0))),
                            row.lsn,
                            row.row_id,
                        );
                        let identity_window = OriginalPersistenceFrame::new(frame.original)?;
                        let key = identity_window.encode(&(table.as_str(), row.row_id))?;
                        let mutation = if row.deleted_tx.is_none() {
                            Some(content_identity::original_producers::row_mutation(
                                &identity_window,
                                table,
                                row,
                                Some(meta),
                            )?)
                        } else {
                            None
                        };
                        row_identity_heads.consider(
                            &identity_window,
                            &write_txn,
                            &key,
                            row.lsn,
                            row.created_tx,
                            mutation.as_ref(),
                        )?;
                        Ok(())
                    })?;
                    if let Some(indexes) = indexes {
                        indexes.finish(self, &write_txn, table, meta)?;
                    }
                }
                // The replacement is complete here: its commitments are checked
                // and published before the operational rows fold their own.
                let (replacement_commitments, _replacement_commitment_credit) =
                    commitments.finish(&write_txn)?.into_parts();
                let (operational_index_identity_mutations, _operational_commitment_credits) = self
                    .flush_received_schema_operational_rows_in_write_original(
                        &frame, &write_txn, stage,
                    )?;

                for table in &stage.affected_tables {
                    self.retire_table_source_provenance_in_write_original(
                        &frame, &write_txn, table, true, false, None,
                    )?;
                }
                {
                    let mut lsn_table = write_txn
                        .open_table(SYNC_ROW_SOURCE_LSN_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut kind_table = write_txn
                        .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
                        .map_err(Self::storage_error)?;
                    stage.visit_sync_sources_original(&frame, |table, row_id, lsn, kind| {
                        let window = OriginalPersistenceFrame::new(frame.original)?;
                        window.reserve(table.len().saturating_add(16))?;
                        let key = Self::sync_row_source_lsn_key(table, row_id);
                        lsn_table
                            .insert(key.as_slice(), lsn.0)
                            .map_err(Self::storage_error)?;
                        kind_table
                            .insert(key.as_slice(), kind)
                            .map_err(Self::storage_error)?;
                        Ok(())
                    })?;
                }

                if stage.replace_graph {
                    if write_txn.graph_prepared()? {
                        self.retire_working_graph_replacement_in_write_original(
                            &frame,
                            &write_txn,
                            &stage.affected_graph_edge_types,
                            stage.graph_selection.as_ref(),
                        )?;
                    } else {
                        self.retire_graph_replacement_in_write_original(
                            &frame,
                            &write_txn,
                            &stage.edges,
                            Some(&stage.affected_graph_edge_types),
                        )?;
                    }
                    self.erase_received_graph_adjacencies_in_write_original(
                        &frame,
                        &write_txn,
                        &stage.affected_graph_edge_types,
                        Some((&write_txn, stage.graph_selection.as_ref())),
                    )?;
                    stage.visit_edges_original(&frame, &write_txn, |edge, raw| {
                        let window = OriginalPersistenceFrame::new(frame.original)?;
                        let _graph_window = self.original_graph_window(&window)?;
                        let insert =
                            |fwd_key: &[u8], rev_key: &[u8], encoded: &[u8]| -> Result<()> {
                                let mut fwd = write_txn
                                    .open_table(GRAPH_FWD_TABLE)
                                    .map_err(Self::storage_error)?;
                                let mut rev = write_txn
                                    .open_table(GRAPH_REV_TABLE)
                                    .map_err(Self::storage_error)?;
                                fwd.insert(
                                    observed_graph_key!("graph_fwd", "insert", fwd_key, 5),
                                    encoded,
                                )
                                .map_err(Self::storage_error)?;
                                self.graph_writer_window(|| {
                                    Self::install_graph_creation_in_write(&write_txn, edge, encoded)
                                })?;
                                rev.insert(
                                    observed_graph_key!("graph_rev", "insert", rev_key, 5),
                                    encoded,
                                )
                                .map_err(Self::storage_error)?;
                                Ok(())
                            };
                        if let Some((key, encoded)) = raw {
                            let mut reverse = window.copy_bytes(key)?;
                            reverse[..16].copy_from_slice(&key[16..32]);
                            reverse[16..32].copy_from_slice(&key[..16]);
                            insert(key, &reverse, encoded)
                        } else {
                            let _json_scratch = window.json_scratch(edge.properties.values())?;
                            window.record(edge)?;
                            let encoded = window.encode(edge)?;
                            let fwd_key = Self::graph_fwd_key(edge);
                            let rev_key = Self::graph_rev_key(edge);
                            insert(&fwd_key, &rev_key, &encoded)
                        }
                    })?;
                }

                if stage.replace_vectors {
                    // Authenticate the old fixed inputs while their raw bodies and
                    // source companions still belong to the committed image.
                    let previous = db.begin_read().map_err(Self::storage_error)?;
                    self.reconcile_paged_builds_for_image_in_write(
                        &write_txn,
                        partition_image.as_ref().expect("prepared vector image"),
                        &previous,
                        stage,
                        Some(&frame),
                    )?;
                    self.remove_vector_entries_matching_in_write(&write_txn, &|index| {
                        stage.affected_tables.contains(&index.table)
                    })?;
                    let mut vectors =
                        content_size::open_vectors(&write_txn).map_err(Self::storage_error)?;
                    stage.visit_vector_bodies_original(&frame, |entry| {
                        let frame = OriginalPersistenceFrame::new(frame.original)?;
                        let quantization = vector_quantization
                            .get(&entry.index)
                            .copied()
                            .unwrap_or_default();
                        let encoded =
                            Self::encode_vector_entry_original(&frame, entry, quantization)?;
                        let key = Self::vector_key(entry);
                        vectors
                            .insert(key.as_slice(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        Ok(())
                    })?;
                    self.replace_paged_vector_partition_image_in_write_original(
                        &frame,
                        &write_txn,
                        partition_image.as_ref().expect("prepared vector image"),
                        &stage.affected_tables,
                        ws.commit_lsn.unwrap_or(Lsn(0)),
                    )?;
                    for (table, meta) in &stage.table_meta {
                        vector_membership_pages::ensure_declared_routes(
                            &write_txn,
                            table,
                            meta,
                            Some(
                                crate::memory_accounting::persistence_original_projection(
                                    frame.original,
                                )?
                                .as_handle(),
                            ),
                        )?;
                    }
                }

                {
                    let mut ddl_table = write_txn
                        .open_table(DDL_LOG_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut grouped = BTreeMap::<Lsn, Vec<&DdlChange>>::new();
                    for (lsn, ddl) in &stage.ddl_log {
                        grouped.entry(*lsn).or_default().push(ddl);
                    }
                    for (lsn, ddl) in grouped {
                        for (index, change) in ddl.iter().enumerate() {
                            let key = Self::ddl_log_key_for_index(lsn, index, ddl.len());
                            self.append_ddl_summary_in_write(&write_txn, &key, change)?;
                            let encoded = frame.encode(*change)?;
                            ddl_table
                                .insert(key.as_str(), encoded.as_slice())
                                .map_err(Self::storage_error)?;
                        }
                    }
                }

                if let (Some(lsn), Some(tx)) = (ws.commit_lsn, Self::write_set_visibility_tx(ws)) {
                    let mut index = write_txn
                        .open_table(COMMIT_INDEX_TABLE)
                        .map_err(Self::storage_error)?;
                    index.insert(lsn.0, tx.0).map_err(Self::storage_error)?;
                }
                // Graph history may name an interior member. Install only these
                // receiver-generated membership facts before its proof checks;
                // the ordinary config/identity phase still publishes them below
                // in this SAME unpublished transaction and single END commit.
                {
                    let mut config = write_txn
                        .open_table(CONFIG_TABLE)
                        .map_err(Self::storage_error)?;
                    for (key, value) in original_schema_config_values(
                        &stage.config_values,
                        schema_config,
                        config_cut,
                    )?
                    .chain(&ws.config_writes)
                    .filter(|(key, _)| {
                        key.starts_with(crate::database::connected_replay::MEMBERSHIP_PREFIX)
                    }) {
                        let window = OriginalPersistenceFrame::new(frame.original)?;
                        let position = key
                            .strip_prefix(crate::database::connected_replay::MEMBERSHIP_PREFIX)
                            .filter(|suffix| suffix.len() == 20)
                            .and_then(|suffix| suffix.parse::<u64>().ok())
                            .ok_or_else(|| {
                                Error::Other("received graph membership key is invalid".into())
                            })?;
                        let (start, end): (u64, u64) = window.decode(value)?;
                        if start == 0
                            || start > position
                            || position > end
                            || Some(Lsn(end)) != ws.commit_lsn
                        {
                            return Err(Error::Other(
                                "received graph membership exceeds its acceptance".into(),
                            ));
                        }
                        config
                            .insert(key.as_str(), value.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }
                let mut change_log_identity =
                    schema_projection::OriginalMutationLog::new_change_log(
                        self, &frame, &write_txn,
                    )?;
                self.retire_received_change_log_paged_original(
                    &frame,
                    &write_txn,
                    &stage.structurally_dropped_tables,
                    &mut change_log_identity,
                )?;
                Self::stage_graph_commit_support(&write_txn, graph_commit_support)?;
                let mut written_change_log_count = 0usize;
                if !change_log.is_empty() || stage.graph_occurrences {
                    let mut log = write_txn
                        .open_table(CHANGE_LOG_TABLE)
                        .map_err(Self::storage_error)?;
                    let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                    let receiver = Self::write_set_visibility_tx(ws).ok_or_else(|| {
                        Error::Other("received graph history lost its receiver identity".into())
                    })?;
                    let mut append = |entry: &ChangeLogEntry,
                                      predecessor: Option<&[u8]>|
                     -> Result<()> {
                        let window = OriginalPersistenceFrame::new(frame.original)?;
                        let _graph_window = self.original_graph_window(&window)?;
                        window.reserve(Self::change_log_entry_key_len())?;
                        let mut key = String::with_capacity(Self::change_log_entry_key_len());
                        let mut encoded = Vec::new();
                        Self::write_change_log_entry_key(lsn, written_change_log_count, &mut key);
                        self.encode_change_log_with_selected_predecessor_in_write(
                            &write_txn,
                            entry,
                            predecessor.map(|key| (key, receiver)),
                            &mut encoded,
                        )?;
                        log.insert(
                            observed_graph_key!("history", "insert", key.as_str(), 8),
                            encoded.as_slice(),
                        )
                        .map_err(Self::storage_error)?;
                        self.graph_writer_window(|| {
                            Self::write_change_log_selectors_in_write(
                                &write_txn,
                                key.as_str(),
                                encoded.as_slice(),
                            )
                        })?;
                        written_change_log_count =
                            written_change_log_count.checked_add(1).ok_or_else(|| {
                                Error::Other("received graph history count overflow".into())
                            })?;
                        Ok(())
                    };
                    if stage.graph_occurrences {
                        let selection = stage.graph_selection.as_ref().ok_or_else(|| {
                            Error::Other("received graph history lost its frozen selection".into())
                        })?;
                        let mut entries = change_log
                            .iter()
                            .filter(|entry| {
                                !matches!(
                                    entry,
                                    ChangeLogEntry::EdgeInsert { .. }
                                        | ChangeLogEntry::EdgeDelete { .. }
                                )
                            })
                            .peekable();
                        write_txn.visit_graph_events_original(
                            frame.original,
                            selection,
                            |entry, predecessor| {
                                while entries.peek().is_some_and(|next| next.lsn() <= entry.lsn()) {
                                    append(entries.next().expect("peeked history entry"), None)?;
                                }
                                append(entry, predecessor)
                            },
                        )?;
                        for entry in entries {
                            append(entry, None)?;
                        }
                    } else {
                        for entry in change_log {
                            append(entry, None)?;
                        }
                    }
                }
                let mut committed_config_values = Vec::new();
                {
                    let mut config = write_txn
                        .open_table(CONFIG_TABLE)
                        .map_err(Self::storage_error)?;
                    for key in &ws.config_deletes {
                        Self::update_policy_hold_release_index(&write_txn, key, false)?;
                        config.remove(key.as_str()).map_err(Self::storage_error)?;
                    }
                    for (key, value) in original_schema_config_values(
                        &stage.config_values,
                        schema_config,
                        config_cut,
                    )?
                    .chain(&ws.config_writes)
                    {
                        Self::raise_config_identity_floors_in_write(&write_txn, key, value)?;
                        Self::update_policy_hold_release_index(&write_txn, key, true)?;
                        let value = if stage
                            .config_max_u64_keys
                            .iter()
                            .any(|max_key| max_key == key)
                        {
                            let incoming = frame.decode::<u64>(value)?;
                            let current = config
                                .get(key.as_str())
                                .map_err(Self::storage_error)?
                                .map(|value| frame.decode::<u64>(value.value()))
                                .transpose()?
                                .unwrap_or(0);
                            frame.encode(&current.max(incoming))?
                        } else {
                            value.clone()
                        };
                        config
                            .insert(key.as_str(), value.as_slice())
                            .map_err(Self::storage_error)?;
                        committed_config_values.push((key.clone(), value));
                    }
                }
                // The complete received-schema core image now exists only inside
                // this live Redb transaction. Sink queues and trigger audits for
                // received rows belong immediately below this checkpoint and must
                // commit with that image, never in a later transaction.
                #[cfg(test)]
                if take_received_schema_side_effect_persistence_fault_for_test() {
                    return Err(Error::Other(
                        ("injected received-schema side-effect persistence failure".to_string())
                            .into(),
                    ));
                }
                if !sink_events.is_empty() {
                    let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                    let mut overflow_audits = Vec::new();
                    let mut by_sink: BTreeMap<&str, Vec<(usize, &SinkQueueEntry)>> =
                        BTreeMap::new();
                    for (index, (sink, entry)) in sink_events.iter().enumerate() {
                        by_sink
                            .entry(sink.as_str())
                            .or_default()
                            .push((index, entry));
                    }
                    for (sink, entries) in by_sink {
                        let table_name = Self::sink_queue_table_name(sink);
                        let table_def: TableDefinition<u64, &[u8]> =
                            TableDefinition::new(table_name.as_str());
                        let mut table = write_txn
                            .open_table(table_def)
                            .map_err(Self::storage_error)?;
                        let mut existing = table.len().map_err(Self::storage_error)? as usize;
                        for (index, entry) in entries {
                            if existing >= max_sink_queue_depth {
                                return Err(Error::Other(
                                    (format!("sink queue is full: {sink}")).into(),
                                ));
                            }
                            let overflow_id = if existing >= max_sink_queue_depth {
                                table
                                    .first()
                                    .map_err(Self::storage_error)?
                                    .map(|(key, _)| key.value())
                            } else {
                                None
                            };
                            if let Some(id) = overflow_id {
                                table.remove(id).map_err(Self::storage_error)?;
                                existing = existing.saturating_sub(1);
                                overflow_audits.push((index, sink.to_string()));
                            }
                            let encoded = frame.encode(entry)?;
                            table
                                .insert(entry.id, encoded.as_slice())
                                .map_err(Self::storage_error)?;
                            existing = existing.saturating_add(1);
                        }
                    }
                    if !overflow_audits.is_empty() {
                        let mut audit_table = write_txn
                            .open_table(SINK_AUDIT_TABLE)
                            .map_err(Self::storage_error)?;
                        for (index, sink) in overflow_audits {
                            let key = Self::sink_audit_key(lsn, index, &sink);
                            let audit = SinkAuditEntry {
                                lsn,
                                kind: SinkAuditKind::QueueOverflow {
                                    sink,
                                    dropped_count: 1,
                                },
                            };
                            let encoded = frame.encode(&audit)?;
                            audit_table
                                .insert(key.as_str(), encoded.as_slice())
                                .map_err(Self::storage_error)?;
                        }
                    }
                }
                if !trigger_audits.is_empty() {
                    {
                        let mut audit_table = write_txn
                            .open_table(TRIGGER_AUDIT_TABLE)
                            .map_err(Self::storage_error)?;
                        let mut stamps = write_txn
                            .open_table(TRIGGER_AUDIT_STAMPS_TABLE)
                            .map_err(Self::storage_error)?;
                        let stamped_at = Wallclock::now().0;
                        for (index, entry) in trigger_audits {
                            let key = Self::trigger_audit_key(*index, &entry.trigger_name);
                            let encoded = frame.encode(entry)?;
                            audit_table
                                .insert(key.as_str(), encoded.as_slice())
                                .map_err(Self::storage_error)?;
                            stamps
                                .insert(key.as_str(), stamped_at)
                                .map_err(Self::storage_error)?;
                        }
                    }
                    {
                        let mut config_table = write_txn
                            .open_table(CONFIG_TABLE)
                            .map_err(Self::storage_error)?;
                        let mut ring: Vec<TriggerAuditEntry> = config_table
                            .get(TRIGGER_AUDIT_RING_CONFIG_KEY)
                            .map_err(Self::storage_error)?
                            .map(|value| frame.decode(value.value()))
                            .transpose()?
                            .unwrap_or_default();
                        frame.array::<TriggerAuditEntry>(
                            ring.len()
                                .checked_add(trigger_audits.len())
                                .and_then(|n| n.checked_mul(4))
                                .ok_or_else(|| {
                                    Error::Other("trigger audit ring preparation overflow".into())
                                })?,
                        )?;
                        ring.extend(trigger_audits.iter().map(|(_, entry)| entry.clone()));
                        let overflow = ring
                            .len()
                            .saturating_sub(crate::database::trigger::TRIGGER_AUDIT_RING_CAPACITY);
                        if overflow > 0 {
                            ring.drain(0..overflow);
                        }
                        let current_next_index = config_table
                            .get(TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY)
                            .map_err(Self::storage_error)?
                            .map(|value| frame.decode::<u64>(value.value()))
                            .transpose()?
                            .unwrap_or(0);
                        let next_index = trigger_audits
                            .iter()
                            .map(|(index, _)| index.saturating_add(1))
                            .max()
                            .unwrap_or(0)
                            .max(current_next_index);
                        let encoded_ring = frame.encode(&ring)?;
                        config_table
                            .insert(TRIGGER_AUDIT_RING_CONFIG_KEY, encoded_ring.as_slice())
                            .map_err(Self::storage_error)?;
                        committed_config_values
                            .push((TRIGGER_AUDIT_RING_CONFIG_KEY.to_owned(), encoded_ring));
                        let encoded_next_index = frame.encode(&next_index)?;
                        config_table
                            .insert(
                                TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY,
                                encoded_next_index.as_slice(),
                            )
                            .map_err(Self::storage_error)?;
                        committed_config_values.push((
                            TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY.to_owned(),
                            encoded_next_index,
                        ));
                    }
                }
                let mut identity_mutations = retired_identity;
                identity_mutations.extend(replacement_commitments)?;
                identity_mutations.extend(operational_index_identity_mutations)?;
                for table in &stage.affected_tables {
                    if !stage.table_meta.contains_key(table) {
                        let frame = OriginalPersistenceFrame::new(frame.original)?;
                        identity_mutations.extend(
                            content_identity::original_producers::remove_schema_mutations(
                                &frame, table,
                            )?,
                        )?;
                    }
                }
                for (table, meta) in &stage.table_meta {
                    let frame = OriginalPersistenceFrame::new(frame.original)?;
                    identity_mutations.extend(
                        content_identity::original_producers::schema_mutations(
                            &frame, table, meta,
                        )?,
                    )?;
                }
                // The history retirement above, removals before the packed
                // survivors, so the physical store and the authenticated history
                // it proves never disagree about which records this installation
                // left behind or where they sit.
                change_log_identity.append_to(&mut identity_mutations)?;
                row_identity_heads.visit(&frame, &write_txn, |mutation| {
                    identity_mutations.push(mutation)
                })?;
                for (table, row_id, _) in &ws.relational_deletes {
                    let frame = OriginalPersistenceFrame::new(frame.original)?;
                    identity_mutations.push(
                        content_identity::original_producers::remove_row_mutation(
                            &frame, table, *row_id,
                        )?,
                    )?;
                }
                for (table, row) in &stage.operational_write_set.relational_inserts {
                    let frame = OriginalPersistenceFrame::new(frame.original)?;
                    identity_mutations.push(content_identity::original_producers::row_mutation(
                        &frame,
                        table,
                        row,
                        stage.operational_table_meta.get(table),
                    )?)?;
                }
                let latest = schema_projection::ReceivedLatestMutations::new(&write_txn)?;
                stage.visit_edges_original(&frame, &write_txn, |edge, _raw| {
                    let window = OriginalPersistenceFrame::new(frame.original)?;
                    window.reserve(edge.edge_type.len().saturating_mul(4).saturating_add(512))?;
                    let key = window.encode(&(&edge.edge_type, edge.source, edge.target))?;
                    let mutation = if edge.deleted_tx.is_none() {
                        Some(content_identity::original_producers::edge_mutation(
                            &window, edge,
                        )?)
                    } else {
                        None
                    };
                    latest.consider(
                        &window,
                        &write_txn,
                        &key,
                        edge.lsn,
                        edge.created_tx,
                        mutation.as_ref(),
                    )?;
                    Ok(())
                })?;
                latest.visit(&frame, &write_txn, |mutation| {
                    identity_mutations.push(mutation)
                })?;
                let latest = schema_projection::ReceivedLatestMutations::new(&write_txn)?;
                stage.visit_vector_bodies_original(&frame, |vector| {
                    if let Some(floor) = table_read_floors.get_mut(vector.index.table.as_str()) {
                        *floor = Some(floor.map_or(vector.lsn, |lsn| lsn.max(vector.lsn)));
                    }
                    fold_floor(
                        &mut recovery_floors,
                        vector.created_tx.max(vector.deleted_tx.unwrap_or(TxId(0))),
                        vector.lsn,
                        vector.row_id,
                    );
                    let window = OriginalPersistenceFrame::new(frame.original)?;
                    let quantization = vector_quantization
                        .get(&vector.index)
                        .copied()
                        .unwrap_or_default();
                    identity_mutations.push(
                        content_identity::original_producers::vector_version_mutation(
                            &window,
                            vector,
                            quantization,
                        )?,
                    )?;
                    window.reserve(
                        vector
                            .index
                            .table
                            .len()
                            .saturating_add(vector.index.column.len())
                            .saturating_mul(4)
                            .saturating_add(512),
                    )?;
                    let key = window.encode(&(
                        &vector.index.table,
                        &vector.index.column,
                        vector.row_id,
                    ))?;
                    let mutation = if vector.deleted_tx.is_none() {
                        Some(content_identity::original_producers::vector_mutation(
                            &window,
                            vector,
                            quantization,
                        )?)
                    } else {
                        None
                    };
                    latest.consider(
                        &window,
                        &write_txn,
                        &key,
                        vector.lsn,
                        vector.created_tx,
                        mutation.as_ref(),
                    )
                })?;
                latest.visit(&frame, &write_txn, |mutation| {
                    identity_mutations.push(mutation)
                })?;
                stage.visit_sync_sources_original(&frame, |table, row_id, lsn, kind| {
                    let frame = OriginalPersistenceFrame::new(frame.original)?;
                    identity_mutations.push(
                        content_identity::original_producers::sync_source_mutation(
                            &frame, table, row_id, lsn.0, kind,
                        )?,
                    )?;
                    Ok(())
                })?;
                let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                frame.reserve(Self::change_log_entry_key_len())?;
                let mut change_key = String::with_capacity(Self::change_log_entry_key_len());
                for index in 0..written_change_log_count {
                    let window = OriginalPersistenceFrame::new(frame.original)?;
                    Self::write_change_log_entry_key(lsn, index, &mut change_key);
                    identity_mutations.push(Self::stored_change_log_mutation_in_write_original(
                        &window,
                        &write_txn,
                        &change_key,
                    )?)?;
                }
                let mut grouped_ddl = BTreeMap::<Lsn, Vec<&DdlChange>>::new();
                for (ddl_lsn, change) in &stage.ddl_log {
                    grouped_ddl.entry(*ddl_lsn).or_default().push(change);
                }
                for (ddl_lsn, changes) in grouped_ddl {
                    for (index, change) in changes.iter().enumerate() {
                        let frame = OriginalPersistenceFrame::new(frame.original)?;
                        let key = Self::ddl_log_key_for_index(ddl_lsn, index, changes.len());
                        identity_mutations.push_at(
                            "received_ddl_history",
                            content_identity::original_producers::encoded_history_mutation(
                                &frame, &key, *change,
                            )?,
                        )?;
                    }
                }
                // The committed history records the very commit-index entry this
                // flush stores above, with the same transaction id.
                if let (Some(commit_lsn), Some(tx)) =
                    (ws.commit_lsn, Self::write_set_visibility_tx(ws))
                {
                    identity_mutations.push(
                        content_identity::original_producers::commit_index_mutation(
                            &frame, commit_lsn, tx,
                        )?,
                    )?;
                }
                for key in &ws.config_deletes {
                    let frame = OriginalPersistenceFrame::new(frame.original)?;
                    identity_mutations.extend(
                        content_identity::original_producers::config_mutations(&frame, key, None)?,
                    )?;
                }
                for (key, value) in &committed_config_values {
                    let frame = OriginalPersistenceFrame::new(frame.original)?;
                    identity_mutations.extend(
                        content_identity::original_producers::config_mutations(
                            &frame,
                            key,
                            Some(value),
                        )?,
                    )?;
                }
                if !sink_events.is_empty() {
                    let table = write_txn
                        .open_table(SINK_AUDIT_TABLE)
                        .map_err(Self::storage_error)?;
                    for item in table.iter().map_err(Self::storage_error)? {
                        let frame = OriginalPersistenceFrame::new(frame.original)?;
                        let (key, value) = item.map_err(Self::storage_error)?;
                        identity_mutations.push(
                            content_identity::original_producers::status_mutation(
                                &frame,
                                b"sink-audit",
                                key.value().as_bytes(),
                                value.value(),
                            )?,
                        )?;
                    }
                }
                if !trigger_audits.is_empty() {
                    let table = write_txn
                        .open_table(TRIGGER_AUDIT_TABLE)
                        .map_err(Self::storage_error)?;
                    for item in table.iter().map_err(Self::storage_error)? {
                        let frame = OriginalPersistenceFrame::new(frame.original)?;
                        let (key, value) = item.map_err(Self::storage_error)?;
                        identity_mutations.push(
                            content_identity::original_producers::status_mutation(
                                &frame,
                                b"trigger-audit",
                                key.value().as_bytes(),
                                value.value(),
                            )?,
                        )?;
                    }
                    let table = write_txn
                        .open_table(TRIGGER_AUDIT_STAMPS_TABLE)
                        .map_err(Self::storage_error)?;
                    for item in table.iter().map_err(Self::storage_error)? {
                        let frame = OriginalPersistenceFrame::new(frame.original)?;
                        let (key, stamp) = item.map_err(Self::storage_error)?;
                        identity_mutations.push(
                            content_identity::original_producers::status_mutation(
                                &frame,
                                b"trigger-stamp",
                                key.value().as_bytes(),
                                &stamp.value().to_le_bytes(),
                            )?,
                        )?;
                    }
                }
                for sink in &sink_queue_names {
                    let table_window = OriginalPersistenceFrame::new(frame.original)?;
                    table_window.reserve(sink.len().saturating_add(256))?;
                    let table_name = Self::sink_queue_table_name(sink);
                    let table_def: TableDefinition<u64, &[u8]> =
                        TableDefinition::new(table_name.as_str());
                    let table = write_txn
                        .open_table(table_def)
                        .map_err(Self::storage_error)?;
                    for item in table.iter().map_err(Self::storage_error)? {
                        let frame = OriginalPersistenceFrame::new(frame.original)?;
                        let (id, value) = item.map_err(Self::storage_error)?;
                        let mut decoded = original_event_decode::decode_sink_entry(
                            frame.original,
                            value.value(),
                        )?;
                        frame.retain(&mut decoded.allocation)?;
                        let entry = decoded.value;
                        frame.reserve(sink.len().saturating_add(128))?;
                        let key = format!("{}:{sink}:{}", sink.len(), id.value());
                        identity_mutations.push(
                            content_identity::original_producers::encoded_status_mutation(
                                &frame,
                                b"sink-queue",
                                key.as_bytes(),
                                &entry,
                            )?,
                        )?;
                    }
                }
                write_txn.discard_scratch()?;
                identity_mutations.apply(self, Some(&group_plan))?;
                if take_received_schema_pre_commit_fault_for_test() {
                    return Err(Error::Other(
                        ("injected received-schema Redb pre-commit failure".to_string()).into(),
                    ));
                }
                // Failure is reached after the unit and its outcome are staged.
                #[cfg(any(test, feature = "test-seams"))]
                if ws
                    .config_writes
                    .iter()
                    .any(|(key, _)| key.starts_with("delivery_hub_outcome.v1."))
                {
                    crate::custody::durability::before_custody_commit(&write_txn)?;
                }
                for (table, lsn) in table_read_floors {
                    if let Some(lsn) = lsn {
                        let window = OriginalPersistenceFrame::new(frame.original)?;
                        window.reserve(table.len().saturating_mul(4).saturating_add(256))?;
                        Self::raise_table_read_lsn_in_write(&write_txn, table, lsn)?;
                    }
                }
                if let Some((tx, lsn, row)) = recovery_floors {
                    // Recovery bounds encode into their existing fixed stack array.
                    Self::raise_noncanonical_writer_floors_in_write(&write_txn, tx, lsn, row)?;
                }
                self.preserve_graph_commit_support(
                    &write_txn,
                    graph_commit_support,
                    ws.commit_lsn.zip(Self::write_set_visibility_tx(ws)),
                )?;
                // The grouping that decides the retention clocks is admitted first.
                let _clock_grouping = crate::memory_accounting::reserve_preparation_from_original(
                    frame.original,
                    Self::retention_clock_grouping_bytes(ws),
                    "writer_source",
                    "retention_clock_grouping",
                    "Raise MEMORY_LIMIT to decide this commit's retention clocks.",
                )?;
                Self::write_writer_recovery_bounds_in_write(&write_txn, Some(ws))?;
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
                self.complete_writer_schema_change(schema);
                Ok(())
            })
        })
    }

    /// Measurement only: run the owned no-op received-schema flush for one
    /// DDL entry at `lsn` on this store, exactly as its producer would.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn flush_received_schema_noop_ddl_for_test(
        &self,
        accountant: crate::memory_accounting::AccountantHandle,
        lsn: Lsn,
        change: DdlChange,
    ) -> Result<()> {
        let original = crate::memory_accounting::OwnedMemoryReservation::new(accountant);
        let ws = WriteSet {
            commit_lsn: Some(lsn),
            ..WriteSet::default()
        };
        let stage = ReceivedSchemaPersistenceProjection {
            row_adj_inserts: Vec::new(),
            row_adj_deletes: Vec::new(),
            dropped_row_graph: Vec::new(),
            working_write: parking_lot::Mutex::new(None),
            schema_noop: true,
            affected_tables: HashSet::new(),
            replace_graph: false,
            affected_graph_edge_types: HashSet::new(),
            graph_selection: None,
            graph_occurrences: false,
            replace_vectors: false,
            vector_image_memory: crate::read_image_memory::ImageMemory::new(None)?,
            retired_vector_construction: parking_lot::Mutex::new(Vec::new()),
            table_meta: HashMap::new(),
            row_changed_tables: HashSet::new(),
            authored_pending_rows: Vec::new(),
            rows: HashMap::new(),
            row_records: None,
            private_rows: None,
            sync_sources: Vec::new(),
            sync_source_records: None,
            edges: Vec::new(),
            vectors: Vec::new(),
            vector_memberships: Vec::new(),
            vector_records: None,
            ddl_log: vec![(lsn, change)],
            config_values: Vec::new(),
            config_max_u64_keys: Vec::new(),
            operational_write_set: WriteSet::default(),
            operational_table_meta: HashMap::new(),
            operational_deleted_rows: HashMap::new(),
            structurally_dropped_tables: HashSet::new(),
        };
        self.flush_owned_received_schema_noop(&original, &ws, &[], &stage, None, 0)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn flush_owned_received_schema_noop(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        ws: &WriteSet,
        change_log: &[ChangeLogEntry],
        stage: &ReceivedSchemaPersistenceProjection,
        schema_config: Option<&[(String, Vec<u8>)]>,
        config_cut: usize,
    ) -> Result<()> {
        let frame = OriginalPersistenceFrame::new(original)?;
        frame.plan_write_set(ws, &[], &[])?;

        #[cfg(any(test, feature = "test-seams"))]
        let flush_started = std::time::Instant::now();
        #[cfg(any(test, feature = "test-seams"))]
        {
            crate::database::record_received_schema_persistence_work_for_test(
                "noop-ddl-log-writes",
                stage.ddl_log.len() as u64,
            );
            crate::database::record_received_schema_persistence_work_for_test(
                "noop-config-writes",
                stage
                    .config_values
                    .len()
                    .saturating_add(ws.config_writes.len()) as u64,
            );
            crate::database::record_received_schema_persistence_work_for_test(
                "noop-config-deletes",
                ws.config_deletes.len() as u64,
            );
            crate::database::record_received_schema_persistence_work_for_test(
                "noop-operational-row-writes",
                stage
                    .operational_write_set
                    .relational_deletes
                    .len()
                    .saturating_add(stage.operational_write_set.relational_inserts.len())
                    as u64,
            );
        }
        for (key, value) in
            original_schema_config_values(&stage.config_values, schema_config, config_cut)?
        {
            frame.plan_config(key, value)?;
        }
        let group_plan = custody_groups::GroupPlan::prepaid(
            original_schema_config_values(&stage.config_values, schema_config, config_cut)?
                .chain(&ws.config_writes),
        )?;
        let mut schema = if stage.table_meta.is_empty() {
            None
        } else {
            self.prepare_writer_schema_change(
                std::iter::empty(),
                stage
                    .table_meta
                    .iter()
                    .map(|(name, meta)| (name.as_str(), meta)),
            )?
        };
        self.with_db(|db| {
            #[cfg(any(test, feature = "test-seams"))]
            let begin_started = std::time::Instant::now();
            // Unjoined terminal: this flush still returns bare Error;
            // take_or_begin_funded cannot be flattened at this internal call.
            let write_txn =
                received_working::ReceivedWorkingWrite::take_or_begin(self, db, original, stage)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let graph_commit_support =
                Self::capture_graph_commit_support(&write_txn, ws.commit_lsn)?;
            self.graph_writer_window(|| {
                Self::apply_received_row_graph_parts_in_write(
                    &write_txn,
                    &stage.row_adj_inserts,
                    &stage.row_adj_deletes,
                    &stage.dropped_row_graph,
                )?;
                self.retire_received_shape_losses_in_write(&write_txn, stage, None)
            })?;
            #[cfg(any(test, feature = "test-seams"))]
            crate::database::record_received_schema_persistence_profile_for_test(
                "noop-begin-write",
                begin_started,
            );
            let (operational_index_identity_mutations, _operational_commitment_credits) = self
                .flush_received_schema_operational_rows_in_write_original(
                    &frame, &write_txn, stage,
                )?;
            if !stage.table_meta.is_empty() {
                let mut metadata = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                for (table, meta) in &stage.table_meta {
                    let encoded = self.schema_update_in_table(
                        &metadata,
                        table,
                        meta,
                        stage.ddl_log.iter().map(|(_, ddl)| ddl),
                    )?;
                    metadata
                        .insert(Self::meta_key(table).as_str(), encoded.bytes.as_slice())
                        .map_err(Self::storage_error)?;
                    Self::raise_table_read_lsn_in_write(
                        &write_txn,
                        table,
                        ws.commit_lsn.unwrap_or(Lsn(0)),
                    )?;
                }
            }
            if !stage.ddl_log.is_empty() {
                let mut ddl = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let mut grouped = BTreeMap::<Lsn, Vec<&DdlChange>>::new();
                for (lsn, change) in &stage.ddl_log {
                    grouped.entry(*lsn).or_default().push(change);
                }
                for (lsn, changes) in grouped {
                    for (index, change) in changes.iter().enumerate() {
                        let key = Self::ddl_log_key_for_index(lsn, index, changes.len());
                        self.append_ddl_summary_in_write(&write_txn, &key, change)?;
                        let encoded = frame.encode(*change)?;
                        ddl.insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }
            }
            let mut committed_config_values = Vec::new();
            {
                let mut config = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for key in &ws.config_deletes {
                    Self::update_policy_hold_release_index(&write_txn, key, false)?;
                    config.remove(key.as_str()).map_err(Self::storage_error)?;
                }
                for (key, value) in
                    original_schema_config_values(&stage.config_values, schema_config, config_cut)?
                        .chain(&ws.config_writes)
                {
                    Self::raise_config_identity_floors_in_write(&write_txn, key, value)?;
                    Self::update_policy_hold_release_index(&write_txn, key, true)?;
                    let value = if ws.config_max_u64_keys.iter().any(|max_key| max_key == key) {
                        let incoming = frame.decode::<u64>(value)?;
                        let current = config
                            .get(key.as_str())
                            .map_err(Self::storage_error)?
                            .map(|value| frame.decode::<u64>(value.value()))
                            .transpose()?
                            .unwrap_or(0);
                        frame.encode(&current.max(incoming))?
                    } else {
                        value.clone()
                    };
                    config
                        .insert(key.as_str(), value.as_slice())
                        .map_err(Self::storage_error)?;
                    committed_config_values.push((key.clone(), value));
                }
            }
            if let (Some(lsn), Some(tx)) = (ws.commit_lsn, ws.visibility_floor) {
                let mut index = write_txn
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                index.insert(lsn.0, tx.0).map_err(Self::storage_error)?;
            }
            Self::stage_graph_commit_support(&write_txn, graph_commit_support)?;
            if !change_log.is_empty() {
                let mut log = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                let mut key = String::with_capacity(Self::change_log_entry_key_len());
                let mut encoded = Vec::new();
                for (index, entry) in change_log.iter().enumerate() {
                    Self::write_change_log_entry_key(lsn, index, &mut key);
                    self.encode_change_log_into_in_write(&write_txn, entry, &mut encoded)?;
                    log.insert(
                        observed_graph_key!("history", "insert", key.as_str(), 8),
                        encoded.as_slice(),
                    )
                    .map_err(Self::storage_error)?;
                    self.graph_writer_window(|| {
                        Self::write_change_log_selectors_in_write(
                            &write_txn,
                            key.as_str(),
                            encoded.as_slice(),
                        )
                    })?;
                }
            }
            let mut identity_mutations =
                OriginalMutationVector::from_vec(&frame, operational_index_identity_mutations);
            for (table, meta) in &stage.table_meta {
                identity_mutations.extend(
                    content_identity::original_producers::schema_mutations(&frame, table, meta)?,
                )?;
            }
            for key in &ws.config_deletes {
                identity_mutations.extend(
                    content_identity::original_producers::config_mutations(&frame, key, None)?,
                )?;
            }
            for (key, value) in &committed_config_values {
                identity_mutations.extend(
                    content_identity::original_producers::config_mutations(
                        &frame,
                        key,
                        Some(value),
                    )?,
                )?;
            }
            let mut grouped_ddl = BTreeMap::<Lsn, Vec<&DdlChange>>::new();
            for (ddl_lsn, change) in &stage.ddl_log {
                grouped_ddl.entry(*ddl_lsn).or_default().push(change);
            }
            for (ddl_lsn, changes) in grouped_ddl {
                for (index, change) in changes.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(ddl_lsn, index, changes.len());
                    identity_mutations.push_at(
                        "received_noop_ddl_history",
                        content_identity::original_producers::encoded_history_mutation(
                            &frame, &key, *change,
                        )?,
                    )?;
                }
            }
            for (table, row_id, _) in &stage.operational_write_set.relational_deletes {
                identity_mutations.push(
                    content_identity::original_producers::remove_row_mutation(
                        &frame, table, *row_id,
                    )?,
                )?;
            }
            for (table, row) in &stage.operational_write_set.relational_inserts {
                identity_mutations.push(content_identity::original_producers::row_mutation(
                    &frame,
                    table,
                    row,
                    stage.operational_table_meta.get(table),
                )?)?;
            }
            let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
            let mut change_key = String::with_capacity(Self::change_log_entry_key_len());
            for (index, _entry) in change_log.iter().enumerate() {
                Self::write_change_log_entry_key(lsn, index, &mut change_key);
                identity_mutations.push(Self::stored_change_log_mutation_in_write(
                    &write_txn,
                    &change_key,
                )?)?;
            }
            if let (Some(commit_lsn), Some(tx)) = (ws.commit_lsn, ws.visibility_floor) {
                identity_mutations.push(
                    content_identity::original_producers::commit_index_mutation(
                        &frame, commit_lsn, tx,
                    )?,
                )?;
            }
            write_txn.discard_scratch()?;
            if !identity_mutations.is_empty() {
                Self::apply_owned_content_identity_with_manifests_and_groups_in_write(
                    &write_txn,
                    &identity_mutations,
                    &[],
                    &mut content_identity::original_writer::ProofAdmission::new(
                        frame.original,
                        self.proof_scratch_owner(),
                    )?,
                    Some(&group_plan),
                )?;
            }
            if take_received_schema_pre_commit_fault_for_test() {
                return Err(Error::Other(
                    ("injected received-schema Redb pre-commit failure".to_string()).into(),
                ));
            }
            #[cfg(any(test, feature = "test-seams"))]
            let durable_commit_started = std::time::Instant::now();
            self.preserve_graph_commit_support(
                &write_txn,
                graph_commit_support,
                ws.commit_lsn.zip(ws.visibility_floor),
            )?;
            // The grouping that decides the retention clocks is admitted first.
            let _clock_grouping = crate::memory_accounting::reserve_preparation_from_original(
                frame.original,
                Self::retention_clock_grouping_bytes(ws),
                "writer_source",
                "retention_clock_grouping",
                "Raise MEMORY_LIMIT to decide this commit's retention clocks.",
            )?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, Some(ws))?;
            {
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10)?;
                let graph_commit_result = write_txn.commit();
                if graph_commit_result.is_ok() {
                    self.complete_writer_schema_change(schema.take());
                }
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)?;
            #[cfg(any(test, feature = "test-seams"))]
            crate::database::record_received_schema_persistence_profile_for_test(
                "noop-durable-commit",
                durable_commit_started,
            );
            Ok(())
        })?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::database::record_received_schema_persistence_profile_for_test(
            "noop-flush-total",
            flush_started,
        );
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn flush_owned_local_schema_stage(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        ws: &WriteSet,
        stage: &LocalSchemaPersistenceProjection,
        schema_config: Option<&[(String, Vec<u8>)]>,
        config_cut: usize,
    ) -> Result<()> {
        let frame = OriginalPersistenceFrame::new(original)?;
        frame.plan_write_set(ws, &[], &[])?;
        for meta in stage.affected_table_meta.values() {
            frame.record(meta)?;
        }

        let lsn = ws.commit_lsn.ok_or_else(|| {
            Error::Other(("local schema stage is missing its commit LSN".to_string()).into())
        })?;
        if stage.ddl.is_empty() {
            return Err(Error::Other(
                ("local schema stage contains no DDL".to_string()).into(),
            ));
        }
        let schema = self.prepare_writer_schema_change(
            std::iter::empty(),
            stage
                .affected_table_meta
                .iter()
                .map(|(name, meta)| (name.as_str(), meta)),
        )?;
        for (key, value) in
            original_schema_config_values(&stage.config_values, schema_config, config_cut)?
        {
            frame.plan_config(key, value)?;
        }
        let group_plan = custody_groups::GroupPlan::prepaid(
            original_schema_config_values(&stage.config_values, schema_config, config_cut)?
                .chain(&ws.config_writes),
        )?;
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            let graph_commit_support = Self::capture_graph_commit_support(&write_txn, Some(lsn))?;

            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                for (table, meta) in &stage.affected_table_meta {
                    let key = Self::meta_key(table);
                    let encoded_schema =
                        self.schema_update_in_table(&meta_table, table, meta, &stage.ddl)?;
                    let encoded = &encoded_schema.bytes;
                    meta_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    vector_membership_pages::ensure_declared_routes(
                        &write_txn,
                        table,
                        meta,
                        Some(
                            crate::memory_accounting::persistence_original_projection(
                                frame.original,
                            )?
                            .as_handle(),
                        ),
                    )?;
                }
            }

            // `open_table` is both the Redb table creation path and a no-op
            // for an existing relation. Do not delete, scan, or rewrite rows:
            // another committed local table must remain byte-for-byte intact.
            for table in stage.affected_table_meta.keys() {
                let table_name = Self::rel_table_name(table);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let _relation = content_size::open_rows(
                    &write_txn,
                    table_def,
                    stage.affected_table_meta.get(table),
                )
                .map_err(Self::storage_error)?;
            }
            for (table, meta) in &stage.affected_table_meta {
                Self::rebuild_relational_indexes_in_write_original(
                    &frame,
                    &write_txn,
                    &self.path,
                    table,
                    meta,
                    None,
                    &mut content_identity::original_writer::ProofAdmission::new(
                        frame.original,
                        self.proof_scratch_owner(),
                    )?,
                    None,
                )?;
            }

            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for key in &ws.config_deletes {
                    Self::update_policy_hold_release_index(&write_txn, key, false)?;
                    config_table
                        .remove(key.as_str())
                        .map_err(Self::storage_error)?;
                }
                for (key, encoded) in
                    original_schema_config_values(&stage.config_values, schema_config, config_cut)?
                        .chain(&ws.config_writes)
                {
                    Self::raise_config_identity_floors_in_write(&write_txn, key, encoded)?;
                    Self::update_policy_hold_release_index(&write_txn, key, true)?;
                    config_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                for (index, change) in stage.ddl.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(lsn, index, stage.ddl.len());
                    self.append_ddl_summary_in_write(&write_txn, &key, change)?;
                    let encoded = frame.encode(change)?;
                    ddl_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            {
                let mut index = write_txn
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                index
                    .insert(lsn.0, stage.commit_index_tx.0)
                    .map_err(Self::storage_error)?;
            }

            let mut identity_mutations = OriginalMutationVector::new(&frame);
            for (table, meta) in &stage.affected_table_meta {
                identity_mutations.extend(
                    content_identity::original_producers::schema_mutations(&frame, table, meta)?,
                )?;
            }
            for key in &ws.config_deletes {
                identity_mutations.extend(
                    content_identity::original_producers::config_mutations(&frame, key, None)?,
                )?;
            }
            for (key, encoded) in
                original_schema_config_values(&stage.config_values, schema_config, config_cut)?
                    .chain(&ws.config_writes)
            {
                Self::raise_config_identity_floors_in_write(&write_txn, key, encoded)?;
                identity_mutations.extend(
                    content_identity::original_producers::config_mutations(
                        &frame,
                        key,
                        Some(encoded),
                    )?,
                )?;
            }
            for (index, change) in stage.ddl.iter().enumerate() {
                let key = Self::ddl_log_key_for_index(lsn, index, stage.ddl.len());
                identity_mutations.push(
                    content_identity::original_producers::encoded_history_mutation(
                        &frame, &key, change,
                    )?,
                )?;
            }
            identity_mutations.push(
                content_identity::original_producers::commit_index_mutation(
                    &frame,
                    lsn,
                    stage.commit_index_tx,
                )?,
            )?;
            Self::apply_owned_content_identity_with_manifests_and_groups_in_write(
                &write_txn,
                &identity_mutations,
                &[],
                &mut content_identity::original_writer::ProofAdmission::new(
                    frame.original,
                    self.proof_scratch_owner(),
                )?,
                Some(&group_plan),
            )?;

            if take_received_schema_pre_commit_fault_for_test() {
                return Err(Error::Other(
                    ("injected received-schema Redb pre-commit failure".to_string()).into(),
                ));
            }
            self.preserve_graph_commit_support(
                &write_txn,
                graph_commit_support,
                Some((lsn, stage.commit_index_tx)),
            )?;
            // The grouping that decides the retention clocks is admitted first.
            let _clock_grouping = crate::memory_accounting::reserve_preparation_from_original(
                frame.original,
                Self::retention_clock_grouping_bytes(ws),
                "writer_source",
                "retention_clock_grouping",
                "Raise MEMORY_LIMIT to decide this commit's retention clocks.",
            )?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, Some(ws))?;
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
            self.complete_writer_schema_change(schema);
            Ok(())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn retire_received_schema_generation_in_write_original<'f, 'o, 'w>(
        &self,
        frame: &'f OriginalPersistenceFrame<'o>,
        write_txn: &'w redb::WriteTransaction,
        stage: &ReceivedSchemaPersistenceProjection,
        commitments: &mut historical_index_commitment::CommitmentBatch,
        working: Option<&received_working::ReceivedWorkingWrite>,
    ) -> Result<schema_projection::OriginalMutationLog<'f, 'o, 'w>> {
        let mut mutations = schema_projection::OriginalMutationLog::new(self, frame, write_txn)?;
        frame.array::<&String>(stage.affected_tables.len())?;
        let mut affected = stage.affected_tables.iter().collect::<Vec<_>>();
        affected.sort();

        for table in &affected {
            let table_window = OriginalPersistenceFrame::new(frame.original)?;
            table_window.reserve(table.len().saturating_mul(4).saturating_add(512))?;
            // A newly declared table has no retired generation. Opening its
            // missing relation here would create a table only to delete it
            // before writing the replacement image.
            let exists = {
                let metadata = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                metadata
                    .get(Self::meta_key(table).as_str())
                    .map_err(Self::storage_error)?
                    .is_some()
            };
            if !exists {
                continue;
            }
            let table_name = Self::rel_table_name(table);
            let table_def: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(table_name.as_str());
            match write_txn.open_table(table_def) {
                Ok(relation) => drop(relation),
                Err(redb::TableError::TableTypeMismatch { .. }) => continue,
                Err(error) => return Err(Self::storage_error(error)),
            }
            let mut previous_row_id = None;
            self.visit_relational_rows_in_write_original(
                frame,
                write_txn,
                table,
                |row, window| {
                    if previous_row_id != Some(row.row_id) {
                        mutations.push(
                            content_identity::original_producers::remove_row_mutation(
                                window, table, row.row_id,
                            )?,
                        )?;
                        previous_row_id = Some(row.row_id);
                    }
                    Ok(())
                },
            )?;
        }

        for table in &affected {
            let meta_frame = OriginalPersistenceFrame::new(frame.original)?;
            meta_frame.reserve(table.len().saturating_mul(4).saturating_add(512))?;
            let old = {
                let metadata = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::meta_key(table);
                let Some(encoded) = metadata.get(key.as_str()).map_err(Self::storage_error)? else {
                    continue;
                };
                Self::decode_table_meta_versioned_original(&meta_frame, encoded.value())?.0
            };
            let old = &old;
            let replacement = stage.table_meta.get(*table);
            let kept = replacement
                .map(|meta| {
                    Self::relational_index_declarations(meta)
                        .into_iter()
                        .map(|declaration| declaration.name)
                        .collect::<HashSet<_>>()
                })
                .unwrap_or_default();
            for declaration in Self::relational_index_declarations(old) {
                if kept.contains(&declaration.name) {
                    // The rebuild of the surviving declaration retires and
                    // replaces its postings itself.
                    continue;
                }
                commitments.drop_index(table, &declaration.name)?;
                let name = Self::relational_index_table_name(table, &declaration.name);
                let definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                    TableDefinition::new(name.as_str());
                if let Ok(existing) = write_txn.open_table(definition) {
                    for entry in existing.iter().map_err(Self::storage_error)? {
                        let local_frame = OriginalPersistenceFrame::new(frame.original)?;
                        let frame = &local_frame;
                        let decoded = decode_relational_index_entry_original(
                            frame,
                            &self.path,
                            Some(entry.map_err(Self::storage_error)?),
                        )?
                        .expect("an existing index iterator entry decodes");
                        let (_, position, deleted_tx) = decoded;
                        if deleted_tx.is_some() {
                            continue;
                        }
                        frame.array::<(bool, Value)>(position.key.len())?;
                        let logical_key = DurableRelationalIndexKey(frame.encode(
                            &EncodedRelationalIndexPosition::from(RelationalIndexPosition {
                                key: position.key,
                                row_id: position.row_id,
                                created_tx: TxId(0),
                            }),
                        )?);
                        mutations.push(content_identity::original_producers::remove_relational_index_posting_mutation(frame,
                            table,
                            &declaration.name,
                            &logical_key.0,
                        )?)?;
                    }
                }
                match write_txn.delete_table(definition) {
                    Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(error) => return Err(Self::storage_error(error)),
                }
                let current_name =
                    Self::current_relational_index_table_name(table, &declaration.name);
                let current_definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                    TableDefinition::new(current_name.as_str());
                write_txn
                    .delete_table(current_definition)
                    .map_err(Self::storage_error)?;
            }
            if replacement.is_some() {
                continue;
            }
            // A dropped table keeps no maintained counts, no readiness and no
            // retention expiries.
            self.erase_received_index_counts_original(frame, write_txn, table, |mutation| {
                mutations.push(mutation)
            })?;
            if write_txn
                .open_table(RELATIONAL_INDEX_READY_TABLE)
                .map_err(Self::storage_error)?
                .remove(table.as_str())
                .map_err(Self::storage_error)?
                .is_some()
            {
                mutations.push(
                    content_identity::original_producers::remove_status_mutation(
                        frame,
                        RELATIONAL_INDEX_COUNT_READY_IDENTITY_NAMESPACE,
                        table.as_bytes(),
                    )?,
                )?;
            }
            self.erase_received_retention_expiries_original(
                frame,
                write_txn,
                table,
                &mut mutations,
            )?;
        }

        for table in &affected {
            self.retire_table_source_provenance_in_write_original(
                frame,
                write_txn,
                table,
                false,
                true,
                Some(&mut mutations),
            )?;
        }

        if stage.replace_vectors {
            match content_size::open_vectors(write_txn) {
                Ok(vectors) => {
                    for item in vectors.iter().map_err(Self::storage_error)? {
                        let local_frame = OriginalPersistenceFrame::new(frame.original)?;
                        let frame = &local_frame;
                        let (_, value) = item.map_err(Self::storage_error)?;
                        let entry: PersistedVectorEntry = frame.decode(value.value())?;
                        if !stage.affected_tables.contains(&entry.index.table) {
                            continue;
                        }
                        let quantization = entry.vector.quantization();
                        mutations.push(
                            content_identity::original_producers::remove_vector_version_mutation(
                                frame,
                                &entry.index,
                                entry.row_id,
                                entry.created_tx,
                                entry.lsn,
                                quantization,
                            )?,
                        )?;
                        mutations.push(
                            content_identity::original_producers::remove_vector_mutation(
                                frame,
                                &entry.index,
                                entry.row_id,
                                quantization,
                            )?,
                        )?;
                    }
                }
                Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(error) => return Err(Self::storage_error(error)),
            }
        }

        if stage.replace_graph {
            let forward = write_txn
                .open_table(GRAPH_FWD_TABLE)
                .map_err(Self::storage_error)?;
            for item in forward
                .iter()
                .map_err(Self::storage_error)?
                .inspect(|_entry| {
                    #[cfg(any(test, feature = "test-seams"))]
                    if let Ok((key, _)) = _entry {
                        crate::read_work_probe::graph_storage_key(
                            "graph_fwd",
                            "iteration_step",
                            key.value(),
                        );
                    }
                })
            {
                let local_frame = OriginalPersistenceFrame::new(frame.original)?;
                let frame = &local_frame;
                let (_key, value) = item.map_err(Self::storage_error)?;
                #[cfg(any(test, feature = "test-seams"))]
                crate::read_work_probe::note_graph(
                    "graph_payload",
                    "payload_decode",
                    _key.value(),
                    None,
                    Some("received_schema_identity_preparation"),
                );
                let edge = frame.decode::<AdjEntry>(value.value())?;
                let selected = if let Some(selection) = &stage.graph_selection {
                    working
                        .ok_or_else(|| {
                            Error::Other("graph identity retirement lost its frozen write".into())
                        })?
                        .selected_graph_type_original(frame.original, selection, &edge.edge_type)?
                } else {
                    stage.affected_graph_edge_types.contains(&edge.edge_type)
                };
                if selected {
                    mutations.push(content_identity::original_producers::remove_edge_mutation(
                        frame,
                        edge.source,
                        &edge.edge_type,
                        edge.target,
                    )?)?;
                }
            }
        }
        Ok(mutations)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn flush_received_schema_operational_rows_in_write_original(
        &self,
        frame: &OriginalPersistenceFrame<'_>,
        write_txn: &redb::WriteTransaction,
        stage: &ReceivedSchemaPersistenceProjection,
    ) -> Result<(
        Vec<content_identity::ContentIdentityMutation>,
        Vec<historical_index_commitment::CommitmentCredit>,
    )> {
        let ws = &stage.operational_write_set;
        // Postings and the row versions they project are committed to
        // separately and must agree: see `historical_index_commitment`.
        let mut commitments =
            historical_index_commitment::CommitmentBatch::for_original(frame.original)?;
        for (table, row_id, deleted_tx) in &ws.relational_deletes {
            let old = stage
                .operational_deleted_rows
                .get(table)
                .and_then(|rows| rows.get(row_id))
                .ok_or_else(|| {
                    Error::Other(
                        (format!(
                            "received-schema operational update is missing row {} from {table}",
                            row_id.0
                        ))
                        .into(),
                    )
                })?;
            let mut deleted = old.clone();
            deleted.deleted_tx = Some(*deleted_tx);
            let table_name = Self::rel_table_name(table);
            let definition: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(table_name.as_str());
            let mut relation = content_size::open_rows(
                write_txn,
                definition,
                stage.operational_table_meta.get(table),
            )
            .map_err(Self::storage_error)?;
            self.verify_relational_group_in_write_original(
                frame.original,
                write_txn,
                table,
                *row_id,
                &*relation,
            )?;
            let key = Self::rel_row_key(&deleted);
            let encoded = Self::encode_versioned_row_original(
                frame,
                &deleted,
                stage.operational_table_meta.get(table),
            )?;
            {
                let predecessor = relation.get(key.as_slice()).map_err(Self::storage_error)?;
                self.replace_relational_manifest_in_write_original(
                    frame.original,
                    write_txn,
                    table,
                    key.as_slice(),
                    predecessor.as_ref().map(|body| body.value()),
                    Some(encoded.as_slice()),
                )?;
            }
            let keys = content_size::VersionLife::keys_of(relation.indexes(), &deleted)?;
            relation.insert_version(
                &mut commitments,
                key.as_slice(),
                encoded.as_slice(),
                &content_size::VersionLife {
                    keys: &keys,
                    created: deleted.created_tx,
                    deleted: deleted.deleted_tx,
                },
            )?;
        }
        for (table, row) in &ws.relational_inserts {
            let table_name = Self::rel_table_name(table);
            let definition: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(table_name.as_str());
            let mut relation = content_size::open_rows(
                write_txn,
                definition,
                stage.operational_table_meta.get(table),
            )
            .map_err(Self::storage_error)?;
            let key = Self::rel_row_key(row);
            let encoded = Self::encode_versioned_row_original(
                frame,
                row,
                stage.operational_table_meta.get(table),
            )?;
            {
                let predecessor = relation.get(key.as_slice()).map_err(Self::storage_error)?;
                self.replace_relational_manifest_in_write_original(
                    frame.original,
                    write_txn,
                    table,
                    key.as_slice(),
                    predecessor.as_ref().map(|body| body.value()),
                    Some(encoded.as_slice()),
                )?;
            }
            let keys = content_size::VersionLife::keys_of(relation.indexes(), row)?;
            relation.insert_version(
                &mut commitments,
                key.as_slice(),
                encoded.as_slice(),
                &content_size::VersionLife {
                    keys: &keys,
                    created: row.created_tx,
                    deleted: row.deleted_tx,
                },
            )?;
        }
        // Declared first, so dropped last: these charges outlive the mutations they pay for.
        let mut mutations_commitment_credits: Vec<historical_index_commitment::CommitmentCredit> =
            Vec::new();
        let mut mutations = Self::flush_relational_index_changes_in_write_original(
            frame,
            write_txn,
            ws,
            &stage.operational_table_meta,
            Some(&stage.operational_deleted_rows),
            &mut commitments,
        )?;
        let (published, credit) = commitments.finish(write_txn)?.into_parts();
        mutations_commitment_credits.push(credit);
        mutations.extend(published);
        // The credits travel with the mutations they pay for.
        Ok((mutations, mutations_commitment_credits))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn encode_versioned_row_original(
        frame: &OriginalPersistenceFrame<'_>,
        row: &VersionedRow,
        meta: Option<&TableMeta>,
    ) -> Result<Vec<u8>> {
        let mut encoded = Vec::new();
        Self::encode_versioned_row_into_original(frame, row, meta, &mut encoded)?;
        Ok(encoded)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn encode_versioned_row_into_original(
        frame: &OriginalPersistenceFrame<'_>,
        row: &VersionedRow,
        meta: Option<&TableMeta>,
        encoded: &mut Vec<u8>,
    ) -> Result<()> {
        Self::encode_versioned_row_with_deleted_tx_into_original(
            frame,
            row,
            row.deleted_tx,
            meta,
            encoded,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn encode_versioned_row_with_deleted_tx_into_original(
        frame: &OriginalPersistenceFrame<'_>,
        row: &VersionedRow,
        deleted_tx: Option<TxId>,
        meta: Option<&TableMeta>,
        encoded: &mut Vec<u8>,
    ) -> Result<()> {
        Self::encode_versioned_row_with_f32_overlay_into_original(
            frame, row, deleted_tx, meta, None, encoded,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn encode_versioned_row_with_f32_overlay_into_original(
        frame: &OriginalPersistenceFrame<'_>,
        row: &VersionedRow,
        deleted_tx: Option<TxId>,
        meta: Option<&TableMeta>,
        f32_overlay: Option<&F32RowOverlay<'_>>,
        encoded: &mut Vec<u8>,
    ) -> Result<()> {
        let _json_scratch = frame.json_scratch(row.values.values())?;
        frame.record(row)?;
        let (_, json_copies) =
            crate::memory_accounting::preparation_json_work_bytes(row.values.values())?;
        frame.reserve(json_copies)?;
        frame.map::<String, PersistedValue>(row.values.len())?;
        if let Some(vectors) = f32_overlay {
            for (name, value) in vectors {
                frame.reserve(name.len())?;
                frame.array::<f32>(value.len())?;
            }
        }
        let values = row
            .values
            .iter()
            .map(|(column, value)| {
                let persisted = match value {
                    Value::Vector(vector) => {
                        let quantization = Self::column_quantization(meta, column);
                        if matches!(quantization, VectorQuantization::F32) {
                            PersistedValue::Vector(PersistedVector::from_f32(vector, quantization))
                        } else {
                            PersistedValue::Plain(Value::Null)
                        }
                    }
                    Value::Null
                        if f32_overlay
                            .and_then(|vectors| vectors.get(column))
                            .is_some()
                            && matches!(
                                Self::column_quantization(meta, column),
                                VectorQuantization::F32
                            ) =>
                    {
                        PersistedValue::Vector(PersistedVector::from_f32(
                            f32_overlay
                                .and_then(|vectors| vectors.get(column))
                                .expect("checked F32 row-vector overlay"),
                            VectorQuantization::F32,
                        ))
                    }
                    _ => PersistedValue::Plain(crate::memory_accounting::preparation_copy_value(
                        value,
                    )),
                };
                (column.clone(), persisted)
            })
            .collect::<HashMap<_, _>>();
        Self::encode_into_original(
            frame,
            &PersistedVersionedRow {
                row_id: row.row_id,
                values,
                created_tx: row.created_tx,
                deleted_tx,
                lsn: row.lsn,
                created_at: row.created_at,
            },
            encoded,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn f32_vectors_from_row_original<'a>(
        frame: &OriginalPersistenceFrame<'_>,
        row: &VersionedRow,
        meta: Option<&TableMeta>,
    ) -> Result<F32RowOverlay<'a>> {
        let _json_scratch = frame.json_scratch(row.values.values())?;
        frame.record(row)?;
        frame.map::<String, std::borrow::Cow<'_, [f32]>>(row.values.len())?;
        let mut vectors = HashMap::with_capacity(row.values.len());
        for (column, value) in &row.values {
            match value {
                Value::Vector(vector)
                    if matches!(
                        Self::column_quantization(meta, column),
                        VectorQuantization::F32
                    ) =>
                {
                    vectors.insert(column.clone(), std::borrow::Cow::Owned(vector.clone()));
                }
                _ => {}
            }
        }
        Ok(vectors)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn decode_versioned_row_original(
        frame: &OriginalPersistenceFrame<'_>,
        bytes: &[u8],
        _meta: Option<&TableMeta>,
    ) -> Result<VersionedRow> {
        let mut decoded = original_event_decode::decode_row(frame.original, bytes)?;
        frame.retain(&mut decoded.allocation)?;
        let persisted = decoded.value;
        frame.reserve(
            persisted
                .values
                .iter()
                .map(|(column, value)| {
                    column
                        .len()
                        .saturating_add(std::mem::size_of::<(String, Value)>().saturating_mul(4))
                        .saturating_add(match value {
                            PersistedValue::Vector(vector) => vector.decoded_bytes(),
                            _ => 0,
                        })
                })
                .sum(),
        )?;
        frame.map::<String, Value>(persisted.values.len())?;
        let values = persisted
            .values
            .into_iter()
            .map(|(column, value)| {
                let value = match value {
                    PersistedValue::Plain(value) => value,
                    PersistedValue::Vector(vector) => Value::Vector(vector.to_f32()),
                };
                (column, value)
            })
            .collect::<HashMap<_, _>>();
        Ok(VersionedRow {
            row_id: persisted.row_id,
            values,
            created_tx: persisted.created_tx,
            deleted_tx: persisted.deleted_tx,
            lsn: persisted.lsn,
            created_at: persisted.created_at,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn encode_vector_entry_original(
        frame: &OriginalPersistenceFrame<'_>,
        entry: &VectorEntry,
        quantization: VectorQuantization,
    ) -> Result<Vec<u8>> {
        frame.reserve(
            entry
                .index
                .table
                .len()
                .saturating_add(entry.index.column.len()),
        )?;
        frame.array::<f32>(entry.vector.len())?;
        frame.encode(&PersistedVectorEntry {
            index: entry.index.clone(),
            row_id: entry.row_id,
            vector: PersistedVector::from_f32(&entry.vector, quantization),
            created_tx: entry.created_tx,
            deleted_tx: entry.deleted_tx,
            lsn: entry.lsn,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn decode_vector_entry_original(
        frame: &OriginalPersistenceFrame<'_>,
        bytes: &[u8],
    ) -> Result<VectorEntry> {
        let persisted: PersistedVectorEntry = frame.decode(bytes)?;
        frame.reserve(persisted.vector.decoded_bytes())?;
        Ok(VectorEntry {
            index: persisted.index,
            row_id: persisted.row_id,
            vector: persisted.vector.to_f32(),
            created_tx: persisted.created_tx,
            deleted_tx: persisted.deleted_tx,
            lsn: persisted.lsn,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn flush_relational_index_changes_in_write_original(
        frame: &OriginalPersistenceFrame<'_>,
        write_txn: &redb::WriteTransaction,
        ws: &WriteSet,
        table_meta: &HashMap<String, TableMeta>,
        deleted_rows: Option<&HashMap<String, HashMap<RowId, VersionedRow>>>,
        commitments: &mut historical_index_commitment::CommitmentBatch,
    ) -> Result<Vec<content_identity::ContentIdentityMutation>> {
        let inserted = ws
            .relational_inserts
            .iter()
            .map(|(table, row)| ((table.as_str(), row.row_id), row))
            .collect::<HashMap<_, _>>();
        let mut count_changes = BTreeMap::<Vec<u8>, (BTreeSet<RowId>, BTreeSet<RowId>)>::new();
        let mut identity_mutations = OriginalMutationVector::new(frame);
        let affected = ws
            .relational_inserts
            .iter()
            .map(|(table, _)| table)
            .chain(ws.relational_deletes.iter().map(|(table, _, _)| table))
            .collect::<BTreeSet<_>>();
        for table in affected {
            let Some(meta) = table_meta.get(table) else {
                continue;
            };
            let maintain_counts = {
                let ready = write_txn
                    .open_table(RELATIONAL_INDEX_READY_TABLE)
                    .map_err(Self::storage_error)?;
                ready
                    .get(table.as_str())
                    .map_err(Self::storage_error)?
                    .is_some_and(|version| version.value() == RELATIONAL_INDEX_READY_VERSION)
            };
            for declaration in Self::relational_index_declarations(meta) {
                let name = Self::relational_index_table_name(table, &declaration.name);
                let mut index = historical_index_commitment::HistoricalPostings::open(
                    write_txn,
                    table,
                    &declaration,
                )
                .map_err(Self::storage_error)?;
                let current_name =
                    Self::current_relational_index_table_name(table, &declaration.name);
                let current_definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                    TableDefinition::new(current_name.as_str());
                let mut current = write_txn
                    .open_table(current_definition)
                    .map_err(Self::storage_error)?;
                for (deleted_table, row_id, deleted_tx) in &ws.relational_deletes {
                    if deleted_table != table {
                        continue;
                    }
                    // A staged delete can reach commit after another writer
                    // has already deleted the same row. There is then no live
                    // version to take the posting's key from, and no posting
                    // to retire: the delete is the commit no-op it is in
                    // memory, here too.
                    let Some(old) = deleted_rows
                        .and_then(|tables| tables.get(table))
                        .and_then(|rows| rows.get(row_id))
                    else {
                        continue;
                    };
                    let old_key =
                        contextdb_relational::index_key_for_row(&declaration.columns, &old.values);
                    let replacement_keeps_key =
                        inserted.get(&(table.as_str(), *row_id)).is_some_and(|new| {
                            contextdb_relational::index_key_for_row(
                                &declaration.columns,
                                &new.values,
                            ) == old_key
                        });
                    if replacement_keeps_key {
                        continue;
                    }
                    if maintain_counts {
                        for prefix_len in 1..=old_key.len() {
                            // Exactly the declared members, and nothing else:
                            // silence maintains none.
                            if !crate::maintained_counts::maintained_count_member(
                                meta,
                                &declaration.columns[..prefix_len],
                            ) {
                                continue;
                            }
                            let count_key = Self::relational_index_count_key(
                                table,
                                &declaration.columns[..prefix_len],
                                &old_key[..prefix_len],
                            )?;
                            #[cfg(any(test, feature = "test-seams"))]
                            observe_index_prefix_count(
                                table,
                                meta,
                                &declaration,
                                prefix_len,
                                &count_key,
                            );
                            let changes = count_changes.entry(count_key).or_default();
                            changes.1.insert(*row_id);
                        }
                    }
                    // An update that leaves these indexed columns unchanged
                    // keeps the original posting. Its creating transaction
                    // can therefore precede the current row version's.
                    let logical_key =
                        DurableRelationalIndexKey::new(old_key.clone(), old.row_id, TxId(0))?;
                    let created_tx: TxId = current
                        .get(&logical_key)
                        .map_err(Self::storage_error)?
                        .map(|value| Self::decode_exact(value.value()))
                        .transpose()?
                        .ok_or_else(|| {
                            crate::funded_error::corruption_text(
                                crate::funded_error::original_budget(frame.original),
                                &current_name,
                                "current index has no posting for a deleted row".into(),
                            )
                        })?;
                    let key = DurableRelationalIndexKey::new(old_key, old.row_id, created_tx)?;
                    let stamp: Option<TxId> = index
                        .get(&key)
                        .map_err(Self::storage_error)?
                        .map(|value| Self::decode_exact(value.value()))
                        .transpose()?
                        .ok_or_else(|| {
                            crate::funded_error::corruption_text(
                                crate::funded_error::original_budget(frame.original),
                                &name,
                                "current index names a missing versioned posting".into(),
                            )
                        })?;
                    if stamp.is_some() {
                        return Err(crate::funded_error::corruption_text(
                            crate::funded_error::original_budget(frame.original),
                            &name,
                            "current index names a deleted posting".into(),
                        ));
                    }
                    current.remove(&logical_key).map_err(Self::storage_error)?;
                    index.put(commitments, write_txn, key, Some(*deleted_tx))?;
                    let logical_key = DurableRelationalIndexKey::new(
                        contextdb_relational::index_key_for_row(&declaration.columns, &old.values),
                        old.row_id,
                        TxId(0),
                    )?;
                    identity_mutations.push(
                        content_identity::original_producers::remove_relational_index_posting_mutation(frame,
                            table,
                            &declaration.name,
                            &logical_key.0,
                        )?,
                    )?;
                }
                for (inserted_table, row) in &ws.relational_inserts {
                    if inserted_table != table {
                        continue;
                    }
                    let key =
                        contextdb_relational::index_key_for_row(&declaration.columns, &row.values);
                    let replacement_keeps_key = deleted_rows
                        .and_then(|tables| tables.get(table))
                        .and_then(|rows| rows.get(&row.row_id))
                        .is_some_and(|old| {
                            contextdb_relational::index_key_for_row(
                                &declaration.columns,
                                &old.values,
                            ) == key
                        });
                    if replacement_keeps_key {
                        continue;
                    }
                    if maintain_counts {
                        for prefix_len in 1..=key.len() {
                            if !crate::maintained_counts::maintained_count_member(
                                meta,
                                &declaration.columns[..prefix_len],
                            ) {
                                continue;
                            }
                            let count_key = Self::relational_index_count_key(
                                table,
                                &declaration.columns[..prefix_len],
                                &key[..prefix_len],
                            )?;
                            #[cfg(any(test, feature = "test-seams"))]
                            observe_index_prefix_count(
                                table,
                                meta,
                                &declaration,
                                prefix_len,
                                &count_key,
                            );
                            let changes = count_changes.entry(count_key).or_default();
                            changes.0.insert(row.row_id);
                        }
                    }
                    let key = DurableRelationalIndexKey::new(key, row.row_id, row.created_tx)?;
                    index.put(commitments, write_txn, key, None)?;
                    let logical_key = DurableRelationalIndexKey::new(
                        contextdb_relational::index_key_for_row(&declaration.columns, &row.values),
                        row.row_id,
                        TxId(0),
                    )?;
                    let created = frame.encode(&row.created_tx)?;
                    current
                        .insert(&logical_key, created.as_slice())
                        .map_err(Self::storage_error)?;
                    #[cfg(any(test, feature = "test-seams"))]
                    observe_index_posting(table, &declaration);
                    identity_mutations.push(
                        content_identity::original_producers::relational_index_posting_mutation(
                            frame,
                            table,
                            &declaration.name,
                            &logical_key.0,
                        )?,
                    )?;
                }
            }
        }
        if !count_changes.is_empty() {
            {
                let mut counts = write_txn
                    .open_table(RELATIONAL_INDEX_COUNT_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, (incremented_rows, decremented_rows)) in count_changes {
                    let increments = u64::try_from(incremented_rows.len()).map_err(|_| {
                        Error::Other(
                            ("relational index count increment overflow".to_owned()).into(),
                        )
                    })?;
                    let decrements = u64::try_from(decremented_rows.len()).map_err(|_| {
                        Error::Other(
                            ("relational index count decrement overflow".to_owned()).into(),
                        )
                    })?;
                    let current = counts
                        .get(key.as_slice())
                        .map_err(Self::storage_error)?
                        .map(|count| count.value())
                        .unwrap_or(0);
                    let next = current
                        .checked_sub(decrements)
                        .and_then(|count| count.checked_add(increments))
                        .ok_or_else(|| Error::StoreCorrupted {
                            path: "relational index count table".into(),
                            reason: "live index-prefix cardinality underflowed or overflowed"
                                .into(),
                        })?;
                    #[cfg(any(test, feature = "test-seams"))]
                    observe_index_prefix_count_value(&key, (next > 0).then_some(next));
                    if next == 0 {
                        counts.remove(key.as_slice()).map_err(Self::storage_error)?;
                        identity_mutations
                            .push(Self::remove_relational_index_count_mutation(&key))?;
                    } else {
                        counts
                            .insert(key.as_slice(), next)
                            .map_err(Self::storage_error)?;
                        identity_mutations
                            .push(Self::relational_index_count_mutation(&key, next))?;
                    }
                }
            }
        }
        Ok(identity_mutations.into_vec())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn retire_undeclared_relational_indexes_in_write_original(
        frame: &OriginalPersistenceFrame<'_>,
        write_txn: &redb::WriteTransaction,
        database_path: &Path,
        table: &str,
        previous_meta: &TableMeta,
        meta: &TableMeta,
        admission: &mut content_identity::original_writer::ProofAdmission,
    ) -> Result<()> {
        let still_declared = Self::relational_index_declarations(meta)
            .into_iter()
            .map(|declaration| declaration.name)
            .collect::<HashSet<_>>();
        // Declared first, so dropped last: these charges outlive the mutations they pay for.
        let mut mutations_commitment_credits: Vec<historical_index_commitment::CommitmentCredit> =
            Vec::new();
        let mut mutations = OriginalMutationVector::new(frame);
        // A retired index's commitments go with its postings.
        let mut commitments =
            historical_index_commitment::CommitmentBatch::for_original(frame.original)?;
        for declaration in Self::relational_index_declarations(previous_meta) {
            if still_declared.contains(&declaration.name) {
                continue;
            }
            commitments.drop_index(table, &declaration.name)?;
            let name = Self::relational_index_table_name(table, &declaration.name);
            let definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                TableDefinition::new(name.as_str());
            if let Ok(existing) = write_txn.open_table(definition) {
                for entry in existing.iter().map_err(Self::storage_error)? {
                    let decoded = decode_relational_index_entry(
                        database_path,
                        Some(entry.map_err(Self::storage_error)?),
                    )?
                    .expect("an existing index iterator entry decodes");
                    let (_, position, deleted_tx) = decoded;
                    // The declaration being retired is the one that wrote these
                    // postings, so a posting that does not match it IS a damaged
                    // store and is reported as one.
                    Self::validated_index_position(&declaration, previous_meta, &position)
                        .map_err(|reason| {
                            crate::funded_error::corruption_display(
                                crate::funded_error::original_budget(frame.original),
                                database_path.display(),
                                reason,
                            )
                        })?;
                    if deleted_tx.is_some() {
                        continue;
                    }
                    let logical_key =
                        DurableRelationalIndexKey::new(position.key, position.row_id, TxId(0))?;
                    mutations.push(content_identity::original_producers::remove_relational_index_posting_mutation(frame,
                        table,
                        &declaration.name,
                        &logical_key.0,
                    )?)?;
                }
            }
            #[cfg(any(test, feature = "test-seams"))]
            let retire = !historical_index_commitment::take_retirement_skip_for_test();
            #[cfg(not(any(test, feature = "test-seams")))]
            let retire = true;
            if retire {
                match write_txn.delete_table(definition) {
                    Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(error) => return Err(Self::storage_error(error)),
                }
            }
            let current_name = Self::current_relational_index_table_name(table, &declaration.name);
            let current_definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                TableDefinition::new(current_name.as_str());
            write_txn
                .delete_table(current_definition)
                .map_err(Self::storage_error)?;
        }
        let (published, credit) = commitments.finish(write_txn)?.into_parts();
        mutations_commitment_credits.push(credit);
        mutations.extend(published)?;
        if mutations.is_empty() {
            return Ok(());
        }
        Self::apply_owned_content_identity_with_manifests_in_write(
            write_txn,
            &mutations,
            &[],
            admission,
        )?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn rebuild_relational_indexes_in_write_original(
        frame: &OriginalPersistenceFrame<'_>,
        write_txn: &redb::WriteTransaction,
        database_path: &Path,
        table: &str,
        meta: &TableMeta,
        supplied_rows: Option<&[VersionedRow]>,
        admission: &mut content_identity::original_writer::ProofAdmission,
        shared_commitments: Option<&mut historical_index_commitment::CommitmentBatch>,
    ) -> Result<()> {
        // A rebuild alone in its transaction checks and publishes its own
        // commitments; inside a larger replacement it folds into the caller's.
        let mut own_commitments = match shared_commitments {
            Some(_) => None,
            None => Some(historical_index_commitment::CommitmentBatch::for_original(
                frame.original,
            )?),
        };
        let commitments = match shared_commitments {
            Some(shared) => shared,
            None => own_commitments
                .as_mut()
                .expect("a rebuild alone owns its batch"),
        };
        // Declared first, so dropped last: these charges outlive the mutations they pay for.
        let mut count_mutations_commitment_credits: Vec<
            historical_index_commitment::CommitmentCredit,
        > = Vec::new();
        let mut count_mutations = OriginalMutationVector::new(frame);
        let mut rebuilt_counts = BTreeMap::<Vec<u8>, BTreeSet<RowId>>::new();
        {
            let mut counts = write_txn
                .open_table(RELATIONAL_INDEX_COUNT_TABLE)
                .map_err(Self::storage_error)?;
            let mut retired = Vec::new();
            for entry in counts.iter().map_err(Self::storage_error)? {
                let (key, _) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                let decoded: EncodedRelationalIndexCountKey = frame.decode(key)?;
                if decoded.table == table {
                    retired.push(key.to_vec());
                }
            }
            for key in retired {
                counts.remove(key.as_slice()).map_err(Self::storage_error)?;
                count_mutations.push(Self::remove_relational_index_count_mutation(&key))?;
            }
        }
        for declaration in Self::relational_index_declarations(meta) {
            let name = Self::relational_index_table_name(table, &declaration.name);
            let definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                TableDefinition::new(name.as_str());
            if let Ok(existing) = write_txn.open_table(definition) {
                for entry in existing.iter().map_err(Self::storage_error)? {
                    let decoded = decode_relational_index_entry(
                        database_path,
                        Some(entry.map_err(Self::storage_error)?),
                    )?
                    .expect("an existing index iterator entry decodes");
                    let (_, position, deleted_tx) = decoded;
                    Self::validated_index_position(&declaration, meta, &position).map_err(
                        |reason| {
                            crate::funded_error::corruption_display(
                                crate::funded_error::original_budget(frame.original),
                                database_path.display(),
                                reason,
                            )
                        },
                    )?;
                    if deleted_tx.is_some() {
                        continue;
                    }
                    let logical_key =
                        DurableRelationalIndexKey::new(position.key, position.row_id, TxId(0))?;
                    count_mutations.push(
                        content_identity::original_producers::remove_relational_index_posting_mutation(frame,
                            table,
                            &declaration.name,
                            &logical_key.0,
                        )?,
                    )?;
                }
            }
            match write_txn.delete_table(definition) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(error) => return Err(Self::storage_error(error)),
            }
            let current_name = Self::current_relational_index_table_name(table, &declaration.name);
            let current_definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                TableDefinition::new(current_name.as_str());
            write_txn
                .delete_table(current_definition)
                .map_err(Self::storage_error)?;
            // A rebuild derives both commitments of this index from the rows
            // it walks, which are the authority for what the postings cover.
            commitments.reset(
                write_txn,
                table,
                &declaration,
                historical_index_commitment::Side::Postings,
            )?;
            commitments.reset(
                write_txn,
                table,
                &declaration,
                historical_index_commitment::Side::Versions,
            )?;
            let mut index = historical_index_commitment::HistoricalPostings::open(
                write_txn,
                table,
                &declaration,
            )
            .map_err(Self::storage_error)?;
            let mut active = HashMap::<RowId, (IndexKey, TxId, Option<TxId>)>::new();
            let mut write_row = |row: &VersionedRow| -> Result<()> {
                let key =
                    contextdb_relational::index_key_for_row(&declaration.columns, &row.values);
                commitments.fold_interval(
                    write_txn,
                    table,
                    &declaration,
                    historical_index_commitment::Side::Versions,
                    &historical_index_commitment::key_row(&key, row.row_id)?,
                    row.created_tx,
                    row.deleted_tx,
                    true,
                )?;
                // A run continues only while the key is unchanged and the
                // versions are contiguous; a gap starts a new interval.
                let (created_tx, durable_key) = match active.get(&row.row_id) {
                    Some((prior, created_tx, end))
                        if prior == &key && *end == Some(row.created_tx) =>
                    {
                        (
                            *created_tx,
                            DurableRelationalIndexKey::new(key.clone(), row.row_id, *created_tx)?,
                        )
                    }
                    _ => (
                        row.created_tx,
                        DurableRelationalIndexKey::new(key.clone(), row.row_id, row.created_tx)?,
                    ),
                };
                index.put(commitments, write_txn, durable_key, row.deleted_tx)?;
                active.insert(row.row_id, (key, created_tx, row.deleted_tx));
                Ok(())
            };
            if let Some(rows) = supplied_rows {
                // Match the durable relation's key order without copying row
                // bodies. Callers already own these rows; this pointer-only
                // ordering keeps version publication identical to a Redb walk.
                let mut ordered = rows.iter().collect::<Vec<_>>();
                ordered.sort_unstable_by_key(|row| (row.row_id, row.created_tx, row.lsn));
                for row in ordered {
                    write_row(row)?;
                }
            } else {
                let relation_name = Self::rel_table_name(table);
                let relation_definition: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(relation_name.as_str());
                match write_txn.open_table(relation_definition) {
                    Ok(relation) => {
                        for entry in relation.iter().map_err(Self::storage_error)? {
                            let (_, value) = entry.map_err(Self::storage_error)?;
                            #[cfg(any(test, feature = "test-seams"))]
                            RELATIONAL_INDEX_REBUILD_ROWS_DECODED
                                .with(|count| count.set(count.get().saturating_add(1)));
                            let _row_scope = crate::read_image_memory::active_owner()?
                                .row_owner()?
                                .enter();
                            let row = Self::decode_versioned_row_original(
                                frame,
                                value.value(),
                                Some(meta),
                            )?;
                            write_row(&row)?;
                        }
                    }
                    Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(error) => return Err(Self::storage_error(error)),
                }
            }
            drop(write_row);
            drop(index);
            let mut current = write_txn
                .open_table(current_definition)
                .map_err(Self::storage_error)?;
            for (row_id, (key, created_tx, end)) in active {
                if end.is_some() {
                    continue;
                }
                let logical_key = DurableRelationalIndexKey::new(key.clone(), row_id, TxId(0))?;
                let created = frame.encode(&created_tx)?;
                current
                    .insert(&logical_key, created.as_slice())
                    .map_err(Self::storage_error)?;
                count_mutations.push(
                    content_identity::original_producers::relational_index_posting_mutation(
                        frame,
                        table,
                        &declaration.name,
                        &logical_key.0,
                    )?,
                )?;
                for prefix_len in 1..=key.len() {
                    // The rebuild writes exactly the declared members, so a
                    // recompute after a declaration change lands the same set
                    // an append would have written.
                    if !crate::maintained_counts::maintained_count_member(
                        meta,
                        &declaration.columns[..prefix_len],
                    ) {
                        continue;
                    }
                    let count_key = Self::relational_index_count_key(
                        table,
                        &declaration.columns[..prefix_len],
                        &key[..prefix_len],
                    )?;
                    #[cfg(any(test, feature = "test-seams"))]
                    observe_index_prefix_count(table, meta, &declaration, prefix_len, &count_key);
                    rebuilt_counts.entry(count_key).or_default().insert(row_id);
                }
            }
        }
        {
            let mut counts = write_txn
                .open_table(RELATIONAL_INDEX_COUNT_TABLE)
                .map_err(Self::storage_error)?;
            for (key, rows) in rebuilt_counts {
                let count = u64::try_from(rows.len()).map_err(|_| {
                    Error::Other(("relational index count rebuild overflow".to_owned()).into())
                })?;
                counts
                    .insert(key.as_slice(), count)
                    .map_err(Self::storage_error)?;
                count_mutations.push(Self::relational_index_count_mutation(&key, count))?;
            }
        }
        write_txn
            .open_table(RELATIONAL_INDEX_READY_TABLE)
            .map_err(Self::storage_error)?
            .insert(table, RELATIONAL_INDEX_READY_VERSION)
            .map_err(Self::storage_error)?;
        count_mutations.push(content_identity::original_producers::status_mutation(
            frame,
            RELATIONAL_INDEX_COUNT_READY_IDENTITY_NAMESPACE,
            table.as_bytes(),
            RELATIONAL_INDEX_READY_VERSION,
        )?)?;
        if let Some(own) = own_commitments {
            let (published, credit) = own.finish(write_txn)?.into_parts();
            count_mutations_commitment_credits.push(credit);
            count_mutations.extend(published)?;
        }
        Self::apply_owned_content_identity_with_manifests_in_write(
            write_txn,
            &count_mutations,
            &[],
            admission,
        )?;
        Ok(())
    }
}

impl RedbPersistence {
    // Internal metadata counterpart: the caller lends its real assembly
    // original before the existing DDL commit boundary, without a new writer.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn flush_original_encoded_config_and_ddl<'c>(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        config: impl Iterator<Item = (&'c str, &'c [u8])> + Clone,
        lsn: Lsn,
        ddl: &[DdlChange],
    ) -> Result<()> {
        let storage_scope =
            native_storage_admission::StorageAdmissionScope::writer_original(original)?;
        storage_scope.run_original_unqualified(|| {
            let frame = OriginalPersistenceFrame::new(original)?;
            for (key, value) in config.clone() {
                frame.plan_config(key, value)?;
            }
            for change in ddl {
                frame.reserve(256)?;
                frame.record(change)?;
            }
            self.with_db(|db| {
                let write_txn = Self::begin_history_write(db)?;
                #[cfg(any(test, feature = "test-seams"))]
                let mut graph_publication_attempt =
                    crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
                let mut mutations = OriginalMutationVector::new(&frame);
                {
                    let mut table = write_txn
                        .open_table(CONFIG_TABLE)
                        .map_err(Self::storage_error)?;
                    for (key, encoded) in config.clone() {
                        Self::raise_config_identity_floors_in_write(&write_txn, key, encoded)?;
                        Self::update_policy_hold_release_index(&write_txn, key, true)?;
                        table.insert(key, encoded).map_err(Self::storage_error)?;
                        mutations.extend(
                            content_identity::original_producers::config_mutations(
                                &frame,
                                key,
                                Some(encoded),
                            )?,
                        )?;
                    }
                }
                {
                    let mut table = write_txn
                        .open_table(DDL_LOG_TABLE)
                        .map_err(Self::storage_error)?;
                    for (index, change) in ddl.iter().enumerate() {
                        let key = Self::ddl_log_key_for_index(lsn, index, ddl.len());
                        self.append_ddl_summary_in_write(&write_txn, &key, change)?;
                        let encoded = frame.encode(change)?;
                        table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        mutations.push(
                            content_identity::original_producers::encoded_history_mutation(
                                &frame, &key, change,
                            )?,
                        )?;
                    }
                }
                Self::apply_owned_content_identity_with_manifests_and_groups_in_write(
                    &write_txn,
                    &mutations,
                    &[],
                    &mut content_identity::original_writer::ProofAdmission::new(
                        frame.original,
                        self.proof_scratch_owner(),
                    )?,
                    None,
                )?;
                Self::raise_noncanonical_writer_floors_in_write(
                    &write_txn,
                    TxId(0),
                    lsn,
                    RowId(0),
                )?;
                Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
                {
                    #[cfg(any(test, feature = "test-seams"))]
                    graph_observation_checkpoint(10)?;
                    let committed = write_txn.commit();
                    #[cfg(any(test, feature = "test-seams"))]
                    if committed.is_ok() {
                        graph_publication_attempt.committed();
                        graph_observation_checkpoint(11)?;
                    }
                    committed
                }
                .map_err(Self::storage_error)?;
                Ok(())
            })
        })
    }
}

impl RedbPersistence {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn with_original_blob_pages<R>(
        original: &crate::memory_accounting::OwnedMemoryReservation,
        read: impl FnOnce() -> Result<R>,
    ) -> Result<R> {
        // Actual Redb page/window allocations are admitted before their reads.
        // The owned page credit releases after the final borrowed guard; cached
        // pages remain under the existing cache owner, never a new accountant.
        let memory =
            crate::memory_accounting::persistence_original_projection(original)?.read_budget()?;
        graph_read::with_pages(memory, read)
    }
}

impl RetainedReadSource {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_physical_frontier(
        &self,
    ) -> Result<(redb::StorageSnapshotIdentity, SnapshotId, Lsn)> {
        if self.registration.snapshot.storage_snapshot != self.snapshot
            || self.registration.registration.is_none()
        {
            return Err(vector_paged::corruption(
                "image vector holder has no authentic registration",
            ));
        }
        Ok((
            self.snapshot,
            self.logical_frontier,
            self.graph_lsn_frontier,
        ))
    }
}

#[cfg(all(test, feature = "test-seams"))]
impl RedbPersistence {
    pub(crate) fn recycle_cache_handle_for_test(&self) -> Result<Option<u64>> {
        let mut handle = self.db.write().unwrap();
        self.recycle_database_handle(&mut handle)
    }
    pub(crate) fn original_cache_reserve_for_test(&self) -> (usize, usize, usize) {
        cache_reserve::last_reserve_for_test()
    }
}

#[cfg(all(test, feature = "test-seams"))]
impl RedbPersistence {
    pub(crate) fn original_cache_controls_for_test(&self) -> redb::CacheReserveOwner {
        self.cache_installation
            .owner
            .lock()
            .as_ref()
            .unwrap()
            .clone()
    }
}

#[cfg(test)]
pub(crate) fn install_source_fixture_cache_reserve_for_test(
    database: &redb::Database,
    accountant: &crate::memory_accounting::AccountantHandle,
    capacity: usize,
) -> (redb::CacheReserveOwner, usize) {
    // Exercise the SAME settlement and genuine original invoice as the engine.
    // No disconnected parent or numeric-only capacity discharges a source bill.
    cache_reserve::with_preparation(accountant, true, || {
        let bytes = database
            .settle_startup_cache_budget(capacity)
            .map_err(RedbPersistence::storage_error)?;
        let reserve = cache_reserve::original(accountant, bytes, false)?;
        let control = cache_reserve::last_reserve_for_test().1;
        Ok((database.retain_cache_reserve(reserve), bytes + control))
    })
    .unwrap()
}
