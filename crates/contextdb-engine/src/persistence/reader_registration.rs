use super::*;

#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
pub(super) fn observed_graph_storage_key<K: AsRef<[u8]>>(
    family: &str,
    operation: &str,
    key: K,
    boundary: u8,
) -> Result<K> {
    graph_observation_checkpoint(boundary)?;
    Ok(crate::read_work_probe::graph_storage_key(
        family, operation, key,
    ))
}

/// Interior free pages over the pages a trim would leave.
pub(crate) fn reclaimable_free_ratio(free: u64, pages: u64, trailing: u64) -> f64 {
    let interior_free = free.saturating_sub(trailing);
    let kept_pages = pages.saturating_sub(trailing);
    if kept_pages == 0 {
        0.0
    } else {
        interior_free as f64 / kept_pages as f64
    }
}

#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub(crate) fn storage_statistics_captures_for_test() -> u64 {
    STORAGE_STATISTICS_CAPTURES.with(std::cell::Cell::get)
}

#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub(crate) fn storage_statistics_pages_walked_for_test() -> (u64, u64) {
    (
        STORAGE_STATISTICS_PAGES_WALKED.with(std::cell::Cell::get),
        STORAGE_STATISTICS_PAGES_WALKED_UNDER_PRUNING_GUARD.with(std::cell::Cell::get),
    )
}

#[cfg(any(test, feature = "test-seams"))]
impl PruningGuardHeldForTest {
    pub(crate) fn enter() -> Self {
        PRUNING_GUARD_HELD_DEPTH.with(|depth| depth.set(depth.get() + 1));
        Self(())
    }
}

#[cfg(any(test, feature = "test-seams"))]
impl Drop for PruningGuardHeldForTest {
    fn drop(&mut self) {
        PRUNING_GUARD_HELD_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

impl OwnerCursorCoordinationScope {
    pub(super) fn enter() -> Self {
        Self(OWNER_CURSOR_COORDINATION_NONBLOCKING.with(|mode| mode.replace(true)))
    }
}

impl Drop for OwnerCursorCoordinationScope {
    fn drop(&mut self) {
        OWNER_CURSOR_COORDINATION_NONBLOCKING.with(|mode| mode.set(self.0));
    }
}

/// Whether a declared constraint already bounds the live-row count over
/// exactly these columns at 0 or 1.
///
/// A primary key, or a unique constraint over the same column SET, admits
/// at most one live row per key — so the count carries no information the
/// index postings do not already carry, and an exact-key range over them
/// reads at most one entry. Column ORDER does not matter: a constraint over
/// `(a, b)` bounds a count keyed `(b, a)` just as well.
///
/// NULL is the exception, and it is not a small one. A row with NULL in a
/// constrained column does not take part in the uniqueness check at all
/// (`docs/query-language.md`), so any number of rows can share an index key
/// that contains one. A column list is bounded only when every column in it
/// is NOT NULL.
pub(crate) fn relational_index_prefix_is_constraint_bounded(
    meta: &TableMeta,
    columns: &[(String, contextdb_core::SortDirection)],
) -> bool {
    let names = columns
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<BTreeSet<_>>();
    if names.len() != columns.len() {
        return false;
    }
    let every_column_is_declared_not_null = names.iter().all(|name| {
        meta.columns
            .iter()
            .find(|column| column.name == *name)
            .is_some_and(|column| !column.nullable || column.primary_key)
    });
    if !every_column_is_declared_not_null {
        return false;
    }
    if !meta.primary_key_columns.is_empty()
        && meta
            .primary_key_columns
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            == names
    {
        return true;
    }
    if meta.unique_constraints.iter().any(|constraint| {
        constraint
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            == names
    }) {
        return true;
    }
    columns.len() == 1
        && meta
            .columns
            .iter()
            .any(|column| column.name == columns[0].0 && (column.primary_key || column.unique))
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn observe_index_posting(table: &str, declaration: &IndexDecl) {
    INDEX_POSTINGS.with(|postings| {
        let mut postings = postings.borrow_mut();
        let entry = postings
            .entry((table.to_owned(), declaration.name.clone()))
            .or_insert_with(|| {
                (
                    declaration
                        .columns
                        .iter()
                        .map(|(name, _)| name.clone())
                        .collect(),
                    0,
                )
            });
        entry.1 += 1;
    });
}

#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from append_work.rs"
    )
)]
pub(crate) fn take_index_postings_for_test() -> Vec<IndexPostingTouch> {
    INDEX_POSTINGS.with(|postings| {
        std::mem::take(&mut *postings.borrow_mut())
            .into_iter()
            .map(|((table, index), (columns, postings))| IndexPostingTouch {
                table,
                index,
                columns,
                postings,
            })
            .collect()
    })
}

/// Whether the table declares a primary key or a unique constraint over exactly
/// these columns, in any order.
#[cfg(any(test, feature = "test-seams"))]
pub(super) fn prefix_is_uniquely_constrained(meta: &TableMeta, columns: &[String]) -> bool {
    let names = columns.iter().map(String::as_str).collect::<BTreeSet<_>>();
    if names.len() != columns.len() {
        return false;
    }
    if !meta.primary_key_columns.is_empty()
        && meta
            .primary_key_columns
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            == names
    {
        return true;
    }
    if meta.unique_constraints.iter().any(|constraint| {
        constraint
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            == names
    }) {
        return true;
    }
    columns.len() == 1
        && meta
            .columns
            .iter()
            .any(|column| column.name == columns[0] && (column.primary_key || column.unique))
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn observe_index_prefix_count(
    table: &str,
    meta: &TableMeta,
    declaration: &IndexDecl,
    prefix_columns: usize,
    count_key: &[u8],
) {
    let columns = declaration.columns[..prefix_columns]
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    let uniquely_constrained = prefix_is_uniquely_constrained(meta, &columns);
    INDEX_PREFIX_COUNT_TOUCHES.with(|touches| {
        touches
            .borrow_mut()
            .entry(count_key.to_vec())
            .or_default()
            .insert((
                table.to_owned(),
                columns,
                uniquely_constrained,
                declaration.name.clone(),
                declaration.columns.len(),
                prefix_columns,
            ))
    });
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn observe_index_prefix_count_value(count_key: &[u8], value: Option<u64>) {
    INDEX_PREFIX_COUNT_VALUES.with(|values| values.borrow_mut().insert(count_key.to_vec(), value));
}

/// Charge the time since `started` to `stage`, and restart the clock so the
/// next stage measures from here. Stages therefore partition the window rather
/// than overlapping it.
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn observe_append_stage(stage: AppendStage, started: &mut std::time::Instant) {
    let nanos = started.elapsed().as_nanos() as u64;
    *started = std::time::Instant::now();
    APPEND_STAGE_NANOS.with(|stages| {
        let mut current = stages.get();
        let field = match stage {
            AppendStage::WriteSetPreparation => &mut current.write_set_preparation,
            AppendStage::TableMetaClone => &mut current.table_meta_clone,
            AppendStage::RowsIndexesAndLogs => &mut current.rows_indexes_and_logs,
            AppendStage::ContentIdentity => &mut current.content_identity,
            AppendStage::Commit => &mut current.commit,
        };
        *field = field.saturating_add(nanos);
        stages.set(current);
    });
}

#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature"
    )
)]
pub(crate) fn reset_append_stages_for_test() {
    APPEND_STAGE_NANOS.with(|stages| stages.set(AppendStageNanos::default()));
}

#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from append_work.rs"
    )
)]
pub(crate) fn take_append_stages_for_test() -> AppendStageNanos {
    APPEND_STAGE_NANOS.with(|stages| stages.replace(AppendStageNanos::default()))
}

#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from append_work.rs"
    )
)]
pub(crate) fn reset_index_prefix_counts_for_test() {
    INDEX_PREFIX_COUNT_TOUCHES.with(|touches| touches.borrow_mut().clear());
    INDEX_PREFIX_COUNT_VALUES.with(|values| values.borrow_mut().clear());
    INDEX_POSTINGS.with(|postings| postings.borrow_mut().clear());
    reset_append_stages_for_test();
}

#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from append_work.rs"
    )
)]
pub(crate) fn take_index_prefix_counts_for_test() -> Vec<IndexPrefixCountTouch> {
    let values = INDEX_PREFIX_COUNT_VALUES.with(|values| std::mem::take(&mut *values.borrow_mut()));
    INDEX_PREFIX_COUNT_TOUCHES.with(|touches| {
        std::mem::take(&mut *touches.borrow_mut())
            .into_iter()
            .map(|(key, attributions)| {
                let (table, prefix_columns, uniquely_constrained) = attributions
                    .iter()
                    .next()
                    .map(|(table, columns, unique, ..)| (table.clone(), columns.clone(), *unique))
                    .unwrap_or_default();
                IndexPrefixCountTouch {
                    table,
                    prefix_columns,
                    uniquely_constrained,
                    maintained_by: attributions
                        .into_iter()
                        .map(|(_, _, _, index, width, prefix)| (index, width, prefix))
                        .collect(),
                    count_after_the_write: values.get(&key).copied().flatten(),
                }
            })
            .collect()
    })
}

impl From<RelationalIndexPosition> for EncodedRelationalIndexPosition {
    fn from(position: RelationalIndexPosition) -> Self {
        Self {
            key: position
                .key
                .into_iter()
                .map(|part| match part {
                    DirectedValue::Asc(TotalOrdAsc(value)) => (false, value),
                    DirectedValue::Desc(TotalOrdDesc(value)) => (true, value),
                })
                .collect(),
            row_id: position.row_id,
            created_tx: position.created_tx,
        }
    }
}

impl From<EncodedRelationalIndexPosition> for RelationalIndexPosition {
    fn from(position: EncodedRelationalIndexPosition) -> Self {
        Self {
            key: position
                .key
                .into_iter()
                .map(|(descending, value)| {
                    if descending {
                        DirectedValue::Desc(TotalOrdDesc(value))
                    } else {
                        DirectedValue::Asc(TotalOrdAsc(value))
                    }
                })
                .collect(),
            row_id: position.row_id,
            created_tx: position.created_tx,
        }
    }
}

impl DurableRelationalIndexKey {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn new(key: IndexKey, row_id: RowId, created_tx: TxId) -> Result<Self> {
        Ok(Self(RedbPersistence::encode(
            &EncodedRelationalIndexPosition::from(RelationalIndexPosition {
                key,
                row_id,
                created_tx,
            }),
        )?))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn decode(&self) -> Result<RelationalIndexPosition> {
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        let position = RedbPersistence::decode::<EncodedRelationalIndexPosition>(&self.0)?;
        crate::read_image_memory::reserve(
            position
                .key
                .len()
                .saturating_mul(std::mem::size_of::<DirectedValue>()),
        )?;
        Ok(position.into())
    }

    /// The only decode a stored posting key is allowed to go through.
    ///
    /// Exact: a key carrying trailing bytes is not a position, it is damaged
    /// storage, and a permissive decode that ignores those bytes disagrees with
    /// the durable comparator about where the key belongs - so the same bytes
    /// sort in the comparator's malformed region and then decode back into the
    /// ordinary path. Canonical: bytes that decode to a position but are not
    /// the bytes this encoder produces for it are equally not something this
    /// store wrote. Both are reported as corruption before the posting can
    /// order, compare, advance a cursor or reach an answer.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn decode_canonical(&self) -> Result<RelationalIndexPosition> {
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        let position = RedbPersistence::decode_exact::<EncodedRelationalIndexPosition>(&self.0)
            .map_err(|error| match error {
                Error::Other(reason) => Error::Other(
                    (format!("durable index position is not an exactly decodable key: {reason}"))
                        .into(),
                ),
                other => other,
            })?;
        // Value::Json creates a temporary String even for a sizing writer.
        // Admit that concrete serializer overlap on THIS read operation first.
        let (json_scratch, _) = crate::memory_accounting::preparation_json_work_bytes(
            position.key.iter().map(|(_, value)| value),
        )?;
        let memory = crate::read_image_memory::active_owner()?;
        memory.reserve(json_scratch)?;
        struct JsonScratch<'a>(&'a crate::read_image_memory::ImageMemory, usize);
        impl Drop for JsonScratch<'_> {
            fn drop(&mut self) {
                self.0.release(self.1);
            }
        }
        let _json_scratch = JsonScratch(&memory, json_scratch);
        // Count without allocating, then fund an exact buffer before re-encoding.
        let mut size = bincode::enc::write::SizeWriter::default();
        bincode::serde::encode_into_writer(&position, &mut size, bincode::config::standard())
            .map_err(|error| Error::Other((format!("bincode encode error: {error}")).into()))?;
        crate::read_image_memory::reserve(size.bytes_written)?;
        let mut canonical = vec![0; size.bytes_written];
        bincode::serde::encode_into_slice(&position, &mut canonical, bincode::config::standard())
            .map_err(|error| Error::Other((format!("bincode encode error: {error}")).into()))?;
        if canonical != self.0 {
            return Err(Error::Other(
                ("durable index position is not canonically encoded".to_owned()).into(),
            ));
        }
        crate::read_image_memory::reserve(
            position
                .key
                .len()
                .saturating_mul(std::mem::size_of::<DirectedValue>()),
        )?;
        Ok(position.into())
    }
}

impl RedbValue for DurableRelationalIndexKey {
    type SelfType<'a> = DurableRelationalIndexKey;
    type AsBytes<'a> = &'a [u8];

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self(data.to_vec())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> {
        &value.0
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("contextdb.RelationalIndexPosition.v1")
    }
}

impl RedbKey for DurableRelationalIndexKey {
    fn compare(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
        RedbPersistence::compare_relational_index_order_keys(left, right)
    }
}
impl RedbValue for BorrowedRelationalIndexKey {
    type SelfType<'a> = &'a [u8];
    type AsBytes<'a> = &'a [u8];
    fn fixed_width() -> Option<usize> {
        DurableRelationalIndexKey::fixed_width()
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        data
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> {
        value
    }
    fn type_name() -> redb::TypeName {
        DurableRelationalIndexKey::type_name()
    }
}
impl RedbKey for BorrowedRelationalIndexKey {
    fn compare(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
        DurableRelationalIndexKey::compare(left, right)
    }
}
impl OwnerCursorStoragePin {
    pub(crate) fn storage_identity(&self) -> redb::StorageSnapshotIdentity {
        self.read.storage_snapshot_identity()
    }
}

impl OwnerCursorPinAdmissionError {
    #[cfg(test)]
    pub(super) fn into_error(self) -> Error {
        match self {
            Self::Busy(error) | Self::Refused(error) => error,
        }
    }
}

impl From<Error> for OwnerCursorPinAdmissionError {
    fn from(error: Error) -> Self {
        Self::Refused(error)
    }
}

impl WriterBootBinding {
    /// The binding for an open data descriptor on this boot, or `None` where
    /// the boot or the file's birth cannot be known.
    pub(crate) fn of_data_file(file: &File) -> Option<Self> {
        let boot_id = current_boot_id()?;
        let (data_dev, data_ino, data_birth_nanos) = data_file_birth_identity(file)?;
        Some(Self {
            boot_id,
            data_dev,
            data_ino,
            data_birth_nanos,
        })
    }
}

/// This kernel boot's identity. Linux only: elsewhere there is no boot token
/// that is unique across reboots, so no store is read past a crash there.
pub(super) fn current_boot_id() -> Option<[u8; 16]> {
    #[cfg(any(test, feature = "test-seams"))]
    if read_persistence_test_scaffold::reader_sees_another_boot_for_test() {
        return Some([0xEE; 16]);
    }
    #[cfg(target_os = "linux")]
    {
        let text = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
        uuid::Uuid::parse_str(text.trim())
            .ok()
            .map(uuid::Uuid::into_bytes)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(unix)]
pub(super) fn data_file_birth_identity(file: &File) -> Option<(u64, u64, u128)> {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = file.metadata().ok()?;
    let birth = metadata
        .created()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_nanos();
    Some((metadata.dev(), metadata.ino(), birth))
}

#[cfg(not(unix))]
pub(super) fn data_file_birth_identity(_file: &File) -> Option<(u64, u64, u128)> {
    None
}

/// Whether the data file lives on a local filesystem whose page cache every
/// process on this boot reads coherently. Anything else -- network, FUSE,
/// overlay, unknown -- is not trusted to hold a dead writer's pages.
#[cfg(target_os = "linux")]
pub(super) fn data_file_on_trusted_local_filesystem(file: &File) -> bool {
    use std::os::unix::io::AsRawFd as _;
    #[cfg(any(test, feature = "test-seams"))]
    if read_persistence_test_scaffold::reader_sees_untrusted_filesystem_for_test() {
        return false;
    }
    const EXT4_SUPER_MAGIC: i64 = 0xEF53;
    const XFS_SUPER_MAGIC: i64 = 0x5846_5342;
    const BTRFS_SUPER_MAGIC: i64 = 0x9123_683E;
    const TMPFS_MAGIC: i64 = 0x0102_1994;
    const F2FS_SUPER_MAGIC: i64 = 0xF2F5_2010;
    // SAFETY: `statfs` is plain data and fstatfs fills it for an open fd.
    let mut stats: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstatfs(file.as_raw_fd(), &mut stats) } != 0 {
        return false;
    }
    #[allow(clippy::unnecessary_cast)]
    let kind = stats.f_type as i64;
    matches!(
        kind,
        EXT4_SUPER_MAGIC | XFS_SUPER_MAGIC | BTRFS_SUPER_MAGIC | TMPFS_MAGIC | F2FS_SUPER_MAGIC
    )
}

#[cfg(not(target_os = "linux"))]
pub(super) fn data_file_on_trusted_local_filesystem(_file: &File) -> bool {
    false
}

pub(super) fn companion_status_tags(
    status: &OwnerReadStatus,
) -> std::result::Result<(u8, u8), String> {
    status.validate().map_err(|error| error.to_string())?;
    let state = match status.state {
        OwnerServingState::Serving => 0,
        OwnerServingState::ServingDisabled => 1,
        OwnerServingState::NotServing => 2,
        OwnerServingState::NotApplicable => 3,
    };
    let reason = match &status.reason {
        None => 0,
        Some(OwnerServingReason::DisabledByConfiguration) => 1,
        Some(OwnerServingReason::StartupFailure(_)) => 2,
        Some(OwnerServingReason::ShutdownDraining) => 3,
        Some(OwnerServingReason::PlatformUnsupported) => 4,
    };
    Ok((state, reason))
}

pub(super) fn encode_companion_record(
    fields: &CompanionRecord,
) -> std::result::Result<EncodedCompanionRecord, String> {
    let (state, reason) = companion_status_tags(&fields.owner_read_status)?;
    let startup_reason = match &fields.owner_read_status.reason {
        Some(OwnerServingReason::StartupFailure(reason)) => reason.as_bytes(),
        _ => &[],
    };
    if startup_reason.len() > MAX_COMPANION_STARTUP_REASON_BYTES {
        return Err(format!(
            "companion startup-failure reason exceeds {MAX_COMPANION_STARTUP_REASON_BYTES} bytes"
        ));
    }
    let startup_reason_len = u32::try_from(startup_reason.len())
        .map_err(|_| "companion startup-failure reason is too long".to_owned())?;
    let mut payload = Vec::with_capacity(
        COMPANION_RECORD_MAGIC.len() + 2 + 8 + 16 + 16 + 8 + 32 + 4 + 2 + 4 + startup_reason.len(),
    );
    payload.extend_from_slice(COMPANION_RECORD_MAGIC);
    payload.extend_from_slice(&fields.format_version.to_le_bytes());
    payload.extend_from_slice(&fields.generation.to_le_bytes());
    payload.extend_from_slice(&fields.database_identity.0);
    payload.extend_from_slice(&fields.writer_run_number.0);
    payload.extend_from_slice(&fields.owner_user.0.to_le_bytes());
    payload.extend_from_slice(&fields.channel_address.0);
    payload.extend_from_slice(&fields.process_id.to_le_bytes());
    payload.push(state);
    payload.push(reason);
    if reason == 2 {
        payload.extend_from_slice(&startup_reason_len.to_le_bytes());
        payload.extend_from_slice(startup_reason);
    }
    match &fields.writer_boot {
        None => payload.push(0),
        Some(binding) => {
            payload.push(1);
            payload.extend_from_slice(&binding.boot_id);
            payload.extend_from_slice(&binding.data_dev.to_le_bytes());
            payload.extend_from_slice(&binding.data_ino.to_le_bytes());
            payload.extend_from_slice(&binding.data_birth_nanos.to_le_bytes());
        }
    }
    let checksum = *blake3::hash(&payload).as_bytes();
    Ok(EncodedCompanionRecord {
        fields: fields.clone(),
        checksummed_payload: payload,
        stored_checksum: checksum,
    })
}

impl<'a> CompanionPayloadCursor<'a> {
    pub(super) fn take(&mut self, length: usize) -> std::result::Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| "companion payload offset overflow".to_owned())?;
        let bytes = self
            .payload
            .get(self.offset..end)
            .ok_or_else(|| "companion payload is truncated".to_owned())?;
        self.offset = end;
        Ok(bytes)
    }

    pub(super) fn array<const N: usize>(&mut self) -> std::result::Result<[u8; N], String> {
        self.take(N)?
            .try_into()
            .map_err(|_| "companion payload field has the wrong width".to_owned())
    }

    pub(super) fn u8(&mut self) -> std::result::Result<u8, String> {
        Ok(self.array::<1>()?[0])
    }

    pub(super) fn u16(&mut self) -> std::result::Result<u16, String> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    pub(super) fn u32(&mut self) -> std::result::Result<u32, String> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    pub(super) fn u64(&mut self) -> std::result::Result<u64, String> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    pub(super) fn finish(self) -> std::result::Result<(), String> {
        if self.offset == self.payload.len() {
            Ok(())
        } else {
            Err("companion payload has trailing bytes".to_owned())
        }
    }
}

pub(super) fn decode_companion_record(
    payload: &[u8],
    stored_checksum: [u8; 32],
) -> std::result::Result<EncodedCompanionRecord, String> {
    if *blake3::hash(payload).as_bytes() != stored_checksum {
        return Err("companion record checksum mismatch".to_owned());
    }
    let mut cursor = CompanionPayloadCursor { payload, offset: 0 };
    if cursor.take(COMPANION_RECORD_MAGIC.len())? != COMPANION_RECORD_MAGIC {
        return Err("companion record magic mismatch".to_owned());
    }
    let format_version = cursor.u16()?;
    let generation = cursor.u64()?;
    let database_identity = DatabaseIdentity(cursor.array()?);
    let writer_run_number = WriterRunNumber(cursor.array()?);
    let owner_user = LocalUserIdentity(cursor.u64()?);
    let channel_address = ChannelAddress(cursor.array()?);
    let process_id = cursor.u32()?;
    let state = match cursor.u8()? {
        0 => OwnerServingState::Serving,
        1 => OwnerServingState::ServingDisabled,
        2 => OwnerServingState::NotServing,
        3 => OwnerServingState::NotApplicable,
        other => return Err(format!("unknown companion serving-state tag {other}")),
    };
    let reason = match cursor.u8()? {
        0 => None,
        1 => Some(OwnerServingReason::DisabledByConfiguration),
        2 => {
            let length = usize::try_from(cursor.u32()?)
                .map_err(|_| "companion reason length does not fit usize".to_owned())?;
            if length > MAX_COMPANION_STARTUP_REASON_BYTES {
                return Err(format!(
                    "companion startup-failure reason exceeds {MAX_COMPANION_STARTUP_REASON_BYTES} bytes"
                ));
            }
            let reason = String::from_utf8(cursor.take(length)?.to_vec())
                .map_err(|error| format!("companion startup reason is not UTF-8: {error}"))?;
            Some(OwnerServingReason::StartupFailure(reason))
        }
        3 => Some(OwnerServingReason::ShutdownDraining),
        4 => Some(OwnerServingReason::PlatformUnsupported),
        other => return Err(format!("unknown companion serving-reason tag {other}")),
    };
    let writer_boot = match cursor.u8()? {
        0 => None,
        1 => Some(WriterBootBinding {
            boot_id: cursor.array()?,
            data_dev: cursor.u64()?,
            data_ino: cursor.u64()?,
            data_birth_nanos: u128::from_le_bytes(cursor.array()?),
        }),
        other => return Err(format!("unknown companion writer-boot tag {other}")),
    };
    cursor.finish()?;
    let owner_read_status = OwnerReadStatus { state, reason };
    owner_read_status
        .validate()
        .map_err(|error| error.to_string())?;
    Ok(EncodedCompanionRecord {
        fields: CompanionRecord {
            format_version,
            generation,
            database_identity,
            writer_run_number,
            owner_user,
            channel_address,
            process_id,
            owner_read_status,
            writer_boot,
        },
        checksummed_payload: payload.to_vec(),
        stored_checksum,
    })
}

pub(super) fn bounded_companion_startup_reason(reason: &str) -> String {
    let normalized = reason.to_ascii_lowercase();
    for (needle, public_reason) in [
        ("permission denied", "permission denied"),
        ("address already in use", "address already in use"),
        ("no such file", "path not found"),
        ("not found", "path not found"),
        ("connection refused", "connection refused"),
        ("timed out", "operation timed out"),
        ("unsupported", "platform unsupported"),
        ("invalid input", "invalid operating-system input"),
        ("resource busy", "resource busy"),
        ("read-only file system", "read-only file system"),
        ("too many open files", "file-descriptor limit reached"),
    ] {
        if normalized.contains(needle) {
            return public_reason.to_owned();
        }
    }
    "operating-system error (details redacted)".to_owned()
}

pub(super) fn bounded_companion_status(status: OwnerReadStatus) -> OwnerReadStatus {
    let reason = match status.reason {
        Some(OwnerServingReason::StartupFailure(reason)) => Some(
            OwnerServingReason::StartupFailure(bounded_companion_startup_reason(&reason)),
        ),
        other => other,
    };
    OwnerReadStatus {
        state: status.state,
        reason,
    }
}

pub(super) fn prepare_companion_record_for_publication(
    fields: &CompanionRecord,
) -> std::result::Result<EncodedCompanionRecord, String> {
    let record = encode_companion_record(fields)?;
    #[cfg(any(test, feature = "test-seams"))]
    read_persistence_test_scaffold::companion_update_planned_for_test(&record);
    Ok(record)
}

pub(super) fn recover_companion_record(
    payload: &[u8],
    stored_checksum: [u8; 32],
) -> std::result::Result<EncodedCompanionRecord, String> {
    decode_companion_record(payload, stored_checksum)
}

/// The identity a live writer published beside its store: which database this
/// is, which run of it is current, whose account owns it, and the channel
/// address a reader would dial. A reader derives none of this itself, so an
/// owner that answers on a channel answers as exactly the run recorded here.
pub(crate) fn published_writer_identity(
    database_path: &Path,
) -> Option<(
    DatabaseIdentity,
    WriterRunNumber,
    LocalUserIdentity,
    ChannelAddress,
)> {
    let record = inspect_companion_record(database_path).ok()?;
    Some((
        record.fields.database_identity,
        record.fields.writer_run_number,
        record.fields.owner_user,
        record.fields.channel_address,
    ))
}

/// Whether the trusted companion proves a writer owns this store right now.
///
/// A published writer identity is durable and can outlive its process. The
/// advisory lock is the live fact route selection needs when a pathname
/// accepts a connection but never authenticates an owner. This probe touches
/// only the companion beside the store; the committed file is never opened.
/// `None` is deliberately indeterminate: an absent, untrusted, replaced, or
/// unprobeable companion never licenses a direct-file fallback.
pub(crate) fn observed_writer_hold(database_path: &Path) -> Option<bool> {
    let companion = companion_path(database_path);
    let file = open_trusted_companion(&companion)?;
    let before_probe = file.metadata().ok()?;
    validate_companion_metadata(&companion, &file, &before_probe, CompanionUse::Reading).ok()?;
    let held = writer_holds_companion(&file, database_path)?;
    validate_companion_metadata(&companion, &file, &before_probe, CompanionUse::Reading).ok()?;
    Some(held)
}

pub(crate) fn observed_writer_state(database_path: &Path) -> Option<ObservedWriterState> {
    // ONE open. The record and the lock that proves the record is still THIS
    // writer's are observed through the same companion file, and the writer's
    // own run identity is read on both sides of the lock probe. Two separate
    // opens could span a handover -- one writer exiting, the next acquiring --
    // and the arriving writer's lock would then vouch for the departed
    // writer's reason, telling a reader why a process that is no longer there
    // could not serve it.
    let companion = companion_path(database_path);
    // The same trust boundary the writer's own companion path applies: no
    // symlink is followed, and the inode this reader is looking at must be one
    // current-user-owned 0600 regular file that is STILL the file at this
    // pathname on both sides of the probe. Without that, a symlinked or
    // substituted companion could hand a reader a locked, checksummed
    // "the owner will not serve you" about a store nobody owns.
    let mut file = open_trusted_companion(&companion)?;
    let before_probe = file.metadata().ok()?;
    for _ in 0..RECORDED_OWNER_OBSERVATION_ATTEMPTS {
        validate_companion_metadata(&companion, &file, &before_probe, CompanionUse::Reading)
            .ok()?;
        // A writer takes the claim-window byte before replacing the previous
        // run's durable record. While that byte is held, the bytes below can
        // still describe the departed writer and must not be attributed to
        // the live holder merely because the whole-file writer lock is held.
        // The claim-window path is what waits for the arriving writer's own
        // decision; this status path deliberately has no answer yet.
        match query_companion_range_lock(&file, COMPANION_CLAIM_WINDOW_LOCK_OFFSET).ok()? {
            RangeLockOutcome::Taken => {}
            RangeLockOutcome::HeldByAnother => return None,
        }
        let before = active_companion_record(&mut file)?;
        let held = writer_holds_companion(&file, database_path)?;
        let after = active_companion_record(&mut file)?;
        validate_companion_metadata(&companion, &file, &before_probe, CompanionUse::Reading)
            .ok()?;
        match query_companion_range_lock(&file, COMPANION_CLAIM_WINDOW_LOCK_OFFSET).ok()? {
            RangeLockOutcome::Taken => {}
            RangeLockOutcome::HeldByAnother => return None,
        }
        if before.fields.writer_run_number != after.fields.writer_run_number
            || before.fields.database_identity != after.fields.database_identity
        {
            // The store changed hands around the probe. Nothing observed here
            // describes one writer, so it is observed again rather than
            // reported.
            continue;
        }
        return Some(ObservedWriterState {
            identity: after.fields.database_identity,
            run: after.fields.writer_run_number,
            held,
            status: after.fields.owner_read_status,
        });
    }
    None
}

pub(crate) fn recorded_held_owner_status(database_path: &Path) -> Option<OwnerReadStatus> {
    let observed = observed_writer_state(database_path)?;
    observed.held.then_some(observed.status)
}

/// What a live holder recorded when it decided not to serve inspection.
///
/// `Serving` is reached through the channel itself. `NotApplicable` is the
/// deliberately undecided placeholder published while a writer is inside its
/// claim window. Neither is an unserved-owner refusal.
pub(crate) fn recorded_unserved_owner(database_path: &Path) -> Option<OwnerReadStatus> {
    let status = recorded_held_owner_status(database_path)?;
    if matches!(
        status.state,
        OwnerServingState::Serving | OwnerServingState::NotApplicable
    ) {
        return None;
    }
    Some(status)
}

/// Take, or release, one advisory byte-range lock on an already-open trusted
/// companion.
///
/// The lock is tied to this open file description, so it survives every other
/// open and close of the same pathname in this process -- which is what lets a
/// reader and a writer inside ONE process contend exactly as two processes do,
/// and what makes a proof of this behavior a proof of the deployed behavior.
#[cfg(unix)]
pub(super) fn companion_range_lock(
    file: &File,
    offset: u64,
    lock_type: libc::c_short,
    blocking: bool,
) -> std::io::Result<RangeLockOutcome> {
    use std::os::unix::io::AsRawFd as _;

    let mut request: libc::flock = unsafe { std::mem::zeroed() };
    request.l_type = lock_type;
    request.l_whence = libc::SEEK_SET as libc::c_short;
    request.l_start = offset as libc::off_t;
    request.l_len = 1;
    let command = if blocking {
        libc::F_OFD_SETLKW
    } else {
        libc::F_OFD_SETLK
    };
    let answered = unsafe { libc::fcntl(file.as_raw_fd(), command, &request) };
    if answered != -1 {
        return Ok(RangeLockOutcome::Taken);
    }
    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EAGAIN) | Some(libc::EACCES) => Ok(RangeLockOutcome::HeldByAnother),
        _ => Err(error),
    }
}

#[cfg(not(unix))]
pub(super) fn companion_range_lock(
    _file: &File,
    _offset: u64,
    _lock_type: i16,
    _blocking: bool,
) -> std::io::Result<RangeLockOutcome> {
    Err(std::io::Error::other(
        "advisory byte-range coordination is not implemented on this platform",
    ))
}

#[cfg(unix)]
pub(super) fn take_companion_range_lock_shared(
    file: &File,
    offset: u64,
) -> std::io::Result<RangeLockOutcome> {
    companion_range_lock(file, offset, RangeLockMode::Shared.lock_type(), false)
}

#[cfg(unix)]
pub(super) fn take_companion_range_lock_exclusive(
    file: &File,
    offset: u64,
) -> std::io::Result<RangeLockOutcome> {
    companion_range_lock(file, offset, RangeLockMode::Exclusive.lock_type(), false)
}

/// Ask whether another description holds `offset` exclusive, holding
/// nothing. Asking can never make the holder's own request fail, which taking
/// the byte even momentarily could: a writer trying for the byte at the same
/// instant would be refused because somebody looked.
#[cfg(unix)]
pub(super) fn query_companion_range_lock(
    file: &File,
    offset: u64,
) -> std::io::Result<RangeLockOutcome> {
    use std::os::unix::io::AsRawFd as _;

    let mut request: libc::flock = unsafe { std::mem::zeroed() };
    request.l_type = libc::F_RDLCK as libc::c_short;
    request.l_whence = libc::SEEK_SET as libc::c_short;
    request.l_start = offset as libc::off_t;
    request.l_len = 1;
    // SAFETY: `request` is initialized and the descriptor stays owned by `file`.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_GETLK, &mut request) } == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(if request.l_type == libc::F_UNLCK as libc::c_short {
        RangeLockOutcome::Taken
    } else {
        RangeLockOutcome::HeldByAnother
    })
}

#[cfg(not(unix))]
pub(super) fn query_companion_range_lock(
    _file: &File,
    _offset: u64,
) -> std::io::Result<RangeLockOutcome> {
    Err(std::io::Error::other(
        "advisory byte-range coordination is not implemented on this platform",
    ))
}

#[cfg(unix)]
impl RangeLockMode {
    pub(super) fn lock_type(self) -> libc::c_short {
        match self {
            Self::Shared => libc::F_RDLCK as libc::c_short,
            Self::Exclusive => libc::F_WRLCK as libc::c_short,
        }
    }
}

#[cfg(unix)]
pub(super) fn block_on_companion_range_lock(
    file: &File,
    offset: u64,
    mode: RangeLockMode,
) -> std::io::Result<()> {
    companion_range_lock(file, offset, mode.lock_type(), true).map(|_| ())
}

#[cfg(unix)]
pub(super) fn release_companion_range_lock(file: &File, offset: u64) {
    let _ = companion_range_lock(file, offset, libc::F_UNLCK as libc::c_short, false);
}

/// Make letting go of a coordination byte something a waiter can hear.
///
/// An `fcntl` unlock writes no content, changes no metadata and closes no
/// descriptor, so it raises none of `IN_CLOSE`, `IN_MODIFY` or `IN_ATTRIB` --
/// the three notifications a coordination wait sleeps on. A waiter would then
/// wake only because the holder happened to write or close the file
/// afterwards, which the publication commit path does and the recovery probe
/// does not, and a waiter that armed after the holder's last write would sleep
/// to its own deadline and refuse a read the store had been ready to answer
/// almost the whole time. Stamping the file after the unlock raises
/// `IN_ATTRIB` without writing a byte or closing anything, so the release
/// itself is the event.
///
/// Only an EXCLUSIVE holder announces. A shared holder blocks nobody, and a
/// read must not pay a syscall to announce a release no one is waiting on.
#[cfg(unix)]
pub(super) fn announce_companion_release(file: &File) {
    use std::os::unix::io::AsRawFd as _;

    // Both stamps to "now", which is what `touch` does: no content is written
    // and no descriptor is closed, and the kernel raises `IN_ATTRIB`.
    let now = [
        libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_NOW,
        },
        libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_NOW,
        },
    ];
    // Best effort by construction: a stamp that cannot be made leaves the
    // waiter exactly where it was before this existed -- waiting on its own
    // deadline -- and never fails a release that has already happened.
    unsafe {
        libc::futimens(file.as_raw_fd(), now.as_ptr());
    }
}

#[cfg(not(unix))]
pub(super) fn announce_companion_release(_file: &File) {}

#[cfg(not(unix))]
pub(super) fn take_companion_range_lock_shared(
    _file: &File,
    _offset: u64,
) -> std::io::Result<RangeLockOutcome> {
    Err(std::io::Error::other(
        "advisory byte-range coordination is not implemented on this platform",
    ))
}

#[cfg(not(unix))]
pub(super) fn take_companion_range_lock_exclusive(
    _file: &File,
    _offset: u64,
) -> std::io::Result<RangeLockOutcome> {
    Err(std::io::Error::other(
        "advisory byte-range coordination is not implemented on this platform",
    ))
}

#[cfg(not(unix))]
pub(super) fn release_companion_range_lock(_file: &File, _offset: u64) {}

#[cfg(unix)]
impl WakePipe {
    pub(super) fn new() -> std::io::Result<Self> {
        use std::os::fd::FromRawFd as _;

        let mut ends = [0_i32; 2];
        #[cfg(any(
            target_os = "linux",
            target_os = "android",
            target_os = "freebsd",
            target_os = "netbsd",
            target_os = "openbsd",
            target_os = "dragonfly"
        ))]
        let made = unsafe { libc::pipe2(ends.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
        // Without pipe2 (macOS) the flags are set on each end right after creation.
        #[cfg(not(any(
            target_os = "linux",
            target_os = "android",
            target_os = "freebsd",
            target_os = "netbsd",
            target_os = "openbsd",
            target_os = "dragonfly"
        )))]
        let made = {
            let made = unsafe { libc::pipe(ends.as_mut_ptr()) };
            if made == 0 {
                for end in ends {
                    let flags = unsafe { libc::fcntl(end, libc::F_GETFL) };
                    let marked =
                        unsafe { libc::fcntl(end, libc::F_SETFL, flags | libc::O_NONBLOCK) };
                    let closed = unsafe { libc::fcntl(end, libc::F_SETFD, libc::FD_CLOEXEC) };
                    if flags < 0 || marked < 0 || closed < 0 {
                        let error = std::io::Error::last_os_error();
                        unsafe {
                            libc::close(ends[0]);
                            libc::close(ends[1]);
                        }
                        return Err(error);
                    }
                }
            }
            made
        };
        if made != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self {
            reader: unsafe { std::os::fd::OwnedFd::from_raw_fd(ends[0]) },
            writer: unsafe { std::os::fd::OwnedFd::from_raw_fd(ends[1]) },
        })
    }

    pub(super) fn wake(&self) {
        use std::os::fd::AsRawFd as _;

        let byte = 1_u8;
        let _ = unsafe {
            libc::write(
                self.writer.as_raw_fd(),
                std::ptr::from_ref(&byte).cast::<libc::c_void>(),
                1,
            )
        };
    }

    pub(super) fn drain(&self) {
        use std::os::fd::AsRawFd as _;

        let mut scratch = [0_u8; 64];
        loop {
            let read = unsafe {
                libc::read(
                    self.reader.as_raw_fd(),
                    scratch.as_mut_ptr().cast::<libc::c_void>(),
                    scratch.len(),
                )
            };
            if read <= 0 {
                return;
            }
        }
    }
}

#[cfg(unix)]
impl std::task::Wake for PipeWaker {
    fn wake(self: Arc<Self>) {
        self.0.wake();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.wake();
    }
}
#[cfg(unix)]
impl From<std::io::Error> for CoordinationWatchFailure {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[cfg(unix)]
impl CoordinationWatch {
    /// Arm the watch BEFORE asking for the byte, so a release that happens
    /// between the ask and the sleep is already queued rather than missed.
    pub(super) fn arm(
        companion: &Path,
        stop: Option<&OwnerReadCancellation>,
    ) -> std::io::Result<Self> {
        Self::arm_with_listener(companion, |wake| {
            Ok(stop.map(|stop| {
                let told = Arc::clone(wake);
                stop.tell_on_cancel(move || told.wake())
            }))
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn arm_funded(
        companion: &Path,
        stop: &OwnerReadCancellation,
    ) -> std::result::Result<Self, CoordinationWatchFailure> {
        Self::arm_with_listener(companion, |wake| {
            let told = Arc::clone(wake);
            stop.try_tell_on_cancel(move || told.wake())
                .map(Some)
                .map_err(CoordinationWatchFailure::Admission)
        })
    }

    pub(super) fn arm_with_listener<E: From<std::io::Error>>(
        companion: &Path,
        listener: impl FnOnce(
            &Arc<WakePipe>,
        ) -> std::result::Result<
            Option<contextdb_core::read_contract::CancellationListener>,
            E,
        >,
    ) -> std::result::Result<Self, E> {
        let wake = Arc::new(WakePipe::new()?);
        #[cfg(any(test, feature = "test-seams"))]
        read_persistence_test_scaffold::note_coordination_wake_pipe_for_test();
        let listener = listener(&wake)?;
        #[cfg(target_os = "linux")]
        let notifications = companion_notifications(companion)?;
        #[cfg(not(target_os = "linux"))]
        let _ = companion;
        Ok(Self {
            wake,
            #[cfg(target_os = "linux")]
            notifications,
            _listener: listener,
        })
    }

    pub(super) fn waker(&self) -> std::task::Waker {
        std::task::Waker::from(Arc::new(PipeWaker(Arc::clone(&self.wake))))
    }

    /// Sleep until something about this companion happens, or until somebody
    /// pokes the wake pipe. On a platform with no notification channel the
    /// caller's own thread takes the blocking acquisition instead: the answer
    /// is the same and nothing is left running either way, but a stop is
    /// noticed at the next wake rather than at once.
    #[cfg(target_os = "linux")]
    pub(super) fn sleep(
        &self,
        held: &File,
        offset: u64,
        timeout: Option<std::time::Duration>,
    ) -> std::io::Result<()> {
        use std::os::fd::AsRawFd as _;

        let _ = (held, offset);
        let mut watched = [
            libc::pollfd {
                fd: self.notifications.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: self.wake.reader.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        loop {
            let timeout_ms = timeout
                .map(|duration| duration.as_millis().max(1).min(i32::MAX as u128) as i32)
                .unwrap_or(-1);
            let ready = unsafe { libc::poll(watched.as_mut_ptr(), 2, timeout_ms) };
            if ready >= 0 {
                break;
            }
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::Interrupted {
                return Err(error);
            }
            if timeout.is_some() {
                break;
            }
        }
        drain_companion_notifications(&self.notifications);
        self.wake.drain();
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    pub(super) fn sleep(
        &self,
        held: &File,
        offset: u64,
        timeout: Option<std::time::Duration>,
    ) -> std::io::Result<()> {
        if let Some(timeout) = timeout {
            use std::os::fd::AsRawFd as _;
            let mut watched = libc::pollfd {
                fd: self.wake.reader.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            let timeout_ms = timeout.as_millis().max(1).min(i32::MAX as u128) as i32;
            loop {
                let ready = unsafe { libc::poll(&mut watched, 1, timeout_ms) };
                if ready >= 0 {
                    self.wake.drain();
                    return Ok(());
                }
                let error = std::io::Error::last_os_error();
                if error.kind() != std::io::ErrorKind::Interrupted {
                    return Err(error);
                }
                return Ok(());
            }
        }
        self.wake.drain();
        block_on_companion_range_lock(held, offset, RangeLockMode::Exclusive)?;
        release_companion_range_lock(held, offset);
        Ok(())
    }
}

/// Watch a store's companion for the things that mean a coordination byte may
/// have changed hands: a holder closing its descriptor, which is what letting
/// go of a hold looks like from outside and is what the kernel does for a
/// holder that dies, and a writer publishing into the record.
#[cfg(target_os = "linux")]
pub(super) fn companion_notifications(companion: &Path) -> std::io::Result<std::os::fd::OwnedFd> {
    use std::os::fd::FromRawFd as _;
    use std::os::unix::ffi::OsStrExt as _;

    let started = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
    if started < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let notifications = unsafe { std::os::fd::OwnedFd::from_raw_fd(started) };
    let pathname = std::ffi::CString::new(companion.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("companion pathname contains an interior NUL"))?;
    let watched = unsafe {
        use std::os::fd::AsRawFd as _;
        libc::inotify_add_watch(
            notifications.as_raw_fd(),
            pathname.as_ptr(),
            libc::IN_CLOSE | libc::IN_MODIFY | libc::IN_ATTRIB,
        )
    };
    if watched < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(notifications)
}

/// Take every queued notification before re-asking for the byte, so a burst of
/// them costs one wake rather than one wake each.
#[cfg(target_os = "linux")]
pub(super) fn drain_companion_notifications(notifications: &std::os::fd::OwnedFd) {
    use std::os::fd::AsRawFd as _;

    let mut scratch = [0_u8; 4096];
    loop {
        let read = unsafe {
            libc::read(
                notifications.as_raw_fd(),
                scratch.as_mut_ptr().cast::<libc::c_void>(),
                scratch.len(),
            )
        };
        if read <= 0 {
            return;
        }
    }
}

/// A writer that holds this store and has not yet published what it decided
/// about serving inspection.
///
/// The question is asked of the CLAIM ITSELF, not of anything published: a
/// writer holds the companion's claim-window byte from the moment it first
/// takes the store until its decision is there to be read, and the kernel
/// takes that hold away if the writer dies. So a held byte means "somebody
/// owns this store and has not answered yet" no matter what the record says --
/// including when the record still describes a PREVIOUS run, and when there is
/// no record at all because the store is being created. Waiting for a
/// published `not_applicable` would have missed both, which is exactly the
/// stretch in which a caller was being told the store was free.
///
/// The store file is not opened, read, or locked, and nothing here delays the
/// writer: the companion beside the store carries the whole answer, and asking
/// for a shared byte a writer holds exclusively never stands in that writer's
/// way.
pub(crate) fn observe_unsettled_claim(database_path: &Path) -> Option<UnsettledClaim> {
    let companion = companion_path(database_path);
    // The same trust boundary every other companion reader applies: no symlink
    // followed, one current-user-owned regular file, still the file at this
    // pathname on both sides of the probe.
    let file = open_trusted_companion(&companion)?;
    let before_probe = file.metadata().ok()?;
    validate_companion_metadata(&companion, &file, &before_probe, CompanionUse::Reading).ok()?;
    let held = match query_companion_range_lock(&file, COMPANION_CLAIM_WINDOW_LOCK_OFFSET) {
        Ok(RangeLockOutcome::Taken) => false,
        Ok(RangeLockOutcome::HeldByAnother) => true,
        Err(_) => return None,
    };
    validate_companion_metadata(&companion, &file, &before_probe, CompanionUse::Reading).ok()?;
    held.then_some(UnsettledClaim {
        companion: file,
        pathname: companion,
    })
}

/// Wait out a writer's claim window inside the budget the caller declared.
///
/// Nothing polls and nothing sleeps for an interval: the caller's thread sleeps
/// on the companion's own notifications and re-asks for the byte each time one
/// arrives, so the writer publishing its decision IS what wakes this. The other
/// thing that may end it is the caller's own declared budget, and that budget
/// is spent against the CLOCK the caller supplied -- never against real time --
/// so a proof advances a clock instead of waiting an hour.
///
/// The writer is never blocked or delayed: a shared request never stands in the
/// way of the exclusive hold it already owns, and the store file is untouched.
pub(crate) fn wait_for_claim_settlement(
    claim: UnsettledClaim,
    budget_ms: u64,
    clock: &dyn contextdb_core::read_contract::DeadlineClock,
    cancellation: Option<&OwnerReadCancellation>,
) -> ClaimSettlement {
    #[cfg(not(unix))]
    {
        let _ = (claim, budget_ms, clock, cancellation);
        return ClaimSettlement::StillHeld;
    }
    #[cfg(unix)]
    {
        let UnsettledClaim {
            companion,
            pathname,
        } = claim;
        let deadline_ms = clock.now_ms().saturating_add(budget_ms);
        let Ok(watch) = CoordinationWatch::arm(&pathname, cancellation) else {
            return ClaimSettlement::StillHeld;
        };
        let waker = watch.waker();
        let mut context = std::task::Context::from_waker(&waker);
        let mut expiry = clock.wait_until(deadline_ms);
        loop {
            if cancellation.is_some_and(OwnerReadCancellation::is_cancelled) {
                return ClaimSettlement::Cancelled;
            }
            match query_companion_range_lock(&companion, COMPANION_CLAIM_WINDOW_LOCK_OFFSET) {
                Ok(RangeLockOutcome::Taken) => {
                    return ClaimSettlement::Settled;
                }
                Ok(RangeLockOutcome::HeldByAnother) => {}
                Err(_) => return ClaimSettlement::StillHeld,
            }
            if std::pin::Pin::as_mut(&mut expiry)
                .poll(&mut context)
                .is_ready()
            {
                return ClaimSettlement::StillHeld;
            }
            if watch
                .sleep(&companion, COMPANION_CLAIM_WINDOW_LOCK_OFFSET, None)
                .is_err()
            {
                return ClaimSettlement::StillHeld;
            }
        }
    }
}

/// Open a companion the way every trusted holder of one opens it: no symlink
/// followed, not inherited across an exec, and read-only because a reader has
/// no business writing one.
#[cfg(unix)]
pub(super) fn open_trusted_companion(companion: &Path) -> Option<File> {
    use std::os::unix::fs::OpenOptionsExt as _;

    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(companion)
        .ok()
}

#[cfg(not(unix))]
pub(super) fn open_trusted_companion(companion: &Path) -> Option<File> {
    File::open(companion).ok()
}

/// Open a companion the way a would-be WRITER of this store opens one to ask
/// whether readers are still holding it: no symlink followed, not inherited
/// across an exec, and readable AND writable, because the exclusive side of an
/// advisory byte-range lock is only available on a descriptor open for writing.
/// Nothing is ever written through it.
#[cfg(unix)]
pub(super) fn open_trusted_companion_for_exclusive_hold(companion: &Path) -> std::io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt as _;

    OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(companion)
}

#[cfg(not(unix))]
pub(super) fn open_trusted_companion_for_exclusive_hold(companion: &Path) -> std::io::Result<File> {
    OpenOptions::new().read(true).write(true).open(companion)
}

impl ReaderStoreHold {
    /// Take the hold, waiting out the momentary exclusive request a waiter
    /// makes. Waiting here is what stops a reader arriving in that instant
    /// from holding the store with nothing to show for it -- which is the
    /// invisible hold this whole mechanism exists to abolish.
    pub(super) fn take(database_path: &Path) -> Option<Self> {
        let companion = companion_path(database_path);
        let file = open_trusted_companion(&companion)?;
        block_on_companion_range_lock(
            &file,
            COMPANION_READER_HOLD_LOCK_OFFSET,
            RangeLockMode::Shared,
        )
        .ok()?;
        Some(Self { file })
    }
}

impl Drop for ReaderStoreHold {
    fn drop(&mut self) {
        release_companion_range_lock(&self.file, COMPANION_READER_HOLD_LOCK_OFFSET);
    }
}

/// The published record in the companion this file is open on.
pub(super) fn active_companion_record(file: &mut File) -> Option<EncodedCompanionRecord> {
    read_companion_state(file).ok()?.active_record
}

/// Whether a writer is holding this store right now, asked of the advisory
/// companion this file is already open on.
///
/// A reader is the only caller, and a reader must not open the store to find
/// out: taking the shared side of the same lock a writer takes exclusively
/// answers it and is released immediately.
pub(super) fn writer_holds_companion(file: &File, database_path: &Path) -> Option<bool> {
    #[cfg(not(any(test, feature = "test-seams")))]
    let _ = database_path;
    let held = match query_companion_range_lock(file, COMPANION_OWNER_LOCK_OFFSET).ok()? {
        RangeLockOutcome::Taken => false,
        RangeLockOutcome::HeldByAnother => true,
    };
    // The probe holds nothing here: it asked, it did not take.
    #[cfg(any(test, feature = "test-seams"))]
    read_persistence_test_scaffold::pause_reader_at_probe(database_path);
    Some(held)
}

pub(super) fn inspect_companion_record(
    database_path: &Path,
) -> std::result::Result<EncodedCompanionRecord, String> {
    read_active_companion_slot(database_path)
        .and_then(|(payload, checksum)| recover_companion_record(&payload, checksum))
}

pub(super) fn read_active_companion_slot(
    database_path: &Path,
) -> std::result::Result<(Vec<u8>, [u8; 32]), String> {
    let path = companion_path(database_path);
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let mut file = options
        .open(&path)
        .map_err(|error| format!("open companion {}: {error}", path.display()))?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("inspect companion {}: {error}", path.display()))?;
    validate_companion_metadata(&path, &file, &metadata, CompanionUse::Reading)
        .map_err(|error| error.to_string())?;
    let state = read_companion_state(&mut file)?;
    let record = state
        .active_record
        .ok_or_else(|| "companion has no published record".to_owned())?;
    Ok((record.checksummed_payload, record.stored_checksum))
}

impl<T> SinkQueueWindowRead<T> {
    pub(super) fn empty(depth: u64, newest_id: u64) -> Self {
        Self {
            depth,
            newest_id,
            entries: Vec::new(),
            credits: Vec::new(),
            owner: None,
            exhausted: true,
            refusal: None,
        }
    }
}

impl ReadHistoryProofOwner {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn new(
        memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Arc<Self>> {
        let memory: contextdb_core::read_memory::ReadMemoryOwner = memory.into();

        let control = contextdb_core::read_memory::ReadCredit::try_new(
            memory,
            std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>(),
            "reader",
            "history_proof_controls",
            "Reduce the requested history or raise the declared reader memory limit.",
        )?;
        Ok(Arc::new(Self {
            credit: parking_lot::Mutex::new(control),
            refusal: parking_lot::Mutex::new(None),
        }))
    }
}

impl content_identity::ProofPoolGrowth for ReadHistoryProofOwner {
    fn grow(&self, bytes: usize) -> bool {
        match self.credit.lock().try_grow_for(
            bytes,
            "reader",
            "authenticated_history_range",
            "Reduce the requested history or raise the declared reader memory limit.",
        ) {
            Ok(()) => true,
            Err(error) => {
                *self.refusal.lock() = Some(error);
                false
            }
        }
    }

    fn ceiling(&self) -> usize {
        READ_HISTORY_PROOF_POOL_BYTES
    }
}

impl ReadPersistenceImage {
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from read_content_test_adapter.rs"
        )
    )]
    pub(crate) fn content_identity_for_test(&self) -> DirectTypedImageDigest {
        self.content_identity.clone()
    }

    /// The newest committed transaction this image's snapshot holds, whether
    /// the open kept the commit index or left it in the store.
    #[cfg(test)]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from ordered_run_integrity_tests.rs"
        )
    )]
    pub(crate) fn newest_committed_tx_for_test(&self) -> Option<TxId> {
        self.commit_index.values().max().copied().or_else(|| {
            self.history_on_request
                .as_ref()
                .and_then(|history| history.commit_index_last.map(|(_, tx)| tx))
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from read_content_test_adapter.rs"
        )
    )]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(crate) fn hydrate_logical_content_for_test(
        &mut self,
        retained_source: &RetainedReadSource,
    ) -> Result<()> {
        let _decode_scope = self.memory.enter();
        // History an open leaves in the store, read through the same
        // admitted, authenticated on-request path a reader uses.
        if self.history_on_request.is_some() {
            let checkpoint = || self.open_context.checkpoint();
            let read_memory = self.open_context.read_memory();
            let history = retained_source
                .load_history(
                    &Arc::new(crate::memory_accounting::MemoryAccountant::no_limit()).into(),
                    &checkpoint,
                    read_memory.clone(),
                )
                .map_err(crate::database::ImageHistoryError::into_engine_error)?;
            self.change_log = history.change_log;
            self.ddl_log = history.ddl_log;
            let mut lower = std::ops::Bound::Unbounded;
            self.commit_index.clear();
            while let Some((lsn, tx)) =
                retained_source.commit_entry_from(lower, false, read_memory.clone(), &checkpoint)?
            {
                self.memory.reserve(
                    2 * (11 * std::mem::size_of::<(Lsn, TxId)>()
                        + 16 * std::mem::size_of::<usize>()
                        + crate::sync_types::held::ALLOCATION_BYTES),
                )?;
                self.commit_index.insert(lsn, tx);
                lower = std::ops::Bound::Excluded(lsn);
            }
        }
        if self.lazy_relational {
            let (tables, vector_entries, current_vectors) = retained_source
                .compatibility_vector_projection_for_test(&self.table_meta, &self.ddl_log)?;
            self.relational_tables = tables;
            self.vector_entries = vector_entries;
            self.current_vectors = current_vectors;
        }
        // Audit records and queued payloads an open leaves in the store.
        let history = |error: LoadReadImageError| Error::Other((error.to_string()).into());
        let (config_values, sink_audit, trigger_audit, trigger_audit_stamps, sink_queues) =
            retained_source.with_read(|read_txn| {
                Ok((
                    decode_read_raw_string_table(read_txn, CONFIG_TABLE, None).map_err(history)?,
                    decode_read_raw_string_table(read_txn, SINK_AUDIT_TABLE, None)
                        .map_err(history)?,
                    decode_read_raw_string_table(read_txn, TRIGGER_AUDIT_TABLE, None)
                        .map_err(history)?,
                    decode_read_trigger_audit_stamps(read_txn).map_err(history)?,
                    decode_read_sink_queues(read_txn).map_err(history)?,
                ))
            })?;
        self.config_values = config_values;
        self.sink_audit = sink_audit;
        self.trigger_audit = trigger_audit;
        self.trigger_audit_stamps = trigger_audit_stamps;
        self.sink_queues = sink_queues;
        Ok(())
    }

    pub(crate) fn into_runtime_parts(
        self,
        retained_source: RetainedReadSource,
    ) -> ReadPersistenceImageParts {
        ReadPersistenceImageParts {
            open_context: self.open_context,
            content_identity: self.content_identity,
            memory: self.memory,
            table_meta: self.table_meta,
            relational_tables: self.relational_tables,
            lazy_relational: self.lazy_relational,
            read_bounds: self.read_bounds,
            vector_entries: self.vector_entries,
            raw_vector_memberships: self.raw_vector_memberships,
            current_vectors: self.current_vectors,
            sync_source_lsns: self.sync_source_lsns,
            sync_source_kinds: self.sync_source_kinds,
            change_log: self.change_log,
            ddl_log: self.ddl_log,
            commit_index: self.commit_index,
            history_on_request: self.history_on_request,
            config_values: self.config_values,
            sink_audit: self.sink_audit,
            trigger_audit: self.trigger_audit,
            trigger_audit_stamps: self.trigger_audit_stamps,
            sink_queues: self.sink_queues,
            sink_queue_extents: self.sink_queue_extents,
            vector_generation_catalog: self.vector_generation_catalog,
            retained_source,
            source_path: self
                .source_path
                .expect("a finished read image carries its released source path"),
            store_file_bytes: self.store_file_bytes,
        }
    }
}

impl ReadPersistenceReleaseReceipt {
    pub(crate) fn breadcrumb_path(&self) -> Option<&Path> {
        self.breadcrumb_path.as_deref()
    }

    pub(crate) fn source_accesses(&self) -> u64 {
        self.source_accesses
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(crate) fn validate_released(&self) -> std::result::Result<(), LoadReadImageError> {
        if self.source_accesses == 0 {
            return Err(LoadReadImageError::Release(
                "sealed release receipt records no persistence source access".to_owned(),
            ));
        }
        let Some(path) = &self.breadcrumb_path else {
            return Ok(());
        };
        match std::fs::symlink_metadata(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(LoadReadImageError::Release(format!(
                "cannot verify removal of reader breadcrumb {}: {error}",
                path.display()
            ))),
            Ok(_) => Err(LoadReadImageError::Release(format!(
                "reader breadcrumb still exists after source release: {}",
                path.display()
            ))),
        }
    }
}

impl ReadPersistenceLoad {
    pub(crate) fn into_parts(
        self,
    ) -> (
        ReadPersistenceImage,
        ReadPersistenceReleaseReceipt,
        RetainedReadSource,
    ) {
        (self.image, self.release_receipt, self.source)
    }
}

impl LoadReadImageError {
    #[cfg(test)]
    pub(crate) fn kind(&self) -> LoadReadImageErrorKind {
        match self {
            Self::Memory(_) => LoadReadImageErrorKind::Memory,
            Self::Release(_) => LoadReadImageErrorKind::Release,
            Self::RequiresWriter { cause, .. } => LoadReadImageErrorKind::RequiresWriter(*cause),
            Self::StoreMissing { .. } => LoadReadImageErrorKind::StoreMissing,
            Self::Contention { .. } | Self::RegistryContention { .. } => {
                LoadReadImageErrorKind::Contention
            }
            Self::Corrupt { cause, .. } => LoadReadImageErrorKind::Corrupt(*cause),
            Self::InvalidState { .. } => LoadReadImageErrorKind::InvalidState,
            Self::Io { .. } | Self::IoAtPath { .. } => LoadReadImageErrorKind::Io,
            #[cfg(unix)]
            Self::WaitIo { .. } => LoadReadImageErrorKind::Io,
        }
    }

    /// The reader-facing answer to a refused hydration.
    ///
    /// Persistence owns the damage vocabulary, so it also owns the single
    /// translation of that vocabulary into the words a reader is given: what
    /// is wrong with the store, and whether a writable reopen is the way out.
    /// Keeping the translation here is what lets the reading surface stay
    /// closed over its own types instead of learning how persistence spells a
    /// decode failure.
    pub(crate) fn into_direct_reader_error(
        self,
    ) -> crate::direct_file_reader::DirectFileReaderError {
        use crate::direct_file_reader::{
            DirectFileReaderError, DirectRepairRequiredCause, DirectStoreDiagnostic,
        };

        // Memory refusals must not format (or copy static labels) on their way
        // out. Wait diagnostics already own a cell prepaid by the actual source.
        match self {
            Self::Memory(Error::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
            }) => DirectFileReaderError::MemoryBudget {
                subsystem,
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
            },
            Self::Memory(Error::ReadCancelled) => DirectFileReaderError::Cancelled,
            Self::Memory(Error::ReadFailure(failure)) => {
                DirectFileReaderError::ReadFailure(failure)
            }
            Self::Memory(error) => crate::read_image::history_answer_error(error),
            error @ Self::Corrupt {
                cause: ReadImageCorruptionCause::MalformedRecord,
                ..
            } => DirectFileReaderError::CorruptStore(
                DirectStoreDiagnostic::MalformedRecord,
                error.to_string().into(),
            ),
            error @ Self::Corrupt {
                cause: ReadImageCorruptionCause::LegacyLayout,
                ..
            } => DirectFileReaderError::LegacyLayout(
                DirectStoreDiagnostic::LegacyLayout,
                error.to_string().into(),
            ),
            error @ Self::RequiresWriter { cause, .. } => {
                let reason = error.to_string();
                let cause = match cause {
                    ReadImageRequiresWriterCause::NonMonotonicCommitIndex => {
                        DirectRepairRequiredCause::NonMonotonicCommitIndex
                    }
                    ReadImageRequiresWriterCause::InvalidVectorReference => {
                        DirectRepairRequiredCause::InvalidVectorReference
                    }
                    ReadImageRequiresWriterCause::RowVectorDivergence => {
                        DirectRepairRequiredCause::RowVectorDivergence
                    }
                    ReadImageRequiresWriterCause::WritableSanitizerWouldChange => {
                        DirectRepairRequiredCause::WritableSanitizerWouldChange
                    }
                    ReadImageRequiresWriterCause::MissingCommitIndex => {
                        DirectRepairRequiredCause::MissingCommitIndex
                    }
                };
                let failure = ReadFailure::new(
                    ReadFailureKind::DirectReadRequiresWriter,
                    ReadFailureDetail::Reason { reason },
                )
                .expect("direct-read refusal carries a plain reason");
                DirectFileReaderError::DirectReadRequiresWriter { failure, cause }
            }
            error @ Self::StoreMissing { .. } => DirectFileReaderError::StoreNotFound {
                failure: ReadFailure::new(
                    ReadFailureKind::StoreNotFound,
                    ReadFailureDetail::Reason {
                        reason: error.to_string(),
                    },
                )
                .expect("a missing-store refusal carries a plain reason"),
            },
            error @ (Self::Contention { .. } | Self::RegistryContention { .. }) => {
                DirectFileReaderError::Contended {
                    reason: error.to_string(),
                }
            }
            // A release proof that failed, durable state that is invalid and an
            // I/O error are each something that WENT WRONG, with words saying
            // what; the caller gets that sentence, never a "prerequisite
            // missing" label.
            error @ (Self::Release(_)
            | Self::InvalidState { .. }
            | Self::Io { .. }
            | Self::IoAtPath { .. }) => DirectFileReaderError::Engine(error.to_string().into()),
            #[cfg(unix)]
            error @ Self::WaitIo { .. } => DirectFileReaderError::Engine(error.to_string().into()),
        }
    }
}

/// Test adapters resolve the established session limits and clock here;
/// production callers must supply their active open context.
#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
pub(crate) fn load_read_image(
    path: &Path,
) -> std::result::Result<ReadPersistenceLoad, LoadReadImageError> {
    load_read_image_with_memory(path, None)
}

#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
pub(crate) fn load_read_image_with_memory(
    path: &Path,
    limit: Option<usize>,
) -> std::result::Result<ReadPersistenceLoad, LoadReadImageError> {
    let limits = crate::read_session::ReadSessionOptions::default().limits;
    // Match the file reader: a declared image limit owns its parent budget;
    // the default statement limit applies only when no image limit is supplied.
    let budget = limit.map_or(limits.memory, |bytes| {
        u64::try_from(bytes).unwrap_or(u64::MAX)
    });
    let declared = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(budget)
        .map_err(LoadReadImageError::Memory)?;
    let open_context = crate::executor::bounded::ReadOpenContext::new(
        declared.child().map_err(LoadReadImageError::Memory)?,
        limits,
        crate::read_session::read_clock(),
        OwnerReadCancellation::new(),
    )
    .map_err(LoadReadImageError::Memory)?;
    load_read_image_at_registration_with_context(path, limit, None, None, open_context)
}

/// Production hydration retains the supplied active context through target
/// construction. Storage pages use its existing declared reader child.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from empty_declared_index_readiness_tests.rs"
    )
)]
pub(crate) fn load_read_image_at_registration_with_context(
    path: &Path,
    limit: Option<usize>,
    runtime_dir: Option<&Path>,
    registration: Option<ReadSnapshotRegistrationGuard>,
    open_context: crate::executor::bounded::ReadOpenContext,
) -> std::result::Result<ReadPersistenceLoad, LoadReadImageError> {
    load_read_image_at_registration_with_cache_share(
        path,
        limit,
        None,
        runtime_dir,
        registration,
        open_context,
    )
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn load_read_image_at_registration_with_cache_share(
    path: &Path,
    limit: Option<usize>,
    cache_share: Option<u8>,
    runtime_dir: Option<&Path>,
    registration: Option<ReadSnapshotRegistrationGuard>,
    open_context: crate::executor::bounded::ReadOpenContext,
) -> std::result::Result<ReadPersistenceLoad, LoadReadImageError> {
    open_context
        .checkpoint()
        .map_err(LoadReadImageError::Memory)?;
    if let Some(percent) = cache_share
        && percent > 100
    {
        return Err(LoadReadImageError::Memory(Error::InvalidSetting {
            name: "READ_CACHE_SHARE",
            value: percent.to_string(),
            allowed: "0..=100, an integer percent".to_owned(),
        }));
    }
    let budget = limit.unwrap_or_else(|| open_context.memory_limit());
    let read_memory = open_context.read_memory();
    let memory =
        crate::read_image_memory::ImageMemory::new_direct_reader(limit, read_memory.clone())
            .map_err(LoadReadImageError::Memory)?;
    // The persistence cache coexists with decoded records and later lazy page
    // reads. Admit its fixed ceiling before Redb opens and retain that charge
    // for the source handle's lifetime.
    let cache_bytes = cache_share.map_or_else(
        || redb_cache_bytes_for_limit(Some(budget)),
        |percent| redb_reader_cache_bytes(budget, Some(percent)),
    );
    let source_bytes = cache_bytes.saturating_add(read_source_fixed_reservation_bytes());
    #[cfg(test)]
    let source_observation = direct_source_contract_rows::begin(0, &memory);
    let source_memory = SourceMemoryReservation::new(memory.clone(), source_bytes)
        .map_err(LoadReadImageError::Memory)?;
    source_memory.configure_cache(cache_bytes);
    #[cfg(test)]
    drop(source_observation);
    let result = memory.scope(|| {
        let mut source = ReadImageSource::start(
            path,
            cache_bytes,
            budget.saturating_sub(read_source_fixed_reservation_bytes()),
            source_memory,
            runtime_dir,
            registration,
            Some(&open_context),
        )?;
        open_context
            .checkpoint()
            .map_err(LoadReadImageError::Memory)?;
        #[cfg(any(test, feature = "test-seams"))]
        read_persistence_test_scaffold::pause_reader_before_decode(path);
        let image = graph_read::with_pages_result(
            read_memory.clone(),
            || {
                source.with_read_transaction(|tx| {
                    decode_complete_read_image(tx, path, memory.clone(), open_context.clone())
                })
            },
            LoadReadImageError::Memory,
        )?;
        open_context
            .checkpoint()
            .map_err(LoadReadImageError::Memory)?;
        graph_read::with_pages_result(
            read_memory,
            || source.finish(image),
            LoadReadImageError::Memory,
        )
    });
    if let Some(error) = memory.take_failure() {
        return Err(LoadReadImageError::Memory(error));
    }
    result
}

/// Sole identity source for the companion record's owner-user field. The
/// record writer must not substitute database-file metadata for this value.
pub(super) fn companion_effective_user_identity() -> LocalUserIdentity {
    #[cfg(any(test, feature = "test-seams"))]
    read_persistence_test_scaffold::note_companion_effective_user_source_call_for_test();
    #[cfg(any(test, feature = "test-seams"))]
    if let Some(identity) =
        read_persistence_test_scaffold::companion_effective_user_override_for_test()
    {
        return identity;
    }
    #[cfg(unix)]
    {
        LocalUserIdentity(nix::unistd::Uid::effective().as_raw() as u64)
    }
    #[cfg(not(unix))]
    {
        LocalUserIdentity(0)
    }
}

pub(super) fn encode_channel_address(address: ChannelAddress) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(address.0.len() * 2);
    for byte in address.0 {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

/// Canonical runtime directory shared by the per-hydration breadcrumb files
/// for one database: the directory beside that database's channel socket.
/// Both the runtime directory and the address are derived by local transport
/// from the runtime ROOT every caller here is handed, so a reader publishing
/// its identity and a writer looking for it reach the same directory.
pub(super) fn reader_breadcrumb_directory(
    database_path: &Path,
    runtime_root: &Path,
) -> std::io::Result<std::path::PathBuf> {
    let address = derive_channel_address(database_path)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    Ok(reader_breadcrumb_root(runtime_root).join(encode_channel_address(address)))
}

/// A registration lives directly inside each candidate coordination directory.
/// The platform default has a `contextdb` child; a supplied directory is used
/// as given when it participates.
pub(super) fn reader_breadcrumb_root(runtime_root: &Path) -> PathBuf {
    runtime_root.to_path_buf()
}

pub(super) fn safe_reader_process_display_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_READER_PROCESS_NAME_BYTES
        && name
            .bytes()
            .all(|byte| matches!(byte, b' '..=b'~') && byte != b'/' && byte != b'\\')
}

pub(super) fn encode_reader_breadcrumb(breadcrumb: &ReaderBreadcrumb) -> std::io::Result<Vec<u8>> {
    let name = breadcrumb.process_name.as_bytes();
    if name.len() > MAX_READER_PROCESS_NAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "reader process name exceeds the breadcrumb limit",
        ));
    }
    let name_len = u32::try_from(name.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "reader process name is too long",
        )
    })?;
    let mut bytes = Vec::with_capacity(READER_BREADCRUMB_MAGIC.len() + 4 + 8 + 4 + name.len());
    bytes.extend_from_slice(READER_BREADCRUMB_MAGIC);
    bytes.extend_from_slice(&breadcrumb.process_id.to_le_bytes());
    bytes.extend_from_slice(&breadcrumb.process_start.0.to_le_bytes());
    bytes.extend_from_slice(&name_len.to_le_bytes());
    bytes.extend_from_slice(name);
    Ok(bytes)
}

pub(super) fn decode_reader_breadcrumb(bytes: &[u8]) -> std::io::Result<ReaderBreadcrumb> {
    let mut cursor = CompanionPayloadCursor {
        payload: bytes,
        offset: 0,
    };
    if cursor
        .take(READER_BREADCRUMB_MAGIC.len())
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?
        != READER_BREADCRUMB_MAGIC
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "reader breadcrumb magic mismatch",
        ));
    }
    let process_id = cursor
        .u32()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let process_start = contextdb_core::read_contract::ProcessStartIdentity(
        cursor
            .u64()
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?,
    );
    let name_len = usize::try_from(
        cursor
            .u32()
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?,
    )
    .map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "reader process-name length does not fit usize",
        )
    })?;
    let process_name = String::from_utf8(
        cursor
            .take(name_len)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?
            .to_vec(),
    )
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    cursor
        .finish()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    Ok(ReaderBreadcrumb {
        process_id,
        process_name,
        process_start,
    })
}

pub(super) fn reader_runtime_user() -> Option<u64> {
    #[cfg(unix)]
    {
        Some(nix::unistd::Uid::effective().as_raw() as u64)
    }
    #[cfg(not(unix))]
    {
        None
    }
}

pub(super) fn validate_reader_runtime_directory(runtime_directory: &Path) -> std::io::Result<()> {
    if !runtime_directory.is_absolute() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "reader runtime directory is not absolute",
        ));
    }
    let metadata = std::fs::symlink_metadata(runtime_directory)?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "reader runtime directory is not a direct directory",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;

        let Some(user) = reader_runtime_user() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "reader runtime user is unavailable",
            ));
        };
        if metadata.uid() as u64 != user
            || metadata.mode() & 0o077 != 0
            || metadata.mode() & 0o200 == 0
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "reader runtime directory is not owner-only and writable",
            ));
        }
    }
    #[cfg(not(unix))]
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "reader runtime validation is unavailable on this platform",
        ));
    }
    Ok(())
}

pub(super) fn validate_reader_breadcrumb_directory(
    runtime_root: &Path,
    directory: &Path,
) -> std::io::Result<()> {
    let contextdb_directory = reader_breadcrumb_root(runtime_root);
    for path in [&contextdb_directory, directory] {
        let metadata = std::fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "reader breadcrumb directory is not a direct directory",
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;

            let Some(user) = reader_runtime_user() else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "reader runtime user is unavailable",
                ));
            };
            if metadata.uid() as u64 != user || metadata.mode() & 0o077 != 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "reader breadcrumb directory is not owner-only",
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn create_reader_breadcrumb_directory(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;

        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700);
        match builder.create(path) {
            Ok(()) => {
                use std::os::unix::fs::PermissionsExt as _;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            Err(error) => Err(error),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "reader breadcrumb directories are unavailable on this platform",
        ))
    }
}

/// The runtime directory this process reads and publishes reader breadcrumbs
/// in when nobody has supplied one: the platform base's owner-only `contextdb`
/// child, resolved ONCE here so no caller re-derives it from a root.
pub(super) fn platform_reader_runtime_base() -> Option<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        std::env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .or_else(|| {
                reader_runtime_user().map(|user| PathBuf::from("/run/user").join(user.to_string()))
            })
    }
    #[cfg(target_os = "macos")]
    {
        let environment = crate::local_transport::ProcessRuntimeDirectoryEnvironment;
        crate::local_transport::RuntimeDirectoryEnvironment::macos_user_temporary_directory(
            &environment,
        )
        .ok()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

pub(super) fn default_reader_runtime_directory() -> Option<RuntimeDirectory> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let requested = platform_reader_runtime_base()?;
        validate_reader_runtime_directory(&requested).ok()?;
        let runtime = RuntimeDirectory::within(&requested);
        create_reader_breadcrumb_directory(runtime.path()).ok()?;
        validate_reader_runtime_directory(runtime.path()).ok()?;
        Some(runtime)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

/// Reader coordination uses the platform directory when it is available.
/// A supplied channel directory also participates in discovery, and supplies
/// the coordination directory on systems without a platform directory.
pub(super) fn reader_runtime_directory(supplied: Option<&Path>) -> Option<RuntimeDirectory> {
    default_reader_runtime_directory().or_else(|| {
        supplied
            .filter(|root| validate_reader_runtime_directory(root).is_ok())
            .map(RuntimeDirectory::supplied)
    })
}

pub(super) fn reader_publication_directories(supplied: Option<&Path>) -> Vec<RuntimeDirectory> {
    let mut roots = Vec::new();
    if let Some(default) = default_reader_runtime_directory() {
        roots.push(default);
    }
    if let Some(root) = supplied.filter(|root| validate_reader_runtime_directory(root).is_ok())
        && !roots.iter().any(|known| known.path() == root)
    {
        roots.push(RuntimeDirectory::supplied(root));
    }
    roots
}

/// The reader registry roots that already exist, found without creating any:
/// the platform directory's `contextdb` child and a supplied directory.
#[cfg(unix)]
pub(super) fn existing_reader_registry_roots(supplied: Option<&Path>) -> Vec<RuntimeDirectory> {
    let mut roots = Vec::new();
    if let Some(base) = platform_reader_runtime_base() {
        let runtime = RuntimeDirectory::within(&base);
        if validate_reader_runtime_directory(runtime.path()).is_ok() {
            roots.push(runtime);
        }
    }
    if let Some(root) = supplied
        && validate_reader_runtime_directory(root).is_ok()
        && !roots.iter().any(|known| known.path() == root)
    {
        roots.push(RuntimeDirectory::supplied(root));
    }
    roots
}

pub(super) fn reader_inventory_directories(supplied: Option<&Path>) -> Vec<RuntimeDirectory> {
    let mut roots = reader_publication_directories(supplied);
    if let Some(base) = platform_reader_runtime_base() {
        let inspectable_candidate = std::fs::symlink_metadata(&base)
            .is_ok_and(|metadata| metadata.is_dir() || metadata.file_type().is_symlink());
        if inspectable_candidate {
            let runtime = RuntimeDirectory::within(&base);
            if !roots.iter().any(|known| known.path() == runtime.path()) {
                roots.push(runtime);
            }
        }
    }
    if let Some(root) = supplied {
        let present_candidate = std::fs::symlink_metadata(root)
            .is_ok_and(|metadata| metadata.is_dir() || metadata.file_type().is_symlink());
        // A channel-only directory with unsafe permissions cannot have been
        // used for an authenticated reader registration. If it is visibly
        // empty, its channel startup failure must not veto the store open.
        // An unreadable or nonempty candidate remains part of the inventory
        // and fails closed when its contents cannot be authenticated.
        let visibly_empty = validate_reader_runtime_directory(root).is_err()
            && std::fs::read_dir(root).is_ok_and(|mut entries| entries.next().is_none());
        if present_candidate && !visibly_empty && !roots.iter().any(|known| known.path() == root) {
            roots.push(RuntimeDirectory::supplied(root));
        }
    }
    roots
}

pub(super) fn current_reader_breadcrumb() -> std::io::Result<ReaderBreadcrumb> {
    let process_id = std::process::id();
    #[cfg(target_os = "linux")]
    {
        let stat = std::fs::read_to_string("/proc/self/stat")?;
        let after_name = stat
            .rfind(") ")
            .map(|index| &stat[index + 2..])
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Linux process stat has no command terminator",
                )
            })?;
        let value = after_name
            .split_whitespace()
            .nth(19)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Linux process stat has no start identity",
                )
            })?
            .parse::<u64>()
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        let process_start = contextdb_core::read_contract::ProcessStartIdentity(value);
        let mut names = Vec::new();
        if let Ok(name) = std::fs::read_to_string("/proc/self/comm") {
            names.push(name.strip_suffix('\n').unwrap_or(&name).to_owned());
        }
        if let Ok(executable) = std::fs::read_link("/proc/self/exe")
            && let Some(name) = executable.file_name().and_then(|name| name.to_str())
        {
            names.push(name.to_owned());
        }
        if let Ok(command_line) = std::fs::read("/proc/self/cmdline")
            && let Some(argument_zero) = command_line.split(|byte| *byte == 0).next()
            && let Ok(argument_zero) = std::str::from_utf8(argument_zero)
            && let Some(name) = Path::new(argument_zero)
                .file_name()
                .and_then(|name| name.to_str())
        {
            names.push(name.to_owned());
        }
        names.sort();
        names.dedup();
        for process_name in names
            .into_iter()
            .filter(|name| safe_reader_process_display_name(name))
        {
            let breadcrumb = ReaderBreadcrumb {
                process_id,
                process_name,
                process_start,
            };
            if verified_linux_reader_process(&breadcrumb).is_some() {
                return Ok(breadcrumb);
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "the current process name could not be verified",
        ))
    }
    #[cfg(target_os = "macos")]
    {
        macos_reader_breadcrumb(process_id)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = process_id;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "the operating system does not expose a verified process-start identity",
        ))
    }
}

pub(super) fn current_reader_registration_process() -> std::io::Result<ReaderBreadcrumb> {
    #[cfg(target_os = "linux")]
    {
        Ok(ReaderBreadcrumb {
            process_id: std::process::id(),
            process_name: current_reader_breadcrumb()
                .map(|breadcrumb| breadcrumb.process_name)
                .unwrap_or_else(|_| "contextdb-reader".to_owned()),
            process_start: linux_process_start(std::process::id())?,
        })
    }
    #[cfg(target_os = "macos")]
    {
        let process_name = std::env::current_exe()?
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| safe_reader_process_display_name(name))
            .unwrap_or("contextdb-reader")
            .to_owned();
        Ok(ReaderBreadcrumb {
            process_id: std::process::id(),
            process_name,
            process_start: macos_process_start(std::process::id())?,
        })
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let process_name = std::env::current_exe()?
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| safe_reader_process_display_name(name))
            .unwrap_or("contextdb-reader")
            .to_owned();
        Ok(ReaderBreadcrumb {
            process_id: std::process::id(),
            process_name,
            process_start: contextdb_core::read_contract::ProcessStartIdentity(0),
        })
    }
}

#[cfg(target_os = "linux")]
pub(super) fn linux_process_start(
    process_id: u32,
) -> std::io::Result<contextdb_core::read_contract::ProcessStartIdentity> {
    let stat = std::fs::read_to_string(format!("/proc/{process_id}/stat"))?;
    let after_name = stat
        .rfind(") ")
        .map(|index| &stat[index + 2..])
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Linux process stat has no command terminator",
            )
        })?;
    let value = after_name
        .split_whitespace()
        .nth(19)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Linux process stat has no start identity",
            )
        })?
        .parse::<u64>()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    Ok(contextdb_core::read_contract::ProcessStartIdentity(value))
}

#[cfg(target_os = "macos")]
pub(super) fn macos_process_info(
    process_id: u32,
) -> std::io::Result<(
    libc::proc_bsdinfo,
    contextdb_core::read_contract::ProcessStartIdentity,
)> {
    let pid = libc::c_int::try_from(process_id)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    if pid == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "a process id of 0 has no reader start identity",
        ));
    }
    // SAFETY: `proc_bsdinfo` is a plain integer record. All-zero is a valid value.
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = libc::c_int::try_from(std::mem::size_of::<libc::proc_bsdinfo>())
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    const _: () = assert!(std::mem::size_of::<libc::proc_bsdinfo>() == 136);
    // SAFETY: `info` is a live `proc_bsdinfo` and `size` is its byte length.
    // `PROC_PIDTBSDINFO` with arg 0 returns that record and does not select a zombie.
    let written = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            std::ptr::addr_of_mut!(info).cast(),
            size,
        )
    };
    if written <= 0 {
        return Err(std::io::Error::last_os_error());
    }
    if written != size {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "macOS process information has an unexpected size",
        ));
    }
    if info.pbi_pid != process_id || info.pbi_start_tvusec >= 1_000_000 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "macOS process start identity was rejected",
        ));
    }
    let packed = info
        .pbi_start_tvsec
        .checked_mul(1_000_000)
        .and_then(|seconds| seconds.checked_add(info.pbi_start_tvusec))
        .filter(|packed| *packed != 0)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "macOS process start identity was rejected",
            )
        })?;
    Ok((
        info,
        contextdb_core::read_contract::ProcessStartIdentity(packed),
    ))
}

#[cfg(target_os = "macos")]
pub(super) fn macos_reader_process_display_name(info: &libc::proc_bsdinfo) -> Option<String> {
    // Inspect only the native fixed-size buffers, never an unbounded CStr or
    // caller-supplied argv. Either kernel-observed name can identify a reader.
    for field in [&info.pbi_name[..], &info.pbi_comm[..]] {
        let Some(end) = field.iter().position(|byte| *byte == 0) else {
            continue;
        };
        let bytes = field[..end]
            .iter()
            .map(|byte| *byte as u8)
            .collect::<Vec<_>>();
        if let Ok(name) = std::str::from_utf8(&bytes)
            && safe_reader_process_display_name(name)
        {
            return Some(name.to_owned());
        }
    }
    None
}

#[cfg(target_os = "macos")]
pub(super) fn macos_reader_breadcrumb(process_id: u32) -> std::io::Result<ReaderBreadcrumb> {
    let (before, process_start) = macos_process_info(process_id)?;
    let process_name = macos_reader_process_display_name(&before).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "macOS reader has no safe operating-system process name",
        )
    })?;
    // A PID may have been recycled while its name was inspected. Require the
    // same nonzero microsecond start identity and display name on both sides.
    let (after, after_start) = macos_process_info(process_id)?;
    if after_start != process_start
        || macos_reader_process_display_name(&after).as_deref() != Some(process_name.as_str())
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "macOS reader process identity changed during inspection",
        ));
    }
    Ok(ReaderBreadcrumb {
        process_id,
        process_name,
        process_start,
    })
}

pub(super) fn next_reader_breadcrumb_path(
    database_path: &Path,
    runtime_root: &Path,
    breadcrumb: &ReaderBreadcrumb,
) -> std::io::Result<PathBuf> {
    let directory = reader_breadcrumb_directory(database_path, runtime_root)?;
    let sequence = READER_BREADCRUMB_SEQUENCE.fetch_add(1, Ordering::SeqCst);
    let name = format!(
        "{:010}-{:016x}-{sequence:016x}.reader",
        breadcrumb.process_id, breadcrumb.process_start.0
    );
    Ok(directory.join(name))
}

pub(super) fn next_anonymous_reader_breadcrumb_path(
    database_path: &Path,
    runtime_root: &Path,
) -> std::io::Result<PathBuf> {
    let directory = reader_breadcrumb_directory(database_path, runtime_root)?;
    let sequence = READER_BREADCRUMB_SEQUENCE.fetch_add(1, Ordering::SeqCst);
    let name = format!(
        "anonymous-{}-{sequence:016x}.reader",
        uuid::Uuid::new_v4().simple()
    );
    Ok(directory.join(name))
}

pub(super) fn reader_snapshot_directory(runtime_root: &Path, identity: (u64, u64)) -> PathBuf {
    reader_breadcrumb_root(runtime_root)
        .join(format!("storage-{:016x}-{:016x}", identity.0, identity.1))
}

pub(super) fn reader_snapshot_registry_lock_path(
    runtime_root: &Path,
    identity: (u64, u64),
) -> PathBuf {
    reader_breadcrumb_root(runtime_root).join(format!(
        "storage-{:016x}-{:016x}.snapshot-registry-lock",
        identity.0, identity.1
    ))
}

pub(super) fn next_reader_snapshot_path(
    runtime_root: &Path,
    identity: (u64, u64),
    process: &ReaderBreadcrumb,
) -> PathBuf {
    let sequence = READER_SNAPSHOT_SEQUENCE.fetch_add(1, Ordering::SeqCst);
    reader_snapshot_directory(runtime_root, identity).join(format!(
        "{:010}-{:016x}-{sequence:016x}.snapshot",
        process.process_id, process.process_start.0
    ))
}

pub(super) fn encode_reader_snapshot(
    snapshot: &RegisteredReaderSnapshot,
) -> std::io::Result<Vec<u8>> {
    let name = snapshot.process.process_name.as_bytes();
    let name_len = u16::try_from(name.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "reader process name exceeds the snapshot-registration limit",
        )
    })?;
    let mut payload = Vec::with_capacity(256 + name.len());
    payload.extend_from_slice(READER_SNAPSHOT_MAGIC);
    payload.extend_from_slice(&READER_SNAPSHOT_VERSION.to_le_bytes());
    payload.extend_from_slice(&snapshot.physical_identity.0.to_le_bytes());
    payload.extend_from_slice(&snapshot.physical_identity.1.to_le_bytes());
    payload.extend_from_slice(&snapshot.descriptor_number.to_le_bytes());
    payload.extend_from_slice(&snapshot.handle_identity);
    payload.extend_from_slice(&snapshot.owner_user.0.to_le_bytes());
    payload.extend_from_slice(&snapshot.process.process_id.to_le_bytes());
    payload.extend_from_slice(&snapshot.process.process_start.0.to_le_bytes());
    payload.extend_from_slice(&name_len.to_le_bytes());
    payload.extend_from_slice(name);
    match snapshot.database_identity {
        Some(identity) => {
            payload.push(1);
            payload.extend_from_slice(&identity.0);
        }
        None => payload.push(0),
    }
    payload.extend_from_slice(&snapshot.storage_snapshot.transaction_id.to_le_bytes());
    match snapshot.storage_snapshot.user_root {
        Some(root) => {
            payload.push(1);
            payload.extend_from_slice(&root.page);
            payload.extend_from_slice(&root.checksum.to_le_bytes());
            payload.extend_from_slice(&root.length.to_le_bytes());
        }
        None => payload.push(0),
    }
    let checksum = blake3::hash(&payload);
    payload.extend_from_slice(checksum.as_bytes());
    Ok(payload)
}

pub(super) fn decode_reader_snapshot(bytes: &[u8]) -> std::io::Result<RegisteredReaderSnapshot> {
    let payload_len = bytes.len().checked_sub(32).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "reader snapshot registration is truncated",
        )
    })?;
    let (payload, checksum) = bytes.split_at(payload_len);
    if blake3::hash(payload).as_bytes() != checksum {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "reader snapshot registration checksum mismatch",
        ));
    }
    let mut cursor = CompanionPayloadCursor { payload, offset: 0 };
    if cursor
        .take(READER_SNAPSHOT_MAGIC.len())
        .map_err(std::io::Error::other)?
        != READER_SNAPSHOT_MAGIC
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "reader snapshot registration magic mismatch",
        ));
    }
    let version = cursor.u16().map_err(std::io::Error::other)?;
    if version != READER_SNAPSHOT_VERSION {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "reader snapshot registration version is unsupported",
        ));
    }
    let physical_identity = (
        cursor.u64().map_err(std::io::Error::other)?,
        cursor.u64().map_err(std::io::Error::other)?,
    );
    let descriptor_number = cursor.u32().map_err(std::io::Error::other)?;
    let handle_identity = cursor.array().map_err(std::io::Error::other)?;
    let owner_user = LocalUserIdentity(cursor.u64().map_err(std::io::Error::other)?);
    let process_id = cursor.u32().map_err(std::io::Error::other)?;
    let process_start = contextdb_core::read_contract::ProcessStartIdentity(
        cursor.u64().map_err(std::io::Error::other)?,
    );
    let name_len = usize::from(cursor.u16().map_err(std::io::Error::other)?);
    let process_name = String::from_utf8(
        cursor
            .take(name_len)
            .map_err(std::io::Error::other)?
            .to_vec(),
    )
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let database_identity = match cursor.u8().map_err(std::io::Error::other)? {
        0 => None,
        1 => Some(DatabaseIdentity(
            cursor.array().map_err(std::io::Error::other)?,
        )),
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "reader snapshot database-identity tag is invalid",
            ));
        }
    };
    let transaction_id = cursor.u64().map_err(std::io::Error::other)?;
    let user_root = match cursor.u8().map_err(std::io::Error::other)? {
        0 => None,
        1 => Some(redb::StorageUserRootIdentity {
            page: cursor.array().map_err(std::io::Error::other)?,
            checksum: u128::from_le_bytes(cursor.array().map_err(std::io::Error::other)?),
            length: cursor.u64().map_err(std::io::Error::other)?,
        }),
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "reader snapshot root tag is invalid",
            ));
        }
    };
    cursor.finish().map_err(std::io::Error::other)?;
    Ok(RegisteredReaderSnapshot {
        physical_identity,
        owner_user,
        database_identity,
        storage_snapshot: redb::StorageSnapshotIdentity {
            transaction_id,
            user_root,
        },
        handle_identity,
        process: ReaderBreadcrumb {
            process_id,
            process_name,
            process_start,
        },
        descriptor_number,
    })
}

#[cfg(unix)]
pub(super) fn serialize_runtime_root(
    root: &Path,
    nonblocking: bool,
) -> std::io::Result<RuntimeRootSerializer> {
    lock_runtime_root(root, nonblocking, false)
}

/// Keep the shared publication directory alive while this store's registry
/// lock serializes its own join/leave. Other stores may publish concurrently.
#[cfg(unix)]
pub(super) fn hold_runtime_root(root: &Path, nonblocking: bool) -> std::io::Result<RuntimeRootSerializer> {
    lock_runtime_root(root, nonblocking, true)
}

#[cfg(unix)]
fn lock_runtime_root(
    root: &Path,
    nonblocking: bool,
    shared: bool,
) -> std::io::Result<RuntimeRootSerializer> {
    use std::os::unix::fs::OpenOptionsExt as _;
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
        .open(root)?;
    let try_lock = || {
        if shared {
            fs2::FileExt::try_lock_shared(&directory)
        } else {
            fs2::FileExt::try_lock_exclusive(&directory)
        }
    };
    if nonblocking {
        try_lock().map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock
                || error.raw_os_error() == fs2::lock_contended_error().raw_os_error()
            {
                std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "the runtime root's reader registries are being joined or left",
                )
            } else {
                error
            }
        })?;
    } else {
        #[cfg(any(test, feature = "test-seams"))]
        if read_persistence_test_scaffold::reader_join_wait_hook_installed_for_test() {
            if try_lock().is_ok() {
                return Ok(RuntimeRootSerializer {
                    _directory: directory,
                });
            }
            read_persistence_test_scaffold::run_reader_join_wait_hook_for_test();
        }
        if shared {
            fs2::FileExt::lock_shared(&directory)?;
        } else {
            fs2::FileExt::lock_exclusive(&directory)?;
        }
    }
    Ok(RuntimeRootSerializer {
        _directory: directory,
    })
}

#[cfg(not(unix))]
pub(super) fn serialize_runtime_root(
    _root: &Path,
    _nonblocking: bool,
) -> std::io::Result<RuntimeRootSerializer> {
    Ok(RuntimeRootSerializer)
}

#[cfg(not(unix))]
pub(super) fn hold_runtime_root(root: &Path, nonblocking: bool) -> std::io::Result<RuntimeRootSerializer> {
    serialize_runtime_root(root, nonblocking)
}

/// Remove the per-store registry directory once it is empty, and then the
/// registration root when this process created it (the platform directory's
/// `contextdb` child) and it is empty too. An operator-supplied root is never
/// removed. Only empty directories are removed, so another reader's
/// registration keeps both.
pub(super) fn remove_empty_breadcrumb_directories(path: &Path, created_root: Option<&Path>) {
    let Some(store_directory) = path.parent() else {
        return;
    };
    if std::fs::remove_dir(store_directory).is_ok()
        && let Some(root) = created_root
        && store_directory.parent() == Some(root)
    {
        let _ = std::fs::remove_dir(root);
    }
}

impl ReaderBreadcrumbGuard {
    /// Publish into `runtime`'s registration directory. When `runtime` is the
    /// platform directory's `contextdb` child, this process created that
    /// child, and the last registration to leave removes it again.
    pub(super) fn create_in(
        database_path: &Path,
        runtime: &RuntimeDirectory,
        breadcrumb: Option<&ReaderBreadcrumb>,
    ) -> std::io::Result<Self> {
        // Publication joins the runtime root's registries: under its
        // serializer no leaver can remove the `contextdb` child between
        // creating it and using it, so nothing here retries.
        let _serializer = hold_runtime_root(runtime.root(), false)?;
        recreate_created_runtime_root(runtime);
        let mut guard = Self::create(database_path, runtime.path(), breadcrumb)?;
        if runtime.root() != runtime.path() {
            guard.created_root = Some(runtime.path().to_path_buf());
            guard.serializer_root = Some(runtime.root().to_path_buf());
        }
        Ok(guard)
    }

    /// Diagnostic publication never waits behind authoritative coordination.
    /// Original request failures remain typed even when a note is omitted.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating. "
    )]
    pub(super) fn create_for_open(
        database_path: &Path,
        runtime: &RuntimeDirectory,
        breadcrumb: Option<&ReaderBreadcrumb>,
        context: &crate::executor::bounded::ReadOpenContext,
    ) -> std::result::Result<Option<Self>, LoadReadImageError> {
        context.checkpoint().map_err(LoadReadImageError::Memory)?;
        let serializer = hold_runtime_root(runtime.root(), true);
        context.checkpoint().map_err(LoadReadImageError::Memory)?;
        let Ok(_serializer) = serializer else {
            return Ok(None);
        };
        recreate_created_runtime_root(runtime);
        let created = Self::create_with_lock_mode(database_path, runtime.path(), breadcrumb, true);
        context.checkpoint().map_err(LoadReadImageError::Memory)?;
        let Ok(mut guard) = created else {
            return Ok(None);
        };
        if runtime.root() != runtime.path() {
            guard.created_root = Some(runtime.path().to_path_buf());
            guard.serializer_root = Some(runtime.root().to_path_buf());
        }
        Ok(Some(guard))
    }

    pub(super) fn create(
        database_path: &Path,
        runtime_root: &Path,
        breadcrumb: Option<&ReaderBreadcrumb>,
    ) -> std::io::Result<Self> {
        Self::create_with_lock_mode(database_path, runtime_root, breadcrumb, false)
    }

    fn create_with_lock_mode(
        database_path: &Path,
        runtime_root: &Path,
        breadcrumb: Option<&ReaderBreadcrumb>,
        try_only: bool,
    ) -> std::io::Result<Self> {
        validate_reader_runtime_directory(runtime_root)?;
        let path = match breadcrumb {
            Some(breadcrumb) => {
                next_reader_breadcrumb_path(database_path, runtime_root, breadcrumb)?
            }
            None => next_anonymous_reader_breadcrumb_path(database_path, runtime_root)?,
        };
        let directory = path.parent().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "reader breadcrumb has no runtime directory",
            )
        })?;
        let contextdb_directory = reader_breadcrumb_root(runtime_root);
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        create_reader_breadcrumb_directory(&contextdb_directory)?;
        create_reader_breadcrumb_directory(directory)?;
        validate_reader_breadcrumb_directory(runtime_root, directory)?;
        let mut file = options.open(&path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        let publish = (|| -> std::io::Result<std::fs::Metadata> {
            if try_only {
                fs2::FileExt::try_lock_exclusive(&file)?;
            } else {
                fs2::FileExt::lock_exclusive(&file)?;
            }
            match breadcrumb {
                Some(breadcrumb) => file.write_all(&encode_reader_breadcrumb(breadcrumb)?)?,
                None => file.write_all(ANONYMOUS_READER_BREADCRUMB)?,
            }
            file.sync_data()?;
            let metadata = file.metadata()?;
            if !reader_breadcrumb_path_still_names(&path, &metadata) {
                return Err(std::io::Error::other(
                    "reader breadcrumb path changed during publication",
                ));
            }
            Ok(metadata)
        })();
        let metadata = match publish {
            Ok(metadata) => metadata,
            Err(error) => {
                if let Ok(metadata) = file.metadata()
                    && reader_breadcrumb_path_still_names(&path, &metadata)
                {
                    let _ = std::fs::remove_file(&path);
                }
                let _ = fs2::FileExt::unlock(&file);
                drop(file);
                return Err(error);
            }
        };
        Ok(Self {
            file: Some(file),
            path,
            metadata,
            removed: false,
            created_root: None,
            serializer_root: None,
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    /// Remove this breadcrumb's emptied store directory and, only while the
    /// runtime root's serializer is held, the `contextdb` child it created.
    pub(super) fn remove_directories(&self, nonblocking: bool) {
        let lifetime = self
            .serializer_root
            .as_deref()
            .and_then(|root| hold_runtime_root(root, nonblocking).ok());
        remove_empty_breadcrumb_directories(&self.path, None);
        drop(lifetime);
        if let (Some(root), Some(created)) = (
            self.serializer_root.as_deref(),
            self.created_root.as_deref(),
        ) && let Ok(_exclusive) = serialize_runtime_root(root, true)
        {
            let _ = std::fs::remove_dir(created);
        }
    }

    /// No caller may read through the handoff after this release begins.
    /// Removal and unlock signal departure; a failed handoff closes its
    /// retained handles before the breadcrumb descriptor itself is closed.
    pub(super) fn release<T>(
        mut self,
        retained: T,
        on_handoff_settled: impl FnOnce(),
    ) -> std::io::Result<(PathBuf, T)> {
        let removal = match std::fs::symlink_metadata(&self.path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
            Ok(_) if reader_breadcrumb_path_still_names(&self.path, &self.metadata) => {
                std::fs::remove_file(&self.path)
            }
            Ok(_) => Err(std::io::Error::other(
                "reader breadcrumb path no longer names the published inode",
            )),
        };
        let unlock = if let Some(file) = self.file.as_ref() {
            fs2::FileExt::unlock(file)
        } else {
            Ok(())
        };
        // Preserve removal-first error precedence without closing the
        // descriptor until a failed retained handoff has released its handles.
        if let Err(error) = removal.and(unlock) {
            drop(retained);
            on_handoff_settled();
            drop(self.file.take());
            return Err(error);
        }
        on_handoff_settled();
        drop(self.file.take());
        self.remove_directories(true);
        self.removed = true;
        Ok((self.path.clone(), retained))
    }
}

impl Drop for ReaderBreadcrumbGuard {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            if reader_breadcrumb_path_still_names(&self.path, &self.metadata) {
                let _ = std::fs::remove_file(&self.path);
            }
            let _ = fs2::FileExt::unlock(&file);
            drop(file);
        }
        if !self.removed {
            // A failed hydration may still hold its coordination locks here,
            // so this leave takes the serializer only by try.
            self.remove_directories(true);
        }
    }
}

impl ReaderRegistrationTarget<'_> {
    pub(super) fn created_root(&self) -> Option<&Path> {
        (!self.preserve_root && self.runtime.root() != self.runtime.path())
            .then(|| self.runtime.path())
    }
}

/// Re-create the platform directory's `contextdb` child when a concurrent last
/// reader removed it after this process resolved it. An operator-supplied
/// directory is never created.
pub(super) fn recreate_created_runtime_root(runtime: &RuntimeDirectory) {
    if runtime.root() != runtime.path() {
        let _ = create_reader_breadcrumb_directory(runtime.path());
    }
}

impl ReaderSnapshotRegistryLock {
    pub(super) fn create(runtime_root: &Path, identity: (u64, u64)) -> std::io::Result<Self> {
        let (file, path) = Self::create_descriptor(runtime_root, identity)?;
        Self::from_open_file(file, path)
    }

    /// Open, creating if absent, this physical registry's lock file. Nothing
    /// about the returned descriptor is validated yet.
    pub(super) fn create_descriptor(
        runtime_root: &Path,
        identity: (u64, u64),
    ) -> std::io::Result<(File, PathBuf)> {
        validate_reader_runtime_directory(runtime_root)?;
        let contextdb_directory = reader_breadcrumb_root(runtime_root);
        create_reader_breadcrumb_directory(&contextdb_directory)?;
        validate_reader_breadcrumb_directory(runtime_root, &contextdb_directory)?;
        let path = reader_snapshot_registry_lock_path(runtime_root, identity);
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        Ok((options.open(&path)?, path))
    }

    /// Create this physical registry's lock and hold it, for a reader about
    /// to publish its registration.
    ///
    /// The last reader of a registry removes the lock's name, and then the
    /// empty directory it lives in, while holding the lock (see
    /// [`ReaderSnapshotFileGuard`]'s drop). An arriving reader can meet that
    /// teardown at any point: its open finds no directory or no name, or the
    /// file it opened loses its name before it is validated, before it is
    /// locked, or while it waits for the lock. Each of those is a finished
    /// teardown, recognisable because the descriptor is still the trusted
    /// private lock file in every respect except that it no longer has a
    /// name, and the reader opens the lock afresh. Anything else that fails
    /// the trust checks -- another owner, permissions, file type, contents,
    /// extra links, or a name now pointing at some other file while ours is
    /// still linked -- is refused as before.
    ///
    /// Only the coordination lock is retaken. The reader's data descriptor,
    /// its shared load lock and the storage snapshot it pinned stay exactly
    /// as they were, so no read is restarted on newer data. There is no
    /// attempt cap: a retry happens only after another reader has completed
    /// a whole registration and teardown, so every retry is somebody else's
    /// finished work, never a fault, and a healthy open never fails for it.
    pub(super) fn acquire_for_registration(
        runtime_root: &Path,
        identity: (u64, u64),
    ) -> std::io::Result<Self> {
        Self::acquire_for_registration_with_wait(runtime_root, identity, None)
    }

    fn acquire_for_registration_with_wait(
        runtime_root: &Path,
        identity: (u64, u64),
        mut checkpoint: Option<&mut dyn FnMut() -> std::io::Result<()>>,
    ) -> std::io::Result<Self> {
        #[cfg(any(test, feature = "test-seams"))]
        let mut attempt = 0_u64;
        loop {
            if let Some(checkpoint) = checkpoint.as_mut() {
                checkpoint()?;
            }
            #[cfg(any(test, feature = "test-seams"))]
            {
                read_persistence_test_scaffold::count_registration_attempt_for_test(
                    identity,
                    |counts| {
                        counts.opens += 1;
                        if attempt > 0 {
                            counts.retries += 1;
                        }
                    },
                );
                attempt += 1;
            }
            validate_reader_runtime_directory(runtime_root)?;
            let (file, path) = Self::create_descriptor(runtime_root, identity)?;
            // Refuse an untrusted descriptor before any blocking wait.
            let metadata = match Self::validate_open_file(&file, &path) {
                Ok(metadata) => metadata,
                Err(_) if registry_lock_descriptor_departed(&file) => continue,
                Err(error) => return Err(error),
            };
            let mut lock = Self {
                file,
                path,
                metadata,
                locked: false,
            };
            let first = fs2::FileExt::try_lock_exclusive(&lock.file);
            match first {
                Ok(()) => (),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    #[cfg(any(test, feature = "test-seams"))]
                    if read_persistence_test_scaffold::reader_join_wait_hook_installed_for_test() {
                        read_persistence_test_scaffold::run_reader_join_wait_hook_for_test();
                    }
                    if let Some(checkpoint) = checkpoint.as_mut() {
                        loop {
                            checkpoint()?;
                            match fs2::FileExt::try_lock_exclusive(&lock.file) {
                                Ok(()) => break,
                                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                    std::thread::yield_now();
                                }
                                Err(error) => return Err(error),
                            }
                        }
                    } else {
                        fs2::FileExt::lock_exclusive(&lock.file)?;
                    }
                }
                Err(error) => return Err(error),
            }
            lock.locked = true;
            #[cfg(any(test, feature = "test-seams"))]
            read_persistence_test_scaffold::count_registration_attempt_for_test(
                identity,
                |counts| {
                    counts.l0_takes += 1;
                },
            );
            // A last leave may have unlinked this inode while we waited. A
            // reopen is justified only by that completed removal, never Busy.
            if !reader_snapshot_registry_lock_path_still_names(&lock.path, &lock.metadata) {
                let departed = registry_lock_descriptor_departed(&lock.file);
                lock.unlock()?;
                if departed {
                    continue;
                }
                return Err(std::io::Error::other(
                    "reader snapshot registry lock changed while it was acquired",
                ));
            }
            #[cfg(any(test, feature = "test-seams"))]
            read_persistence_test_scaffold::reader_registration_lock_checkpoint_for_test(
                identity,
                read_persistence_test_scaffold::ReaderRegistrationLockPhase::DescriptorOpened,
            );
            // These observations are inside the exclusion that prevents a
            // normal leave from unlinking the descriptor; replacement is still
            // authenticated after either adversarial checkpoint.
            Self::validate_open_file(&lock.file, &lock.path)?;
            #[cfg(any(test, feature = "test-seams"))]
            read_persistence_test_scaffold::reader_registration_lock_checkpoint_for_test(
                identity,
                read_persistence_test_scaffold::ReaderRegistrationLockPhase::Validated,
            );
            if reader_snapshot_registry_lock_path_still_names(&lock.path, &lock.metadata) {
                return Ok(lock);
            }
            return Err(std::io::Error::other(
                "reader snapshot registry lock changed while it was acquired",
            ));
        }
    }

    /// Owner admission holds the commit mutex. A teardown race or an occupied
    /// registry is a retryable coordination refusal, never a wait or a retry loop.
    pub(super) fn try_acquire_for_registration(
        runtime_root: &Path,
        identity: (u64, u64),
    ) -> std::io::Result<Self> {
        #[cfg(any(test, feature = "test-seams"))]
        read_persistence_test_scaffold::count_registration_attempt_for_test(identity, |counts| {
            counts.opens += 1;
        });
        let (file, path) = Self::create_descriptor(runtime_root, identity)?;
        #[cfg(any(test, feature = "test-seams"))]
        read_persistence_test_scaffold::reader_registration_lock_checkpoint_for_test(
            identity,
            read_persistence_test_scaffold::ReaderRegistrationLockPhase::DescriptorOpened,
        );
        let metadata = match Self::validate_open_file(&file, &path) {
            Ok(metadata) => metadata,
            Err(_) if registry_lock_descriptor_departed(&file) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "reader snapshot registry lock departed before owner cursor validation",
                ));
            }
            Err(error) => return Err(error),
        };
        let mut lock = Self {
            file,
            path,
            metadata,
            locked: false,
        };
        lock.try_lock_exclusive()?;
        #[cfg(any(test, feature = "test-seams"))]
        read_persistence_test_scaffold::count_registration_attempt_for_test(identity, |counts| {
            counts.l0_takes += 1;
        });
        Ok(lock)
    }

    pub(super) fn open(runtime_root: &Path, identity: (u64, u64)) -> std::io::Result<Self> {
        validate_reader_runtime_directory(runtime_root)?;
        let contextdb_directory = reader_breadcrumb_root(runtime_root);
        validate_reader_breadcrumb_directory(runtime_root, &contextdb_directory)?;
        let path = reader_snapshot_registry_lock_path(runtime_root, identity);
        let mut options = OpenOptions::new();
        options.read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        Self::from_open_file(options.open(&path)?, path)
    }

    pub(super) fn validate_open_file(
        file: &File,
        path: &Path,
    ) -> std::io::Result<std::fs::Metadata> {
        let metadata = file.metadata()?;
        if !trusted_reader_snapshot_registry_lock_metadata(&metadata)
            || !reader_snapshot_registry_lock_path_still_names(path, &metadata)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "reader snapshot registry lock is not a trusted private regular file",
            ));
        }
        Ok(metadata)
    }

    pub(super) fn from_open_file(file: File, path: PathBuf) -> std::io::Result<Self> {
        let metadata = Self::validate_open_file(&file, &path)?;
        Ok(Self {
            file,
            path,
            metadata,
            locked: false,
        })
    }

    pub(super) fn lock_exclusive(&mut self) -> std::io::Result<()> {
        if !self.locked {
            fs2::FileExt::lock_exclusive(&self.file)?;
            self.locked = true;
        }
        if !reader_snapshot_registry_lock_path_still_names(&self.path, &self.metadata) {
            let _ = self.unlock();
            return Err(std::io::Error::other(
                "reader snapshot registry lock changed while it was acquired",
            ));
        }
        Ok(())
    }

    pub(super) fn try_lock_exclusive(&mut self) -> std::io::Result<()> {
        if !self.locked {
            fs2::FileExt::try_lock_exclusive(&self.file).map_err(|error| {
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.raw_os_error() == fs2::lock_contended_error().raw_os_error()
                {
                    std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "reader snapshot registry lock is held by another operation",
                    )
                } else {
                    error
                }
            })?;
            self.locked = true;
        }
        if !reader_snapshot_registry_lock_path_still_names(&self.path, &self.metadata) {
            let departed = registry_lock_descriptor_departed(&self.file);
            let _ = self.unlock();
            return Err(std::io::Error::new(
                if departed {
                    std::io::ErrorKind::WouldBlock
                } else {
                    std::io::ErrorKind::InvalidData
                },
                "reader snapshot registry lock changed during owner cursor admission",
            ));
        }
        Ok(())
    }

    fn acquire_for_teardown(
        &mut self,
        identity: (u64, u64),
        nonblocking: bool,
    ) -> std::io::Result<()> {
        loop {
            if !self.locked {
                if nonblocking {
                    fs2::FileExt::try_lock_exclusive(&self.file)?;
                } else {
                    fs2::FileExt::lock_exclusive(&self.file)?;
                }
                self.locked = true;
            }
            if reader_snapshot_registry_lock_path_still_names(&self.path, &self.metadata) {
                return Ok(());
            }
            let departed = registry_lock_descriptor_departed(&self.file);
            self.unlock()?;
            if !departed {
                return Err(std::io::Error::other(
                    "reader snapshot registry lock changed during teardown",
                ));
            }
            // Only a completed unlink permits another open. Missing or
            // untrusted replacement coordination leaves the old record stale;
            // inventory reclaims it after this guard closes its descriptor.
            let root = self.path.parent().ok_or(std::io::ErrorKind::InvalidInput)?;
            let replacement = Self::open(root, identity)?;
            *self = replacement;
        }
    }

    pub(super) fn unlock(&mut self) -> std::io::Result<()> {
        if self.locked {
            fs2::FileExt::unlock(&self.file)?;
            self.locked = false;
        }
        Ok(())
    }
}

impl Drop for ReaderSnapshotRegistryLock {
    fn drop(&mut self) {
        let _ = self.unlock();
    }
}

impl ReaderSnapshotFileGuard {
    /// The caller holds every publication registry lock until all records are
    /// installed. Partial publication can therefore roll back without waiting.
    ///
    /// The registry lock is acquired before opening the fence and retained
    /// through publication. Its pathname is authenticated again here so a
    /// same-user replacement cannot substitute another publication fence.
    pub(super) fn create_locked(
        target: &ReaderRegistrationTarget<'_>,
        snapshot: &RegisteredReaderSnapshot,
        registry_lock: ReaderSnapshotRegistryLock,
        try_only_teardown: bool,
        transaction: Option<
            contextdb_core::read_memory::ReadShared<read_coordination::RegistrationTransaction>,
        >,
    ) -> std::io::Result<Self> {
        let runtime_root = target.runtime.path();
        validate_reader_runtime_directory(runtime_root)?;
        if let Some(fence) = target.fence {
            let named = fence
                .lock()
                .map_err(|_| std::io::Error::other("reader publication fence is unavailable"))?
                .path_still_names();
            if !named {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "the reader publication fence departed before registration",
                ));
            }
        }
        let contextdb_directory = reader_breadcrumb_root(runtime_root);
        let directory = reader_snapshot_directory(runtime_root, snapshot.physical_identity);
        create_reader_breadcrumb_directory(&contextdb_directory)?;
        create_reader_breadcrumb_directory(&directory)?;
        validate_reader_breadcrumb_directory(runtime_root, &directory)?;
        let path =
            next_reader_snapshot_path(runtime_root, snapshot.physical_identity, &snapshot.process);
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        let mut file = options.open(&path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        let publish = (|| -> std::io::Result<std::fs::Metadata> {
            if try_only_teardown {
                fs2::FileExt::try_lock_exclusive(&file)?;
            } else {
                fs2::FileExt::lock_exclusive(&file)?;
            }
            file.write_all(&encode_reader_snapshot(snapshot)?)?;
            file.sync_data()?;
            let metadata = file.metadata()?;
            if !reader_snapshot_path_still_names(&path, &metadata) {
                return Err(std::io::Error::other(
                    "reader snapshot path changed during publication",
                ));
            }
            Ok(metadata)
        })();
        let metadata = match publish {
            Ok(metadata) => metadata,
            Err(error) => {
                let _ = std::fs::remove_file(&path);
                let _ = fs2::FileExt::unlock(&file);
                return Err(error);
            }
        };
        Ok(Self {
            file: Some(file),
            path,
            metadata,
            registry_lock,
            try_only_teardown,
            opening_transaction: transaction,
            database_path: target.database_path.to_path_buf(),
            physical_identity: snapshot.physical_identity,
            created_root: target.created_root().map(Path::to_path_buf),
            serializer_root: target.runtime.root().to_path_buf(),
        })
    }
}

impl ReaderSnapshotFileGuard {
    /// Replace this reader's own record in place with `snapshot`, on the
    /// descriptor it already holds: one write, the length set, one sync. No
    /// registry lock is taken -- nobody parses a record while the reader holds
    /// the data lock, and the record stays this reader's throughout.
    #[cfg(unix)]
    pub(super) fn rewrite(
        &self,
        snapshot: &RegisteredReaderSnapshot,
        torn: bool,
    ) -> std::io::Result<()> {
        use std::os::unix::fs::FileExt as _;
        let file = self
            .file
            .as_ref()
            .ok_or_else(|| std::io::Error::other("reader snapshot record is already released"))?;
        let bytes = encode_reader_snapshot(snapshot)?;
        let written = if torn {
            &bytes[..bytes.len() / 2]
        } else {
            &bytes[..]
        };
        file.write_all_at(written, 0)?;
        file.set_len(written.len() as u64)?;
        if torn {
            return Err(std::io::Error::other("injected torn reader record"));
        }
        file.sync_data()
    }
}

impl Drop for ReaderSnapshotFileGuard {
    fn drop(&mut self) {
        // A leave shares the runtime root's lifetime hold, then takes this
        // store's registry lock, in the same order as a join. An owner's leave
        // takes both only by try; unrelated stores keep joining concurrently.
        let serializer = hold_runtime_root(
            &self.serializer_root,
            self.try_only_teardown
                || self
                    .opening_transaction
                    .as_ref()
                    .is_some_and(|transaction| transaction.is_staging()),
        )
        .ok();
        let nonblocking = self.try_only_teardown
            || self
                .opening_transaction
                .as_ref()
                .is_some_and(|transaction| transaction.is_staging());
        let coordinated = serializer.is_some()
            && self
                .registry_lock
                .acquire_for_teardown(self.physical_identity, nonblocking)
                .is_ok();
        #[cfg(any(test, feature = "test-seams"))]
        if coordinated {
            read_persistence_test_scaffold::reader_leave_holding_serializer_for_test();
            read_persistence_test_scaffold::pause_reader_at_physical(
                self.physical_identity,
                read_persistence_test_scaffold::ReaderPausePoint::LeavingWithRegistryExclusion,
            );
        }
        if let Some(file) = self.file.take() {
            // The record is identified by the descriptor this guard still
            // holds, as it is now: an in-place rewrite of the record changes
            // its length, so the metadata taken when it was created would no
            // longer name it and the reader would leave its own record behind.
            let held = file.metadata().ok();
            if coordinated
                && held
                    .as_ref()
                    .is_some_and(|held| reader_snapshot_path_still_names(&self.path, held))
            {
                let _ = std::fs::remove_file(&self.path);
            }
            let _ = fs2::FileExt::unlock(&file);
        }
        // If owner cleanup cannot take the registry immediately, closing the
        // unlocked snapshot file makes its leftover record stale. The existing
        // inventory removes that record before interpreting its snapshot, so it
        // cannot keep a transaction floor alive for this process's lifetime.
        // The last reader removes what it published, and only then. Every
        // removal below is decided under the registry lock this teardown holds,
        // and nothing below waits: a gate that cannot be proven at once leaves
        // everything where it is.
        if coordinated && let Some(registry_directory) = self.path.parent() {
            with_last_reader_gate(&self.database_path, self.physical_identity, |gate_held| {
                remove_registry_if_last(
                    registry_directory,
                    &self.registry_lock.path,
                    gate_held,
                    None,
                )
            });
        }
        // A different store may be joining under another shared lifetime hold.
        // Release ours before trying exclusive empty-root removal; if any join
        // still uses the root, leave it in place rather than unlinking it.
        drop(serializer);
        if coordinated
            && let Some(root) = self.created_root.as_deref()
            && let Ok(_exclusive) = serialize_runtime_root(&self.serializer_root, true)
        {
            let _ = std::fs::remove_dir(root);
        }
    }
}

/// Run `remove` while holding the gate a last departing reader must hold to
/// remove anything, passing whether the gate was granted.
///
/// The gate is the store's own reader admission, taken on a fresh descriptor
/// of the registered physical file -- its own open file description, so no
/// other guard's release can drop it: the reader hold and the shared data
/// lock, through the one door every reader comes through. A writer that has
/// claimed the store, is acquiring it, or owns it refuses that door, and then
/// nothing is removed; while the gate is held no such writer can start, and a
/// writer waiting for readers waits behind this short cleanup instead of
/// being refused by it. Nothing here waits.
#[cfg(unix)]
pub(super) fn with_last_reader_gate<T>(
    database_path: &Path,
    physical_identity: (u64, u64),
    remove: impl FnOnce(bool) -> T,
) -> T {
    use std::os::unix::fs::OpenOptionsExt as _;
    let Ok(data) = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC)
        .open(database_path)
    else {
        return remove(false);
    };
    if physical_admission::held_against(
        &data,
        physical_admission::READER_HOLD_OFFSET,
        physical_admission::ByteLock::Exclusive,
    )
    .unwrap_or(true)
    {
        return remove(false);
    }
    match physical_admission::admit_through_reader_gate(&data, physical_identity) {
        Ok(hold) => {
            let removed = remove(true);
            let _ = hold.release();
            removed
        }
        Err(_) => remove(false),
    }
}

#[cfg(not(unix))]
pub(super) fn with_last_reader_gate<T>(
    _database_path: &Path,
    _physical_identity: (u64, u64),
    remove: impl FnOnce(bool) -> T,
) -> T {
    remove(false)
}

/// Remove one store's reader registry when it holds no registration of any
/// process: each Clear fence (`purge_publication::remove_if_clear`), then the
/// empty registry directory, the registry lock file, and the root when this
/// process created it and it is empty. The caller holds that registry's lock
/// and, for the whole call, a data gate that excludes a purge-capable writer;
/// without the gate nothing is removed. Anything unexpected in the directory,
/// or a fence that cannot be removed, leaves everything in place.
pub(super) fn remove_registry_if_last(
    registry_directory: &Path,
    registry_lock_path: &Path,
    data_gate_held: bool,
    created_root: Option<&Path>,
) {
    if !data_gate_held {
        return;
    }
    let Ok(entries) = std::fs::read_dir(registry_directory) else {
        return;
    };
    let mut fences = Vec::new();
    for entry in entries {
        let Ok(entry) = entry else {
            return;
        };
        if !purge_publication::is_fence_file_name(&entry.file_name()) {
            // A registration of some process, or something unknown.
            return;
        }
        fences.push(entry.path());
    }
    for fence in fences {
        if purge_publication::remove_if_clear(&fence) != purge_publication::FenceRemoval::Removed {
            return;
        }
    }
    if std::fs::remove_dir(registry_directory).is_err() {
        return;
    }
    let _ = std::fs::remove_file(registry_lock_path);
    if let Some(root) = created_root {
        let _ = std::fs::remove_dir(root);
    }
}

impl redb::StorageBackend for ReadSnapshotFileView {
    fn len(&self) -> std::io::Result<u64> {
        self.file.metadata().map(|metadata| metadata.len())
    }

    #[cfg(unix)]
    fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        use std::os::unix::fs::FileExt as _;
        self.file.read_exact_at(out, offset)
    }

    #[cfg(windows)]
    fn read(&self, mut offset: u64, mut out: &mut [u8]) -> std::io::Result<()> {
        use std::os::windows::fs::FileExt as _;
        while !out.is_empty() {
            match self.file.seek_read(out, offset) {
                Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
                Ok(count) => {
                    offset += count as u64;
                    out = &mut out[count..];
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn set_len(&self, _len: u64) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "read snapshot backend is immutable",
        ))
    }

    fn sync_data(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn write(&self, _offset: u64, _data: &[u8]) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "read snapshot backend is immutable",
        ))
    }
}

impl ReadSnapshotRegistrationGuard {
    /// Whether this admitted reader may read a store whose writer died
    /// without a writer repairing it first: that writer's published run must
    /// name this boot and this exact data file -- device, inode and birth --
    /// and the file must sit on a local filesystem whose page cache every
    /// process on this boot shares. Every page of a commit reaches the file
    /// before a header naming it, so only then is whatever commit the header
    /// names wholly there. Anything else is refused, typed, before any page
    /// beyond the header is read.
    #[cfg(unix)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn crashed_store_is_readable(
        &self,
        database_path: &Path,
    ) -> std::result::Result<(), LoadReadImageError> {
        let refuse = |reason: &str| {
            Err(direct_read_requires_writer(format!(
                "this store has an unclean image whose consistency cannot be verified because {reason}; a byte-copy taken while a writer is live can be inconsistent. Create a consistent copy with Database::export_snapshot, or close the writer cleanly before copying. A writable open must recover this file before it can be read"
            )))
        };
        let Some(boot) = current_boot_id() else {
            return refuse("this platform cannot identify its last writer's boot");
        };
        if !data_file_on_trusted_local_filesystem(&self.descriptor) {
            return refuse(
                "its file is not on a local filesystem trusted to hold the writer's pages",
            );
        }
        let Some((dev, ino, birth)) = data_file_birth_identity(&self.descriptor) else {
            return refuse("its data file's identity cannot be read");
        };
        let Ok(record) = inspect_companion_record(database_path) else {
            return refuse("its companion record cannot be read");
        };
        let Some(binding) = record.fields.writer_boot else {
            return refuse("its last writer never finished opening it");
        };
        if binding.boot_id != boot {
            return refuse("the machine has restarted since its last writer opened it");
        }
        if (binding.data_dev, binding.data_ino, binding.data_birth_nanos) != (dev, ino, birth) {
            return refuse("its companion names a different data file");
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn handoff_publication_hold(
        &self,
        _path: &Path,
        cancellation: &OwnerReadCancellation,
        deadline: (Arc<dyn contextdb_core::read_contract::DeadlineClock>, u64),
        active_ms: u64,
        _memory: crate::read_image_memory::ImageMemoryHandle,
    ) -> std::result::Result<File, LoadReadImageError> {
        use physical_admission::{ByteLock, Granted, READER_HOLD_OFFSET};
        let checkpoint = || {
            read_coordination::check_handoff_authority(
                cancellation,
                deadline.0.as_ref(),
                deadline.1,
                active_ms,
            )
            .map_err(LoadReadImageError::Memory)
        };
        checkpoint()?;
        let file = physical_admission::reopen_independent_native(&self.descriptor, false).map_err(
            |reason| LoadReadImageError::WaitIo {
                operation: "reopen reader publication hold",
                reason,
            },
        )?;
        loop {
            checkpoint()?;
            let acquired =
                physical_admission::try_byte(&file, READER_HOLD_OFFSET, ByteLock::Shared).map_err(
                    |reason| LoadReadImageError::WaitIo {
                        operation: "acquire reader publication hold",
                        reason: physical_admission::WaitIo::Io(reason),
                    },
                )?;
            checkpoint()?;
            if acquired == Granted::Yes {
                return Ok(file);
            }
            std::thread::yield_now();
        }
    }

    /// Publish a conservative transaction floor before asking the owner for
    /// the cursor's first page. The root is not known yet; transaction zero
    /// prevents a replacement writer from reclaiming any deferred page until
    /// the exact root is registered or this request ends.
    #[cfg(test)]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from owner_cursor_admission_tests.rs"
        )
    )]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(crate) fn owner_handoff_start(
        path: &Path,
        identity: DatabaseIdentity,
        runtime_dir: Option<&Path>,
    ) -> std::result::Result<Self, LoadReadImageError> {
        let clock: Arc<dyn contextdb_core::read_contract::DeadlineClock> =
            Arc::new(crate::local_transport::MonotonicDeadlineClock::new());
        let active_ms = contextdb_core::read_contract::ReadLimits::default().active_ms;
        let deadline = clock.now_ms().saturating_add(active_ms);
        Self::owner_handoff_start_within(
            path,
            identity,
            runtime_dir,
            &OwnerReadCancellation::new(),
            (clock, deadline),
            active_ms,
            crate::read_contract::decoder_memory(
                contextdb_core::read_contract::ReadLimits::SHIPPED_MEMORY,
            )
            .map_err(LoadReadImageError::Memory)?,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(test)]
    pub(crate) fn owner_handoff_start_within(
        path: &Path,
        identity: DatabaseIdentity,
        runtime_dir: Option<&Path>,
        cancellation: &OwnerReadCancellation,
        deadline: (Arc<dyn contextdb_core::read_contract::DeadlineClock>, u64),
        active_ms: u64,
        memory: crate::read_image_memory::ImageMemoryHandle,
    ) -> std::result::Result<Self, LoadReadImageError> {
        Self::owner_handoff_start_with_transaction(
            path,
            identity,
            runtime_dir,
            cancellation,
            deadline,
            active_ms,
            memory,
            None,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating. "
    )]
    pub(crate) fn owner_handoff_start_with_transaction(
        path: &Path,
        identity: DatabaseIdentity,
        runtime_dir: Option<&Path>,
        cancellation: &OwnerReadCancellation,
        deadline: (Arc<dyn contextdb_core::read_contract::DeadlineClock>, u64),
        active_ms: u64,
        memory: crate::read_image_memory::ImageMemoryHandle,
        transaction: Option<
            contextdb_core::read_memory::ReadShared<read_coordination::RegistrationTransaction>,
        >,
    ) -> std::result::Result<Self, LoadReadImageError> {
        #[cfg(unix)]
        let mut guard = Self::open_descriptor(path, false)?;
        #[cfg(not(unix))]
        let mut guard = Self::open(path)?;
        guard.opening_transaction = transaction;
        if published_writer_identity(path).map(|value| value.0) != Some(identity) {
            return Err(invalid_read_image_state(
                "owner cursor file identity changed",
            ));
        }
        #[cfg(target_os = "linux")]
        let _publication_hold = guard.handoff_publication_hold(
            path,
            cancellation,
            deadline.clone(),
            active_ms,
            memory,
        )?;
        guard.snapshot.database_identity = Some(identity);
        guard.publish_bounded(
            path,
            redb::StorageSnapshotIdentity {
                transaction_id: 0,
                user_root: None,
            },
            runtime_dir,
            &|| {
                read_coordination::check_handoff_authority(
                    cancellation,
                    deadline.0.as_ref(),
                    deadline.1,
                    active_ms,
                )
            },
        )?;
        Ok(guard)
    }

    /// Complete an already-published handoff without asking whether the old
    /// writer still owns the companion. The provisional registration pins the
    /// same physical file and database incarnation through this transfer.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn owner_handoff_finish(
        provisional: &Self,
        path: &Path,
        identity: DatabaseIdentity,
        physical_identity: (u64, u64),
        snapshot: redb::StorageSnapshotIdentity,
        runtime_dir: Option<&Path>,
        cancellation: &OwnerReadCancellation,
        deadline: (Arc<dyn contextdb_core::read_contract::DeadlineClock>, u64),
        active_ms: u64,
        memory: crate::read_image_memory::ImageMemoryHandle,
    ) -> std::result::Result<Self, LoadReadImageError> {
        if provisional.snapshot.physical_identity != physical_identity
            || provisional.snapshot.database_identity != Some(identity)
        {
            return Err(invalid_read_image_state(
                "owner cursor file identity changed",
            ));
        }
        let handle_identity = uuid::Uuid::new_v4().into_bytes();
        let mut guard = Self {
            descriptor: Arc::clone(&provisional.descriptor),
            #[cfg(target_os = "linux")]
            presence: None,
            snapshot: RegisteredReaderSnapshot {
                handle_identity,
                ..provisional.snapshot.clone()
            },
            registration: None,
            purge_publication: None,
            // This is a new exact-root registration. The provisional guard
            // stays published while publish() installs this guard in every
            // candidate root. Its erasure baselines are transferred below.
            other_registrations: Vec::new(),
            other_purge_publications: Vec::new(),
            load_lock_held: false,
            reader_hold_held: false,
            record_write_started: false,
            opening_transaction: provisional.opening_transaction.clone(),
        };
        #[cfg(target_os = "linux")]
        let _publication_hold = guard.handoff_publication_hold(
            path,
            cancellation,
            deadline.clone(),
            active_ms,
            memory,
        )?;
        guard.publish_bounded(path, snapshot, runtime_dir, &|| {
            read_coordination::check_handoff_authority(
                cancellation,
                deadline.0.as_ref(),
                deadline.1,
                active_ms,
            )
        })?;
        // The provisional registration began before the owner produced the
        // first page. Preserve that history for each matching registry: a
        // purge between the response and exact-root publication cannot be
        // reclassified as old news by open_for_reader's fresh baseline.
        for continuation in guard
            .purge_publication
            .iter()
            .chain(guard.other_purge_publications.iter())
        {
            let mut continuation = continuation.lock().map_err(|_| LoadReadImageError::Io {
                operation: "transfer owner cursor erasure history",
                reason: "continuation purge fence is unavailable".to_owned(),
            })?;
            for prior in provisional
                .purge_publication
                .iter()
                .chain(provisional.other_purge_publications.iter())
            {
                let prior = prior.lock().map_err(|_| LoadReadImageError::Io {
                    operation: "transfer owner cursor erasure history",
                    reason: "provisional purge fence is unavailable".to_owned(),
                })?;
                continuation.inherit_reader_baseline(&prior);
            }
        }
        Ok(guard)
    }

    pub(crate) fn retain_owner_snapshot(&self) -> Self {
        assert!(
            !self.load_lock_held,
            "only published owner snapshots can be shared"
        );
        Self {
            descriptor: Arc::clone(&self.descriptor),
            #[cfg(target_os = "linux")]
            presence: self.presence.as_ref().map(Arc::clone),
            snapshot: self.snapshot.clone(),
            registration: self.registration.clone(),
            purge_publication: self.purge_publication.clone(),
            other_registrations: self.other_registrations.clone(),
            other_purge_publications: self.other_purge_publications.clone(),
            load_lock_held: false,
            reader_hold_held: false,
            record_write_started: false,
            opening_transaction: self.opening_transaction.clone(),
        }
    }

    /// A direct reader's registration, published PROVISIONALLY -- transaction
    /// zero, no root, which retains every page -- before it asks for anything a
    /// writer waits on. All registry and fence work (including the bounded
    /// departure retry) happens here, holding neither the reader hold nor the
    /// data lock. Then the reader asks the admission gate.
    #[cfg(unix)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_provisional(
        database_path: &Path,
        runtime_dir: Option<&Path>,
    ) -> std::result::Result<Self, LoadReadImageError> {
        Self::open_provisional_with_transaction(database_path, runtime_dir, None)
    }

    #[cfg(unix)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating. "
    )]
    pub(super) fn open_provisional_with_transaction(
        database_path: &Path,
        runtime_dir: Option<&Path>,
        transaction: Option<
            contextdb_core::read_memory::ReadShared<read_coordination::RegistrationTransaction>,
        >,
    ) -> std::result::Result<Self, LoadReadImageError> {
        let mut guard = Self::open_descriptor(database_path, false)?;
        guard.opening_transaction = transaction;
        guard.publish(
            database_path,
            redb::StorageSnapshotIdentity {
                transaction_id: 0,
                user_root: None,
            },
            runtime_dir,
        )?;
        #[cfg(any(test, feature = "test-seams"))]
        read_persistence_test_scaffold::pause_reader_at_point(
            database_path,
            read_persistence_test_scaffold::ReaderPausePoint::AfterProvisional,
        );
        guard.admit()?;
        Ok(guard)
    }

    #[cfg(unix)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating. "
    )]
    pub(super) fn open_provisional_for_read(
        database_path: &Path,
        runtime_dir: Option<&Path>,
        context: &crate::executor::bounded::ReadOpenContext,
    ) -> std::result::Result<Self, LoadReadImageError> {
        context.checkpoint().map_err(LoadReadImageError::Memory)?;
        let mut guard = Self::open_descriptor(database_path, false)?;
        guard.opening_transaction = context.registration_transaction().cloned();
        guard.admit()?;
        guard.publish_bounded(
            database_path,
            redb::StorageSnapshotIdentity {
                transaction_id: 0,
                user_root: None,
            },
            runtime_dir,
            &|| context.checkpoint(),
        )?;
        #[cfg(any(test, feature = "test-seams"))]
        read_persistence_test_scaffold::pause_reader_at_point(
            database_path,
            read_persistence_test_scaffold::ReaderPausePoint::AfterProvisional,
        );
        context.checkpoint().map_err(LoadReadImageError::Memory)?;
        Ok(guard)
    }

    /// After a failed admission has given back its data lock and reader hold:
    /// rewrite every record to say this reader serves nothing. Best effort --
    /// a record that cannot be rewritten stays as it was, and is treated as
    /// uncertain by erasure.
    #[cfg(unix)]
    pub(super) fn mark_failed(&mut self) {
        self.snapshot.storage_snapshot = redb::StorageSnapshotIdentity {
            transaction_id: FAILED_READER_TRANSACTION,
            user_root: None,
        };
        for registration in self
            .registration
            .iter()
            .chain(self.other_registrations.iter())
        {
            let _ = registration.rewrite(&self.snapshot, false);
        }
    }

    /// Ask the admission gate for the reader hold and the data lock.
    #[cfg(unix)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn admit(&mut self) -> std::result::Result<(), LoadReadImageError> {
        match physical_admission::admit_through_reader_gate(
            self.descriptor.as_ref(),
            self.snapshot.physical_identity,
        ) {
            Ok(admitted) => {
                admitted.transfer_to_owner();
                self.load_lock_held = true;
                self.reader_hold_held = true;
                Ok(())
            }
            Err(
                refusal @ (physical_admission::ReaderGateRefusal::Claimed
                | physical_admission::ReaderGateRefusal::GateHeld
                | physical_admission::ReaderGateRefusal::DataHeld),
            ) => Err(LoadReadImageError::Contention {
                reason: refusal.to_string(),
            }),
            Err(refusal) => Err(LoadReadImageError::Io {
                operation: "lock the exact read descriptor",
                reason: refusal.to_string(),
            }),
        }
    }

    /// Under the data lock: take the erasure baselines again at the captured
    /// root, then turn every provisional record into the exact one in place.
    /// Fixed work per publication root, no registry lock, no waiting. A
    /// failure leaves each record provisional, exact or torn-but-live -- all
    /// of which retain every page and refuse erasure -- and the caller gives
    /// the admission locks back before the guard is ever dropped.
    #[cfg(unix)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn upgrade_in_place(
        &mut self,
        database_path: &Path,
        storage_snapshot: redb::StorageSnapshotIdentity,
    ) -> std::result::Result<(), LoadReadImageError> {
        #[cfg(any(test, feature = "test-seams"))]
        let fault = read_persistence_test_scaffold::registration_fault_for_test(database_path);
        #[cfg(not(any(test, feature = "test-seams")))]
        let _ = database_path;
        #[cfg(any(test, feature = "test-seams"))]
        let injected = |what: &str| LoadReadImageError::Io {
            operation: "publish the reader snapshot registration",
            reason: format!("injected {what}"),
        };
        #[cfg(any(test, feature = "test-seams"))]
        if fault == Some(read_persistence_test_scaffold::RegistrationFault::FenceTryFails) {
            return Err(injected("fence try failure"));
        }
        for fence in self
            .purge_publication
            .iter()
            .chain(self.other_purge_publications.iter())
        {
            fence
                .lock()
                .map_err(|_| invalid_read_image_state("the erasure fence is unavailable"))?
                .rebaseline_without_waiting()
                .map_err(|error| LoadReadImageError::Io {
                    operation: "take the erasure baseline at the captured root",
                    reason: error.to_string(),
                })?;
        }
        self.snapshot.storage_snapshot = storage_snapshot;
        self.record_write_started = true;
        for (index, registration) in self
            .registration
            .iter()
            .chain(self.other_registrations.iter())
            .enumerate()
        {
            #[cfg(any(test, feature = "test-seams"))]
            {
                use read_persistence_test_scaffold::RegistrationFault;
                if index > 0 && fault == Some(RegistrationFault::SecondRootStaysProvisional) {
                    return Err(injected("provisional second root"));
                }
                if fault == Some(RegistrationFault::FdatasyncFails) {
                    registration.rewrite(&self.snapshot, false).ok();
                    return Err(injected("record sync failure"));
                }
                let torn = fault == Some(RegistrationFault::TornRecord);
                registration
                    .rewrite(&self.snapshot, torn)
                    .map_err(|error| LoadReadImageError::Io {
                        operation: "publish the reader snapshot registration",
                        reason: error.to_string(),
                    })?;
                continue;
            }
            #[cfg(not(any(test, feature = "test-seams")))]
            {
                let _ = index;
                registration
                    .rewrite(&self.snapshot, false)
                    .map_err(|error| LoadReadImageError::Io {
                        operation: "publish the reader snapshot registration",
                        reason: error.to_string(),
                    })?;
            }
        }
        Ok(())
    }

    #[cfg(unix)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_descriptor(
        database_path: &Path,
        lock: bool,
    ) -> std::result::Result<Self, LoadReadImageError> {
        use std::os::fd::AsRawFd as _;
        use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};

        let descriptor = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(database_path)
            .map_err(|error| read_descriptor_open_error(database_path, error))?;
        let metadata = descriptor
            .metadata()
            .map_err(|error| LoadReadImageError::Io {
                operation: "inspect the exact read descriptor",
                reason: error.to_string(),
            })?;
        let process =
            current_reader_registration_process().map_err(|error| LoadReadImageError::Io {
                operation: "authenticate the reader process",
                reason: error.to_string(),
            })?;
        let descriptor_number = u32::try_from(descriptor.as_raw_fd()).map_err(|_| {
            LoadReadImageError::InvalidState {
                reason: "reader descriptor number does not fit the registration".to_owned(),
            }
        })?;
        let descriptor = Arc::new(descriptor);
        // Admission to the physical store: the one door every starting reader
        // comes through, whatever name it opened the store by. Every refusal
        // and every later failure here gives back what it took.
        let admitted = if lock {
            match physical_admission::admit_through_reader_gate(
                descriptor.as_ref(),
                (metadata.dev(), metadata.ino()),
            ) {
                Ok(admitted) => {
                    // The returned registration guard owns releasing these
                    // bytes on every later failure and successful close.
                    admitted.transfer_to_owner();
                    true
                }
                Err(
                    refusal @ (physical_admission::ReaderGateRefusal::Claimed
                    | physical_admission::ReaderGateRefusal::GateHeld
                    | physical_admission::ReaderGateRefusal::DataHeld),
                ) => {
                    return Err(LoadReadImageError::Contention {
                        reason: refusal.to_string(),
                    });
                }
                Err(refusal) => {
                    return Err(LoadReadImageError::Io {
                        operation: "lock the exact read descriptor",
                        reason: refusal.to_string(),
                    });
                }
            }
        } else {
            false
        };
        let handle_identity = uuid::Uuid::new_v4().into_bytes();
        Ok(Self {
            descriptor,
            #[cfg(target_os = "linux")]
            presence: None,
            snapshot: RegisteredReaderSnapshot {
                physical_identity: (metadata.dev(), metadata.ino()),
                owner_user: LocalUserIdentity(nix::unistd::Uid::effective().as_raw() as u64),
                database_identity: None,
                storage_snapshot: redb::StorageSnapshotIdentity {
                    transaction_id: 0,
                    user_root: None,
                },
                handle_identity,
                process,
                descriptor_number,
            },
            registration: None,
            purge_publication: None,
            other_registrations: Vec::new(),
            other_purge_publications: Vec::new(),
            load_lock_held: admitted,
            reader_hold_held: admitted,
            record_write_started: false,
            opening_transaction: None,
        })
    }

    #[cfg(not(unix))]
    pub(super) fn open(database_path: &Path) -> std::result::Result<Self, LoadReadImageError> {
        let descriptor = OpenOptions::new()
            .read(true)
            .open(database_path)
            .map_err(|error| read_descriptor_open_error(database_path, error))?;
        fs2::FileExt::try_lock_shared(&descriptor).map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                LoadReadImageError::Contention {
                    reason: "the data descriptor is held by a writer".to_owned(),
                }
            } else {
                LoadReadImageError::Io {
                    operation: "lock the exact read descriptor",
                    reason: error.to_string(),
                }
            }
        })?;
        Ok(Self {
            descriptor: Arc::new(descriptor),
            snapshot: RegisteredReaderSnapshot {
                physical_identity: (0, 0),
                owner_user: LocalUserIdentity(0),
                database_identity: None,
                storage_snapshot: redb::StorageSnapshotIdentity {
                    transaction_id: 0,
                    user_root: None,
                },
                handle_identity: uuid::Uuid::new_v4().into_bytes(),
                process: current_reader_registration_process().map_err(|error| {
                    LoadReadImageError::Io {
                        operation: "identify the reader process",
                        reason: error.to_string(),
                    }
                })?,
                descriptor_number: 0,
            },
            registration: None,
            purge_publication: None,
            other_registrations: Vec::new(),
            other_purge_publications: Vec::new(),
            load_lock_held: true,
            reader_hold_held: false,
            record_write_started: false,
            opening_transaction: None,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn from_owner(
        path: &Path,
        identity: DatabaseIdentity,
        writer_run: WriterRunNumber,
        physical_identity: (u64, u64),
        snapshot: redb::StorageSnapshotIdentity,
        runtime_dir: Option<&Path>,
    ) -> std::result::Result<Self, LoadReadImageError> {
        #[cfg(unix)]
        let mut guard = Self::open_descriptor(path, false)?;
        #[cfg(not(unix))]
        let mut guard = Self::open(path)?;
        if guard.snapshot.physical_identity != physical_identity
            || published_writer_identity(path).map(|value| (value.0, value.1))
                != Some((identity, writer_run))
        {
            return Err(invalid_read_image_state(
                "owner cursor file identity changed",
            ));
        }
        guard.publish_with_admission_mode(path, snapshot, runtime_dir, true)?;
        if observed_writer_hold(path) != Some(true) {
            // An accepted handler can publish its first page after ordinary
            // owner withdrawal. Keep a quiescent descriptor locked until its
            // registration is complete, then recheck the same incarnation.
            #[cfg(unix)]
            {
                match physical_admission::admit_through_reader_gate(
                    guard.descriptor.as_ref(),
                    guard.snapshot.physical_identity,
                ) {
                    Ok(admitted) => {
                        admitted.transfer_to_owner();
                        guard.reader_hold_held = true;
                    }
                    Err(refusal) => {
                        return Err(LoadReadImageError::Contention {
                            reason: format!("owner cursor handoff descriptor is held: {refusal}"),
                        });
                    }
                }
            }
            #[cfg(not(unix))]
            fs2::FileExt::try_lock_shared(guard.descriptor.as_ref()).map_err(|error| {
                LoadReadImageError::Contention {
                    reason: format!("owner cursor handoff descriptor is held: {error}"),
                }
            })?;
            guard.load_lock_held = true;
        }
        if guard.snapshot.database_identity != Some(identity)
            || published_writer_identity(path).map(|value| (value.0, value.1))
                != Some((identity, writer_run))
        {
            return Err(invalid_read_image_state(
                "owner cursor companion identity changed",
            ));
        }
        guard.release_load_lock()?;
        Ok(guard)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn publish(
        &mut self,
        database_path: &Path,
        storage_snapshot: redb::StorageSnapshotIdentity,
        runtime_dir: Option<&Path>,
    ) -> std::result::Result<(), LoadReadImageError> {
        self.publish_with_admission_mode(database_path, storage_snapshot, runtime_dir, false)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn publish_with_admission_mode(
        &mut self,
        database_path: &Path,
        storage_snapshot: redb::StorageSnapshotIdentity,
        runtime_dir: Option<&Path>,
        owner_cursor: bool,
    ) -> std::result::Result<(), LoadReadImageError> {
        self.publish_once(
            database_path,
            storage_snapshot,
            runtime_dir,
            owner_cursor,
            owner_cursor,
            None,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating. "
    )]
    fn publish_bounded(
        &mut self,
        database_path: &Path,
        storage_snapshot: redb::StorageSnapshotIdentity,
        runtime_dir: Option<&Path>,
        checkpoint: &dyn Fn() -> Result<()>,
    ) -> std::result::Result<(), LoadReadImageError> {
        checkpoint().map_err(LoadReadImageError::Memory)?;
        let result = self.publish_once(
            database_path,
            storage_snapshot,
            runtime_dir,
            false,
            true,
            Some(checkpoint),
        );
        checkpoint().map_err(LoadReadImageError::Memory)?;
        result
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating. "
    )]
    fn publish_once(
        &mut self,
        database_path: &Path,
        storage_snapshot: redb::StorageSnapshotIdentity,
        runtime_dir: Option<&Path>,
        _owner_cursor: bool,
        nonblocking: bool,
        checkpoint: Option<&dyn Fn() -> Result<()>>,
    ) -> std::result::Result<(), LoadReadImageError> {
        self.snapshot.storage_snapshot = storage_snapshot;
        if self.snapshot.database_identity.is_none() {
            self.snapshot.database_identity =
                published_writer_identity(database_path).map(|(identity, _, _, _)| identity);
        }
        #[cfg(not(unix))]
        return Ok(());
        #[cfg(unix)]
        {
            #[cfg(target_os = "linux")]
            let _publication_hold = if self.reader_hold_held {
                None
            } else {
                use physical_admission::{ByteLock, Granted, READER_HOLD_OFFSET};
                let hold = physical_admission::reopen_independent(&self.descriptor, false)
                    .map_err(|error| LoadReadImageError::Io {
                        operation: "reopen reader publication descriptor",
                        reason: error.to_string(),
                    })?;
                match physical_admission::try_byte(&hold, READER_HOLD_OFFSET, ByteLock::Shared)
                    .map_err(|error| LoadReadImageError::Io {
                        operation: "acquire reader publication hold",
                        reason: error.to_string(),
                    })? {
                    Granted::Yes => Some(hold),
                    Granted::HeldByAnother => {
                        return Err(LoadReadImageError::RegistryContention {
                            path: database_path.to_path_buf(),
                            reason: "reader inventory holds physical publication exclusion"
                                .to_owned(),
                        });
                    }
                }
            };
            let result = (|| {
                let runtimes = reader_publication_directories(runtime_dir);
                if runtimes.is_empty() {
                    return Err(LoadReadImageError::Io {
                        operation: "locate the authoritative erasure publication registry",
                        reason: "no private runtime directory is available".to_owned(),
                    });
                }
                // The physical fence exists before the live snapshot record can
                // become visible. A writer cannot acquire the data descriptor
                // until both are complete, so every registration it can observe
                // has an exact publication coordination object.
                // The fence names the writer incarnation this reader is racing,
                // and that incarnation can only come from a trusted companion --
                // which IS the coordination artifact. With no trusted companion
                // there is no such writer, so there is no fence to publish: a
                // stale, damaged or unreadable companion is not ownership and is
                // read exactly as an absent one is, which is what a store that
                // never had a companion beside it gets. Refusing outright hid a
                // readable idle store behind an artifact that proves nothing.
                //
                // The snapshot registration is published either way. It is what
                // holds this reader's pages against erasure, and a reader that
                // holds pages says so whether or not anybody is listening.
                let open_fence = |runtime: &RuntimeDirectory| {
                    let database_identity = self.snapshot.database_identity?;
                    let open = if nonblocking {
                        purge_publication::open_for_owner_cursor
                    } else {
                        purge_publication::open_for_reader
                    };
                    Some(
                        open(
                            runtime.path(),
                            self.snapshot.physical_identity,
                            database_identity,
                        )
                        .map(Mutex::new)
                        .map_err(|error| {
                            if error.kind() == std::io::ErrorKind::WouldBlock {
                                LoadReadImageError::RegistryContention {
                                    path: runtime.path().to_path_buf(),
                                    reason: error.to_string(),
                                }
                            } else {
                                LoadReadImageError::IoAtPath {
                                    operation: "publish the authoritative erasure fence",
                                    path: runtime.path().to_path_buf(),
                                    reason: error.to_string(),
                                }
                            }
                        }),
                    )
                };
                // Each root is joined under a shared lifetime hold, never two
                // at once. The per-store registry lock excludes this store's
                // leave; the shared hold prevents removal of the root while
                // unrelated stores may still join or leave. An owner cursor takes
                // every lock here by try and refuses, retryably, rather than wait;
                // a later root's refusal drops the roots already joined, whose
                // owner teardown is itself try-only.
                let mut fences = Vec::with_capacity(runtimes.len());
                let mut registrations = Vec::with_capacity(runtimes.len());
                let contention = |runtime: &RuntimeDirectory, error: std::io::Error| {
                    LoadReadImageError::RegistryContention {
                        path: runtime.path().to_path_buf(),
                        reason: error.to_string(),
                    }
                };
                let io_failure = |runtime: &RuntimeDirectory, error: std::io::Error| {
                    LoadReadImageError::IoAtPath {
                        operation: "publish the reader snapshot registration",
                        path: runtime.path().to_path_buf(),
                        reason: error.to_string(),
                    }
                };
                for runtime in &runtimes {
                    #[cfg(any(test, feature = "test-seams"))]
                    read_persistence_test_scaffold::pause_reader_at_physical(
                        self.snapshot.physical_identity,
                        read_persistence_test_scaffold::ReaderPausePoint::BeforeRegistration,
                    );
                    let serializer = loop {
                        if let Some(checkpoint) = checkpoint {
                            checkpoint().map_err(LoadReadImageError::Memory)?;
                        }
                        match hold_runtime_root(runtime.root(), nonblocking) {
                            Ok(hold) => break hold,
                            Err(error)
                                if error.kind() == std::io::ErrorKind::WouldBlock
                                    && checkpoint.is_some() =>
                            {
                                #[cfg(any(test, feature = "test-seams"))]
                                if read_persistence_test_scaffold::reader_join_wait_hook_installed_for_test() {
                                    read_persistence_test_scaffold::run_reader_join_wait_hook_for_test();
                                }
                                std::thread::yield_now();
                            }
                            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                return Err(contention(runtime, error));
                            }
                            Err(error) => return Err(io_failure(runtime, error)),
                        }
                    };
                    recreate_created_runtime_root(runtime);
                    #[cfg(any(test, feature = "test-seams"))]
                    read_persistence_test_scaffold::pause_reader_at_physical(
                        self.snapshot.physical_identity,
                        read_persistence_test_scaffold::ReaderPausePoint::AfterRuntimeRootLifetimeHold,
                    );
                    let registry_lock = if _owner_cursor {
                        ReaderSnapshotRegistryLock::try_acquire_for_registration(
                            runtime.path(),
                            self.snapshot.physical_identity,
                        )
                    } else if let Some(checkpoint) = checkpoint {
                        let mut failure = None;
                        let mut wait = || match checkpoint() {
                            Ok(()) => Ok(()),
                            Err(error) => {
                                failure = Some(error);
                                Err(std::io::ErrorKind::Interrupted.into())
                            }
                        };
                        let locked = ReaderSnapshotRegistryLock::acquire_for_registration_with_wait(
                            runtime.path(),
                            self.snapshot.physical_identity,
                            Some(&mut wait),
                        );
                        if let Some(error) = failure {
                            return Err(LoadReadImageError::Memory(error));
                        }
                        locked
                    } else {
                        ReaderSnapshotRegistryLock::acquire_for_registration(
                            runtime.path(),
                            self.snapshot.physical_identity,
                        )
                    }
                    .map_err(|error| {
                        if error.kind() == std::io::ErrorKind::WouldBlock {
                            contention(runtime, error)
                        } else {
                            io_failure(runtime, error)
                        }
                    })?;
                    let fence = open_fence(runtime).transpose()?;
                    #[cfg(any(test, feature = "test-seams"))]
                    read_persistence_test_scaffold::reader_fences_opened_for_test(
                        std::slice::from_ref(runtime),
                        self.snapshot.physical_identity,
                        _owner_cursor,
                    );
                    let target = ReaderRegistrationTarget {
                        database_path,
                        runtime,
                        preserve_root: runtime_dir
                            .is_some_and(|supplied| supplied == runtime.path()),
                        fence: fence.as_ref(),
                    };
                    let created = ReaderSnapshotFileGuard::create_locked(
                        &target,
                        &self.snapshot,
                        registry_lock,
                        _owner_cursor,
                        self.opening_transaction.clone(),
                    );
                    // The acquired registry lock spans the fence open and
                    // registration, excluding same-store last-reader removal.
                    let mut registration = created.map_err(|error| {
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::NotFound | std::io::ErrorKind::WouldBlock
                        ) {
                            contention(runtime, error)
                        } else {
                            io_failure(runtime, error)
                        }
                    })?;
                    registration
                        .registry_lock
                        .unlock()
                        .map_err(|error| io_failure(runtime, error))?;
                    drop(serializer);
                    fences.push(fence);
                    registrations.push(Arc::new(registration));
                }
                for fence in fences.into_iter().flatten() {
                    let fence = Arc::new(fence);
                    if self.purge_publication.is_none() {
                        self.purge_publication = Some(fence);
                    } else {
                        self.other_purge_publications.push(fence);
                    }
                }
                for registration in registrations {
                    if self.registration.is_none() {
                        self.registration = Some(registration);
                    } else {
                        self.other_registrations.push(registration);
                    }
                }
                // Publish before making physical presence visible. Until then
                // this guard serves nothing, and a direct capture still owns its
                // hydration hold. The registry must already account for every
                // presence offset maintenance can observe.
                #[cfg(target_os = "linux")]
                if self.presence.is_none() {
                    self.presence = Some(Arc::new(
                        reader_presence::ReaderPresenceGuard::acquire(
                            Arc::clone(&self.descriptor),
                            self.snapshot.handle_identity,
                        )
                        .map_err(|error| LoadReadImageError::Io {
                            operation: "hold reader presence on the database file",
                            reason: error.to_string(),
                        })?,
                    ));
                }
                Ok(())
            })();
            if result.is_err() {
                #[cfg(target_os = "linux")]
                {
                    self.presence = None;
                }
                self.registration = None;
                self.other_registrations.clear();
                self.purge_publication = None;
                self.other_purge_publications.clear();
            }
            result
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn publish_bounded_result<T>(
        &self,
        scope: &crate::executor::ReadPublicationScope,
        value: T,
    ) -> Result<T> {
        self.with_publication_fences(value, |fence, value| {
            fence.publish_with_budget(
                scope.snapshot_lsn,
                &scope.tables,
                value,
                Some(&scope.budget),
            )
        })
    }

    /// A replacement route cannot discard an erasure published since the
    /// retained reader's snapshot, even when the new writer has newer rows.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn check_turnover_with_budget(
        &self,
        snapshot_lsn: Lsn,
        budget: &crate::executor::ReadPublicationBudget,
    ) -> Result<()> {
        self.with_publication_fences((), |fence, ()| {
            fence.check_turnover_with_budget(snapshot_lsn, budget)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn with_publication_fences<T>(
        &self,
        value: T,
        mut publish: impl FnMut(&mut purge_publication::Fence, T) -> Result<T>,
    ) -> Result<T> {
        // A fence exists when a trusted companion named the writer incarnation
        // this reader is racing. Without one there is no such writer and no
        // erasure to be caught by, so the result is the reader's own and goes
        // back unchanged -- the same answer a store with no companion beside
        // it gives.
        let Some(fence) = self.purge_publication.as_ref() else {
            return Ok(value);
        };
        let mut value = value;
        for fence in std::iter::once(fence).chain(self.other_purge_publications.iter()) {
            let mut fence = fence.lock().map_err(|_| {
                Error::ReadFailure(
                    ReadFailure::new(
                        ReadFailureKind::AuthoritativeErasure,
                        ReadFailureDetail::Reason {
                            reason: "authoritative erasure publication state is unavailable"
                                .to_owned(),
                        },
                    )
                    .expect("authoritative-erasure refusal carries ordinary reason detail"),
                )
            })?;
            value = publish(&mut fence, value)?;
        }
        Ok(value)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn view(&self) -> std::result::Result<ReadSnapshotFileView, LoadReadImageError> {
        self.descriptor
            .try_clone()
            .map(|file| ReadSnapshotFileView { file })
            .map_err(|error| LoadReadImageError::Io {
                operation: "clone the exact read descriptor",
                reason: error.to_string(),
            })
    }

    pub(super) fn file_len(&self) -> Option<u64> {
        self.descriptor
            .metadata()
            .map(|metadata| metadata.len())
            .ok()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn release_load_lock(&mut self) -> std::result::Result<(), LoadReadImageError> {
        if self.load_lock_held {
            #[cfg(unix)]
            let released = physical_admission::release_data(self.descriptor.as_ref());
            #[cfg(not(unix))]
            let released = fs2::FileExt::unlock(self.descriptor.as_ref());
            released.map_err(|error| {
                LoadReadImageError::Release(format!(
                    "release exact read-descriptor load lock: {error}"
                ))
            })?;
            self.load_lock_held = false;
        }
        self.release_reader_hold()
    }

    /// Give back the admission reader hold, after the data lock.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn release_reader_hold(&mut self) -> std::result::Result<(), LoadReadImageError> {
        #[cfg(unix)]
        if self.reader_hold_held {
            physical_admission::release_byte(
                self.descriptor.as_ref(),
                physical_admission::READER_HOLD_OFFSET,
            )
            .map_err(|error| {
                LoadReadImageError::Release(format!("release the admission reader hold: {error}"))
            })?;
            self.reader_hold_held = false;
        }
        Ok(())
    }

    pub(crate) fn database_identity(&self) -> Option<DatabaseIdentity> {
        self.snapshot.database_identity
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn observation(&self) -> ReaderSnapshotRegistrationObservation {
        ReaderSnapshotRegistrationObservation {
            snapshot: self.snapshot.clone(),
            path: self.registration.as_ref().map(|guard| guard.path.clone()),
            descriptor_open: self.descriptor.metadata().is_ok(),
            load_lock_held: self.load_lock_held,
        }
    }
}

impl Drop for ReadSnapshotRegistrationGuard {
    fn drop(&mut self) {
        // The authenticated registration must outlive physical presence,
        // including while registry teardown waits for its serializer.
        #[cfg(target_os = "linux")]
        drop(self.presence.take());
        if self.load_lock_held {
            #[cfg(unix)]
            let _ = physical_admission::release_data(self.descriptor.as_ref());
            #[cfg(not(unix))]
            let _ = fs2::FileExt::unlock(self.descriptor.as_ref());
        }
        let _ = self.release_reader_hold();
        // Teardown must enter the registry critical section while the
        // authenticated data descriptor still exists. Rust would otherwise
        // drop `descriptor` before the later `registration` field.
        let staging = self
            .opening_transaction
            .as_ref()
            .is_some_and(|transaction| transaction.is_staging());
        for registration in self
            .registration
            .take()
            .into_iter()
            .chain(self.other_registrations.drain(..))
        {
            if staging {
                // Only the thread consuming the final strong count owns the record.
                // Other readers retain their original normal teardown authority.
                if let Some(mut registration) = Arc::into_inner(registration) {
                    registration.try_only_teardown = true;
                    drop(registration);
                }
            } else {
                drop(registration);
            }
        }
        drop(self.purge_publication.take());
        self.other_purge_publications.clear();
    }
}

#[cfg(target_os = "linux")]
pub(super) fn verified_linux_reader_process(
    breadcrumb: &ReaderBreadcrumb,
) -> Option<ReaderProcessIdentity> {
    if !safe_reader_process_display_name(&breadcrumb.process_name) {
        return None;
    }
    let process_directory = PathBuf::from(format!("/proc/{}", breadcrumb.process_id));
    let stat = std::fs::read_to_string(process_directory.join("stat")).ok()?;
    let after_name = stat.rfind(") ").map(|index| &stat[index + 2..])?;
    let process_start = after_name
        .split_whitespace()
        .nth(19)?
        .parse::<u64>()
        .ok()
        .map(contextdb_core::read_contract::ProcessStartIdentity)?;
    let mut names = HashSet::new();
    if let Ok(name) = std::fs::read_to_string(process_directory.join("comm")) {
        names.insert(name.strip_suffix('\n').unwrap_or(&name).to_owned());
    }
    if let Ok(executable) = std::fs::read_link(process_directory.join("exe"))
        && let Some(name) = executable.file_name().and_then(|name| name.to_str())
    {
        names.insert(name.to_owned());
    }
    if let Ok(command_line) = std::fs::read(process_directory.join("cmdline"))
        && let Some(argument_zero) = command_line.split(|byte| *byte == 0).next()
        && let Ok(argument_zero) = std::str::from_utf8(argument_zero)
        && let Some(name) = Path::new(argument_zero)
            .file_name()
            .and_then(|name| name.to_str())
    {
        names.insert(name.to_owned());
    }
    if !names.contains(&breadcrumb.process_name) {
        return None;
    }
    Some(ReaderProcessIdentity {
        process_id: breadcrumb.process_id,
        process_start,
    })
}

pub(super) fn verify_locked_reader_breadcrumb(
    breadcrumb: ReaderBreadcrumb,
) -> Option<ReaderBreadcrumb> {
    #[cfg(target_os = "linux")]
    {
        let observed = verified_linux_reader_process(&breadcrumb)?;
        breadcrumb.is_live_for(&observed).then_some(breadcrumb)
    }
    #[cfg(target_os = "macos")]
    {
        if !safe_reader_process_display_name(&breadcrumb.process_name) {
            return None;
        }
        let observed = macos_reader_breadcrumb(breadcrumb.process_id).ok()?;
        (observed == breadcrumb).then_some(breadcrumb)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = breadcrumb;
        None
    }
}

#[cfg(unix)]
pub(super) fn trusted_reader_breadcrumb_metadata(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    metadata.file_type().is_file()
        && metadata.uid() == nix::unistd::Uid::effective().as_raw()
        && metadata.mode() & 0o7777 == 0o600
        && metadata.nlink() == 1
        && metadata.len() <= MAX_READER_BREADCRUMB_BYTES as u64
}

#[cfg(unix)]
pub(super) fn same_reader_breadcrumb_inode(
    left: &std::fs::Metadata,
    right: &std::fs::Metadata,
) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.uid() == right.uid()
        && left.mode() == right.mode()
        && left.nlink() == right.nlink()
        && left.len() == right.len()
}

#[cfg(unix)]
pub(super) fn open_trusted_reader_breadcrumb(
    path: &Path,
) -> std::io::Result<(File, std::fs::Metadata)> {
    let entry_metadata = std::fs::symlink_metadata(path)?;
    if !trusted_reader_breadcrumb_metadata(&entry_metadata) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "reader breadcrumb entry is not a trusted private regular file",
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options.open(path)?;
    let descriptor_metadata = file.metadata()?;
    if !trusted_reader_breadcrumb_metadata(&descriptor_metadata)
        || !same_reader_breadcrumb_inode(&entry_metadata, &descriptor_metadata)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "reader breadcrumb changed while it was opened",
        ));
    }
    Ok((file, descriptor_metadata))
}

#[cfg(not(unix))]
pub(super) fn open_trusted_reader_breadcrumb(
    _path: &Path,
) -> std::io::Result<(File, std::fs::Metadata)> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "reader breadcrumb inspection requires Unix no-follow opens",
    ))
}

#[cfg(unix)]
pub(super) fn reader_breadcrumb_path_still_names(
    path: &Path,
    inspected: &std::fs::Metadata,
) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|current| {
        trusted_reader_breadcrumb_metadata(&current)
            && same_reader_breadcrumb_inode(inspected, &current)
    })
}

#[cfg(not(unix))]
pub(super) fn reader_breadcrumb_path_still_names(
    _path: &Path,
    _inspected: &std::fs::Metadata,
) -> bool {
    false
}

#[cfg(unix)]
pub(super) fn trusted_reader_snapshot_metadata(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    metadata.file_type().is_file()
        && metadata.uid() == nix::unistd::Uid::effective().as_raw()
        && metadata.mode() & 0o7777 == 0o600
        && metadata.nlink() == 1
        && metadata.len() <= MAX_READER_SNAPSHOT_BYTES
}

#[cfg(unix)]
pub(super) fn trusted_reader_snapshot_registry_lock_metadata(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    metadata.file_type().is_file()
        && metadata.uid() == nix::unistd::Uid::effective().as_raw()
        && metadata.mode() & 0o7777 == 0o600
        && metadata.nlink() == 1
        && metadata.len() == 0
}

#[cfg(not(unix))]
pub(super) fn trusted_reader_snapshot_registry_lock_metadata(
    _metadata: &std::fs::Metadata,
) -> bool {
    false
}

/// Whether an open registry-lock descriptor is a lock file a departing reader
/// has already unlinked: the trusted private empty regular file in every
/// respect except that no name reaches it any more.
#[cfg(unix)]
pub(super) fn registry_lock_descriptor_departed(file: &File) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    file.metadata().is_ok_and(|metadata| {
        metadata.file_type().is_file()
            && metadata.uid() == nix::unistd::Uid::effective().as_raw()
            && metadata.mode() & 0o7777 == 0o600
            && metadata.nlink() == 0
            && metadata.len() == 0
    })
}
