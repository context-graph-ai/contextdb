//! Seal only the current operation's paged native dependency closure.
//! A borrowed native writer supplies the exclusive transaction and real scope.
use super::*;
use super::{
    control::ScopeRecord,
    header,
    io::{KindTableName, NativeIo, PENDING},
    memory::{MARKER_BYTES, OPERATION_PREFIX_BYTES, SealWorkspace},
    semantic::SealView,
};
use contextdb_vector::ivf::storage::PageKey;
use redb::ReadableTable;
use std::cell::{Ref, RefCell};

pub(super) trait NativeSealView: SealView {
    fn write(&self) -> &redb::WriteTransaction;
    fn scope_key(&self) -> &[u8; 64];
    fn record(&self) -> Ref<'_, ScopeRecord>;
    fn record_cell(&self) -> &RefCell<ScopeRecord>;
    fn dirty_kinds(&self) -> [u16; control::ROOTS];
    fn commit_frontier(&self) -> Result<u64>;
    fn header_written(&self, key: PageKey) -> Result<()>;
    fn roots_staged(&self) -> Result<()>;
}

pub(super) fn operation_prefix(scope: &[u8; 64], operation: u64) -> [u8; OPERATION_PREFIX_BYTES] {
    let mut key = [0; OPERATION_PREFIX_BYTES];
    key[..64].copy_from_slice(scope);
    key[64..].copy_from_slice(&operation.to_be_bytes());
    key
}
pub(super) fn marker_key(
    scope: &[u8; 64],
    operation: u64,
    storage: u64,
    page: PageKey,
) -> [u8; MARKER_BYTES] {
    let mut key = [0; MARKER_BYTES];
    key[..OPERATION_PREFIX_BYTES].copy_from_slice(&operation_prefix(scope, operation));
    key[OPERATION_PREFIX_BYTES..OPERATION_PREFIX_BYTES + 8].copy_from_slice(&storage.to_be_bytes());
    key[OPERATION_PREFIX_BYTES + 8..].copy_from_slice(&header::key_bytes(page));
    key
}

/// Preserve the first old counter payload for this operation; repeated puts
/// never replace it with an intermediate counter state. Empty is header-only.
pub(super) fn mark(
    write: &redb::WriteTransaction,
    key: &[u8; MARKER_BYTES],
    first_old: Option<&[u8; PAGE_BYTES]>,
) -> Result<bool> {
    let mut pending = write
        .open_table(PENDING)
        .map_err(RedbPersistence::storage_error)?;
    let (new, upgrade) = {
        let value = pending
            .get(key.as_slice())
            .map_err(RedbPersistence::storage_error)?;
        match value {
            None => (true, false),
            Some(value) => {
                if value.value().len() != 0 && value.value().len() != PAGE_BYTES {
                    return Err(corrupt());
                }
                (false, value.value().is_empty() && first_old.is_some())
            }
        }
    };
    if new || upgrade {
        pending
            .insert(key.as_slice(), first_old.map_or(&[][..], |p| p.as_slice()))
            .map_err(RedbPersistence::storage_error)?;
    }
    Ok(new)
}

/// A native first-key cursor is reopened after removal. Dependency insertion
/// before the previous key cannot be skipped; no decoded pending set exists.
fn first_marker(
    view: &impl NativeSealView,
    prefix: &[u8; OPERATION_PREFIX_BYTES],
    work: &mut SealWorkspace,
) -> Result<bool> {
    view.checkpoint()?;
    let pending = view
        .write()
        .open_table(PENDING)
        .map_err(RedbPersistence::storage_error)?;
    let mut range = pending
        .range(prefix.as_slice()..)
        .map_err(RedbPersistence::storage_error)?;
    let Some(entry) = range.next() else {
        return Ok(false);
    };
    let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
    if !key.value().starts_with(prefix) {
        return Ok(false);
    }
    if key.value().len() != MARKER_BYTES {
        return Err(corrupt());
    }
    work.marker.copy_from_slice(key.value());
    match value.value().len() {
        0 => {
            work.original_present = false;
            work.previous_payload.fill(0);
        }
        PAGE_BYTES => {
            work.original_present = true;
            work.previous_payload.copy_from_slice(value.value());
        }
        _ => return Err(corrupt()),
    }
    Ok(true)
}

pub(super) fn require_operation_empty(
    write: &redb::WriteTransaction,
    prefix: &[u8; OPERATION_PREFIX_BYTES],
) -> Result<()> {
    require_pending_prefix_empty(write, prefix)
}
pub(super) fn require_pending_prefix_empty(
    write: &redb::WriteTransaction,
    prefix: &[u8],
) -> Result<()> {
    let pending = write
        .open_table(PENDING)
        .map_err(RedbPersistence::storage_error)?;
    let mut range = pending
        .range(prefix..)
        .map_err(RedbPersistence::storage_error)?;
    if let Some(entry) = range.next() {
        let (key, _) = entry.map_err(RedbPersistence::storage_error)?;
        if key.value().starts_with(prefix) {
            return Err(corrupt());
        }
    }
    Ok(())
}
pub(super) fn require_global_empty(write: &redb::WriteTransaction) -> Result<()> {
    let pending = write
        .open_table(PENDING)
        .map_err(RedbPersistence::storage_error)?;
    if pending
        .first()
        .map_err(RedbPersistence::storage_error)?
        .is_some()
    {
        return Err(corrupt());
    }
    Ok(())
}

pub(super) fn seal_operation(
    view: &impl NativeSealView,
    operation: u64,
    work: &mut SealWorkspace,
) -> Result<()> {
    let prefix = operation_prefix(view.scope_key(), operation);
    while first_marker(view, &prefix, work)? {
        view.checkpoint()?;
        let key = header::key_from_bytes(&work.marker[OPERATION_PREFIX_BYTES + 8..])?;
        let storage = u64::from_be_bytes(
            work.marker[OPERATION_PREFIX_BYTES..OPERATION_PREFIX_BYTES + 8]
                .try_into()
                .map_err(|_| corrupt())?,
        );
        {
            let record = view.record();
            let family = record.family(key.generation).ok_or_else(corrupt)?;
            if family.storage != storage {
                return Err(corrupt());
            }
        }
        let name = KindTableName::new(storage, key.kind)?;
        NativeIo::Borrowed(view.write()).with_existing_value(
            name.as_str()?,
            &header::key_bytes(key),
            |value| {
                let value = value.ok_or_else(corrupt)?;
                if value.len() != header::VALUE_BYTES {
                    return Err(corrupt());
                }
                work.native_value.copy_from_slice(value);
                Ok(())
            },
        )?;
        {
            let payload: &[u8; PAGE_BYTES] = work.native_value[header::HEADER_BYTES..]
                .try_into()
                .map_err(|_| corrupt())?;
            {
                let record = view.record();
                let family = record.family(key.generation).ok_or_else(corrupt)?;
                header::check_staged(
                    header::HeaderIdentity {
                        index: &record.index,
                        tuple: &record.tuple,
                        family,
                    },
                    key,
                    &work.native_value,
                )?;
            }
            if work.original_present {
                semantic::payload_dependencies(
                    view,
                    key,
                    &work.previous_payload,
                    payload,
                    &mut work.related_metadata,
                )?;
            }
            let records = semantic::derive(view, key, payload, &mut work.related_metadata)?;
            let record = view.record();
            let family = record.family(key.generation).ok_or_else(corrupt)?;
            header::seal_in_place(
                header::HeaderIdentity {
                    index: &record.index,
                    tuple: &record.tuple,
                    family,
                },
                key,
                records,
                &mut work.native_value,
            )?;
        }
        view.checkpoint()?;
        NativeIo::Borrowed(view.write()).put(
            name.as_str()?,
            &header::key_bytes(key),
            &work.native_value,
        )?;
        view.header_written(key)?;
        {
            let record = view.record();
            let family = record.family(key.generation).ok_or_else(corrupt)?;
            super::frontier::stage(
                &NativeIo::Borrowed(view.write()),
                view.scope_key(),
                family,
                key,
                view.commit_frontier()?,
                &mut work.control_key,
                &mut work.previous_payload,
            )?;
        }
        view.write()
            .open_table(PENDING)
            .map_err(RedbPersistence::storage_error)?
            .remove(work.marker.as_slice())
            .map_err(RedbPersistence::storage_error)?;
    }
    require_operation_empty(view.write(), &prefix)?;
    work.changed_kinds = view.dirty_kinds();
    // A journal-only or visibility-only operation need not publish a manifest.
    // Advance only families with actual sealed mutations, using the real row
    // commit LSN. This does not advance authoritative source completeness.
    let committed_through = view.commit_frontier()?;
    {
        let mut record = view.record_cell().borrow_mut();
        for slot in 0..control::ROOTS {
            if work.changed_kinds[slot] == 0 {
                continue;
            }
            let family = record.families[slot].as_mut().ok_or_else(corrupt)?;
            if committed_through < family.source_frontier
                || committed_through < family.committed_through_lsn
            {
                return Err(corrupt());
            }
            family.committed_through_lsn = committed_through;
        }
    }
    // All value/table/range handles above have dropped before root capture.
    for slot in 0..control::ROOTS {
        for tag in 1u8..=16 {
            let bit = 1u16 << (tag - 1);
            if work.changed_kinds[slot] & bit == 0 {
                continue;
            }
            view.checkpoint()?;
            let (storage, generation) = {
                let record = view.record();
                let family = record.families[slot].as_ref().ok_or_else(corrupt)?;
                (family.storage, family.generation)
            };
            let kind = header::kind_from_tag(tag)?;
            let name = KindTableName::new(storage, kind)?;
            let root = NativeIo::Borrowed(view.write())
                .fingerprint(name.as_str()?)?
                .ok_or_else(corrupt)?;
            let mut record = view.record_cell().borrow_mut();
            let family = record.families[slot].as_mut().ok_or_else(corrupt)?;
            if family.storage != storage || family.generation != generation {
                return Err(corrupt());
            }
            family.kinds.present |= bit;
            family.kinds.roots[usize::from(tag - 1)] = root;
        }
    }
    // The scope table is outside every kind root. This write cannot mutate
    // any already captured family fingerprint.
    view.checkpoint()?;
    {
        let record = view.record();
        record.encode(view.scope_key(), work.control_payload()?)?;
    }
    NativeIo::Borrowed(view.write()).put(
        SCOPES.name(),
        view.scope_key(),
        &work.native_value[..PAGE_BYTES],
    )?;
    view.roots_staged()
}
