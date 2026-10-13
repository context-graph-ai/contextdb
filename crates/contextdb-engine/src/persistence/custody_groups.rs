//! Private derived membership; never CONFIG, content identity or export data.
use super::*;

pub(super) const GROUP_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("read_custody_groups");
const REVERSE_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("read_custody_groups_reverse");
pub(super) const COMPLETE: &[u8] = b"";
pub(super) const COMPLETE_VALUE: &[u8] = b"complete";

pub(super) fn invalidate(write: &redb::WriteTransaction) -> Result<()> {
    // An absent table is never created by maintenance or repair.
    if write
        .table_root_fingerprint(GROUP_TABLE)
        .map_err(RedbPersistence::storage_error)?
        .is_some()
    {
        write
            .open_table(GROUP_TABLE)
            .map_err(RedbPersistence::storage_error)?
            .remove(COMPLETE)
            .map_err(RedbPersistence::storage_error)?;
    }
    Ok(())
}

/// Start a complete, empty index in this transaction. The caller then
/// maintains every committed custody entry into it with funded scratch, so the
/// marker is true again when the transaction commits.
pub(super) fn reset_complete(write: &redb::WriteTransaction) -> Result<()> {
    for result in [
        write.delete_table(GROUP_TABLE),
        write.delete_table(REVERSE_TABLE),
    ] {
        match result {
            Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(RedbPersistence::storage_error(error)),
        }
    }
    write
        .open_table(REVERSE_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    write
        .open_table(GROUP_TABLE)
        .map_err(RedbPersistence::storage_error)?
        .insert(COMPLETE, COMPLETE_VALUE)
        .map_err(RedbPersistence::storage_error)?;
    super::custody_pending::create(write)
}

pub(super) fn initialize_empty(write: &redb::WriteTransaction) -> Result<()> {
    // Only an actual empty database can acquire the completion marker. This is
    // deliberately stronger than finding an empty custody mirror.
    if write
        .list_tables()
        .map_err(RedbPersistence::storage_error)?
        .next()
        .is_some()
    {
        return Ok(());
    }
    write
        .open_table(REVERSE_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    write
        .open_table(GROUP_TABLE)
        .map_err(RedbPersistence::storage_error)?
        .insert(COMPLETE, COMPLETE_VALUE)
        .map_err(RedbPersistence::storage_error)?;
    super::custody_pending::create(write)
}

mod native {
    use super::*;
    use crate::custody::{
        canonical::invalid,
        native_record_codec::{decode_record, finite_into, finite_len, visit_groups},
    };

    fn damage<T>(_: T) -> Error {
        invalid()
    }
    fn add(a: usize, b: usize) -> Result<usize> {
        a.checked_add(b).ok_or_else(invalid)
    }
    fn wide(n: usize) -> Result<[u8; 8]> {
        Ok(u64::try_from(n).map_err(damage)?.to_be_bytes())
    }
    fn take<'a>(bytes: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
        let (head, tail) = bytes.split_at_checked(n).ok_or_else(invalid)?;
        *bytes = tail;
        Ok(head)
    }
    fn length(bytes: &mut &[u8]) -> Result<usize> {
        usize::try_from(u64::from_be_bytes(
            take(bytes, 8)?.try_into().map_err(damage)?,
        ))
        .map_err(damage)
    }

    /// Flat reverse value: repeated [u64 full-key length, full-key]. One exact
    /// byte allocation; group walks, hashing, and length passes use only stack.
    fn reverse_size(key: &str, value: &[u8]) -> Result<usize> {
        let record = decode_record(value).map_err(damage)?;
        let mut size = 0usize;
        visit_groups(&record, &mut |walk| {
            let group = finite_len(|e| walk(e))?;
            u64::try_from(group).map_err(|_| crate::custody::native_decode::Invalid::Codec)?;
            let entry = group
                .checked_add(key.len())
                .and_then(|n| n.checked_add(16))
                .ok_or(crate::custody::native_decode::Invalid::Codec)?;
            size = size
                .checked_add(entry)
                .ok_or(crate::custody::native_decode::Invalid::Codec)?;
            Ok(())
        })
        .map_err(damage)?;
        Ok(size)
    }
    fn fill_reverse(key: &str, value: &[u8], bytes: &mut [u8]) -> Result<()> {
        let record = decode_record(value).map_err(damage)?;
        let mut offset = 0usize;
        visit_groups(&record, &mut |walk| {
            use crate::custody::native_decode::Invalid;
            let group = finite_len(|e| walk(e))?;
            let full = group
                .checked_add(key.len())
                .and_then(|n| n.checked_add(8))
                .ok_or(Invalid::Codec)?;
            let end = offset
                .checked_add(8)
                .and_then(|n| n.checked_add(full))
                .ok_or(Invalid::Codec)?;
            let target = bytes.get_mut(offset..end).ok_or(Invalid::Codec)?;
            target[..8].copy_from_slice(
                &u64::try_from(full)
                    .map_err(|_| Invalid::Codec)?
                    .to_be_bytes(),
            );
            target[8..16].copy_from_slice(
                &u64::try_from(group)
                    .map_err(|_| Invalid::Codec)?
                    .to_be_bytes(),
            );
            finite_into(&mut target[16..16 + group], |e| walk(e))?;
            target[16 + group..].copy_from_slice(key.as_bytes());
            offset = end;
            Ok(())
        })
        .map_err(damage)?;
        if offset != bytes.len() {
            return Err(invalid());
        }
        Ok(())
    }

    pub(in super::super) fn plan_bytes(key: &str, value: &[u8]) -> Result<usize> {
        if !crate::custody::records::owned_key(key) {
            return Ok(0);
        }
        // Each plan item retains key, value digest and the complete reverse
        // value. No per-entry Vec/String or allocator metadata object exists.
        add(
            add(add(key.len(), 48)?, reverse_size(key, value)?)?,
            std::mem::size_of::<usize>(),
        )
    }
    impl GroupPlan {
        /// Exact single-occurrence sibling for the artifact's existing upfront
        /// proof admission. Both allocations remain inside that admission.
        pub(in super::super) fn single_prepaid(key: &str, value: &[u8]) -> Result<Self> {
            let total = plan_bytes(key, value)?;
            if total == 0 {
                return Ok(Self {
                    bytes: Vec::new(),
                    offsets: Vec::new(),
                });
            }
            let size = total - std::mem::size_of::<usize>();
            let mut bytes = vec![0; size];
            bytes[..8].copy_from_slice(&wide(key.len())?);
            bytes[8..8 + key.len()].copy_from_slice(key.as_bytes());
            let start = 8 + key.len();
            bytes[start..start + 32].copy_from_slice(blake3::hash(value).as_bytes());
            bytes[start + 32..start + 40].copy_from_slice(&wide(size - start - 40)?);
            fill_reverse(key, value, &mut bytes[start + 40..])?;
            Ok(Self {
                bytes,
                offsets: vec![0],
            })
        }
        /// Caller has prepaid plan_bytes for each item on the exact original
        /// frame before opening its transaction. No admission occurs here.
        pub(in super::super) fn prepaid<'a>(
            values: impl Iterator<Item = &'a (String, Vec<u8>)> + Clone,
        ) -> Result<Self> {
            let slots = values
                .clone()
                .filter(|(key, _)| crate::custody::records::owned_key(key))
                .count();
            let total = values
                .clone()
                .try_fold(0usize, |n, (key, value)| add(n, plan_bytes(key, value)?))?;
            let directory = std::alloc::Layout::array::<usize>(slots)
                .map_err(damage)?
                .size();
            let size = total.checked_sub(directory).ok_or_else(invalid)?;
            let mut bytes = vec![0; size];
            let mut offsets = Vec::with_capacity(slots);
            let mut offset = 0usize;
            for (key, value) in values {
                let planned = plan_bytes(key, value)?;
                if planned == 0 {
                    continue;
                }
                let size = planned - std::mem::size_of::<usize>();
                offsets.push(offset);
                let item = &mut bytes[offset..offset + size];
                item[..8].copy_from_slice(&wide(key.len())?);
                item[8..8 + key.len()].copy_from_slice(key.as_bytes());
                let start = 8 + key.len();
                item[start..start + 32].copy_from_slice(blake3::hash(value).as_bytes());
                item[start + 32..start + 40].copy_from_slice(&wide(size - start - 40)?);
                fill_reverse(key, value, &mut item[start + 40..])?;
                offset += size;
            }
            // Allocation-free sort of the fixed, prepaid offset directory.
            let mut plan = Self { bytes, offsets };
            let bytes = &plan.bytes;
            plan.offsets.sort_unstable_by(|a, b| {
                let (ak, ah, _) = Self::entry(&bytes[*a..]);
                let (bk, bh, _) = Self::entry(&bytes[*b..]);
                ak.cmp(bk).then_with(|| ah.cmp(bh))
            });
            Ok(plan)
        }
        fn entry(mut bytes: &[u8]) -> (&[u8], &[u8], &[u8]) {
            // Only this constructor writes the immutable plan; every length
            // was checked and every byte initialized before TX entry.
            let n = length(&mut bytes).expect("prepaid group plan key length");
            let key = take(&mut bytes, n).expect("prepaid group plan key");
            let hash = take(&mut bytes, 32).expect("prepaid group plan digest");
            let n = length(&mut bytes).expect("prepaid group plan reverse length");
            let reverse = take(&mut bytes, n).expect("prepaid group plan reverse");
            (key, hash, reverse)
        }
        fn get(&self, key: &str, value: &[u8]) -> Result<Option<&[u8]>> {
            let hash = blake3::hash(value);
            let found = self.offsets.binary_search_by(|offset| {
                let (stored, digest, _) = Self::entry(&self.bytes[*offset..]);
                stored
                    .cmp(key.as_bytes())
                    .then_with(|| digest.cmp(hash.as_bytes()))
            });
            Ok(found
                .ok()
                .map(|index| Self::entry(&self.bytes[self.offsets[index]..]).2))
        }
    }

    pub(in super::super) fn maintain(
        write: &redb::WriteTransaction,
        key: &str,
        value: Option<&[u8]>,
        funding: Funding<'_>,
    ) -> Result<()> {
        use redb::ReadableTable;
        if write
            .table_root_fingerprint(GROUP_TABLE)
            .map_err(RedbPersistence::storage_error)?
            .is_none()
        {
            return Ok(());
        }
        let mut groups = write
            .open_table(GROUP_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        let complete = groups
            .get(COMPLETE)
            .map_err(RedbPersistence::storage_error)?
            .is_some_and(|v| v.value() == COMPLETE_VALUE);
        if !complete {
            return Ok(());
        }
        // Ordinary scratch drops before its originating proof debit is released.
        // Owned bytes are borrowed from the pre-TX plan throughout the kernel.
        let mut funding = funding;
        let scratch = match (value, &mut funding) {
            (Some(value), Funding::Legacy(admission)) => {
                let bytes = reverse_size(key, value)?;
                admission.admit(bytes)?;
                let mut scratch = vec![0; bytes];
                fill_reverse(key, value, &mut scratch)?;
                Some(scratch)
            }
            _ => None,
        };
        let new = match (value, funding) {
            (None, _) => None,
            (Some(value), Funding::Prepaid(plan)) => match plan.get(key, value)? {
                Some(bytes) => Some(bytes),
                None => {
                    drop(groups);
                    return invalidate(write);
                }
            },
            (Some(_), Funding::Legacy(_)) => scratch.as_deref(),
            (Some(_), Funding::Unavailable) => {
                drop(groups);
                return invalidate(write);
            }
        };
        let mut reverse = write
            .open_table(REVERSE_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        {
            // Borrow the old reverse guard. No old record decode/copy/scratch.
            let previous = reverse.get(key).map_err(RedbPersistence::storage_error)?;
            if previous.is_none()
                && write
                    .open_table(READ_CUSTODY_CONFIG_TABLE)
                    .map_err(RedbPersistence::storage_error)?
                    .get(key)
                    .map_err(RedbPersistence::storage_error)?
                    .is_some()
            {
                return Err(invalid());
            }
            if let Some(previous) = previous {
                let mut bytes = previous.value();
                while !bytes.is_empty() {
                    let n = length(&mut bytes)?;
                    let entry = take(&mut bytes, n)?;
                    let mut membership = entry;
                    let group_len = length(&mut membership)?;
                    take(&mut membership, group_len)?;
                    if membership != key.as_bytes() {
                        return Err(invalid());
                    }
                    groups
                        .remove(entry)
                        .map_err(RedbPersistence::storage_error)?;
                }
            }
        }
        if let Some(new) = new {
            let mut bytes = new;
            while !bytes.is_empty() {
                let n = length(&mut bytes)?;
                let entry = take(&mut bytes, n)?;
                groups
                    .insert(entry, b"".as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            reverse
                .insert(key, new)
                .map_err(RedbPersistence::storage_error)?;
        } else {
            reverse
                .remove(key)
                .map_err(RedbPersistence::storage_error)?;
        }
        Ok(())
    }
}
pub(super) use native::{maintain, plan_bytes};

pub(super) struct GroupPlan {
    bytes: Vec<u8>,
    offsets: Vec<usize>,
}
pub(super) enum Funding<'a> {
    Legacy(&'a mut content_identity::ProofAdmission),
    Prepaid(&'a GroupPlan),
    Unavailable,
}
